#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 DIRECT_BASE_URL TUNNEL_BASE_URL [RESULT_DIRECTORY]" >&2
  exit 2
fi

direct_base=${1%/}
tunnel_base=${2%/}
result_directory=${3:-"benchmarks/e2e/results/$(date -u +%Y%m%dT%H%M%SZ)"}
oha=${OHA:-oha}
duration=${DURATION:-10s}
repetitions=${REPETITIONS:-3}
processes=${BENCH_PROCESSES:-}
tunnel_connect_to=${TUNNEL_CONNECT_TO:-}
read -r -a paths <<< "${BENCH_PATHS:-direct tunnel}"

mkdir -p "$result_directory"
summary_csv="$result_directory/summary.csv"
process_csv="$result_directory/processes.csv"
printf '%s\n' 'run,path,case,response_bytes,request_bytes,concurrency,keepalive,requests_per_sec,mean_ms,p50_ms,p95_ms,p99_ms,success_rate' > "$summary_csv"
printf '%s\n' 'run,path,case,process,pid,cpu_percent_of_one_core,peak_rss_kib' > "$process_csv"

default_cases=(
  'latency 0 0 1 keepalive'
  'small_download 1024 0 32 keepalive'
  'download_c1 1048576 0 1 keepalive'
  'download_c16 1048576 0 16 keepalive'
  'download_c64 1048576 0 64 keepalive'
  'upload_c1 0 1048576 1 keepalive'
  'upload_c16 0 1048576 16 keepalive'
  'upload_c64 0 1048576 64 keepalive'
  'round_trip_bulk 1048576 1048576 16 keepalive'
  'new_connection 0 0 8 new'
)
if [[ -n ${BENCH_CASES:-} ]]; then
  IFS=';' read -r -a cases <<< "$BENCH_CASES"
else
  cases=("${default_cases[@]}")
fi

process_ticks() {
  awk '{ print $14 + $15 }' "/proc/$1/stat"
}

process_rss() {
  awk '/^VmRSS:/ { print $2 }' "/proc/$1/status"
}

run_case() {
  local run=$1 path=$2 case_name=$3 response_size=$4 request_size=$5 concurrency=$6 connection_mode=$7
  local base url output started_ns finished_ns elapsed_ns load_pid body_path
  local -a options process_specs
  local -A initial_ticks peak_rss

  if [[ $path == direct ]]; then
    base=$direct_base
  else
    base=$tunnel_base
  fi
  url="$base/bytes/$response_size"
  output="$result_directory/${run}-${path}-${case_name}.json"
  options=(--no-tui --no-color --wait-ongoing-requests-after-deadline \
    --output-format json --output "$output" -z "$duration" -c "$concurrency")
  if (( request_size > 0 )); then
    body_path="$result_directory/request-${request_size}.bin"
    if [[ ! -e $body_path ]]; then
      truncate -s "$request_size" "$body_path"
    fi
    options+=(--method POST -D "$body_path")
  fi
  if [[ $connection_mode == new ]]; then
    options+=(--disable-keepalive)
  fi
  if [[ $path == tunnel && -n $tunnel_connect_to ]]; then
    options+=(--connect-to "$tunnel_connect_to")
  fi

  read -r -a process_specs <<< "$processes"
  for specification in "${process_specs[@]}"; do
    local pid=${specification#*=}
    if [[ -r /proc/$pid/stat ]]; then
      initial_ticks[$specification]=$(process_ticks "$pid")
      peak_rss[$specification]=$(process_rss "$pid")
    fi
  done

  started_ns=$(date +%s%N)
  "$oha" "${options[@]}" "$url" &
  load_pid=$!
  while kill -0 "$load_pid" 2>/dev/null; do
    for specification in "${process_specs[@]}"; do
      local pid=${specification#*=}
      if [[ -r /proc/$pid/status ]]; then
        local rss
        rss=$(process_rss "$pid")
        if (( rss > ${peak_rss[$specification]:-0} )); then
          peak_rss[$specification]=$rss
        fi
      fi
    done
    sleep 0.1
  done
  wait "$load_pid"
  finished_ns=$(date +%s%N)
  elapsed_ns=$((finished_ns - started_ns))

  jq -r \
    --arg run "$run" --arg path "$path" --arg case "$case_name" \
    --arg response_size "$response_size" --arg request_size "$request_size" \
    --arg concurrency "$concurrency" --arg keepalive "$connection_mode" \
    '[ $run, $path, $case, $response_size, $request_size, $concurrency, $keepalive,
       .summary.requestsPerSec, (.summary.average * 1000),
       (.latencyPercentiles.p50 * 1000), (.latencyPercentiles.p95 * 1000),
       (.latencyPercentiles.p99 * 1000), .summary.successRate ] | @csv' \
    "$output" >> "$summary_csv"

  for specification in "${process_specs[@]}"; do
    local name=${specification%%=*} pid=${specification#*=}
    if [[ -r /proc/$pid/stat && -n ${initial_ticks[$specification]+set} ]]; then
      local final_ticks cpu_percent
      final_ticks=$(process_ticks "$pid")
      cpu_percent=$(awk -v ticks="$((final_ticks - initial_ticks[$specification]))" \
        -v hz="$(getconf CLK_TCK)" -v elapsed_ns="$elapsed_ns" \
        'BEGIN { printf "%.3f", ticks / hz / (elapsed_ns / 1000000000) * 100 }')
      printf '%s,%s,%s,%s,%s,%s,%s\n' \
        "$run" "$path" "$case_name" "$name" "$pid" "$cpu_percent" "${peak_rss[$specification]}" \
        >> "$process_csv"
    fi
  done
}

# Warm both paths before recording results.
"$oha" --no-tui --output-format quiet -z 2s -c 4 "$direct_base/bytes/1024"
warm_options=()
if [[ -n $tunnel_connect_to ]]; then
  warm_options+=(--connect-to "$tunnel_connect_to")
fi
"$oha" --no-tui --output-format quiet -z 2s -c 4 "${warm_options[@]}" "$tunnel_base/bytes/1024"

for ((run = 1; run <= repetitions; run++)); do
  for definition in "${cases[@]}"; do
    read -r case_name response_size request_size concurrency connection_mode <<< "$definition"
    for path in "${paths[@]}"; do
      if [[ $path != direct && $path != tunnel ]]; then
        echo "BENCH_PATHS entries must be 'direct' or 'tunnel'" >&2
        exit 2
      fi
      run_case "$run" "$path" "$case_name" "$response_size" "$request_size" "$concurrency" "$connection_mode"
    done
  done
done

echo "results written to $result_directory"
column -s, -t "$summary_csv" 2>/dev/null || cat "$summary_csv"
