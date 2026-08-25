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
direct_label=${DIRECT_LABEL:-direct}
tunnel_label=${TUNNEL_LABEL:-tunnel}
append=${BENCH_APPEND:-0}
new_connection_requests=${NEW_CONNECTION_REQUESTS:-2000}
read -r -a paths <<< "${BENCH_PATHS:-direct tunnel}"

for label in "$direct_label" "$tunnel_label"; do
  if [[ ! $label =~ ^[A-Za-z0-9_.-]+$ ]]; then
    echo "benchmark labels may contain only letters, numbers, dots, underscores, and hyphens" >&2
    exit 2
  fi
done

mkdir -p "$result_directory"
summary_csv="$result_directory/summary.csv"
process_csv="$result_directory/processes.csv"
summary_header='run,path,case,response_bytes,request_bytes,concurrency,keepalive,responses,errors,elapsed_seconds,requests_per_sec,mean_ms,p50_ms,p95_ms,p99_ms,ttfb_mean_ms,ttfb_p50_ms,ttfb_p95_ms,ttfb_p99_ms,success_rate,response_mbps,request_mbps,total_payload_mbps'
process_header='run,path,case,process,pid,cpu_percent_of_one_core,peak_rss_kib'

if [[ $append == 1 ]]; then
  if [[ ! -e $summary_csv || $(head -n 1 "$summary_csv") != "$summary_header" ]]; then
    echo "BENCH_APPEND=1 requires an existing summary.csv with the current schema" >&2
    exit 2
  fi
else
  if [[ -e $summary_csv ]]; then
    echo "$summary_csv already exists; choose another directory or set BENCH_APPEND=1" >&2
    exit 2
  fi
  printf '%s\n' "$summary_header" > "$summary_csv"
  printf '%s\n' "$process_header" > "$process_csv"
fi

default_cases=(
  'latency_c1 1 0 1 keepalive'
  'latency_c64 1 0 64 keepalive'
  'small_c32 1024 0 32 keepalive'
  'small_c128 1024 0 128 keepalive'
  'small_c192 1024 0 192 keepalive'
  'medium_c64 65536 0 64 keepalive'
  'medium_c192 65536 0 192 keepalive'
  'download_1m_c1 1048576 0 1 keepalive'
  'download_1m_c16 1048576 0 16 keepalive'
  'download_1m_c64 1048576 0 64 keepalive'
  'download_1m_c192 1048576 0 192 keepalive'
  'download_8m_c1 8388608 0 1 keepalive'
  'download_8m_c16 8388608 0 16 keepalive'
  'upload_1m_c1 1 1048576 1 keepalive'
  'upload_1m_c16 1 1048576 16 keepalive'
  'upload_1m_c64 1 1048576 64 keepalive'
  'upload_1m_c192 1 1048576 192 keepalive'
  'upload_8m_c1 1 8388608 1 keepalive'
  'upload_8m_c16 1 8388608 16 keepalive'
  'round_trip_1m_c16 1048576 1048576 16 keepalive'
  'round_trip_1m_c64 1048576 1048576 64 keepalive'
  'round_trip_1m_c192 1048576 1048576 192 keepalive'
  'new_connection 1 0 8 new'
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
  local base label url output load_pid body_path elapsed_seconds
  local specification pid rss final_ticks cpu_percent name index
  local -a options process_specs=() monitored_specs=() initial_ticks=() peak_rss=()

  if [[ $path == direct ]]; then
    base=$direct_base
    label=$direct_label
  else
    base=$tunnel_base
    label=$tunnel_label
  fi
  url="$base/bytes/$response_size"
  output="$result_directory/${run}-${label}-${case_name}.json"
  if [[ -e $output ]]; then
    echo "$output already exists; refusing to overwrite it" >&2
    exit 2
  fi
  options=(--no-tui --no-color --wait-ongoing-requests-after-deadline
    --output-format json --output "$output" -c "$concurrency")
  if [[ $connection_mode == new ]]; then
    options+=(-n "$new_connection_requests" --disable-keepalive)
  else
    options+=(-z "$duration")
  fi
  if (( request_size > 0 )); then
    body_path="$result_directory/request-${request_size}.bin"
    if [[ ! -e $body_path ]]; then
      truncate -s "$request_size" "$body_path"
    fi
    options+=(--method POST -D "$body_path")
  fi
  if [[ $path == tunnel && -n $tunnel_connect_to ]]; then
    options+=(--connect-to "$tunnel_connect_to")
  fi

  echo "run=$run path=$label case=$case_name"

  if [[ -n $processes ]]; then
    read -r -a process_specs <<< "$processes"
    for specification in "${process_specs[@]}"; do
      pid=${specification#*=}
      if [[ -r /proc/$pid/stat ]]; then
        monitored_specs+=("$specification")
        initial_ticks+=("$(process_ticks "$pid")")
        peak_rss+=("$(process_rss "$pid")")
      fi
    done
  fi

  "$oha" "${options[@]}" "$url" &
  load_pid=$!
  while kill -0 "$load_pid" 2>/dev/null; do
    for ((index = 0; index < ${#monitored_specs[@]}; index++)); do
      specification=${monitored_specs[$index]}
      pid=${specification#*=}
      if [[ -r /proc/$pid/status ]]; then
        rss=$(process_rss "$pid")
        if (( rss > peak_rss[index] )); then
          peak_rss[$index]=$rss
        fi
      fi
    done
    sleep 0.1
  done
  wait "$load_pid"
  elapsed_seconds=$(jq -r '.summary.total' "$output")

  jq -r \
    --arg run "$run" --arg path "$label" --arg case "$case_name" \
    --arg response_size "$response_size" --arg request_size "$request_size" \
    --arg concurrency "$concurrency" --arg keepalive "$connection_mode" \
    '[ $run, $path, $case, $response_size, $request_size, $concurrency, $keepalive,
       ([.statusCodeDistribution[]] | add // 0),
       ([.errorDistribution[]] | add // 0),
       .summary.total, .summary.requestsPerSec, (.summary.average * 1000),
       (.latencyPercentiles.p50 * 1000), (.latencyPercentiles.p95 * 1000),
       (.latencyPercentiles.p99 * 1000),
       (.details.firstByte.average * 1000),
       (.firstBytePercentiles.p50 * 1000), (.firstBytePercentiles.p95 * 1000),
       (.firstBytePercentiles.p99 * 1000), .summary.successRate,
       (.summary.requestsPerSec * ($response_size | tonumber) * 8 / 1000000),
       (.summary.requestsPerSec * ($request_size | tonumber) * 8 / 1000000),
       (.summary.requestsPerSec * (($response_size | tonumber) + ($request_size | tonumber)) * 8 / 1000000)
     ] | @csv' \
    "$output" >> "$summary_csv"

  for ((index = 0; index < ${#monitored_specs[@]}; index++)); do
    specification=${monitored_specs[$index]}
    name=${specification%%=*}
    pid=${specification#*=}
    if [[ -r /proc/$pid/stat ]]; then
      final_ticks=$(process_ticks "$pid")
      cpu_percent=$(awk -v ticks="$((final_ticks - initial_ticks[index]))" \
        -v hz="$(getconf CLK_TCK)" -v elapsed="$elapsed_seconds" \
        'BEGIN { printf "%.3f", ticks / hz / elapsed * 100 }')
      printf '%s,%s,%s,%s,%s,%s,%s\n' \
        "$run" "$label" "$case_name" "$name" "$pid" "$cpu_percent" "${peak_rss[$index]}" \
        >> "$process_csv"
    fi
  done
}

# Warm only the paths selected for this phase.
for path in "${paths[@]}"; do
  if [[ $path == direct ]]; then
    "$oha" --no-tui --output-format quiet -z 2s -c 4 "$direct_base/bytes/1024"
  elif [[ $path == tunnel ]]; then
    if [[ -n $tunnel_connect_to ]]; then
      "$oha" --no-tui --output-format quiet -z 2s -c 4 \
        --connect-to "$tunnel_connect_to" "$tunnel_base/bytes/1024"
    else
      "$oha" --no-tui --output-format quiet -z 2s -c 4 "$tunnel_base/bytes/1024"
    fi
  else
    echo "BENCH_PATHS entries must be 'direct' or 'tunnel'" >&2
    exit 2
  fi
done

for ((run = 1; run <= repetitions; run++)); do
  for definition in "${cases[@]}"; do
    read -r case_name response_size request_size concurrency connection_mode <<< "$definition"
    for path in "${paths[@]}"; do
      run_case "$run" "$path" "$case_name" "$response_size" "$request_size" "$concurrency" "$connection_mode"
    done
  done
done

echo "results written to $result_directory"
echo "summary written to $summary_csv"
node "$(dirname "$0")/summarize.js" "$summary_csv" "$result_directory/aggregate.csv"
echo "medians written to $result_directory/aggregate.csv"
