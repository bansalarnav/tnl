#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
binary="$here/target/release/quic-comparison"
results=${RESULTS:-"$here/results.jsonl"}
total_bytes=${TOTAL_BYTES:-268435456}
stream_counts=${STREAM_COUNTS:-"1 8 32"}
samples=${SAMPLES:-5}
warmups=${WARMUPS:-1}
port=${PORT:-44330}
server_cpus=${SERVER_CPUS:-}
client_cpus=${CLIENT_CPUS:-}

if ! command -v cmake >/dev/null; then
    echo "cmake is required to build quiche's BoringSSL dependency" >&2
    exit 1
fi

cargo build --release --manifest-path "$here/Cargo.toml"
: > "$results"

server_pid=
cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

pin() {
    local cpus=$1
    shift
    if [[ -n "$cpus" ]]; then
        exec taskset --cpu-list "$cpus" "$@"
    else
        exec "$@"
    fi
}

run_one() {
    local backend=$1 workload=$2 streams=$3
    local bytes_each=$((total_bytes / streams))
    local server_log
    server_log=$(mktemp)

    pin "$server_cpus" "$binary" "$backend" --addr "127.0.0.1:$port" >"$server_log" 2>&1 &
    server_pid=$!
    for _ in $(seq 1 200); do
        if grep -q '^READY ' "$server_log"; then break; fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            cat "$server_log" >&2
            return 1
        fi
        sleep 0.025
    done
    if ! grep -q '^READY ' "$server_log"; then
        echo "$backend did not become ready" >&2
        cat "$server_log" >&2
        return 1
    fi

    pin "$client_cpus" "$binary" client \
        --addr "127.0.0.1:$port" \
        --workload "$workload" \
        --streams "$streams" \
        --bytes-per-stream "$bytes_each" \
        --repetitions "$((warmups + samples))" \
        | tail -n "$samples" \
        | sed "s/^{/{\"backend\":\"$backend\",/" \
        | tee -a "$results"

    cleanup
    server_pid=
    rm -f "$server_log"
}

for backend in quiche tokio-quiche s2n-quic; do
    for workload in download upload bidi; do
        for streams in $stream_counts; do
            run_one "$backend" "$workload" "$streams"
        done
    done
done

echo "wrote $results"
