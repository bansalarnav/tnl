# TNLD vs Cloudflare Tunnel benchmark

Run on August 24, 2026 from this Mac. The origin was the same in-memory Node HTTP server on `127.0.0.1:18080`. Only one tunnel client ran at a time.

## Setup

- TNLD: `tnlc 0.1.0`, remote server in St. Louis, HTTPS visitor endpoint
- Cloudflare: `cloudflared 2026.8.2`, accountless Quick Tunnel, Chicago `ord12` edge, default QUIC connector
- Load generator: `oha 1.16.0`
- Three repetitions per case, five seconds each, with keep-alive
- Values below are medians of the three repetitions
- All TNLD and Cloudflare measurements returned 100% HTTP 200 responses
- Cloudflare returned `cf-cache-status: DYNAMIC` for the 1 MiB response

## Results

| Case | Metric | TNLD | Cloudflare default | Result |
|---|---:|---:|---:|---:|
| Empty response, concurrency 1 | p50 latency | 30.32 ms | 28.75 ms | Cloudflare 5.2% lower |
| 1 KiB response, concurrency 32 | requests/s | 753.15 | 844.00 | Cloudflare 12.1% higher |
| 1 MiB download, concurrency 1 | throughput | 95.93 Mbps | 42.56 Mbps | TNLD 2.25x |
| 1 MiB download, concurrency 16 | throughput | 146.58 Mbps | 53.54 Mbps | TNLD 2.74x |
| 1 MiB download, concurrency 64 | throughput | 140.52 Mbps | 57.63 Mbps | TNLD 2.44x |
| 1 MiB upload, concurrency 16 | throughput | 133.27 Mbps | 67.89 Mbps | TNLD 1.96x |
| 1 MiB upload plus 1 MiB response, concurrency 16 | throughput per direction | 74.15 Mbps | 43.46 Mbps | TNLD 1.71x |

TNLD lost the small-message cases by a little and won every bulk-transfer case by a lot on this run.

## Cloudflare HTTP/2 connector check

Cloudflare was also measured with its connector pinned to HTTP/2 instead of the default QUIC choice. Bulk throughput improved substantially: 87.08 Mbps at download concurrency 1, 138.79 Mbps at concurrency 16, 129.92 Mbps at concurrency 64, 145.63 Mbps for uploads, and 71.79 Mbps per direction for the combined case. With HTTP/2, the two products were close. TNLD led most bulk cases by 3% to 10%, while Cloudflare led small responses and uploads.

## Excluded case

The fresh-connection case was excluded. Localhost socket churn filled the Mac's ephemeral-port table with about 32,500 `TIME_WAIT` entries and affected later direct-baseline runs. The direct baseline's first clean repetition showed that the in-memory origin was not the throughput limit. Raw files for the excluded direct case remain in the directory for inspection.

## Files

- `summary.csv` contains every recorded row.
- Each JSON file is the raw `oha` output for one repetition.
- Rows named `cloudflare_quic` are the Cloudflare default used in the main table.
- Rows named `cloudflare` are the pinned HTTP/2 check.
