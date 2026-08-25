# TNLD vs Cloudflare Tunnel at concurrency 192

Run on August 25, 2026 from this Mac. Both tunnels forwarded to the same Node HTTP server on
`127.0.0.1:18080`. The direct path, TNLD, and Cloudflare ran in separate phases. No tunnel clients
shared the machine during a measured phase.

## Setup

- TNLD used `tnlc 0.1.0` and the TNLD server in St. Louis.
- Cloudflare used `cloudflared 2026.8.2` and an accountless Quick Tunnel. It selected QUIC to the
  `ord08` Chicago edge.
- `oha 1.16.0` generated load from the same Mac that hosted the origin.
- Each case ran for five seconds and was repeated three times. The fresh-connection case used 2,000
  requests per repetition instead of a time limit.
- The tables report medians across the three repetitions.
- Mbps is application payload throughput. It excludes HTTP framing and TLS overhead.

## Short responses and connection setup

| Case | TNLD req/s | Cloudflare req/s | TNLD p50 | Cloudflare p50 |
|---|---:|---:|---:|---:|
| 1 byte, concurrency 1 | 28.52 | 34.22 | 34.08 ms | 28.00 ms |
| 1 byte, concurrency 64 | 1,374.67 | 1,770.66 | 41.59 ms | 34.18 ms |
| 1 KiB, concurrency 32 | 816.08 | 806.81 | 37.96 ms | 34.25 ms |
| 1 KiB, concurrency 128 | 2,789.81 | 3,296.77 | 41.53 ms | 35.77 ms |
| 1 KiB, concurrency 192 | 4,058.44 | 4,676.98 | 42.95 ms | 37.30 ms |
| Fresh TLS connections, concurrency 8 | 83.66 | 120.51 | 93.17 ms | 64.57 ms |

Cloudflare had lower median latency in every short-response case. TNLD edged it by 1.1% in request
rate at 1 KiB and concurrency 32, but Cloudflare was 13% to 29% faster in the other short-response
and connection-setup cases.

## Downloads

| Case | TNLD Mbps | Cloudflare Mbps | TNLD p50 | Cloudflare p50 |
|---|---:|---:|---:|---:|
| 64 KiB, concurrency 64 | 115.98 | 78.59 | 221.75 ms | 385.28 ms |
| 64 KiB, concurrency 192 | 115.58 | 70.23 | 595.95 ms | 1,361.52 ms |
| 1 MiB, concurrency 1 | 76.80 | 59.69 | 99.13 ms | 135.51 ms |
| 1 MiB, concurrency 16 | 114.41 | 80.94 | 1,071.39 ms | 1,582.83 ms |
| 1 MiB, concurrency 64 | 117.76 | 76.31 | 4,196.16 ms | 6,808.28 ms |
| 1 MiB, concurrency 192 | 122.28 | 69.08 | 11,874.44 ms | 20,558.34 ms |
| 8 MiB, concurrency 1 | 117.38 | 67.73 | 542.03 ms | 977.17 ms |
| 8 MiB, concurrency 16 | 126.11 | 71.71 | 7,902.06 ms | 14,868.71 ms |

TNLD won every download case. Its lead grew with response size and concurrency, reaching 76% more
throughput for 8 MiB responses at concurrency 16 and 77% more for 1 MiB responses at concurrency 192.

## Uploads

| Case | TNLD Mbps | Cloudflare Mbps | TNLD p50 | Cloudflare p50 |
|---|---:|---:|---:|---:|
| 1 MiB, concurrency 1 | 89.60 | 40.86 | 86.14 ms | 171.34 ms |
| 1 MiB, concurrency 16 | 141.35 | 82.48 | 850.32 ms | 1,649.00 ms |
| 1 MiB, concurrency 64 | 125.81 | 85.09 | 3,547.16 ms | 6,300.92 ms |
| 1 MiB, concurrency 192 | 124.09 | 105.02 | 10,891.29 ms | 15,308.04 ms |
| 8 MiB, concurrency 1 | 129.49 | 96.56 | 505.07 ms | 706.57 ms |
| 8 MiB, concurrency 16 | 136.74 | 102.56 | 7,116.07 ms | 10,468.62 ms |

TNLD won every upload case. The lead ranged from 18% at concurrency 192 to 119% at concurrency 1.

## Simultaneous upload and download

Each request uploaded 1 MiB and downloaded 1 MiB. The Mbps column is the combined payload rate.

| Concurrency | TNLD Mbps | Cloudflare Mbps | TNLD p50 | Cloudflare p50 |
|---:|---:|---:|---:|---:|
| 16 | 135.82 | 94.02 | 1,815.51 ms | 2,990.42 ms |
| 64 | 140.11 | 101.71 | 6,427.82 ms | 10,542.68 ms |
| 192 | 143.58 | 106.91 | 17,338.30 ms | 30,097.29 ms |

TNLD moved 34% to 45% more payload in these cases. The concurrency-192 result comes with a serious
reliability problem described below.

## Reliability

Cloudflare completed 174,363 responses with no recorded errors or HTTP 429 responses. TNLD completed
150,001 responses and `oha` recorded 66 timeouts:

- Three timeouts in repetition 1 of the 1 MiB upload at concurrency 192.
- Twenty-six and twenty-one timeouts in repetitions 2 and 3 of the 1 MiB bidirectional case at
  concurrency 192. Those runs had 86.73% and 89.06% success.
- Sixteen timeouts in repetition 3 of the fresh-connection case, for 99.2% success in that run.

TNLD's overall success rate was 99.956%, but that aggregate hides the sharp failure in the hardest
bidirectional case. Cloudflare was slower on payloads and handled the full matrix without an error.

## Bottom line

Cloudflare is better for tiny HTTP requests and repeated TLS connection setup. TNLD is much faster for
payloads on this route. At concurrency 192, TNLD trades some reliability for that speed, while the
Cloudflare Quick Tunnel remains slower but completes every request.

This is still a Quick Tunnel comparison. Quick Tunnels use one connector connection and Cloudflare
limits them to 200 in-flight requests. A normal named Cloudflare Tunnel uses a different connection
layout, so these numbers should not be presented as a production Cloudflare Tunnel benchmark.

## Files

- `aggregate.csv` contains the medians, minimum success rate, and total errors for every case.
- `summary.csv` contains one row per repetition.
- The 207 JSON files are the raw `oha` results for all three paths.
