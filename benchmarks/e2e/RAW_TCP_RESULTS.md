# Raw TCP results

The final local comparison used release binaries, five-second cases, and three repetitions. Each
tunnel run immediately followed a direct-origin run for the same case. All samples completed with
zero errors.

The old implementation wrapped visitor TLS ciphertext in another TLS connection between `tnld`
and `tnlc`. Protocol v4 keeps HTTPS control sessions but replaces dedicated data connections with
raw TCP sockets authenticated by a nonce-bound HMAC proof.

| Workload | Outer TLS | Raw TCP | Change |
|---|---:|---:|---:|
| 1 MiB download, c1 | 232 MiB/s | 364 MiB/s | +57.0% |
| 1 MiB download, c16 | 1,064 MiB/s | 1,308 MiB/s | +23.0% |
| 1 MiB download, c64 | 1,173 MiB/s | 1,209 MiB/s | +3.1% |
| 1 MiB upload, c1 | 208 MiB/s | 260 MiB/s | +25.1% |
| 1 MiB upload, c16 | 1,010 MiB/s | 997 MiB/s | -1.3% |

The five bulk rows have a 19.7% geometric-mean absolute improvement. After normalizing each tunnel
sample against its adjacent direct run, the geometric-mean improvement is 14.5%. The c16 upload
result is a tie within run variance. Its raw-TCP three-run mean was 0.4% higher even though its
median was 1.3% lower.

| Workload | Outer TLS p50 | Raw TCP p50 |
|---|---:|---:|
| Empty response, c1 | 0.587 ms | 0.701 ms |
| Empty response, c64 | 4.658 ms | 4.642 ms |
| 1 KiB response, c32 | 2.333 ms | 2.439 ms |
| 1 MiB download, c1 | 4.004 ms | 2.498 ms |
| 1 MiB download, c16 | 13.990 ms | 11.633 ms |
| 1 MiB download, c64 | 45.389 ms | 44.063 ms |
| 1 MiB upload, c1 | 4.415 ms | 3.569 ms |
| 1 MiB upload, c16 | 14.625 ms | 14.191 ms |
| Fresh connections, c8 | 4.205 ms | 4.846 ms |

The empty c1 raw-TCP median includes one host outlier whose adjacent direct rate fell from roughly
6,000 to 2,930 requests per second. The other two old and new pairs were close. The benchmark's
direct path is plain HTTP over loopback while its tunnel path includes visitor TLS, so the direct
ratio does not isolate transport overhead for tiny sequential requests.
