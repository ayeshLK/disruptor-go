# Performance results

## Notation legend

| Notation | Meaning |
|---|---|
| SPSC | One producer and one consumer |
| MPSC | Multiple producers and one consumer |
| broadcast-N | Every event is delivered independently to N consumers |
| pipeline-N | Every event passes through N ordered consumer stages |
| B, KiB, MiB | Bytes, 1,024 bytes, and 1,048,576 bytes respectively |
| ns, μs, ns/op, μs/op | Nanoseconds, microseconds, or either unit per benchmark operation |
| events/s, Published/s | Source events completed or published per second |
| K, M | Decimal thousand and million suffixes in summarized rates |
| deliveries/s | Handler invocations completed per second across all consumers or stages |
| payload MiB/s | Logical source payload bytes completed per second; not aggregate memory traffic or internal copying |
| B/op, allocs/op | Heap bytes and heap allocations per benchmark operation |
| p50, p95, p99, p99.9 | Nearest-rank latency percentiles |
| max | Largest sampled latency |
| GOMAXPROCS | Maximum number of CPUs executing Go code simultaneously |
| working set | Ring size multiplied by reusable payload size, excluding event metadata |
| CPU, RSS | Aggregate process CPU utilization and resident set size |

See `PERFORMANCE.md` for the canonical matrix, measurement separation, and
regression policy. Add every new abbreviation, unit, percentile label, or
throughput term to this legend when it first appears.

## Full v1 baseline — 2026-09-12

Measured from clean commit `b3a4c3d6225ca3f7fc000f10503a927e16f3eb19`.
These are local development results, not portable guarantees or release
thresholds. The `powersave` governor, existing swap use, and ordinary desktop
activity make the wide MPSC distributions especially important to retain.

### Environment

- Time: `2026-09-12T07:46:29+05:30`
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- Cache: 128 KiB L1d, 1 MiB L2, and 8 MiB L3 (aggregate `lscpu` values)
- OS: Linux 7.0.0-30-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`
- Initial load average: 1.04, 1.12, 1.16
- Memory: 15 GiB total, 6.9 GiB available, 1.1 GiB swap in use

### Microbenchmarks

```bash
go test -run='^$' -bench=. -benchmem -benchtime=1s -count=10
```

The table summarizes all ten sequential samples as minimum / median / maximum.
All cases measured 0 B/op and 0 allocs/op except the two blocking waits shown.
The complete raw output remains the comparison input; summary values alone
must not be used for a statistical regression decision.

| Area | Scenario | ns/op min / median / max | Allocations |
|---|---|---:|---:|
| Claim/publish | Single, batch 1 | 12.15 / 13.91 / 16.21 | 0 / 0 |
| Claim/publish | Single, batch 16 | 1.474 / 1.577 / 1.764 | 0 / 0 |
| Claim/publish | Single, batch 256 | 0.880 / 0.957 / 1.155 | 0 / 0 |
| Claim/publish | Multi, batch 1 | 20.00 / 22.01 / 24.28 | 0 / 0 |
| Claim/publish | Multi, batch 16 | 7.995 / 8.659 / 9.337 | 0 / 0 |
| Claim/publish | Multi, batch 256 | 7.362 / 10.18 / 15.41 | 0 / 0 |
| Try claim/publish | Single, batch 1 | 9.687 / 9.872 / 10.17 | 0 / 0 |
| Try claim/publish | Single, batch 16 | 1.207 / 1.216 / 1.325 | 0 / 0 |
| Try claim/publish | Single, batch 256 | 0.767 / 0.866 / 1.078 | 0 / 0 |
| Try claim/publish | Multi, batch 1 | 20.52 / 23.57 / 27.02 | 0 / 0 |
| Try claim/publish | Multi, batch 16 | 7.344 / 8.254 / 9.034 | 0 / 0 |
| Try claim/publish | Multi, batch 256 | 6.661 / 7.246 / 8.771 | 0 / 0 |
| Publication gap | Multi-producer scan | 34.13 / 36.83 / 41.61 | 0 / 0 |
| Topology | SPSC | 30.85 / 35.79 / 41.05 | 0 / 0 |
| Topology | MPSC-2 | 97.31 / 112.0 / 125.7 | 0 / 0 |
| Topology | MPSC-4 | 125.8 / 136.4 / 178.1 | 0 / 0 |
| Topology | broadcast-2 | 31.62 / 35.15 / 40.39 | 0 / 0 |
| Topology | pipeline-2 | 25.35 / 26.45 / 49.37 | 0 / 0 |
| Consumer wait | Blocking | 162.8 / 169.7 / 185.7 | 112 B/op / 1 alloc/op |
| Consumer wait | Sleeping | 45.44 / 47.31 / 56.18 | 0 / 0 |
| Consumer wait | Yielding | 34.63 / 37.58 / 43.90 | 0 / 0 |
| Consumer wait | Busy-spin | 38.38 / 47.36 / 59.88 | 0 / 0 |
| Producer wait | Blocking | 34,020 / 35,560 / 37,060 | 112 B/op / 1 alloc/op |
| Producer wait | Yielding | 1,981 / 2,066 / 2,234 | 0 / 0 |
| Producer wait | Busy-spin | 611.3 / 623.9 / 695.3 | 0 / 0 |
| Baseline | Buffered channel SPSC | 68.45 / 70.73 / 80.57 | 0 / 0 |
| Baseline | Buffered channel MPSC | 89.82 / 97.17 / 124.3 | 0 / 0 |
| Baseline | Buffered channel broadcast-2 | 93.87 / 96.34 / 105.5 | 0 / 0 |
| Baseline | Buffered channel pipeline-2 | 90.54 / 96.68 / 109.0 | 0 / 0 |
| Legacy | Raw publish | 11.84 / 12.50 / 13.25 | 0 / 0 |
| Legacy | SPSC | 21.58 / 23.02 / 23.94 | 0 / 0 |

Payload entries are preallocated for all 65,536 slots and fully touched by the
producer and consumer. The 4 KiB referenced case measures external reusable
storage separately from the inline case.

| Scenario | ns/op min / median / max | payload MiB/s min / median / max | Working set |
|---|---:|---:|---:|
| SPSC inline 16 B | 37.84 / 39.92 / 42.22 | 361.4 / 382.4 / 403.2 | 1 MiB |
| SPSC inline 256 B | 155.0 / 165.1 / 178.4 | 1,369 / 1,479 / 1,576 | 16 MiB |
| SPSC inline 4 KiB | 2,152 / 2,298 / 2,722 | 1,435 / 1,700 / 1,815 | 256 MiB |
| SPSC referenced 4 KiB | 2,246 / 2,406 / 2,616 | 1,493 / 1,624 / 1,739 | 256 MiB |
| MPSC inline 16 B | 122.1 / 133.2 / 137.4 | 111.1 / 114.5 / 125.0 | 1 MiB |
| MPSC inline 256 B | 231.6 / 235.0 / 257.2 | 949.4 / 1,039 / 1,054 | 16 MiB |
| MPSC inline 4 KiB | 3,332 / 3,406 / 3,834 | 1,019 / 1,147 / 1,172 | 256 MiB |
| MPSC referenced 4 KiB | 3,334 / 3,434 / 3,779 | 1,034 / 1,138 / 1,171 | 256 MiB |

<details>
<summary>All microbenchmark samples in execution order</summary>

| Scenario | Ten ns/op samples | Ten payload MiB/s samples |
|---|---|---|
| Buffered channel broadcast-2 | 100.9, 93.87, 95.85, 96.84, 94.85, 93.98, 105.5, 94.21, 103.1, 97.83 | — |
| Buffered channel pipeline-2 | 95.76, 97.20, 101.1, 90.54, 93.04, 94.56, 96.15, 109.0, 108.9, 108.7 | — |
| Try claim/publish, single, batch 1 | 10.17, 9.878, 9.857, 9.734, 9.875, 9.733, 9.687, 9.986, 10.01, 9.870 | — |
| Try claim/publish, single, batch 16 | 1.215, 1.214, 1.207, 1.215, 1.222, 1.216, 1.214, 1.239, 1.254, 1.325 | — |
| Try claim/publish, single, batch 256 | 0.8062, 0.8042, 0.7674, 0.7818, 0.8141, 0.9183, 1.036, 1.078, 0.9998, 0.9630 | — |
| Try claim/publish, multi, batch 1 | 21.44, 24.72, 22.34, 22.42, 27.02, 26.92, 25.68, 24.73, 21.61, 20.52 | — |
| Try claim/publish, multi, batch 16 | 8.823, 8.355, 9.034, 8.985, 8.534, 8.153, 7.878, 7.404, 7.344, 7.680 | — |
| Try claim/publish, multi, batch 256 | 6.725, 6.888, 6.661, 6.922, 7.707, 7.116, 7.376, 8.771, 7.771, 7.556 | — |
| Multi-producer publication gap scan | 39.32, 36.79, 34.13, 34.93, 41.61, 39.50, 37.57, 36.86, 36.14, 34.51 | — |
| Claim/publish, single, batch 1 | 14.15, 13.24, 13.53, 16.21, 14.47, 12.84, 12.15, 14.57, 13.83, 13.99 | — |
| Claim/publish, single, batch 16 | 1.693, 1.701, 1.529, 1.474, 1.493, 1.584, 1.764, 1.570, 1.525, 1.594 | — |
| Claim/publish, single, batch 256 | 0.9143, 0.8802, 1.012, 1.155, 1.039, 0.9132, 0.9730, 0.9339, 0.9817, 0.9413 | — |
| Claim/publish, multi, batch 1 | 22.02, 22.25, 24.28, 20.35, 21.69, 22.07, 22.00, 23.79, 20.55, 20.00 | — |
| Claim/publish, multi, batch 16 | 7.995, 8.644, 8.510, 9.337, 8.570, 8.813, 8.674, 8.499, 8.865, 8.875 | — |
| Claim/publish, multi, batch 256 | 7.362, 8.329, 8.724, 9.109, 8.875, 11.33, 11.25, 13.56, 15.41, 12.78 | — |
| Topology SPSC | 30.85, 41.05, 37.85, 35.94, 35.10, 37.96, 32.82, 36.08, 35.64, 32.89 | — |
| Topology MPSC-2 | 125.7, 112.1, 106.5, 97.31, 101.9, 119.2, 111.9, 112.2, 116.6, 104.8 | — |
| Topology MPSC-4 | 164.6, 178.1, 132.2, 137.3, 142.9, 132.0, 140.8, 126.0, 135.5, 125.8 | — |
| Topology broadcast-2 | 36.23, 31.62, 32.95, 36.10, 40.39, 35.21, 34.62, 34.41, 35.67, 35.09 | — |
| Topology pipeline-2 | 26.10, 28.53, 25.35, 27.64, 26.45, 49.37, 26.14, 26.01, 26.45, 26.97 | — |
| Consumer wait, blocking | 176.6, 165.1, 172.0, 164.4, 170.4, 164.4, 162.8, 171.7, 185.7, 168.9 | — |
| Consumer wait, sleeping | 47.33, 54.21, 47.29, 46.62, 46.42, 46.86, 56.18, 48.68, 45.44, 48.70 | — |
| Consumer wait, yielding | 34.63, 41.08, 35.94, 38.36, 37.71, 35.44, 43.90, 37.44, 36.70, 37.93 | — |
| Consumer wait, busy-spin | 38.38, 49.20, 53.62, 59.88, 47.39, 42.18, 44.52, 47.33, 44.29, 52.45 | — |
| SPSC inline 16 B | 37.84, 38.39, 39.34, 40.50, 42.22, 40.92, 42.07, 41.12, 39.06, 39.32 | 403.2, 397.5, 387.9, 376.8, 361.4, 372.9, 362.7, 371.1, 390.7, 388.1 |
| SPSC inline 256 B | 178.4, 170.9, 157.9, 155.0, 170.1, 166.2, 159.6, 167.4, 164.0, 157.0 | 1,369, 1,429, 1,546, 1,576, 1,435, 1,469, 1,530, 1,458, 1,488, 1,555 |
| SPSC inline 4 KiB | 2,152, 2,667, 2,294, 2,300, 2,295, 2,217, 2,722, 2,153, 2,304, 2,394 | 1,815, 1,465, 1,703, 1,698, 1,702, 1,762, 1,435, 1,814, 1,696, 1,631 |
| SPSC referenced 4 KiB | 2,565, 2,357, 2,323, 2,447, 2,458, 2,616, 2,444, 2,256, 2,246, 2,369 | 1,523, 1,657, 1,682, 1,596, 1,589, 1,493, 1,598, 1,732, 1,739, 1,649 |
| MPSC inline 16 B | 133.9, 122.1, 137.4, 136.3, 133.2, 133.4, 129.0, 133.3, 130.9, 129.9 | 114.0, 125.0, 111.1, 112.0, 114.5, 114.3, 118.3, 114.5, 116.5, 117.4 |
| MPSC inline 256 B | 232.3, 257.2, 237.7, 231.6, 235.3, 234.7, 248.5, 236.5, 233.8, 234.7 | 1,051, 949.4, 1,027, 1,054, 1,038, 1,040, 982.4, 1,032, 1,044, 1,040 |
| MPSC inline 4 KiB | 3,383, 3,332, 3,723, 3,426, 3,454, 3,366, 3,386, 3,356, 3,834, 3,511 | 1,155, 1,172, 1,049, 1,140, 1,131, 1,160, 1,154, 1,164, 1,019, 1,112 |
| MPSC referenced 4 KiB | 3,372, 3,334, 3,394, 3,735, 3,412, 3,456, 3,392, 3,582, 3,608, 3,779 | 1,158, 1,171, 1,151, 1,046, 1,145, 1,130, 1,152, 1,090, 1,083, 1,034 |
| Buffered channel MPSC | 111.5, 91.67, 92.72, 96.43, 92.28, 124.3, 122.9, 89.82, 97.90, 108.5 | — |
| Raw single-producer publish | 12.48, 13.25, 12.66, 11.84, 13.08, 12.38, 12.18, 13.01, 12.51, 12.27 | — |
| Legacy SPSC | 23.06, 21.58, 23.56, 21.65, 21.88, 23.69, 22.98, 22.22, 23.94, 23.77 | — |
| Buffered channel SPSC | 70.76, 76.65, 73.00, 70.70, 68.45, 70.68, 69.77, 80.57, 69.64, 70.90 | — |
| Producer wait, yielding | 1,981, 2,034, 2,214, 2,142, 2,050, 2,083, 2,027, 2,050, 2,234, 2,109 | — |
| Producer wait, blocking | 35,491, 36,193, 35,554, 34,302, 34,952, 34,020, 37,056, 35,567, 36,248, 35,991 | — |
| Producer wait, busy-spin | 618.7, 631.4, 626.3, 695.3, 621.5, 630.4, 611.8, 676.2, 621.3, 611.3 | — |

</details>

### End-to-end throughput

The runner was built once with `go build -o /tmp/lib-disruptor-loadtest
./cmd/loadtest`. Every run used `mode=throughput`, `repetitions=5`, ring size
65,536, maximum batch 256, yielding producer and consumer waits, a 60-second
per-repetition timeout, and environment label `local-powersave`. The differing
flags and all five source-event samples are below. Broadcast-2 and pipeline-2
perform two deliveries per source event.

| Scenario | Events / warmup | Working set | M events/s, runs 1–5 | payload MiB/s, runs 1–5 |
|---|---:|---:|---|---|
| SPSC | 50M / 1M | 0 | 51.446, 49.505, 50.897, 53.407, 53.067 | — |
| MPSC-4 | 20M / 1M | 0 | 3.621, 1.852, 4.111, 1.836, 7.469 | — |
| broadcast-2 | 50M / 1M | 0 | 52.931, 49.565, 49.124, 49.038, 50.957 | — |
| pipeline-2 | 50M / 1M | 0 | 57.314, 58.436, 59.085, 56.330, 50.375 | — |
| SPSC, 16 B | 50M / 1M | 1 MiB | 36.815, 34.873, 35.265, 36.295, 24.916 | 561.759, 532.117, 538.102, 553.811, 380.195 |
| SPSC, 256 B | 10M / 500K | 16 MiB | 2.869, 2.189, 1.785, 1.772, 1.760 | 700.559, 534.343, 435.738, 432.732, 429.782 |
| SPSC, 4 KiB | 1M / 100K | 256 MiB | 0.173, 0.116, 0.115, 0.116, 0.116 | 675.551, 453.178, 448.415, 451.449, 452.220 |
| MPSC-4, 16 B | 20M / 1M | 1 MiB | 1.497, 7.883, 7.616, 6.831, 1.320 | 22.842, 120.287, 116.216, 104.232, 20.143 |
| MPSC-4, 256 B | 10M / 500K | 16 MiB | 0.778, 0.649, 0.643, 0.655, 0.650 | 189.965, 158.541, 157.064, 159.825, 158.599 |
| MPSC-4, 4 KiB | 1M / 100K | 256 MiB | 0.164, 0.136, 0.113, 0.105, 0.090 | 641.061, 532.874, 442.194, 409.237, 352.963 |

Load-run allocation fields are total runtime deltas and include runner
bookkeeping. Values below are `allocations/bytes` in run order.

| Scenario | Runs 1–5 |
|---|---|
| SPSC | 6/424, 6/424, 6/424, 8/1016, 6/424 |
| MPSC-4 | 39/23504, 15/2224, 15/2256, 12/784, 13/1264 |
| broadcast-2 | 8/1016, 8/1016, 8/1016, 6/424, 6/424 |
| pipeline-2 | 14/6336, 8/1016, 8/1016, 6/424, 6/424 |
| SPSC, 16 B | 6/424, 6/424, 7/536, 6/424, 8/1016 |
| SPSC, 256 B | 9/1272, 6/424, 6/424, 6/424, 8/1016 |
| SPSC, 4 KiB | 9/1272, 8/1016, 6/424, 7/904, 6/424 |
| MPSC-4, 16 B | 39/23504, 12/784, 15/2224, 16/2336, 12/784 |
| MPSC-4, 256 B | 20/6696, 15/1856, 15/2224, 16/2336, 16/2336 |
| MPSC-4, 4 KiB | 29/13456, 12/784, 12/784, 14/1376, 15/944 |

For exact invocation reconstruction, topology names use `topology=broadcast`
except pipeline-2; producer/consumer counts follow the scenario name; and
`payload-size` is 0, 16, 256, or 4096 as shown. An initial one-million-event
no-payload pilot was discarded because its 20–46 ms repetitions were too short.

### Sampled end-to-end latency

Latency was measured separately with zero-byte events and exactly 10,000 samples
per repetition. SPSC used 50M events, 1M warmup, and `sample-every=5000`;
MPSC-4 used 20M events, 1M warmup, and `sample-every=2000`. Other flags matched
the throughput runs.

| Scenario/run | M events/s | p50 | p95 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|---:|
| SPSC/1 | 38.769 | 839 ns | 2,765 ns | 866,199 ns | 1,562,394 ns | 1,646,958 ns |
| SPSC/2 | 39.119 | 894 ns | 870,738 ns | 1,427,781 ns | 1,655,182 ns | 1,713,358 ns |
| SPSC/3 | 38.885 | 893 ns | 795,808 ns | 1,409,616 ns | 1,682,117 ns | 1,756,894 ns |
| SPSC/4 | 35.718 | 881 ns | 482,786 ns | 1,387,864 ns | 2,692,352 ns | 4,649,806 ns |
| SPSC/5 | 26.758 | 855 ns | 727,667 ns | 1,778,454 ns | 4,367,807 ns | 5,014,913 ns |
| MPSC-4/1 | 11.052 | 230 ns | 903 ns | 1,833 ns | 21,677 ns | 55,346 ns |
| MPSC-4/2 | 1.997 | 1,118 ns | 63,849,108 ns | 78,413,051 ns | 98,252,683 ns | 100,542,236 ns |
| MPSC-4/3 | 1.868 | 53,406,003 ns | 62,365,675 ns | 68,899,925 ns | 99,080,219 ns | 100,653,915 ns |
| MPSC-4/4 | 7.613 | 350 ns | 1,067 ns | 2,133 ns | 116,389 ns | 397,713 ns |
| MPSC-4/5 | 1.578 | 54,573,953 ns | 63,108,031 ns | 71,138,079 ns | 93,358,175 ns | 94,953,957 ns |

Latency allocation/byte deltas were SPSC: `8/1160, 6/424, 6/424, 6/424,
6/424`; and MPSC-4: `15/2224, 39/23504, 14/1744, 12/784, 12/784`.

The bimodal MPSC throughput and 50–100 ms latency stalls are scheduler-sensitive
behavior on this shared desktop host. They are retained as measured and make
this host unsuitable for a latency regression gate.

### Process resource evidence

GNU `time -v` wrapped all five repetitions of each process. CPU is aggregate
process utilization; RSS and context switches cover setup and warmup too.

| Scenario | CPU | Max RSS | Voluntary / involuntary context switches |
|---|---:|---:|---:|
| SPSC | 211% | 13,528 KiB | 197,532 / 174 |
| MPSC-4 | 544% | 15,160 KiB | 2,675,937 / 196,041 |
| broadcast-2 | 310% | 12,884 KiB | 185,196 / 250 |
| pipeline-2 | 322% | 12,836 KiB | 333,852 / 210 |
| SPSC, 16 B | 219% | 15,540 KiB | 523,466 / 197 |
| SPSC, 256 B | 235% | 42,016 KiB | 1,547,736 / 455 |
| SPSC, 4 KiB | 245% | 365,436 KiB | 3,052,694 / 925 |
| MPSC-4, 16 B | 546% | 14,396 KiB | 2,161,219 / 160,813 |
| MPSC-4, 256 B | 576% | 42,196 KiB | 5,397,535 / 431,978 |
| MPSC-4, 4 KiB | 536% | 508,064 KiB | 4,045,731 / 260,855 |
| SPSC latency | 216% | 13,012 KiB | 430,248 / 460 |
| MPSC-4 latency | 549% | 15,028 KiB | 2,101,837 / 131,154 |

## Historical initial baseline — 2026-09-11

These are local development numbers, not portable performance guarantees.

The results below predate load-report schema version 1. Their
`Published events/s` values include the final consumer drain and therefore
correspond to the new `end_to_end_events_per_second` field, not the separately
measured publication rate.

### Environment

- CPU: Intel Core i7-10510U, 4 cores / 8 threads
- CPU governor: `powersave`
- OS: Linux 7.0.0-30-generic x86_64
- Go: 1.26.2
- GOMAXPROCS: 8

### Microbenchmarks

Command:

```bash
go test -run='^$' -bench=. -benchmem -benchtime=500ms -count=3
```

| Scenario | Three samples | Allocations |
|---|---:|---:|
| Raw single-producer publish | 15.56, 15.65, 14.59 ns/op | 0 B/op, 0 allocs/op |
| Disruptor SPSC | 31.38, 35.46, 33.81 ns/op | 0 B/op, 0 allocs/op |
| Buffered channel SPSC | 93.13, 89.95, 90.16 ns/op | 0 B/op, 0 allocs/op |

Median SPSC throughput was approximately 29.6 million events/s for the ring and
11.1 million events/s for the channel baseline. The benchmark includes waiting
until the consumer has handled the last event.

### Million-event load runs

Common settings: ring size 65,536, maximum batch 256, and one latency sample per
1,024 published sequences.

| Topology | Published events/s | Deliveries/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| 1 producer → 1 consumer | 17.73M | 17.73M | 377 ns | 2.67 μs | 4.07 μs |
| 4 producers → 1 consumer | 3.77M | 3.77M | 760 ns | 1.76 μs | 9.57 μs |
| 1 producer → 2 consumers | 7.30M | 14.61M | 1.01 μs | 7.81 μs | 22.09 μs |

The load command includes sampled clock reads, JSON reporting, and load-tool
bookkeeping. Compare load runs only when their flags, machine state, Go version,
and CPU governor match. The `powersave` governor makes this baseline unsuitable
as a release threshold; use repeated runs on a fixed-frequency idle host for
regression gating.

### Producer capacity wait comparison

Measured on 2026-09-12 on the same environment above. The microbenchmark uses a
one-slot SPSC ring and makes the consumer yield before every gate advancement,
forcing every producer claim through the configured capacity-wait path.

Command:

```bash
go test -run='^$' -bench='BenchmarkProducerWaitUnderSlowGate' \
  -benchmem -benchtime=500ms -count=3
```

| Producer wait | Three samples | Allocations | CPU tradeoff |
|---|---:|---:|---|
| Busy-spin | 426.7, 345.9, 380.1 ns/op | 0 B/op, 0 allocs/op | Highest CPU use; lowest forced-handoff time |
| Yielding | 1.542, 1.468, 1.517 μs/op | 0 B/op, 0 allocs/op | Scheduler-friendly default |
| Blocking | 36.16, 37.15, 37.18 μs/op | 112 B/op, 1 alloc/op | Lowest waiting CPU; scheduler/channel wake cost |

A sequential load-tool comparison used 500,000 events, one producer, one
consumer, ring size 64, batch size 1, and one latency sample per 64 events:

```bash
go run ./cmd/loadtest -events=500000 -producers=1 -consumers=1 \
  -ring-size=64 -batch-size=1 -sample-every=64 \
  -producer-wait=<yielding|blocking|busy-spin>
```

| Producer wait | Published/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Yielding | 18.67M | 2.42 μs | 3.43 μs | 4.03 μs |
| Blocking | 6.54M | 4.73 μs | 10.03 μs | 15.90 μs |
| Busy-spin | 12.66M | 4.77 μs | 5.40 μs | 6.50 μs |

These local powersave-mode samples demonstrate tradeoffs, not universal
rankings. Blocking deliberately exchanges throughput and allocation cost for
sleeping during sustained backpressure; batching gate updates amortizes its
wake cost. Busy-spin consumes a core and can compete with the consumer on a
shared host, while yielding performed best in this particular load topology.
