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

## Claim-abandonment benchmark refresh — 2026-09-15

Measured from clean commit `83f8811`. This is a microbenchmark-only refresh
covering the claim-abandonment implementation and its optimized normal paths.
These are local development results, not portable guarantees or release
thresholds. No load-test throughput or sampled-latency runs were performed for
this entry.

### Environment and command

- Time: `2026-09-15` (wall-clock start time was not recorded)
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- OS: Linux 7.0.0-31-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`

```bash
go test -run='^$' -bench=. -benchmem -benchtime=1s -count=10
```

The tables summarize all ten sequential samples as minimum / median / maximum.
All cases measured 0 B/op and 0 allocs/op except the blocking waits shown.

### Microbenchmarks

| Benchmark family | ns/op min / median / max | Allocations |
|---|---:|---:|
| Buffered channel broadcast-2 | 91.02 / 91.60 / 101.8 | 0 / 0 |
| Buffered channel pipeline-2 | 88.95 / 91.94 / 113.5 | 0 / 0 |
| Try claim/publish, single, batch 1 | 10.37 / 11.63 / 12.61 | 0 / 0 |
| Try claim/publish, single, batch 16 | 1.386 / 1.502 / 1.801 | 0 / 0 |
| Try claim/publish, single, batch 256 | 0.8297 / 0.8573 / 0.9488 | 0 / 0 |
| Try claim/publish, multi, batch 1 | 30.26 / 32.72 / 35.15 | 0 / 0 |
| Try claim/publish, multi, batch 16 | 16.24 / 16.93 / 20.78 | 0 / 0 |
| Try claim/publish, multi, batch 256 | 15.21 / 15.48 / 15.96 | 0 / 0 |
| Publication gap, multi-producer scan | 58.00 / 58.89 / 61.98 | 0 / 0 |
| Try claim/discard, single | 22.99 / 23.39 / 24.78 | 0 / 0 |
| Try claim/discard, multi | 33.76 / 34.41 / 34.69 | 0 / 0 |
| Claim/publish, single, batch 1 | 14.00 / 14.43 / 15.61 | 0 / 0 |
| Claim/publish, single, batch 16 | 1.762 / 1.825 / 1.995 | 0 / 0 |
| Claim/publish, single, batch 256 | 1.007 / 1.014 / 1.031 | 0 / 0 |
| Claim/publish, multi, batch 1 | 34.93 / 35.86 / 38.38 | 0 / 0 |
| Claim/publish, multi, batch 16 | 16.12 / 16.29 / 16.44 | 0 / 0 |
| Claim/publish, multi, batch 256 | 14.57 / 15.30 / 22.50 | 0 / 0 |
| Topology, SPSC | 24.17 / 27.61 / 37.41 | 0 / 0 |
| Topology, MPSC-2 | 95.56 / 105.15 / 119.7 | 0 / 0 |
| Topology, MPSC-4 | 105.0 / 110.8 / 117.8 | 0 / 0 |
| Topology, broadcast-2 | 30.73 / 35.54 / 64.55 | 0 / 0 |
| Topology, pipeline-2 | 31.08 / 32.87 / 46.18 | 0 / 0 |
| Consumer wait, blocking | 125.0 / 182.2 / 283.7 | 112 B/op / 1 alloc/op |
| Consumer wait, sleeping | 24.84 / 30.70 / 33.20 | 0 / 0 |
| Consumer wait, yielding | 24.93 / 27.86 / 33.81 | 0 / 0 |
| Consumer wait, busy-spin | 29.12 / 33.25 / 34.13 | 0 / 0 |
| Buffered channel MPSC | 72.36 / 73.62 / 78.95 | 0 / 0 |
| Raw publish | 10.03 / 10.25 / 10.95 | 0 / 0 |
| SPSC | 17.77 / 18.32 / 19.23 | 0 / 0 |
| Buffered channel SPSC | 55.43 / 57.75 / 61.87 | 0 / 0 |
| Producer wait, yielding | 1,518 / 1,604 / 1,698 | 0 / 0 |
| Producer wait, blocking | 32,812 / 34,105 / 34,729 | 112 B/op / 1 alloc/op |
| Producer wait, busy-spin | 481.2 / 498.3 / 510.7 | 0 / 0 |

### Payload sensitivity

| Scenario | ns/op min / median / max | payload MiB/s min / median / max | Allocations |
|---|---:|---:|---:|
| SPSC inline 16 B | 35.49 / 36.63 / 43.41 | 351.5 / 409.6 / 430.0 | 0 / 0 |
| SPSC inline 256 B | 147.1 / 159.8 / 219.2 | 1,114 / 1,527.5 / 1,660 | 0 / 0 |
| SPSC inline 4 KiB | 1,824 / 2,359.5 / 2,927 | 1,335 / 1,663 / 2,142 | 0 / 0 |
| SPSC referenced 4 KiB | 1,985 / 2,104 / 2,855 | 1,368 / 1,858 / 1,968 | 0 / 0 |
| MPSC inline 16 B | 101.4 / 106.25 / 129.1 | 118.2 / 143.6 / 150.5 | 0 / 0 |
| MPSC inline 256 B | 188.6 / 194.75 / 205.0 | 1,191 / 1,253.5 / 1,294 | 0 / 0 |
| MPSC inline 4 KiB | 2,731 / 2,824.5 / 2,939 | 1,329 / 1,383 / 1,431 | 0 / 0 |
| MPSC referenced 4 KiB | 2,781 / 2,822.5 / 2,869 | 1,361 / 1,384 / 1,405 | 0 / 0 |

## Batch publication API refresh — 2026-09-15

Measured from clean commit `7ad9f998afface508b487ac3465c3aac36c8b283`, which
includes the merged `PublishN` and `TryPublishN` API. This is a focused
microbenchmark entry for the new helpers and their manual-range comparison; it
is not a replacement for the canonical matrix. A separate full canonical run
was attempted but exceeded the local ten-minute execution limit before
completion and is not recorded here. These are local development results, not
portable guarantees or release thresholds.

### Environment and command

- Time: `2026-09-15` (wall-clock start time was not recorded)
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- OS: Linux 7.0.0-31-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`

```bash
go test -run='^$' -bench='^(BenchmarkBatchPublishHelpers|BenchmarkManualTryPublishRange)$' \\
  -benchmem -benchtime=1s -count=10
```

Values are minimum / median / maximum across ten sequential samples. Every case
reported 0 B/op and 0 allocs/op.

| Benchmark | ns/op min / median / max | Allocations |
|---|---:|---:|
| Batch helper, single, blocking, batch 1 | 16.5 / 17.305 / 18.83 | 0 / 0 |
| Batch helper, single, try, batch 1 | 14.73 / 15.15 / 16.72 | 0 / 0 |
| Batch helper, single, blocking, batch 16 | 38.8 / 42.8 / 45.93 | 0 / 0 |
| Batch helper, single, try, batch 16 | 41.61 / 41.91 / 49.5 | 0 / 0 |
| Batch helper, single, blocking, batch 256 | 500.9 / 629.65 / 697.7 | 0 / 0 |
| Batch helper, single, try, batch 256 | 561.9 / 592.5 / 641.9 | 0 / 0 |
| Batch helper, multi, blocking, batch 1 | 40.22 / 41.005 / 41.7 | 0 / 0 |
| Batch helper, multi, try, batch 1 | 38.28 / 38.655 / 42.06 | 0 / 0 |
| Batch helper, multi, blocking, batch 16 | 267 / 270.05 / 280.1 | 0 / 0 |
| Batch helper, multi, try, batch 16 | 261.8 / 266.15 / 319.1 | 0 / 0 |
| Batch helper, multi, blocking, batch 256 | 3,887 / 3,996 / 4,815 | 0 / 0 |
| Batch helper, multi, try, batch 256 | 3,960 / 4,037.5 / 5,199 | 0 / 0 |
| Manual range, single, try, batch 1 | 12.51 / 14.415 / 15.83 | 0 / 0 |
| Manual range, single, try, batch 16 | 28.52 / 29.52 / 37.24 | 0 / 0 |
| Manual range, single, try, batch 256 | 302.4 / 307.65 / 332.2 | 0 / 0 |
| Manual range, multi, try, batch 1 | 32.34 / 33.665 / 34.84 | 0 / 0 |
| Manual range, multi, try, batch 16 | 242.2 / 247 / 270.6 | 0 / 0 |
| Manual range, multi, try, batch 256 | 3,690 / 3,808 / 4,181 | 0 / 0 |

The helper path remains allocation-free. The helper includes translator callback
invocation and range-resolution work, so the manual comparison is a lower-level
baseline rather than an expected equality target. The wide single-producer
batch-256 distribution reinforces that these local measurements should not be
used as release thresholds.

## Pull-based poller API refresh — 2026-09-15

Measured from commit `3521cf6`, which adds the pull-based `EventPoller` API.
This is a focused microbenchmark entry; processing cases include a successful
`TryPublishN` followed by one poll, while the idle case measures polling with
no available event. It is not a replacement for the canonical matrix. These
are local development results, not portable guarantees or release thresholds.

### Environment and command

- Time: `2026-09-15` (wall-clock start time was not recorded)
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- OS: Linux 7.0.0-31-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`

```bash
go test -run='^$' -bench='^BenchmarkEventPoller' -benchmem \\
  -benchtime=1s -count=10
```

Values are minimum / median / maximum across ten sequential samples. Every case
reported 0 B/op and 0 allocs/op.

| Benchmark | ns/op min / median / max | Allocations |
|---|---:|---:|
| Poller, single, batch 1 | 31.46 / 32.11 / 33.09 | 0 / 0 |
| Poller, single, batch 16 | 148.3 / 151.35 / 159.3 | 0 / 0 |
| Poller, single, batch 256 | 1,903 / 1,962 / 2,080 | 0 / 0 |
| Poller, multi, batch 1 | 49.97 / 51.295 / 52.57 | 0 / 0 |
| Poller, multi, batch 16 | 358 / 368.95 / 418.1 | 0 / 0 |
| Poller, multi, batch 256 | 5,724 / 5,780 / 6,407 | 0 / 0 |
| Poller idle | 4.638 / 4.7405 / 4.899 | 0 / 0 |

The poller steady-state paths are allocation-free. Processing values include
producer work by design; the idle case isolates the no-event polling path.

## Post-merge poller API refresh — 2026-09-15

Measured from merged commit `6910ef1` (`feat: add pull-based event poller
(#35)`) on `origin/main`. This repeats the focused poller benchmark after the
API landed on the main branch; it does not replace either the earlier poller
entry or the canonical matrix. These are local development results, not
portable guarantees or release thresholds.

### Environment and command

- Time: `2026-09-15` (wall-clock start time was not recorded)
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- OS: Linux 7.0.0-31-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`

```bash
go test -run='^$' -bench='^BenchmarkEventPoller' -benchmem \\
  -benchtime=1s -count=10
```

Values are minimum / median / maximum across ten sequential samples. Every case
reported 0 B/op and 0 allocs/op.

| Benchmark | ns/op min / median / max | Allocations |
|---|---:|---:|
| Poller, single, batch 1 | 32.75 / 34.215 / 42.38 | 0 / 0 |
| Poller, single, batch 16 | 148.4 / 152.95 / 171.2 | 0 / 0 |
| Poller, single, batch 256 | 1,920 / 2,048.5 / 2,809 | 0 / 0 |
| Poller, multi, batch 1 | 51.49 / 57.23 / 61.01 | 0 / 0 |
| Poller, multi, batch 16 | 352.8 / 357.15 / 372.3 | 0 / 0 |
| Poller, multi, batch 256 | 5,186 / 5,211.5 / 5,943 | 0 / 0 |
| Poller idle | 4.179 / 4.288 / 4.386 | 0 / 0 |

The post-merge run confirms allocation-free processing and idle paths. The
wider single-producer batch-256 and multi-producer batch-1 ranges show ordinary
scheduler and host-load variance; neither run should be treated as a release
threshold.

## Full v1 refresh — 2026-09-12

Measured from clean commit `0baae43e00268197d5072101709d35311f9e5490`.
The changes since the previous full run are documentation-only, so this is an
environmental refresh rather than a code-performance comparison. These are
local development results, not portable guarantees or release thresholds. The
`powersave` governor and ordinary desktop activity make the wide MPSC
distributions especially important to retain.

### Environment

- Time: `2026-09-12T18:09:42+05:30` (microbenchmark start); load runs began at `2026-09-12T18:19:40+05:30`
- CPU: Intel Core i7-10510U, 4 cores / 8 logical CPUs
- Cache: 128 KiB L1d, 1 MiB L2, and 8 MiB L3 (aggregate `lscpu` values)
- OS: Linux 7.0.0-31-generic x86_64
- Go: 1.26.2 linux/amd64
- GOMAXPROCS: 8
- CPU governor: `powersave`
- Initial load average: 1.25, 0.84, 0.37
- Memory: 15 GiB total, 11 GiB available, no swap in use

### Microbenchmarks

```bash
go test -run='^$' -bench=. -benchmem -benchtime=1s -count=10
```

The table summarizes all ten sequential samples as minimum / median / maximum.
All cases measured 0 B/op and 0 allocs/op except the two blocking waits shown.
All samples appear below, while the raw output remains outside the repository.
Summary values alone must not be used for a statistical regression decision.

| Area | Scenario | ns/op min / median / max | Allocations |
|---|---|---:|---:|
| Claim/publish | Single, batch 1 | 13.40 / 13.77 / 13.98 | 0 / 0 |
| Claim/publish | Single, batch 16 | 1.700 / 1.773 / 1.823 | 0 / 0 |
| Claim/publish | Single, batch 256 | 0.9408 / 0.9616 / 1.030 | 0 / 0 |
| Claim/publish | Multi, batch 1 | 22.15 / 22.69 / 23.80 | 0 / 0 |
| Claim/publish | Multi, batch 16 | 8.386 / 8.493 / 8.637 | 0 / 0 |
| Claim/publish | Multi, batch 256 | 7.352 / 7.409 / 7.880 | 0 / 0 |
| Try claim/publish | Single, batch 1 | 10.39 / 11.08 / 11.77 | 0 / 0 |
| Try claim/publish | Single, batch 16 | 1.601 / 1.655 / 2.463 | 0 / 0 |
| Try claim/publish | Single, batch 256 | 0.9152 / 0.9377 / 0.9966 | 0 / 0 |
| Try claim/publish | Multi, batch 1 | 20.75 / 21.50 / 22.92 | 0 / 0 |
| Try claim/publish | Multi, batch 16 | 8.036 / 8.429 / 10.01 | 0 / 0 |
| Try claim/publish | Multi, batch 256 | 6.501 / 8.186 / 11.17 | 0 / 0 |
| Publication gap | Multi-producer scan | 37.54 / 40.41 / 47.39 | 0 / 0 |
| Topology | SPSC | 33.29 / 36.80 / 38.51 | 0 / 0 |
| Topology | MPSC-2 | 98.28 / 103.9 / 114.7 | 0 / 0 |
| Topology | MPSC-4 | 123.6 / 136.9 / 144.5 | 0 / 0 |
| Topology | broadcast-2 | 29.88 / 32.76 / 41.04 | 0 / 0 |
| Topology | pipeline-2 | 26.58 / 28.98 / 30.45 | 0 / 0 |
| Consumer wait | Blocking | 168.4 / 172.0 / 179.5 | 112 B/op / 1 alloc/op |
| Consumer wait | Sleeping | 43.07 / 46.26 / 48.18 | 0 / 0 |
| Consumer wait | Yielding | 31.86 / 35.86 / 38.13 | 0 / 0 |
| Consumer wait | Busy-spin | 41.52 / 44.99 / 48.66 | 0 / 0 |
| Producer wait | Blocking | 34,098 / 35,810 / 36,015 | 112 B/op / 1 alloc/op |
| Producer wait | Yielding | 2,181 / 2,286 / 2,294 | 0 / 0 |
| Producer wait | Busy-spin | 640.4 / 648.0 / 672.0 | 0 / 0 |
| Baseline | Buffered channel SPSC | 71.75 / 73.01 / 73.85 | 0 / 0 |
| Baseline | Buffered channel MPSC | 93.09 / 106.1 / 116.9 | 0 / 0 |
| Baseline | Buffered channel broadcast-2 | 97.48 / 100.6 / 105.6 | 0 / 0 |
| Baseline | Buffered channel pipeline-2 | 95.17 / 99.19 / 122.7 | 0 / 0 |
| Legacy | Raw publish | 12.48 / 13.10 / 14.21 | 0 / 0 |
| Legacy | SPSC | 21.80 / 22.42 / 24.97 | 0 / 0 |

Payload entries are preallocated for each of the payload benchmark's 1,024
slots and fully touched by the producer and consumer. The 4 KiB referenced
case measures external reusable storage separately from the inline case.

| Scenario | ns/op min / median / max | payload MiB/s min / median / max | Working set |
|---|---:|---:|---:|
| SPSC inline 16 B | 39.21 / 40.52 / 43.81 | 348.3 / 376.6 / 389.1 | 16 KiB |
| SPSC inline 256 B | 152.6 / 192.1 / 219.8 | 1,111 / 1,272 / 1,600 | 256 KiB |
| SPSC inline 4 KiB | 2,129 / 2,556 / 3,144 | 1,243 / 1,529 / 1,835 | 4 MiB |
| SPSC referenced 4 KiB | 2,315 / 2,419 / 2,715 | 1,439 / 1,616 / 1,687 | 4 MiB |
| MPSC inline 16 B | 120.5 / 132.8 / 136.9 | 111.5 / 115.0 / 126.6 | 16 KiB |
| MPSC inline 256 B | 254.6 / 260.2 / 384.3 | 635.3 / 938.4 / 959.0 | 256 KiB |
| MPSC inline 4 KiB | 3,200 / 3,241 / 3,831 | 1,020 / 1,206 / 1,221 | 4 MiB |
| MPSC referenced 4 KiB | 3,209 / 3,588 / 3,748 | 1,042 / 1,089 / 1,217 | 4 MiB |

<details>
<summary>All microbenchmark samples in execution order</summary>

| Scenario | Ten ns/op samples | Ten payload MiB/s samples |
|---|---|---|
| Buffered channel broadcast-2 | 105.6, 97.48, 98.67, 102.7, 100.4, 98.53, 100.8, 105.6, 102.3, 99.19 | — |
| Buffered channel pipeline-2 | 95.17, 98.15, 95.47, 98.94, 95.40, 99.44, 107.8, 119.1, 116.8, 122.7 | — |
| Try claim/publish, single, batch 1 | 10.74, 10.66, 11.16, 11.77, 10.39, 11.37, 10.50, 11.06, 11.30, 11.09 | — |
| Try claim/publish, single, batch 16 | 1.602, 1.721, 1.601, 1.621, 2.410, 2.463, 1.743, 1.651, 1.658, 1.647 | — |
| Try claim/publish, single, batch 256 | 0.9258, 0.9966, 0.9858, 0.9340, 0.9879, 0.9359, 0.9394, 0.9722, 0.9152, 0.9288 | — |
| Try claim/publish, multi, batch 1 | 22.45, 21.35, 22.92, 21.42, 21.20, 20.75, 21.58, 22.15, 21.20, 21.91 | — |
| Try claim/publish, multi, batch 16 | 9.479, 9.577, 10.01, 8.586, 8.643, 8.052, 8.153, 8.272, 8.222, 8.036 | — |
| Try claim/publish, multi, batch 256 | 7.067, 8.821, 11.17, 11.12, 8.237, 7.080, 6.501, 6.897, 9.285, 8.134 | — |
| Multi-producer publication gap scan | 45.88, 44.65, 43.31, 41.55, 47.39, 39.05, 38.99, 39.26, 38.06, 37.54 | — |
| Claim/publish, single, batch 1 | 13.75, 13.82, 13.52, 13.40, 13.55, 13.88, 13.83, 13.58, 13.78, 13.98 | — |
| Claim/publish, single, batch 16 | 1.700, 1.823, 1.754, 1.813, 1.773, 1.773, 1.778, 1.714, 1.776, 1.765 | — |
| Claim/publish, single, batch 256 | 0.9431, 0.9478, 0.9408, 0.9618, 0.9696, 0.9652, 0.9614, 0.9595, 1.030, 0.9649 | — |
| Claim/publish, multi, batch 1 | 23.07, 22.28, 23.16, 22.15, 22.84, 22.53, 22.31, 22.32, 23.80, 22.90 | — |
| Claim/publish, multi, batch 16 | 8.386, 8.560, 8.513, 8.637, 8.482, 8.599, 8.481, 8.498, 8.487, 8.455 | — |
| Claim/publish, multi, batch 256 | 7.411, 7.469, 7.384, 7.880, 7.403, 7.560, 7.436, 7.352, 7.355, 7.406 | — |
| Topology SPSC | 34.95, 33.29, 38.21, 36.92, 37.73, 34.16, 36.75, 38.51, 36.84, 33.62 | — |
| Topology MPSC-2 | 98.64, 105.6, 114.7, 102.2, 110.1, 101.5, 114.7, 99.62, 98.28, 110.3 | — |
| Topology MPSC-4 | 136.0, 123.6, 129.1, 127.6, 139.4, 138.5, 137.8, 133.6, 142.0, 144.5 | — |
| Topology broadcast-2 | 30.56, 29.88, 31.76, 30.24, 41.04, 33.37, 32.14, 33.60, 35.80, 34.54 | — |
| Topology pipeline-2 | 28.63, 29.74, 29.51, 28.41, 29.57, 28.16, 30.45, 29.33, 28.23, 26.58 | — |
| Consumer wait, blocking | 177.0, 175.2, 171.1, 169.6, 173.5, 169.8, 179.5, 169.6, 172.9, 168.4 | — |
| Consumer wait, sleeping | 44.77, 43.07, 46.93, 45.67, 46.87, 48.18, 46.74, 46.52, 43.16, 45.99 | — |
| Consumer wait, yielding | 35.54, 38.13, 33.36, 36.17, 36.18, 35.40, 35.12, 31.86, 37.25, 37.54 | — |
| Consumer wait, busy-spin | 45.54, 44.92, 46.29, 48.66, 41.52, 44.54, 45.22, 44.34, 43.41, 45.06 | — |
| SPSC inline 16 B | 40.25, 39.21, 41.57, 43.02, 42.18, 40.52, 40.28, 40.51, 43.81, 40.16 | 379.1, 389.1, 367.0, 354.7, 361.8, 376.6, 378.8, 376.6, 348.3, 380.0 |
| SPSC inline 256 B | 152.6, 189.5, 164.9, 201.4, 176.9, 210.4, 219.8, 194.7, 199.5, 179.7 | 1600, 1289, 1480, 1212, 1380, 1160, 1111, 1254, 1224, 1359 |
| SPSC inline 4 KiB | 2875, 2560, 2575, 2412, 2551, 2381, 2603, 3144, 2199, 2129 | 1359, 1526, 1517, 1619, 1531, 1640, 1501, 1243, 1776, 1835 |
| SPSC referenced 4 KiB | 2315, 2715, 2349, 2594, 2392, 2689, 2454, 2431, 2406, 2317 | 1687, 1439, 1663, 1506, 1633, 1453, 1592, 1607, 1624, 1686 |
| MPSC inline 16 B | 134.8, 136.5, 136.9, 131.5, 134.6, 124.5, 129.5, 134.1, 126.3, 120.5 | 113.2, 111.8, 111.5, 116.1, 113.4, 122.6, 117.8, 113.8, 120.8, 126.6 |
| MPSC inline 256 B | 254.6, 258.1, 256.5, 260.4, 258.5, 286.4, 346.9, 384.3, 259.9, 262.8 | 959.0, 945.8, 951.9, 937.6, 944.6, 852.4, 703.8, 635.3, 939.2, 929.0 |
| MPSC inline 4 KiB | 3831, 3606, 3200, 3238, 3251, 3214, 3224, 3244, 3277, 3224 | 1020, 1083, 1221, 1207, 1202, 1215, 1212, 1204, 1192, 1212 |
| MPSC referenced 4 KiB | 3222, 3209, 3243, 3406, 3748, 3717, 3694, 3535, 3641, 3683 | 1212, 1217, 1205, 1147, 1042, 1051, 1057, 1105, 1073, 1060 |
| Buffered channel MPSC | 102.7, 94.51, 93.09, 99.37, 115.9, 115.9, 116.9, 107.3, 112.4, 104.9 | — |
| Raw single-producer publish | 14.21, 12.95, 13.30, 13.60, 13.08, 13.40, 13.12, 12.86, 12.48, 12.56 | — |
| Legacy SPSC | 21.80, 22.42, 21.93, 24.72, 23.17, 24.97, 22.41, 21.99, 22.72, 22.14 | — |
| Buffered channel SPSC | 72.55, 71.77, 73.29, 72.72, 73.45, 71.75, 72.26, 73.30, 73.46, 73.85 | — |
| Producer wait, yielding | 2181, 2294, 2291, 2286, 2274, 2286, 2281, 2268, 2292, 2290 | — |
| Producer wait, blocking | 35626, 35900, 34098, 35968, 35867, 35632, 35568, 35960, 36015, 35753 | — |
| Producer wait, busy-spin | 642.7, 650.0, 649.6, 660.8, 666.3, 641.2, 672.0, 646.4, 642.0, 640.4 | — |

</details>

### End-to-end throughput

The runner was built once with `go build -o /tmp/lib-disruptor-perf-hDooxtvi/loadtest
./cmd/loadtest`. Every run used `mode=throughput`, `repetitions=5`, ring size
65,536, maximum batch 256, yielding producer and consumer waits, a 60-second
per-repetition timeout, and environment label `local-powersave`. The differing
flags and all five source-event samples are below. Broadcast-2 and pipeline-2
perform two deliveries per source event.

| Scenario | Events / warmup | Working set | M events/s, runs 1–5 | payload MiB/s, runs 1–5 |
|---|---:|---:|---|---|
| SPSC | 50M / 1M | 0 | 42.248, 40.778, 42.197, 45.853, 46.175 | — |
| MPSC-4 | 20M / 1M | 0 | 1.773, 1.104, 1.234, 5.246, 1.063 | — |
| broadcast-2 | 50M / 1M | 0 | 53.345, 52.790, 54.434, 41.911, 30.960 | — |
| pipeline-2 | 50M / 1M | 0 | 57.633, 56.365, 59.442, 42.278, 34.625 | — |
| SPSC, 16 B | 50M / 1M | 1 MiB | 34.356, 35.757, 28.174, 22.261, 22.863 | 524.226, 545.604, 429.901, 339.674, 348.859 |
| SPSC, 256 B | 10M / 500K | 16 MiB | 2.618, 1.333, 1.345, 1.369, 1.360 | 639.255, 325.452, 328.249, 334.217, 332.134 |
| SPSC, 4 KiB | 1M / 100K | 256 MiB | 0.144, 0.112, 0.114, 0.113, 0.113 | 562.759, 438.662, 443.913, 441.253, 440.776 |
| MPSC-4, 16 B | 20M / 1M | 1 MiB | 1.190, 0.935, 1.067, 1.010, 1.187 | 18.153, 14.265, 16.281, 15.406, 18.120 |
| MPSC-4, 256 B | 10M / 500K | 16 MiB | 0.696, 0.631, 0.678, 0.679, 0.663 | 169.840, 154.013, 165.493, 165.754, 161.922 |
| MPSC-4, 4 KiB | 1M / 100K | 256 MiB | 0.101, 0.086, 0.087, 0.088, 0.088 | 392.847, 336.612, 338.246, 345.250, 343.918 |

Load-run allocation fields are total runtime deltas and include runner
bookkeeping. Values below are `allocations/bytes` in run order.

| Scenario | Runs 1–5 |
|---|---|
| SPSC | 6/424, 6/424, 6/424, 6/424, 6/424 |
| MPSC-4 | 39/23504, 16/2336, 13/1264, 15/2224, 12/784 |
| broadcast-2 | 8/1016, 6/424, 8/1016, 8/1016, 6/424 |
| pipeline-2 | 14/6336, 6/424, 8/1016, 6/424, 6/424 |
| SPSC, 16 B | 9/1272, 6/424, 6/424, 6/424, 8/1016 |
| SPSC, 256 B | 9/1272, 6/424, 6/424, 8/1016, 6/424 |
| SPSC, 4 KiB | 9/1272, 6/424, 8/920, 8/1016, 6/424 |
| MPSC-4, 16 B | 40/23984, 14/1376, 15/2256, 13/1264, 15/1856 |
| MPSC-4, 256 B | 21/7544, 15/2224, 14/1376, 12/784, 12/784 |
| MPSC-4, 4 KiB | 21/7544, 16/2336, 12/784, 13/896, 15/2224 |

For exact invocation reconstruction, topology names use `topology=broadcast`
except pipeline-2; producer/consumer counts follow the scenario name; and
`payload-size` is 0, 16, 256, or 4096 as shown.

### Sampled end-to-end latency

Latency was measured separately with zero-byte events and exactly 10,000 samples
per repetition. SPSC used 50M events, 1M warmup, and `sample-every=5000`;
MPSC-4 used 20M events, 1M warmup, and `sample-every=2000`. Other flags matched
the throughput runs.

| Scenario/run | M events/s | p50 | p95 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|---:|
| SPSC/1 | 33.975 | 851 ns | 1,468 ns | 4,303 ns | 64,780 ns | 137,635 ns |
| SPSC/2 | 34.582 | 820 ns | 1,476 ns | 8,845 ns | 196,861 ns | 282,617 ns |
| SPSC/3 | 24.667 | 1,215 ns | 1,918 ns | 3,145 ns | 52,629 ns | 152,835 ns |
| SPSC/4 | 23.136 | 1,401 ns | 2,002 ns | 3,708 ns | 40,383 ns | 244,022 ns |
| SPSC/5 | 23.317 | 1,373 ns | 1,964 ns | 3,345 ns | 25,418 ns | 203,229 ns |
| MPSC-4/1 | 1.275 | 59,518,812 ns | 77,202,200 ns | 93,308,430 ns | 123,826,301 ns | 126,773,437 ns |
| MPSC-4/2 | 2.024 | 595 ns | 61,655,016 ns | 67,166,371 ns | 71,563,843 ns | 73,034,988 ns |
| MPSC-4/3 | 7.529 | 301 ns | 584 ns | 1,644 ns | 6,155 ns | 207,337 ns |
| MPSC-4/4 | 1.470 | 59,399,607 ns | 67,517,182 ns | 73,891,872 ns | 79,189,399 ns | 80,408,905 ns |
| MPSC-4/5 | 7.632 | 349 ns | 687 ns | 3,650 ns | 142,859 ns | 226,167 ns |

Latency allocation/byte deltas were SPSC: `6/424, 6/424, 6/424, 6/424,
6/424`; and MPSC-4: `39/23504, 15/2224, 13/1264, 13/1264, 15/2224`.

The bimodal MPSC throughput and 60–127 ms latency stalls are scheduler-sensitive
behavior on this shared desktop host. They are retained as measured and make
this host unsuitable for a latency regression gate.

### Process resource evidence

GNU `time -v` wrapped all five repetitions of each process. CPU is aggregate
process utilization; RSS and context switches cover setup and warmup too.

| Scenario | CPU | Max RSS | Voluntary / involuntary context switches |
|---|---:|---:|---:|
| SPSC | 214% | 13,876 KiB | 257,148 / 1,006 |
| MPSC-4 | 535% | 15,572 KiB | 3,897,953 / 323,583 |
| broadcast-2 | 305% | 12,500 KiB | 109,144 / 1,332 |
| pipeline-2 | 314% | 14,028 KiB | 226,309 / 1,182 |
| SPSC, 16 B | 220% | 14,804 KiB | 528,128 / 885 |
| SPSC, 256 B | 251% | 44,628 KiB | 2,678,184 / 4,227 |
| SPSC, 4 KiB | 247% | 381,240 KiB | 3,325,034 / 4,706 |
| MPSC-4, 16 B | 517% | 12,372 KiB | 4,646,968 / 550,931 |
| MPSC-4, 256 B | 580% | 43,476 KiB | 5,253,569 / 389,575 |
| MPSC-4, 4 KiB | 532% | 373,164 KiB | 3,718,544 / 267,111 |
| SPSC latency | 217% | 13,012 KiB | 484,310 / 1,158 |
| MPSC-4 latency | 538% | 15,316 KiB | 2,345,885 / 188,171 |

## Superseded full v1 baseline — 2026-09-12

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

Payload entries are preallocated for each of the payload benchmark's 1,024 slots and fully touched by the
producer and consumer. The 4 KiB referenced case measures external reusable
storage separately from the inline case.

| Scenario | ns/op min / median / max | payload MiB/s min / median / max | Working set |
|---|---:|---:|---:|
| SPSC inline 16 B | 37.84 / 39.92 / 42.22 | 361.4 / 382.4 / 403.2 | 16 KiB |
| SPSC inline 256 B | 155.0 / 165.1 / 178.4 | 1,369 / 1,479 / 1,576 | 256 KiB |
| SPSC inline 4 KiB | 2,152 / 2,298 / 2,722 | 1,435 / 1,700 / 1,815 | 4 MiB |
| SPSC referenced 4 KiB | 2,246 / 2,406 / 2,616 | 1,493 / 1,624 / 1,739 | 4 MiB |
| MPSC inline 16 B | 122.1 / 133.2 / 137.4 | 111.1 / 114.5 / 125.0 | 16 KiB |
| MPSC inline 256 B | 231.6 / 235.0 / 257.2 | 949.4 / 1,039 / 1,054 | 256 KiB |
| MPSC inline 4 KiB | 3,332 / 3,406 / 3,834 | 1,019 / 1,147 / 1,172 | 4 MiB |
| MPSC referenced 4 KiB | 3,334 / 3,434 / 3,779 | 1,034 / 1,138 / 1,171 | 4 MiB |

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
