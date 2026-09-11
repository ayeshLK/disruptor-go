# Initial performance baseline

Measured on 2026-09-11. These are local development numbers, not portable
performance guarantees.

## Environment

- CPU: Intel Core i7-10510U, 4 cores / 8 threads
- CPU governor: `powersave`
- OS: Linux 7.0.0-30-generic x86_64
- Go: 1.26.2
- GOMAXPROCS: 8

## Microbenchmarks

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

## Million-event load runs

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
