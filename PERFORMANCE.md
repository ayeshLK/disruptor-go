# Performance testing strategy

This document defines how lib-disruptor performance is measured. It separates
repeatable protocol microbenchmarks from end-to-end load measurements and from
regression decisions made on controlled hardware.

## Objectives

- Detect accidental allocations and meaningful hot-path regressions.
- Compare supported producer, topology, batching, and wait-policy choices.
- Measure payload sensitivity without confusing application copying with ring
  protocol overhead.
- Preserve raw samples and enough environment data to reproduce a result.
- Avoid treating shared CI runner timing as a portable performance guarantee.

## Canonical matrix

The complete microbenchmark matrix uses a small event so protocol costs remain
visible:

| Area | Scenarios |
|---|---|
| Claims and publication | blocking and non-blocking, single and multi producer, batches of 1/16/256 |
| Publication gaps | out-of-order multi-producer publication and contiguous visibility scan |
| Topologies | SPSC, MPSC with 2/4 producers, two-consumer broadcast, two-stage pipeline |
| Producer capacity waits | yielding, blocking, busy-spin under a slow gate |
| Consumer waits | blocking, sleeping, yielding, busy-spin |
| Baselines | equivalently buffered Go channels where delivery semantics match |

Payload sensitivity is a focused SPSC and MPSC sweep rather than a cross-product
with every topology. Inline payloads of 16 B, 256 B, and 4 KiB are touched in
full by both producer and consumer. A reusable referenced 4 KiB payload measures
external-memory access separately. Payload storage is allocated by the event
factory and reused for every ring lap.

Both event size and ring size must be reported because their product determines
the approximate ring working set. Payload benchmarks report events/s and
payload MiB/s; the latter is logical source payload throughput (events multiplied
by payload size), not aggregate producer/consumer memory traffic or bytes copied
internally by the ring.

## Measurement layers

### Microbenchmarks

Use Go benchmarks for isolated costs and allocation reporting. Run all samples
sequentially on an otherwise idle host:

```bash
go test -run='^$' -bench=. -benchmem -benchtime=1s -count=10
```

Keep the raw output. Comparisons must use the same Go version, benchmark flags,
`GOMAXPROCS`, CPU governor, and host. Optional comparison tools such as
`benchstat` are developer tooling and must not be added to the library module.

### Load runs

The load runner executes one scenario at a time. Throughput mode disables clock
sampling; latency mode enables explicitly sampled end-to-end observations.
Warmup events are excluded from reported durations and runtime allocation/GC
deltas. Each repetition constructs a fresh ring and processors.

Example:

```bash
go run ./cmd/loadtest -mode=throughput -topology=broadcast \
  -events=1000000 -warmup-events=100000 -repetitions=5 \
  -producers=1 -consumers=2 -ring-size=65536 -batch-size=256
```

Latency runs should collect at least 10,000 samples when tail percentiles are
reported. Sampling changes the workload, so throughput and latency runs are not
directly comparable.

### Report semantics

- `publish_seconds` ends when every publisher returns; producer backpressure is
  included.
- `drain_seconds` is the remaining time for terminal consumers to acknowledge
  the final measured event. `end_to_end_seconds` includes both phases.
- `deliveries_per_second` counts every broadcast consumer or pipeline stage;
  `end_to_end_events_per_second` counts each source event once.
- Latency starts immediately before payload initialization and ends in every
  broadcast consumer or only the terminal pipeline stage. Percentiles use the
  nearest-rank method.
- Allocation and GC fields are runtime deltas for the measured phase and include
  load-runner bookkeeping. Stable per-operation allocation expectations belong
  in the microbenchmarks and allocation tests.

### Regression decisions

Pull-request CI enforces correctness and stable allocation expectations. It may
publish timing results for inspection, but shared hosted-runner timings do not
block a change. Timing gates require a controlled runner and a variance study.
A regression must be both statistically credible across repeated samples and
large enough to matter operationally; thresholds are selected only after the
runner's normal variance has been recorded.

Release evidence includes the raw benchmark output, raw load JSON, environment
record, commit, commands, and a human-readable summary in `BENCHMARKS.md`.

The manual `Performance evidence` workflow packages those raw files plus GNU
`time -v` process CPU, memory, and context-switch statistics. Because it runs on
a shared GitHub-hosted machine, its timing remains informational.

## Environment record

Record the date, commit, dirty state, Go version, OS/architecture, CPU model,
logical CPU count, `GOMAXPROCS`, CPU governor, power/virtualization constraints,
and exact flags. Run busy-spin cases only when the producer and consumers have
enough CPU capacity; CPU contention can reverse their apparent ranking.

## Notation

The shared notation legend is maintained in `BENCHMARKS.md`. New abbreviations,
units, percentile labels, or throughput terms introduced by a result must be
added to that legend in the same change.
