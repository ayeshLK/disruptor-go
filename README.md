# disruptor-go

[![Go Reference](https://pkg.go.dev/badge/github.com/ayeshLK/disruptor-go.svg)](https://pkg.go.dev/github.com/ayeshLK/disruptor-go)
[![Release](https://img.shields.io/github/v/release/ayeshLK/disruptor-go)](https://github.com/ayeshLK/disruptor-go/releases/latest)
[![CI](https://github.com/ayeshLK/disruptor-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ayeshLK/disruptor-go/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ayeshLK/disruptor-go/branch/main/graph/badge.svg)](https://codecov.io/gh/ayeshLK/disruptor-go)
[![License](https://img.shields.io/github/license/ayeshLK/disruptor-go)](LICENSE)

A generic, standard-library-only Go implementation of the core LMAX Disruptor
protocol: reusable preallocated events, monotonic sequences, bounded
backpressure, consumer barriers, publication-gap detection, and batch handling.

This is an independent Go implementation. It does not bind to the Java library,
wrap a channel, use cgo, or use `unsafe`. Version `0.1.0` requires Go 1.25 or
newer and may change before a stable release.

## Install

Install the current release explicitly so builds remain reproducible:

```sh
go get github.com/ayeshLK/disruptor-go@v0.1.0
```

Import the root package as `disruptor`:

```go
import disruptor "github.com/ayeshLK/disruptor-go"
```

Because this is a pre-v1 module, review the [changelog](CHANGELOG.md) before
upgrading to a new minor version.

## Quick start

```go
ring, err := disruptor.New(
    1024,
    disruptor.SingleProducer,
    func() *OrderEvent { return new(OrderEvent) },
    disruptor.BlockingWait(),
)
if err != nil {
    return err
}

processor, err := disruptor.NewBatchProcessor(
    ring,
    ring.NewBarrier(),
    func(event *OrderEvent, sequence int64, endOfBatch bool) error {
        log.Printf("sequence=%d order=%d", sequence, event.OrderID)
        return nil
    },
)
if err != nil {
    return err
}

ring.AddGatingSequences(processor.Sequence())
go processor.Run(context.Background())

return ring.Publish(context.Background(), func(event *OrderEvent, _ int64) error {
    event.OrderID = 42
    return nil
})
```

See [`examples/basic`](examples/basic) for the complete lifecycle.

## Lower-level claims

```go
high, err := ring.NextN(ctx, 4)
if err != nil {
    return err
}
low := high - 3
for sequence := low; sequence <= high; sequence++ {
    ring.Get(sequence).OrderID = nextOrderID()
}
ring.PublishRange(low, high)
```

`TryNext`, `TryNextN`, and `TryPublish` return immediately with
`ErrInsufficientCapacity` instead of waiting for consumers.

## Producer modes

- `SingleProducer` avoids atomic claims. Exactly one goroutine may claim or
  publish for the lifetime of the ring.
- `MultiProducer` uses CAS claims and per-slot lap flags. Consumers stop at the
  first unpublished gap even if later sequences have been published.

Use `MultiProducer` whenever publisher calls can overlap.

## Consumer graphs

Parallel broadcast:

```go
a, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerA)
b, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerB)
ring.AddGatingSequences(a.Sequence(), b.Sequence())
```

Pipeline `A → B`:

```go
a, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerA)
b, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(a.Sequence()), handlerB)
ring.AddGatingSequences(b.Sequence())
```

Only terminal consumers should gate reuse. B cannot pass A, so gating on B
protects both pipeline stages.

## Wait strategies

| Strategy | Behavior | Typical use |
|---|---|---|
| `BlockingWait()` | Sleep until publication | Default/shared hosts |
| `SleepingWait()` | Spin, yield, then sleep | Balanced CPU and latency |
| `YieldingWait()` | Yield while waiting | Busy systems with spare cores |
| `BusySpinWait()` | Continuously check | Dedicated cores, lowest jitter |

Wait strategies govern consumers. Producers waiting for capacity yield to the
Go scheduler.

## Ownership and safety

- Events are created once by `EventFactory` and reused.
- A producer owns a claimed event until publication.
- Consumers may access an event only while handling its sequence.
- Never retain an event after the consumer sequence advances.
- Downstream handlers may observe mutations made by upstream handlers.
- Handler errors leave the current batch unacknowledged. Restarting that
  processor replays the batch, so restartable handlers should be idempotent.
- Build and test applications with the race detector.

Go atomic publication and observation establish the visibility order. Go
atomics are sequentially consistent.

## Validation

```bash
go test ./...
go test -race ./...
go vet ./...
go run ./examples/basic
```

Microbenchmarks include an equivalent buffered-channel SPSC baseline:

```bash
go test -run='^$' -bench=. -benchmem ./...
```

Run concurrent load with sampled latency:

```bash
go run ./cmd/loadtest \
  -events=1000000 \
  -producers=1 \
  -consumers=1 \
  -ring-size=65536 \
  -batch-size=256
```

The load command prints JSON with publication and delivery throughput plus
sampled p50/p95/p99 end-to-end latency.

## Design notes

- Sequences start at `-1`.
- Physical index is `uint64(sequence) & uint64(bufferSize-1)`.
- A multi-producer availability flag is the sequence's lap number.
- Gating lists use copy-on-write atomic snapshots.
- `Sequence` pads `atomic.Int64` without architecture-specific `unsafe` logic.
- Assemble topology before publishing. A newly added runtime gate starts at the
  current claim cursor and does not replay older events.

Deferred from v0.1: fluent topology DSL, worker pools, replaying dynamic graph
changes, CPU affinity, `unsafe` padding, persistence, and cross-process delivery.

## Project policies

See [CONTRIBUTING.md](CONTRIBUTING.md) for validation and release conventions,
[SECURITY.md](SECURITY.md) for private vulnerability reporting, and
[BENCHMARKS.md](BENCHMARKS.md) for dated local performance measurements.

## License

Copyright 2026 Ayesh Almeida. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE).
