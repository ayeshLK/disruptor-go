# lib-disruptor

<p align="center">
  <a href="https://pkg.go.dev/github.com/ayeshLK/lib-disruptor"><img src="https://pkg.go.dev/badge/github.com/ayeshLK/lib-disruptor.svg" alt="Go Reference"></a>
  <a href="https://github.com/ayeshLK/lib-disruptor/releases/latest"><img src="https://img.shields.io/github/v/release/ayeshLK/lib-disruptor" alt="Release"></a>
  <a href="https://github.com/ayeshLK/lib-disruptor/actions/workflows/ci.yml"><img src="https://github.com/ayeshLK/lib-disruptor/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/ayeshLK/lib-disruptor"><img src="https://codecov.io/gh/ayeshLK/lib-disruptor/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/ayeshLK/lib-disruptor" alt="Apache-2.0 license"></a>
</p>

> A generic, dependency-free Go implementation of the core LMAX Disruptor
> protocol for bounded, ordered event pipelines.

`lib-disruptor` uses a reusable, preallocated ring buffer to coordinate
producers and ordered batch consumers. It gives applications explicit
backpressure and topology control without cgo, `unsafe`, Java bindings, or a
channel-backed substitute.

> [!IMPORTANT]
> `lib-disruptor` is a pre-v1 module. Minor releases may include breaking
> changes; review the [changelog](CHANGELOG.md) and
> [pre-v1 migration guide](docs/migration-v1.md) before upgrading. The planned
> v1 compatibility contract is documented in
> [API_COMPATIBILITY.md](API_COMPATIBILITY.md).

## Why use it?

- Reuse factory-allocated events instead of allocating on every handoff.
- Choose single-producer or CAS-based multi-producer publication.
- Build broadcast and staged consumer pipelines with explicit gating.
- Integrate consumers into an existing event loop with a pull-based poller.
- Select consumer and producer waiting policies for your latency and CPU budget.

This library is for in-process, performance-sensitive event processing. It is
not a distributed queue, persistence layer, topology DSL, or general
replacement for Go channels.

## Install

Requires Go 1.25 or newer.

```sh
go get github.com/ayeshLK/lib-disruptor@v0.3.0
```

Import the root package as `disruptor`:

```go
import disruptor "github.com/ayeshLK/lib-disruptor"
```

## Quick start

The basic lifecycle is: create a ring, attach and gate a processor, publish
events, then drain and close the ring.

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

See [`examples/basic`](examples/basic) for a complete, runnable example with
orderly shutdown and processor-result handling. For production topology,
sizing, ownership, and operational guidance, read the
[production guide](docs/production.md).

## Resolve raw claims

The `Next` and `TryNext` APIs transfer ownership of a claimed event to the
caller. Resolve every successful claim with `PublishSequence`/`PublishRange` or
`DiscardSequence`/`DiscardRange`, including failure and panic paths. Discarded
sequences advance barriers and gating without invoking consumer handlers; use
`IsDiscarded` when reading a raw barrier range. See the
[usage guide](docs/usage.md#publishing-events) for a safe deferred-resolution
pattern.

## Choose your setup

### Producer mode

| Mode | Use when | Notes |
|---|---|---|
| `SingleProducer` | One goroutine will claim and publish for the ring's lifetime | Lowest claim overhead; do not call publishing APIs from another goroutine. |
| `MultiProducer` | Publisher calls may overlap | Uses CAS claims and preserves ordered consumer visibility across publication gaps. |

### Consumer wait strategy

| Strategy | Best fit |
|---|---|
| `BlockingWait()` | Shared hosts and low CPU use |
| `SleepingWait()` | A balance of CPU use and latency |
| `YieldingWait()` | Busy systems with spare cores |
| `BusySpinWait()` | Dedicated cores and lowest jitter |

The consumer wait strategy is passed to `New`. Producer capacity waiting is
configured separately with `WithProducerWait`; see the
[usage guide](docs/usage.md#producer-capacity-waits).

## Model your consumer graph

| Topology | Barrier and gate arrangement |
|---|---|
| Broadcast | `producer -> A` and `producer -> B`; gate both `A` and `B`. |
| Pipeline | `producer -> A -> B`; create `B`'s barrier from `A.Sequence()` and gate only terminal consumer `B`. |

Gating sequences prevent a producer from overwriting an event before the
terminal consumers have acknowledged it. Assemble a topology before publishing;
a gate added later starts at the current claim cursor and does not replay old
events.

## Next steps

- Read the [usage guide](docs/usage.md) for batch claims, producer capacity
  waits, graph setup, shutdown, error handling, and event-ownership rules.
- Read the [production guide](docs/production.md) for sizing, topology recipes,
  lifecycle operations, and common mistakes.
- Use `EventPoller` when an application-controlled loop should pull available
  events without dedicating a consumer goroutine.
- Browse the [API reference](https://pkg.go.dev/github.com/ayeshLK/lib-disruptor)
  for the complete public contract.
- See [PERFORMANCE.md](PERFORMANCE.md) for the measurement model and
  [BENCHMARKS.md](BENCHMARKS.md) for dated, machine-specific results.

## Contributing and security

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) for the
development and validation workflow, [SECURITY.md](SECURITY.md) for private
vulnerability reporting, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for
community expectations.

## License

Copyright 2026 Ayesh Almeida. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE).
