# Production usage and topology guide

`lib-disruptor` is an in-process bounded protocol. It gives the application
explicit control over producer ownership, consumer dependencies, backpressure,
and shutdown. Build the topology before publishing and treat the sequences as
part of the ownership contract.

For API details and upgrade rules, see the [usage guide](usage.md), the [API
compatibility contract](../API_COMPATIBILITY.md), and the [pre-v1 migration
guide](migration-v1.md).

## Start with a topology

There are two common consumer graphs:

- **Broadcast:** every consumer receives every event. Create an independent
  barrier for each consumer and gate every terminal consumer.
- **Pipeline:** each stage observes the mutations made by the previous stage.
  Create a dependent barrier from the upstream sequence and gate only the final
  stage.

Assemble barriers and gates before the first publication. A gate added later
starts at the current producer cursor and does not replay earlier events.

### Broadcast

```go
first, err := disruptor.NewBatchProcessor(
    ring,
    ring.NewBarrier(),
    handleFirst,
)
if err != nil {
    return err
}
second, err := disruptor.NewBatchProcessor(
    ring,
    ring.NewBarrier(),
    handleSecond,
)
if err != nil {
    return err
}
ring.AddGatingSequences(first.Sequence(), second.Sequence())
```

Start both processors before publishing. The producer cannot reuse a slot until
both consumers have acknowledged it.

### Pipeline

```go
first, err := disruptor.NewBatchProcessor(
    ring,
    ring.NewBarrier(),
    enrich,
)
if err != nil {
    return err
}
second, err := disruptor.NewBatchProcessor(
    ring,
    ring.NewBarrier(first.Sequence()),
    persist,
)
if err != nil {
    return err
}
ring.AddGatingSequences(second.Sequence())
```

Only the terminal stage gates the ring. Gating an upstream stage as well is
usually unnecessary and can reduce capacity; omitting the terminal gate allows
producers to overwrite events before the pipeline finishes.

## Size the ring for the slowest path

The ring size must be a positive power of two. Select it from the largest
expected number of in-flight events, not only the average throughput:

- include the largest producer batch and bursts from every concurrent producer;
- include the lag of the slowest terminal consumer;
- include enough headroom for scheduler pauses, downstream I/O, and recovery;
- account for the reusable event size when estimating memory usage.

A larger ring absorbs bursts but does not fix a permanently slow consumer. The
slowest registered gating sequence remains the backpressure boundary. Measure
with the same topology, payload size, batch size, wait policies, and
`GOMAXPROCS` used in production.

## Choose producer and wait policies

Use `SingleProducer` only when one goroutine owns every claim and publication
operation for the ring's lifetime. It has the lowest claim overhead, but calls
from multiple goroutines are not supported. Use `MultiProducer` when publisher
calls can overlap; its CAS protocol preserves ordered consumer visibility across
publication gaps.

Consumer and producer waits are independent:

| Workload | Consumer wait | Producer wait |
|---|---|---|
| Shared host, CPU is scarce | `BlockingWait()` | `ProducerWaitBlocking` |
| General service workload | `SleepingWait()` or `YieldingWait()` | `ProducerWaitYielding` |
| Dedicated low-latency cores | `BusySpinWait()` | `ProducerWaitBusySpin` |

Start with yielding or blocking policies and measure before reserving dedicated
cores. `TryNext`, `TryNextN`, `TryPublish`, and `TryPublishN` never wait for
capacity regardless of the configured producer policy.

## Publish batches deliberately

Use `Publish` for one event and `PublishN` for a contiguous batch when the
translator can initialize events directly:

```go
err := ring.PublishN(ctx, 32, func(event *OrderEvent, sequence int64) error {
    event.Sequence = sequence
    event.Timestamp = time.Now()
    return nil
})
```

`TryPublishN` is the immediate-capacity equivalent. A nil translator is rejected
before claiming, and a non-positive count returns `ErrInvalidClaimSize`.
Translation stops on its first returned error, but the complete claimed range is
still published to prevent a permanent visibility gap. A panic also resolves
the range before propagating. Therefore, events after a partial failure must be
safe for consumers to observe; use factory defaults that are valid, or use the
raw claim APIs and explicit resolution when that behavior is not acceptable.

For a manual batch, every successful `NextN` or `TryNextN` claim must be
resolved with `PublishRange`, `PublishSequence`, `DiscardRange`, or
`DiscardSequence`, including cancellation, errors, and panic paths. Leaving a
multi-producer claim unpublished stops dependent consumers at that gap.

## Respect event ownership

The factory allocates one event per physical slot. A producer owns the event
from claim until publication or discard. A consumer may read it while handling
its sequence, but must not retain its address or mutate it after advancing its
consumer sequence. The same event will be reused after the ring wraps.

Use pointer event types when translators and handlers need in-place mutation.
For data that must outlive the consumer, copy the required fields into an
application-owned value before returning from the handler. Run the race
detector against the complete topology; it catches retained-event and
cross-stage ownership mistakes that unit tests may miss.

## Handle failures and restart safely

Treat `BatchProcessor.Run` as the supervision boundary:

- handler errors return as `*HandlerError` with the failing sequence;
- recovered panics return as `*HandlerPanicError`;
- the failed batch is not acknowledged and is replayable after restart;
- handlers should be idempotent or use an application-level deduplication key;
- inspect processor results separately from a `Shutdown` context error.

`WithMaxBatchSize` bounds the number of available events selected for one
handler run. A handler should finish within the memory and latency budget of
the ring; batch size is not a substitute for backpressure monitoring.

## Integrate with an application event loop

Use `EventPoller` when the application already owns an event loop and should
pull available ring events without dedicating a goroutine to `Run`:

```go
poller, err := disruptor.NewEventPoller(
    ring,
    ring.NewBarrier(),
    handleEvent,
    disruptor.WithMaxBatchSize(64),
)
if err != nil {
    return err
}
ring.AddGatingSequences(poller.Sequence())

for {
    state, err := poller.Poll()
    if err != nil {
        return err
    }
    switch state {
    case disruptor.PollIdle:
        pollOtherSources()
    case disruptor.PollGating:
        pollOtherSources()
    case disruptor.PollProcessing:
        continue
    }
}
```

`Poll` is non-blocking. `PollIdle` means the producer has not advanced;
`PollGating` means a publication gap or upstream dependency is preventing the
next sequence from becoming consumable. The poller must be called by one loop,
and its sequence should be registered as a gate. Handler failures leave the
selected batch replayable, just like `BatchProcessor`.

## Shut down and cancel predictably

Stop and join all publisher goroutines before calling `Shutdown`:

```go
publishers.Wait()
if err := ring.Shutdown(ctx); err != nil {
    return err
}
for _, result := range processorResults {
    if err := <-result; err != nil && !errors.Is(err, disruptor.ErrClosed) {
        return err
    }
}
```

`Shutdown` rejects new claims, snapshots the terminal gates, waits for them to
reach the final claimed sequence, and closes consumer waits on every return
path. A context error means the drain was interrupted; it does not mean that
the ring remained open.

Use `Close` for immediate termination when draining is not required. It
unblocks waits for unavailable or dependency-gated events with `ErrClosed`; visible
events remain readable. Context cancellation, processor `Halt`, barrier alerts,
and close are all intended to unblock waiting operations.

A processor can be restarted after cancellation, halt, an alert, or a handler
failure. A closed ring is terminal. Do not start a second `Run` call while the
first one is active.

## Common mistakes

- Calling a `SingleProducer` ring from multiple goroutines.
- Publishing an event before its fields are initialized.
- Leaving a raw claim unresolved after an error or panic.
- Retaining or mutating a reused event after advancing a consumer sequence.
- Gating an upstream pipeline stage instead of the terminal stage.
- Forgetting to gate broadcast consumers independently.
- Calling `Shutdown` while publishers can still claim or publish.
- Treating a handler error as acknowledged work instead of replayable work.
- Using busy-spin waits without dedicated CPU capacity.
- Assuming a larger ring fixes a consumer that is permanently slower than the
  producers.

## Operational validation

Before production rollout, validate the actual topology with:

```bash
go test -race ./...
go vet ./...
go run ./examples/basic
```

Benchmark representative payload sizes and batches under controlled conditions.
Record ring size, topology, producer mode, wait policies, `GOMAXPROCS`, CPU
state, allocations, throughput, and sampled latency. The repository's
`PERFORMANCE.md` and `BENCHMARKS.md` describe the measurement model; local
results are evidence for a workload, not portable guarantees.

## Explicit non-goals

The v1 boundary remains a generic in-process ring protocol. It does not include
or imply:

- a worker pool;
- a fluent topology DSL;
- persistence or replay storage;
- CPU affinity;
- cross-process transport;
- cgo or `unsafe` code; or
- a channel-backed substitute for the ring protocol.
