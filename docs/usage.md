# Usage guide

This guide covers the protocol details that matter when building a production
pipeline with `lib-disruptor`. For the first runnable ring, start with the
[README](../README.md#quick-start). For production topology and operational
recipes, see the [production guide](production.md). For compatibility guarantees
and upgrade notes, see the [API compatibility contract](../API_COMPATIBILITY.md)
and [pre-v1 migration guide](migration-v1.md).

## Event lifetime and publication

An event is allocated once by the `EventFactory` and reused every time the ring
wraps. A producer owns an event after claiming its sequence and until it
publishes that sequence. Consumers may access an event only while handling its
sequence.

Do not retain an event or mutate it after the consumer advances its sequence.
Downstream pipeline handlers may observe mutations made by upstream handlers.
Build and test applications with the race detector.

Claiming reserves a sequence; it does not make writes visible. Publication is
the visibility boundary. With `MultiProducer`, consumers stop at the first
unpublished sequence even when producers have published later claims.

## Publishing events

`Publish` is the simplest way to claim, fill, and publish one event:

```go
err := ring.Publish(ctx, func(event *OrderEvent, sequence int64) error {
    event.OrderID = nextOrderID()
    return nil
})
```

`TryPublish` has the same translator contract but returns
`ErrInsufficientCapacity` immediately when it cannot claim an event.

`PublishN` and `TryPublishN` apply the same translator to a contiguous batch
and pass each event's logical sequence to it:

```go
err := ring.PublishN(ctx, 4, func(event *OrderEvent, sequence int64) error {
	event.OrderID = nextOrderID(sequence)
	return nil
})
```

`PublishN` waits for capacity; `TryPublishN` returns
`ErrInsufficientCapacity` immediately. A non-positive count returns
`ErrInvalidClaimSize`, and a nil translator returns `ErrNilTranslator` before
claiming. Translation stops at the first returned error, but the complete
claimed range is still published so a partial translation cannot leave a
publication gap. A panic also publishes the complete range before propagating;
callers that need recovery must recover around the helper call. The remaining
events in a partially translated range must therefore be safe for consumers to
observe, just as with a translator error in `Publish`.

For one producer filling a batch manually, claim a range and publish it after every
event has been initialized:

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

`TryNext` and `TryNextN` are the non-blocking counterparts. Every successful
raw claim must eventually be resolved with `PublishSequence`/`PublishRange` or
`DiscardSequence`/`DiscardRange`, including claims abandoned after cancellation,
application failure, or a panic. A discard acknowledges the logical sequence
without delivering its event to processors:

```go
high, err := ring.NextN(ctx, 4)
if err != nil {
    return err
}
low := high - 3
resolved := false
defer func() {
    if !resolved {
        ring.DiscardRange(low, high)
    }
}()
for sequence := low; sequence <= high; sequence++ {
    ring.Get(sequence).OrderID = nextOrderID()
}
ring.PublishRange(low, high)
resolved = true
```

`SequenceBarrier.WaitFor` advances across discarded sequences, while
`IsDiscarded` identifies holes that raw consumers must skip. A discarded range
is safe to wrap and does not expose stale slot data. `MultiProducer` claims may
be resolved in any order; `SingleProducer` claim and resolution calls remain
owned by one goroutine and should resolve in claim order. Duplicate resolution
calls are ignored, and the first publish or discard wins.

By design, `Publish` and `TryPublish` publish their claimed sequence even when
the translator returns an error, preventing a visibility gap without requiring a
separate discard.

## Producer capacity waits

Consumer wait strategies and producer capacity waiting are independent. The
default producer policy is `ProducerWaitYielding`:

```go
ring, err := disruptor.New(
    1024,
    disruptor.MultiProducer,
    factory,
    disruptor.BlockingWait(),
    disruptor.WithProducerWait(disruptor.ProducerWaitBlocking),
)
```

| Mode | Capacity behavior | Typical use |
|---|---|---|
| `ProducerWaitYielding` | Yields to the Go scheduler | General-purpose default |
| `ProducerWaitBlocking` | Waits until a gate advances | Shared hosts and sustained backpressure |
| `ProducerWaitBusySpin` | Continuously checks capacity | Dedicated cores and lowest handoff latency |

`TryNext`, `TryNextN`, and `TryPublish` remain non-blocking regardless of this
configuration.

## Consumer graphs

### Broadcast

Every consumer receives each event. Each terminal consumer must gate reuse:

```go
a, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerA)
b, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerB)
ring.AddGatingSequences(a.Sequence(), b.Sequence())
```

### Pipeline

The second stage cannot advance beyond the first. Gate only the terminal stage;
that protects both consumers from reuse:

```go
a, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(), handlerA)
b, _ := disruptor.NewBatchProcessor(ring, ring.NewBarrier(a.Sequence()), handlerB)
ring.AddGatingSequences(b.Sequence())
```

Assemble the graph before publishing. A gate added at runtime starts at the
current claim cursor and does not replay older events.

## Batch processing and failures

`BatchProcessor` invokes a handler in sequence order and advances its sequence
only after the selected batch succeeds. Use `WithMaxBatchSize` to bound the
largest batch selected for a handler run.

Treat the error returned by `BatchProcessor.Run` as the supervision path. A
handler error is returned as `HandlerError`, and a recovered handler panic is
returned as `HandlerPanicError`; both retain the failing sequence and support
`errors.Is` and `errors.As` when applicable.

A failed or panicked batch is left unacknowledged and will be replayed after a
processor restart. Make restartable handlers idempotent. `Halt` wakes a running
processor and permits its current selected batch to finish. A processor can be
restarted after halt, cancellation, alert, handler failure, or recovered panic;
a closed ring is terminal and subsequent runs return `ErrClosed`.

## Closing a ring

Stop and join publisher goroutines before calling `Shutdown`:

```go
if err := ring.Shutdown(ctx); err != nil {
    return err
}
```

`Shutdown` rejects future claims, snapshots the terminal gates registered at
that point, waits for them to reach the final claimed sequence, then closes
consumer waits. It always closes the ring before returning. A context error
means the drain was interrupted; inspect processor results separately for
handler failures.

Use `Close` when consumers should stop immediately rather than drain. Visible
events remain readable, while waits for unavailable or dependency-gated events
return `ErrClosed`. Context cancellation, `Halt`, barrier alerts, and close all
unblock waiters.

## Protocol notes

- Logical sequences start at `InitialSequence` (`-1`) and increase monotonically.
- The ring size must be a positive power of two; slots use a mask rather than modulo.
- A multi-producer availability flag tracks a sequence's ring lap, distinguishing current publication from stale wrapped data.
- Gating lists use copy-on-write atomic snapshots.
- `Sequence` pads `atomic.Int64` without architecture-specific `unsafe` logic.

### Sequence range

Logical sequences use signed `int64` arithmetic and must not cross the
`int64` boundary. The library does not support sequence wraparound at
`MaxInt64`; applications with an exceptionally long-lived ring must recreate it
before reaching that limit. Boundary tests cover representable values near the
limit, but overflow is an operational limit rather than a supported protocol
state.

For benchmarks and measurement guidance, see [PERFORMANCE.md](../PERFORMANCE.md).
