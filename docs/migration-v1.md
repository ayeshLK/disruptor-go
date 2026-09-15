# Pre-v1 migration guide

This guide records the intentional compatibility changes between the early
pre-v1 releases and the API that will be frozen for `v1.0.0`. Minor releases
before v1 may still change, but documenting the changes here makes upgrades
reviewable.

## From v0.1.0 and earlier

### Ring construction options

`New` accepts variadic `RingOption` values after the wait strategy. Existing
ordinary calls remain valid:

```go
ring, err := disruptor.New(size, disruptor.MultiProducer, factory, wait)
```

Code that assigns `New` to a function value or wraps its exact function type
must include the variadic options parameter in that type.

### Producer capacity waiting

Producer capacity waiting is configured independently from consumer waiting:

```go
ring, err := disruptor.New(
    size,
    disruptor.MultiProducer,
    factory,
    disruptor.BlockingWait(),
    disruptor.WithProducerWait(disruptor.ProducerWaitBlocking),
)
```

`ProducerWaitYielding` remains the default. `TryNext`, `TryNextN`, and
`TryPublish` remain immediately non-blocking regardless of the configured
policy.

### Lifecycle and shutdown

Use `Close` for immediate termination. Use `Shutdown` after publisher
operations have returned when terminal consumers must drain the final claimed
sequence:

```go
if err := ring.Shutdown(ctx); err != nil {
    return err
}
```

`Shutdown` rejects future claims, waits for registered terminal gates, and
closes consumer waits on every return path. Stop and join publisher goroutines
before calling it. A context error means the drain was interrupted; inspect
processor results separately for handler failures.

### Processor failures and restart

`BatchProcessor.Run` is the supervision path. Handler errors are returned as
`*HandlerError`; recovered panics are returned as `*HandlerPanicError`. The
failed batch is not acknowledged and will be replayed after a restart. Make
handlers idempotent and use `errors.As` or `errors.Is` when classifying failures.

`Halt` wakes a processor and lets the current selected batch finish. A closed
ring is terminal and subsequent runs return `ErrClosed`.

### Event ownership

Events are factory-allocated once and reused on every ring lap. Do not retain or
mutate an event after its consumer advances. Downstream pipeline handlers may
observe upstream mutations, so synchronize any data that must outlive the
consumer sequence separately.

`Sequence`, `RingBuffer`, `SequenceBarrier`, `BatchProcessor`, and shared wait
strategies must not be copied after first use. Pass them by pointer where the
API provides a pointer type.

## From v0.3.0

### Batch translation and publication

`PublishN` and `TryPublishN` provide the ergonomic equivalent of manually
combining `NextN`/`TryNextN`, repeated translation, and `PublishRange`:

```go
err := ring.PublishN(ctx, 4, func(event *OrderEvent, sequence int64) error {
	event.OrderID = nextOrderID(sequence)
	return nil
})
```

They pass each logical sequence to the existing `EventTranslator` contract.
Translation stops at the first error, but the entire claimed range is still
published; a panic also resolves the range before propagating. Code that needs
to avoid publishing partially initialized events should use raw claims and
explicitly publish or discard each resolved range instead.

### Raw claim abandonment

Raw claims that cannot be initialized must be resolved explicitly:

```go
sequence, err := ring.TryNext()
if err != nil {
    return err
}
if err := initialize(sequence); err != nil {
    ring.DiscardSequence(sequence)
    return err
}
ring.PublishSequence(sequence)
```

Use `DiscardRange` for a contiguous abandoned range. Discarded claims are
resolved in publication order and skipped by processors. `IsDiscarded` is
provided for protocol-aware consumers and tests; normal handlers do not need
to call it.

### Sealed wait strategies

`WaitStrategy` is intentionally not externally implementable. Use the four
built-in constructors. This restriction is part of the v1 compatibility
contract and avoids making internal wait-state behavior an extension API.

### Advanced sequencer interface

`Sequencer` remains exported for advanced integrations, but its method set is a
compatibility commitment. New applications should use `RingBuffer`; code that
implements `Sequencer` must track interface changes as part of each pre-v1
upgrade.

## Before v1.0.0

Before adopting v1, review the committed exported API snapshot and the
compatibility contract:

- [`API_COMPATIBILITY.md`](../API_COMPATIBILITY.md)
- [`api_public.txt`](../api_public.txt)
- [`docs/usage.md`](usage.md)

The v1 release will freeze the symbols, signatures, interface methods, and
exported fields represented by that snapshot. Intentional changes after the
freeze will require a deprecation or major-version plan.
