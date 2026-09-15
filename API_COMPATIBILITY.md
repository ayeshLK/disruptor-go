# API compatibility contract

`lib-disruptor` is preparing for a `v1.0.0` compatibility promise. Until that
release, minor versions may contain breaking changes. The exported API snapshot
in [`api_public.txt`](api_public.txt) is the reviewable baseline for the current
pre-v1 surface.

## Compatibility policy

Once v1 is established:

- Existing exported names, signatures, constants, sentinel errors, interface
  methods, and exported struct fields remain source-compatible across minor
  and patch releases.
- Behavioral changes that affect the documented lifecycle, publication,
  ownership, cancellation, or error contracts require a compatibility review.
- Deprecated API remains available for at least one minor release unless a
  security or correctness issue requires an exception.
- The module continues to target Go 1.25 or newer and has no runtime
  dependencies outside the standard library.
- Unexported implementation details are not compatibility commitments.

The snapshot check runs in CI through `cmd/apicheck`. An intentional public API
change must update the snapshot in the same reviewed change and explain the
compatibility impact in the migration documentation or release notes.

## Public API review

The current exported surface is intentionally grouped as follows. Names and
signatures are captured in `api_public.txt`; this table records their intended
long-term role.

| Surface | Decision |
|---|---|
| `New`, `NewSequence`, `NewBatchProcessor` | Supported constructors. |
| `RingBuffer` claims, batch publication, discard, topology, and lifecycle methods | Supported application-facing API. |
| `Sequence` and `SequenceBarrier` | Supported synchronization and dependency API; pointer ownership is required. |
| `BatchProcessor`, `EventPoller`, `PollState`, `EventHandler`, `EventTranslator`, and processor options | Supported consumer API. |
| `ProducerType`, producer-wait modes, ring options, and sentinel errors | Supported configuration and error vocabulary. |
| Built-in wait-strategy types and constructors | Supported implementations; constructors are preferred over direct value construction. |
| `Sequencer` | Retained as an advanced compatibility surface; see the decision below. |
| `HandlerError` and `HandlerPanicError` | Supported sequence-aware supervision errors. |

Names are intentionally descriptive and use standard Go initialisms and
constructor conventions. No exported symbol is currently marked deprecated.

### `Sequencer`

`Sequencer` remains an advanced exported interface. Its method set is part of
the compatibility surface, but most applications should use `RingBuffer`
instead of implementing or calling a sequencer directly. Changes to this
interface require an explicit compatibility decision because external
implementations are possible.

### `WaitStrategy`

`WaitStrategy` is intentionally sealed by unexported methods. Applications
must use `BlockingWait`, `SleepingWait`, `YieldingWait`, or `BusySpinWait`;
custom implementations are not a supported extension point. The built-in
strategies must remain safe for concurrent use by multiple barriers.

### Ownership and non-copyable state

The following values contain synchronization state or represent shared
protocol state. They must be passed by pointer and must not be copied after
first use:

- `RingBuffer`
- `Sequence`
- `SequenceBarrier`
- `BatchProcessor`
- Built-in wait-strategy values after they are shared by barriers

Events returned by a ring are reused. A consumer must not retain an event or
mutate it after advancing its consumer sequence. A `SingleProducer` ring must
be claimed and published by exactly one goroutine for its lifetime.

## Contract summary

- Logical sequences begin at `InitialSequence` (`-1`) and increase
  monotonically.
- Claiming reserves a sequence; publication makes producer writes visible.
- Multi-producer consumers stop at the first unpublished sequence, even when
  later claims are already published.
- `Publish` and `TryPublish` publish their claim even when translation returns
  an error, preventing a permanent publication gap.
- `PublishN` and `TryPublishN` translate contiguous batches using each
  event's logical sequence; a translation error or panic still resolves the
  complete claimed range.
- `EventPoller` is a non-blocking, application-loop consumer. It must preserve
  barrier gaps, dependency ordering, batch replay, and gating semantics.
- `DiscardSequence` and `DiscardRange` resolve raw claims that will not be
  initialized; processors skip those claims while preserving sequence order.
- `Close` stops waits immediately. `Shutdown` rejects new claims, drains the
  registered terminal gates to the final claim, and closes consumer waits.
- Context cancellation, alerts, `Halt`, and close unblock operations that can
  wait.
- A failed or panicking processor batch remains unacknowledged and is
  replayable after restart. Handlers should therefore be idempotent.
- `errors.Is` and `errors.As` can be used with exported sentinel and handler
  errors.

## Updating the baseline

The baseline is generated and checked with:

```bash
go run ./cmd/apicheck > api_public.txt
go run ./cmd/apicheck -check api_public.txt
```

Do not update the snapshot merely to make CI pass. Review the API difference,
update the migration guidance when needed, and record the intentional change in
the release notes.
