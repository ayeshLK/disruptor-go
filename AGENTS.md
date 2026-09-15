# AGENTS.md

## Project overview

This repository is a generic, standard-library-only Go implementation of the
core LMAX Disruptor protocol. It is intentionally independent of the Java
library: do not add Java interop, cgo, `unsafe`, or a channel-backed substitute
for the ring protocol unless the project direction explicitly changes.

The public package is `github.com/ayeshLK/lib-disruptor` (`package disruptor`).
The current compatibility target is Go 1.25 or newer. Keep the module free of
third-party dependencies unless a dependency is clearly justified and approved.

## Repository map

- `ring_buffer.go`: generic preallocated storage and producer-facing API.
- `sequence.go`: padded atomic sequence and minimum-sequence helpers.
- `sequencer.go`: shared sequencer contract, gates, close, and capacity waiting.
- `sequencer_single.go`: single-producer claim/publish path.
- `sequencer_multi.go`: CAS claims and per-slot publication availability.
- `producer_wait.go`: configurable producer capacity-wait policies and wakeups.
- `barrier.go`: consumer dependency barriers and gap-aware visibility.
- `wait_strategy.go`: blocking, sleeping, yielding, and busy-spin waits.
- `processor.go`: ordered batch consumer lifecycle and acknowledgement.
- `processor_error.go`: sequence-aware handler error and panic contracts.
- `errors.go`: exported sentinel errors.
- `examples/basic`: minimal end-to-end usage.
- `cmd/loadtest`: configurable concurrent throughput/latency runner.
- `benchmark_test.go`: microbenchmarks and buffered-channel baseline.
- `benchmark_matrix_test.go`: canonical topology, wait, batching, and payload matrix.
- `PERFORMANCE.md`: measurement model, canonical matrix, and regression policy.
- `BENCHMARKS.md`: dated, machine-specific baseline results.
- `API_COMPATIBILITY.md`: v1 public-surface and compatibility contract.
- `api_public.txt`: generated exported API baseline checked by CI.
- `cmd/apicheck`: dependency-free exported API snapshot checker.
- `docs/migration-v1.md`: pre-v1 migration guidance.
- `docs/production.md`: production topology and operational guide.
- `poller.go`: non-blocking application-loop consumer.
- `benchmark_poller_test.go`: poller processing and idle benchmarks.

## Current handoff state (2026-09-15)

- `v0.4.0` is the latest published release. Its immutable tag points to
  `81e38d8f13b4a05f62cd28ae9ee686f0a9e2de2b`.
- PR #30 (`feat: establish v1 API compatibility contract`) is merged at
  `56c9032fd512e98d4affa801aaba82900fb05fc3`. It established the exported API
  baseline, compatibility contract, migration guide, and v1-capable release
  validation.
- PR #31 (`feat: add batch publication helpers`) is merged at
  `b43c90ca350414c4a03ddf599cc959d48f18e37a`. It closes issue #9 and adds
  `PublishN` and `TryPublishN`.
- PR #32 (`docs: add production usage guide`) is merged at
  `7ad9f998afface508b487ac3465c3aac36c8b283` and closes issue #16. PR #33
  recorded the focused benchmark refresh at
  `190c0dbdfa0388c38557cb5cf06c670805e98551`.
- No P1 issues remain open. The remaining open P2 issues are #10 (pull-based
  event poller), #17 (v1 release validation), and #18 (release-readiness
  tracker).
- Issue #10 is implemented on this branch at `3521cf6`; PR #35 is open with
  the poller API, benchmark entry, and synchronized handoff context. These
  changes are not part of `main` until the PR is merged.
- `BENCHMARKS.md` contains focused batch-publication and poller API refreshes
  dated 2026-09-15. The poller entry is measured from `3521cf6`. A full
  canonical run exceeded the local ten-minute limit before completion and is
  intentionally not recorded.
- Before starting new work, fetch `origin/main`; do not assume this branch or
  the local remote-tracking ref includes a newly merged PR.

## Correctness invariants

Treat these as design constraints, not implementation details:

- Logical sequences start at `InitialSequence` (`-1`) and increase monotonically.
- Ring size must remain a power of two; physical slots use a mask, not modulo.
- Entries are allocated once by `EventFactory` and reused on every ring lap.
- Claiming reserves a sequence but must not make its event visible to consumers.
- Publication is the visibility boundary for producer writes.
- `SingleProducer` permits exactly one claiming/publishing goroutine for the
  lifetime of the ring. Do not add atomics to this hot path without evidence.
- `MultiProducer` claims with CAS. Its per-slot lap flags distinguish a current
  publication from stale data left by a previous wrap.
- A multi-producer barrier must stop at the first unpublished sequence even when
  later claimed sequences have already been published.
- Producers may not wrap over the minimum gating sequence. Only terminal
  consumers should gate a pipeline; parallel broadcast consumers all gate.
- Blocking producer waits snapshot their wake channel before rechecking the
  minimum gate, preventing lost wakeups. Gate advancement and removal must wake
  blocked producers; yielding remains the default policy.
- A dependent barrier cannot advance beyond its slowest upstream sequence.
- A processor advances its sequence only after the whole selected batch succeeds.
  Handler failure or panic leaves that batch unacknowledged and therefore
  replayable; `Run` is the sole error-reporting path.
- `Publish` and `TryPublish` publish their claimed sequence even if the translator
  returns an error. Preserve this behavior so a failed translator cannot leave a
  permanent publication gap.
- `PublishN` and `TryPublishN` validate before claiming, stop translation at the
  first error, and publish the complete claimed range on error or panic. Events
  after a partial translation must therefore be safe for consumers to observe.
- `EventPoller.Poll` never waits. It reports idle or gating state without
  advancing its sequence, and advances only after the selected batch succeeds;
  handler failure or panic leaves the batch replayable. Register its sequence as
  a gate when the poller owns ring capacity.
- Events must not be retained or mutated after a consumer advances its sequence.
- Context cancellation, `Halt`, barrier alerts, and `Close` must unblock waiters.
  Close is immediate rather than draining: visible events remain readable, while
  waits for unavailable or dependency-gated events return `ErrClosed`.
- `Shutdown` may begin only after publisher operations return. It seals future
  claims, snapshots the current gating sequences, waits for their minimum to
  reach the final claim boundary, and closes consumer waits on every return path.
  A context error means the drain was interrupted, not that the ring stayed open.
- Types containing atomics must not be copied after first use; use pointers.

When changing concurrency code, reason explicitly about claim order, publish
order, wraparound, memory visibility, cancellation, and reuse under a slow gate.
Prefer Go's typed atomics and documented synchronization semantics.

## API and implementation conventions

- Preserve idiomatic Go naming, exported documentation, and sentinel errors that
  callers can inspect with `errors.Is`.
- Prefer pointer event types (for example, `*OrderEvent`) when translators and
  handlers need in-place mutation.
- Accept `context.Context` on operations that can block; non-blocking variants
  should return `ErrInsufficientCapacity` immediately.
- Keep topology assembly explicit. A newly added gate starts at the current claim
  cursor and does not replay older events.
- Wait strategies must be safe when shared by concurrent barriers.
- Keep hot paths allocation-free where practical. Avoid interfaces, closures,
  timers, logging, and hidden heap escapes inside per-event loops unless measured.
- Use `gofmt`; keep files focused and tests in package `disruptor` when they need
  to validate internal protocol behavior.
- Every Go source file, including tests, examples, benchmarks, and commands, must
  begin with the Apache-2.0 header used by this repository: `Copyright 2026
  Ayesh Almeida`.
- Do not silently broaden the project scope into a topology DSL, worker pool,
  persistence, CPU affinity, cross-process transport, or architecture-specific
  padding.

## Validation

Run the smallest relevant test while iterating, then before handing off a change:

```bash
gofmt -w <changed-go-files>
go build ./...
go test -shuffle=on ./...
go test -race ./...
go vet ./...
```

Run one test with `go test . -run '^TestName$'` (or the relevant package path).
Run one benchmark with `go test . -run '^$' -bench '^BenchmarkName$' -benchmem`.
There is no separate linter; formatting, vet, tests, race detection, coverage,
and source-header checks are the quality gates. `go mod tidy` must not leave a
module-file diff, and the root package must maintain at least 85% coverage:

```bash
go test -covermode=atomic -coverprofile=coverage.out .
go tool cover -func=coverage.out
```

For concurrency-sensitive changes, repeat tests to expose timing failures:

```bash
go test -race -count=10 -timeout=60s ./...
```

Also run the example when changing public lifecycle or API behavior:

```bash
go run ./examples/basic
```

New behavior should include deterministic tests. In particular, cover both
producer modes where relevant, publication gaps, wrap/backpressure, processor
batch boundaries, pipeline visibility, cancellation/close, and wait alerts.
Avoid timing-only assertions; use bounded timeouts only to prevent a deadlock
from hanging the suite.

## Release workflow

Use the normal pull-request workflow; direct pushes require explicit maintainer
authorization. PR descriptions should state observable behavior, affected
concurrency invariants, linked issues, and validation performed.

Use Conventional Commit subjects for commits and squash-merge titles that reach
`main`: `fix:` selects a patch release, `feat:` selects a minor release,
and `type!:` or a `BREAKING CHANGE:` footer identifies an incompatible
change.

Release Please owns `CHANGELOG.md`; do not add a manual `Unreleased` section.
A maintainer manually runs the Prepare Release workflow to create or update a
reviewable version and changelog pull request. Preparation must fail while an
earlier merged release PR remains `autorelease: pending`.

Release workflows prefer `RELEASE_PLEASE_TOKEN` when configured and otherwise
use `github.token`; a separate release PAT is not required for the normal path.

After the release PR is reviewed and merged, a maintainer runs the Publish
Release workflow through the protected `release` environment. Publication
validates the prepared version, changelog, module graph, formatting, tests, race
behavior, coverage, and example before Release Please creates the immutable
root-module tag and GitHub Release. Do not manually create, move, reuse, or
delete release tags, or bypass this lifecycle with `gh release create`.

Keep third-party actions pinned to full commit SHAs and workflow permissions at
least privilege. Do not create repositories, tags, releases, commits, pushes, or
remote settings unless the user explicitly authorizes those external changes.

## Performance work

Use the existing benchmark and load tools rather than one-off programs:

```bash
go test -run='^$' -bench=. -benchmem -benchtime=1s -count=10
go run ./cmd/loadtest -mode=throughput -events=1000000 -warmup-events=100000 \
  -repetitions=5 -producers=1 -consumers=1 -ring-size=65536 -batch-size=256
```

For comparisons, keep Go version, `GOMAXPROCS`, machine load, CPU governor, ring
size, batching, topology, and sampling flags fixed. Report all samples and
allocations, not only the best result. Compare against the buffered-channel
baseline where appropriate. Do not present laptop or powersave-mode measurements
as portable guarantees or hard release thresholds.

Keep throughput and sampled-latency load runs separate. Payload cases must touch
all reported bytes, reuse factory-allocated storage, and report event size, ring
size, approximate working set, events/s, and payload MiB/s. Add every new unit,
abbreviation, percentile, and topology notation to the legend in
`BENCHMARKS.md`. Shared hosted-runner timings are informational; timing gates
require a controlled runner and a documented variance study.

If updating `BENCHMARKS.md`, record the date, CPU/OS, Go version, `GOMAXPROCS`,
exact commands and flags, repeated results, latency sampling rate, and notable
environment conditions. Separate microbenchmark numbers from load-tool numbers;
the latter include clock reads, JSON reporting, and bookkeeping.

## Change checklist

Before declaring work complete, confirm:

- The public behavior and any non-obvious concurrency reasoning are documented.
- Tests would fail if the new behavior or fixed invariant regressed.
- `go test ./...`, the race detector, and `go vet ./...` pass.
- Hot-path changes include allocation and performance evidence when relevant.
- README examples and `BENCHMARKS.md` remain accurate for user-visible changes.
- No generated output, local profiles, benchmark binaries, or editor artifacts
  were added to the repository.
