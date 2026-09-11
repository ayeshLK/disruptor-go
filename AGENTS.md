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
- `processor_error.go`: sequence-aware handler error and panic contracts.
- `sequencer_multi.go`: CAS claims and per-slot publication availability.
- `barrier.go`: consumer dependency barriers and gap-aware visibility.
- `wait_strategy.go`: blocking, sleeping, yielding, and busy-spin waits.
- `processor.go`: ordered batch consumer lifecycle and acknowledgement.
- `errors.go`: exported sentinel errors.
- `examples/basic`: minimal end-to-end usage.
- `cmd/loadtest`: configurable concurrent throughput/latency runner.
- `benchmark_test.go`: microbenchmarks and buffered-channel baseline.
- `BENCHMARKS.md`: dated, machine-specific baseline results.

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
- A dependent barrier cannot advance beyond its slowest upstream sequence.
- A processor advances its sequence only after the whole selected batch succeeds.
  Handler failure or panic leaves that batch unacknowledged and therefore
  replayable; `Run` is the sole error-reporting path.
- `Publish` and `TryPublish` publish their claimed sequence even if the translator
  returns an error. Preserve this behavior so a failed translator cannot leave a
  permanent publication gap.
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
- Do not silently broaden v0.1 scope into a topology DSL, worker pool, persistence,
  CPU affinity, cross-process transport, or architecture-specific padding.

## Validation

Run the smallest relevant test while iterating, then before handing off a change:

```bash
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
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

Use Conventional Commit subjects for commits and squash-merge titles that reach
`main`: `fix:` selects a patch release, `feat:` selects a minor release,
and `type!:` or a `BREAKING CHANGE:` footer identifies an incompatible
change.

Release Please owns `CHANGELOG.md`; do not add a manual `Unreleased` section.
A maintainer manually runs the Prepare Release workflow to create or update a
reviewable version and changelog pull request. Preparation must fail while an
earlier merged release PR remains `autorelease: pending`.

After the release PR is reviewed and merged, a maintainer runs the Publish
Release workflow through the protected `release` environment. Publication
validates the prepared version, changelog, module graph, formatting, tests, race
behavior, coverage, and example before Release Please creates the immutable
root-module tag and GitHub Release. Do not manually create, move, reuse, or
delete release tags, or bypass this lifecycle with `gh release create`.

Keep third-party actions pinned to full commit SHAs and workflow permissions at
least privilege. Do not create repositories, tags, releases, commits, pushes, or
remote settings unless the user explicitly authorizes those external changes.

## Current handoff state (2026-09-11)

- Version `v0.1.0` is published from commit `bc3d54f`; the release preparation,
  release-PR CI, main-branch CI, and publication workflows all completed
  successfully.
- Both release workflows prefer `RELEASE_PLEASE_TOKEN` when configured and fall
  back to `github.token`. No release PAT is currently configured. Repository
  Actions permissions retain read-only defaults while allowing workflows to
  create release pull requests, matching `lib-websubhub`.
- There are no open pull requests. The `v1` issue label groups the proposed,
  intentionally unprioritized stable-release backlog in issue #18. Individual
  work items are issues #7 through #17.
- The proposed v1 boundary remains in-process and standard-library-only. The v1
  tracker explicitly excludes a topology DSL, worker pool, persistence, CPU
  affinity, cross-process transport, cgo, `unsafe`, and channel substitution.
- Issues #7 and #8 establish close and drain behavior. Issue #11 establishes the
  processor failure, panic, halt, and restart contracts; continue with #12.

## Performance work

Use the existing benchmark and load tools rather than one-off programs:

```bash
go test -run='^$' -bench=. -benchmem -benchtime=500ms -count=3
go run ./cmd/loadtest -events=1000000 -producers=1 -consumers=1 \
  -ring-size=65536 -batch-size=256
```

For comparisons, keep Go version, `GOMAXPROCS`, machine load, CPU governor, ring
size, batching, topology, and sampling flags fixed. Report all samples and
allocations, not only the best result. Compare against the buffered-channel
baseline where appropriate. Do not present laptop or powersave-mode measurements
as portable guarantees or hard release thresholds.

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
