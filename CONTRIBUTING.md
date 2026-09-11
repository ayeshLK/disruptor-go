# Contributing

Thank you for helping improve `disruptor-go`.

## Development setup

Install Go 1.25 or newer. The module and its tests intentionally use only the Go
standard library; dependencies require explicit architecture and security
review.

## Validate changes

Run the complete suite before opening a pull request:

```sh
gofmt -w <changed-go-files>
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out .
go tool cover -func=coverage.out
go run ./examples/basic
```

Root-package statement coverage must remain at least 85%. Coverage does not
replace explicit assertions for concurrency invariants. Do not commit
`coverage.out`.

For concurrency-sensitive changes, also run:

```sh
go test -race -count=10 -timeout=60s .
```

Use the existing benchmarks and load runner for performance work. Performance
results are machine-specific and are not hard CI thresholds.

## Testing expectations

Add deterministic regression tests for changed behavior. Cover both producer
modes where relevant, publication gaps, wraparound and slow gates, processor
batch acknowledgement, dependency visibility, cancellation, close, and alerts.
Avoid timing-only assertions; bounded deadlines should only prevent deadlocks
from hanging the suite.

## Commits and pull requests

Use Conventional Commit subjects. `fix:` selects a patch release, `feat:`
selects a minor release, and `type!:` or a `BREAKING CHANGE:` footer marks an
incompatible change.

Pull requests should explain observable behavior, identify affected concurrency
invariants, link relevant issues, and list validation performed. Keep commits
focused and exclude generated output and local benchmark artifacts.

## Releases

The root module uses immutable semantic-version tags such as `v0.1.0`. Do not
create, move, reuse, or delete release tags manually.

Release Please owns `CHANGELOG.md`. A maintainer manually runs Prepare Release
to create or update a version and changelog pull request. After review and merge,
a maintainer runs Publish Release through the protected `release` environment.
Publication reruns validation before Release Please creates the tag and GitHub
Release.

Workflow changes must preserve least-privilege permissions and pin third-party
actions to full commit SHAs.

## Repository hygiene and licensing

Do not commit credentials, binaries, coverage profiles, local workspaces, editor
state, or profiles. Every Go source file, including tests, examples, benchmarks,
and commands, must begin with the repository's Apache-2.0 header and `Copyright
2026 Ayesh Almeida` statement. Contributions intentionally submitted to this
project are
licensed under Apache-2.0 as described by section 5 of `LICENSE`.
