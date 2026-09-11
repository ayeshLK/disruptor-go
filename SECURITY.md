# Security policy

## Reporting a vulnerability

Do not disclose suspected vulnerabilities in a public issue. Use GitHub private
vulnerability reporting or the Security Advisory interface for
`ayeshLK/lib-disruptor`. Include affected versions, reproduction steps, impact,
and any suggested mitigation.

If private reporting is unavailable, contact the repository owner privately
through their GitHub profile and request a secure reporting channel without
including exploit details in the first message. Please allow time to confirm the
report and coordinate a fix before public disclosure.

## Supported versions

Before the first tagged release, only the current default branch receives
security fixes. After releases begin, this section will identify supported
release lines explicitly.

## Security boundary

This package coordinates goroutines in one Go process. It does not provide
cross-process transport, persistence, authentication, authorization, or input
validation for event contents. Applications remain responsible for protecting
the data placed in events and for ensuring that handlers and translators do not
retain reused ring entries beyond their ownership window.

Run applications with the Go race detector during development. Treat malformed
topology, unbounded handler work, ignored cancellation, and retained event
pointers as correctness and availability risks.
