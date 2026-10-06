# Security Policy

## Supported versions

NetbaIoT is pre-1.0. Only the latest published release receives security fixes.
Upgrade to that release before requesting a backport; the project does not promise
parallel maintenance of older minor or patch versions. Unreleased `main` may
contain fixes under validation.

## Reporting a vulnerability

Do not open a public GitHub issue for an undisclosed security vulnerability.
Use [GitHub's private vulnerability reporting](https://github.com/sskycn/netbaiot/security/advisories/new)
to send a report to the repository maintainers. Private vulnerability reporting
is enabled for this repository. You need a GitHub account; select
**Report a vulnerability** on the repository's Security tab.

If that private form becomes unavailable, do not post exploit details or secrets
publicly. You may open an issue asking maintainers to restore the private reporting
channel, without identifying the vulnerability, affected endpoint, or reproduction.
No separate security email address is currently published by the project.

## What to include

- Affected release version or commit.
- Protocol, endpoint, and relevant deployment configuration.
- Minimal reproduction steps, expected behavior, and observed behavior.
- Security impact and prerequisites (for example, authenticated or unauthenticated access).
- Sanitized logs with credentials, tokens, HMAC keys, sensitive payloads, and local paths removed.
- A suggested mitigation or fix, if known.

Public TLS keys under `tests/fixtures/` and credentials in the loopback tutorial
are intentional test fixtures. They must never be used in production. Report a
production credential exposure privately; do not paste it into an issue.

## Disclosure

Please allow reasonable time for private triage, reproduction, and a fix before
public disclosure. Maintainers will coordinate disclosure through the private
advisory when possible. The project does not promise a response or remediation SLA.

See the [security overview](docs/security.md) and [operations guide](docs/operations-guide.md)
for transport, authentication, secret handling, and planned-restart boundaries.
