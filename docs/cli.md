# `netbaiot` CLI

The unified CLI runs the gateway, provides a development demo and local config
checks, and manages a running gateway through `netbaiot-client`. Both
`netbaiot serve` and the compatibility `netbaiot-server` call the same server
library and signal/drain/recovery implementation.

```bash
netbaiot --help
netbaiot serve --help
netbaiot demo --help
netbaiot config --help
netbaiot serve --config configs/development.json
netbaiot demo
netbaiot demo --once
netbaiot config check --config configs/tutorial.json
netbaiot --output json config check --config configs/tutorial.json
netbaiot config limits
netbaiot version
netbaiot init my-gateway
netbaiot init --production my-production
netbaiot doctor --config netbaiot.json
netbaiot config schema
netbaiot --version
```

`serve` and `config check` default to `configs/development.json`. Use `--config`
for an explicit path; configuration file paths stay relative to the current working
directory. `config limits` prints the actual `Limits::default()` as pretty JSON.
`version` reads the binary's Cargo package version, without upgrading the release.
Local commands do not require management endpoint/token flags.

`demo` uses the existing standard-MQTT SDK, real authentication/ACL/codec/router,
a development-only loopback HTTP sink (one connection at a time, 64 KiB bodies,
16 queued notifications / 1 MiB encoded-byte budget, two-second connection deadlines),
dynamic ports and private temporary
recovery files. It separately reports QoS1 PUBACK/EventAccepted and observed sink
ACK. No Python, Mosquitto executable or external broker is required. `--once`
finishes one sample and gracefully exits; otherwise it prints an ordinary
`mosquitto_pub` example and waits for Ctrl-C (Unix also handles SIGTERM). The sample
device credential is displayed intentionally for local experimentation; it is
never a production template. The random demo management secret is not printed.

## Configuration diagnostics

`config check` reuses server static validation, actual bounded PEM loading/key
matching and management-auth construction. It collects independent errors when
possible. It does not bind, start workers, deliver events, contact remote URLs,
acquire a recovery lock or modify/read recovery snapshots. It checks recovery path
metadata only. Missing directories may be created later by the runtime. Port
availability, remote behavior, snapshot integrity and final runtime ownership still
need runtime validation.

JSON stdout has the stable shape `{"valid":true,"diagnostics":[]}`. A failure has
`valid:false` with `code`, `path`, `message` and `help` per diagnostic. Values and
serde error text are omitted so credentials cannot leak. Human mode prints the same
codes and fields with remediation. Parse/type errors stop their dependent subtree;
independent static errors are collected.

| Code | Meaning |
| --- | --- |
| NBI-CFG-001 | Malformed JSON, trailing JSON or incompatible field type |
| NBI-CFG-002 | Unknown configuration field |
| NBI-CFG-003 | Invalid listener address or development listener scope |
| NBI-CFG-004 | Public device ingress missing TLS |
| NBI-CFG-005 | Public management missing TLS |
| NBI-CFG-006 | TLS file/CA/key parse or matching failure |
| NBI-CFG-007 | Invalid management authentication, mTLS mapping or admin secret source |
| NBI-CFG-008 | Inconsistent business RPC/legacy stream settings |
| NBI-CFG-009 | Missing/unsafe business endpoint or delivery selection |
| NBI-CFG-010 | Invalid count/byte/deadline limits or hierarchy |
| NBI-CFG-011 | Invalid device authentication/credential/codec settings |
| NBI-CFG-012 | Invalid/inaccessible recovery directory path |
| NBI-CFG-013 | Missing/unreadable/oversized configuration file |

Both HTTP provider and sink URLs use one security predicate: HTTPS, or HTTP on
literal loopback/localhost, without URL username/password. The runtime keeps its
existing no-redirect behavior. Checks never issue requests.

## Manage a running gateway

```text
netbaiot server status
netbaiot server drain --yes
netbaiot device status DEVICE --tenant TENANT --product PRODUCT
netbaiot events subscribe [--tenant ...] [--product ...] [--device ...] [--type ...]
netbaiot command send DEVICE --json JSON
netbaiot command send DEVICE --payload-file PATH
netbaiot auth invalidate --device DEVICE --tenant TENANT --product PRODUCT
```

Global flags can precede or follow the command: `--endpoint`, `--token`,
`--api-key`, `--event-address`, `--event-token`, `--output human|json`.
Environment fallbacks remain `NETBAIOT_ENDPOINT`, `NETBAIOT_TOKEN`,
`NETBAIOT_API_KEY`, `NETBAIOT_EVENT_ADDRESS`, `NETBAIOT_EVENT_TOKEN`,
`NETBAIOT_TENANT`, and `NETBAIOT_PRODUCT`. Explicit credential flags take priority;
otherwise `NETBAIOT_TOKEN` takes priority over `NETBAIOT_API_KEY`.
Production automation should prefer protected environment/secret sources to
command-line secrets, which may be visible in process listings. Token-file support
is deferred. Tokens are not printed in help, Debug or command errors.

Clap rejects unknown flags/commands, repeated singleton options, missing values,
invalid IDs/output/addresses, simultaneous token/API-key flags and simultaneous
JSON/file payloads. These previously ambiguous/ignored inputs now fail with usage
exit 2. Drain still requires `--yes`; no offline command queue or blind retry was
added. Event output is flushed before manual ACK, and cancelling subscription stops
its client-owned tasks. `--output json` emits data/JSONL only on stdout; runtime logs
and error messages use stderr. Demo is intentionally interactive human output.

Exit codes remain 0 success, 2 usage/configuration, 3 authentication, 4 forbidden,
5 device offline and 6 unavailable/runtime/I/O. The compatibility server keeps its
original nonzero failure convention and accepts its existing default/positional
path and `--print-default-limits` forms.

Project setup, non-destructive doctor checks and IDE schema support are documented
in [configuration](configuration.md) and [doctor](doctor.md). Maintenance is separate
from the operator CLI: see [cargo xtask](maintenance.md).
