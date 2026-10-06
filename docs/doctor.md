# Local environment diagnosis

```bash
netbaiot doctor --config netbaiot.json
netbaiot --output json doctor --config netbaiot.json
netbaiot doctor --config netbaiot.json --network
```

Doctor defaults to netbaiot.json. It calls the same configuration validator and
local TLS/secret-source checks as config check. Independent checks report PASS,
WARN, FAIL or SKIP. JSON has stable ok/checks/configuration fields; each check has
id/status/code/message. No FAIL means exit0; configuration failures exit2 and
filesystem/binding/expiry/network failures exit6. WARN does not fail.

| Check | Behavior |
| --- | --- |
| configuration | Shared real config diagnostics (NBI-CFG-*); no duplicated validator |
| device/management/business ports | Bounded local TCP/UDP bind and immediate close; NBI-DOC-002 |
| recovery | Real local directory, exclusive gateway lock, private random probe creation/write/sync/delete, real bounded MQTT/spool decoders; NBI-DOC-001 |
| device/management/business TLS and client CA | Actual PEM/key/CA config checks plus certificate UTC validity; <30 days WARN, expired/not-yet-valid FAIL; NBI-DOC-005 |
| provider/sink network | Default SKIP; --network enables bounded async DNS/TCP/TLS only; NBI-DOC-003 |
| business ACK | SKIP/NOT TESTED; never submits fake business events or device commands |
| time | Prints current UTC Unix seconds; trusted clock synchronization NOT CHECKED (NBI-DOC-004) |

Binding PASS means available **now**, not a reservation: serve rechecks ports and
ownership. A running gateway normally makes its configured ports/lock unavailable
to preflight, which does not itself indicate the live gateway is unhealthy.

Recovery inspection never truncates, renames, deletes or replaces existing
snapshots. It can create a missing local recovery directory and the runtime's
persistent `.netbaiot.lock` ownership metadata. That lock file is never unlinked.
Only `.netbaiot-doctor-<random>` probes are deleted; failure is reported. Paths and
parents must be trusted local filesystem components, as for the gateway spool.
Configuration limits still bound decoder reads; arbitrary disk/kernel stalls do
not gain a general hardware deadline guarantee.

--network uses a three-second whole-probe deadline, asynchronous DNS with one
attempt/one concurrent request/no response cache, at most16 candidate addresses,
and verified HTTPS server certificates using public roots. It sends no HTTP
method, Authorization header, credential, webhook body or auth mutation. Reachability
PASS does not prove provider authentication or the business ACK path. No endpoint
URL/query/private key/secret content is printed. Custom private network CAs are not
a new doctor feature; an untrusted endpoint fails TLS verification.
