# Delivery and persistence semantics

## Acceptance

PostgreSQL acceptance uses one transaction for normalized `ingress_messages`, its
`delivery_jobs` row, and any command execution ACK update. A receipt is returned
only after commit. The integration test injects an outbox insert failure and
verifies that both the message insert and command execution change roll back.
A connection loss near commit creates an uncertain result: retry the same source
ID and content. No exactly-once network claim is made.

Unique key: `(tenant_id, product_id, device_id, source_message_id)`. Canonical JSON
contains trusted device, source ID, occurrence time and typed payload, excluding
random application ID and arrival time. Same key/content returns the original
receipt; different content conflicts. The default retention window is 24 hours
from first acceptance and is not extended by duplicates. After expiry the same
source ID may be accepted as new. Devices must not reuse IDs inside that window.

Logical storage counts and charged bytes have device/tenant/global caps. PostgreSQL
maintains exact counter rows in the same transaction as each message/outbox pair.
The global, tenant, and device updates are one statement with a fixed lock order;
rollback removes both the durable rows and their charge. The global row remains a
deliberate cross-process serialization point near the capacity knee, but normal
admission no longer scans retained message or command tables. Startup migration
backfills the counters from existing retained data.

The volatile store implements the same transaction semantics under one bounded
mutex, but cannot survive a crash. Its receipts explicitly report `volatile`.

## Outbox worker

Every job has attempts, due time, TTL, lease owner, lease expiry and sanitized last
error. PostgreSQL uses `FOR UPDATE SKIP LOCKED`. Claim size is one for the single
serial delivery worker, ensuring an earlier slow call cannot exhaust later jobs'
leases. Ready jobs drain immediately; idle polling and maintenance default to
200 ms. No transaction is open across an external HTTP delivery call.

The external call timeout defaults to 5 seconds; lease is 30 seconds. Retryable
network errors, 429 and 5xx get bounded exponential full jitter. Other HTTP status
failures are terminal. Default maximum attempts is 5, job TTL one hour. Exhausted
or expired jobs remain inspectable until ingress retention expires. Lease ownership
and attempt number protect against stale completion; successful delivery followed
by a lost database update can be delivered again. Consumers must be idempotent.

Runtime storage/time-out failures put each fixed worker into a degraded state with
bounded exponential full-jitter probes. Individual failed messages are not retried
forever or buffered in RAM. The process remains alive, durable ingress fails without
claiming success, and workers resume after a successful database call. Startup
database failure remains fatal. Database statement/lock/acquisition calls and
surrounding external operations have deadlines; shutdown cancels a pending backoff.

## Commands

Business code uses `CommandRouter` or the separately authorized admin HTTP route.
Commands are persisted before routing. IDs are client-generated UUIDs; same ID and
identical command is idempotent, conflicting reuse fails. Commands have a bounded
expiry horizon, byte limit and device/tenant/global count limits. Terminal records
also count until retention, preventing a fast producer from bypassing quotas.

The worker batches current stream-device lookups. HTTP independently leases one
command at pull time. Claims create bounded attempts and delay another claim until
lease expiry plus bounded exponential full jitter. Lease expiry and retry due time
are separate; backoff does not prolong a writer's lease. There is no unbounded offline spool. Retry reuses command_id, so
**devices must deduplicate execution**, even if transport packet IDs differ.

| Signal | Delivery state | Execution |
|---|---|---|
| persisted | Queued | Unknown |
| claimed | Dispatching | unchanged |
| successful transport write | Sent | unchanged |
| MQTT PUBACK | Received | unchanged |
| device application command_ack | Received | Running / Succeeded / Failed |
| TTL / attempts exhausted | Expired / Failed | unchanged |

HTTP sets Sent after the connection finishes writing its response. This still
cannot prove receipt or execution. Command ACK ingress checks device ownership and
expiry. Terminal execution is monotonic. Late ACKs for expired commands are rejected.
Transport state and attempt history cannot regress Received to Sent.
Claims return attempt and lease_expires_at; queue and HTTP callbacks retain them.
Store updates check the attempt and unexpired lease while holding the command lock.
An old callback returns an explicit stale result and cannot update a newer attempt.
Expired callbacks do not mutate completed records. Commands are retained through
expires_at plus command_ttl_ms; attempts cascade-delete with the command. UDP has
no command route. No shared/distributed socket routing is implemented.
