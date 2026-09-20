# Control plane and configuration cache

The control plane owns device definitions, credentials, product/codec bindings,
device configuration, routes, and sink definitions. NetbaIoT retains only bounded
runtime snapshots.

Startup validates a static bootstrap snapshot, constructs sinks/routes, recovers
committed restart-spool records, and only then enters `RUNNING`/ready. An external
HTTP authentication provider may be configured; requests are timeout and
concurrency bounded and are never retried indefinitely.

`ControlSnapshot` has a monotonically increasing revision plus products, device
configurations, and routes. A replacement is fully validated for count, bytes,
unique keys, product references, and revision before the immutable indexed snapshot
is swapped. Route updates are serialized and validated against installed sinks
before config/event-router state changes.

Device configuration values are shared as `Arc<DeviceConfigSnapshot>`. Device GET
uses revision/ETag; device application result is a separate `ConfigAck` event.
Auth/config caches are cold after restart and are never placed in the delivery spool.
