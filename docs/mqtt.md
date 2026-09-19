# Embedded MQTT server

NetbaIoT accepts MQTT connections directly. No external broker or broker crate is
used. The internal incremental packet codec implements the intentionally limited
IoT profile below. This is not a claim of full MQTT 3.1.1 conformance.

Protocol reference: [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).

## Supported profile

| Packet | Behavior |
|---|---|
| CONNECT / CONNACK | MQTT protocol name, level 4; one CONNECT per connection |
| PUBLISH | QoS 0/1 device uplink and application ACK; exact topic ACL |
| PUBACK | QoS 1 packet lifecycle, separate from execution state |
| SUBSCRIBE / SUBACK | Exact own `down` / `up_ack`; QoS 0/1; failed filters get 0x80 |
| UNSUBSCRIBE / UNSUBACK | Generation-owned exact subscriptions |
| PINGREQ / PINGRESP | Connected clients only |
| DISCONNECT | Close and remove clean-session state |

Only CleanSession=1 is supported; Session Present is always zero. Empty client IDs
are rejected. Client ID must equal the provisioned credential ID. This prevents
another authenticated device from claiming somebody else's client ID.
Username is the credential ID; password is the 64-character provisioned key.
The credential provider, not username/topic/body parsing, produces `DeviceKey`.

CleanSession=0 and any Will request are refused with CONNACK 0x05. Bad credentials
receive 0x04, invalid client identity 0x02, and tenant connection overload 0x03.
MQTT 5 CONNECT is refused with a v5-shaped CONNACK reason 0x84 and zero properties;
other unsupported levels receive the 3.1.1 unsupported-version CONNACK 0x01.
The MQTT 5 refusal is not MQTT 5 support or an interoperability claim.

QoS2 PUBLISH/control packets are protocol violations and close the connection.
QoS2 subscriptions receive SUBACK 0x80. Wildcard subscriptions, shared
subscriptions, persistent sessions, retained messages, LWT, MQTT-over-WebSocket,
MQTT-SN, bridges and clustering are unsupported. Retained PUBLISH closes the
connection; NetbaIoT never accepts it and then claims retained persistence. MQTT
3.1.1 has no negative PUBLISH ACK, so rejected publishes close without PUBACK.

## Topics and authorization

```
v1/t/{tenant}/p/{product}/d/{device}/up
v1/t/{tenant}/p/{product}/d/{device}/up_ack
v1/t/{tenant}/p/{product}/d/{device}/down
v1/t/{tenant}/p/{product}/d/{device}/down_ack
```

An authenticated device may publish only its own `up` and `down_ack` (the latter
requires command permission). It may subscribe only to its own `down` (command
permission) and `up_ack`. `down_ack` accepts only a typed command ACK. Core
identifiers are 1–64 ASCII bytes from `[A-Za-z0-9_.:-]`. Slash, plus, hash, NUL,
controls and Unicode identifiers are rejected at provisioning/deserialization.

Subscriptions have an exact-topic hash index; sending never scans all connected
clients. Insertion checks connection/device, tenant and global counts. Stale
session cleanup is generation-conditional. Limits also bound filters per packet,
UTF-8 topic bytes, topic depth and packet IDs. Syntactically valid wildcards are rejected by ACL (SUBACK 0x80); malformed filters
(such as `a+` or `a/#/b`) close SUBSCRIBE/UNSUBSCRIBE connections. Valid unsupported
UNSUBSCRIBE filters are no-ops with UNSUBACK. No wildcard matcher is implemented.

## State, timers and backpressure

`Accepted -> AwaitConnect -> Authenticating -> Connected -> Draining -> Closed`.
No traffic is authenticated before CONNECT succeeds. The connection task owns
incremental input, protocol state, outbound channel and QoS1 packet identifiers.
Partial fixed headers, Remaining Length, variable headers and payloads are retained
until complete. Invalid lengths/flags/UTF-8/zero packet IDs fail without allocating
from an unchecked length. Each stream holds a global memory reservation.

Nonzero keepalive expires at 1.5 times its advertised interval, measured since the
last complete received packet. Keepalive=0 disables the MQTT timer; the separately
documented server idle policy still closes after 120 seconds by default. Incomplete
packets have an independent 30-second read deadline. Auth, CONNECT and writes also
have deadlines. Authentication observes EOF and shutdown before installing a session.
Buffered partial tails keep their original read deadline, including after cancellation. Control packets count toward device/tenant/global packet rates.
Publish parsing does not retain the short-lived protocol admission permit across
codec, PostgreSQL, PUBACK queueing, or socket writes. Durable ingress instead uses
its own device→tenant→node→byte permits and a bounded 16-item/2 MiB/25 ms wait.
MQTT 3.1.1 has no negative PUBLISH ACK, so expiration or overload closes without
PUBACK and releases every partially acquired permit through the connection owner.

Outbound commands have message and encoded-byte permits at connection, tenant and
node levels. QoS1 commands retain permits until PUBACK or disconnect. Application
receipt QoS1 entries retain bounded packet state. Packet IDs are nonzero and never
reused while in flight. Unknown nonzero PUBACK is ignored. QoS1 acknowledgement
inactivity eventually closes the clean session; durable commands can be retried
with the same command ID. No packet retransmission timer is spawned per message.

## Acknowledgements

Uplink PUBACK is emitted only after successful configured ingress acceptance in
this implementation. A subscribed `up_ack` carries the application receipt after
that same boundary. PostgreSQL mode means transaction commit; development mode
explicitly says `volatile`. Neither receipt means the business endpoint processed
the event. Source IDs, not MQTT packet IDs, identify durable duplicates.

Downlink transport writes set `Sent`; PUBACK sets `Received`; neither changes
execution from `Unknown`. Both callbacks carry the claimed attempt; the store fences
stale or expired leases before updating command state. A valid device `command_ack` updates execution inside
the ingress transaction. Downlinks always have retain=false. Subscriptions must
be restored after reconnect. A command arriving before the device subscribes is
left for bounded store retry; no claim of offline MQTT session persistence is made.
