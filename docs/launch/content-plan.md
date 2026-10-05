# Launch content plan

These are article outlines, not commitments or completed claims. Keep every
example tied to code, tests, or a clearly labeled measurement.

## 1. Why My IoT Gateway Has No Database

- **Main question:** Which state belongs in an ingress gateway and which belongs in an application?
- **Key points:** Separate live routing from durable business data; explain the limited restart spool; list state intentionally owned by backend systems.
- **Subsystem:** runtime lifecycle and restart spool.

## 2. MQTT PUBACK Does Not Mean Your Database Has the Message

- **Main question:** What exactly has happened when a QoS1 publisher gets PUBACK?
- **Key points:** Define `EventAccepted`; show required sink enqueue; separate consumer ACK and database commit; discuss consumer idempotency.
- **Subsystem:** MQTT ingress, EventBus, business sink.

## 3. Why Every Queue in NetbaIoT Has a Limit

- **Main question:** How do count and byte budgets change overload behavior?
- **Key points:** Explain variable payload sizes; show required-sink atomic admission; distinguish backpressure from best-effort drop; include a slow-sink scenario.
- **Subsystem:** limits, quota, EventBus.

## 4. Graceful Restart Without Pretending It Is Crash Durability

- **Main question:** What does the restart spool preserve, and what can a crash still lose?
- **Key points:** Walk the shutdown lifecycle; describe atomic spool commit; distinguish inflight uncertainty; state the abrupt-failure window.
- **Subsystem:** lifecycle, EventBus spool, MQTT recovery.

## 5. MQTT QoS 2 Is More Complicated Than It Looks

- **Main question:** Which MQTT state transitions does the broker own?
- **Key points:** Separate packet identifiers from event IDs; show inbound/outbound handshakes; describe session incarnation and operation tokens; avoid business exactly-once claims.
- **Subsystem:** MQTT packet codecs and broker state machines.

## 6. Multiplexing MQTT and Framed TCP on One Listener

- **Main question:** How can one TCP listener classify both protocols without changing their wire formats?
- **Key points:** Explain TLS-before-classification; bounded prefix detection; shared admission lease and deadline; failure closes without fallback.
- **Subsystem:** device ingress classifier and TCP framing.

## 7. Designing a Bounded Authentication Cache

- **Main question:** How can normal device traffic avoid one remote authentication call per message?
- **Key points:** Bind auth context at connection setup; positive/negative TTLs and invalidation; single-flight bounds; safe credential fingerprints and fail-closed misses.
- **Subsystem:** AuthCache and session registration.

## 8. Authenticated UDP Without Pretending It Is Encrypted

- **Main question:** What does HMAC-authenticated UDP protect, and what does it not?
- **Key points:** Describe NBI1/NBA1; timestamp and replay checks; exact-byte retry; confidentiality and downlink limitations.
- **Subsystem:** UDP transport and replay state.

## 9. What a 20k msg/s Benchmark Actually Proves

- **Main question:** What evidence can a shared-host loopback benchmark support?
- **Key points:** Include measurement SHA and host; distinguish offered/completed rate; account for load-generator CPU; name unmeasured TLS, sink, fanout, and network combinations.
- **Subsystem:** benchmark harness and performance reports.

## 10. Building an IoT Gateway That Stops at the Gateway Boundary

- **Main question:** Which responsibilities should remain in an existing backend?
- **Key points:** Show ingress and `DeviceEvent`; keep durable state and offline intent in the app; explain online-only command routing; use one concrete integration flow.
- **Subsystem:** public protocol, client SDK, command router.
