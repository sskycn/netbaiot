# Device key provisioning profile v0.3

NBI1 datagrams, NBA1 receipts and HMAC-SHA256 are unchanged. The existing
`DeviceAuthenticator` interface is the key provider; its original exports remain
available and its definition lives in `netbaiot-runtime::security::key_provider`.
Static verifiers and providers without `replay_policy` retain the 64-sequence
window, including unseen out-of-order messages.

`netbaiot-core::security::hkdf::derive_udp_hmac_key` implements RFC 5869 using
RustCrypto HKDF-SHA256, with 32 bytes of device root secret as input, salt
`netbaiot/device-key/v0.3`, info `netbaiot/udp-hmac-sha256/v0.3`, and a 32-byte
output. Decode the provisioning root secret before derivation. HMAC continues
using raw key bytes and the original signed envelope. Keys have no Debug or
serialization implementation and owned derivation buffers are zeroized on drop.

An authority may derive roots from its protected versioned master keys and public
metadata, including tenant/device/credential identity, generation and random
salt. Neither master nor root travels to the gateway: the existing protected
Business RPC `device.resolve_verifier` returns only the derived verifier, identity
and optional `replay_policy: "monotonic"`. It must check revocation, expiry and
minimum authority revision. Install this gateway revision before enabling such
credentials; older RPC decoders reject the new opt-in field. Legacy responses
omit it and remain byte-shape compatible.

Monotonic providers reject unseen sequences below the high-water mark within
one DeviceKey/boot ID/credential version. Already committed duplicates in the
64-entry receipt window still receive an NBA1 ACK without another EventAccepted.
Failed ingestion consumes no sequence. Existing timestamp skew, bounded replay
TTL/capacity, HMAC verification through `verify_slice`, and invalidation epoch
fences apply to every packet, including retries. Boot IDs must be random and
unique on restart; persist sequence or choose a new boot ID after device restart.
Rotate before sequence exhaustion. Never wrap a sequence within one boot.

For monotonic providers only, ingress replaces the codec source ID with the
canonical authenticated envelope identity
`udp:{credential_version:08x}:{boot_id_hex_32}:{sequence:016x}` (62 characters).
The codec still validates its payload. Business consumers reserve this namespace
and deduplicate it within the authenticated DeviceKey in their own transaction.
This permits durable business deduplication across gateway restarts without a
gateway database or new public event fields. Other transports/providers preserve
their existing source IDs.

Replay memory resets on gateway restart; captured fresh packets can again pass
gateway admission, but a durable consumer must suppress their business effects.
NBA1 continues to acknowledge EventAccepted, not business persistence or
exactly-once physical execution. See [device protocol](device-protocol.md) for the
unchanged packet layout and acknowledgement boundary.
