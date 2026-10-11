use crate::common::Services;
use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio::net::UdpSocket;
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::sync::CancellationToken;
pub const ACK_SIZE: usize = 64;
pub const MIN_ENVELOPE_SIZE: usize = 76;

/// NBA1 confirms EventAccepted, not a business sink ACK or command execution.
pub fn encode_ack(
    verifier: &DeviceVerifier,
    credential_version: u32,
    boot_id: [u8; 16],
    sequence: u64,
) -> Result<[u8; ACK_SIZE]> {
    let mut ack = [0; ACK_SIZE];
    ack[..4].copy_from_slice(b"NBA1");
    ack[4..8].copy_from_slice(&credential_version.to_be_bytes());
    ack[8..24].copy_from_slice(&boot_id);
    ack[24..32].copy_from_slice(&sequence.to_be_bytes());
    let tag = verifier.sign(&ack[..32])?;
    ack[32..].copy_from_slice(&tag);
    Ok(ack)
}

/// Binary network-order envelope. Signature covers every preceding byte.
pub struct Envelope<'a> {
    pub credential_id: &'a str,
    pub credential_version: u32,
    pub boot_id: [u8; 16],
    pub sequence: u64,
    pub timestamp: i64,
    pub payload: &'a [u8],
    pub signed: &'a [u8],
    pub tag: &'a [u8],
}
pub fn decode(input: &[u8], maximum: usize) -> Result<Envelope<'_>> {
    if input.len() > maximum || input.len() < MIN_ENVELOPE_SIZE || input.get(..4) != Some(b"NBI1") {
        return Err(Error::Invalid);
    }
    let mut at = 5usize;
    let id_len = usize::from(input[4]);
    if id_len == 0 || id_len > 64 {
        return Err(Error::Invalid);
    }
    fn take<'a>(input: &'a [u8], at: &mut usize, n: usize) -> Result<&'a [u8]> {
        let end = at.checked_add(n).ok_or(Error::Invalid)?;
        let bytes = input.get(*at..end).ok_or(Error::Invalid)?;
        *at = end;
        Ok(bytes)
    }
    let credential_id =
        std::str::from_utf8(take(input, &mut at, id_len)?).map_err(|_| Error::Invalid)?;
    DeviceId::new(credential_id).map_err(|_| Error::Invalid)?;
    let credential_version = u32::from_be_bytes(
        take(input, &mut at, 4)?
            .try_into()
            .map_err(|_| Error::Invalid)?,
    );
    let boot_id = take(input, &mut at, 16)?
        .try_into()
        .map_err(|_| Error::Invalid)?;
    let sequence = u64::from_be_bytes(
        take(input, &mut at, 8)?
            .try_into()
            .map_err(|_| Error::Invalid)?,
    );
    let timestamp = i64::from_be_bytes(
        take(input, &mut at, 8)?
            .try_into()
            .map_err(|_| Error::Invalid)?,
    );
    let length = usize::from(u16::from_be_bytes(
        take(input, &mut at, 2)?
            .try_into()
            .map_err(|_| Error::Invalid)?,
    ));
    let payload = take(input, &mut at, length)?;
    let signed = &input[..at];
    let tag = take(input, &mut at, 32)?;
    if at != input.len() {
        return Err(Error::Invalid);
    }
    Ok(Envelope {
        credential_id,
        credential_version,
        boot_id,
        sequence,
        timestamp,
        payload,
        signed,
        tag,
    })
}
struct Replay {
    max: u64,
    bits: u64,
    expires: i64,
}
/// Only committed EventAccepted sequences can take the duplicate fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayDecision {
    New,
    AcceptedDuplicate,
}
type ReplayKey = (DeviceKey, [u8; 16], u32);
type ReplayExpiry = (i64, TenantId, ProductId, DeviceId, [u8; 16], u32);
fn expiry_key(expires: i64, key: &ReplayKey) -> ReplayExpiry {
    (
        expires,
        key.0.tenant_id.clone(),
        key.0.product_id.clone(),
        key.0.device_id.clone(),
        key.1,
        key.2,
    )
}
pub struct ReplayWindow {
    entries: HashMap<ReplayKey, Replay>,
    // Exactly one expiry key per live entry: refresh removes the previous key.
    // No stale heap nodes accumulate under a hot device or a backwards wall clock.
    expirations: BTreeSet<ReplayExpiry>,
    devices: HashMap<DeviceKey, usize>,
    tenants: HashMap<TenantId, usize>,
    limits: Arc<Limits>,
}
impl ReplayWindow {
    pub fn new(limits: Arc<Limits>) -> Self {
        Self {
            entries: HashMap::new(),
            expirations: BTreeSet::new(),
            devices: HashMap::new(),
            tenants: HashMap::new(),
            limits,
        }
    }
    pub fn check(
        &mut self,
        device: &DeviceKey,
        credential_version: u32,
        boot: [u8; 16],
        seq: u64,
        timestamp: i64,
        now: i64,
    ) -> Result<ReplayDecision> {
        if now.abs_diff(timestamp) > self.limits.udp_clock_skew_ms {
            return Err(Error::Authentication);
        }
        self.prune(now);
        let key = (device.clone(), boot, credential_version);
        if let Some(entry) = self.entries.get(&key) {
            if seq <= entry.max {
                let delta = entry.max - seq;
                if delta >= 64 {
                    return Err(Error::Conflict);
                }
                if entry.bits & (1 << delta) != 0 {
                    return Ok(ReplayDecision::AcceptedDuplicate);
                }
            }
        } else if self.entries.len() >= self.limits.max_replay_entries
            || self.devices.get(device).copied().unwrap_or(0)
                >= self.limits.max_replay_entries_per_device
            || self.tenants.get(&device.tenant_id).copied().unwrap_or(0)
                >= self.limits.max_replay_entries_per_tenant
        {
            return Err(Error::Overloaded);
        }
        Ok(ReplayDecision::New)
    }
    /// Called only after a successful New check and EventAccepted, with no intervening
    /// replay mutation. The UDP replay mutex owns this ordering across the await.
    pub fn commit(
        &mut self,
        device: DeviceKey,
        credential_version: u32,
        boot: [u8; 16],
        seq: u64,
        now: i64,
    ) {
        let key = (device, boot, credential_version);
        let expires = now.saturating_add(self.limits.replay_ttl_ms as i64);
        let previous = self.entries.get(&key).map(|entry| entry.expires);
        if previous != Some(expires) {
            if let Some(previous) = previous {
                self.expirations.remove(&expiry_key(previous, &key));
            } else {
                *self.devices.entry(key.0.clone()).or_default() += 1;
                *self.tenants.entry(key.0.tenant_id.clone()).or_default() += 1;
            }
            self.expirations.insert(expiry_key(expires, &key));
        }
        let e = self.entries.entry(key).or_insert(Replay {
            max: seq,
            bits: 0,
            expires,
        });
        if seq > e.max {
            let delta = seq - e.max;
            e.bits = if delta >= 64 { 0 } else { e.bits << delta };
            e.max = seq;
        }
        let delta = e.max.saturating_sub(seq);
        if delta < 64 {
            e.bits |= 1 << delta;
        }
        e.expires = expires;
    }

    fn prune(&mut self, now: i64) {
        while self.expirations.first().is_some_and(|entry| entry.0 <= now) {
            let Some((_, tenant_id, product_id, device_id, boot, version)) =
                self.expirations.pop_first()
            else {
                break;
            };
            let device = DeviceKey {
                tenant_id,
                product_id,
                device_id,
            };
            if self
                .entries
                .remove(&(device.clone(), boot, version))
                .is_some()
            {
                if let Some(count) = self.devices.get_mut(&device) {
                    *count -= 1;
                    if *count == 0 {
                        self.devices.remove(&device);
                    }
                }
                if let Some(count) = self.tenants.get_mut(&device.tenant_id) {
                    *count -= 1;
                    if *count == 0 {
                        self.tenants.remove(&device.tenant_id);
                    }
                }
            }
        }
    }
}
#[cfg(test)]
#[path = "udp_replay_tests.rs"]
mod replay_tests;

pub async fn serve(socket: UdpSocket, s: Arc<Services>, stop: CancellationToken) -> Result<()> {
    let l = &s.ingress.limits;
    let mut input = vec![0; l.max_udp_datagram_size + 1];
    let socket = Arc::new(socket);
    let replay = Arc::new(Mutex::new(ReplayWindow::new(l.clone())));
    // There is no waiting datagram queue. Both task count and retained datagram
    // bytes are bounded by this ceiling times max_udp_datagram_size.
    let mut tasks = JoinSet::new();
    let mut receive_error = None;
    loop {
        let (len, peer) = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if result.is_err() {
                    tracing::error!("UDP datagram worker failed");
                }
                continue;
            }
            result = socket.recv_from(&mut input) => match result {
                Ok(received) => received,
                Err(_) => {
                    receive_error = Some(Error::Unavailable);
                    break;
                }
            },
        };
        s.ingress.metrics.inc(Metric::UdpDatagrams);
        if s.rates.take(peer.ip()).is_err() {
            continue;
        }
        if len > l.max_udp_datagram_size || tasks.len() >= l.max_udp_inflight_datagrams {
            s.ingress.metrics.inc(Metric::UdpOverloadDrops);
            continue;
        }
        let packet = input[..len].to_vec();
        let ingress = s.ingress.clone();
        let replay = replay.clone();
        let socket = socket.clone();
        tasks.spawn(async move {
            if let Err(error) = accept_and_ack_concurrent(&packet, &ingress, &replay, |ack| {
                socket.try_send_to(ack, peer)
            })
            .await
            {
                tracing::debug!(%error, "UDP datagram rejected");
            }
        });
    }
    // Quiesce stops new receipts, while already started admissions retain their
    // guards and complete before the server drains or spools required work.
    while let Some(result) = tasks.join_next().await {
        if result.is_err() {
            tracing::error!("UDP datagram worker failed during shutdown");
        }
    }
    match receive_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
/// The send boundary is synchronous and injectable for deterministic socket-pressure tests.
/// Send failure is deliberately outside acceptance error semantics.
#[cfg(test)]
async fn accept_and_ack(
    input: &[u8],
    ingress: &Ingress,
    replay: &mut ReplayWindow,
    send: impl FnOnce(&[u8]) -> std::io::Result<usize>,
) -> Result<()> {
    // Also gates duplicates: quiescing does not start new receipt work.
    let _admission = ingress.lifecycle.begin_admission()?;
    let envelope = decode(input, ingress.limits.max_udp_datagram_size)?;
    let verified = ingress
        .auth_cache
        .verify_signed_with_verifier(envelope.credential_id, envelope.signed, envelope.tag)
        .await?;
    accept_verified(&envelope, &verified, ingress, replay).await?;
    send_ack(&envelope, &verified, ingress, send);
    Ok(())
}

async fn accept_and_ack_concurrent(
    input: &[u8],
    ingress: &Ingress,
    replay: &Mutex<ReplayWindow>,
    send: impl FnOnce(&[u8]) -> std::io::Result<usize>,
) -> Result<()> {
    let _admission = ingress.lifecycle.begin_admission()?;
    let envelope = decode(input, ingress.limits.max_udp_datagram_size)?;
    let verified = ingress
        .auth_cache
        .verify_signed_with_verifier(envelope.credential_id, envelope.signed, envelope.tag)
        .await?;
    {
        let mut replay = replay.lock().await;
        accept_verified(&envelope, &verified, ingress, &mut replay).await?;
    }
    send_ack(&envelope, &verified, ingress, send);
    Ok(())
}

async fn accept_verified(
    envelope: &Envelope<'_>,
    verified: &VerifiedDatagram,
    ingress: &Ingress,
    replay: &mut ReplayWindow,
) -> Result<()> {
    let auth = verified.identity();
    if auth.credential_version != envelope.credential_version {
        return Err(Error::Authentication);
    }
    if !auth.permissions.publish {
        return Err(Error::Forbidden);
    }
    match replay.check(
        &auth.device_key,
        envelope.credential_version,
        envelope.boot_id,
        envelope.sequence,
        envelope.timestamp,
        now_ms(),
    )? {
        ReplayDecision::New => {
            ingress
                .ingest(
                    auth,
                    IngressEnvelope {
                        transport: Transport::Udp,
                        payload: envelope.payload,
                        require_command_ack: false,
                        validated_at: std::time::Instant::now(),
                        validation_us: 0,
                    },
                )
                .await?;
            replay.commit(
                auth.device_key.clone(),
                envelope.credential_version,
                envelope.boot_id,
                envelope.sequence,
                now_ms(),
            );
            ingress.metrics.inc(Metric::UdpAccepted);
        }
        ReplayDecision::AcceptedDuplicate => {
            ingress.metrics.inc(Metric::UdpAcceptedDuplicates);
        }
    }
    Ok(())
}

fn send_ack(
    envelope: &Envelope<'_>,
    verified: &VerifiedDatagram,
    ingress: &Ingress,
    send: impl FnOnce(&[u8]) -> std::io::Result<usize>,
) {
    // Invalidation during admission must not leave an old signer usable for a receipt.
    let receipt = ingress
        .auth_cache
        .with_current_verifier(&verified, |verifier| {
            let ack = encode_ack(
                verifier,
                envelope.credential_version,
                envelope.boot_id,
                envelope.sequence,
            )?;
            match send(&ack) {
                Ok(ACK_SIZE) => ingress.metrics.inc(Metric::UdpAcksSent),
                result => {
                    ingress.metrics.inc(Metric::UdpAckSendFailures);
                    tracing::debug!(?result, "UDP acceptance ACK not emitted");
                }
            }
            Ok(())
        });
    if let Err(error) = receipt {
        ingress.metrics.inc(Metric::UdpAckSendFailures);
        tracing::debug!(%error, "UDP acceptance ACK suppressed");
    }
    // Acceptance is irrevocable, including when invalidation suppresses the receipt.
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        }
    }
    #[test]
    fn replay_reordering_and_expiry() {
        let l = Arc::new(Limits::default());
        let mut w = ReplayWindow::new(l);
        let k = key();
        w.check(&k, 1, [1; 16], 5, 1000, 1000).unwrap();
        w.commit(k.clone(), 1, [1; 16], 5, 1000);
        assert_eq!(
            w.check(&k, 1, [1; 16], 5, 1000, 1000).unwrap(),
            ReplayDecision::AcceptedDuplicate
        );
        w.check(&k, 1, [1; 16], 4, 1000, 1000).unwrap();
        w.commit(k.clone(), 1, [1; 16], 4, 1000);
        assert_eq!(
            w.check(&k, 1, [1; 16], 4, 1000, 1000).unwrap(),
            ReplayDecision::AcceptedDuplicate
        );
        assert!(w.check(&k, 1, [1; 16], 6, 1000, 200000).is_err());
    }
    #[test]
    fn arbitrary_envelopes() {
        for n in 0..1300 {
            let _ = decode(&vec![0; n], 1200);
        }
        assert!(decode(b"NBI1\xff", 1200).is_err());
    }
}

#[cfg(test)]
mod audit {
    use super::*;
    #[test]
    fn full_replay_cache_never_evicts_live_entry_and_sequence_does_not_wrap() {
        let mut w = ReplayWindow::new(Arc::new(Limits {
            max_replay_entries: 1,
            max_replay_entries_per_device: 1,
            ..Limits::default()
        }));
        let k = DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        };
        w.check(&k, 1, [1; 16], u64::MAX, 1000, 1000).unwrap();
        w.commit(k.clone(), 1, [1; 16], u64::MAX, 1000);
        assert!(matches!(
            w.check(&k, 1, [2; 16], 0, 1000, 1000),
            Err(Error::Overloaded)
        ));
        assert!(matches!(
            w.check(&k, 1, [1; 16], u64::MAX, 1000, 1000),
            Ok(ReplayDecision::AcceptedDuplicate)
        ));
        assert!(matches!(
            w.check(&k, 1, [1; 16], 0, 1000, 1000),
            Err(Error::Conflict)
        ));
        assert!(w.check(&k, 1, [1; 16], u64::MAX - 63, 1000, 1000).is_ok());
        assert!(w.check(&k, 1, [1; 16], u64::MAX - 64, 1000, 1000).is_err());
        assert!(w.check(&k, 1, [1; 16], 1, i64::MIN, i64::MAX).is_err());
        w.check(&k, 1, [2; 16], 0, 121001, 121001).unwrap();
        assert!(w.check(&k, 1, [1; 16], u64::MAX, 1000, 121001).is_err());
    }
}

#[cfg(test)]
#[path = "udp_tests.rs"]
mod ack_tests;
