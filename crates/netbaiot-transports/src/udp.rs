use crate::common::Services;
use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{collections::HashMap, sync::Arc};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
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
    if input.len() > maximum
        || input.len() < 4 + 1 + 1 + 4 + 16 + 8 + 8 + 2 + 32
        || input.get(..4) != Some(b"NBI1")
    {
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
pub struct ReplayWindow {
    entries: HashMap<(DeviceKey, [u8; 16]), Replay>,
    limits: Arc<Limits>,
}
impl ReplayWindow {
    pub fn new(limits: Arc<Limits>) -> Self {
        Self {
            entries: HashMap::new(),
            limits,
        }
    }
    pub fn check(
        &mut self,
        device: &DeviceKey,
        boot: [u8; 16],
        seq: u64,
        timestamp: i64,
        now: i64,
    ) -> Result<()> {
        if now.abs_diff(timestamp) > self.limits.udp_clock_skew_ms {
            return Err(Error::Authentication);
        }
        self.entries.retain(|_, v| v.expires > now);
        let key = (device.clone(), boot);
        if let Some(entry) = self.entries.get(&key) {
            if seq <= entry.max {
                let delta = entry.max - seq;
                if delta >= 64 || entry.bits & (1 << delta) != 0 {
                    return Err(Error::Conflict);
                }
            }
        } else if self.entries.len() >= self.limits.max_replay_entries
            || self.entries.keys().filter(|(d, _)| d == device).count()
                >= self.limits.max_replay_entries_per_device
            || self
                .entries
                .keys()
                .filter(|(d, _)| d.tenant_id == device.tenant_id)
                .count()
                >= self.limits.max_replay_entries_per_tenant
        {
            return Err(Error::Overloaded);
        }
        Ok(())
    }
    pub fn commit(&mut self, device: DeviceKey, boot: [u8; 16], seq: u64, now: i64) {
        let e = self.entries.entry((device, boot)).or_insert(Replay {
            max: seq,
            bits: 0,
            expires: 0,
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
        e.expires = now.saturating_add(self.limits.replay_ttl_ms as i64);
    }
}
pub async fn serve(socket: UdpSocket, s: Arc<Services>, stop: CancellationToken) -> Result<()> {
    let l = &s.ingress.limits;
    let mut input = vec![0; l.max_udp_datagram_size + 1];
    let mut replay = ReplayWindow::new(l.clone());
    loop {
        let (len, peer) = tokio::select! {biased;_=stop.cancelled()=>break,result=socket.recv_from(&mut input)=>result.map_err(|_|Error::Unavailable)?};
        s.ingress.metrics.inc(Metric::UdpDatagrams);
        if s.rates.take(peer.ip()).is_err() {
            continue;
        }
        let result = async {
            let envelope = decode(&input[..len], l.max_udp_datagram_size)?;
            let auth = s
                .ingress
                .authenticate(AuthenticationRequest::Signed {
                    credential_id: envelope.credential_id,
                    message: envelope.signed,
                    tag: envelope.tag,
                })
                .await?;
            if auth.credential_version != envelope.credential_version {
                return Err(Error::Authentication);
            }
            replay.check(
                &auth.device_key,
                envelope.boot_id,
                envelope.sequence,
                envelope.timestamp,
                now_ms(),
            )?;
            s.ingress
                .ingest(
                    &auth,
                    IngressEnvelope {
                        transport: Transport::Udp,
                        payload: envelope.payload,
                        require_command_ack: false,
                    },
                )
                .await?;
            replay.commit(
                auth.device_key,
                envelope.boot_id,
                envelope.sequence,
                now_ms(),
            );
            // Uplink only: no reply avoids spoofed receipts and UDP amplification.
            Ok::<(), Error>(())
        }
        .await;
        if let Err(e) = result {
            tracing::debug!(error=%e,"UDP datagram rejected");
        }
    }
    Ok(())
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
        w.check(&k, [1; 16], 5, 1000, 1000).unwrap();
        w.commit(k.clone(), [1; 16], 5, 1000);
        assert!(w.check(&k, [1; 16], 5, 1000, 1000).is_err());
        w.check(&k, [1; 16], 4, 1000, 1000).unwrap();
        w.commit(k.clone(), [1; 16], 4, 1000);
        assert!(w.check(&k, [1; 16], 4, 1000, 1000).is_err());
        assert!(w.check(&k, [1; 16], 6, 1000, 200000).is_err());
    }
    #[test]
    fn arbitrary_envelopes() {
        for n in 0..1300 {
            let _ = decode(&vec![0; n], 1200);
        }
        assert!(decode(b"NBI1\xff", 1200).is_err());
    }
}
