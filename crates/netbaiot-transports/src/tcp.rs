use crate::common::*;
use bytes::{Buf, Bytes, BytesMut};
use netbaiot_core::*;
use netbaiot_runtime::*;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
/// Framing extension point, independent of DeviceCodec.
pub trait TcpFramer: Send + Sync {
    fn decode(&self, input: &mut BytesMut) -> Result<Option<Bytes>>;
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>>;
}
pub struct LengthPrefixFramer {
    pub maximum: usize,
}
impl TcpFramer for LengthPrefixFramer {
    fn decode(&self, input: &mut BytesMut) -> Result<Option<Bytes>> {
        if input.len() < 4 {
            return Ok(None);
        }
        let raw = u32::from_be_bytes(input[..4].try_into().map_err(|_| Error::Invalid)?);
        let len = usize::try_from(raw).map_err(|_| Error::Invalid)?;
        if len == 0 || len > self.maximum {
            return Err(Error::Invalid);
        }
        let total = len.checked_add(4).ok_or(Error::Invalid)?;
        if input.len() < total {
            return Ok(None);
        }
        let mut frame = input.split_to(total);
        frame.advance(4);
        Ok(Some(frame.freeze()))
    }
    fn encode(&self, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.is_empty() || payload.len() > self.maximum {
            return Err(Error::Invalid);
        }
        let len = u32::try_from(payload.len()).map_err(|_| Error::Invalid)?;
        let mut out = Vec::with_capacity(payload.len() + 4);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(payload);
        Ok(out)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Handshake {
    credential_id: String,
    secret: String,
}
async fn next(
    reader: &mut Reader,
    stream: &mut BoxStream,
    framer: &dyn TcpFramer,
    idle: Instant,
) -> Result<Bytes> {
    loop {
        if let Some(frame) = framer.decode(&mut reader.buffer)? {
            reader.consumed();
            return Ok(frame);
        }
        reader.read_more(stream, idle).await?;
    }
}
pub async fn connection(
    mut stream: BoxStream,
    s: Arc<Services>,
    mut connection: ConnectionLease,
    stop: CancellationToken,
) -> Result<()> {
    let l = &s.ingress.limits;
    let framer = LengthPrefixFramer {
        maximum: l.max_tcp_frame_size,
    };
    let mut reader = Reader::new(l.max_tcp_frame_size + 4, l.packet_read_timeout_ms);
    let hello = tokio::select! {_=stop.cancelled()=>return Ok(()),hello=next(&mut reader,&mut stream,&framer,Instant::now()+Duration::from_millis(l.connect_timeout_ms))=>hello?};
    if hello.len() > l.max_username_bytes + l.max_password_bytes + 128 {
        return Err(Error::Invalid);
    }
    let hello: Handshake = serde_json::from_slice(&hello).map_err(|_| Error::Invalid)?;
    if hello.credential_id.len() > l.max_username_bytes || hello.secret.len() > l.max_password_bytes
    {
        return Err(Error::Authentication);
    }
    let auth = authenticate_stream(
        &s,
        AuthenticationRequest::Secret {
            credential_id: &hello.credential_id,
            secret: hello.secret.as_bytes(),
        },
        &mut reader,
        &mut stream,
        &stop,
    )
    .await?;
    connection.authenticate(&auth.device_key)?;
    let (session, mut outbound) = s
        .ingress
        .sessions
        .register(&auth.device_key, Transport::Tcp)?;
    write(
        &mut stream,
        &framer.encode(br#"{"authenticated":true}"#)?,
        l.write_timeout_ms,
    )
    .await?;
    let mut last = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = session.cancel.cancelled() => break,
            item = outbound.recv() => {
                let Some(item) = item else { break };
                if item.expires_at.min(item.lease_expires_at) <= now_ms() { continue; }
                let frame = framer.encode(&item.bytes)?;
                write(&mut stream, &frame, l.write_timeout_ms).await?;
                s.router.state(&auth.device_key, item.command_id, item.attempt, DeliveryState::Sent).await?;
            }
            frame = next(&mut reader, &mut stream, &framer, last + Duration::from_millis(l.idle_timeout_ms)) => {
                let frame = frame?;
                last = Instant::now();
                s.ingress.metrics.inc(Metric::TcpFrames);
                let receipt = s.ingress.ingest(&auth, IngressEnvelope {
                    transport: Transport::Tcp, payload: &frame, require_command_ack: false,
                }).await?;
                let bytes = serde_json::to_vec(&receipt).map_err(|_| Error::Internal)?;
                write(&mut stream, &framer.encode(&bytes)?, l.write_timeout_ms).await?;
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_multiple_and_invalid() {
        let f = LengthPrefixFramer { maximum: 16 };
        let wire = f.encode(b"test").unwrap();
        let mut b = BytesMut::new();
        for (i, byte) in wire.iter().enumerate() {
            b.extend_from_slice(&[*byte]);
            assert_eq!(f.decode(&mut b).unwrap().is_some(), i == wire.len() - 1);
        }
        b.extend_from_slice(&wire);
        b.extend_from_slice(&wire);
        assert!(f.decode(&mut b).unwrap().is_some());
        assert!(f.decode(&mut b).unwrap().is_some());
        for n in [0u32, 17, u32::MAX] {
            assert!(
                f.decode(&mut BytesMut::from(n.to_be_bytes().as_slice()))
                    .is_err()
            );
        }
        let wire = f.encode(&[1; 16]).unwrap();
        assert_eq!(
            f.decode(&mut BytesMut::from(wire.as_slice()))
                .unwrap()
                .unwrap()
                .len(),
            16
        );
    }
}
