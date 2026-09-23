//! Bounded application-prefix classification, after TLS. No business/auth parsing.
use crate::{common::BoxStream, mqtt::packet::fixed_header};
use netbaiot_core::Transport;
use netbaiot_runtime::{Error, Result};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    time::Instant,
};

pub const MAX_PREFIX: usize = 12; // fixed header (1 + 4) + MQTT name/level (7)
/// Limits::validate caps frames at 1 MiB: a legal TCP length starts with 0,
/// MQTT CONNECT with 0x10. Other application protocols have no parser fallback.
pub fn classify_prefix(input: &[u8], max_tcp: usize, max_mqtt: usize) -> Result<Option<Transport>> {
    if max_tcp > 1_048_576 || input.len() > MAX_PREFIX {
        return Err(Error::Invalid);
    }
    let Some(first) = input.first() else {
        return Ok(None);
    };
    if *first == 0 {
        if input.len() < 4 {
            return Ok(None);
        }
        let length = u32::from_be_bytes(input[..4].try_into().map_err(|_| Error::Invalid)?);
        if length == 0 || usize::try_from(length).map_err(|_| Error::Invalid)? > max_tcp {
            return Err(Error::Invalid);
        }
        // JSON allows leading whitespace; the existing handshake parser validates it all.
        return match input.get(4) {
            None => Ok(None),
            Some(b'{' | b' ' | b'\t' | b'\r' | b'\n') => Ok(Some(Transport::Tcp)),
            _ => Err(Error::Invalid),
        };
    }
    if *first == 0x10 {
        let Some((_, header, total)) = fixed_header(input, max_mqtt)? else {
            return Ok(None);
        };
        // CONNECT variable header plus the (possibly empty) ClientId length.
        if total - header < 12 {
            return Err(Error::Invalid);
        }
        let signature = b"\x00\x04MQTT";
        let available = &input[header..input.len().min(header + signature.len())];
        if !signature.starts_with(available) {
            return Err(Error::Invalid);
        }
        // Read the protocol level too. Level 4 proceeds normally; other levels
        // must reach the authoritative parser to preserve MQTT-3.1.2-2's
        // CONNACK=1 then close. This never accepts an unsupported session.
        return Ok((available.len() == signature.len()
            && input.get(header + signature.len()).is_some())
        .then_some(Transport::Mqtt));
    }
    Err(Error::Invalid)
}

/// Replays every byte consumed by detection, then reads directly from the stream.
pub struct PrefixedStream {
    stream: BoxStream,
    prefix: [u8; MAX_PREFIX],
    position: usize,
    length: usize,
}
impl AsyncRead for PrefixedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position < self.length {
            let count = out.remaining().min(self.length - self.position);
            out.put_slice(&self.prefix[self.position..self.position + count]);
            self.position += count;
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.stream).poll_read(cx, out)
        }
    }
}
impl AsyncWrite for PrefixedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub enum DetectionError {
    Timeout,
    Eof,
    Invalid,
    Resource,
    Io,
}
impl DetectionError {
    pub fn error(&self) -> Error {
        match self {
            Self::Timeout => Error::Timeout,
            Self::Eof | Self::Io => Error::Unavailable,
            Self::Invalid => Error::Invalid,
            Self::Resource => Error::Overloaded,
        }
    }
}

pub async fn classify_device_stream(
    stream: BoxStream,
    max_tcp: usize,
    max_mqtt: usize,
    deadline: Instant,
) -> std::result::Result<(Transport, BoxStream), DetectionError> {
    let mut stream = PrefixedStream {
        stream,
        prefix: [0; MAX_PREFIX],
        position: 0,
        length: 0,
    };
    loop {
        if Instant::now() >= deadline {
            return Err(DetectionError::Timeout);
        }
        match classify_prefix(&stream.prefix[..stream.length], max_tcp, max_mqtt) {
            Ok(Some(transport)) => return Ok((transport, Box::new(stream))),
            Ok(None) => {}
            Err(Error::Overloaded) => return Err(DetectionError::Resource),
            Err(_) => return Err(DetectionError::Invalid),
        }
        if stream.length == MAX_PREFIX {
            return Err(DetectionError::Invalid);
        }
        let read = tokio::time::timeout_at(
            deadline,
            stream.stream.read(&mut stream.prefix[stream.length..]),
        )
        .await
        .map_err(|_| DetectionError::Timeout)?
        .map_err(|_| DetectionError::Io)?;
        if read == 0 {
            return Err(DetectionError::Eof);
        }
        stream.length += read;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    fn mqtt() -> Vec<u8> {
        b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00".to_vec()
    }
    fn tcp() -> Vec<u8> {
        let json = br#" {"credential_id":"device","secret":"key"}"#;
        let mut bytes = (json.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(json);
        bytes
    }
    fn prefix(bytes: &[u8]) -> Result<Option<Transport>> {
        classify_prefix(bytes, 65_536, 65_536)
    }

    #[test]
    fn signatures_lengths_and_adversarial_collisions() {
        for malformed in [
            b"GET ".as_slice(),
            b"POST ",
            b"PATCH ",
            b"GET/",
            b"POST\t",
            b"GETTING ",
            b"get ",
            b"PRI ",
            b"\xff\xaa\xff\x00",
            b"\x10\x80\x80\x80\x80",
            b"\x10\x80\x00",
            b"\x10\x01",
            b"\x10\x0c\x00\x04AMQP\x04",
        ] {
            assert!(prefix(malformed).is_err(), "{malformed:?}");
        }
        for n in [0u32, 65_537, u32::MAX] {
            assert!(prefix(&n.to_be_bytes()).is_err());
        }
        for n in [1u32, 65_536] {
            let mut bytes = n.to_be_bytes().to_vec();
            assert_eq!(prefix(&bytes).unwrap(), None);
            bytes.push(b'{');
            assert_eq!(prefix(&bytes).unwrap(), Some(Transport::Tcp));
        }
        assert!(prefix(b"\0\0\0\x01x").is_err());
        assert_eq!(
            prefix(b"\x10\x0c\x00\x04MQTT\x05").unwrap(),
            Some(Transport::Mqtt)
        );
        assert_eq!(
            prefix(b"\x10\x80\x01\x00\x04MQTT\x04").unwrap(),
            Some(Transport::Mqtt)
        );
        assert!(matches!(
            prefix(b"\x10\xff\xff\x7f"),
            Err(Error::Overloaded)
        ));
        // Even adversarial configuration cannot turn ASCII or MQTT into a TCP length.
        assert!(classify_prefix(b"GET {", usize::MAX, 65_536).is_err());
        assert!(prefix(b"\x10\x00\x00\x01{").is_err());
        for length in [1, 320, 65_536, 1_048_576] {
            assert_eq!(u32::to_be_bytes(length)[0], 0);
        }
    }

    #[tokio::test]
    async fn fragmented_and_coalesced_prefixes_preserve_all_bytes_and_writes() {
        for (protocol, bytes) in [(Transport::Mqtt, mqtt()), (Transport::Tcp, tcp())] {
            for capacity in [1, 4096] {
                let (mut peer, stream) = tokio::io::duplex(capacity);
                let sent = bytes.clone();
                let writer = tokio::spawn(async move {
                    peer.write_all(&sent).await.unwrap();
                    peer.shutdown().await.unwrap();
                    let mut response = Vec::new();
                    peer.read_to_end(&mut response).await.unwrap();
                    assert_eq!(response, b"ack");
                });
                let (actual, mut replay) = classify_device_stream(
                    Box::new(stream),
                    65_536,
                    65_536,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
                assert_eq!(actual, protocol);
                // Includes a zero-size read and smaller-than-prefix buffers.
                assert_eq!(replay.read(&mut []).await.unwrap(), 0);
                let mut received = Vec::new();
                let mut small = [0; 2];
                loop {
                    let n = replay.read(&mut small).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    received.extend_from_slice(&small[..n]);
                }
                assert_eq!(received, bytes);
                replay.write_all(b"ack").await.unwrap();
                replay.flush().await.unwrap();
                replay.shutdown().await.unwrap();
                writer.await.unwrap();
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn eof_slow_prefix_and_expired_ready_bytes() {
        for partial in [b"".as_slice(), b"\x10", b"\x10\x80", b"\x00\x00"] {
            let (mut peer, stream) = tokio::io::duplex(64);
            peer.write_all(partial).await.unwrap();
            peer.shutdown().await.unwrap();
            assert!(matches!(
                classify_device_stream(
                    Box::new(stream),
                    65_536,
                    65_536,
                    Instant::now() + Duration::from_millis(10)
                )
                .await,
                Err(DetectionError::Eof)
            ));
        }
        let (mut peer, stream) = tokio::io::duplex(64);
        peer.write_all(b"\x10").await.unwrap();
        assert!(matches!(
            classify_device_stream(
                Box::new(stream),
                65_536,
                65_536,
                Instant::now() + Duration::from_millis(10)
            )
            .await,
            Err(DetectionError::Timeout)
        ));
        let (mut peer, stream) = tokio::io::duplex(64);
        peer.write_all(b"GET ").await.unwrap();
        assert!(matches!(
            classify_device_stream(Box::new(stream), 65_536, 65_536, Instant::now()).await,
            Err(DetectionError::Timeout)
        ));
    }
}
