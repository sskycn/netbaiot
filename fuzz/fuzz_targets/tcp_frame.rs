#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    use netbaiot_core::{tcp_ack, tcp_frame};
    if data.len() >= 4 {
        if let Ok((kind, length)) = tcp_frame::decode_header(data[..4].try_into().unwrap(), 65536) {
            assert!((1..=65536).contains(&length));
            match kind {
                tcp_frame::FrameKind::Payload => {
                    assert_eq!(tcp_frame::payload_header(length).unwrap(), data[..4]);
                }
                tcp_frame::FrameKind::EventAccepted => {
                    assert_eq!(length, tcp_ack::BODY_LEN);
                    assert_eq!(&data[..4], &tcp_ack::FRAME_HEADER);
                    if let Some(body) = data.get(4..4 + length) {
                        if let Ok(receipt) = tcp_ack::decode(body) {
                            assert_eq!(
                                &tcp_ack::encode(&receipt).unwrap()[..],
                                &data[..4 + length]
                            );
                        }
                    }
                }
            }
        }
    }
    if let Ok(receipt) = netbaiot_core::tcp_ack::decode(data) {
        let encoded = netbaiot_core::tcp_ack::encode(&receipt).unwrap();
        assert_eq!(&encoded[4..], data);
    }
    if data.len() > 65540 {
        return;
    }
    use netbaiot_transports::tcp::TcpFramer;
    let framer = netbaiot_transports::tcp::LengthPrefixFramer { maximum: 65536 };
    let mut input = bytes::BytesMut::new();
    for part in data.chunks(3) {
        if input.len() + part.len() > 65540 {
            break;
        }
        input.extend_from_slice(part);
        loop {
            let before = input.len();
            match framer.decode(&mut input) {
                Ok(Some(frame)) => {
                    assert!(input.len() < before);
                    assert!(frame.len() <= 65536);
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
});
