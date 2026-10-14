use super::*;

fn metered(limits: Limits, maximum: usize) -> (Reader, ConnectionReadBudget, ConnectionLease) {
    let connections = Connections::new(Arc::new(limits), Arc::new(Metrics::default()));
    let mut lease = connections
        .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
        .unwrap();
    let tracking = lease.read_budget();
    let reader = Reader::with_budget(maximum, 30_000, lease.read_budget());
    (reader, tracking, lease)
}
fn key() -> netbaiot_core::DeviceKey {
    netbaiot_core::DeviceKey {
        tenant_id: netbaiot_core::TenantId::new("t").unwrap(),
        product_id: netbaiot_core::ProductId::new("p").unwrap(),
        device_id: netbaiot_core::DeviceId::new("d").unwrap(),
    }
}

#[tokio::test]
async fn failed_growth_preserves_every_prefix_and_shared_alias() {
    for prefix in [1, 4, 8, 9, 64] {
        for tenant_pressure in [false, true] {
            let fixed = Limits::default().connection_memory_reservation;
            let limit = fixed + prefix;
            let limits = Limits {
                global_connection_logical_bytes: if tenant_pressure { fixed + 256 } else { limit },
                max_connection_bytes_per_tenant: if tenant_pressure { limit } else { fixed + 256 },
                max_read_buffer_per_connection: 128,
                ..Limits::default()
            };
            let (mut reader, tracking, mut lease) = metered(limits, 128);
            let input = vec![7; prefix];
            let mut source = input.as_slice();
            let idle = Instant::now() + Duration::from_secs(10);
            while reader.buffer.len() < prefix {
                reader
                    .read_more_to(&mut source, idle, prefix)
                    .await
                    .unwrap();
            }
            lease.authenticate(&key()).unwrap();
            reader.authenticate(&key()).unwrap();
            let capacity = reader.buffer.capacity();
            assert!(matches!(
                reader
                    .read_more_to(&mut tokio::io::empty(), idle, 128)
                    .await,
                Err(Error::Overloaded)
            ));
            assert_eq!(&reader.buffer[..], &input);
            assert_eq!(reader.buffer.capacity(), capacity);
            assert_eq!(tracking.capacity().unwrap(), capacity);
            let alias = reader.take_frame(prefix).unwrap().slice(..1);
            reader.consumed();
            drop((reader, lease));
            assert_eq!(tracking.capacity().unwrap(), capacity);
            assert_eq!(&alias[..], &[7]);
            drop(alias);
            assert_eq!(tracking.capacity().unwrap(), 0);
        }
    }
}

#[tokio::test]
async fn inline_growth_preserves_exact_global_and_tenant_final_fit() {
    for prefix in [1, 4, 8] {
        let fixed = Limits::default().connection_memory_reservation;
        let limits = Limits {
            global_connection_logical_bytes: fixed + 128,
            max_connection_bytes_per_tenant: fixed + 128,
            max_read_buffer_per_connection: 128,
            ..Limits::default()
        };
        let (mut reader, tracking, mut lease) = metered(limits, 128);
        let source = vec![7; 128];
        let mut input = source.as_slice();
        let idle = Instant::now() + Duration::from_secs(10);
        reader.read_more_to(&mut input, idle, prefix).await.unwrap();
        lease.authenticate(&key()).unwrap();
        reader.authenticate(&key()).unwrap();
        reader.read_more_to(&mut input, idle, 128).await.unwrap();
        assert_eq!(&reader.buffer[..], &source);
        assert_eq!(tracking.capacity().unwrap(), 128);
        drop((reader, lease));
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
}

#[tokio::test]
async fn shared_previous_frame_retains_its_full_allocation_until_last_slice_drops() {
    let limits = Limits {
        max_read_buffer_per_connection: 64,
        ..Limits::default()
    };
    let (mut reader, tracking, lease) = metered(limits, 64);
    let mut source = &[7; 16][..];
    let idle = Instant::now() + Duration::from_secs(10);
    reader.read_more_to(&mut source, idle, 16).await.unwrap();
    reader.read_more_to(&mut source, idle, 16).await.unwrap();
    let alias = reader.take_frame(4).unwrap().slice(..1);
    reader.consumed();
    assert!(matches!(
        reader.read_more_to(&mut tokio::io::empty(), idle, 64).await,
        Err(Error::Overloaded)
    ));
    assert_eq!(reader.buffer.len(), 12);
    assert_eq!(tracking.capacity().unwrap(), 16);
    drop(alias);
    let mut source = &[7; 52][..];
    reader.read_more_to(&mut source, idle, 64).await.unwrap();
    assert_eq!(tracking.capacity().unwrap(), 64);
    drop((reader, lease));
    assert_eq!(tracking.capacity().unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn grown_read_cancellation_timeout_and_disconnect_release_all_capacity() {
    for prefix in [1, 8, 9, 64] {
        let limits = Limits {
            max_read_buffer_per_connection: 128,
            ..Limits::default()
        };
        let (mut reader, tracking, lease) = metered(limits, 128);
        let idle = Instant::now() + Duration::from_secs(60);
        let source = vec![7; prefix];
        let mut input = source.as_slice();
        while reader.buffer.len() < prefix {
            reader.read_more_to(&mut input, idle, prefix).await.unwrap();
        }
        let (peer, mut stream) = tokio::io::duplex(128);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(1),
                reader.read_more_to(&mut stream, idle, 128)
            )
            .await
            .is_err()
        );
        assert_eq!(&reader.buffer[..], &source);
        assert_eq!(tracking.capacity().unwrap(), 128);
        drop(peer);
        assert!(matches!(
            reader.read_more_to(&mut stream, idle, 128).await,
            Err(Error::Unavailable)
        ));
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(matches!(
            reader.read_more_to(&mut stream, idle, 128).await,
            Err(Error::Timeout)
        ));
        drop((reader, lease));
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn suspended_read_rejects_ready_bytes_after_the_packet_deadline() {
    use std::{future::Future, task::Poll};
    let (mut reader, tracking, lease) = metered(Limits::default(), 8);
    reader.read_timeout = Duration::from_millis(5);
    let idle = Instant::now() + Duration::from_secs(1);
    reader.read_more_to(&mut &[7][..], idle, 1).await.unwrap();
    let (mut peer, mut stream) = tokio::io::duplex(8);
    let mut read = Box::pin(reader.read_more_to(&mut stream, idle, 2));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::advance(Duration::from_millis(6)).await;
    peer.write_all(&[8]).await.unwrap();
    assert!(matches!(read.await, Err(Error::Timeout)));
    assert_eq!(&reader.buffer[..1], &[7]);
    assert_eq!(tracking.capacity().unwrap(), 2);
    drop((reader, lease));
    assert_eq!(tracking.capacity().unwrap(), 0);
}

#[tokio::test]
async fn maximum_mqtt311_mqtt5_and_tcp_frames_survive_preauthentication_reads() {
    use crate::mqtt::codec::{v5, v311};
    use crate::tcp::{LengthPrefixFramer, TcpFramer};
    let limits = Limits::default();
    let payload311 = vec![7; limits.max_mqtt_packet_size - 7];
    let wire311 = v311::publish("a", &payload311, 0, None, false, false, &limits).unwrap();
    let payload5 = vec![7; limits.max_mqtt_packet_size - 8];
    let wire5 = v5::publish(
        v5::OutboundPublish {
            topic: "a",
            payload: &payload5,
            qos: 0,
            packet_id: None,
            retain: false,
            dup: false,
            properties: &v5::Properties::default(),
        },
        limits.max_mqtt_packet_size,
    )
    .unwrap();
    let framer = LengthPrefixFramer {
        maximum: limits.max_tcp_frame_size,
    };
    let payload_tcp = vec![7; limits.max_tcp_frame_size];
    let wire_tcp = framer.encode(&payload_tcp).unwrap();
    for (protocol, wire) in [(0, wire311), (1, wire5), (2, wire_tcp)] {
        let maximum = if protocol == 2 {
            limits.max_tcp_frame_size + 4
        } else {
            limits.max_mqtt_packet_size
        };
        assert_eq!(wire.len(), maximum);
        for prefix in [1, 4, 8, 9, 64, 4096] {
            let (mut reader, tracking, mut lease) = metered(limits.clone(), maximum);
            let mut input = wire.as_slice();
            let idle = Instant::now() + Duration::from_secs(10);
            while reader.buffer.len() < prefix {
                reader.read_more_to(&mut input, idle, prefix).await.unwrap();
            }
            lease.authenticate(&key()).unwrap();
            reader.authenticate(&key()).unwrap();
            reader
                .read_more_to(&mut input, idle, maximum)
                .await
                .unwrap();
            match protocol {
                0 => assert!(
                    matches!(v311::decode(&mut reader.buffer, &limits).unwrap(), Some(v311::Packet::Publish { payload, .. }) if payload == payload311)
                ),
                1 => assert!(
                    matches!(v5::decode(&mut reader.buffer, &limits).unwrap(), Some(v5::Packet::Publish { payload, .. }) if payload == payload5)
                ),
                _ => {
                    let frame = framer.decode(&mut reader.buffer).unwrap().unwrap();
                    let frame = reader.own_frame(frame);
                    assert_eq!(&frame[..], &payload_tcp);
                    reader.consumed();
                    assert_eq!(tracking.capacity().unwrap(), maximum);
                    drop(frame);
                }
            }
            reader.consumed();
            drop((reader, lease));
            assert_eq!(tracking.capacity().unwrap(), 0);
        }
    }
}

#[tokio::test]
async fn maximum_legal_frame_grows_from_every_buffered_prefix() {
    for prefix in [1, 4, 8, 9, 64, 4096] {
        let limits = Arc::new(Limits::default());
        let maximum = limits.max_read_buffer_per_connection;
        let connections = Connections::new(limits, Arc::new(Metrics::default()));
        let mut lease = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
            .unwrap();
        let tracking = lease.read_budget();
        let mut reader = Reader::with_budget(maximum, 30_000, lease.read_budget());
        let source = vec![7; maximum];
        let mut input = source.as_slice();
        let idle = Instant::now() + Duration::from_secs(10);
        while reader.buffer.len() < prefix {
            reader.read_more_to(&mut input, idle, prefix).await.unwrap();
        }
        let result = reader.read_more_to(&mut input, idle, maximum).await;
        assert!(
            result.is_ok(),
            "legal maximum after {prefix} buffered bytes: {result:?}"
        );
        assert_eq!(reader.buffer.len(), maximum);
        assert_eq!(tracking.capacity().unwrap(), maximum);
        assert_eq!(&reader.buffer[..], &source);
        let frame = reader.take_frame(maximum).unwrap();
        reader.consumed();
        drop((frame, reader, lease));
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
}

#[tokio::test]
async fn coalesced_tcp_frame_tails_grow_into_a_legal_maximum_frame() {
    use crate::tcp::{LengthPrefixFramer, TcpFramer};
    let limits = Limits::default();
    let maximum = limits.max_tcp_frame_size + 4;
    let framer = LengthPrefixFramer {
        maximum: limits.max_tcp_frame_size,
    };
    let first = framer.encode(b"tiny").unwrap();
    let body = vec![7; limits.max_tcp_frame_size];
    let second = framer.encode(&body).unwrap();
    for prefix in [1, 4, 8, 9, 64, 4096] {
        let mut input = first.to_vec();
        input.extend_from_slice(&second);
        let mut source = input.as_slice();
        let (mut reader, tracking, lease) = metered(limits.clone(), maximum);
        let idle = Instant::now() + Duration::from_secs(10);
        while reader.buffer.len() < first.len() + prefix {
            reader
                .read_more_to(&mut source, idle, first.len() + prefix)
                .await
                .unwrap();
        }
        let frame = framer.decode(&mut reader.buffer).unwrap().unwrap();
        let frame = reader.own_frame(frame);
        assert_eq!(&frame[..], b"tiny");
        drop(frame);
        reader.consumed();
        assert_eq!(&reader.buffer[..], &second[..prefix]);
        reader
            .read_more_to(&mut source, idle, maximum)
            .await
            .unwrap();
        let frame = framer.decode(&mut reader.buffer).unwrap().unwrap();
        let frame = reader.own_frame(frame);
        assert_eq!(&frame[..], &body);
        reader.consumed();
        drop((reader, lease));
        assert_eq!(tracking.capacity().unwrap(), maximum);
        drop(frame);
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
}

#[tokio::test]
async fn short_tail_grows_below_the_previous_full_allocation_capacity() {
    let limits = Arc::new(Limits {
        max_read_buffer_per_connection: 64,
        ..Limits::default()
    });
    let connections = Connections::new(limits, Arc::new(Metrics::default()));
    let mut lease = connections
        .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
        .unwrap();
    let tracking = lease.read_budget();
    let mut reader = Reader::with_budget(64, 30_000, lease.read_budget());
    let mut input = &[7; 10][..];
    let idle = Instant::now() + Duration::from_secs(10);
    reader.read_more_to(&mut input, idle, 64).await.unwrap();
    reader.read_more_to(&mut input, idle, 64).await.unwrap();
    drop(reader.take_frame(2).unwrap());
    reader.consumed();
    assert_eq!(reader.buffer.len(), 8);
    assert_eq!(reader.buffer.capacity(), 62);
    assert_eq!(tracking.capacity().unwrap(), 64);
    let mut input = &[7; 55][..];
    reader.read_more_to(&mut input, idle, 63).await.unwrap();
    assert_eq!(reader.buffer.len(), 63);
    assert_eq!(tracking.capacity().unwrap(), reader.buffer.capacity());
    drop((reader, lease));
    assert_eq!(tracking.capacity().unwrap(), 0);
}
