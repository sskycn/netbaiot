use super::*;
use netbaiot_protocol::*;
use std::{collections::BTreeMap, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn device() -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("tenant").unwrap(),
        product_id: ProductId::new("product").unwrap(),
        device_id: DeviceId::new("device").unwrap(),
    }
}

#[tokio::test]
async fn credentials_are_redacted_and_transport_is_required() {
    let credentials = DeviceCredentials::new("credential", "secret-value").unwrap();
    assert!(!format!("{credentials:?}").contains("secret-value"));
    let result = DeviceClient::builder()
        .device(device())
        .credentials(credentials)
        .connect()
        .await;
    assert!(matches!(
        result,
        Err(DeviceSdkError::InvalidConfiguration(_))
    ));
}

#[test]
fn canonical_topics_are_identity_scoped() {
    assert_eq!(topic(&device(), "up"), "v1/t/tenant/p/product/d/device/up");
}

#[test]
fn reconnect_backoff_is_jittered_and_bounded() {
    let policy = DeviceReconnectPolicy::default();
    let mut previous_ceiling = policy.initial_backoff;
    for attempt in 1..100 {
        let delay = device_reconnect_delay(policy, attempt, 7);
        assert!(!delay.is_zero());
        assert!(delay <= policy.maximum_backoff);
        if attempt <= 6 {
            assert!(delay <= previous_ceiling);
            previous_ceiling = previous_ceiling
                .saturating_mul(2)
                .min(policy.maximum_backoff);
        }
    }
    assert_ne!(
        device_reconnect_delay(policy, 4, 7),
        device_reconnect_delay(policy, 4, 8)
    );
}

#[test]
fn terminal_connack_codes_do_not_retry() {
    assert!(terminal_connect_error(3, MqttProtocolVersion::V311).is_none());
    assert!(matches!(
        terminal_connect_error(4, MqttProtocolVersion::V311),
        Some(DeviceSdkError::Unauthenticated)
    ));
    assert!(matches!(
        terminal_connect_error(2, MqttProtocolVersion::V311),
        Some(DeviceSdkError::InvalidConfiguration(_))
    ));
}

async fn read_mqtt_packet(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await.unwrap();
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let byte = stream.read_u8().await.unwrap();
        remaining += usize::from(byte & 0x7f) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let mut body = vec![0; remaining];
    stream.read_exact(&mut body).await.unwrap();
    let mut packet = vec![first[0]];
    packet.extend_from_slice(&body);
    packet
}

#[tokio::test]
async fn full_command_buffer_disconnects_without_acknowledging_overflow() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (start, ready) = tokio::sync::oneshot::channel();
        let command = DeviceCommand {
            command_id: CommandId::generate(),
            device: device(),
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "test".into(),
                arguments: BTreeMap::new(),
            },
        };
        let expected = command.command_id;
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(read_mqtt_packet(&mut socket).await[0], 0x10);
            socket.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let subscribe = read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x90, 3, subscribe[1], subscribe[2], 1])
                .await
                .unwrap();
            ready.await.unwrap();
            let topic = topic(&device(), "down");
            let payload = serde_json::to_vec(&command).unwrap();
            for id in [1u16, 2] {
                let mut body = Vec::new();
                body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                body.extend_from_slice(topic.as_bytes());
                body.extend_from_slice(&id.to_be_bytes());
                body.extend_from_slice(&payload);
                let mut packet = vec![0x32];
                let mut remaining = body.len();
                loop {
                    let mut byte = (remaining % 128) as u8;
                    remaining /= 128;
                    if remaining != 0 {
                        byte |= 0x80;
                    }
                    packet.push(byte);
                    if remaining == 0 {
                        break;
                    }
                }
                packet.extend_from_slice(&body);
                socket.write_all(&packet).await.unwrap();
                let reply = read_mqtt_packet(&mut socket).await;
                if id == 1 {
                    assert_eq!(reply, [0x40, 0, 1]);
                } else {
                    assert_eq!(reply, [0xe0]); // Overflow must not PUBACK packet 2.
                }
            }
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .command_buffer_items(1)
            .connect()
            .await
            .unwrap();
        let oversized = BTreeMap::from([(
            "padding".into(),
            Scalar::Text("x".repeat(DEFAULT_MAX_PAYLOAD_BYTES)),
        )]);
        assert!(matches!(
            client.publish_telemetry(oversized).await,
            Err(DeviceSdkError::Overloaded)
        ));
        start.send(()).unwrap();
        broker.await.unwrap();
        assert_eq!(client.metrics().commands_received, 1);
        let mut commands = client.commands().unwrap();
        assert_eq!(commands.recv().await.unwrap().command_id, expected);
        assert!(commands.receiver.try_recv().is_err());
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn command_connection_waits_for_successful_suback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let broker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let connect = read_mqtt_packet(&mut socket).await;
        assert_eq!(connect[0] & 0xf0, 0x10);
        socket.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();
        let subscribe = read_mqtt_packet(&mut socket).await;
        assert_eq!(subscribe[0], 0x82);
        let packet_id = &subscribe[1..3];
        socket
            .write_all(&[0x90, 0x03, packet_id[0], packet_id[1], 0x80])
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let result = DeviceClient::builder()
        .device(device())
        .credentials(DeviceCredentials::new("credential", "secret").unwrap())
        .mqtt_endpoint(format!("mqtt://{address}"))
        .mqtt_connect_timeout(Duration::from_secs(1))
        .connect()
        .await;
    assert!(matches!(result, Err(DeviceSdkError::Forbidden)));
    broker.await.unwrap();
}

#[tokio::test]
async fn v5_sdk_connects_subscribes_and_publishes_with_expiry() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut socket).await;
            assert_eq!(connect[7], 5);
            assert!(connect.windows(5).any(|part| part == [0x11, 0, 0, 0, 60]));
            socket.write_all(&[0x20, 0x03, 0, 0, 0]).await.unwrap();
            let subscribe = read_mqtt_packet(&mut socket).await;
            assert_eq!(subscribe[0], 0x82);
            socket
                .write_all(&[0x90, 0x04, subscribe[1], subscribe[2], 0, 1])
                .await
                .unwrap();
            let publish = read_mqtt_packet(&mut socket).await;
            assert_eq!(publish[0] & 0xf0, 0x30);
            let topic_end = 3 + usize::from(u16::from_be_bytes([publish[1], publish[2]]));
            assert_eq!(publish[topic_end + 2], 5);
            assert_eq!(&publish[topic_end + 3..topic_end + 8], &[0x02, 0, 0, 0, 9]);
            socket
                .write_all(&[0x40, 0x02, publish[topic_end], publish[topic_end + 1]])
                .await
                .unwrap();
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .session_expiry_interval(60)
            .message_expiry_interval(Some(9))
            .connect()
            .await
            .unwrap();
        client.publish_telemetry(BTreeMap::new()).await.unwrap();
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v311_reconnect_replays_qos1_with_same_identifier_and_dup() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut first).await;
            first.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut first).await;
            first
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let sent = read_mqtt_packet(&mut first).await;
            assert_eq!(sent[0] & 8, 0);
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut second).await;
            second.write_all(&[0x20, 2, 1, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let replay = read_mqtt_packet(&mut second).await;
            assert_eq!(replay[0] & 8, 8);
            assert_eq!(&replay[1..], &sent[1..]);
            let topic_len = usize::from(u16::from_be_bytes([replay[1], replay[2]]));
            let id_at = 3 + topic_len;
            second
                .write_all(&[0x40, 2, replay[id_at], replay[id_at + 1]])
                .await
                .unwrap();
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .connect()
            .await
            .unwrap();
        let mut receipts = client.publish_receipts();
        client
            .publish(
                DeviceUplink::new(
                    SourceMessageId::new("same-message").unwrap(),
                    DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
                ),
                PublishQos::AtLeastOnce,
            )
            .await
            .unwrap();
        let receipt = receipts.recv().await.unwrap();
        assert_eq!(
            receipt.source_message_id,
            SourceMessageId::new("same-message").unwrap()
        );
        assert_eq!(receipt.result, PublishResult::Puback);
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v5_receive_maximum_one_and_negative_puback_are_observed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x20, 6, 0, 0, 3, 0x21, 0, 1])
                .await
                .unwrap();
            let sub = read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            let first = read_mqtt_packet(&mut socket).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_mqtt_packet(&mut socket))
                    .await
                    .is_err()
            );
            let topic_len = usize::from(u16::from_be_bytes([first[1], first[2]]));
            let at = 3 + topic_len;
            socket
                .write_all(&[0x40, 4, first[at], first[at + 1], 0x97, 0])
                .await
                .unwrap();
            let second = read_mqtt_packet(&mut socket).await;
            let topic_len = usize::from(u16::from_be_bytes([second[1], second[2]]));
            let at = 3 + topic_len;
            socket
                .write_all(&[0x40, 2, second[at], second[at + 1]])
                .await
                .unwrap();
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        let mut receipts = client.publish_receipts();
        for sequence in 1..=2 {
            client
                .publish(
                    DeviceUplink::new(
                        SourceMessageId::new(format!("negative-{sequence}")).unwrap(),
                        DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
                    ),
                    PublishQos::AtLeastOnce,
                )
                .await
                .unwrap();
        }
        assert_eq!(
            receipts.recv().await.unwrap().result,
            PublishResult::Rejected(0x97)
        );
        assert_eq!(receipts.recv().await.unwrap().result, PublishResult::Puback);
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v5_reconnect_replay_obeys_new_receive_maximum() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut first).await;
            first.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut first).await;
            first
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            let original_a = read_mqtt_packet(&mut first).await;
            let original_b = read_mqtt_packet(&mut first).await;
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x20, 6, 1, 0, 3, 0x21, 0, 1])
                .await
                .unwrap();
            let sub = read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            let replay_a = read_mqtt_packet(&mut second).await;
            assert_eq!(replay_a[0], 0x3a);
            assert_eq!(&replay_a[1..], &original_a[1..]);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_mqtt_packet(&mut second))
                    .await
                    .is_err()
            );
            let topic_len = usize::from(u16::from_be_bytes([replay_a[1], replay_a[2]]));
            let at = 3 + topic_len;
            second
                .write_all(&[0x40, 2, replay_a[at], replay_a[at + 1]])
                .await
                .unwrap();
            let replay_b = read_mqtt_packet(&mut second).await;
            assert_eq!(replay_b[0], 0x3a);
            assert_eq!(&replay_b[1..], &original_b[1..]);
            let topic_len = usize::from(u16::from_be_bytes([replay_b[1], replay_b[2]]));
            let at = 3 + topic_len;
            second
                .write_all(&[0x40, 2, replay_b[at], replay_b[at + 1]])
                .await
                .unwrap();
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        let mut receipts = client.publish_receipts();
        for sequence in 1..=2 {
            client
                .publish(
                    DeviceUplink::new(
                        SourceMessageId::new(format!("window-{sequence}")).unwrap(),
                        DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
                    ),
                    PublishQos::AtLeastOnce,
                )
                .await
                .unwrap();
        }
        for _ in 0..2 {
            assert_eq!(receipts.recv().await.unwrap().result, PublishResult::Puback);
        }
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn qos0_written_receipt_has_no_packet_id_or_replay() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut first).await;
            first.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut first).await;
            first
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let published = read_mqtt_packet(&mut first).await;
            assert_eq!(published[0], 0x30);
            let topic_len = usize::from(u16::from_be_bytes([published[1], published[2]]));
            assert_eq!(published[3 + topic_len], b'{');
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut second).await;
            second.write_all(&[0x20, 2, 1, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_mqtt_packet(&mut second))
                    .await
                    .is_err()
            );
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .connect()
            .await
            .unwrap();
        let mut receipts = client.publish_receipts();
        client
            .publish(
                DeviceUplink::new(
                    SourceMessageId::new("qos0-no-replay").unwrap(),
                    DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
                ),
                PublishQos::AtMostOnce,
            )
            .await
            .unwrap();
        assert_eq!(
            receipts.recv().await.unwrap().result,
            PublishResult::Written
        );
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn final_client_drop_closes_owned_connection() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut socket).await;
            socket.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .connect()
            .await
            .unwrap();
        drop(client);
        broker.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handshake_deadline_covers_missing_connack_and_suback() {
    for missing_connack in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut socket).await;
            if !missing_connack {
                socket.write_all(&[0x20, 2, 0, 0]).await.unwrap();
                read_mqtt_packet(&mut socket).await;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        });
        let result = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .mqtt_connect_timeout(Duration::from_millis(100))
            .connect()
            .await;
        assert!(matches!(result, Err(DeviceSdkError::Timeout)));
        broker.await.unwrap();
    }
}

#[tokio::test]
async fn queued_command_before_suback_is_accepted_before_puback() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let command = DeviceCommand {
            command_id: CommandId::generate(),
            device: device(),
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "test".into(),
                arguments: BTreeMap::new(),
            },
        };
        let expected = command.command_id;
        let (done, hold) = tokio::sync::oneshot::channel::<()>();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut socket).await;
            socket.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut socket).await;
            let topic = topic(&device(), "down");
            let payload = serde_json::to_vec(&command).unwrap();
            let mut body = Vec::new();
            body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            body.extend_from_slice(topic.as_bytes());
            body.extend_from_slice(&9u16.to_be_bytes());
            body.extend_from_slice(&payload);
            let mut packet = vec![0x32];
            netbaiot_mqtt_wire::put_variable(body.len(), &mut packet).unwrap();
            packet.extend_from_slice(&body);
            socket.write_all(&packet).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), read_mqtt_packet(&mut socket))
                    .await
                    .expect("early command PUBACK missing"),
                [0x40, 0, 9]
            );
            socket
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let _ = hold.await;
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .connect()
            .await
            .unwrap();
        let mut commands = client.commands().unwrap();
        assert_eq!(commands.recv().await.unwrap().command_id, expected);
        done.send(()).unwrap();
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v5_session_loss_releases_old_inflight_without_republishing() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut first).await;
            first.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut first).await;
            first
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            read_mqtt_packet(&mut first).await;
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut second).await;
            second.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read_mqtt_packet(&mut second))
                    .await
                    .is_err()
            );
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        let mut receipts = client.publish_receipts();
        client
            .publish(
                DeviceUplink::new(
                    SourceMessageId::new("lost-session").unwrap(),
                    DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
                ),
                PublishQos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert_eq!(
            receipts.recv().await.unwrap().result,
            PublishResult::SessionLost
        );
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v5_unknown_old_session_reconnects_with_clean_start() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (done, hold) = tokio::sync::oneshot::channel::<()>();
        let broker = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut first).await;
            assert_eq!(connect[8] & 2, 0);
            first.write_all(&[0x20, 3, 1, 0, 0]).await.unwrap();
            let (mut second, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut second).await;
            assert_eq!(connect[8] & 2, 2);
            second.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut second).await;
            second
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            let _ = hold.await;
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        done.send(()).unwrap();
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v311_unknown_old_session_is_cleared_before_persistent_connect() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut old, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut old).await;
            assert_eq!(connect[8] & 2, 0);
            old.write_all(&[0x20, 2, 1, 0]).await.unwrap();

            let (mut clear, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut clear).await;
            assert_eq!(connect[8] & 2, 2);
            clear.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut clear).await;
            clear
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            assert_eq!(read_mqtt_packet(&mut clear).await, [0xe0]);

            let (mut fresh, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut fresh).await;
            assert_eq!(connect[8] & 2, 0);
            fresh.write_all(&[0x20, 2, 0, 0]).await.unwrap();
            let sub = read_mqtt_packet(&mut fresh).await;
            fresh
                .write_all(&[0x90, 3, sub[1], sub[2], 1])
                .await
                .unwrap();
            let _ = read_mqtt_packet(&mut fresh).await;
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .connect()
            .await
            .unwrap();
        assert!(client.mqtt_connected());
        client.shutdown();
        broker.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn v5_server_keep_alive_controls_ping_without_command_polling() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x20, 6, 0, 0, 3, 0x13, 0, 1])
                .await
                .unwrap();
            let sub = read_mqtt_packet(&mut socket).await;
            socket
                .write_all(&[0x90, 4, sub[1], sub[2], 0, 1])
                .await
                .unwrap();
            assert_eq!(read_mqtt_packet(&mut socket).await, [0xc0]);
            socket.write_all(&[0xd0, 0]).await.unwrap();
            assert_eq!(read_mqtt_packet(&mut socket).await, [0xc0]);
            socket.write_all(&[0xd0, 0]).await.unwrap();
        });
        let client = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .protocol_version(MqttProtocolVersion::V5)
            .connect()
            .await
            .unwrap();
        broker.await.unwrap();
        client.shutdown();
    })
    .await
    .unwrap();
}
