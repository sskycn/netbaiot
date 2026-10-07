use super::*;

#[test]
fn shared_payload_preserves_current_compact_recovery_bytes() {
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/shared/up".into(),
        payload: vec![0, 1, 255].into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let mut encoded = Vec::new();
    encode_message(&mut encoded, &message).unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&(message.topic.len() as u16).to_be_bytes());
    expected.extend_from_slice(message.topic.as_bytes());
    expected.extend_from_slice(&3u32.to_be_bytes());
    expected.extend_from_slice(&[0, 1, 255, 1, 0, 2]);
    expected.extend_from_slice(&(-1i64).to_be_bytes());
    expected.extend_from_slice(&[0, 0, 0, 0, 0]);
    assert_eq!(encoded, expected);
    let mut reader = RecordReader::new(&encoded);
    assert_eq!(
        decode_message(&mut reader, &Limits::default()).unwrap(),
        message
    );
    reader.finish().unwrap();
}

#[tokio::test]
async fn mqtt_temporary_file_budget_blocks_commit_without_replacing_snapshot() {
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-temp-budget-{}",
        uuid::Uuid::new_v4()
    ));
    let broker = MqttBroker::new(Arc::new(Limits {
        spool_max_records: 1,
        ..Limits::default()
    }));
    let path = broker.commit_to(&directory).await.unwrap();
    let image = fs::read(&path).unwrap();
    for index in 0..17 {
        fs::write(directory.join(format!("abandoned-{index}.tmp")), b"").unwrap();
    }
    let result = broker.commit_to(&directory).await;
    assert!(matches!(result, Err(Error::Overloaded)));
    assert_eq!(fs::read(&path).unwrap(), image);
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 18);
    for index in 0..17 {
        fs::remove_file(directory.join(format!("abandoned-{index}.tmp"))).unwrap();
    }
    broker.commit_to(&directory).await.unwrap();
    assert!(broker.recover_from(&directory).await.unwrap());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn mqtt_recovery_storage_commit_replace_retry_and_io_errors() {
    let directory =
        std::env::temp_dir().join(format!("netbaiot-mqtt-storage-{}", uuid::Uuid::new_v4()));
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    assert!(!broker.recover_from(&directory).await.unwrap());
    let path = broker.commit_to(&directory).await.unwrap();
    let first = fs::read(&path).unwrap();
    broker.commit_to(&directory).await.unwrap();
    assert!(broker.recover_from(&directory).await.unwrap());
    // The old committed bytes survive any failure before replacement.
    let small = MqttBroker::new(Arc::new(Limits {
        mqtt_recovery_max_bytes: 1,
        ..(*limits).clone()
    }));
    assert!(small.commit_to(&directory).await.is_err());
    assert_eq!(fs::read(&path).unwrap(), first);
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    broker.commit_to(&directory).await.unwrap();
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::PermissionDenied.into())
        }
    }
    assert!(matches!(
        read_storage_snapshot(Broken, first.len(), first.len() + 1, &limits),
        Err(Error::Storage)
    ));
    let mut extra = first.clone();
    extra.push(42);
    assert!(matches!(
        read_storage_snapshot(Cursor::new(extra), first.len(), first.len() + 1, &limits),
        Err(Error::Invalid)
    ));
    #[cfg(unix)]
    {
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(directory.join("missing"), &path).unwrap();
        assert!(matches!(
            broker.recover_from(&directory).await,
            Err(Error::Storage)
        ));
    }
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn recovery_keeps_publish_properties_and_byte_accounting() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let auth = auth("a");
    let topic = "v1/t/t/p/p/d/a/up";
    let mut attachment = broker
        .attach_v5(&auth, "metadata".into(), false, 60, u16::MAX)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    attachment.detach().unwrap();
    let message = BrokerMessage {
        topic: topic.into(),
        payload: b"value".to_vec().into(),
        qos: 1,
        retain: true,
        properties: (PublishProperties {
            payload_format: Some(1),
            expires_at_ms: Some(now_ms() + 30_000),
            content_type: Some("text/plain".into()),
            response_topic: Some(topic.into()),
            correlation_data: Some(vec![0, 1, 2]),
            user_properties: vec![("key".into(), "value".into())],
        })
        .into(),
    };
    broker.route(&auth.device_key, message.clone()).unwrap();
    let original = broker.usage().unwrap();
    assert!(original.4 >= message.bytes());
    let directory =
        std::env::temp_dir().join(format!("netbaiot-mqtt-v4-props-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&directory).await.unwrap();
    let restored = MqttBroker::new(limits);
    restored.recover_from(&directory).await.unwrap();
    assert_eq!(restored.usage().unwrap(), original);
    {
        let state = restored.state.lock().unwrap();
        assert_eq!(
            state.retained.get(topic).unwrap().message.properties,
            message.properties
        );
        assert_eq!(
            state
                .sessions
                .values()
                .next()
                .unwrap()
                .offline
                .front()
                .unwrap()
                .properties,
            message.properties
        );
    }
    fs::remove_dir_all(&directory).unwrap();
}

#[tokio::test]
async fn v5_message_expiry_cleans_offline_retained_and_recovery_state() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let auth = auth("a");
    let topic = "v1/t/t/p/p/d/a/up";
    let mut attachment = broker
        .attach_v5(&auth, "expiry".into(), false, 60, u16::MAX)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    attachment.detach().unwrap();
    broker
        .route(
            &auth.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"value".to_vec().into(),
                qos: 1,
                retain: true,
                properties: (PublishProperties {
                    expires_at_ms: Some(now_ms() + 30_000),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    assert_eq!(broker.usage().unwrap().3, 1);
    {
        let mut state = broker.state.lock().unwrap();
        state
            .retained
            .get_mut(topic)
            .unwrap()
            .message
            .properties
            .expires_at_ms = Some(now_ms() - 1);
        state
            .retained_expiry
            .update(topic.to_owned(), Some(now_ms() - 1));
        state
            .sessions
            .get_mut(&attachment.key)
            .unwrap()
            .offline
            .front_mut()
            .unwrap()
            .properties
            .expires_at_ms = Some(now_ms() - 1);
        sync_session_usage(&mut state, &attachment.key).unwrap();
    }
    let directory =
        std::env::temp_dir().join(format!("netbaiot-mqtt-v4-expiry-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&directory).await.unwrap();
    let restored = MqttBroker::new(limits);
    restored.recover_from(&directory).await.unwrap();
    assert_eq!(restored.usage().unwrap().3, 0);
    assert_eq!(restored.usage().unwrap().4, 0);
    assert_eq!(restored.state.lock().unwrap().offline_count, 0);
    fs::remove_dir_all(&directory).unwrap();
    broker.tick().unwrap();
    assert_eq!(broker.usage().unwrap().3, 0);
    assert_eq!(broker.state.lock().unwrap().offline_count, 0);
}

#[test]
fn retained_qos2_reserved_state_round_trips_recovery() {
    let limits = Arc::new(Limits {
        max_retained_messages: 2,
        max_retained_messages_per_tenant: 2,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits.clone());
    let device = auth("retain-recover");
    let a = "v1/t/t/p/p/d/retain-recover/up";
    let b = "v1/t/t/p/p/d/retain-recover/down_ack";
    let message = |topic: &str, payload: &[u8], qos| BrokerMessage {
        topic: topic.into(),
        payload: bytes::Bytes::copy_from_slice(payload),
        qos,
        retain: true,
        properties: Default::default(),
    };
    broker
        .route(&device.device_key, message(a, b"old", 1))
        .unwrap();
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            11,
            message(a, b"replacement", 2),
        )
        .unwrap();
    broker
        .route(&device.device_key, message(a, b"", 0))
        .unwrap();
    broker
        .route(&device.device_key, message(b, b"competitor", 1))
        .unwrap();
    let snapshot = broker.snapshot().unwrap();
    attachment.detach().unwrap();
    let recovered = MqttBroker::new(limits);
    recovered.restore(snapshot).unwrap();
    assert_eq!(recovered.state.lock().unwrap().retained_reserved_count, 1);
    let mut resumed = recovered.attach(&device, "client".into(), false).unwrap();
    assert!(resumed.session_present);
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = recovered
        .begin_inbound_qos2_delivery(&resumed.key, resumed.generation, 11)
        .unwrap()
    else {
        panic!("restored transaction must complete");
    };
    recovered
        .finish_inbound_qos2_delivery(&resumed.key, session_incarnation, 11, operation_id)
        .unwrap();
    recovered
        .route_inbound_qos2(
            &resumed.key,
            session_incarnation,
            11,
            operation_id,
            &device.device_key,
        )
        .unwrap();
    assert!(recovered.has_retained_topic(a).unwrap());
    assert!(recovered.has_retained_topic(b).unwrap());
    resumed.detach().unwrap();
}

#[tokio::test]
async fn recovery_file_round_trip_preserves_session_retained_and_inflight() {
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-recovery-{}-{}",
        std::process::id(),
        now_ms()
    ));
    fs::create_dir_all(&directory).unwrap();

    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let a = auth("recovery");
    let mut attachment = broker.attach(&a, "persistent".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/recovery/#",
            1,
        )
        .unwrap();
    broker
        .route(
            &a.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/recovery/up".into(),
                payload: b"durable".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(first) = attachment.receiver.recv().await.unwrap() else {
        panic!("expected publish")
    };
    let packet_id = first.packet_id.unwrap();
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();
    broker.commit_to(&directory).await.unwrap();

    let recovered = MqttBroker::new(Arc::new(Limits::default()));
    assert!(recovered.recover_from(&directory).await.unwrap());
    let mut resumed = recovered.attach(&a, "persistent".into(), false).unwrap();
    assert!(resumed.session_present);
    let BrokerFrame::Publish(replayed) = resumed.receiver.recv().await.unwrap() else {
        panic!("expected replayed publish")
    };
    assert_eq!(replayed.packet_id, Some(packet_id));
    assert!(replayed.dup);
    assert!(!replayed.message.retain);
    let mut retained_subscriber = recovered
        .attach(&a, "retained-reader".into(), true)
        .unwrap();
    recovered
        .subscribe(
            &retained_subscriber.key,
            retained_subscriber.generation,
            "v1/t/t/p/p/d/recovery/#",
            1,
        )
        .unwrap();
    let BrokerFrame::Publish(retained) = retained_subscriber.receiver.recv().await.unwrap() else {
        panic!("expected retained publish")
    };
    assert!(retained.message.retain);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn mqtt_recovery_semantic_invalid_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("semantic-invalid");
    let attachment = broker.attach(&device, "persistent".into(), false).unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            5,
            BrokerMessage {
                topic: "semantic/invalid".into(),
                payload: vec![1].into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let mut snapshot = broker.snapshot().unwrap();
    snapshot.sessions[0].inbound_qos2.insert(
        5,
        InboundQos2State::AwaitPubrel(BrokerMessage {
            topic: "semantic/invalid".into(),
            payload: vec![1].into(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        }),
    );
    assert!(matches!(broker.restore(snapshot), Err(Error::Invalid)));
}

#[tokio::test]
async fn mqtt_recovery_v6_large_client_id_will_fits_admitted_bounds() {
    let mut configured = Limits {
        max_client_id_bytes: 40_000,
        ..Limits::default()
    };
    configured.mqtt_recovery_max_bytes = configured.mqtt_recovery_upper_bound().unwrap();
    configured.validate().unwrap();
    let limits = Arc::new(configured);
    let broker = MqttBroker::new(limits.clone());
    let owner = auth("wide-will").device_key;
    let origin = SessionKey {
        device: owner.clone(),
        client_id: "c".repeat(limits.max_client_id_bytes),
    };
    let pending = PendingWill {
        owner: owner.clone(),
        origin: Some(origin.clone()),
        message: BrokerMessage {
            topic: "v1/t/t/p/p/d/wide-will/up".into(),
            // ClientId and Will payload together fit the configured 65,536-byte packet.
            payload: vec![0x7a; 20_000].into(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        },
        due_at_ms: Some(i64::MAX),
        cancel_on_resume: Some((origin.clone(), 1)),
        message_expiry_interval: None,
        retained_reservation: RetainedReservation::default(),
    };
    {
        let mut state = lock(&broker.state).unwrap();
        reserve_will_capacity(&mut state, &owner.tenant_id, pending.bytes(), &limits).unwrap();
        insert_pending_will(&mut state, pending);
    }
    let directory =
        std::env::temp_dir().join(format!("netbaiot-wide-will-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&directory).await.unwrap();
    let image = fs::read(directory.join(RECOVERY_FILE)).unwrap();
    let first_record_bytes = u32::from_be_bytes(image[49..53].try_into().unwrap()) as usize;
    let previous_record_ceiling = limits.max_mqtt_packet_size
        + limits.max_topic_bytes * 2
        + limits.max_mqtt_property_bytes
        + 2_048;
    assert!(first_record_bytes > previous_record_ceiling);
    let recovered = MqttBroker::new(limits);
    assert!(recovered.recover_from(&directory).await.unwrap());
    assert_eq!(recovered.pending_will_count().unwrap(), 1);
    assert_eq!(
        all_pending_wills(&recovered.state.lock().unwrap())
            .next()
            .unwrap()
            .origin,
        Some(origin)
    );
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn mqtt_recovery_whole_image_integrity_001() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("integrity");
    let topic = "v1/t/t/p/p/d/integrity/up";
    let mut attachment = broker.attach(&device, "persistent".into(), false).unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"outbound".to_vec().into(),
                qos: 2,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    let _ = attachment.receiver.recv().await.unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            77,
            BrokerMessage {
                topic: topic.into(),
                payload: b"inbound".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-integrity-{}-{}",
        std::process::id(),
        now_ms()
    ));
    broker.commit_to(&directory).await.unwrap();
    let image = fs::read(directory.join(RECOVERY_FILE)).unwrap();
    let records_end = image.len() - RECOVERY_TRAILER_BYTES;
    let mut records = Vec::new();
    let mut at = RECOVERY_HEADER_BYTES;
    while at < records_end {
        let kind = image[at];
        let length = usize::try_from(u32::from_be_bytes(
            image[at + 1..at + 5].try_into().unwrap(),
        ))
        .unwrap();
        let end = at + RECORD_HEADER_BYTES + length + RECORD_CHECKSUM_BYTES;
        records.push((kind, at, end));
        at = end;
    }
    assert_eq!(at, records_end);
    for kind in [RECORD_OFFLINE, RECORD_OUTBOUND, RECORD_INBOUND_QOS2] {
        let (_, start, end) = records
            .iter()
            .copied()
            .find(|record| record.0 == kind)
            .unwrap();
        let mut mutated = image.clone();
        mutated.drain(start..end);
        assert!(decode_mqtt_recovery(&mutated, &limits).is_err());
    }
    assert!(decode_mqtt_recovery(&image[..records_end], &limits).is_err());

    let (_, first_start, first_end) = records[1];
    let (_, second_start, second_end) = records[2];
    let mut reordered = Vec::with_capacity(image.len());
    reordered.extend_from_slice(&image[..first_start]);
    reordered.extend_from_slice(&image[second_start..second_end]);
    reordered.extend_from_slice(&image[first_end..second_start]);
    reordered.extend_from_slice(&image[first_start..first_end]);
    reordered.extend_from_slice(&image[second_end..]);
    assert!(decode_mqtt_recovery(&reordered, &limits).is_err());

    let mut unknown = Vec::with_capacity(image.len() + RECORD_HEADER_BYTES + 32);
    unknown.extend_from_slice(&image[..records_end]);
    unknown.push(0xff);
    unknown.extend_from_slice(&0u32.to_be_bytes());
    unknown.extend_from_slice(&Sha256::digest([]));
    unknown.extend_from_slice(&image[records_end..]);
    assert!(decode_mqtt_recovery(&unknown, &limits).is_err());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn qos2_recovery_resumes_each_protocol_stage_without_reallocation() {
    let limits = Arc::new(Limits::default());
    let a = auth("qos2-recovery");
    let broker = MqttBroker::new(limits.clone());
    let mut attachment = broker.attach(&a, "qos2".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/qos2-recovery/#",
            2,
        )
        .unwrap();
    broker
        .route(
            &a.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/qos2-recovery/up".into(),
                payload: b"outbound".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(first) = attachment.receiver.recv().await.unwrap() else {
        panic!("expected qos2 publish")
    };
    let packet_id = first.packet_id.unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            77,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/qos2-recovery/up".into(),
                payload: b"inbound".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();

    let before_pubrec = MqttBroker::new(limits.clone());
    before_pubrec.restore(broker.snapshot().unwrap()).unwrap();
    let mut resumed = before_pubrec.attach(&a, "qos2".into(), false).unwrap();
    let BrokerFrame::Publish(retry) = resumed.receiver.recv().await.unwrap() else {
        panic!("expected qos2 publish retry")
    };
    assert_eq!(retry.packet_id, Some(packet_id));
    assert!(retry.dup);
    assert!(
        before_pubrec
            .inbound_qos2_message(&resumed.key, resumed.generation, 77)
            .unwrap()
            .is_some()
    );

    before_pubrec
        .pubrec(&resumed.key, resumed.generation, packet_id)
        .unwrap();
    let before_pubcomp = MqttBroker::new(limits);
    before_pubcomp
        .restore(before_pubrec.snapshot().unwrap())
        .unwrap();
    let mut resumed = before_pubcomp.attach(&a, "qos2".into(), false).unwrap();
    assert!(matches!(
        resumed.receiver.recv().await,
        Some(BrokerFrame::Pubrel {
            packet_id: id,
            dup: true
        }) if id == packet_id
    ));

    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = before_pubcomp
        .begin_inbound_qos2_delivery(&resumed.key, resumed.generation, 77)
        .unwrap()
    else {
        panic!("expected inbound QoS2 delivery ownership")
    };
    before_pubcomp
        .finish_inbound_qos2_delivery(&resumed.key, session_incarnation, 77, operation_id)
        .unwrap();
    let accepted_stage = MqttBroker::new(Arc::new(Limits::default()));
    accepted_stage
        .restore(before_pubcomp.snapshot().unwrap())
        .unwrap();
    let accepted_owner = accepted_stage.attach(&a, "qos2".into(), false).unwrap();
    assert!(matches!(
        accepted_stage
            .inbound_qos2_message(&accepted_owner.key, accepted_owner.generation, 77)
            .unwrap(),
        Some((_, true))
    ));
}

#[tokio::test]
async fn recovery_file_supports_legal_state_larger_than_event_spool_record() {
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-large-recovery-{}-{}",
        std::process::id(),
        now_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let limits = Arc::new(Limits {
        max_offline_bytes_per_session: 2_097_152,
        max_mqtt_session_state_bytes: 4_194_304,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits.clone());
    let a = auth("large-recovery");
    let attachment = broker.attach(&a, "persistent".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/large-recovery/#",
            1,
        )
        .unwrap();
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();
    for sequence in 0..20u8 {
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/large-recovery/up".into(),
                    payload: vec![sequence; 60_000].into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    broker.commit_to(&directory).await.unwrap();
    assert!(fs::metadata(directory.join(RECOVERY_FILE)).unwrap().len() > 1_048_576);
    let recovered = MqttBroker::new(limits);
    assert!(recovered.recover_from(&directory).await.unwrap());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
#[ignore = "manual 10/50/100 MiB recovery benchmark"]
async fn mqtt_recovery_streaming_benchmark_manual() {
    let limits = Arc::new(Limits {
        max_retained_messages: 3_000,
        max_retained_messages_per_tenant: 3_000,
        max_retained_bytes: 125_829_120,
        max_retained_bytes_per_tenant: 125_829_120,
        mqtt_recovery_max_bytes: 335_544_320,
        ..Limits::default()
    });
    limits.validate().unwrap();
    for logical_mib in [10usize, 50, 100] {
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-bench-{}-{}-{logical_mib}",
            std::process::id(),
            now_ms()
        ));
        let broker = MqttBroker::new(limits.clone());
        let target = logical_mib * 1_048_576;
        let mut payload_bytes = 0usize;
        let mut index = 0usize;
        while payload_bytes < target {
            let length = (target - payload_bytes).min(60_000);
            let device = auth(&format!("bench-{index}"));
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: (format!("v1/t/t/p/p/d/bench-{index}/up")).into(),
                        payload: vec![index as u8; length].into(),
                        qos: 0,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            payload_bytes += length;
            index += 1;
        }
        let encode_started = std::time::Instant::now();
        broker.commit_to(&directory).await.unwrap();
        let encode_elapsed = encode_started.elapsed();
        let file_bytes = fs::metadata(directory.join(RECOVERY_FILE)).unwrap().len();
        let restored = MqttBroker::new(limits.clone());
        let decode_started = std::time::Instant::now();
        assert!(restored.recover_from(&directory).await.unwrap());
        let decode_elapsed = decode_started.elapsed();
        println!(
            "logical_mib={logical_mib} logical_bytes={payload_bytes} file_bytes={file_bytes} encode_ms={} decode_ms={} max_record_bytes={}",
            encode_elapsed.as_millis(),
            decode_elapsed.as_millis(),
            recovery_record_max(&limits).unwrap()
        );
        fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
#[ignore = "manual near-capacity NBMQ v6 commit and recovery"]
async fn mqtt_recovery_v6_near_default_capacity_manual() {
    let limits = Arc::new(Limits::default());
    limits.validate().unwrap();
    let broker = MqttBroker::new(limits.clone());
    for index in 0..128 {
        let mut identity = auth(&format!("d{index}"));
        identity.device_key.tenant_id = TenantId::new(format!("t{}", index / 16)).unwrap();
        let topic = format!("v1/t/t{}/p/p/d/d{index}/up", index / 16);
        let mut attachment = broker
            .attach_v5(&identity, format!("client-{index}"), false, 3_600, 4)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, &topic, 1)
            .unwrap();
        attachment.detach().unwrap();
        for _ in 0..110 {
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: topic.clone().into(),
                        payload: vec![0x5a; 8_192].into(),
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
    }
    {
        let mut state = lock(&broker.state).unwrap();
        for index in 0..256 {
            let mut owner = auth(&format!("w{index}")).device_key;
            owner.tenant_id = TenantId::new(format!("t{}", index / 32)).unwrap();
            let origin = SessionKey {
                device: owner.clone(),
                client_id: "c".repeat(limits.max_client_id_bytes),
            };
            let pending = PendingWill {
                owner: owner.clone(),
                origin: Some(origin.clone()),
                message: BrokerMessage {
                    topic: (format!("v1/t/t{}/p/p/d/w{index}/up", index / 32)).into(),
                    payload: vec![0x77; 65_000].into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
                due_at_ms: Some(i64::MAX),
                cancel_on_resume: Some((origin, 1)),
                message_expiry_interval: None,
                retained_reservation: RetainedReservation::default(),
            };
            reserve_will_capacity(&mut state, &owner.tenant_id, pending.bytes(), &limits).unwrap();
            insert_pending_will(&mut state, pending);
        }
    }
    for index in 0..1_024 {
        let mut owner = auth(&format!("r{index}")).device_key;
        owner.tenant_id = TenantId::new(format!("t{}", index / 128)).unwrap();
        broker
            .route(
                &owner,
                BrokerMessage {
                    topic: (format!("v1/t/t{}/p/p/d/r{index}/up", index / 128)).into(),
                    payload: vec![0x88; 64_900].into(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    assert_broker_accounting(&broker);
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-v6-near-capacity-{}",
        uuid::Uuid::new_v4()
    ));
    broker.commit_to(&directory).await.unwrap();
    let file_bytes = fs::metadata(directory.join(RECOVERY_FILE)).unwrap().len() as usize;
    println!(
        "NBMQ v6 file_bytes={file_bytes} configured_max={} proven_upper_bound={}",
        limits.mqtt_recovery_max_bytes,
        limits.mqtt_recovery_upper_bound().unwrap()
    );
    assert!(file_bytes > limits.mqtt_recovery_max_bytes * 95 / 100);
    let recovered = MqttBroker::new(limits.clone());
    assert!(recovered.recover_from(&directory).await.unwrap());
    assert_broker_accounting(&recovered);
    assert_eq!(recovered.pending_will_count().unwrap(), 256);
    assert_eq!(recovered.usage().unwrap().3, 1_024);
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn recovery_file_rejects_corruption_and_unknown_version() {
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-corrupt-{}-{}",
        std::process::id(),
        now_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    broker.commit_to(&directory).await.unwrap();
    let path = directory.join(RECOVERY_FILE);

    let mut corrupt = fs::read(&path).unwrap();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x80;
    fs::write(&path, &corrupt).unwrap();
    assert!(matches!(
        broker.recover_from(&directory).await,
        Err(Error::Invalid)
    ));

    broker.commit_to(&directory).await.unwrap();
    let mut unknown_version = fs::read(&path).unwrap();
    unknown_version[4..8].copy_from_slice(&(RECOVERY_VERSION + 1).to_be_bytes());
    fs::write(&path, &unknown_version).unwrap();
    assert!(matches!(
        broker.recover_from(&directory).await,
        Err(Error::UnsupportedRecoveryVersion(version)) if version == RECOVERY_VERSION + 1
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn command_progress_follows_started_qos_recovery_after_write_failure() {
    use netbaiot_core::DeliveryState;
    use netbaiot_runtime::CommandProgress;
    for v5 in [false, true] {
        for qos in [1, 2] {
            let broker = MqttBroker::new(Arc::new(Limits::default()));
            let device = auth("progress-reconnect");
            let attach = || {
                if v5 {
                    broker.attach_v5(&device, "progress".into(), false, 60, 8)
                } else {
                    broker.attach(&device, "progress".into(), false)
                }
                .unwrap()
            };
            let mut attachment = attach();
            let topic = "v1/t/t/p/p/d/progress-reconnect/down";
            broker
                .subscribe(&attachment.key, attachment.generation, topic, qos)
                .unwrap();
            let expiry = now_ms() + 10_000;
            let progress = CommandProgress::new(expiry, Arc::new(Metrics::default()));
            broker
                .send_live_tracked(
                    &attachment.key,
                    attachment.generation,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: vec![1].into(),
                        qos,
                        retain: false,
                        properties: (PublishProperties {
                            expires_at_ms: Some(expiry),
                            ..Default::default()
                        })
                        .into(),
                    },
                    Some(progress.clone()),
                )
                .unwrap();
            let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                panic!("publish expected")
            };
            let id = delivery.packet_id.unwrap();
            assert!(
                broker
                    .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                    .unwrap()
            );
            progress.update(DeliveryState::Failed); // The write outcome is uncertain.
            assert!(!progress.expire(expiry));
            attachment.detach().unwrap();
            let mut resumed = attach();
            let BrokerFrame::Publish(retransmit) = resumed.receiver.try_recv().unwrap() else {
                panic!("retransmitted publish expected")
            };
            assert_eq!(retransmit.packet_id, Some(id));
            assert!(Arc::ptr_eq(
                retransmit.progress.as_ref().unwrap(),
                &progress
            ));
            assert!(
                broker
                    .begin_outbound_transfer(&resumed.key, resumed.generation, &retransmit)
                    .unwrap()
            );
            progress.update(DeliveryState::Sent);
            assert_eq!(progress.state(), DeliveryState::Sent);
            // Exact protocol acknowledgement is newer evidence even if the
            // most recent write failed after the device received its bytes.
            progress.update(DeliveryState::Failed);
            assert!(
                broker
                    .pubcomp(&resumed.key, resumed.generation, id)
                    .is_err()
            );
            assert_eq!(progress.state(), DeliveryState::Failed);
            if qos == 1 {
                broker.puback(&resumed.key, resumed.generation, id).unwrap();
            } else {
                broker.pubrec(&resumed.key, resumed.generation, id).unwrap();
                broker
                    .pubcomp(&resumed.key, resumed.generation, id)
                    .unwrap();
            }
            assert_eq!(progress.state(), DeliveryState::Received);
            progress.update(DeliveryState::Failed); // Stale failure cannot undo ACK.
            assert_eq!(progress.state(), DeliveryState::Received);
            resumed.detach().unwrap();
        }
    }
}
