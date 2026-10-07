use super::*;

#[tokio::test]
async fn v4_recovery_preserves_v5_session_and_v3_remains_readable() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let auth = auth("a");
    let mut attachment = broker
        .attach_v5(&auth, "v5".into(), false, 60, u16::MAX)
        .unwrap();
    attachment.detach().unwrap();
    let directory = std::env::temp_dir().join(format!("netbaiot-mqtt-v4-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&directory).await.unwrap();
    let restored = MqttBroker::new(Arc::new(Limits::default()));
    assert!(restored.recover_from(&directory).await.unwrap());
    let mut resumed = restored
        .attach_v5(&auth, "v5".into(), false, 60, u16::MAX)
        .unwrap();
    assert!(resumed.session_present);
    resumed.detach().unwrap();
    fs::remove_dir_all(&directory).unwrap();

    let v311 = MqttBroker::new(Arc::new(Limits::default()));
    let mut old = v311.attach(&auth, "old".into(), false).unwrap();
    old.detach().unwrap();
    let state = v311.state.lock().unwrap();
    let session = state.sessions.values().next().unwrap();
    let mut payload = Vec::new();
    encode_session_meta(&mut payload, session).unwrap();
    payload.truncate(payload.len() - 13);
    let mut header = Vec::new();
    header.extend_from_slice(RECOVERY_MAGIC);
    header.extend_from_slice(&RECOVERY_VERSION_V3.to_be_bytes());
    header.extend_from_slice(&state.generation.to_be_bytes());
    let mut record = vec![RECORD_SESSION];
    record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&Sha256::digest(&payload));
    let mut digest = Sha256::new();
    digest.update(&header);
    digest.update(&record);
    let mut image = header.clone();
    image.extend_from_slice(&Sha256::digest(&header));
    image.extend_from_slice(&record);
    image.extend_from_slice(RECOVERY_TRAILER_MAGIC);
    image.extend_from_slice(&1u64.to_be_bytes());
    image.extend_from_slice(&(record.len() as u64).to_be_bytes());
    image.extend_from_slice(&digest.finalize());
    drop(state);
    let snapshot = decode_mqtt_recovery(&image, &Limits::default()).unwrap();
    assert_eq!(snapshot.format_version, RECOVERY_VERSION_V3);
    let recovered = MqttBroker::new(Arc::new(Limits::default()));
    recovered.restore(snapshot).unwrap();
    let mut resumed = recovered.attach(&auth, "old".into(), false).unwrap();
    assert!(resumed.session_present);
    resumed.detach().unwrap();
}

#[tokio::test]
async fn mqtt_recovery_v1_compat_001() {
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-v1-{}-{}",
        std::process::id(),
        now_ms()
    ));
    fs::create_dir_all(&directory).unwrap();
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("v1-compatible");
    let attachment = broker.attach(&device, "persistent".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/v1-compatible/#",
            1,
        )
        .unwrap();
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();
    let mut snapshot = broker.snapshot().unwrap();
    snapshot.format_version = RECOVERY_VERSION_V1;
    let payload = serde_json::to_vec(&snapshot).unwrap();
    let mut image = Vec::new();
    image.extend_from_slice(RECOVERY_MAGIC);
    image.extend_from_slice(&RECOVERY_VERSION_V1.to_be_bytes());
    image.extend_from_slice(&snapshot.snapshot_generation.to_be_bytes());
    image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    image.extend_from_slice(&payload);
    image.extend_from_slice(&Sha256::digest(&payload));
    fs::write(directory.join(RECOVERY_FILE), image).unwrap();
    let restored = MqttBroker::new(Arc::new(Limits::default()));
    assert!(restored.recover_from(&directory).await.unwrap());
    assert!(
        restored
            .attach(&device, "persistent".into(), false)
            .unwrap()
            .session_present
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn mqtt_recovery_v1_large_compat_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("v1-large");
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/v1-large/up".into(),
                payload: vec![0xa5; 8 * 1024].into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    let mut snapshot = broker.snapshot().unwrap();
    snapshot.format_version = RECOVERY_VERSION_V1;
    let mut legacy = serde_json::to_value(&snapshot).unwrap();
    let object = legacy.as_object_mut().unwrap();
    object.remove("pending_wills");
    if let Some(sessions) = object
        .get_mut("sessions")
        .and_then(|value| value.as_array_mut())
    {
        for session in sessions {
            let session = session.as_object_mut().unwrap();
            session.remove("incarnation");
            session.remove("authorization");
            session.remove("inbound_reservations");
        }
    }
    let payload = serde_json::to_vec(&legacy).unwrap();
    let mut image = Vec::new();
    image.extend_from_slice(RECOVERY_MAGIC);
    image.extend_from_slice(&RECOVERY_VERSION_V1.to_be_bytes());
    image.extend_from_slice(&snapshot.snapshot_generation.to_be_bytes());
    image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    image.extend_from_slice(&payload);
    image.extend_from_slice(&Sha256::digest(&payload));
    let lowered_v3_limits = Limits {
        mqtt_recovery_max_bytes: 1_024,
        ..Limits::default()
    };
    assert!(image.len() > lowered_v3_limits.mqtt_recovery_max_bytes);
    let decoded = decode_mqtt_recovery(&image, &lowered_v3_limits).unwrap();
    assert_eq!(decoded.format_version, RECOVERY_VERSION_V1);
    assert_eq!(decoded.retained.len(), 1);
}

#[test]
fn mqtt_recovery_v2_compat_001() {
    let device = auth("v2-compatible");
    let mut payload = Vec::new();
    put_string(&mut payload, device.device_key.tenant_id.as_str()).unwrap();
    put_string(&mut payload, device.device_key.product_id.as_str()).unwrap();
    put_string(&mut payload, device.device_key.device_id.as_str()).unwrap();
    put_string(&mut payload, "persistent").unwrap();
    payload.extend_from_slice(&1u64.to_be_bytes());
    payload.push(1);
    payload.extend_from_slice(&device.credential_version.to_be_bytes());
    payload.extend_from_slice(&device.auth_generation.to_be_bytes());
    payload.push(1);
    payload.push(1);
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&now_ms().to_be_bytes());
    let mut header = Vec::new();
    header.extend_from_slice(RECOVERY_MAGIC);
    header.extend_from_slice(&RECOVERY_VERSION_V2.to_be_bytes());
    header.extend_from_slice(&1u64.to_be_bytes());
    let mut image = header.clone();
    image.extend_from_slice(&Sha256::digest(&header));
    image.push(RECORD_SESSION);
    image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    image.extend_from_slice(&payload);
    image.extend_from_slice(&Sha256::digest(&payload));
    let decoded = decode_mqtt_recovery(&image, &Limits::default()).unwrap();
    assert_eq!(decoded.format_version, RECOVERY_VERSION_V2);
    let restored = MqttBroker::new(Arc::new(Limits::default()));
    restored.restore(decoded).unwrap();
    // v2 did not carry codec provenance, so it is readable but conservatively reset at attach.
    assert!(
        !restored
            .attach(&device, "persistent".into(), false)
            .unwrap()
            .session_present
    );
}

#[test]
fn mqtt_recovery_acl_ownership_001() {
    let limits = Arc::new(Limits::default());
    let source = MqttBroker::new(limits.clone());
    let device = auth("acl-owner");
    let attachment = source.attach(&device, "persistent".into(), false).unwrap();
    source
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/acl-owner/#",
            1,
        )
        .unwrap();
    let base = source.snapshot().unwrap();
    let foreign = BrokerMessage {
        topic: "v1/t/t/p/p/d/other/up".into(),
        payload: b"foreign".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };

    let mut invalid = base.clone();
    invalid.sessions[0]
        .subscriptions
        .insert("v1/t/t/p/p/d/other/#".into(), Subscription::v311(1));
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut invalid = base.clone();
    invalid.sessions[0].offline.push_back(foreign.clone());
    invalid.sessions[0].offline_bytes = foreign.bytes();
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut invalid = base.clone();
    invalid.sessions[0]
        .outbound
        .insert(1, OutboundState::AwaitPuback(foreign.clone()));
    invalid.sessions[0].outbound_order.push_back(1);
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut invalid = base.clone();
    invalid.sessions[0].inbound_qos2.insert(
        2,
        InboundQos2State::AwaitPubrel(BrokerMessage { qos: 2, ..foreign }),
    );
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut invalid = base.clone();
    let retained_message = BrokerMessage {
        topic: "v1/t/t/p/p/d/acl-owner/up".into(),
        payload: b"retained".to_vec().into(),
        qos: 1,
        retain: true,
        properties: Default::default(),
    };
    invalid.retained.push((
        retained_message.topic.to_string(),
        RetainedMessage {
            tenant_id: TenantId::new("other-tenant").unwrap(),
            message: retained_message,
            origin: None,
        },
    ));
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut invalid = base.clone();
    invalid.pending_wills.push(PendingWill {
        owner: device.device_key.clone(),
        origin: None,
        message: BrokerMessage {
            topic: "v1/t/t/p/p/d/other/up".into(),
            payload: b"foreign-will".to_vec().into(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        },
        due_at_ms: None,
        cancel_on_resume: None,
        message_expiry_interval: None,
        retained_reservation: RetainedReservation::default(),
    });
    assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

    let mut legacy = base;
    legacy.sessions[0].authorization = None;
    let restored = MqttBroker::new(limits);
    restored.restore(legacy).unwrap();
    assert_eq!(
        restored
            .matching_subscription_count("v1/t/t/p/p/d/acl-owner/up")
            .unwrap(),
        0
    );
    assert!(
        !restored
            .attach(&device, "persistent".into(), false)
            .unwrap()
            .session_present
    );
}

#[tokio::test]
async fn mqtt_recovery_v3_raw_binary_round_trip_bound() {
    let limits = Arc::new(Limits::default());
    limits.validate().unwrap();
    let broker = MqttBroker::new(limits.clone());
    for (index, byte) in [0_u8, 0x7f, 0x80, 0xff].into_iter().enumerate() {
        let device = auth(&format!("binary-recovery-{index}"));
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: (format!("v1/t/t/p/p/d/binary-recovery-{index}/up")).into(),
                    payload: vec![byte; 1024].into(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-mqtt-binary-recovery-{}-{}",
        std::process::id(),
        now_ms()
    ));
    broker.commit_to(&directory).await.unwrap();
    let encoded = fs::read(directory.join(RECOVERY_FILE)).unwrap();
    assert!(encoded.len() <= limits.mqtt_recovery_upper_bound().unwrap());
    assert!(
        encoded
            .windows(1024)
            .any(|window| window.iter().all(|byte| *byte == 0xff))
    );
    let recovered = MqttBroker::new(limits);
    assert!(recovered.recover_from(&directory).await.unwrap());
    assert_eq!(recovered.usage().unwrap().3, 4);
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn mqtt_recovery_historical_binary_fixtures_upgrade_to_v6() {
    let limits = Arc::new(Limits::default());
    let fixtures: [(u32, &[u8]); 6] = [
        (
            1,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v1-empty.nbmq"
            ),
        ),
        (
            2,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v2-empty.nbmq"
            ),
        ),
        (
            3,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v3-empty.nbmq"
            ),
        ),
        (
            4,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v4-empty.nbmq"
            ),
        ),
        (
            5,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v5-qos2-no-local-delayed-will.nbmq"
            ),
        ),
        (
            6,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v6-empty-rollback.nbmq"
            ),
        ),
    ];
    for (version, bytes) in fixtures {
        let decoded = decode_mqtt_recovery(bytes, &limits).unwrap();
        assert_eq!(decoded.format_version, version);
        if version == 5 {
            assert_eq!(decoded.sessions.len(), 1);
            assert!(
                decoded.sessions[0]
                    .subscriptions
                    .values()
                    .any(|s| s.no_local)
            );
            assert!(matches!(
                decoded.sessions[0].inbound_qos2.get(&7),
                Some(InboundQos2State::AwaitPubrel(_))
            ));
            assert_eq!(decoded.pending_wills.len(), 1);
            let pending = &decoded.pending_wills[0];
            assert!(pending.due_at_ms.is_some());
            assert_eq!(
                pending.origin.as_ref(),
                pending.cancel_on_resume.as_ref().map(|(key, _)| key)
            );
        }
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-historical-v{version}-{}",
            uuid::Uuid::new_v4()
        ));
        let broker = MqttBroker::new(limits.clone());
        broker.restore(decoded).unwrap();
        broker.commit_to(&directory).await.unwrap();
        let upgraded = MqttBroker::new(limits.clone());
        assert!(upgraded.recover_from(&directory).await.unwrap());
        let snapshot = upgraded.snapshot().unwrap();
        assert_eq!(snapshot.format_version, RECOVERY_VERSION);
        if version == 5 {
            assert!(
                snapshot.sessions[0]
                    .subscriptions
                    .values()
                    .any(|s| s.no_local)
            );
            assert!(matches!(
                snapshot.sessions[0].inbound_qos2.get(&7),
                Some(InboundQos2State::AwaitPubrel(_))
            ));
            assert_eq!(snapshot.pending_wills.len(), 1);
            assert_eq!(
                snapshot.pending_wills[0].origin,
                snapshot.pending_wills[0]
                    .cancel_on_resume
                    .as_ref()
                    .map(|(key, _)| key.clone())
            );
            let original_key = snapshot.sessions[0].key.clone();
            let will_topic = snapshot.pending_wills[0].message.topic.clone();
            let authorization = snapshot.sessions[0].authorization.as_ref().unwrap();
            let identity = AuthenticatedDevice {
                device_key: original_key.device.clone(),
                credential_version: authorization.credential_version,
                auth_generation: authorization.auth_generation,
                codec_id: authorization.codec_id.clone().unwrap(),
                codec_version: authorization.codec_version.unwrap(),
                permissions: authorization.permissions.clone(),
            };
            let mut observer = upgraded
                .attach_v5(&identity, "fixture-observer".into(), false, 60, 4)
                .unwrap();
            upgraded
                .subscribe_v5(
                    &observer.key,
                    observer.generation,
                    &will_topic,
                    v5::SubscriptionOptions {
                        qos: 1,
                        no_local: true,
                        retain_as_published: false,
                        retain_handling: 0,
                    },
                )
                .unwrap();
            {
                let mut state = lock(&upgraded.state).unwrap();
                let mut will = take_owned_future_wills(&mut state, &original_key)
                    .pop()
                    .unwrap();
                will.due_at_ms = Some(now_ms() - 1);
                insert_pending_will(&mut state, will);
            }
            upgraded.tick().unwrap();
            let BrokerFrame::Publish(delivery) = observer.receiver.try_recv().unwrap() else {
                panic!("observer should receive the recovered Will")
            };
            assert_eq!(delivery.message.payload.as_ref(), b"fixed-will");
            assert!(
                upgraded
                    .state
                    .lock()
                    .unwrap()
                    .sessions
                    .get(&original_key)
                    .unwrap()
                    .offline
                    .is_empty()
            );
            observer.detach().unwrap();
        }
        fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn mqtt_recovery_historical_nonempty_fixtures_preserve_qos_and_retained() {
    let limits = Arc::new(Limits::default());
    let fixtures: [(u32, &[u8]); 4] = [
        (
            1,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v1-session-retained.nbmq"
            ),
        ),
        (
            2,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v2-session-retained.nbmq"
            ),
        ),
        (
            3,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v3-session-retained.nbmq"
            ),
        ),
        (
            4,
            include_bytes!(
                "../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v4-session-retained.nbmq"
            ),
        ),
    ];
    for (version, image) in fixtures {
        let decoded = decode_mqtt_recovery(image, &limits).unwrap();
        assert_eq!(decoded.format_version, version);
        assert_eq!(decoded.sessions.len(), 1);
        assert_eq!(decoded.sessions[0].subscriptions.len(), 1);
        assert!(matches!(
            decoded.sessions[0].inbound_qos2.get(&7),
            Some(InboundQos2State::AwaitPubrel(_))
        ));
        assert_eq!(decoded.retained.len(), 1);
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-historical-state-v{version}-{}",
            uuid::Uuid::new_v4()
        ));
        let broker = MqttBroker::new(limits.clone());
        broker.restore(decoded).unwrap();
        broker
            .commit_to(&directory)
            .await
            .unwrap_or_else(|error| panic!("v{version} commit failed: {error:?}"));
        let recovered = MqttBroker::new(limits.clone());
        assert!(recovered.recover_from(&directory).await.unwrap());
        let snapshot = recovered.snapshot().unwrap();
        assert_eq!(snapshot.format_version, RECOVERY_VERSION);
        assert_eq!(snapshot.sessions.len(), 1);
        assert_eq!(snapshot.sessions[0].subscriptions.len(), 1);
        assert!(matches!(
            snapshot.sessions[0].inbound_qos2.get(&7),
            Some(InboundQos2State::AwaitPubrel(_))
        ));
        assert_eq!(snapshot.retained.len(), 1);
        if version == 2 {
            assert!(snapshot.sessions[0].authorization.is_none());
            let key = snapshot.sessions[0].key.clone();
            let mut identity = auth("device-1");
            identity.device_key = key.device.clone();
            let mut attached = recovered
                .attach(&identity, key.client_id.clone(), false)
                .unwrap();
            assert!(!attached.session_present);
            attached.detach().unwrap();
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
