use super::*;

#[tokio::test]
async fn current_recovery_preserves_mqtt5_persistent_session() {
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

    let mut unknown_profile = base;
    unknown_profile.sessions[0].authorization = None;
    let restored = MqttBroker::new(limits);
    restored.restore(unknown_profile).unwrap();
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
async fn mqtt_recovery_raw_binary_round_trip_bound() {
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

#[test]
fn recovery_rejects_old_future_and_corrupt_versions_before_payload() {
    let limits = Limits::default();
    for version in [0, 1, 2, 3, 4, 5, RECOVERY_VERSION + 1, u32::MAX] {
        let mut header = RECOVERY_MAGIC.to_vec();
        header.extend_from_slice(&version.to_be_bytes());
        assert!(
            matches!(decode_mqtt_recovery(&header, &limits), Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
        );
        let broker = MqttBroker::new(Arc::new(limits.clone()));
        let mut snapshot = broker.snapshot().unwrap();
        snapshot.format_version = version;
        assert!(
            matches!(broker.restore(snapshot), Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
        );
        assert_eq!(broker.usage().unwrap().0, 0);
    }
    for header in [b"NBMQ".as_slice(), b"BAD!\0\0\0\x06", b"NBMQ\0\0\0\x06"] {
        assert!(matches!(
            decode_mqtt_recovery(header, &limits),
            Err(Error::Invalid)
        ));
    }
}

#[tokio::test]
async fn current_golden_bytes_round_trip_without_format_change() {
    let limits = Arc::new(Limits::default());
    for bytes in [
        include_bytes!("../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v6-empty-rollback.nbmq").as_slice(),
        include_bytes!("../../../../../../tests/mqtt_conformance/fixtures/mqtt_recovery/v6-session-qos-will.nbmq").as_slice(),
    ] {
        let snapshot = decode_mqtt_recovery(bytes, &limits).unwrap();
        assert_eq!(snapshot.format_version, RECOVERY_VERSION);
        if !snapshot.sessions.is_empty() {
            assert_eq!(snapshot.sessions.len(), 1);
            assert!(snapshot.sessions[0].subscriptions.values().any(|subscription| subscription.no_local));
            assert!(matches!(snapshot.sessions[0].inbound_qos2.get(&7), Some(InboundQos2State::AwaitPubrel(_))));
            assert_eq!(snapshot.pending_wills.len(), 1);
            assert!(snapshot.pending_wills[0].due_at_ms.is_some());
            assert_eq!(snapshot.pending_wills[0].origin.as_ref(), snapshot.pending_wills[0].cancel_on_resume.as_ref().map(|(key, _)| key));
        }
        let directory = std::env::temp_dir().join(format!("netbaiot-current-golden-{}", uuid::Uuid::new_v4()));
        let broker = MqttBroker::new(limits.clone());
        broker.restore(snapshot).unwrap();
        broker.commit_to(&directory).await.unwrap();
        assert_eq!(fs::read(directory.join(RECOVERY_FILE)).unwrap(), bytes);
        let restored = MqttBroker::new(limits.clone());
        assert!(restored.recover_from(&directory).await.unwrap());
        fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn unsupported_file_versions_preserve_existing_state_and_committed_bytes() {
    for version in [0u32, 1, 2, 3, 4, 5, 7, u32::MAX] {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-rejected-mqtt-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let mut header = RECOVERY_MAGIC.to_vec();
        header.extend_from_slice(&version.to_be_bytes());
        let path = directory.join(RECOVERY_FILE);
        fs::write(&path, &header).unwrap();
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut active = broker
            .attach(&auth("preserved"), "persistent".into(), false)
            .unwrap();
        let before = broker.usage().unwrap();
        assert!(
            matches!(broker.recover_from(&directory).await, Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
        );
        assert_eq!(broker.usage().unwrap(), before);
        assert_eq!(fs::read(&path).unwrap(), header);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        active.detach().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }
}
