use super::*;

#[test]
fn future_will_owner_index_handles_shared_deadline_and_due_promotion() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let deadline = now_ms() + 3_600_000;
    let mut keys = Vec::new();
    {
        let mut state = lock(&broker.state).unwrap();
        for index in 0..64 {
            let owner = auth(&format!("will-{index}")).device_key;
            let key = SessionKey {
                device: owner.clone(),
                client_id: format!("client-{index}"),
            };
            let pending = PendingWill {
                owner: owner.clone(),
                origin: Some(key.clone()),
                message: BrokerMessage {
                    topic: (format!("v1/t/t/p/p/d/will-{index}/up")).into(),
                    payload: vec![7; 32].into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
                due_at_ms: Some(deadline),
                cancel_on_resume: Some((key.clone(), 1)),
                message_expiry_interval: None,
                retained_reservation: RetainedReservation::default(),
            };
            reserve_will_capacity(&mut state, &owner.tenant_id, pending.bytes(), &limits).unwrap();
            insert_pending_will(&mut state, pending);
            keys.push(key);
        }
    }
    assert_broker_accounting(&broker);
    {
        let mut state = lock(&broker.state).unwrap();
        let unrelated = SessionKey {
            device: auth("unrelated").device_key,
            client_id: "unrelated".into(),
        };
        release_clean_start_delays(&mut state, &unrelated);
        cancel_resumed_wills(&mut state, &unrelated, 1);
        assert_eq!(pending_will_count(&state), 64);
        release_clean_start_delays(&mut state, &keys[0]);
        assert_eq!(state.pending_wills.len(), 1);
        cancel_resumed_wills(&mut state, &keys[1], 1);
        assert_eq!(pending_will_count(&state), 63);
        promote_due_wills(&mut state, deadline, 64);
        assert!(state.future_wills.is_empty());
        assert!(state.future_wills_by_session.is_empty());
    }
    assert_broker_accounting(&broker);
}

#[test]
fn delayed_will_deadlines_have_bounded_due_work() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    {
        let mut state = lock(&broker.state).unwrap();
        for index in 0..5 {
            let owner = auth(&format!("will-index-{index}")).device_key;
            let message = BrokerMessage {
                topic: (format!("v1/t/t/p/p/d/will-index-{index}/up")).into(),
                payload: vec![index as u8].into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            };
            reserve_will_capacity(&mut state, &owner.tenant_id, message.bytes(), &limits).unwrap();
            insert_pending_will(
                &mut state,
                PendingWill {
                    owner,
                    origin: None,
                    message,
                    due_at_ms: Some(now_ms() - 1),
                    cancel_on_resume: Some((
                        SessionKey {
                            device: auth(&format!("will-index-{index}")).device_key,
                            client_id: format!("client-{index}"),
                        },
                        1,
                    )),
                    message_expiry_interval: None,
                    retained_reservation: RetainedReservation::default(),
                },
            );
        }
        assert_accounting_consistent(&state);
        promote_due_wills(&mut state, now_ms(), 3);
        assert_eq!(state.pending_wills.len(), 3);
        assert_eq!(pending_will_count(&state), 5);
        assert_accounting_consistent(&state);
        assert_eq!(retry_pending_wills_bounded(&mut state, &limits, 3), 3);
        assert_eq!(pending_will_count(&state), 2);
        assert_accounting_consistent(&state);
    }
    broker.tick().unwrap();
    assert_eq!(broker.pending_will_count().unwrap(), 0);
    assert_broker_accounting(&broker);
}

#[test]
#[ignore = "manual baseline for future delayed-Will retry cost"]
fn benchmark_future_pending_will_retry() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let owner = auth("future-will").device_key;
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/future-will/up".into(),
        payload: vec![1; 64].into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let mut state = lock(&broker.state).unwrap();
    for _ in 0..10_000 {
        insert_pending_will(
            &mut state,
            PendingWill {
                owner: owner.clone(),
                origin: None,
                message: message.clone(),
                due_at_ms: Some(now_ms() + 3_600_000),
                cancel_on_resume: None,
                message_expiry_interval: None,
                retained_reservation: RetainedReservation::default(),
            },
        );
    }
    for run in 1..=5 {
        let mut samples = Vec::with_capacity(100);
        for _ in 0..100 {
            let started = std::time::Instant::now();
            assert_eq!(retry_pending_wills(&mut state, &broker.limits), 0);
            samples.push(started.elapsed().as_nanos());
        }
        samples.sort_unstable();
        println!(
            "WILL,future_10000,{run},{},{},{}",
            samples[50], samples[95], samples[99]
        );
    }
}

#[tokio::test]
async fn v5_delayed_will_is_bounded_cancelled_on_resume_and_recovers() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("will-delay");
    let topic = "v1/t/t/p/p/d/will-delay/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"delayed".to_vec().into(),
                qos: 1,
                retain: true,
                properties: (PublishProperties {
                    expires_at_ms: Some(i64::MAX),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    will.arm_v5(
        attachment.key.clone(),
        attachment.session_incarnation,
        attachment.generation,
        2,
        60,
        Some(5),
    );
    attachment.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_none());
    assert_eq!(broker.pending_will_count().unwrap(), 1);
    assert!(!broker.has_retained_topic(topic).unwrap());
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    assert!(resumed.session_present);
    assert_eq!(broker.pending_will_count().unwrap(), 0);
    resumed.detach().unwrap();

    let mut will = broker
        .reserve_will_for_session(
            resumed.key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"recover".to_vec().into(),
                qos: 1,
                retain: true,
                properties: (PublishProperties {
                    expires_at_ms: Some(i64::MAX),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    will.arm_v5(
        resumed.key.clone(),
        resumed.session_incarnation,
        resumed.generation,
        2,
        60,
        Some(5),
    );
    assert!(will.publish_v5().unwrap().is_none());
    let root = std::env::temp_dir().join(format!("netbaiot-v5-will-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&root).await.unwrap();
    let recovered = MqttBroker::new(limits);
    assert!(recovered.recover_from(&root).await.unwrap());
    assert_eq!(recovered.pending_will_count().unwrap(), 1);
    assert_eq!(
        all_pending_wills(&recovered.state.lock().unwrap())
            .next()
            .unwrap()
            .origin,
        Some(resumed.key.clone())
    );
    assert!(!recovered.has_retained_topic(topic).unwrap());
    {
        let mut state = recovered.state.lock().unwrap();
        let mut pending = take_owned_future_wills(&mut state, &resumed.key)
            .pop()
            .unwrap();
        pending.due_at_ms = Some(now_ms() - 1);
        insert_pending_will(&mut state, pending);
    }
    recovered.tick().unwrap();
    assert_eq!(recovered.pending_will_count().unwrap(), 0);
    assert!(recovered.has_retained_topic(topic).unwrap());
    let state = recovered.state.lock().unwrap();
    assert!(
        state
            .retained
            .get(topic)
            .unwrap()
            .message
            .properties
            .expires_at_ms
            .unwrap()
            > now_ms()
    );
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn takeover_same_session_positive_delay_suppresses_will() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("will-takeover");
    let topic = "v1/t/t/p/p/d/will-takeover/up";
    let mut old = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"old".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm_v5(
        old.key.clone(),
        old.session_incarnation,
        old.generation,
        30,
        60,
        None,
    );
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    assert_eq!(resumed.session_incarnation, old.session_incarnation);
    old.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_none());
    assert_eq!(broker.pending_will_count().unwrap(), 0);
    assert!(!broker.has_retained_topic(topic).unwrap());
    resumed.detach().unwrap();
}

#[test]
fn takeover_clean_start_publishes_old_will() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("will-clean-takeover");
    let topic = "v1/t/t/p/p/d/will-clean-takeover/up";
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"ended".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm_v5(
        resumed.key.clone(),
        resumed.session_incarnation,
        resumed.generation,
        30,
        60,
        None,
    );
    let mut fresh = broker
        .attach_v5(&device, "client".into(), true, 60, 4)
        .unwrap();
    assert!(!fresh.session_present);
    resumed.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_some());
    assert!(broker.has_retained_topic(topic).unwrap());
    fresh.detach().unwrap();
}

#[test]
fn takeover_same_session_zero_delay_publishes_will() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("immediate-will-takeover");
    let topic = "v1/t/t/p/p/d/immediate-will-takeover/up";
    let mut old = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"old".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm_v5(
        old.key.clone(),
        old.session_incarnation,
        old.generation,
        0,
        60,
        None,
    );
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    old.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_some());
    assert!(broker.has_retained_topic(topic).unwrap());
    resumed.detach().unwrap();
}

#[test]
fn takeover_clean_start_zero_delay_publishes_old_will() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("immediate-will-takeover");
    let topic = "v1/t/t/p/p/d/immediate-will-takeover/up";
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"new".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm_v5(
        resumed.key.clone(),
        resumed.session_incarnation,
        resumed.generation,
        0,
        60,
        None,
    );
    let mut fresh = broker
        .attach_v5(&device, "client".into(), true, 60, 4)
        .unwrap();
    resumed.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_some());
    assert!(broker.has_retained_topic(topic).unwrap());
    fresh.detach().unwrap();
}

#[test]
fn current_pending_will_record_preserves_explicit_absent_origin() {
    let limits = Arc::new(Limits::default());
    let owner = auth("unknown-origin-will").device_key;
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/unknown-origin-will/up".into(),
        payload: b"old".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let mut payload = Vec::new();
    put_string(&mut payload, owner.tenant_id.as_str()).unwrap();
    put_string(&mut payload, owner.product_id.as_str()).unwrap();
    put_string(&mut payload, owner.device_id.as_str()).unwrap();
    encode_message(&mut payload, &message).unwrap();
    payload.extend_from_slice(&[0, 0, 0]); // no delayed cancellation, expiry or origin
    let mut header = Vec::new();
    header.extend_from_slice(RECOVERY_MAGIC);
    header.extend_from_slice(&RECOVERY_VERSION.to_be_bytes());
    header.extend_from_slice(&1_u64.to_be_bytes());
    let mut record = Vec::new();
    record.push(RECORD_PENDING_WILL);
    record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&Sha256::digest(&payload));
    let mut stream_hash = Sha256::new();
    stream_hash.update(&header);
    stream_hash.update(&record);
    let mut image = header.clone();
    image.extend_from_slice(&Sha256::digest(&header));
    image.extend_from_slice(&record);
    image.extend_from_slice(RECOVERY_TRAILER_MAGIC);
    image.extend_from_slice(&1_u64.to_be_bytes());
    image.extend_from_slice(&(record.len() as u64).to_be_bytes());
    image.extend_from_slice(&stream_hash.finalize());
    let snapshot = decode_mqtt_recovery(&image, &limits).unwrap();
    assert_eq!(snapshot.format_version, RECOVERY_VERSION);
    assert_eq!(snapshot.pending_wills.len(), 1);
    assert_eq!(snapshot.pending_wills[0].origin, None);
    MqttBroker::new(limits).restore(snapshot).unwrap();
}

#[test]
fn will_origin_filters_no_local_after_takeover_without_suppressing_other_subscribers() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let identity = auth("will-origin");
    let topic = "v1/t/t/p/p/d/will-origin/up";
    let mut old = broker
        .attach_v5(&identity, "client".into(), false, 60, 4)
        .unwrap();
    let mut observer = broker
        .attach_v5(&identity, "observer".into(), false, 60, 4)
        .unwrap();
    let options = v5::SubscriptionOptions {
        qos: 1,
        no_local: true,
        retain_as_published: false,
        retain_handling: 0,
    };
    broker
        .subscribe_v5(&old.key, old.generation, topic, options)
        .unwrap();
    broker
        .subscribe_v5(&observer.key, observer.generation, topic, options)
        .unwrap();
    let mut will = broker
        .reserve_will_for_session(
            old.key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"will".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm_v5(
        old.key.clone(),
        old.session_incarnation,
        old.generation,
        0,
        60,
        None,
    );
    let mut resumed = broker
        .attach_v5(&identity, "client".into(), false, 60, 4)
        .unwrap();
    old.detach().unwrap();
    assert!(will.publish_v5().unwrap().is_some());
    assert!(resumed.receiver.try_recv().is_err());
    let BrokerFrame::Publish(delivery) = observer.receiver.try_recv().unwrap() else {
        panic!("other subscriber must receive the Will")
    };
    assert_eq!(delivery.message.payload.as_ref(), b"will");
    assert_eq!(
        broker
            .state
            .lock()
            .unwrap()
            .retained
            .get(topic)
            .unwrap()
            .origin,
        Some(resumed.key.clone())
    );
    broker
        .puback(
            &observer.key,
            observer.generation,
            delivery.packet_id.unwrap(),
        )
        .unwrap();
    resumed.detach().unwrap();
    observer.detach().unwrap();
}

#[test]
fn mqtt_accepted_will_reservation_survives_later_retained_pressure() {
    let limits = Arc::new(Limits {
        max_retained_messages: 1,
        max_retained_messages_per_tenant: 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("will");
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: "will/reserved".into(),
                payload: vec![0xff; 16].into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    {
        let state = broker.state.lock().unwrap();
        assert_eq!(state.retained_reserved_count, 1);
        assert!(state.retained_reserved_bytes > 0);
    }
    assert!(
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: "will/competitor".into(),
                    payload: vec![1].into(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .is_err()
    );
    will.arm();
    will.publish().unwrap();
    assert!(broker.has_retained_topic("will/reserved").unwrap());
    let state = broker.state.lock().unwrap();
    assert_eq!(state.retained_reserved_count, 0);
    assert_eq!(state.retained_reserved_bytes, 0);
}

#[tokio::test]
async fn will_subscriber_pressure_001() {
    let limits = Arc::new(Limits {
        max_offline_messages_per_session: 1,
        max_offline_messages_per_tenant: 8,
        max_offline_messages: 8,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("will-pressure");
    let will_topic = "v1/t/t/p/p/d/will-pressure/up";
    let fill_topic = "v1/t/t/p/p/d/will-pressure/fill";
    let mut a = broker.attach(&device, "a".into(), false).unwrap();
    let b = broker.attach(&device, "b".into(), false).unwrap();
    broker
        .subscribe(&a.key, a.generation, will_topic, 1)
        .unwrap();
    broker
        .subscribe(&b.key, b.generation, will_topic, 1)
        .unwrap();
    broker
        .subscribe(&b.key, b.generation, fill_topic, 1)
        .unwrap();
    broker.detach(&b.key, b.generation, false).unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: fill_topic.into(),
                payload: b"fill".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: will_topic.into(),
                payload: b"will".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm();
    will.publish().unwrap();
    assert_eq!(broker.pending_will_count().unwrap(), 1);
    assert!(
        a.receiver.try_recv().is_err(),
        "A must not observe a partial route"
    );

    let mut resumed_b = broker.attach(&device, "b".into(), false).unwrap();
    let BrokerFrame::Publish(fill) = resumed_b.receiver.recv().await.unwrap() else {
        panic!("expected queued fill")
    };
    broker
        .puback(
            &resumed_b.key,
            resumed_b.generation,
            fill.packet_id.unwrap(),
        )
        .unwrap();
    let BrokerFrame::Publish(a_will) = a.receiver.recv().await.unwrap() else {
        panic!("expected Will for A")
    };
    let BrokerFrame::Publish(b_will) = resumed_b.receiver.recv().await.unwrap() else {
        panic!("expected Will for B")
    };
    assert_eq!(a_will.message.payload.as_ref(), b"will");
    assert_eq!(b_will.message.payload.as_ref(), b"will");
    assert_eq!(broker.pending_will_count().unwrap(), 0);
}

#[tokio::test]
async fn will_pending_restart_001() {
    let limits = Arc::new(Limits {
        max_offline_messages_per_session: 1,
        max_offline_messages_per_tenant: 8,
        max_offline_messages: 8,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits.clone());
    let device = auth("will-restart");
    let topic = "v1/t/t/p/p/d/will-restart/up";
    let subscriber = broker.attach(&device, "persistent".into(), false).unwrap();
    broker
        .subscribe(&subscriber.key, subscriber.generation, topic, 1)
        .unwrap();
    broker
        .detach(&subscriber.key, subscriber.generation, false)
        .unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"fill".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let mut will = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"restart-will".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    will.arm();
    will.publish().unwrap();
    assert_eq!(broker.pending_will_count().unwrap(), 1);

    let directory = std::env::temp_dir().join(format!(
        "netbaiot-will-pending-{}-{}",
        std::process::id(),
        now_ms()
    ));
    broker.commit_to(&directory).await.unwrap();
    let recovered = MqttBroker::new(limits);
    recovered.recover_from(&directory).await.unwrap();
    assert_eq!(recovered.pending_will_count().unwrap(), 1);
    let mut resumed = recovered
        .attach(&device, "persistent".into(), false)
        .unwrap();
    let BrokerFrame::Publish(fill) = resumed.receiver.recv().await.unwrap() else {
        panic!("expected recovered fill")
    };
    recovered
        .puback(&resumed.key, resumed.generation, fill.packet_id.unwrap())
        .unwrap();
    let BrokerFrame::Publish(will) = resumed.receiver.recv().await.unwrap() else {
        panic!("expected recovered pending Will")
    };
    assert_eq!(will.message.payload.as_ref(), b"restart-will");
    assert_eq!(recovered.pending_will_count().unwrap(), 0);
    fs::remove_dir_all(directory).unwrap();
}
