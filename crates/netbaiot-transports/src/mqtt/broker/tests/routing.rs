use super::*;

#[test]
fn shared_payload_fanout_preserves_each_logical_responsibility() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("shared");
    let topic = "v1/t/t/p/p/d/shared/down";
    let mut attachments = Vec::new();
    for client in ["one", "two"] {
        let attachment = broker.attach(&device, client.into(), false).unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        attachments.push(attachment);
    }
    let before = broker.usage().unwrap();
    let message = BrokerMessage {
        topic: topic.into(),
        payload: vec![7; 16384].into(),
        qos: 1,
        retain: true,
        properties: Default::default(),
    };
    let charge = message.bytes();
    assert_eq!(
        broker.route(&device.device_key, message.clone()).unwrap(),
        2
    );
    {
        let state = broker.state.lock().unwrap();
        assert_eq!(state.session_bytes, before.1 + 2 * charge);
        assert_eq!(state.retained_bytes, charge);
        assert_eq!(
            state.tenant_usage[&device.device_key.tenant_id].qos1_inflight,
            2
        );
        assert_eq!(
            state.retained[topic].message.payload.as_ptr(),
            message.payload.as_ptr()
        );
        for session in state.sessions.values() {
            let OutboundState::AwaitPuback(saved) = session.outbound.values().next().unwrap()
            else {
                panic!("qos1");
            };
            assert_eq!(saved.payload.as_ptr(), message.payload.as_ptr());
        }
        assert_accounting_consistent(&state);
    }
    assert_eq!(
        broker.global_outbound_bytes.available(),
        limits.max_outbound_bytes - 2 * charge
    );
    for attachment in &mut attachments {
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("publish");
        };
        assert_eq!(delivery.message.payload.as_ptr(), message.payload.as_ptr());
        let id = delivery.packet_id.unwrap();
        drop(delivery);
        broker
            .puback(&attachment.key, attachment.generation, id)
            .unwrap();
    }
    assert_eq!(broker.usage().unwrap().1, before.1);
    assert_eq!(
        broker.global_outbound_bytes.available(),
        limits.max_outbound_bytes
    );
    assert_broker_accounting(&broker);
}

#[test]
fn shared_payload_cannot_bypass_atomic_fanout_byte_limits() {
    let device = auth("shared-limit");
    let topic = "v1/t/t/p/p/d/shared-limit/down";
    let message = BrokerMessage {
        topic: topic.into(),
        payload: vec![7; 16384].into(),
        qos: 1,
        retain: true,
        properties: Default::default(),
    };
    let charge = message.bytes();
    let limits = Arc::new(Limits {
        max_outbound_bytes: charge,
        max_outbound_bytes_per_tenant: charge,
        // A second responsibility cannot fall back to the bounded offline
        // queue either. Sharing the physical buffer must not admit it.
        max_offline_bytes_per_session: charge - 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let mut attachments = Vec::new();
    for client in ["one", "two"] {
        let attachment = broker.attach(&device, client.into(), false).unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        attachments.push(attachment);
    }
    let before = broker.usage().unwrap();
    assert!(matches!(
        broker.route(&device.device_key, message),
        Err(Error::Overloaded)
    ));
    assert_eq!(broker.usage().unwrap(), before);
    assert_eq!(broker.global_outbound_bytes.available(), charge);
    assert!(
        attachments
            .iter_mut()
            .all(|a| a.receiver.try_recv().is_err())
    );
    assert_broker_accounting(&broker);
}

#[test]
fn expired_retained_is_not_replayed_while_maintenance_is_budgeted() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let mut topics = Vec::new();
    for index in 0..=(HOT_MAINTENANCE_BUDGET * 2) {
        let identity = auth(&format!("retained-deadline-{index}"));
        let topic = format!("v1/t/t/p/p/d/retained-deadline-{index}/up");
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: topic.clone(),
                    payload: vec![1].into(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        topics.push((identity, topic));
    }
    let past = now_ms() - 1;
    {
        let mut state = lock(&broker.state).unwrap();
        for (index, (_, topic)) in topics.iter().enumerate() {
            let deadline = if index == HOT_MAINTENANCE_BUDGET * 2 {
                past
            } else {
                past - 1
            };
            state
                .retained
                .get_mut(topic)
                .unwrap()
                .message
                .properties
                .expires_at_ms = Some(deadline);
            state.retained_expiry.update(topic.clone(), Some(deadline));
        }
    }
    let (identity, topic) = &topics[HOT_MAINTENANCE_BUDGET * 2];
    let attachment = broker
        .attach_v5(identity, "retained-reader".into(), false, 3_600, 32)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    assert!(attachment.receiver.is_empty());
    assert!(!broker.has_retained_topic(topic).unwrap());
    assert_broker_accounting(&broker);
}

#[test]
fn mqtt_retained_replay_obeys_byte_budget() {
    let device = auth("retained-byte");
    let first_topic = "v1/t/t/p/p/d/retained-byte/up";
    let second_topic = "v1/t/t/p/p/d/retained-byte/down_ack";
    let message = |topic: &str| BrokerMessage {
        topic: topic.into(),
        payload: vec![9; 512].into(),
        qos: 0,
        retain: true,
        properties: Default::default(),
    };
    let charge = message(second_topic).bytes();
    let limits = Arc::new(Limits {
        max_outbound_bytes_per_connection: charge,
        max_outbound_bytes_per_tenant: charge * 2,
        max_outbound_bytes: charge * 3,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    broker
        .route(&device.device_key, message(first_topic))
        .unwrap();
    broker
        .route(&device.device_key, message(second_topic))
        .unwrap();
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    let wildcard = "v1/t/t/p/p/d/retained-byte/#";
    assert!(
        broker
            .subscribe(&attachment.key, attachment.generation, wildcard, 0)
            .is_err()
    );
    assert_eq!(
        broker
            .subscription_qos(&attachment.key, first_topic)
            .unwrap(),
        None
    );
    assert_eq!(broker.global_outbound_bytes.available(), charge * 3);
    broker
        .subscribe(&attachment.key, attachment.generation, first_topic, 0)
        .unwrap();
    let frame = attachment.receiver.try_recv().unwrap();
    assert!(matches!(frame, BrokerFrame::Publish(_)));
    assert!(broker.global_outbound_bytes.available() < charge * 3);
    drop(frame);
    assert_eq!(broker.global_outbound_bytes.available(), charge * 3);
    attachment.detach().unwrap();
}

#[test]
fn mqtt_route_preflight_bounded_memory_001() {
    let broker = route_projection_fixture(1_000, 8 * 1024);
    let state = broker.state.lock().unwrap();
    let stored_payload_bytes = state.offline_bytes;
    let message = BrokerMessage {
        topic: "route/plan/shared".into(),
        payload: b"one-message".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let plan = preflight_route(
        &state,
        &auth("route-plan").device_key,
        None,
        &message,
        &broker.limits,
        message.bytes(),
    )
    .unwrap();
    assert_eq!(plan.targets.len(), 1_000);
    assert!(stored_payload_bytes > 8_000_000);
    assert!(plan.temporary_bytes() < 512 * 1024);
    assert!(plan.temporary_bytes() < stored_payload_bytes / 16);
}

#[test]
fn mqtt_empty_subscription_route_hint_tracks_authoritative_state_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("route-hint");
    let mut attachment = broker
        .attach(&device, "route-hint-client".into(), false)
        .unwrap();
    let topic = "v1/t/t/p/p/d/route-hint/down";
    let message = BrokerMessage {
        topic: topic.into(),
        payload: b"payload".to_vec().into(),
        qos: 0,
        retain: false,
        properties: Default::default(),
    };

    assert_eq!(broker.subscription_count.load(Ordering::Acquire), 0);
    assert_eq!(
        broker.route(&device.device_key, message.clone()).unwrap(),
        0
    );
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 0)
        .unwrap();
    assert_eq!(broker.subscription_count.load(Ordering::Acquire), 1);
    assert_eq!(
        broker.route(&device.device_key, message.clone()).unwrap(),
        1
    );
    assert!(attachment.receiver.try_recv().is_ok());

    broker
        .unsubscribe(&attachment.key, attachment.generation, topic)
        .unwrap();
    assert_eq!(broker.subscription_count.load(Ordering::Acquire), 0);
    assert_eq!(broker.route(&device.device_key, message).unwrap(), 0);
}

#[test]
fn mqtt_empty_subscription_route_hint_is_restored_and_cleaned_001() {
    let source = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("route-hint-restore");
    let mut attachment = source.attach(&device, "persistent".into(), false).unwrap();
    source
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/route-hint-restore/down",
            1,
        )
        .unwrap();
    attachment.detach().unwrap();

    let recovered = MqttBroker::new(Arc::new(Limits::default()));
    recovered.restore(source.snapshot().unwrap()).unwrap();
    assert_eq!(recovered.subscription_count.load(Ordering::Acquire), 1);
    let clean = recovered
        .attach(&device, "persistent".into(), true)
        .unwrap();
    assert_eq!(recovered.subscription_count.load(Ordering::Acquire), 0);
    drop(clean);
}

#[test]
#[ignore = "manual 100/1000/configured-max route preflight benchmark"]
fn mqtt_route_preflight_benchmark_manual() {
    for targets in [100usize, 1_000, 2_000] {
        let broker = route_projection_fixture(targets, 0);
        let state = broker.state.lock().unwrap();
        let started = std::time::Instant::now();
        let message = BrokerMessage {
            topic: "route/plan/shared".into(),
            payload: b"benchmark".to_vec().into(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        };
        let plan = preflight_route(
            &state,
            &auth("route-plan").device_key,
            None,
            &message,
            &broker.limits,
            message.bytes(),
        )
        .unwrap();
        println!(
            "route-targets={targets} planning_us={} temporary_plan_bytes={}",
            started.elapsed().as_micros(),
            plan.temporary_bytes()
        );
    }
}

#[test]
fn retained_replacement_reservation_survives_old_value_deletion() {
    let limits = Arc::new(Limits {
        max_retained_messages: 2,
        max_retained_messages_per_tenant: 2,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("retain-aba");
    let a = "v1/t/t/p/p/d/retain-aba/up";
    let b = "v1/t/t/p/p/d/retain-aba/down_ack";
    let retained = |topic: &str, payload: &[u8], qos| BrokerMessage {
        topic: topic.into(),
        payload: bytes::Bytes::copy_from_slice(payload),
        qos,
        retain: true,
        properties: Default::default(),
    };
    broker
        .route(&device.device_key, retained(a, b"old", 1))
        .unwrap();
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            9,
            retained(a, b"replacement", 2),
        )
        .unwrap();
    assert_eq!(broker.state.lock().unwrap().retained_reserved_count, 1);
    broker
        .route(&device.device_key, retained(a, b"", 0))
        .unwrap();
    broker
        .route(&device.device_key, retained(b, b"competitor", 1))
        .unwrap();
    assert!(
        broker
            .route(&device.device_key, retained("extra/topic", b"extra", 1))
            .is_err(),
        "a third retained topic must not steal the accepted replacement's slot"
    );
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 9)
        .unwrap()
    else {
        panic!("expected accepted QoS2 transaction");
    };
    broker
        .finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 9, operation_id)
        .unwrap();
    broker
        .route_inbound_qos2(
            &attachment.key,
            session_incarnation,
            9,
            operation_id,
            &device.device_key,
        )
        .unwrap();
    assert!(broker.has_retained_topic(a).unwrap());
    assert!(broker.has_retained_topic(b).unwrap());
    assert_eq!(broker.state.lock().unwrap().retained_reserved_count, 0);
    attachment.detach().unwrap();
}

#[test]
fn persistent_unsubscribe_commits_session_trie_and_offline_removal() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("persistent-unsub");
    let topic = "v1/t/t/p/p/d/persistent-unsub/up";
    let first = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .subscribe(&first.key, first.generation, topic, 1)
        .unwrap();
    broker.detach(&first.key, first.generation, false).unwrap();

    let resumed = broker.attach(&device, "client".into(), false).unwrap();
    assert!(resumed.session_present);
    broker
        .unsubscribe(&resumed.key, resumed.generation, topic)
        .unwrap();
    {
        let state = broker.state.lock().unwrap();
        let session = state.sessions.get(&resumed.key).unwrap();
        assert!(!session.subscriptions.contains_key(topic));
        assert!(state.trie.matching(topic).is_empty());
    }
    broker
        .detach(&resumed.key, resumed.generation, false)
        .unwrap();

    assert_eq!(
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"stale".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap(),
        0
    );
    {
        let state = broker.state.lock().unwrap();
        let session = state.sessions.get(&resumed.key).unwrap();
        assert!(session.offline.is_empty());
        assert_eq!(state.offline_count, 0);
    }

    let verify = broker.attach(&device, "client".into(), false).unwrap();
    assert!(verify.session_present);
    assert!(verify.receiver.is_empty());
    broker
        .subscribe(&verify.key, verify.generation, topic, 1)
        .unwrap();
    broker
        .detach(&verify.key, verify.generation, false)
        .unwrap();
    assert_eq!(
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"fresh".to_vec().into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap(),
        1
    );
    let state = broker.state.lock().unwrap();
    assert_eq!(state.sessions.get(&verify.key).unwrap().offline.len(), 1);
}

#[tokio::test]
async fn retained_offline_and_outbound_qos_lifecycle_are_bounded() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let a = auth("a");
    let mut attachment = broker.attach(&a, "a".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/a/#",
            2,
        )
        .unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/a/up".into(),
        payload: b"retained".to_vec().into(),
        qos: 2,
        retain: true,
        properties: Default::default(),
    };
    broker.route(&a.device_key, message).unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
        panic!()
    };
    let id = delivery.packet_id.unwrap();
    assert!(matches!(
        broker
            .pubrec(&attachment.key, attachment.generation, id)
            .unwrap(),
        BrokerFrame::Pubrel { dup: false, .. }
    ));
    assert!(matches!(
        broker
            .pubrec(&attachment.key, attachment.generation, id)
            .unwrap(),
        BrokerFrame::Pubrel { dup: true, .. }
    ));
    broker
        .pubcomp(&attachment.key, attachment.generation, id)
        .unwrap();
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();
    broker
        .route(
            &a.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/a/up".into(),
                payload: b"offline".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let mut resumed = broker.attach(&a, "a".into(), false).unwrap();
    assert!(matches!(
        resumed.receiver.recv().await,
        Some(BrokerFrame::Publish(_))
    ));
}

#[test]
fn retained_replay_capacity_failure_does_not_commit_subscription_or_trie() {
    let broker = MqttBroker::new(Arc::new(Limits {
        max_outbound_messages_per_connection: 1,
        ..Limits::default()
    }));
    let owner = auth("publisher");
    for suffix in ["one", "two"] {
        broker
            .route(
                &owner.device_key,
                BrokerMessage {
                    topic: format!("v1/t/t/p/p/d/publisher/{suffix}"),
                    payload: suffix.as_bytes().to_vec().into(),
                    qos: 0,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let subscriber = auth("subscriber");
    let mut attachment = broker
        .attach(&subscriber, "transactional-subscribe".into(), true)
        .unwrap();
    let filter = "v1/t/t/p/p/d/publisher/#";
    assert!(
        broker
            .subscribe(&attachment.key, attachment.generation, filter, 0)
            .is_err()
    );
    assert_eq!(
        broker.subscription_qos(&attachment.key, filter).unwrap(),
        None
    );
    broker
        .route(
            &owner.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/publisher/future".into(),
                payload: b"future".to_vec().into(),
                qos: 0,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert!(attachment.receiver.try_recv().is_err());
}

#[tokio::test]
async fn routing_uses_minimum_qos_and_retained_replace_delete_semantics() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let a = auth("matrix");
    let mut attachment = broker.attach(&a, "matrix".into(), false).unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/matrix/+",
            1,
        )
        .unwrap();
    for publish_qos in 0..=2 {
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/matrix/up".into(),
                    payload: vec![publish_qos].into(),
                    qos: publish_qos,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected routed publish")
        };
        assert_eq!(delivery.message.qos, publish_qos.min(1));
        assert!(!delivery.message.retain);
        if let Some(packet_id) = delivery.packet_id
            && delivery.message.qos == 1
        {
            broker
                .puback(&attachment.key, attachment.generation, packet_id)
                .unwrap();
        }
    }
    assert_eq!(broker.usage().unwrap().3, 1, "retained value is replaced");
    broker
        .route(
            &a.device_key,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/matrix/up".into(),
                payload: Vec::new().into(),
                qos: 0,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert_eq!(
        broker.usage().unwrap().3,
        0,
        "zero payload deletes retained"
    );
}
