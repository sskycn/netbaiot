use super::*;

#[test]
fn derived_accounting_matches_authoritative_mutation_sequence() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let identity = auth("accounting-sequence");
    let down = "v1/t/t/p/p/d/accounting-sequence/down";
    let up = "v1/t/t/p/p/d/accounting-sequence/up";
    let mut attachment = broker
        .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, down, 2)
        .unwrap();
    assert_broker_accounting(&broker);
    for step in 0..1_000u16 {
        let qos = if step % 2 == 0 { 1 } else { 2 };
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: down.into(),
                    payload: step.to_be_bytes().to_vec().into(),
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert_broker_accounting(&broker);
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected routed delivery")
        };
        let packet_id = delivery.packet_id.unwrap();
        if qos == 1 {
            broker
                .puback(&attachment.key, attachment.generation, packet_id)
                .unwrap();
        } else {
            broker
                .pubrec(&attachment.key, attachment.generation, packet_id)
                .unwrap();
            assert_broker_accounting(&broker);
            broker
                .pubcomp(&attachment.key, attachment.generation, packet_id)
                .unwrap();
        }
        assert_broker_accounting(&broker);
        if step % 7 == 0 {
            let inbound_id = step + 1;
            let inbound = BrokerMessage {
                topic: up.into(),
                payload: step.to_be_bytes().to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            };
            assert!(
                broker
                    .inbound_qos2(
                        &attachment.key,
                        attachment.generation,
                        inbound_id,
                        inbound.clone(),
                    )
                    .unwrap()
            );
            assert_broker_accounting(&broker);
            assert!(
                !broker
                    .inbound_qos2(&attachment.key, attachment.generation, inbound_id, inbound,)
                    .unwrap()
            );
            assert_broker_accounting(&broker);
            broker
                .complete_inbound_qos2(&attachment.key, attachment.generation, inbound_id)
                .unwrap();
            assert_broker_accounting(&broker);
        }
        if step % 11 == 0 {
            broker
                .subscribe(&attachment.key, attachment.generation, down, 1)
                .unwrap();
            assert_broker_accounting(&broker);
            broker
                .unsubscribe(&attachment.key, attachment.generation, down)
                .unwrap();
            assert_broker_accounting(&broker);
            broker
                .subscribe(&attachment.key, attachment.generation, down, 2)
                .unwrap();
            assert_broker_accounting(&broker);
        }
        if step % 19 == 0 {
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: up.into(),
                        payload: vec![1, 2, 3].into(),
                        qos: 0,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert_broker_accounting(&broker);
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: up.into(),
                        payload: Vec::new().into(),
                        qos: 0,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert_broker_accounting(&broker);
        }
        if step % 23 == 0 {
            attachment.detach().unwrap();
            assert_broker_accounting(&broker);
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: down.into(),
                        payload: vec![4, 5, 6].into(),
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert_broker_accounting(&broker);
            attachment = broker
                .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
                .unwrap();
            assert_broker_accounting(&broker);
            let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                panic!("expected resumed offline delivery")
            };
            broker
                .puback(
                    &attachment.key,
                    attachment.generation,
                    delivery.packet_id.unwrap(),
                )
                .unwrap();
            assert_broker_accounting(&broker);
        }
        if step % 101 == 0 {
            attachment.detach().unwrap();
            assert_broker_accounting(&broker);
            let recovered = MqttBroker::new(Arc::new(Limits::default()));
            recovered.restore(broker.snapshot().unwrap()).unwrap();
            assert_broker_accounting(&recovered);
            attachment = broker
                .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
                .unwrap();
            assert_broker_accounting(&broker);
        }
        if step % 173 == 0 {
            attachment.detach().unwrap();
            attachment = broker
                .attach_v5(&identity, "accounting".into(), true, 3_600, 32)
                .unwrap();
            broker
                .subscribe(&attachment.key, attachment.generation, down, 2)
                .unwrap();
            assert_broker_accounting(&broker);
        }
    }
}

#[test]
fn derived_accounting_tracks_tenant_device_and_restore() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let first = auth("shared-device");
    let mut other_tenant = auth("other-device");
    other_tenant.device_key.tenant_id = TenantId::new("other").unwrap();
    let down = "v1/t/t/p/p/d/shared-device/down";
    let mut a = broker.attach(&first, "a".into(), false).unwrap();
    let mut b = broker.attach(&first, "b".into(), false).unwrap();
    let mut c = broker.attach(&other_tenant, "c".into(), false).unwrap();
    assert_broker_accounting(&broker);
    broker.subscribe(&a.key, a.generation, down, 1).unwrap();
    broker.subscribe(&b.key, b.generation, down, 1).unwrap();
    broker
        .subscribe(
            &c.key,
            c.generation,
            "v1/t/other/p/p/d/other-device/down",
            1,
        )
        .unwrap();
    assert_broker_accounting(&broker);
    b.detach().unwrap();
    assert_broker_accounting(&broker);
    broker
        .route(
            &first.device_key,
            BrokerMessage {
                topic: down.into(),
                payload: b"first".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert_broker_accounting(&broker);
    let BrokerFrame::Publish(live) = a.receiver.try_recv().unwrap() else {
        panic!("expected live copy")
    };
    broker
        .puback(&a.key, a.generation, live.packet_id.unwrap())
        .unwrap();
    assert_broker_accounting(&broker);
    b = broker.attach(&first, "b".into(), false).unwrap();
    assert_broker_accounting(&broker);
    let BrokerFrame::Publish(resumed) = b.receiver.try_recv().unwrap() else {
        panic!("expected resumed copy")
    };
    broker
        .puback(&b.key, b.generation, resumed.packet_id.unwrap())
        .unwrap();
    assert_broker_accounting(&broker);
    let inbound = BrokerMessage {
        topic: "v1/t/t/p/p/d/shared-device/up".into(),
        payload: b"inbound".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    assert!(
        broker
            .inbound_qos2(&a.key, a.generation, 77, inbound.clone())
            .unwrap()
    );
    assert!(
        !broker
            .inbound_qos2(&a.key, a.generation, 77, inbound)
            .unwrap()
    );
    assert_broker_accounting(&broker);
    broker
        .complete_inbound_qos2(&a.key, a.generation, 77)
        .unwrap();
    assert_broker_accounting(&broker);
    b.detach().unwrap();
    let mut replaced = broker.attach(&first, "b".into(), true).unwrap();
    assert_broker_accounting(&broker);
    a.detach().unwrap();
    c.detach().unwrap();
    replaced.detach().unwrap();
    let restored = MqttBroker::new(Arc::new(Limits::default()));
    restored.restore(broker.snapshot().unwrap()).unwrap();
    assert_broker_accounting(&restored);
}

#[test]
fn derived_accounting_survives_local_discard_expiry_and_limit_rejection() {
    let limits = Arc::new(Limits {
        max_subscriptions_per_connection: 1,
        max_subscriptions_per_session: 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let identity = auth("accounting-faults");
    let down = "v1/t/t/p/p/d/accounting-faults/down";
    let mut attachment = broker
        .attach_v5(&identity, "faults".into(), false, 3_600, 32)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, down, 2)
        .unwrap();
    assert!(matches!(
        broker.subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/accounting-faults/up",
            1,
        ),
        Err(Error::Overloaded)
    ));
    assert_broker_accounting(&broker);
    for qos in [1, 2] {
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: down.into(),
                    payload: vec![1; 64].into(),
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected outbound publish")
        };
        assert_broker_accounting(&broker);
        if qos == 1 {
            assert!(
                broker
                    .discard_outbound(&attachment.key, attachment.generation, &delivery)
                    .unwrap()
            );
        } else {
            broker
                .pubrec_rejected(
                    &attachment.key,
                    attachment.generation,
                    delivery.packet_id.unwrap(),
                )
                .unwrap();
        }
        assert_broker_accounting(&broker);
    }
    broker
        .route(
            &identity.device_key,
            BrokerMessage {
                topic: down.into(),
                payload: vec![2; 64].into(),
                qos: 1,
                retain: false,
                properties: (PublishProperties {
                    expires_at_ms: Some(now_ms() + 10),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected expiring publish")
    };
    std::thread::sleep(Duration::from_millis(15));
    assert!(
        !broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
            .unwrap()
    );
    assert_broker_accounting(&broker);
    attachment.detach().unwrap();
    broker
        .route(
            &identity.device_key,
            BrokerMessage {
                topic: down.into(),
                payload: vec![3; 64].into(),
                qos: 1,
                retain: false,
                properties: (PublishProperties {
                    expires_at_ms: Some(now_ms() + 10),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    assert_broker_accounting(&broker);
    std::thread::sleep(Duration::from_millis(15));
    broker.tick().unwrap();
    assert_broker_accounting(&broker);
}

#[test]
fn inbound_qos2_repeated_publish_does_not_change_accounting() {
    let (broker, mut attachment, mut message) = qos2_duplicate_case();
    let before = transaction_accounting(&broker, &attachment.key);
    message.payload = b"different-and-larger".to_vec().into();
    message.retain = false;
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, message)
            .unwrap()
    );
    assert_eq!(transaction_accounting(&broker, &attachment.key), before);
    attachment.detach().unwrap();
}

#[test]
fn v5_expired_session_releases_all_accounting_before_reconnect() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let auth = auth("a");
    let mut attachment = broker
        .attach_v5(&auth, "client".into(), false, 1, u16::MAX)
        .unwrap();
    let key = attachment.key.clone();
    let filter = format!("v1/t/t/p/p/d/{}/up", auth.device_key.device_id.as_str());
    broker
        .subscribe(&key, attachment.generation, &filter, 1)
        .unwrap();
    attachment.detach().unwrap();
    {
        let mut state = broker.state.lock().unwrap();
        state.sessions.get_mut(&key).unwrap().expires_at_ms = Some(now_ms() - 1);
    }
    let mut fresh = broker
        .attach_v5(&auth, "client".into(), false, 1, u16::MAX)
        .unwrap();
    assert!(!fresh.session_present);
    assert_eq!(broker.usage().unwrap().2, 0);
    fresh.detach().unwrap();
}
