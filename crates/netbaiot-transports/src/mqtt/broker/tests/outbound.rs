use super::*;

#[tokio::test]
async fn v5_client_receive_maximum_defers_second_publish_until_ack() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("receive-max");
    let topic = "v1/t/t/p/p/d/receive-max/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe_v5(
            &attachment.key,
            attachment.generation,
            topic,
            v5::SubscriptionOptions {
                qos: 1,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
        )
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        broker
            .route_from_session(
                &attachment.key,
                &BrokerMessage {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first PUBLISH")
    };
    assert_eq!(first.message.payload.as_ref(), b"first");
    assert!(attachment.receiver.try_recv().is_err());
    assert_eq!(broker.state.lock().unwrap().offline_count, 1);
    broker
        .puback(
            &attachment.key,
            attachment.generation,
            first.packet_id.unwrap(),
        )
        .unwrap();
    let second = match attachment.receiver.try_recv() {
        Ok(frame) => frame,
        Err(_) => broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
            .unwrap(),
    };
    let BrokerFrame::Publish(second) = second else {
        panic!("expected deferred PUBLISH")
    };
    assert_eq!(second.message.payload.as_ref(), b"second");
    assert_eq!(broker.state.lock().unwrap().offline_count, 0);
    attachment.detach().unwrap();
}

#[tokio::test]
async fn receive_maximum_1_qos2_waits_for_pubcomp() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("qos2-window");
    let topic = "v1/t/t/p/p/d/qos2-window/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        broker
            .route_from_session(
                &attachment.key,
                &BrokerMessage {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first QoS2 PUBLISH")
    };
    let id = first.packet_id.unwrap();
    assert_eq!(first.message.payload.as_ref(), b"first");
    assert!(attachment.receiver.try_recv().is_err());
    assert!(matches!(
        broker.pubrec(&attachment.key, attachment.generation, id).unwrap(),
        BrokerFrame::Pubrel { packet_id, dup: false } if packet_id == id
    ));
    assert!(
        broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
            .is_none()
    );
    assert!(attachment.receiver.try_recv().is_err());
    assert!(matches!(
        broker.pubrec(&attachment.key, attachment.generation, id).unwrap(),
        BrokerFrame::Pubrel { packet_id, dup: true } if packet_id == id
    ));
    assert!(!broker.state.lock().unwrap().sessions[&attachment.key].has_send_quota());
    broker
        .pubcomp(&attachment.key, attachment.generation, id)
        .unwrap();
    assert!(matches!(
        broker.pubcomp(&attachment.key, attachment.generation, id),
        Err(Error::Invalid)
    ));
    let second = match attachment.receiver.try_recv() {
        Ok(frame) => frame,
        Err(_) => broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
            .unwrap(),
    };
    let BrokerFrame::Publish(second) = second else {
        panic!("expected second QoS2 PUBLISH")
    };
    assert_eq!(second.message.payload.as_ref(), b"second");
    attachment.detach().unwrap();
}

#[tokio::test]
async fn qos2_negative_pubrec_releases_send_quota() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("qos2-rejected");
    let topic = "v1/t/t/p/p/d/qos2-rejected/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        broker
            .route_from_session(
                &attachment.key,
                &BrokerMessage {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first PUBLISH")
    };
    broker
        .pubrec_rejected(
            &attachment.key,
            attachment.generation,
            first.packet_id.unwrap(),
        )
        .unwrap();
    let second = broker
        .next_offline(&attachment.key, attachment.generation)
        .unwrap()
        .or_else(|| attachment.receiver.try_recv().ok())
        .unwrap();
    assert!(
        matches!(second, BrokerFrame::Publish(delivery) if delivery.message.payload.as_ref() == b"second")
    );
    attachment.detach().unwrap();
}

#[tokio::test]
async fn reconnect_await_pubcomp_does_not_consume_new_send_window() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("qos2-resume-window");
    let topic = "v1/t/t/p/p/d/qos2-resume-window/up";
    let mut old = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&old.key, old.generation, topic, 2)
        .unwrap();
    for payload in [b"first".to_vec(), b"second".to_vec()] {
        broker
            .route_from_session(
                &old.key,
                &BrokerMessage {
                    topic: topic.into(),
                    payload: payload.into(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let BrokerFrame::Publish(first) = old.receiver.try_recv().unwrap() else {
        panic!("expected first PUBLISH")
    };
    let first_id = first.packet_id.unwrap();
    broker.pubrec(&old.key, old.generation, first_id).unwrap();
    old.detach().unwrap();
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    assert!(resumed.session_present);
    assert!(matches!(
        resumed.receiver.try_recv().unwrap(),
        BrokerFrame::Pubrel { packet_id, dup: true } if packet_id == first_id
    ));
    let second = resumed.receiver.try_recv().unwrap();
    assert!(matches!(second, BrokerFrame::Publish(_)));
    resumed.detach().unwrap();
}

#[test]
fn v5_server_receive_maximum_counts_qos1_against_pending_qos2() {
    let limits = Limits {
        max_inflight_qos1_per_session: 2,
        max_inflight_qos2_per_session: 2,
        ..Default::default()
    };
    let broker = MqttBroker::new(Arc::new(limits));
    let device = auth("server-receive-max");
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/server-receive-max/up".into(),
        payload: b"data".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    for id in [1, 2] {
        assert!(
            broker
                .inbound_receive_available(&attachment.key, attachment.generation, 2, id)
                .unwrap()
        );
        broker
            .inbound_qos2(&attachment.key, attachment.generation, id, message.clone())
            .unwrap();
    }
    assert!(
        broker
            .inbound_receive_available(&attachment.key, attachment.generation, 2, 1)
            .unwrap()
    );
    assert!(
        !broker
            .inbound_receive_available(&attachment.key, attachment.generation, 1, 3)
            .unwrap()
    );
    assert!(
        !broker
            .inbound_receive_available(&attachment.key, attachment.generation, 2, 3)
            .unwrap()
    );
    attachment.detach().unwrap();
}

#[test]
fn v5_no_local_uses_client_id_and_retain_options_control_replay() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let auth = auth("a");
    let topic = "v1/t/t/p/p/d/a/up";
    let mut a = broker
        .attach_v5(&auth, "client-a".into(), false, 60, u16::MAX)
        .unwrap();
    let mut b = broker
        .attach_v5(&auth, "client-b".into(), false, 60, u16::MAX)
        .unwrap();
    let options = v5::SubscriptionOptions {
        qos: 1,
        no_local: true,
        retain_as_published: true,
        retain_handling: 0,
    };
    broker
        .subscribe_v5(&a.key, a.generation, topic, options)
        .unwrap();
    broker
        .subscribe_v5(&b.key, b.generation, topic, options)
        .unwrap();
    broker
        .route_from_session(
            &a.key,
            &BrokerMessage {
                topic: topic.into(),
                payload: b"value".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert!(a.receiver.try_recv().is_err());
    let BrokerFrame::Publish(delivery) = b.receiver.try_recv().unwrap() else {
        panic!("expected PUBLISH")
    };
    assert!(delivery.message.retain);
    broker
        .puback(&b.key, b.generation, delivery.packet_id.unwrap())
        .unwrap();
    broker
        .subscribe_v5(
            &b.key,
            b.generation,
            topic,
            v5::SubscriptionOptions {
                retain_handling: 1,
                ..options
            },
        )
        .unwrap();
    assert!(b.receiver.try_recv().is_err());
    broker
        .subscribe_v5(
            &b.key,
            b.generation,
            topic,
            v5::SubscriptionOptions {
                retain_handling: 0,
                ..options
            },
        )
        .unwrap();
    assert!(matches!(b.receiver.try_recv(), Ok(BrokerFrame::Publish(_))));
    let mut c = broker
        .attach_v5(&auth, "client-c".into(), false, 60, u16::MAX)
        .unwrap();
    broker
        .subscribe_v5(
            &c.key,
            c.generation,
            topic,
            v5::SubscriptionOptions {
                retain_handling: 2,
                ..options
            },
        )
        .unwrap();
    assert!(c.receiver.try_recv().is_err());
    a.detach().unwrap();
    b.detach().unwrap();
    c.detach().unwrap();
}

#[test]
fn ownership_clean_session_qos2_and_snapshot() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits);
    let a = auth("a");
    let b = auth("b");
    let first = broker.attach(&a, "same".into(), false).unwrap();
    broker
        .subscribe(&first.key, first.generation, "v1/t/t/p/p/d/a/#", 2)
        .unwrap();
    broker.detach(&first.key, first.generation, false).unwrap();
    let resumed = broker.attach(&a, "same".into(), false).unwrap();
    assert!(resumed.session_present);
    let other = broker.attach(&b, "same".into(), false).unwrap();
    assert!(!other.session_present);
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/a/up".into(),
        payload: b"x".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    assert!(
        broker
            .inbound_qos2(&resumed.key, resumed.generation, 7, message.clone())
            .unwrap()
    );
    assert!(
        !broker
            .inbound_qos2(&resumed.key, resumed.generation, 7, message.clone())
            .unwrap()
    );
    assert_eq!(
        broker
            .inbound_qos2_message(&resumed.key, resumed.generation, 7)
            .unwrap(),
        Some((message, false))
    );
    broker
        .complete_inbound_qos2(&resumed.key, resumed.generation, 7)
        .unwrap();
    assert_eq!(
        broker
            .inbound_qos2_message(&resumed.key, resumed.generation, 7)
            .unwrap(),
        None
    );
    let snapshot = broker.snapshot().unwrap();
    let restored = MqttBroker::new(Arc::new(Limits::default()));
    restored.restore(snapshot).unwrap();
    assert!(
        restored
            .attach(&a, "same".into(), false)
            .unwrap()
            .session_present
    );
    let clean = restored.attach(&a, "same".into(), true).unwrap();
    assert!(!clean.session_present);
    assert_eq!(
        restored
            .subscription_qos(&clean.key, "v1/t/t/p/p/d/a/up")
            .unwrap(),
        None
    );
}

#[test]
fn tenant_session_bytes_and_qos2_inflight_are_hard_bounded() {
    let limits = Arc::new(Limits {
        max_mqtt_session_state_bytes_per_tenant: 100,
        max_inflight_qos2_per_session: 1,
        max_inflight_qos2_per_tenant: 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let a = auth("a");
    let first = broker.attach(&a, "a".into(), false).unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/a/up".into(),
        payload: b"x".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    assert!(
        broker
            .inbound_qos2(&first.key, first.generation, 1, message)
            .is_err(),
        "the message charge must not exceed the tenant byte ceiling"
    );
    assert!(
        broker.attach(&auth("b"), "b".into(), false).is_err(),
        "a second session must not exceed the tenant byte ceiling"
    );

    let limits = Arc::new(Limits {
        max_inflight_qos2_per_session: 1,
        max_inflight_qos2_per_tenant: 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let first = broker.attach(&a, "a".into(), false).unwrap();
    let b = auth("b");
    let second = broker.attach(&b, "b".into(), false).unwrap();
    let message = |device: &str| BrokerMessage {
        topic: (format!("v1/t/t/p/p/d/{device}/up")).into(),
        payload: b"x".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    broker
        .inbound_qos2(&first.key, first.generation, 1, message("a"))
        .unwrap();
    assert!(
        broker
            .inbound_qos2(&second.key, second.generation, 1, message("b"))
            .is_err()
    );
}

#[test]
fn qos2_clean_session_incarnation_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("clean-incarnation");
    let old = broker.attach(&device, "same".into(), false).unwrap();
    broker
        .inbound_qos2(
            &old.key,
            old.generation,
            7,
            BrokerMessage {
                topic: "clean/incarnation".into(),
                payload: b"old".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let InboundQos2Action::Deliver {
        session_incarnation: old_incarnation,
        operation_id: old_operation,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&old.key, old.generation, 7)
        .unwrap()
    else {
        panic!("old delivery must start")
    };
    let replacement = broker.attach(&device, "same".into(), true).unwrap();
    assert_ne!(old_incarnation, replacement.session_incarnation);
    broker
        .inbound_qos2(
            &replacement.key,
            replacement.generation,
            7,
            BrokerMessage {
                topic: "clean/incarnation".into(),
                payload: b"new".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let InboundQos2Action::Deliver {
        session_incarnation: new_incarnation,
        operation_id: new_operation,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&replacement.key, replacement.generation, 7)
        .unwrap()
    else {
        panic!("new delivery must start")
    };
    assert!(
        broker
            .finish_inbound_qos2_delivery(&old.key, old_incarnation, 7, old_operation,)
            .is_err()
    );
    broker
        .finish_inbound_qos2_delivery(&replacement.key, new_incarnation, 7, new_operation)
        .unwrap();
    broker
        .route_inbound_qos2(
            &replacement.key,
            new_incarnation,
            7,
            new_operation,
            &device.device_key,
        )
        .unwrap();
}

#[test]
fn mqtt_persistent_overload_qos1_001_is_atomic() {
    persistent_route_overload_is_atomic(1);
}

#[test]
fn mqtt_persistent_overload_qos2_001_is_atomic() {
    persistent_route_overload_is_atomic(2);
}

#[tokio::test]
async fn mqtt_tenant_qos1_capacity_release_wakes_other_session() {
    tenant_capacity_wakes_other_session(1).await;
}

#[tokio::test]
async fn mqtt_tenant_qos2_capacity_release_wakes_other_session() {
    tenant_capacity_wakes_other_session(2).await;
}

#[test]
fn reconnect_replay_is_ordered_before_new_live_route() {
    use std::sync::Barrier;

    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("replay-order");
    let topic = "v1/t/t/p/p/d/replay-order/up";
    let mut first = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .subscribe(&first.key, first.generation, topic, 1)
        .unwrap();
    let message = |payload: &[u8]| BrokerMessage {
        topic: topic.into(),
        payload: payload.to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    broker.route(&device.device_key, message(b"old")).unwrap();
    let BrokerFrame::Publish(old) = first.receiver.try_recv().unwrap() else {
        panic!("expected original publish")
    };
    let packet_id = old.packet_id;
    first.detach().unwrap();

    let reached = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let reached_hook = reached.clone();
    let release_hook = release.clone();
    *broker.replay_hook.lock().unwrap() = Some(Arc::new(move || {
        reached_hook.wait();
        release_hook.wait();
    }));
    let reconnect_broker = broker.clone();
    let reconnect_device = device.clone();
    let reconnect = std::thread::spawn(move || {
        reconnect_broker
            .attach(&reconnect_device, "client".into(), false)
            .unwrap()
    });
    reached.wait(); // recovery frame is queued, broker lock still held
    let route_broker = broker.clone();
    let route_device = device.clone();
    let (started, running) = std::sync::mpsc::channel();
    let route = std::thread::spawn(move || {
        started.send(()).unwrap();
        route_broker
            .route(&route_device.device_key, message(b"new"))
            .unwrap()
    });
    running.recv().unwrap();
    release.wait();
    let mut resumed = reconnect.join().unwrap();
    route.join().unwrap();
    let BrokerFrame::Publish(replayed) = resumed.receiver.try_recv().unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(replayed.message.payload.as_ref(), b"old");
    assert_eq!(replayed.packet_id, packet_id);
    assert!(replayed.dup);
    let BrokerFrame::Publish(live) = resumed.receiver.try_recv().unwrap() else {
        panic!("expected new live publish")
    };
    assert_eq!(live.message.payload.as_ref(), b"new");
    assert!(!live.dup);
    resumed.detach().unwrap();
}
