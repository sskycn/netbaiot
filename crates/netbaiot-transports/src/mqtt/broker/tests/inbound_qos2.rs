use super::*;

#[test]
fn inbound_qos2_completion_wakes_waiting_outbound_qos2() {
    let limits = Limits {
        max_inflight_qos2_per_session: 1,
        ..Limits::default()
    };
    let broker = MqttBroker::new(Arc::new(limits));
    let device = auth("qos2-self-route");
    let topic = "v1/t/t/p/p/d/qos2-self-route/up";
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    let message = BrokerMessage {
        topic: topic.into(),
        payload: b"payload".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    broker
        .inbound_qos2(&attachment.key, attachment.generation, 7, message)
        .unwrap();
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
        .unwrap()
    else {
        panic!("inbound QoS2 should be ready to deliver");
    };
    broker
        .finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 7, operation_id)
        .unwrap();
    broker
        .route_inbound_qos2(
            &attachment.key,
            session_incarnation,
            7,
            operation_id,
            &device.device_key,
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("waiting subscriber copy should advance without another packet");
    };
    assert_eq!(delivery.message.payload.as_ref(), b"payload");
    assert_eq!(delivery.message.qos, 2);
    assert_eq!(broker.state.lock().unwrap().offline_count, 0);
    attachment.detach().unwrap();
}

#[test]
fn persistent_inbound_qos2_does_not_consume_new_connection_receive_window() {
    let limits = Limits {
        max_inflight_qos1_per_session: 1,
        max_inflight_qos2_per_session: 2,
        ..Default::default()
    };
    let broker = MqttBroker::new(Arc::new(limits));
    let device = auth("inbound-window-reconnect");
    let mut first = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    broker
        .inbound_qos2(
            &first.key,
            first.generation,
            7,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/inbound-window-reconnect/up".into(),
                payload: b"old".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert!(
        !broker
            .inbound_receive_available(&first.key, first.generation, 1, 8)
            .unwrap()
    );
    first.detach().unwrap();
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    assert!(resumed.session_present);
    assert!(
        broker
            .inbound_receive_available(&resumed.key, resumed.generation, 1, 8)
            .unwrap()
    );
    assert!(
        broker
            .inbound_receive_available(&resumed.key, resumed.generation, 2, 7)
            .unwrap()
    );
    resumed.detach().unwrap();
}

#[test]
fn inbound_qos2_classification_is_read_only_and_generation_fenced() {
    let (broker, mut old, _) = qos2_duplicate_case();
    let before = transaction_accounting(&broker, &old.key);
    assert_eq!(
        broker
            .classify_inbound_qos2_publish(&old.key, old.generation, 7)
            .unwrap(),
        InboundQos2PublishState::ExistingTransaction
    );
    assert_eq!(
        broker
            .classify_inbound_qos2_publish(&old.key, old.generation, 8)
            .unwrap(),
        InboundQos2PublishState::NeedsNewMessageAdmission
    );
    assert_eq!(transaction_accounting(&broker, &old.key), before);

    let device = auth("qos2-duplicate");
    let mut replacement = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    assert!(matches!(
        broker.classify_inbound_qos2_publish(&old.key, old.generation, 7),
        Err(Error::Conflict)
    ));
    let resumed = transaction_accounting(&broker, &replacement.key);
    assert_eq!(
        broker
            .classify_inbound_qos2_publish(&replacement.key, replacement.generation, 7)
            .unwrap(),
        InboundQos2PublishState::ExistingTransaction
    );
    assert_eq!(transaction_accounting(&broker, &replacement.key), resumed);
    old.detach().unwrap();
    replacement.detach().unwrap();
}

#[test]
fn qos2_retransmission_charges_only_the_new_connection_window() {
    let limits = Limits {
        max_inflight_qos1_per_session: 2,
        max_inflight_qos2_per_session: 2,
        ..Default::default()
    };
    let broker = MqttBroker::new(Arc::new(limits));
    let identity = auth("window-retransmit");
    let mut old = broker
        .attach_v5(&identity, "client".into(), false, 60, 4)
        .unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/window-retransmit/up".into(),
        payload: b"original".to_vec().into(),
        qos: 2,
        retain: false,
        properties: Default::default(),
    };
    for id in 1..=2 {
        broker
            .inbound_qos2(&old.key, old.generation, id, message.clone())
            .unwrap();
    }
    old.detach().unwrap();
    let mut resumed = broker
        .attach_v5(&identity, "client".into(), false, 60, 4)
        .unwrap();
    assert!(
        broker
            .inbound_receive_available(&resumed.key, resumed.generation, 1, 3)
            .unwrap()
    );
    for id in 1..=2 {
        assert!(
            broker
                .begin_inbound_qos2_retransmission(&resumed.key, resumed.generation, id)
                .unwrap()
        );
        assert!(
            broker
                .begin_inbound_qos2_retransmission(&resumed.key, resumed.generation, id)
                .unwrap()
        );
    }
    assert!(
        !broker
            .inbound_receive_available(&resumed.key, resumed.generation, 1, 3)
            .unwrap()
    );
    broker
        .finish_inbound_pubcomp(&resumed.key, resumed.generation, 1)
        .unwrap();
    assert!(
        broker
            .inbound_receive_available(&resumed.key, resumed.generation, 1, 3)
            .unwrap()
    );
    assert!(matches!(
        broker.classify_inbound_qos2_publish(&resumed.key, resumed.generation, 1),
        Ok(InboundQos2PublishState::ExistingTransaction)
    ));
    resumed.detach().unwrap();
}

#[test]
fn inbound_qos2_repeated_publish_before_pubrel_repeats_pubrec() {
    let (broker, mut attachment, message) = qos2_duplicate_case();
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, message)
            .unwrap()
    );
    assert!(matches!(
        broker.begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7),
        Ok(InboundQos2Action::Deliver { .. })
    ));
    attachment.detach().unwrap();
}

#[test]
fn inbound_qos2_repeated_publish_does_not_redeliver() {
    let (broker, mut attachment, message) = qos2_duplicate_case();
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
        .unwrap()
    else {
        panic!("original must be delivered")
    };
    broker
        .finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 7, operation_id)
        .unwrap();
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, message)
            .unwrap()
    );
    assert!(matches!(
        broker.begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7),
        Ok(InboundQos2Action::EventAccepted { .. })
    ));
    attachment.detach().unwrap();
}

#[test]
fn inbound_qos2_dup_flag_does_not_create_second_transaction() {
    let (broker, mut attachment, mut message) = qos2_duplicate_case();
    // The connection supplies identical broker state transitions for both wire DUP values.
    for _dup in [false, true] {
        let mut payload = message.payload.to_vec();
        payload.push(b'x');
        message.payload = payload.into();
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
                .unwrap()
        );
    }
    assert_eq!(
        transaction_accounting(&broker, &attachment.key).inbound_qos2_count,
        1
    );
    attachment.detach().unwrap();
}

#[test]
fn inbound_qos2_original_message_remains_authoritative_until_pubrel() {
    let (broker, mut attachment, message) = qos2_duplicate_case();
    let mut changed = message.clone();
    changed.payload = b"replacement".to_vec().into();
    changed.properties.content_type = Some("changed".into());
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, changed)
            .unwrap()
    );
    let InboundQos2Action::Deliver {
        message: delivered, ..
    } = broker
        .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
        .unwrap()
    else {
        panic!("original must be delivered")
    };
    assert_eq!(delivered, message);
    attachment.detach().unwrap();
}

#[test]
fn inbound_qos2_pubcomp_releases_identifier_for_new_message() {
    let (broker, mut attachment, message) = qos2_duplicate_case();
    broker
        .complete_inbound_qos2(&attachment.key, attachment.generation, 7)
        .unwrap();
    broker
        .finish_inbound_pubcomp(&attachment.key, attachment.generation, 7)
        .unwrap();
    let mut next = message;
    next.payload = b"next".to_vec().into();
    assert!(
        broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, next.clone())
            .unwrap()
    );
    assert_eq!(
        broker
            .inbound_qos2_message(&attachment.key, attachment.generation, 7)
            .unwrap()
            .unwrap()
            .0,
        next
    );
    attachment.detach().unwrap();
}

#[test]
fn mqtt_inbound_qos2_takeover_can_finish_event_accepted_handoff() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("takeover");
    let old = broker.attach(&device, "takeover".into(), false).unwrap();
    broker
        .inbound_qos2(
            &old.key,
            old.generation,
            7,
            BrokerMessage {
                topic: "takeover/up".into(),
                payload: vec![7].into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&old.key, old.generation, 7)
        .unwrap()
    else {
        panic!("old connection must own delivery")
    };
    let replacement = broker.attach(&device, "takeover".into(), false).unwrap();
    assert_eq!(old.session_incarnation, replacement.session_incarnation);
    broker
        .finish_inbound_qos2_delivery(&old.key, session_incarnation, 7, operation_id)
        .unwrap();
    let InboundQos2Action::EventAccepted {
        session_incarnation,
        operation_id,
    } = broker
        .begin_inbound_qos2_delivery(&replacement.key, replacement.generation, 7)
        .unwrap()
    else {
        panic!("takeover must observe accepted transaction")
    };
    broker
        .route_inbound_qos2(
            &replacement.key,
            session_incarnation,
            7,
            operation_id,
            &device.device_key,
        )
        .unwrap();
    assert!(
        broker
            .route_inbound_qos2(
                &replacement.key,
                session_incarnation,
                7,
                operation_id,
                &device.device_key,
            )
            .is_err()
    );
    let state = broker.state.lock().unwrap();
    assert!(state.sessions[&replacement.key].inbound_qos2.is_empty());
}
