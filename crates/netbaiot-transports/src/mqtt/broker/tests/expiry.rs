use super::*;

#[test]
fn message_deadline_budget_cleans_only_due_sessions_and_reindexes() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let mut keys = Vec::new();
    for index in 0..5 {
        let identity = auth(&format!("expiry-index-{index}"));
        let down = format!("v1/t/t/p/p/d/expiry-index-{index}/down");
        let mut attachment = broker
            .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, &down, 1)
            .unwrap();
        attachment.detach().unwrap();
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: down,
                    payload: vec![index as u8].into(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        keys.push(attachment.key.clone());
    }
    let mut state = lock(&broker.state).unwrap();
    for key in &keys {
        state
            .sessions
            .get_mut(key)
            .unwrap()
            .offline
            .front_mut()
            .unwrap()
            .properties
            .expires_at_ms = Some(now_ms() - 1);
        sync_session_usage(&mut state, key).unwrap();
    }
    drop(state);
    assert_broker_accounting(&broker);
    {
        let mut state = lock(&broker.state).unwrap();
        prune_expired_messages(&mut state, now_ms(), 3).unwrap();
        assert_eq!(state.offline_count, 2);
        assert_accounting_consistent(&state);
        prune_expired_messages(&mut state, now_ms(), 3).unwrap();
        assert_eq!(state.offline_count, 0);
        assert_accounting_consistent(&state);
    }
}

#[test]
fn target_session_expiry_is_checked_after_bounded_global_maintenance() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let mut identities = Vec::new();
    for index in 0..=HOT_MAINTENANCE_BUDGET {
        let identity = auth(&format!("session-deadline-{index}"));
        let mut attachment = broker
            .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
            .unwrap();
        let key = attachment.key.clone();
        attachment.detach().unwrap();
        identities.push((identity, key));
    }
    let past = now_ms() - 1;
    for (index, (_, key)) in identities.iter().enumerate() {
        let mut state = lock(&broker.state).unwrap();
        state.sessions.get_mut(key).unwrap().expires_at_ms =
            Some(if index == HOT_MAINTENANCE_BUDGET {
                past
            } else {
                past - 1
            });
        sync_session_usage(&mut state, key).unwrap();
    }
    assert_broker_accounting(&broker);
    let identity = &identities[HOT_MAINTENANCE_BUDGET].0;
    let attachment = broker
        .attach_v5(
            identity,
            format!("client-{HOT_MAINTENANCE_BUDGET}"),
            false,
            3_600,
            32,
        )
        .unwrap();
    assert!(!attachment.session_present);
    assert_broker_accounting(&broker);
}

#[test]
fn v5_qos2_retransmission_keeps_first_message_expiry_deadline() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("qos2-expiry");
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    let deadline = now_ms() + 5_000;
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/qos2-expiry/up".into(),
        payload: b"first".to_vec().into(),
        qos: 2,
        retain: false,
        properties: PublishProperties {
            expires_at_ms: Some(deadline),
            ..Default::default()
        },
    };
    assert!(
        broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
            .unwrap()
    );
    let mut retransmit = message.clone();
    retransmit.properties.expires_at_ms = Some(deadline + 1_000);
    assert!(
        !broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                7,
                retransmit.clone()
            )
            .unwrap()
    );
    let stored = broker
        .inbound_qos2_message(&attachment.key, attachment.generation, 7)
        .unwrap()
        .unwrap();
    assert_eq!(stored.0.properties.expires_at_ms, Some(deadline));
    let accounting = transaction_accounting(&broker, &attachment.key);
    retransmit.payload = b"different".to_vec().into();
    assert!(
        !broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                7,
                retransmit.clone()
            )
            .unwrap()
    );
    retransmit = message.clone();
    retransmit.topic = "v1/t/t/p/p/d/qos2-expiry/down".into();
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, retransmit)
            .unwrap()
    );
    retransmit = message.clone();
    retransmit.properties.content_type = Some("other".into());
    assert!(
        !broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, retransmit)
            .unwrap()
    );
    assert_eq!(transaction_accounting(&broker, &attachment.key), accounting);
    attachment.detach().unwrap();
}

#[test]
fn qos1_started_publish_survives_message_expiry_until_puback() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("started-qos1");
    let topic = "v1/t/t/p/p/d/started-qos1/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    broker
        .route_from_session(
            &attachment.key,
            &BrokerMessage {
                topic: topic.into(),
                payload: b"data".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties {
                    expires_at_ms: Some(now_ms() + 10_000),
                    ..Default::default()
                },
            },
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected PUBLISH")
    };
    let id = delivery.packet_id.unwrap();
    assert!(
        broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
            .unwrap()
    );
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
            panic!("expected AwaitPuback")
        };
        message.properties.expires_at_ms = Some(now_ms() - 1);
        sync_session_usage(&mut state, &attachment.key).unwrap();
    }
    broker.tick().unwrap();
    assert!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .contains_key(&id)
    );
    broker
        .puback(&attachment.key, attachment.generation, id)
        .unwrap();
    assert!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .is_empty()
    );
    attachment.detach().unwrap();
}

#[test]
fn unsent_expired_message_is_dropped() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("unsent-expiry");
    let topic = "v1/t/t/p/p/d/unsent-expiry/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    broker
        .route_from_session(
            &attachment.key,
            &BrokerMessage {
                topic: topic.into(),
                payload: b"data".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties {
                    expires_at_ms: Some(now_ms() + 10_000),
                    ..Default::default()
                },
            },
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected PUBLISH")
    };
    let id = delivery.packet_id.unwrap();
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
            panic!("expected AwaitPuback")
        };
        message.properties.expires_at_ms = Some(now_ms() - 1);
        sync_session_usage(&mut state, &attachment.key).unwrap();
    }
    broker.tick().unwrap();
    assert!(
        !broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
            .unwrap()
    );
    assert!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .is_empty()
    );
    attachment.detach().unwrap();
}

#[test]
fn oversized_qos1_delivery_is_settled_locally() {
    oversized_delivery_is_settled_locally(1);
}

#[test]
fn oversized_qos2_delivery_is_settled_locally() {
    oversized_delivery_is_settled_locally(2);
}

#[test]
fn qos2_started_publish_survives_message_expiry_until_pubcomp() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("started-qos2");
    let topic = "v1/t/t/p/p/d/started-qos2/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    broker
        .route_from_session(
            &attachment.key,
            &BrokerMessage {
                topic: topic.into(),
                payload: b"data".to_vec().into(),
                qos: 2,
                retain: false,
                properties: PublishProperties {
                    expires_at_ms: Some(now_ms() + 10_000),
                    ..Default::default()
                },
            },
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected PUBLISH")
    };
    let id = delivery.packet_id.unwrap();
    assert!(
        broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
            .unwrap()
    );
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        let OutboundState::AwaitPubrec(message) = session.outbound.get_mut(&id).unwrap() else {
            panic!("expected AwaitPubrec")
        };
        message.properties.expires_at_ms = Some(now_ms() - 1);
    }
    broker.tick().unwrap();
    assert!(matches!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .get(&id),
        Some(OutboundState::AwaitPubrec(_))
    ));
    broker
        .pubrec(&attachment.key, attachment.generation, id)
        .unwrap();
    broker.tick().unwrap();
    assert!(matches!(
        broker.state.lock().unwrap().sessions[&attachment.key].outbound.get(&id),
        Some(OutboundState::AwaitPubcomp(message)) if message.payload.is_empty()
    ));
    broker
        .pubcomp(&attachment.key, attachment.generation, id)
        .unwrap();
    assert!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .is_empty()
    );
    attachment.detach().unwrap();
}

#[tokio::test]
async fn reconnect_preserves_started_qos_state_after_message_expiry() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("recovered-started");
    let topic = "v1/t/t/p/p/d/recovered-started/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    broker
        .route_from_session(
            &attachment.key,
            &BrokerMessage {
                topic: topic.into(),
                payload: b"first".to_vec().into(),
                qos: 1,
                retain: false,
                properties: PublishProperties {
                    expires_at_ms: Some(now_ms() + 10_000),
                    ..Default::default()
                },
            },
        )
        .unwrap();
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected PUBLISH")
    };
    let id = delivery.packet_id.unwrap();
    broker
        .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
        .unwrap();
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
            panic!("expected AwaitPuback")
        };
        message.properties.expires_at_ms = Some(now_ms() - 1);
    }
    attachment.detach().unwrap();
    let directory =
        std::env::temp_dir().join(format!("netbaiot-started-expiry-{}", uuid::Uuid::new_v4()));
    broker.commit_to(&directory).await.unwrap();
    let recovered = MqttBroker::new(limits);
    recovered.recover_from(&directory).await.unwrap();
    let mut resumed = recovered
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    let BrokerFrame::Publish(retransmit) = resumed.receiver.try_recv().unwrap() else {
        panic!("expected retransmitted PUBLISH")
    };
    assert_eq!(retransmit.packet_id, Some(id));
    assert!(retransmit.dup);
    assert!(
        recovered
            .begin_outbound_transfer(&resumed.key, resumed.generation, &retransmit)
            .unwrap()
    );
    assert!(
        recovered.state.lock().unwrap().sessions[&resumed.key]
            .started_outbound
            .contains(&id)
    );
    recovered
        .puback(&resumed.key, resumed.generation, id)
        .unwrap();
    resumed.detach().unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn v5_clean_start_expiry_and_cross_version_sessions() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let auth = auth("a");
    let mut first = broker
        .attach_v5(&auth, "client".into(), true, 60, u16::MAX)
        .unwrap();
    assert!(!first.session_present);
    first.detach().unwrap();
    let mut resumed = broker
        .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
        .unwrap();
    assert!(resumed.session_present);
    broker
        .set_v5_disconnect_expiry(&resumed.key, resumed.generation, 0)
        .unwrap();
    resumed.detach().unwrap();
    let mut fresh = broker
        .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
        .unwrap();
    assert!(!fresh.session_present);
    fresh.detach().unwrap();
    let mut v311 = broker.attach(&auth, "client".into(), false).unwrap();
    assert!(!v311.session_present);
    v311.detach().unwrap();
    let mut v5 = broker
        .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
        .unwrap();
    assert!(!v5.session_present);
    v5.detach().unwrap();
}

#[test]
fn qos0_drop_cleanup_and_expiry_count_one_terminal_failure() {
    use netbaiot_core::DeliveryState;
    use netbaiot_runtime::{CommandProgress, Metric};
    let metrics = Arc::new(Metrics::default());
    let expiry = now_ms() + 60_000;
    let progress = CommandProgress::new(expiry, metrics.clone());
    let guard = UnsentCommandGuard(Some(progress.clone()));
    let barrier = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            progress.abandon_unsent();
        });
        scope.spawn(|| {
            barrier.wait();
            progress.expire(expiry);
        });
        barrier.wait();
        drop(guard);
    });
    assert!(matches!(
        progress.state(),
        DeliveryState::Failed | DeliveryState::Expired
    ));
    assert_eq!(metrics.get(Metric::CommandFailed), 1);
    assert_eq!(Arc::strong_count(&progress), 1);
}
