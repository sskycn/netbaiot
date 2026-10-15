use super::*;

fn exhausted_packet_identifier_fixture(
    version: MqttVersion,
) -> (Arc<MqttBroker>, Attachment, BrokerMessage) {
    // QoS1 and QoS2 have separate resource ceilings, but share one 16-bit
    // outbound Packet Identifier space. A resumed AwaitPubcomp consumes no
    // PUBLISH window on its new connection.
    let limits = Arc::new(Limits {
        max_inflight_qos1_per_session: usize::from(u16::MAX),
        max_inflight_qos1_per_tenant: usize::from(u16::MAX),
        max_inflight_qos2_per_session: 1,
        max_outbound_messages_per_connection: usize::from(u16::MAX) + 1,
        max_mqtt_session_state_bytes: 16 * 1024 * 1024,
        max_outbound_bytes_per_connection: 16 * 1024 * 1024,
        max_outbound_bytes_per_tenant: 16 * 1024 * 1024,
        max_outbound_bytes: 16 * 1024 * 1024,
        ..Limits::default()
    });
    limits.validate().unwrap();
    let broker = MqttBroker::new(limits);
    let identity = auth("packet-id-exhaustion");
    let attachment = match version {
        MqttVersion::V311 => broker.attach(&identity, "client".into(), false),
        MqttVersion::V5 => broker.attach_v5(&identity, "client".into(), false, 60, u16::MAX),
    }
    .unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/packet-id-exhaustion/up".into(),
        payload: b"pending".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    broker
        .subscribe(&attachment.key, attachment.generation, &message.topic, 1)
        .unwrap();
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        for packet_id in 1..=u16::MAX {
            let outbound = if packet_id == u16::MAX {
                OutboundState::AwaitPubcomp(BrokerMessage {
                    qos: 2,
                    ..message.clone()
                })
            } else {
                session.send_window.insert(packet_id);
                OutboundState::AwaitPuback(message.clone())
            };
            session.insert_outbound(packet_id, outbound);
            session.sent.insert(packet_id);
            session.started_outbound.insert(packet_id);
        }
        let additional = message.bytes() * (usize::from(u16::MAX) + 1);
        session.state_bytes += additional;
        session.offline.push_back(message.clone());
        session.offline_bytes = message.bytes();
        state.session_bytes += additional;
        state.offline_count = 1;
        state.offline_bytes = message.bytes();
        sync_session_usage(&mut state, &attachment.key).unwrap();
        assert_accounting_consistent(&state);
    }
    (broker, attachment, message)
}

#[test]
fn packet_identifier_exhaustion_preserves_offline_during_promotion() {
    for version in [MqttVersion::V311, MqttVersion::V5] {
        let (broker, mut attachment, message) = exhausted_packet_identifier_fixture(version);
        let active = broker.state.lock().unwrap().active[&attachment.key].clone();
        let available = || {
            [
                active.connection_bytes.available(),
                active.tenant_bytes.available(),
                active.global_bytes.available(),
            ]
        };
        let before = available();
        let result = broker.next_offline(&attachment.key, attachment.generation);
        assert_eq!(available(), before);
        {
            let state = broker.state.lock().unwrap();
            let session = &state.sessions[&attachment.key];
            assert_eq!(session.offline.front(), Some(&message));
            assert_eq!(session.offline_bytes, message.bytes());
            assert_accounting_consistent(&state);
        }
        assert!(
            matches!(result, Ok(None)),
            "exhaustion must defer promotion"
        );
        broker
            .puback(&attachment.key, attachment.generation, 1)
            .unwrap();
        let BrokerFrame::Publish(promoted) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected the deferred PUBLISH");
        };
        assert_eq!(promoted.packet_id, Some(1));
        assert_eq!(promoted.message, message);
        assert_eq!(
            available(),
            before.map(|capacity| capacity - message.bytes())
        );
        drop(promoted);
        assert_eq!(available(), before);
        assert_broker_accounting(&broker);
    }
}

#[test]
fn packet_identifier_exhaustion_preserves_offline_during_reconnect() {
    for version in [MqttVersion::V311, MqttVersion::V5] {
        let (broker, mut previous, message) = exhausted_packet_identifier_fixture(version);
        let identity = auth("packet-id-exhaustion");
        previous.detach().unwrap();
        let result = match version {
            MqttVersion::V311 => broker.attach(&identity, "client".into(), false),
            MqttVersion::V5 => broker.attach_v5(&identity, "client".into(), false, 60, u16::MAX),
        };
        {
            let state = broker.state.lock().unwrap();
            let session = &state.sessions[&previous.key];
            assert_eq!(session.offline.front(), Some(&message));
            assert_eq!(session.offline_bytes, message.bytes());
            assert_accounting_consistent(&state);
        }
        let mut resumed = result.unwrap();
        assert!(resumed.session_present);
        let BrokerFrame::Publish(first) = resumed.receiver.try_recv().unwrap() else {
            panic!("expected the oldest retransmitted PUBLISH");
        };
        assert_eq!(first.packet_id, Some(1));
        drop(first);
        broker.puback(&resumed.key, resumed.generation, 1).unwrap();
        let state = broker.state.lock().unwrap();
        let session = &state.sessions[&resumed.key];
        assert!(session.offline.is_empty());
        assert_eq!(session.offline_bytes, 0);
        assert_eq!(state.offline_count, 0);
        assert_eq!(session.outbound_order.back(), Some(&1));
        assert_accounting_consistent(&state);
    }
}

#[test]
fn generated_client_id_never_replaces_explicit_or_recovered_session() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let identity = auth("generated-id");
    let mut explicit = broker
        .attach_v5(&identity, "generated-2".into(), false, 60, 4)
        .unwrap();
    let filter = "v1/t/t/p/p/d/generated-id/up";
    broker
        .subscribe(&explicit.key, explicit.generation, filter, 1)
        .unwrap();
    explicit.detach().unwrap();
    let recovered = MqttBroker::new(Arc::new(Limits::default()));
    recovered.restore(broker.snapshot().unwrap()).unwrap();
    let mut assigned = recovered
        .attach_generated_v5(&identity, 60, 4, &|_| Ok(()))
        .unwrap();
    assert_ne!(assigned.key.client_id.as_ref(), "generated-2");
    assigned.detach().unwrap();
    let mut resumed = recovered
        .attach_v5(&identity, "generated-2".into(), false, 60, 4)
        .unwrap();
    assert!(resumed.session_present);
    assert_eq!(
        recovered.subscription_qos(&resumed.key, filter).unwrap(),
        Some(1)
    );
    resumed.detach().unwrap();
}

#[test]
fn concurrent_generated_client_ids_are_unique() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let identity = auth("generated-concurrent");
    std::thread::scope(|scope| {
        let handles = (0..16)
            .map(|_| {
                let broker = broker.clone();
                let identity = &identity;
                scope.spawn(move || {
                    broker
                        .attach_generated_v5(identity, 60, 4, &|_| Ok(()))
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut attachments = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let ids = attachments
            .iter()
            .map(|attachment| attachment.key.client_id.clone())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids.len(), attachments.len());
        for attachment in &mut attachments {
            attachment.detach().unwrap();
        }
    });
}

#[test]
fn packet_identifier_not_reused_before_qos_exchange_finishes() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("packet-id-expiry");
    let topic = "v1/t/t/p/p/d/packet-id-expiry/up";
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();
    let message = BrokerMessage {
        topic: topic.into(),
        payload: b"data".to_vec().into(),
        qos: 1,
        retain: false,
        properties: (PublishProperties {
            expires_at_ms: Some(now_ms() + 10_000),
            ..Default::default()
        })
        .into(),
    };
    broker
        .route_from_session(&attachment.key, &message)
        .unwrap();
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first PUBLISH")
    };
    let first_id = first.packet_id.unwrap();
    broker
        .begin_outbound_transfer(&attachment.key, attachment.generation, &first)
        .unwrap();
    {
        let mut state = broker.state.lock().unwrap();
        let session = state.sessions.get_mut(&attachment.key).unwrap();
        session.next_packet_id = first_id;
        let OutboundState::AwaitPuback(stored) = session.outbound.get_mut(&first_id).unwrap()
        else {
            panic!("expected AwaitPuback")
        };
        stored.properties.expires_at_ms = Some(now_ms() - 1);
    }
    broker.tick().unwrap();
    broker
        .route_from_session(&attachment.key, &message)
        .unwrap();
    let BrokerFrame::Publish(second) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected second PUBLISH")
    };
    assert_ne!(second.packet_id, Some(first_id));
    attachment.detach().unwrap();
}

#[test]
fn current_outbound_record_preserves_started_flag() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("current-outbound");
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 4)
        .unwrap();
    attachment.detach().unwrap();
    let base = broker.snapshot().unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/current-outbound/down".into(),
        payload: b"current".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    for started in [false, true] {
        let mut snapshot = base.clone();
        let mut record = vec![0, 7, 0, u8::from(started)];
        encode_message(&mut record, &message).unwrap();
        decode_record(RECORD_OUTBOUND, &record, &mut snapshot, &limits).unwrap();
        assert_eq!(snapshot.sessions[0].started_outbound.contains(&7), started);
        assert!(snapshot.sessions[0].outbound.contains_key(&7));
    }
}

#[test]
fn wildcard_trie_and_dollar_rules() {
    assert!(topic_matches("sport/+/player1", "sport/team/player1"));
    assert!(topic_matches("sport/#", "sport"));
    assert!(topic_matches("sport/#", "sport/a/b"));
    assert!(!topic_matches("#", "$SYS/status"));
    assert!(!topic_matches("+", "$SYS"));
    assert!(!topic_matches("+/status", "$SYS/status"));
    assert!(topic_matches("$SYS/#", "$SYS/status"));
    let mut trie = SubscriptionTrie::default();
    let a = SessionKey {
        device: auth("a").device_key,
        client_id: "a".into(),
    };
    let b = SessionKey {
        device: auth("b").device_key,
        client_id: "b".into(),
    };
    trie.insert("sport/+", a.clone(), Subscription::v311(1));
    trie.insert("sport/#", b.clone(), Subscription::v311(2));
    let found = trie.matching("sport/tennis");
    assert_eq!(found.get(&a).map(|value| value.qos), Some(1));
    assert_eq!(found.get(&b).map(|value| value.qos), Some(2));
    trie.remove("sport/+", &a);
    assert!(!trie.matching("sport/tennis").contains_key(&a));
    let authorized = auth("a");
    let limits = Limits::default();
    assert!(subscribe_acl(&authorized, "v1/t/t/p/p/d/a/#", &limits));
    assert!(subscribe_acl(&authorized, "v1/t/t/p/p/d/a/up", &limits));
    for escaped in ["#", "v1/t/t/p/p/d/+/up", "v1/t/t/p/p/d/b/#"] {
        assert!(!subscribe_acl(&authorized, escaped, &limits));
    }
}

#[test]
fn topic_matcher_preserves_empty_levels_and_terminal_wildcards() {
    for (filter, topic, expected) in [
        ("#", "a", true),
        ("+", "a", true),
        ("+", "a/b", false),
        ("+/x", "/x", true),
        ("a/+", "a/", true),
        ("a/+", "a", false),
        ("a/#", "a", true),
        ("a/#", "a/", true),
        ("a/#", "ab", false),
        ("a/+/c", "a//c", true),
        ("a/+/c", "a/b/c", true),
        ("a/+/c", "a/b/d", false),
        ("a//b", "a//b", true),
        ("a//b", "a/b", false),
        ("/a", "/a", true),
        ("/a", "a", false),
        ("a/", "a/", true),
        ("a/", "a", false),
        ("/", "/", true),
        ("+/+", "/", true),
        ("#", "$SYS", false),
        ("+", "$SYS", false),
        ("+/x", "$device/x", false),
        ("$SYS/#", "$SYS", true),
        ("$SYS/+", "$SYS/", true),
        ("a/#/b", "a/b", false),
        ("#/x", "a/x", false),
        ("a+", "a", false),
    ] {
        assert_eq!(
            topic_matches(filter, topic),
            expected,
            "{filter:?} / {topic:?}"
        );
    }
}

#[test]
fn topic_matcher_agrees_with_subscription_trie_for_valid_filters() {
    let key = SessionKey {
        device: auth("matcher").device_key,
        client_id: "matcher".into(),
    };
    let levels = ["", "a", "b", "$SYS"];
    let mut topics = Vec::new();
    for first in levels {
        topics.push(first.to_owned());
        for second in levels {
            topics.push(format!("{first}/{second}"));
            for third in levels {
                topics.push(format!("{first}/{second}/{third}"));
            }
        }
    }
    let filters = [
        "#", "+", "+/+", "+/#", "a/+", "a/#", "a/+/b", "a//b", "/a", "a/", "/", "$SYS/#", "$SYS/+",
        "+/a", "+/+/+", "a/b/#",
    ];
    for filter in filters {
        let mut trie = SubscriptionTrie::default();
        trie.insert(filter, key.clone(), Subscription::v311(1));
        for topic in &topics {
            assert_eq!(
                topic_matches(filter, topic),
                trie.matching(topic).contains_key(&key),
                "{filter:?} / {topic:?}"
            );
        }
    }
}

#[tokio::test]
async fn mqtt_outbound_ack_state_matrix_preserves_transaction_on_wrong_ack() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("ack-matrix");
    let mut attachment = broker.attach(&device, "ack-matrix".into(), false).unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, "matrix/#", 2)
        .unwrap();

    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: "matrix/qos2".into(),
                payload: vec![2].into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(qos2) = attachment.receiver.recv().await.unwrap() else {
        panic!("expected QoS2 publish")
    };
    let qos2_id = qos2.packet_id.unwrap();
    let await_pubrec = transaction_accounting(&broker, &attachment.key);
    assert!(
        broker
            .puback(&attachment.key, attachment.generation, qos2_id)
            .is_err()
    );
    assert_eq!(
        transaction_accounting(&broker, &attachment.key),
        await_pubrec
    );
    assert!(
        broker
            .pubcomp(&attachment.key, attachment.generation, qos2_id)
            .is_err()
    );
    assert_eq!(
        transaction_accounting(&broker, &attachment.key),
        await_pubrec
    );
    assert!(matches!(
        broker
            .pubrec(&attachment.key, attachment.generation, qos2_id)
            .unwrap(),
        BrokerFrame::Pubrel { dup: false, .. }
    ));
    let await_pubcomp = transaction_accounting(&broker, &attachment.key);
    assert!(
        broker
            .puback(&attachment.key, attachment.generation, qos2_id)
            .is_err()
    );
    assert_eq!(
        transaction_accounting(&broker, &attachment.key),
        await_pubcomp
    );
    broker
        .pubcomp(&attachment.key, attachment.generation, qos2_id)
        .unwrap();

    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: "matrix/qos1".into(),
                payload: vec![1].into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(qos1) = attachment.receiver.recv().await.unwrap() else {
        panic!("expected QoS1 publish")
    };
    let qos1_id = qos1.packet_id.unwrap();
    let await_puback = transaction_accounting(&broker, &attachment.key);
    assert!(
        broker
            .pubrec(&attachment.key, attachment.generation, qos1_id)
            .is_err()
    );
    assert_eq!(
        transaction_accounting(&broker, &attachment.key),
        await_puback
    );
    assert!(
        broker
            .pubcomp(&attachment.key, attachment.generation, qos1_id)
            .is_err()
    );
    assert_eq!(
        transaction_accounting(&broker, &attachment.key),
        await_puback
    );
    broker
        .puback(&attachment.key, attachment.generation, qos1_id)
        .unwrap();
}

#[test]
fn mqtt_authz_persistent_reset_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let mut allowed = auth("auth-reset");
    let first = broker.attach(&allowed, "persistent".into(), false).unwrap();
    broker
        .subscribe(
            &first.key,
            first.generation,
            "v1/t/t/p/p/d/auth-reset/down",
            1,
        )
        .unwrap();
    broker.detach(&first.key, first.generation, false).unwrap();
    allowed.permissions.commands = false;
    let replacement = broker.attach(&allowed, "persistent".into(), false).unwrap();
    assert!(!replacement.session_present);
    assert_eq!(
        broker
            .subscription_qos(&replacement.key, "v1/t/t/p/p/d/auth-reset/down")
            .unwrap(),
        None
    );
    drop(replacement);
    let mut rotated = allowed.clone();
    rotated.credential_version += 1;
    let credential_reset = broker.attach(&rotated, "persistent".into(), false).unwrap();
    assert!(!credential_reset.session_present);
    drop(credential_reset);
    rotated.auth_generation += 1;
    let generation_reset = broker.attach(&rotated, "persistent".into(), false).unwrap();
    assert!(!generation_reset.session_present);
    drop(generation_reset);
    let unrelated_auth = auth("auth-unrelated");
    let unrelated = broker
        .attach(&unrelated_auth, "persistent".into(), false)
        .unwrap();
    drop(unrelated);
    assert_eq!(
        broker
            .invalidate_sessions(&AuthInvalidation::Device {
                device: allowed.device_key.clone(),
            })
            .unwrap(),
        1
    );
    assert!(
        broker
            .attach(&unrelated_auth, "persistent".into(), false)
            .unwrap()
            .session_present
    );
}

#[test]
fn mqtt_persistent_codec_provenance_001() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let original = auth("codec-profile");
    let first = broker
        .attach(&original, "persistent".into(), false)
        .unwrap();
    broker
        .inbound_qos2(
            &first.key,
            first.generation,
            9,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/codec-profile/up".into(),
                payload: b"v1".to_vec().into(),
                qos: 2,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    broker.detach(&first.key, first.generation, false).unwrap();

    let exact = broker
        .attach(&original, "persistent".into(), false)
        .unwrap();
    assert!(exact.session_present);
    broker.detach(&exact.key, exact.generation, false).unwrap();

    let mut changed_id = original.clone();
    changed_id.codec_id = CodecId::new("other-codec").unwrap();
    let reset = broker
        .attach(&changed_id, "persistent".into(), false)
        .unwrap();
    assert!(!reset.session_present);
    broker.detach(&reset.key, reset.generation, false).unwrap();

    let mut changed_version = changed_id.clone();
    changed_version.codec_version += 1;
    let reset = broker
        .attach(&changed_version, "persistent".into(), false)
        .unwrap();
    assert!(!reset.session_present);
}

#[test]
fn auth_invalidation_counting_001() {
    let limits = Arc::new(Limits::default());
    let device = auth("counting");
    let sessions = netbaiot_runtime::Sessions::new(limits.clone());
    let (_lease, _commands) = sessions
        .register(Arc::new(device.clone()), netbaiot_core::Transport::Mqtt)
        .unwrap();
    let broker = MqttBroker::new(limits);
    let _active = broker.attach(&device, "active".into(), false).unwrap();
    let offline = broker.attach(&device, "offline".into(), false).unwrap();
    broker
        .detach(&offline.key, offline.generation, false)
        .unwrap();
    let invalidation = AuthInvalidation::Device {
        device: device.device_key.clone(),
    };
    let disconnected_connections = sessions.disconnect_matching(&invalidation).unwrap();
    let invalidated_mqtt_sessions = broker.invalidate_sessions(&invalidation).unwrap();
    let result = netbaiot_core::InvalidationResult {
        invalidated: 1,
        disconnected: disconnected_connections,
        invalidated_cache_entries: 1,
        disconnected_connections,
        invalidated_mqtt_sessions,
    };
    assert_eq!(result.disconnected_connections, 1);
    assert_eq!(result.invalidated_mqtt_sessions, 2);
    assert_eq!(
        result.disconnected, 1,
        "legacy field remains a connection count"
    );
    let decoded: netbaiot_core::InvalidationResult =
        serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
    assert_eq!(decoded, result);
}

#[test]
fn retain_replacement_reservation_001() {
    let limits = Arc::new(Limits {
        max_retained_messages: 2,
        max_retained_messages_per_tenant: 2,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("retain-replace");
    let topic = "retained/replacement";
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"one".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: topic.into(),
                payload: b"qos1-replacement".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    let attachment = broker.attach(&device, "qos2".into(), false).unwrap();
    broker
        .inbound_qos2(
            &attachment.key,
            attachment.generation,
            9,
            BrokerMessage {
                topic: topic.into(),
                payload: b"two".to_vec().into(),
                qos: 2,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    let InboundQos2Action::Deliver {
        session_incarnation,
        operation_id,
        ..
    } = broker
        .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 9)
        .unwrap()
    else {
        panic!("QoS2 replacement must be deliverable")
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
    let mut guard = broker
        .reserve_will(
            device.device_key.clone(),
            BrokerMessage {
                topic: topic.into(),
                payload: b"will".to_vec().into(),
                qos: 1,
                retain: true,
                properties: Default::default(),
            },
        )
        .unwrap();
    guard.publish().unwrap();
    assert!(broker.has_retained_topic(topic).unwrap());
    let state = broker.state.lock().unwrap();
    assert_eq!(state.retained_reserved_count, 0);
    assert_eq!(state.retained_reserved_bytes, 0);
}

#[test]
fn mqtt_attachment_guard_cleans_clean_and_preserves_persistent_session() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("guard");
    drop(broker.attach(&device, "clean".into(), true).unwrap());
    let clean_probe = broker.attach(&device, "clean".into(), false).unwrap();
    assert!(!clean_probe.session_present);
    drop(clean_probe);

    drop(broker.attach(&device, "persistent".into(), false).unwrap());
    let persistent_probe = broker.attach(&device, "persistent".into(), false).unwrap();
    assert!(persistent_probe.session_present);
}

#[tokio::test]
async fn persistent_reconnect_retransmits_outbound_in_original_order() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("a");
    let topic = "v1/t/t/p/p/d/a/up";
    let mut attachment = broker.attach(&device, "ordered".into(), false).unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 1)
        .unwrap();

    let mut expected = Vec::new();
    for sequence in 0u8..16 {
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: vec![sequence].into(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected publish")
        };
        expected.push((delivery.packet_id.unwrap(), delivery.message.payload));
    }
    broker
        .detach(&attachment.key, attachment.generation, false)
        .unwrap();

    let mut resumed = broker.attach(&device, "ordered".into(), false).unwrap();
    assert!(resumed.session_present);
    for (packet_id, payload) in expected {
        let BrokerFrame::Publish(delivery) = resumed.receiver.recv().await.unwrap() else {
            panic!("expected retransmitted publish")
        };
        assert_eq!(delivery.packet_id, Some(packet_id));
        assert_eq!(delivery.message.payload, payload);
        assert!(delivery.dup);
    }
}

#[test]
fn trie_branches_merge_options_and_track_replacement_removal() {
    let keys = ["first", "second", "third"].map(|id| SessionKey {
        device: auth(id).device_key,
        client_id: id.into(),
    });
    let mut topics = Vec::new();
    for first in ["", "a", "b", "$SYS", "温度"] {
        topics.push(first.to_owned());
        for second in ["", "a", "b", "$SYS", "温度"] {
            topics.push(format!("{first}/{second}"));
            for third in ["", "a", "b", "$SYS", "温度"] {
                topics.push(format!("{first}/{second}/{third}"));
            }
        }
    }
    let mut filters = topics.clone();
    filters.extend(
        [
            "#", "+", "+/#", "+/+", "+/+/+", "a/#", "a/+", "a/+/b", "a/+/#", "$SYS/#", "$SYS/+",
            "/#", "/+", "温度/#",
        ]
        .map(str::to_owned),
    );
    let mut trie = SubscriptionTrie::default();
    let mut subscriptions = HashMap::new();
    for (index, filter) in filters.iter().enumerate() {
        let key = keys[index % keys.len()].clone();
        let options = Subscription {
            qos: (index % 3) as u8,
            no_local: index % 2 == 0,
            retain_as_published: index % 5 == 0,
            retain_handling: (index % 3) as u8,
        };
        trie.insert(filter, key.clone(), options);
        subscriptions.insert((filter.clone(), key), options);
    }
    // Check the combined result against independent linear filter matching.
    // Retain Handling applies at SUBSCRIBE, not during publication merging.
    let check = |trie: &SubscriptionTrie,
                 subscriptions: &HashMap<(String, SessionKey), Subscription>| {
        for topic in &topics {
            let mut expected = HashMap::<SessionKey, (u8, bool, bool)>::new();
            for ((filter, key), options) in subscriptions {
                if topic_matches(filter, topic) {
                    let merged = expected.entry(key.clone()).or_insert((0, true, false));
                    merged.0 = merged.0.max(options.qos);
                    merged.1 &= options.no_local;
                    merged.2 |= options.retain_as_published;
                }
            }
            let actual = trie
                .matching(topic)
                .into_iter()
                .map(|(key, options)| {
                    (
                        key,
                        (options.qos, options.no_local, options.retain_as_published),
                    )
                })
                .collect::<HashMap<_, _>>();
            assert_eq!(actual, expected, "topic {topic:?}");
        }
    };
    check(&trie, &subscriptions);
    for (index, filter) in filters.iter().enumerate().step_by(2) {
        let key = keys[index % keys.len()].clone();
        let options = Subscription::v311(2);
        trie.insert(filter, key.clone(), options);
        subscriptions.insert((filter.clone(), key), options);
    }
    check(&trie, &subscriptions);
    for (index, filter) in filters.iter().enumerate() {
        let key = &keys[index % keys.len()];
        trie.remove(filter, key);
        subscriptions.remove(&(filter.clone(), key.clone()));
        if index % 16 == 0 {
            check(&trie, &subscriptions);
        }
    }
    check(&trie, &subscriptions);
    assert!(trie.root.children.is_empty());
}
