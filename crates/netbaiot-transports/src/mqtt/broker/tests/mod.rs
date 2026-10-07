use super::*;
use std::fmt::Debug;
use std::time::Duration;

fn assert_deadline_index<K: Clone + Debug + Eq + Hash>(
    index: &DeadlineIndex<K>,
    expected: &HashMap<K, i64>,
) {
    assert_eq!(&index.by_key, expected);
    assert_eq!(
        index.by_deadline.values().map(HashSet::len).sum::<usize>(),
        expected.len()
    );
    for (&deadline, keys) in &index.by_deadline {
        for key in keys {
            assert_eq!(expected.get(key), Some(&deadline));
        }
    }
}

fn assert_accounting_consistent(state: &BrokerState) {
    let mut sessions = HashMap::new();
    let mut tenants = HashMap::<TenantId, TenantUsage>::new();
    let mut device_subscriptions = HashMap::<DeviceKey, usize>::new();
    let mut global_bytes = 0;
    let mut global_subscriptions = 0;
    let mut global_offline_count = 0;
    let mut global_offline_bytes = 0;
    let mut reserved_count = 0;
    let mut reserved_bytes = 0;
    let mut reserved_tenants = HashMap::<TenantId, (usize, usize)>::new();
    let mut will_bytes = 0;
    let mut will_tenants = HashMap::<TenantId, (usize, usize)>::new();
    let mut session_expiry = HashMap::new();
    let mut message_expiry = HashMap::new();
    for (key, session) in &state.sessions {
        let usage = SessionUsage::from_session(session);
        sessions.insert(key.clone(), usage);
        let tenant = tenants.entry(key.device.tenant_id.clone()).or_default();
        tenant.session_count += 1;
        tenant.session_bytes += usage.state_bytes;
        tenant.subscription_count += usage.subscriptions;
        tenant.offline_count += usage.offline_count;
        tenant.offline_bytes += usage.offline_bytes;
        tenant.qos1_inflight += usage.qos1_inflight;
        tenant.qos2_inflight += usage.qos2_inflight;
        *device_subscriptions.entry(key.device.clone()).or_default() += usage.subscriptions;
        global_bytes += usage.state_bytes;
        global_subscriptions += usage.subscriptions;
        global_offline_count += usage.offline_count;
        global_offline_bytes += usage.offline_bytes;
        if let Some(deadline) = next_message_expiry(session) {
            message_expiry.insert(key.clone(), deadline);
        }
        if !state.active.contains_key(key) {
            let deadline = if session.version == MqttVersion::V5 {
                session.expires_at_ms
            } else {
                Some(
                    session
                        .last_seen_ms
                        .saturating_add(state.session_idle_ttl_ms)
                        .saturating_add(1),
                )
            };
            if let Some(deadline) = deadline {
                session_expiry.insert(key.clone(), deadline);
            }
        }
        for reservation in session.inbound_reservations.values() {
            reserved_count += reservation.global_count;
            reserved_bytes += reservation.global_bytes;
            let tenant = reserved_tenants
                .entry(key.device.tenant_id.clone())
                .or_default();
            tenant.0 += reservation.tenant_count;
            tenant.1 += reservation.tenant_bytes;
        }
    }
    device_subscriptions.retain(|_, count| *count != 0);
    for pending in all_pending_wills(state) {
        let bytes = pending.bytes();
        will_bytes += bytes;
        let tenant = will_tenants
            .entry(pending.owner.tenant_id.clone())
            .or_default();
        tenant.0 += 1;
        tenant.1 += bytes;
        let reservation = pending.retained_reservation;
        reserved_count += reservation.global_count;
        reserved_bytes += reservation.global_bytes;
        let tenant = reserved_tenants
            .entry(pending.owner.tenant_id.clone())
            .or_default();
        tenant.0 += reservation.tenant_count;
        tenant.1 += reservation.tenant_bytes;
    }
    reserved_tenants.retain(|_, usage| *usage != (0, 0));
    assert_eq!(state.session_usage, sessions);
    assert_eq!(state.tenant_usage, tenants);
    assert_eq!(state.device_subscription_count, device_subscriptions);
    assert_eq!(state.session_bytes, global_bytes);
    assert_eq!(state.subscription_count, global_subscriptions);
    assert_eq!(state.offline_count, global_offline_count);
    assert_eq!(state.offline_bytes, global_offline_bytes);
    assert_eq!(
        state.retained_bytes,
        state
            .retained
            .values()
            .map(RetainedMessage::bytes)
            .sum::<usize>()
    );
    let mut retained_tenants = HashMap::<TenantId, (usize, usize)>::new();
    for retained in state.retained.values() {
        let usage = retained_tenants
            .entry(retained.tenant_id.clone())
            .or_default();
        usage.0 += 1;
        usage.1 += retained.bytes();
    }
    assert_eq!(state.retained_tenant_usage, retained_tenants);
    assert_eq!(state.retained_reserved_count, reserved_count);
    assert_eq!(state.retained_reserved_bytes, reserved_bytes);
    assert_eq!(state.retained_reserved_tenants, reserved_tenants);
    assert_eq!(state.will_responsibility_count, pending_will_count(state));
    assert_eq!(state.will_responsibility_bytes, will_bytes);
    assert_eq!(state.will_responsibility_tenants, will_tenants);
    let mut will_owners = HashMap::<SessionKey, BTreeSet<(i64, u64)>>::new();
    for (&deadline, bucket) in &state.future_wills {
        for (&token, pending) in bucket {
            assert_eq!(pending.due_at_ms, Some(deadline));
            if let Some((owner, _)) = &pending.cancel_on_resume {
                will_owners
                    .entry(owner.clone())
                    .or_default()
                    .insert((deadline, token));
            }
        }
    }
    assert_eq!(state.future_wills_by_session, will_owners);
    let mut queued_pending = HashMap::new();
    for ((tenant, qos), queue) in &state.pending_by_tenant {
        for (token, key) in queue {
            assert_eq!(&key.device.tenant_id, tenant);
            assert!(
                queued_pending.insert(key.clone(), (*qos, *token)).is_none(),
                "duplicate pending session"
            );
            assert!(state.sessions.contains_key(key), "deleted pending session");
            assert!(state.active.contains_key(key), "inactive pending session");
            assert!(
                state
                    .sessions
                    .get(key)
                    .and_then(|session| unsent_outbound_qos(session)
                        .or_else(|| session.offline.front().map(|message| message.qos)))
                    .is_some_and(|pending_qos| pending_qos == *qos),
                "pending session with wrong QoS or no pending work"
            );
        }
    }
    assert_eq!(state.pending_sessions, queued_pending);
    assert_eq!(
        state.pending_global.len(),
        state.pending_sessions.len(),
        "global pending index must own every pending session"
    );
    let retained_expiry = state
        .retained
        .iter()
        .filter_map(|(topic, retained)| {
            retained
                .message
                .properties
                .expires_at_ms
                .map(|deadline| (topic.clone(), deadline))
        })
        .collect::<HashMap<_, _>>();
    assert_deadline_index(&state.session_expiry, &session_expiry);
    assert_deadline_index(&state.message_expiry, &message_expiry);
    assert_deadline_index(&state.retained_expiry, &retained_expiry);
}

fn assert_broker_accounting(broker: &MqttBroker) {
    let state = lock(&broker.state).unwrap();
    assert_accounting_consistent(&state);
}

fn auth(device: &str) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new(device).unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}

fn qos2_duplicate_case() -> (Arc<MqttBroker>, Attachment, BrokerMessage) {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("qos2-duplicate");
    let attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 2)
        .unwrap();
    let message = BrokerMessage {
        topic: "v1/t/t/p/p/d/qos2-duplicate/up".into(),
        payload: b"original".to_vec().into(),
        qos: 2,
        retain: true,
        properties: Default::default(),
    };
    assert!(
        broker
            .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
            .unwrap()
    );
    (broker, attachment, message)
}

fn oversized_delivery_is_settled_locally(qos: u8) {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth(if qos == 1 {
        "oversized-qos1"
    } else {
        "oversized-qos2"
    });
    let topic = format!("v1/t/t/p/p/d/{}/up", device.device_key.device_id);
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, &topic, qos)
        .unwrap();
    for payload in [vec![b'x'; 128], b"small".to_vec()] {
        broker
            .route_from_session(
                &attachment.key,
                &BrokerMessage {
                    topic: topic.clone().into(),
                    payload: payload.into(),
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
    }
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first PUBLISH")
    };
    let first_id = first.packet_id.unwrap();
    assert!(
        broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &first)
            .unwrap()
    );
    assert!(
        broker
            .discard_outbound(&attachment.key, attachment.generation, &first)
            .unwrap()
    );
    assert!(
        !broker
            .discard_outbound(&attachment.key, attachment.generation, &first)
            .unwrap()
    );
    assert!(
        !broker.state.lock().unwrap().sessions[&attachment.key]
            .outbound
            .contains_key(&first_id)
    );
    let next = attachment.receiver.try_recv().ok().or_else(|| {
        broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
    });
    let BrokerFrame::Publish(second) = next.unwrap() else {
        panic!("expected following PUBLISH")
    };
    assert_eq!(second.message.payload.as_ref(), b"small");
    assert_eq!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .send_window
            .len(),
        1
    );
    attachment.detach().unwrap();
    let mut resumed = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    assert!(resumed.session_present);
    let BrokerFrame::Publish(resumed_delivery) = resumed.receiver.try_recv().unwrap() else {
        panic!("expected following PUBLISH on reconnect")
    };
    assert_eq!(resumed_delivery.message.payload.as_ref(), b"small");
    assert!(resumed.receiver.try_recv().is_err());
    resumed.detach().unwrap();
}

#[derive(Debug, PartialEq, Eq)]
struct TransactionAccounting {
    session_bytes: usize,
    broker_session_bytes: usize,
    offline_count: usize,
    offline_bytes: usize,
    retained_reserved_count: usize,
    retained_reserved_bytes: usize,
    inbound_qos2_count: usize,
    inbound_window_count: usize,
    tenant_qos2_inflight: usize,
    outbound_order: VecDeque<u16>,
    outbound: HashMap<u16, OutboundState>,
}

fn transaction_accounting(broker: &MqttBroker, key: &SessionKey) -> TransactionAccounting {
    let state = broker.state.lock().unwrap();
    let session = state.sessions.get(key).unwrap();
    TransactionAccounting {
        session_bytes: session.state_bytes,
        broker_session_bytes: state.session_bytes,
        offline_count: state.offline_count,
        offline_bytes: state.offline_bytes,
        retained_reserved_count: state.retained_reserved_count,
        retained_reserved_bytes: state.retained_reserved_bytes,
        inbound_qos2_count: session.inbound_qos2.len(),
        inbound_window_count: session.inbound_window.len(),
        tenant_qos2_inflight: tenant_inflight(&state, &key.device.tenant_id, 2),
        outbound_order: session.outbound_order.clone(),
        outbound: session.outbound.clone(),
    }
}

fn persistent_route_overload_is_atomic(qos: u8) {
    let limits = Arc::new(Limits {
        max_offline_messages_per_session: 1,
        max_offline_messages_per_tenant: 8,
        max_offline_messages: 8,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("atomic-route");
    let a = broker.attach(&device, "a".into(), false).unwrap();
    let b = broker.attach(&device, "b".into(), false).unwrap();
    broker
        .subscribe(&a.key, a.generation, "atomic/shared", qos)
        .unwrap();
    broker
        .subscribe(&b.key, b.generation, "atomic/shared", qos)
        .unwrap();
    broker
        .subscribe(&b.key, b.generation, "atomic/b-only", qos)
        .unwrap();
    broker.detach(&a.key, a.generation, false).unwrap();
    broker.detach(&b.key, b.generation, false).unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: "atomic/b-only".into(),
                payload: vec![1].into(),
                qos,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    assert!(matches!(
        broker.route(
            &device.device_key,
            BrokerMessage {
                topic: "atomic/shared".into(),
                payload: vec![2].into(),
                qos,
                retain: false,
                properties: Default::default(),
            },
        ),
        Err(Error::Overloaded)
    ));
    let state = broker.state.lock().unwrap();
    assert!(state.sessions[&a.key].offline.is_empty());
    assert_eq!(state.sessions[&b.key].offline.len(), 1);
}

fn route_projection_fixture(targets: usize, stored_payload_bytes: usize) -> Arc<MqttBroker> {
    let limits = Arc::new(Limits {
        max_persistent_sessions: targets + 1,
        max_persistent_sessions_per_tenant: targets + 1,
        max_subscriptions_per_device: targets + 1,
        max_subscriptions_per_tenant: targets + 1,
        max_subscriptions: targets + 1,
        max_offline_messages_per_tenant: targets.saturating_mul(2).max(1),
        max_offline_messages: targets.saturating_mul(2).max(1),
        max_offline_bytes_per_tenant: 96 * 1024 * 1024,
        max_offline_bytes: 96 * 1024 * 1024,
        max_mqtt_session_state_bytes_per_tenant: 96 * 1024 * 1024,
        global_mqtt_session_bytes: 128 * 1024 * 1024,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let device = auth("route-plan");
    for index in 0..targets {
        let attachment = broker
            .attach(&device, format!("client-{index}"), false)
            .unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "route/plan/shared",
                1,
            )
            .unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
    }
    if stored_payload_bytes > 0 {
        let mut state = broker.state.lock().unwrap();
        let mut total = 0usize;
        for session in state.sessions.values_mut() {
            let stored = BrokerMessage {
                topic: "route/plan/stored".into(),
                payload: vec![0x5a; stored_payload_bytes].into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            };
            let bytes = stored.bytes();
            session.offline.push_back(stored);
            session.offline_bytes += bytes;
            session.state_bytes += bytes;
            total += bytes;
        }
        state.offline_count += targets;
        state.offline_bytes += total;
        state.session_bytes += total;
    }
    broker
}

async fn tenant_capacity_wakes_other_session(qos: u8) {
    let limits = Arc::new(Limits {
        max_inflight_qos1_per_tenant: 1,
        max_inflight_qos2_per_tenant: 1,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let mut a = broker.attach(&auth("wake-a"), "a".into(), false).unwrap();
    let mut b = broker.attach(&auth("wake-b"), "b".into(), false).unwrap();
    for attachment in [&a, &b] {
        broker
            .subscribe(&attachment.key, attachment.generation, "wake/topic", qos)
            .unwrap();
    }
    broker
        .route(
            &a.key.device,
            BrokerMessage {
                topic: "wake/topic".into(),
                payload: vec![qos].into(),
                qos,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let (live_key, live_generation, packet_id, waiting) =
        if let Ok(BrokerFrame::Publish(delivery)) = a.receiver.try_recv() {
            (
                a.key.clone(),
                a.generation,
                delivery.packet_id.unwrap(),
                &mut b.receiver,
            )
        } else if let Ok(BrokerFrame::Publish(delivery)) = b.receiver.try_recv() {
            (
                b.key.clone(),
                b.generation,
                delivery.packet_id.unwrap(),
                &mut a.receiver,
            )
        } else {
            panic!("one tenant session must receive the initial frame")
        };
    assert!(waiting.try_recv().is_err());
    if qos == 1 {
        broker
            .puback(&live_key, live_generation, packet_id)
            .unwrap();
    } else {
        broker
            .pubrec(&live_key, live_generation, packet_id)
            .unwrap();
        assert!(
            waiting.try_recv().is_err(),
            "PUBREC does not release QoS2 capacity"
        );
        broker
            .pubcomp(&live_key, live_generation, packet_id)
            .unwrap();
    }
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), waiting.recv()).await,
        Ok(Some(BrokerFrame::Publish(_)))
    ));
}

mod accounting;
mod commands;
mod expiry;
mod inbound_qos2;
mod outbound;
mod recovery_current;
mod recovery_storage;
mod routing;
mod sessions;
mod will;

mod shared_metadata;

#[test]
fn broker_operation_probes_are_optional_and_cover_early_returns() {
    for enabled in [false, true] {
        let metrics = Arc::new(if enabled {
            Metrics::with_lock_timing()
        } else {
            Metrics::default()
        });
        let broker = MqttBroker::new_with_metrics(Arc::new(Limits::default()), metrics.clone());
        for site in BrokerProbe::ALL {
            drop(broker.lock_state(site).unwrap());
        }
        let fail = || -> Result<()> {
            let _guard = broker.lock_state(BrokerProbe::Read)?;
            Err(Error::Invalid)
        };
        assert!(fail().is_err());
        {
            let mut guard = broker.lock_state(BrokerProbe::Subscribe).unwrap();
            guard.classify(BrokerProbe::RetainedReplay);
        }
        assert!(broker.state.try_lock().is_ok());
        let rendered = metrics.render();
        if enabled {
            assert!(rendered.contains("netbaiot_broker_site_read_hold_ns_count 2\n"));
            assert!(rendered.contains("netbaiot_broker_site_retained_replay_hold_ns_count 2\n"));
            assert!(rendered.contains("netbaiot_broker_site_subscribe_hold_ns_count 1\n"));
        } else {
            assert!(!rendered.contains("netbaiot_broker_site_"));
        }
    }
}
