//! Serial measurement overlay: no production behavior or limits are changed.
use super::*;
use std::hint::black_box;

fn enabled() -> bool {
    std::env::var_os("NETBAIOT_SECOND_ROUND").is_some()
}

fn probe<T, R>(
    name: &str,
    size: usize,
    iterations: usize,
    batch: usize,
    mut setup: impl FnMut() -> T,
    mut action: impl FnMut(T) -> R,
    mut cleanup: impl FnMut(R),
) {
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    for _ in 0..32 {
        cleanup(action(setup()));
    }
    for run in 1..=3 {
        let mut samples = Vec::with_capacity(iterations);
        let (mut allocations, mut bytes) = (0usize, 0usize);
        for _ in 0..iterations {
            let input = setup();
            let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
            let started = Instant::now();
            let output = black_box(action(input));
            let elapsed = started.elapsed().as_nanos();
            let stats = region.change();
            allocations += stats.allocations + stats.reallocations;
            bytes += stats.bytes_allocated;
            cleanup(output);
            samples.push(elapsed / batch as u128);
        }
        let mean = samples.iter().sum::<u128>() as f64 / iterations as f64;
        samples.sort_unstable();
        let ops = (iterations * batch) as f64;
        println!(
            "SECOND_ROUND,{name},{size},{run},{mean:.3},{},{},{},{},{:.3},{:.3}",
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100],
            samples[iterations - 1],
            allocations as f64 / ops,
            bytes as f64 / ops,
        );
    }
}

fn auth(index: usize, tenants: usize) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new(format!("tenant-{}", index % tenants)).unwrap(),
            product_id: ProductId::new("product").unwrap(),
            device_id: DeviceId::new(format!("device-{index}")).unwrap(),
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

fn publish(topic: &str, qos: u8, expiry: Option<i64>) -> BrokerMessage {
    BrokerMessage {
        topic: topic.into(),
        payload: vec![7; 1024].into(),
        qos,
        retain: false,
        properties: PublishProperties {
            expires_at_ms: expiry,
            ..Default::default()
        },
    }
}

fn limits(count: usize) -> Arc<Limits> {
    Arc::new(Limits {
        max_connections: count + 16,
        max_connections_per_tenant: count + 16,
        max_persistent_sessions: count + 16,
        max_persistent_sessions_per_tenant: count + 16,
        max_subscriptions: count + 16,
        max_subscriptions_per_tenant: count + 16,
        max_subscriptions_per_device: count + 16,
        max_inflight_qos1_per_session: count + 16,
        max_inflight_qos2_per_session: count + 16,
        max_inflight_qos1_per_tenant: (count + 16) * (count + 16),
        max_inflight_qos2_per_tenant: (count + 16) * (count + 16),
        max_outbound_messages_per_connection: count + 16,
        max_offline_messages_per_session: count + 16,
        max_mqtt_session_state_bytes: 16 * 1024 * 1024,
        max_mqtt_session_state_bytes_per_tenant: 512 * 1024 * 1024,
        global_mqtt_session_bytes: 1024 * 1024 * 1024,
        max_outbound_bytes_per_connection: 16 * 1024 * 1024,
        max_outbound_bytes_per_tenant: 256 * 1024 * 1024,
        max_outbound_bytes: 512 * 1024 * 1024,
        ..Limits::default()
    })
}

fn session(offline: usize, outbound: usize, distribution: &str) -> StoredSession {
    let identity = auth(0, 1);
    let key = SessionKey {
        device: identity.device_key.clone(),
        client_id: "matrix".into(),
    };
    let mut session = StoredSession::new(key, 1, SessionAuthorization::from(&identity));
    let deadline = now_ms() + 3_600_000;
    let expiry = |index| match distribution {
        "none" => None,
        "same" => Some(deadline),
        "random" => Some(deadline + ((index * 7919) % 257) as i64),
        _ => Some(deadline + index as i64),
    };
    for index in 0..offline {
        let message = publish("v1/t/tenant-0/p/product/d/device-0/down", 1, expiry(index));
        session.state_bytes += message.bytes();
        session.offline_bytes += message.bytes();
        session.offline.push_back(message);
    }
    for index in 1..=outbound {
        let qos = if index % 2 == 0 { 2 } else { 1 };
        let message = publish(
            "v1/t/tenant-0/p/product/d/device-0/down",
            qos,
            expiry(index),
        );
        session.state_bytes += message.bytes();
        session.insert_outbound(
            index as u16,
            if qos == 1 {
                OutboundState::AwaitPuback(message)
            } else {
                OutboundState::AwaitPubrec(message)
            },
        );
    }
    session
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_topic() {
    if !enabled() {
        return;
    }
    for levels in [1, 4, 8, 16, 32] {
        let topic = vec!["level"; levels].join("/");
        for pattern in [
            "exact",
            "plus",
            "hash",
            "mixed",
            "early_mismatch",
            "late_mismatch",
        ] {
            let mut pieces = vec!["level"; levels];
            match pattern {
                "plus" => pieces.fill("+"),
                "hash" => pieces[levels - 1] = "#",
                "mixed" => {
                    for index in (0..levels).step_by(2) {
                        pieces[index] = "+";
                    }
                }
                "early_mismatch" => pieces[0] = "other",
                "late_mismatch" => pieces[levels - 1] = "other",
                _ => {}
            }
            let filter = pieces.join("/");
            probe(
                &format!("topic_{pattern}"),
                levels,
                1024,
                64,
                || (),
                |_| {
                    for _ in 0..64 {
                        black_box(topic_matches(black_box(&filter), black_box(&topic)));
                    }
                },
                |_| {},
            );
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_preflight() {
    if !enabled() {
        return;
    }
    let topic = "v1/t/tenant-0/p/product/d/device-0/down";
    for fanout in [1, 10, 100, 1000] {
        for tenants in [1, 10, 100, 1000].into_iter().filter(|n| *n <= fanout) {
            let limits = limits(fanout);
            let broker = MqttBroker::new(limits.clone());
            let mut attachments = Vec::new();
            for index in 0..fanout {
                let identity = auth(index, tenants);
                let attachment = broker
                    .attach_v5(&identity, format!("fanout-{index}"), false, 3600, u16::MAX)
                    .unwrap();
                broker
                    .subscribe(&attachment.key, attachment.generation, topic, 2)
                    .unwrap();
                attachments.push(attachment);
            }
            let state = lock(&broker.state).unwrap();
            let owner = auth(0, 1).device_key;
            for qos in [0, 1, 2] {
                let message = publish(topic, qos, None);
                let charge = message.bytes();
                probe(
                    &format!("preflight_tenants{tenants}_qos{qos}"),
                    fanout,
                    256,
                    1,
                    || (),
                    |_| preflight_route(&state, &owner, None, &message, &limits, charge).unwrap(),
                    |plan| assert_eq!(plan.targets.len(), fanout),
                );
            }
            drop(state);
            drop(attachments);
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_session() {
    if !enabled() {
        return;
    }
    for count in [1, 8, 16, 32, 64, 128, 256] {
        let session = session(0, count, "none");
        let limits = limits(count);
        probe(
            "session_usage",
            count,
            1024,
            64,
            || (),
            |_| {
                for _ in 0..64 {
                    black_box(SessionUsage::from_session(black_box(&session)));
                }
            },
            |_| {},
        );
        for qos in [1, 2] {
            probe(
                &format!("capacity_qos{qos}"),
                count,
                1024,
                64,
                || (),
                |_| {
                    for _ in 0..64 {
                        black_box(session.has_outbound_capacity(qos, black_box(&limits)));
                    }
                },
                |_| {},
            );
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_expiry() {
    if !enabled() {
        return;
    }
    for offline in [0, 16, 64, 128, 512] {
        for outbound in [0, 16, 32, 128, 256] {
            for distribution in ["none", "distinct", "same", "random"] {
                let session = session(offline, outbound, distribution);
                probe(
                    &format!("expiry_offline{offline}_{distribution}"),
                    outbound,
                    256,
                    16,
                    || (),
                    |_| {
                        for _ in 0..16 {
                            black_box(next_message_expiry(black_box(&session)));
                        }
                    },
                    |_| {},
                );
                let key = session.key.clone();
                let broker = MqttBroker::new(limits(1024));
                let mut state = lock(&broker.state).unwrap();
                state.sessions.insert(key.clone(), session);
                sync_session_usage(&mut state, &key).unwrap();
                probe(
                    &format!("sync_offline{offline}_{distribution}"),
                    outbound,
                    256,
                    1,
                    || (),
                    |_| {
                        sync_session_usage(&mut state, &key).unwrap();
                    },
                    |_| {},
                );
            }
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_retained() {
    if !enabled() {
        return;
    }
    for count in [10, 100, 1000, 4000, 10000] {
        let broker = MqttBroker::new(limits(1));
        let mut state = lock(&broker.state).unwrap();
        for index in 0..count {
            let topic = format!(
                "v1/t/tenant-0/p/product-{}/d/device-{index}/up",
                index % 100
            );
            state.retained.insert(
                topic.clone(),
                RetainedMessage {
                    message: publish(&topic, 1, None),
                    tenant_id: TenantId::new("tenant-0").unwrap(),
                    origin: None,
                },
            );
        }
        for (name, filter) in [
            ("exact", "v1/t/tenant-0/p/product-1/d/device-1/up"),
            ("plus", "v1/t/tenant-0/p/+/d/device-1/up"),
            ("selective", "v1/t/tenant-0/p/product-1/#"),
            ("v1", "v1/#"),
            ("broad", "#"),
            ("none", "v1/t/missing/#"),
        ] {
            probe(
                &format!("retained_{name}"),
                count,
                256,
                1,
                || (),
                |_| {
                    if filter.contains(['+', '#']) {
                        black_box(
                            state
                                .retained
                                .values()
                                .filter(|entry| topic_matches(filter, &entry.message.topic))
                                .count(),
                        )
                    } else {
                        usize::from(state.retained.contains_key(filter))
                    }
                },
                |_| {},
            );
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_order() {
    if !enabled() {
        return;
    }
    for count in [1, 16, 32, 64, 128, 256, 1024] {
        let session = session(0, count, "none");
        for name in ["front", "middle", "back", "random"] {
            let mut sequence = 0usize;
            probe(
                &format!("remove_{name}"),
                count,
                512,
                1,
                || {
                    sequence += 1;
                    let id = match name {
                        "front" => 1,
                        "middle" => count.div_ceil(2),
                        "back" => count,
                        _ => (sequence * 7919 % count) + 1,
                    };
                    (session.clone(), id as u16)
                },
                |(mut input, id)| {
                    let removed = input.remove_outbound(id).unwrap();
                    (input, removed)
                },
                |_| {},
            );
        }
    }
}

fn finish(broker: &MqttBroker, attachment: &mut Attachment) {
    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
        panic!("publish expected")
    };
    if let Some(id) = delivery.packet_id {
        assert!(
            broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                .unwrap()
        );
        if delivery.message.qos == 1 {
            broker
                .puback(&attachment.key, attachment.generation, id)
                .unwrap();
        } else {
            broker
                .pubrec(&attachment.key, attachment.generation, id)
                .unwrap();
            broker
                .pubcomp(&attachment.key, attachment.generation, id)
                .unwrap();
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_route_ack() {
    if !enabled() {
        return;
    }
    let owner = auth(0, 1);
    let topic = "v1/t/tenant-0/p/product/d/device-0/down";
    for count in [1, 8, 16, 32, 64, 128, 256] {
        for qos in [1, 2] {
            for mix in ["pure", "mixed"] {
                let broker = MqttBroker::new(limits(count));
                let mut attachment = broker
                    .attach_v5(&owner, "matrix".into(), false, 3600, u16::MAX)
                    .unwrap();
                broker
                    .subscribe(&attachment.key, attachment.generation, topic, 2)
                    .unwrap();
                {
                    let mut state = lock(&broker.state).unwrap();
                    let dummy = session(0, count, "none");
                    let target = state.sessions.get_mut(&attachment.key).unwrap();
                    let mut bytes = 0;
                    for (id, mut outbound) in dummy.outbound {
                        if mix == "pure" {
                            let mut message = (match &outbound {
                                OutboundState::AwaitPuback(m)
                                | OutboundState::AwaitPubrec(m)
                                | OutboundState::AwaitPubcomp(m) => m,
                            })
                            .clone();
                            message.qos = qos;
                            outbound = if qos == 1 {
                                OutboundState::AwaitPuback(message)
                            } else {
                                OutboundState::AwaitPubrec(message)
                            };
                        }
                        bytes += outbound.bytes();
                        target.insert_outbound(id, outbound);
                        target.sent.insert(id);
                        target.send_window.insert(id);
                        target.started_outbound.insert(id);
                    }
                    target.state_bytes += bytes;
                    state.session_bytes += bytes;
                    sync_session_usage(&mut state, &attachment.key).unwrap();
                }
                let message = publish(topic, qos, None);
                probe(
                    &format!("route_ack_qos{qos}_{mix}"),
                    count,
                    512,
                    1,
                    || (),
                    |_| {
                        broker
                            .route_from_session(&attachment.key, &message)
                            .unwrap();
                        finish(&broker, &mut attachment);
                    },
                    |_| {},
                );
            }
        }
    }
}

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_metadata() {
    if !enabled() {
        return;
    }
    let owner = auth(0, 1);
    for fanout in [1, 10, 100, 1000] {
        for profile in ["a", "b", "c", "d"] {
            let topic = "t".repeat(if profile == "b" { 256 } else { 32 });
            let broker = MqttBroker::new(limits(fanout));
            let mut attachments = Vec::new();
            for index in 0..fanout {
                let attachment = broker
                    .attach_v5(&owner, format!("meta-{index}"), false, 3600, u16::MAX)
                    .unwrap();
                broker
                    .subscribe(&attachment.key, attachment.generation, &topic, 1)
                    .unwrap();
                attachments.push(attachment);
            }
            let mut message = publish(&topic, 1, None);
            if ["c", "d"].contains(&profile) {
                message.properties = PublishProperties {
                    content_type: Some("application/json".into()),
                    response_topic: Some("v1/t/tenant-0/p/product/d/device-0/down_ack".into()),
                    correlation_data: Some(vec![7; 64]),
                    user_properties: (0..if profile == "c" { 8 } else { 16 })
                        .map(|index| {
                            (
                                format!("key-{index:02}"),
                                "v".repeat(if profile == "c" { 16 } else { 100 }),
                            )
                        })
                        .collect(),
                    ..Default::default()
                };
            }
            assert!(message.properties.valid(&broker.limits));
            let source = attachments[0].key.clone();
            probe(
                &format!("metadata_{profile}"),
                fanout,
                128,
                1,
                || {
                    let mut fresh = message.clone();
                    fresh.payload = vec![7; 1024].into();
                    fresh
                },
                |fresh| broker.route_from_session(&source, &fresh).unwrap(),
                |delivered| {
                    assert_eq!(delivered, fanout);
                    for attachment in &mut attachments {
                        finish(&broker, attachment);
                    }
                },
            );
        }
    }
}

// These controls measure a full state-machine operation with existing inflight
// messages already transferred. Wall time is an upper bound on uncontended hold,
// not a measurement of lock wait under network concurrency.
#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_ownership() {
    if !enabled() {
        return;
    }
    for count in [1, 8, 16, 32, 64, 128, 256, 1024] {
        for qos in [1, 2] {
            let broker = MqttBroker::new(limits(count));
            let owner = auth(0, 1);
            let attachment = broker
                .attach_v5(&owner, "ownership".into(), false, 3600, u16::MAX)
                .unwrap();
            let template = {
                let mut state = lock(&broker.state).unwrap();
                let target = state.sessions.get_mut(&attachment.key).unwrap();
                for id in 1..=count as u16 {
                    let message = publish("v1/t/tenant-0/p/product/d/device-0/down", qos, None);
                    target.state_bytes += message.bytes();
                    target.insert_outbound(
                        id,
                        if qos == 1 {
                            OutboundState::AwaitPuback(message)
                        } else {
                            OutboundState::AwaitPubcomp(message)
                        },
                    );
                    target.started_outbound.insert(id);
                }
                state.session_bytes = state.sessions[&attachment.key].state_bytes;
                sync_session_usage(&mut state, &attachment.key).unwrap();
                state.sessions[&attachment.key].clone()
            };
            for position in ["front", "middle", "back", "random"] {
                let mut sequence = 0;
                probe(
                    &format!("ack_qos{qos}_{position}"),
                    count,
                    256,
                    1,
                    || {
                        {
                            let mut state = lock(&broker.state).unwrap();
                            state
                                .sessions
                                .insert(attachment.key.clone(), template.clone());
                            state.session_bytes = template.state_bytes;
                            sync_session_usage(&mut state, &attachment.key).unwrap();
                        }
                        sequence += 1;
                        (match position {
                            "front" => 1,
                            "middle" => count.div_ceil(2),
                            "back" => count,
                            _ => sequence * 7919 % count + 1,
                        }) as u16
                    },
                    |id| {
                        if qos == 1 {
                            broker
                                .puback(&attachment.key, attachment.generation, id)
                                .unwrap()
                        } else {
                            broker
                                .pubcomp(&attachment.key, attachment.generation, id)
                                .unwrap()
                        }
                    },
                    |_| {},
                );
            }
        }
    }
    for offline in [0, 16, 64, 128, 512] {
        for count in [0, 16, 32, 128, 256] {
            for distribution in ["none", "distinct", "same", "random"] {
                let mut base = session(offline, count, distribution);
                base.started_outbound.extend(base.outbound.keys().copied());
                probe(
                    &format!("expiry_started_offline{offline}_{distribution}"),
                    count,
                    256,
                    16,
                    || (),
                    |_| {
                        for _ in 0..16 {
                            black_box(next_message_expiry(&base));
                        }
                    },
                    |_| {},
                );
                if count > 0 {
                    probe(
                        &format!("expiry_remove_earliest_offline{offline}_{distribution}"),
                        count,
                        256,
                        1,
                        || base.clone(),
                        |mut input| {
                            black_box(input.remove_outbound(1));
                            black_box(next_message_expiry(&input));
                            input
                        },
                        |_| {},
                    );
                }
            }
        }
    }
}
