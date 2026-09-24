//! Manual release-mode benchmarks for broker-local scaling. Run with
//! `NETBAIOT_BROKER_HOTSPOTS=1 cargo test -p netbaiot-transports --release
//! hotspot_bench -- --ignored --nocapture --test-threads=1`.
use super::*;
use std::hint::black_box;

fn identity(index: usize) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("tenant").unwrap(),
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

fn message(topic: String, qos: u8, expires: bool) -> BrokerMessage {
    BrokerMessage {
        topic,
        payload: vec![7; 64],
        qos,
        retain: false,
        properties: PublishProperties {
            expires_at_ms: expires.then(|| now_ms() + 3_600_000),
            ..Default::default()
        },
    }
}

fn measure(name: &str, size: usize, iterations: usize, mut action: impl FnMut()) {
    for _ in 0..8 {
        action();
    }
    for run in 1..=3 {
        let mut samples = Vec::with_capacity(iterations);
        let began = Instant::now();
        for _ in 0..iterations {
            let started = Instant::now();
            action();
            samples.push(started.elapsed().as_nanos());
        }
        let elapsed = began.elapsed();
        samples.sort_unstable();
        println!(
            "HOTSPOT,{name},{size},{run},{:.0},{},{},{}",
            iterations as f64 / elapsed.as_secs_f64(),
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100]
        );
    }
}

fn measure_timed(name: &str, size: usize, iterations: usize, mut action: impl FnMut() -> u128) {
    for _ in 0..8 {
        action();
    }
    for run in 1..=3 {
        let mut samples = Vec::with_capacity(iterations);
        let began = Instant::now();
        for _ in 0..iterations {
            samples.push(action());
        }
        let elapsed = began.elapsed();
        samples.sort_unstable();
        println!(
            "HOTSPOT,{name},{size},{run},{:.0},{},{},{}",
            iterations as f64 / elapsed.as_secs_f64(),
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100]
        );
    }
}

fn sample_session(offline: usize, outbound: usize) -> StoredSession {
    let owner = identity(0);
    let key = SessionKey {
        device: owner.device_key.clone(),
        client_id: "client".into(),
    };
    let mut session = StoredSession::new(key, 1, SessionAuthorization::from(&owner));
    for _ in 0..offline {
        let entry = message("v1/t/tenant/p/product/d/device-0/down".into(), 1, true);
        session.offline_bytes += entry.bytes();
        session.state_bytes += entry.bytes();
        session.offline.push_back(entry);
    }
    for index in 1..=outbound {
        let entry = message("v1/t/tenant/p/product/d/device-0/down".into(), 1, true);
        session.state_bytes += entry.bytes();
        session.insert_outbound(index as u16, OutboundState::AwaitPuback(entry));
    }
    session
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn session_recompute_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_HOTSPOTS").is_none() {
        return;
    }
    for size in [
        0,
        10,
        100,
        Limits::default().max_offline_messages_per_session,
    ] {
        let session = sample_session(size, 1);
        measure("session_usage_offline", size, 2_000, || {
            black_box(SessionUsage::from_session(black_box(&session)));
        });
        measure("message_deadline_offline", size, 2_000, || {
            black_box(next_message_expiry(black_box(&session)));
        });
    }
    for size in [1, 10, Limits::default().max_inflight_qos1_per_session] {
        let session = sample_session(0, size);
        measure("session_usage_outbound", size, 2_000, || {
            black_box(SessionUsage::from_session(black_box(&session)));
        });
        measure("message_deadline_outbound", size, 2_000, || {
            black_box(next_message_expiry(black_box(&session)));
        });
    }
}

fn sample_will(index: usize) -> PendingWill {
    let owner = identity(index).device_key;
    let key = SessionKey {
        device: owner.clone(),
        client_id: format!("client-{index}"),
    };
    PendingWill {
        owner,
        origin: Some(key.clone()),
        message: message(
            format!("v1/t/tenant/p/product/d/device-{index}/up"),
            1,
            false,
        ),
        due_at_ms: Some(now_ms() + 3_600_000 + index as i64),
        cancel_on_resume: Some((key, 1)),
        message_expiry_interval: None,
        retained_reservation: RetainedReservation::default(),
    }
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn future_will_reconnect_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_HOTSPOTS").is_none() {
        return;
    }
    let counts: &[usize] = if std::env::var_os("NETBAIOT_BROKER_WILL_50K").is_some() {
        &[50_000]
    } else {
        &[100, 1_000, 10_000]
    };
    for &count in counts {
        let limits = fanout_limits(count);
        let broker = MqttBroker::new(limits.clone());
        let mut state = lock(&broker.state).unwrap();
        for index in 0..count {
            let pending = sample_will(index);
            reserve_will_capacity(
                &mut state,
                &pending.owner.tenant_id,
                pending.bytes(),
                &limits,
            )
            .unwrap();
            insert_pending_will(&mut state, pending);
        }
        let unrelated = SessionKey {
            device: identity(count + 1).device_key,
            client_id: "unrelated".into(),
        };
        measure("will_clean_unrelated", count, 32, || {
            release_clean_start_delays(&mut state, &unrelated);
        });
        measure("will_resume_unrelated", count, 32, || {
            cancel_resumed_wills(&mut state, &unrelated, 1);
        });
        let target = SessionKey {
            device: identity(0).device_key,
            client_id: "client-0".into(),
        };
        measure_timed("will_resume_owned", count, 32, || {
            let started = Instant::now();
            cancel_resumed_wills(&mut state, &target, 1);
            let nanos = started.elapsed().as_nanos();
            let pending = sample_will(0);
            reserve_will_capacity(
                &mut state,
                &pending.owner.tenant_id,
                pending.bytes(),
                &limits,
            )
            .unwrap();
            insert_pending_will(&mut state, pending);
            nanos
        });
        measure_timed("will_clean_owned", count, 32, || {
            let started = Instant::now();
            release_clean_start_delays(&mut state, &target);
            let nanos = started.elapsed().as_nanos();
            let removed = state.pending_wills.pop_back().unwrap();
            assert_eq!(removed.origin, Some(target.clone()));
            insert_pending_will(&mut state, sample_will(0));
            nanos
        });
    }
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn retained_lookup_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_HOTSPOTS").is_none() {
        return;
    }
    for count in [1_000, 4_000] {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut state = lock(&broker.state).unwrap();
        for index in 0..count {
            let topic = format!("v1/t/tenant/p/p-{}/d/device-{index}/up", index % 100);
            state.retained.insert(
                topic.clone(),
                RetainedMessage {
                    message: message(topic, 1, false),
                    tenant_id: TenantId::new("tenant").unwrap(),
                    origin: None,
                },
            );
        }
        let filters = [
            ("retained_exact", "v1/t/tenant/p/p-1/d/device-1/up"),
            ("retained_plus", "v1/t/tenant/p/+/d/device-1/up"),
            ("retained_selective_hash", "v1/t/tenant/p/p-1/#"),
            ("retained_broad_hash", "#"),
        ];
        for (name, filter) in filters {
            measure(name, count, 500, || {
                let matched = if filter.contains(['+', '#']) {
                    state
                        .retained
                        .values()
                        .filter(|retained| topic_matches(filter, &retained.message.topic))
                        .count()
                } else {
                    state
                        .retained
                        .get(filter)
                        .filter(|retained| topic_matches(filter, &retained.message.topic))
                        .map(|_| 1)
                        .unwrap_or(0)
                };
                black_box(matched);
            });
        }
    }
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn tenant_pending_wake_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_HOTSPOTS").is_none() {
        return;
    }
    for count in [100, 1_000, 4_000] {
        let limits = Arc::new(Limits {
            max_connections: count + 1,
            max_connections_per_tenant: count + 1,
            max_persistent_sessions: count + 1,
            max_persistent_sessions_per_tenant: count + 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits.clone());
        let mut attachments = Vec::with_capacity(count);
        for index in 0..count {
            let auth = identity(index);
            let attachment = broker
                .attach_v5(&auth, format!("client-{index}"), false, 3_600, 32)
                .unwrap();
            attachments.push(attachment);
        }
        let mut state = lock(&broker.state).unwrap();
        for attachment in &attachments {
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            let entry = message(
                format!(
                    "v1/t/tenant/p/product/d/{}/down",
                    attachment.key.device.device_id.as_str()
                ),
                1,
                false,
            );
            session.offline_bytes += entry.bytes();
            session.state_bytes += entry.bytes();
            session.offline.push_back(entry);
            sync_session_usage(&mut state, &attachment.key).unwrap();
            mark_pending(&mut state, &attachment.key);
        }
        state
            .tenant_usage
            .get_mut(&TenantId::new("tenant").unwrap())
            .unwrap()
            .qos1_inflight = limits.max_inflight_qos1_per_tenant;
        measure("pending_wake_full_qos1", count, 50, || {
            wake_tenant_pending(&mut state, &TenantId::new("tenant").unwrap(), 1, &limits).unwrap();
        });
        state
            .tenant_usage
            .get_mut(&TenantId::new("tenant").unwrap())
            .unwrap()
            .qos1_inflight = 0;
        for attachment in &attachments {
            state
                .sessions
                .get_mut(&attachment.key)
                .unwrap()
                .offline
                .front_mut()
                .unwrap()
                .qos = 2;
            unmark_pending(&mut state, &attachment.key);
            mark_pending(&mut state, &attachment.key);
        }
        measure("pending_wake_wrong_qos", count, 50, || {
            wake_tenant_pending(&mut state, &TenantId::new("tenant").unwrap(), 1, &limits).unwrap();
        });
        for attachment in &attachments {
            state
                .sessions
                .get_mut(&attachment.key)
                .unwrap()
                .offline
                .front_mut()
                .unwrap()
                .qos = 1;
            unmark_pending(&mut state, &attachment.key);
            mark_pending(&mut state, &attachment.key);
        }
        let last_key = attachments.last().unwrap().key.clone();
        measure("pending_unmark_mark_tail", count, 50, || {
            unmark_pending(&mut state, &last_key);
            mark_pending(&mut state, &last_key);
        });
        state
            .tenant_usage
            .get_mut(&TenantId::new("tenant").unwrap())
            .unwrap()
            .qos1_inflight = 0;
        drop(state);
        drop(attachments);
    }
}

fn fanout_limits(count: usize) -> Arc<Limits> {
    Arc::new(Limits {
        max_connections: count + 16,
        max_connections_per_tenant: count + 16,
        max_persistent_sessions: count + 16,
        max_persistent_sessions_per_tenant: count + 16,
        max_subscriptions: count + 16,
        max_subscriptions_per_tenant: count + 16,
        max_subscriptions_per_device: count + 16,
        max_mqtt_session_state_bytes_per_tenant: 512 * 1024 * 1024,
        global_mqtt_session_bytes: 1024 * 1024 * 1024,
        max_outbound_bytes_per_tenant: 256 * 1024 * 1024,
        max_outbound_bytes: 512 * 1024 * 1024,
        ..Limits::default()
    })
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn fanout_route_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_EXTRA_BENCH").is_none() {
        return;
    }
    let auth = identity(0);
    let topic = "v1/t/tenant/p/product/d/device-0/down";
    for subscribers in [1, 10, 100, 1_000] {
        let broker = MqttBroker::new(fanout_limits(subscribers));
        let mut attachments = Vec::with_capacity(subscribers);
        for index in 0..subscribers {
            let attachment = broker
                .attach_v5(&auth, format!("fanout-{index}"), false, 3_600, 32)
                .unwrap();
            broker
                .subscribe(&attachment.key, attachment.generation, topic, 1)
                .unwrap();
            attachments.push(attachment);
        }
        for bytes in [64, 1_024, 16_384] {
            for qos in [0, 1] {
                let message = BrokerMessage {
                    topic: topic.into(),
                    payload: vec![7; bytes],
                    qos,
                    retain: false,
                    properties: Default::default(),
                };
                let mut samples = Vec::new();
                let began = Instant::now();
                for _ in 0..30 {
                    let started = Instant::now();
                    assert_eq!(
                        broker.route(&auth.device_key, message.clone()).unwrap(),
                        subscribers
                    );
                    samples.push(started.elapsed().as_nanos());
                    for attachment in &mut attachments {
                        let BrokerFrame::Publish(delivery) =
                            attachment.receiver.try_recv().unwrap()
                        else {
                            panic!("expected publish")
                        };
                        if let Some(packet_id) = delivery.packet_id {
                            broker
                                .puback(&attachment.key, attachment.generation, packet_id)
                                .unwrap();
                        }
                    }
                }
                let elapsed = began.elapsed();
                samples.sort_unstable();
                println!(
                    "FANOUT,{subscribers},{bytes},{qos},{:.0},{},{},{}",
                    30.0 / elapsed.as_secs_f64(),
                    samples[15],
                    samples[28],
                    samples[29]
                );
            }
        }
    }
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn exact_subscribe_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_EXTRA_BENCH").is_none() {
        return;
    }
    let auth = identity(0);
    let target = "v1/t/tenant/p/product/d/device-0/down";
    for retained_count in [0, 1_000, 4_000] {
        let broker = MqttBroker::new(fanout_limits(1));
        let attachment = broker
            .attach_v5(&auth, "exact-bench".into(), false, 3_600, 32)
            .unwrap();
        {
            let mut state = lock(&broker.state).unwrap();
            for index in 0..retained_count {
                let topic = format!("v1/t/tenant/p/product/d/device-{index}/down");
                state.retained.insert(
                    topic.clone(),
                    RetainedMessage {
                        tenant_id: auth.device_key.tenant_id.clone(),
                        message: message(topic, 0, false),
                        origin: None,
                    },
                );
            }
        }
        let options = v5::SubscriptionOptions {
            qos: 0,
            no_local: false,
            retain_as_published: false,
            retain_handling: 2,
        };
        measure("subscribe_exact_no_replay", retained_count, 200, || {
            broker
                .subscribe_v5(&attachment.key, attachment.generation, target, options)
                .unwrap();
        });
    }
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn single_session_route_ack_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_EXTRA_BENCH").is_none() {
        return;
    }
    let auth = identity(0);
    let topic = "v1/t/tenant/p/product/d/device-0/down";
    for offline_count in [0, 100, Limits::default().max_offline_messages_per_session] {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut attachment = broker
            .attach_v5(&auth, "single-session".into(), false, 3_600, 32)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 2)
            .unwrap();
        {
            let mut state = lock(&broker.state).unwrap();
            let added_bytes = {
                let session = state.sessions.get_mut(&attachment.key).unwrap();
                let mut added_bytes = 0;
                for _ in 0..offline_count {
                    let entry = message(topic.into(), 1, true);
                    added_bytes += entry.bytes();
                    session.offline.push_back(entry);
                }
                session.offline_bytes += added_bytes;
                session.state_bytes += added_bytes;
                added_bytes
            };
            state.offline_count += offline_count;
            state.offline_bytes += added_bytes;
            state.session_bytes += added_bytes;
            sync_session_usage(&mut state, &attachment.key).unwrap();
        }
        for qos in [1, 2] {
            measure(
                if qos == 1 {
                    "single_route_puback"
                } else {
                    "single_route_pubcomp"
                },
                offline_count,
                200,
                || {
                    broker
                        .route(&auth.device_key, message(topic.into(), qos, false))
                        .unwrap();
                    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap()
                    else {
                        panic!("expected publish")
                    };
                    let id = delivery.packet_id.unwrap();
                    if qos == 1 {
                        broker
                            .puback(&attachment.key, attachment.generation, id)
                            .unwrap();
                    } else {
                        let BrokerFrame::Pubrel { .. } = broker
                            .pubrec(&attachment.key, attachment.generation, id)
                            .unwrap()
                        else {
                            panic!("expected pubrel")
                        };
                        broker
                            .pubcomp(&attachment.key, attachment.generation, id)
                            .unwrap();
                    }
                },
            );
        }
        let extra_filter = "v1/t/tenant/p/product/d/device-0/up";
        measure("single_subscribe_unsubscribe", offline_count, 200, || {
            broker
                .subscribe(&attachment.key, attachment.generation, extra_filter, 1)
                .unwrap();
            assert!(
                broker
                    .unsubscribe(&attachment.key, attachment.generation, extra_filter)
                    .unwrap()
            );
        });
        let offline_broker = MqttBroker::new(Arc::new(Limits::default()));
        let offline_attachment = offline_broker
            .attach_v5(&auth, "offline-only".into(), false, 3_600, 32)
            .unwrap();
        {
            let mut state = lock(&offline_broker.state).unwrap();
            let session = state.sessions.get_mut(&offline_attachment.key).unwrap();
            let mut added_bytes = 0;
            for _ in 0..offline_count {
                let entry = message(topic.into(), 1, true);
                added_bytes += entry.bytes();
                session.offline.push_back(entry);
            }
            session.offline_bytes += added_bytes;
            session.state_bytes += added_bytes;
            state.offline_count += offline_count;
            state.offline_bytes += added_bytes;
            state.session_bytes += added_bytes;
            sync_session_usage(&mut state, &offline_attachment.key).unwrap();
        }
        measure_timed("single_next_offline", offline_count, 200, || {
            let started = Instant::now();
            let frame = offline_broker
                .next_offline(&offline_attachment.key, offline_attachment.generation)
                .unwrap();
            let nanos = started.elapsed().as_nanos();
            if offline_count > 0 {
                let Some(BrokerFrame::Publish(delivery)) = frame else {
                    panic!("expected offline promotion")
                };
                {
                    let mut state = lock(&offline_broker.state).unwrap();
                    unmark_pending(&mut state, &offline_attachment.key);
                }
                offline_broker
                    .puback(
                        &offline_attachment.key,
                        offline_attachment.generation,
                        delivery.packet_id.unwrap(),
                    )
                    .unwrap();
                let mut state = lock(&offline_broker.state).unwrap();
                let session = state.sessions.get_mut(&offline_attachment.key).unwrap();
                let entry = message(topic.into(), 1, true);
                let bytes = entry.bytes();
                session.offline.push_back(entry);
                session.offline_bytes += bytes;
                session.state_bytes += bytes;
                state.offline_count += 1;
                state.offline_bytes += bytes;
                state.session_bytes += bytes;
                sync_session_usage(&mut state, &offline_attachment.key).unwrap();
            } else {
                assert!(frame.is_none());
            }
            nanos
        });
    }
}

fn metric_total(rendered: &str, name: &str, kind: &str) -> u64 {
    let prefix = format!("netbaiot_{name}_{kind} ");
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

#[test]
#[ignore = "manual release-mode broker benchmark"]
fn concurrent_route_ack_scaling() {
    if std::env::var_os("NETBAIOT_BROKER_CONCURRENT_BENCH").is_none() {
        return;
    }
    let session_count = 10_000;
    let metrics = Arc::new(Metrics::with_lock_timing());
    let broker = MqttBroker::new_with_metrics(fanout_limits(session_count), metrics.clone());
    let mut attachments = Vec::with_capacity(session_count);
    for index in 0..session_count {
        let auth = identity(index);
        let attachment = broker
            .attach_v5(&auth, format!("concurrent-{index}"), false, 3_600, 32)
            .unwrap();
        let topic = format!("v1/t/tenant/p/product/d/device-{index}/down");
        broker
            .subscribe(&attachment.key, attachment.generation, &topic, 1)
            .unwrap();
        attachments.push(attachment);
    }
    for workers in [1, 2, 4, 8] {
        let before = metrics.render();
        let barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let total_operations = 1_600;
        let per_worker = total_operations / workers;
        let mut all_samples = Vec::with_capacity(total_operations);
        let elapsed = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for chunk in attachments.chunks_mut(session_count / workers) {
                let broker = broker.clone();
                let barrier = barrier.clone();
                handles.push(scope.spawn(move || {
                    let mut samples = Vec::with_capacity(per_worker);
                    barrier.wait();
                    for iteration in 0..per_worker {
                        let attachment = &mut chunk[iteration % chunk.len()];
                        let topic = format!(
                            "v1/t/tenant/p/product/d/{}/down",
                            attachment.key.device.device_id.as_str()
                        );
                        let started = Instant::now();
                        broker
                            .route(&attachment.key.device, message(topic, 1, false))
                            .unwrap();
                        let BrokerFrame::Publish(delivery) =
                            attachment.receiver.try_recv().unwrap()
                        else {
                            panic!("expected publish")
                        };
                        broker
                            .puback(
                                &attachment.key,
                                attachment.generation,
                                delivery.packet_id.unwrap(),
                            )
                            .unwrap();
                        samples.push(started.elapsed().as_nanos());
                    }
                    samples
                }));
            }
            barrier.wait();
            let began = Instant::now();
            for handle in handles {
                all_samples.extend(handle.join().unwrap());
            }
            began.elapsed()
        });
        all_samples.sort_unstable();
        let after = metrics.render();
        let wait_sum = metric_total(&after, "broker_lock_wait_us", "sum")
            - metric_total(&before, "broker_lock_wait_us", "sum");
        let wait_count = metric_total(&after, "broker_lock_wait_us", "count")
            - metric_total(&before, "broker_lock_wait_us", "count");
        let hold_sum = metric_total(&after, "broker_lock_hold_us", "sum")
            - metric_total(&before, "broker_lock_hold_us", "sum");
        let hold_count = metric_total(&after, "broker_lock_hold_us", "count")
            - metric_total(&before, "broker_lock_hold_us", "count");
        println!(
            "CONCURRENT,{session_count},{workers},{:.0},{},{},{},{},{},{},{}",
            total_operations as f64 / elapsed.as_secs_f64(),
            all_samples[total_operations / 2],
            all_samples[total_operations * 95 / 100],
            all_samples[total_operations * 99 / 100],
            wait_sum,
            wait_count,
            hold_sum,
            hold_count,
        );
    }
}
