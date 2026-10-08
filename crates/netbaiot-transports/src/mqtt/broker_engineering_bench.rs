// Measurement only: authoritative state, production concurrency and defaults remain unchanged.
fn engineering_probe<T, R>(
    name: &str,
    shape: (usize, usize),
    metrics: Option<&Metrics>,
    mut setup: impl FnMut() -> T,
    mut action: impl FnMut(T) -> R,
    mut cleanup: impl FnMut(R),
) {
    for _ in 0..16 {
        cleanup(action(setup()));
    }
    for repeat in 1..=3 {
        let mut samples = Vec::with_capacity(128);
        let (mut allocations, mut bytes, mut locks, mut hold_ns) = (0, 0, 0, 0);
        for _ in 0..128 {
            let input = setup();
            let before = metrics.map(Metrics::render);
            let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
            let started = Instant::now();
            let output = black_box(action(input));
            samples.push(started.elapsed().as_nanos());
            let stats = region.change();
            allocations += stats.allocations + stats.reallocations;
            bytes += stats.bytes_allocated;
            if let (Some(metrics), Some(before)) = (metrics, before) {
                let after = metrics.render();
                locks += metric_total(&after, "broker_lock_hold_us", "count")
                    - metric_total(&before, "broker_lock_hold_us", "count");
                let sum = |rendered: &str| rendered.lines().filter_map(|line| {
                    line.strip_prefix("netbaiot_broker_site_")
                        .and_then(|v| v.split_once("_hold_ns_sum "))
                        .and_then(|(_, value)| value.parse::<u64>().ok())
                }).sum::<u64>();
                hold_ns += sum(&after) - sum(&before);
            }
            cleanup(output);
        }
        samples.sort_unstable();
        println!(
            "ENGINEERING_MQTT,{name},{},{},{repeat},{},{},{},{:.3},{:.3},{locks},{hold_ns}",
            shape.0, shape.1, samples[64], samples[121], samples[126],
            allocations as f64 / 128.0, bytes as f64 / 128.0,
        );
    }
}

#[test]
#[ignore = "serial release MQTT session matrix and lock measurement"]
fn engineering_mqtt_session_matrix() {
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    let owner = identity(0);
    let topic = "v1/t/tenant/p/product/d/device-0/down";
    for offline in [0, 1, 10, 100, Limits::default().max_offline_messages_per_session] {
        for outbound in [0, 1, 10, 32] {
            let base = sample_session(offline, outbound);
            let payload_bytes = base.offline.iter().map(BrokerMessage::bytes).sum::<usize>()
                + base.outbound.values().map(OutboundState::bytes).sum::<usize>();
            engineering_probe("usage", (offline, outbound), None, || (),
                |_| SessionUsage::from_session(black_box(&base)), |_| {});
            engineering_probe("expiry", (offline, outbound), None, || (),
                |_| next_message_expiry(black_box(&base)), |_| {});

            // One spare QoS1 slot admits the measured operation beside a 32-slot
            // background. This fixture-only change is explicit and is not capacity data.
            let mut fixture_limits = Limits {
                max_inflight_qos1_per_session: 33,
                max_outbound_messages_per_connection: 65,
                ..Limits::default()
            };
            fixture_limits.mqtt_recovery_max_bytes = fixture_limits.mqtt_recovery_upper_bound().unwrap();
            fixture_limits.validate().unwrap();
            let limits = Arc::new(fixture_limits);
            let metrics = Arc::new(Metrics::with_lock_timing());
            let broker = MqttBroker::new_with_metrics(limits.clone(), metrics.clone());
            let mut attachment = broker.attach_v5(&owner, "engineering".into(), false, 3600, 33).unwrap();
            broker.subscribe(&attachment.key, attachment.generation, topic, 1).unwrap();
            {
                let mut state = lock(&broker.state).unwrap();
                let target = state.sessions.get_mut(&attachment.key).unwrap();
                target.offline = base.offline.clone();
                target.offline_bytes = base.offline_bytes;
                target.state_bytes += payload_bytes;
                for (id, entry) in base.outbound.clone() {
                    target.insert_outbound(id, entry);
                    target.sent.insert(id);
                    target.send_window.insert(id);
                    target.started_outbound.insert(id);
                }
                state.offline_count += offline;
                state.offline_bytes += base.offline_bytes;
                state.session_bytes += payload_bytes;
                sync_session_usage(&mut state, &attachment.key).unwrap();
                // Isolate ACK from background offline dispatch. The reconnect
                // measurement below includes its normal resume/promotion work.
                unmark_pending(&mut state, &attachment.key);
            }
            let entry = message(topic.into(), 1, false);
            engineering_probe("route_ack", (offline, outbound), Some(&metrics), || (),
                |_| {
                    assert_eq!(broker.route(&owner.device_key, entry.clone()).unwrap(), 1);
                    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                        panic!("expected measured publish");
                    };
                    broker.puback(&attachment.key, attachment.generation, delivery.packet_id.unwrap()).unwrap();
                }, |_| {});
            engineering_probe("ack", (offline, outbound), Some(&metrics),
                || {
                    broker.route(&owner.device_key, entry.clone()).unwrap();
                    let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                        panic!("expected measured publish");
                    };
                    delivery.packet_id.unwrap()
                },
                |id| broker.puback(&attachment.key, attachment.generation, id).unwrap(), |_| {});
            // Reattach an existing authenticated persistent session; authentication
            // and socket I/O are outside this broker-only measurement.
            attachment.detach().unwrap();
            let snapshot = broker.snapshot().unwrap();
            assert_eq!(snapshot.sessions[0].offline.len(), offline);
            assert_eq!(snapshot.sessions[0].outbound.len(), outbound);
            engineering_probe("reconnect", (offline, outbound), Some(&metrics),
                || {
                    let fresh = MqttBroker::new_with_metrics(limits.clone(), metrics.clone());
                    fresh.restore(snapshot.clone()).unwrap();
                    fresh
                },
                |fresh| {
                    let resumed = fresh.attach_v5(&owner, "engineering".into(), false, 3600, 33).unwrap();
                    assert!(resumed.session_present);
                    resumed
                },
                |mut resumed| resumed.detach().unwrap());
            let state = lock(&broker.state).unwrap();
            let final_session = state.sessions.get(&attachment.key).unwrap();
            assert_eq!(final_session.offline.len(), offline);
            assert_eq!(final_session.outbound.len(), outbound);
        }
    }
}
