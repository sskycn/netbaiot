use super::*;

#[test]
fn mqtt_qos0_live_delivery_obeys_byte_budget() {
    let device = auth("byte-budget");
    let topic = "v1/t/t/p/p/d/byte-budget/up";
    let message = BrokerMessage {
        topic: topic.into(),
        payload: vec![7; 512].into(),
        qos: 0,
        retain: false,
        properties: Default::default(),
    };
    let charge = message.bytes();
    let limits = Arc::new(Limits {
        max_outbound_bytes_per_connection: charge,
        max_outbound_bytes_per_tenant: charge * 2,
        max_outbound_bytes: charge * 3,
        ..Limits::default()
    });
    let broker = MqttBroker::new(limits);
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 0)
        .unwrap();
    assert_eq!(
        broker.route(&device.device_key, message.clone()).unwrap(),
        1
    );
    for _ in 0..100 {
        assert_eq!(
            broker.route(&device.device_key, message.clone()).unwrap(),
            0
        );
    }
    let state = broker.state.lock().unwrap();
    let active = &state.active[&attachment.key];
    assert_eq!(active.connection_bytes.available(), 0);
    assert_eq!(active.tenant_bytes.available(), charge);
    assert_eq!(active.global_bytes.available(), charge * 2);
    drop(state);
    let frame = attachment.receiver.try_recv().unwrap();
    assert_eq!(
        broker.state.lock().unwrap().active[&attachment.key]
            .connection_bytes
            .available(),
        0
    );
    drop(frame);
    let state = broker.state.lock().unwrap();
    let active = &state.active[&attachment.key];
    assert_eq!(active.connection_bytes.available(), charge);
    assert_eq!(active.tenant_bytes.available(), charge * 2);
    assert_eq!(active.global_bytes.available(), charge * 3);
    drop(state);
    attachment.detach().unwrap();
}

#[test]
fn mqtt_live_delivery_obeys_tenant_and_global_byte_budgets() {
    let message = |tenant: &str, device: &str| BrokerMessage {
        topic: (format!("v1/t/{tenant}/p/p/d/{device}/up")).into(),
        payload: vec![3; 256].into(),
        qos: 0,
        retain: false,
        properties: Default::default(),
    };
    let charge = message("t", "aa").bytes();
    let broker = MqttBroker::new(Arc::new(Limits {
        max_outbound_bytes_per_connection: charge,
        max_outbound_bytes_per_tenant: charge * 2,
        max_outbound_bytes: charge * 3,
        ..Limits::default()
    }));
    let mut attachments = Vec::new();
    for (tenant, name) in [
        ("t", "aa"),
        ("t", "bb"),
        ("t", "cc"),
        ("u", "dd"),
        ("u", "ee"),
    ] {
        let mut device = auth(name);
        device.device_key.tenant_id = TenantId::new(tenant).unwrap();
        let attachment = broker.attach(&device, name.into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                &message(tenant, name).topic,
                0,
            )
            .unwrap();
        attachments.push((device, attachment));
    }
    for (index, expected) in [(0, 1), (1, 1), (2, 0), (3, 1), (4, 0)] {
        let (device, _) = &attachments[index];
        assert_eq!(
            broker
                .route(
                    &device.device_key,
                    message(
                        device.device_key.tenant_id.as_str(),
                        device.device_key.device_id.as_str()
                    )
                )
                .unwrap(),
            expected,
        );
    }
    assert_eq!(broker.global_outbound_bytes.available(), 0);
    for index in [0, 1, 3] {
        drop(attachments[index].1.receiver.try_recv().unwrap());
    }
    assert_eq!(broker.global_outbound_bytes.available(), charge * 3);
    let (device, _) = &attachments[2];
    assert_eq!(
        broker
            .route(&device.device_key, message("t", "cc"))
            .unwrap(),
        1
    );
    drop(attachments[2].1.receiver.try_recv().unwrap());
    for (_, mut attachment) in attachments {
        attachment.detach().unwrap();
    }
}

#[test]
fn released_global_outbound_bytes_wake_another_tenant() {
    let mut first_device = auth("aa");
    first_device.device_key.tenant_id = TenantId::new("t").unwrap();
    let mut second_device = auth("bb");
    second_device.device_key.tenant_id = TenantId::new("u").unwrap();
    let message = |tenant: &str, device: &str| BrokerMessage {
        topic: (format!("v1/t/{tenant}/p/p/d/{device}/up")).into(),
        payload: vec![5; 256].into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let charge = message("t", "aa").bytes();
    let broker = MqttBroker::new(Arc::new(Limits {
        max_outbound_bytes_per_connection: charge,
        max_outbound_bytes_per_tenant: charge,
        max_outbound_bytes: charge,
        ..Limits::default()
    }));
    let mut first = broker.attach(&first_device, "first".into(), false).unwrap();
    let mut second = broker
        .attach(&second_device, "second".into(), false)
        .unwrap();
    broker
        .subscribe(&first.key, first.generation, &message("t", "aa").topic, 1)
        .unwrap();
    broker
        .subscribe(&second.key, second.generation, &message("u", "bb").topic, 1)
        .unwrap();
    broker
        .route(&first_device.device_key, message("t", "aa"))
        .unwrap();
    broker
        .route(&second_device.device_key, message("u", "bb"))
        .unwrap();
    assert_eq!(
        broker.state.lock().unwrap().sessions[&second.key]
            .offline
            .len(),
        1
    );
    assert!(second.receiver.try_recv().is_err());
    drop(first.receiver.try_recv().unwrap());
    broker.outbound_bytes_released().unwrap();
    let BrokerFrame::Publish(delivery) = second.receiver.try_recv().unwrap() else {
        panic!("released global byte budget must wake waiting tenant");
    };
    assert_eq!(delivery.message.payload, vec![5; 256]);
    drop(delivery);
    assert_eq!(broker.global_outbound_bytes.available(), charge);
    first.detach().unwrap();
    second.detach().unwrap();
}

#[test]
fn released_global_bytes_wake_deferred_reconnect_replay() {
    let mut first_device = auth("aa");
    first_device.device_key.tenant_id = TenantId::new("t").unwrap();
    let mut second_device = auth("bb");
    second_device.device_key.tenant_id = TenantId::new("u").unwrap();
    let message = |tenant: &str, device: &str| BrokerMessage {
        topic: (format!("v1/t/{tenant}/p/p/d/{device}/up")).into(),
        payload: vec![3; 256].into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    let charge = message("t", "aa").bytes();
    let broker = MqttBroker::new(Arc::new(Limits {
        max_outbound_bytes_per_connection: charge,
        max_outbound_bytes_per_tenant: charge,
        max_outbound_bytes: charge,
        ..Limits::default()
    }));
    let mut second = broker
        .attach(&second_device, "second".into(), false)
        .unwrap();
    broker
        .subscribe(&second.key, second.generation, &message("u", "bb").topic, 1)
        .unwrap();
    broker
        .route(&second_device.device_key, message("u", "bb"))
        .unwrap();
    let BrokerFrame::Publish(original) = second.receiver.try_recv().unwrap() else {
        panic!("expected first transfer")
    };
    let original_id = original.packet_id;
    drop(original);
    second.detach().unwrap();

    let mut first = broker.attach(&first_device, "first".into(), false).unwrap();
    broker
        .subscribe(&first.key, first.generation, &message("t", "aa").topic, 1)
        .unwrap();
    broker
        .route(&first_device.device_key, message("t", "aa"))
        .unwrap();
    let mut resumed = broker
        .attach(&second_device, "second".into(), false)
        .unwrap();
    assert!(resumed.receiver.try_recv().is_err());
    assert!(
        broker
            .state
            .lock()
            .unwrap()
            .pending_sessions
            .contains_key(&resumed.key)
    );
    drop(first.receiver.try_recv().unwrap());
    broker.outbound_bytes_released().unwrap();
    let BrokerFrame::Publish(replayed) = resumed.receiver.try_recv().unwrap() else {
        panic!("byte release must wake the deferred QoS replay")
    };
    assert_eq!(replayed.packet_id, original_id);
    assert!(replayed.dup);
    drop(replayed);
    first.detach().unwrap();
    resumed.detach().unwrap();
}

#[test]
fn command_is_rejected_when_send_quota_is_full() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("command-expiry");
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/command-expiry/down",
            1,
        )
        .unwrap();
    let deadline = now_ms() + 10_000;
    let command = |payload: &[u8]| BrokerMessage {
        topic: "v1/t/t/p/p/d/command-expiry/down".into(),
        payload: payload.to_vec().into(),
        qos: 1,
        retain: false,
        properties: (PublishProperties {
            expires_at_ms: Some(deadline),
            ..Default::default()
        })
        .into(),
    };
    broker
        .send_live(&attachment.key, attachment.generation, command(b"first"))
        .unwrap();
    assert!(matches!(
        broker.send_live(&attachment.key, attachment.generation, command(b"second")),
        Err(Error::Overloaded)
    ));
    let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
        panic!("expected first command")
    };
    assert_eq!(
        broker.state.lock().unwrap().sessions[&attachment.key]
            .offline
            .len(),
        0
    );
    broker
        .puback(
            &attachment.key,
            attachment.generation,
            first.packet_id.unwrap(),
        )
        .unwrap();
    assert!(
        broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
            .is_none()
    );
    attachment.detach().unwrap();
}

#[test]
fn command_requires_subscription_and_current_generation() {
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("command-fence");
    let mut first = broker.attach(&device, "client".into(), false).unwrap();
    let down = "v1/t/t/p/p/d/command-fence/down";
    let message = || BrokerMessage {
        topic: down.into(),
        payload: b"command".to_vec().into(),
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    assert!(matches!(
        broker.send_live(&first.key, first.generation, message()),
        Err(Error::Unavailable)
    ));
    broker
        .subscribe(&first.key, first.generation, down, 1)
        .unwrap();
    let mut second = broker.attach(&device, "client".into(), false).unwrap();
    assert!(matches!(
        broker.send_live(&first.key, first.generation, message()),
        Err(Error::Unavailable)
    ));
    assert!(second.receiver.try_recv().is_err());
    broker
        .send_live(&second.key, second.generation, message())
        .unwrap();
    let BrokerFrame::Publish(delivery) = second.receiver.try_recv().unwrap() else {
        panic!("expected command publish")
    };
    assert!(delivery.command);
    let command_packet_id = delivery.packet_id.unwrap();
    broker
        .route(
            &device.device_key,
            BrokerMessage {
                topic: down.into(),
                payload: b"ordinary".to_vec().into(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let BrokerFrame::Publish(ordinary) = second.receiver.try_recv().unwrap() else {
        panic!("expected ordinary subscription publish")
    };
    assert!(!ordinary.command);
    assert!(
        !broker
            .puback(&second.key, second.generation, ordinary.packet_id.unwrap())
            .unwrap()
    );
    broker
        .unsubscribe(&second.key, second.generation, down)
        .unwrap();
    assert!(matches!(
        broker.send_live(&second.key, second.generation, message()),
        Err(Error::Unavailable)
    ));
    assert!(
        broker
            .puback(&second.key, second.generation, command_packet_id)
            .unwrap()
    );
    first.detach().unwrap();
    second.detach().unwrap();
}

#[test]
fn stale_generation_cannot_send_command_to_replacement_connection() {
    use std::sync::Barrier;

    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("stale-command");
    let down = "v1/t/t/p/p/d/stale-command/down";
    let mut old = broker.attach(&device, "client".into(), false).unwrap();
    broker.subscribe(&old.key, old.generation, down, 1).unwrap();
    let ready = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let worker_broker = broker.clone();
    let old_key = old.key.clone();
    let old_generation = old.generation;
    let worker_ready = ready.clone();
    let worker_release = release.clone();
    let worker = std::thread::spawn(move || {
        let command = BrokerMessage {
            topic: down.into(),
            payload: b"old-command".to_vec().into(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        };
        worker_ready.wait(); // old task has dequeued its command
        worker_release.wait();
        worker_broker.send_live(&old_key, old_generation, command)
    });
    ready.wait();
    let mut replacement = broker.attach(&device, "client".into(), false).unwrap();
    release.wait();
    assert!(matches!(worker.join().unwrap(), Err(Error::Unavailable)));
    assert!(replacement.receiver.try_recv().is_err());
    old.detach().unwrap();
    replacement.detach().unwrap();
}

#[tokio::test]
async fn command_expiry_survives_snapshot_restore() {
    let limits = Arc::new(Limits::default());
    let broker = MqttBroker::new(limits.clone());
    let device = auth("command-recovery");
    let mut attachment = broker
        .attach_v5(&device, "client".into(), false, 60, 1)
        .unwrap();
    broker
        .subscribe(
            &attachment.key,
            attachment.generation,
            "v1/t/t/p/p/d/command-recovery/down",
            1,
        )
        .unwrap();
    let deadline = now_ms() + 30_000;
    broker
        .send_live(
            &attachment.key,
            attachment.generation,
            BrokerMessage {
                topic: "v1/t/t/p/p/d/command-recovery/down".into(),
                payload: b"command".to_vec().into(),
                qos: 1,
                retain: false,
                properties: (PublishProperties {
                    expires_at_ms: Some(deadline),
                    ..Default::default()
                })
                .into(),
            },
        )
        .unwrap();
    attachment.detach().unwrap();
    let root = std::env::temp_dir().join(format!(
        "netbaiot-command-recovery-{}",
        uuid::Uuid::new_v4()
    ));
    broker.commit_to(&root).await.unwrap();
    let recovered = MqttBroker::new(limits);
    assert!(recovered.recover_from(&root).await.unwrap());
    let state = recovered.state.lock().unwrap();
    let session = state.sessions.get(&attachment.key).unwrap();
    let message = session.outbound.values().next().unwrap();
    assert_eq!(
        match message {
            OutboundState::AwaitPuback(message) => message.properties.expires_at_ms,
            _ => None,
        },
        Some(deadline)
    );
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn qos0_drop_and_closed_receiver_release_all_command_resources() {
    use netbaiot_core::DeliveryState;
    use netbaiot_runtime::{CommandProgress, Metric};
    for v5 in [false, true] {
        for closed_before_enqueue in [false, true] {
            let broker = MqttBroker::new(Arc::new(Limits::default()));
            let device = auth("qos0-cleanup");
            let mut attachment = if v5 {
                broker.attach_v5(&device, "command".into(), false, 60, 8)
            } else {
                broker.attach(&device, "command".into(), false)
            }
            .unwrap();
            let topic = "v1/t/t/p/p/d/qos0-cleanup/down";
            broker
                .subscribe(&attachment.key, attachment.generation, topic, 0)
                .unwrap();
            let baseline = broker.usage().unwrap();
            let (active, packet_id) = {
                let state = broker.state.lock().unwrap();
                (
                    state.active[&attachment.key].clone(),
                    state.sessions[&attachment.key].next_packet_id,
                )
            };
            let available = (
                active.connection_bytes.available(),
                active.tenant_bytes.available(),
                active.global_bytes.available(),
            );
            let metrics = Arc::new(Metrics::default());
            let progress = CommandProgress::new(now_ms() + 60_000, metrics.clone());
            if closed_before_enqueue {
                attachment.receiver.close();
            }
            let result = broker.send_live_tracked(
                &attachment.key,
                attachment.generation,
                BrokerMessage {
                    topic: topic.into(),
                    payload: vec![1].into(),
                    qos: 0,
                    retain: false,
                    properties: (PublishProperties::default()).into(),
                },
                Some(progress.clone()),
            );
            assert_eq!(result.is_err(), closed_before_enqueue);
            if !closed_before_enqueue {
                assert_eq!(progress.state(), DeliveryState::Queued);
                assert_eq!(attachment.receiver.len(), 1);
                assert!(active.connection_bytes.available() < available.0);
            }
            let key = attachment.key.clone();
            drop(attachment);
            progress.abandon_unsent(); // Same handoff-error cleanup as the connection loops.
            assert_eq!(progress.state(), DeliveryState::Failed);
            assert_eq!(metrics.get(Metric::CommandFailed), 1);
            assert_eq!(Arc::strong_count(&progress), 1);
            assert_eq!(
                (
                    active.connection_bytes.available(),
                    active.tenant_bytes.available(),
                    active.global_bytes.available()
                ),
                available
            );
            assert_eq!(broker.usage().unwrap(), baseline);
            let state = broker.state.lock().unwrap();
            let session = &state.sessions[&key];
            assert_eq!(session.next_packet_id, packet_id);
            assert!(session.outbound.is_empty());
            assert!(session.outbound_order.is_empty());
            assert!(session.command_progress.is_empty());
            assert!(session.command_outbound.is_empty());
            assert!(session.started_outbound.is_empty());
            assert!(session.send_window.is_empty());
            assert!(session.sent.is_empty());
            assert!(session.offline.is_empty());
            assert_accounting_consistent(&state);
        }
    }
}

#[test]
fn expired_command_at_broker_handoff_releases_all_responsibility() {
    use netbaiot_core::DeliveryState;
    use netbaiot_runtime::CommandProgress;
    let broker = MqttBroker::new(Arc::new(Limits::default()));
    let device = auth("expired-handoff");
    let mut attachment = broker.attach(&device, "client".into(), false).unwrap();
    let topic = "v1/t/t/p/p/d/expired-handoff/down";
    broker
        .subscribe(&attachment.key, attachment.generation, topic, 2)
        .unwrap();
    let usage = broker.usage().unwrap();
    for qos in 0..=2 {
        let metrics = Arc::new(Metrics::default());
        let expiry = now_ms() - 1;
        let progress = CommandProgress::new(expiry, metrics.clone());
        let message = BrokerMessage {
            topic: topic.into(),
            payload: vec![1].into(),
            qos,
            retain: false,
            properties: (PublishProperties {
                expires_at_ms: Some(expiry),
                ..Default::default()
            })
            .into(),
        };
        broker
            .send_live_tracked(
                &attachment.key,
                attachment.generation,
                message.clone(),
                Some(progress.clone()),
            )
            .unwrap();
        // The final enqueue check must also handle expiry after preflight.
        enqueue(
            &mut broker.state.lock().unwrap(),
            &attachment.key,
            message,
            &broker.limits,
            None,
            true,
            Some(progress.clone()),
        )
        .unwrap();
        progress.update(DeliveryState::Sent);
        assert_eq!(progress.state(), DeliveryState::Expired);
        assert_eq!(metrics.get(netbaiot_runtime::Metric::CommandFailed), 1);
        assert!(attachment.receiver.try_recv().is_err());
        assert_eq!(broker.usage().unwrap(), usage);
    }
    attachment.detach().unwrap();
}

#[test]
fn command_progress_expiry_and_exact_ack_transitions_for_both_mqtt_versions() {
    use netbaiot_core::DeliveryState;
    use netbaiot_runtime::CommandProgress;
    for v5 in [false, true] {
        let metrics = Arc::new(netbaiot_runtime::Metrics::default());
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("progress");
        let mut attachment = if v5 {
            broker
                .attach_v5(&device, "progress".into(), false, 60, 8)
                .unwrap()
        } else {
            broker.attach(&device, "progress".into(), false).unwrap()
        };
        let topic = "v1/t/t/p/p/d/progress/down";
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        for expired in [true, false] {
            let expiry = now_ms() + 10_000;
            let progress = CommandProgress::new(expiry, metrics.clone());
            broker
                .send_live_tracked(
                    &attachment.key,
                    attachment.generation,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: vec![1].into(),
                        qos: 1,
                        retain: false,
                        properties: (PublishProperties {
                            expires_at_ms: Some(expiry),
                            ..Default::default()
                        })
                        .into(),
                    },
                    Some(progress.clone()),
                )
                .unwrap();
            let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                panic!("publish expected")
            };
            let id = delivery.packet_id.unwrap();
            if expired {
                assert!(progress.expire(expiry));
                assert!(
                    !broker
                        .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                        .unwrap()
                );
                assert_eq!(progress.state(), DeliveryState::Expired);
                assert_eq!(metrics.get(netbaiot_runtime::Metric::CommandFailed), 1);
                assert!(
                    !broker.state.lock().unwrap().sessions[&attachment.key]
                        .outbound
                        .contains_key(&id)
                );
            } else {
                assert!(
                    broker
                        .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                        .unwrap()
                );
                progress.update(DeliveryState::Sent);
                assert!(!progress.expire(expiry));
                assert!(
                    broker
                        .pubcomp(&attachment.key, attachment.generation, id)
                        .is_err()
                );
                assert_eq!(progress.state(), DeliveryState::Sent);
                assert!(
                    broker
                        .puback(&attachment.key, attachment.generation, id)
                        .unwrap()
                );
                assert_eq!(progress.state(), DeliveryState::Received);
            }
            assert!(
                broker.state.lock().unwrap().sessions[&attachment.key]
                    .command_progress
                    .is_empty()
            );
        }
        drop(attachment);
    }
}
