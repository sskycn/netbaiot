//! Manual, release-mode broker scaling measurements. The environment guard keeps
//! large fixtures out of ordinary `cargo test --all-targets` and CI.
use netbaiot_core::*;
use netbaiot_runtime::Limits;
use netbaiot_transports::mqtt::{
    broker::{Attachment, BrokerFrame, BrokerMessage, MqttBroker},
    topics::{TopicKind, topic},
};
use std::{
    hint::black_box,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Fixture {
    broker: Arc<MqttBroker>,
    auth: AuthenticatedDevice,
    attachment: Attachment,
    topic: String,
    message: BrokerMessage,
}

fn auth(tenant: String, device: String) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new(tenant).unwrap(),
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

fn limits(count: usize) -> Arc<Limits> {
    Arc::new(Limits {
        max_persistent_sessions: count + 64,
        max_persistent_sessions_per_tenant: count + 64,
        max_connections: count + 64,
        max_connections_per_tenant: count + 64,
        max_subscriptions_per_device: count + 64,
        max_subscriptions_per_tenant: count + 64,
        max_subscriptions: count + 64,
        max_offline_messages_per_tenant: count + 64,
        max_offline_messages: count + 64,
        max_mqtt_session_state_bytes_per_tenant: 512 * 1024 * 1024,
        global_mqtt_session_bytes: 1024 * 1024 * 1024,
        ..Limits::default()
    })
}

fn fixture(count: usize, distribution: &str) -> Fixture {
    let broker = MqttBroker::new(limits(count));
    let mut target = None;
    for index in 0..count {
        let tenant = if distribution == "many_tenants" {
            format!("tenant-{}", index % 100)
        } else {
            "tenant".to_owned()
        };
        let device = if distribution == "same_device" {
            "device".to_owned()
        } else {
            format!("device-{index}")
        };
        let identity = auth(tenant, device);
        let mut attachment = broker
            .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
            .unwrap();
        if index == 0 {
            target = Some((identity, attachment));
        } else {
            if distribution == "same_device" {
                broker
                    .subscribe(
                        &attachment.key,
                        attachment.generation,
                        &topic(&identity.device_key, TopicKind::Down),
                        1,
                    )
                    .unwrap();
            }
            attachment.detach().unwrap();
        }
    }
    let (identity, attachment) = target.unwrap();
    let topic = topic(&identity.device_key, TopicKind::Down);
    broker
        .subscribe(&attachment.key, attachment.generation, &topic, 1)
        .unwrap();
    let message = BrokerMessage {
        topic: topic.clone(),
        payload: vec![7; 64],
        qos: 1,
        retain: false,
        properties: Default::default(),
    };
    Fixture {
        broker,
        auth: identity,
        attachment,
        topic,
        message,
    }
}

fn measure(name: &str, sessions: usize, runs: usize, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..5 {
        f();
    }
    for run in 1..=runs {
        let mut samples = Vec::with_capacity(iterations);
        let start = Instant::now();
        for _ in 0..iterations {
            let began = Instant::now();
            f();
            samples.push(began.elapsed().as_nanos());
        }
        let elapsed = start.elapsed();
        samples.sort_unstable();
        println!(
            "SCALE,{name},{sessions},{run},{:.0},{},{},{}",
            iterations as f64 / elapsed.as_secs_f64(),
            samples[iterations / 2],
            samples[iterations * 95 / 100],
            samples[iterations * 99 / 100]
        );
    }
}

fn route_ack(fixture: &mut Fixture) {
    fixture
        .broker
        .route(&fixture.auth.device_key, fixture.message.clone())
        .unwrap();
    let BrokerFrame::Publish(delivery) = fixture.attachment.receiver.try_recv().unwrap() else {
        panic!("expected the single matching subscriber")
    };
    fixture
        .broker
        .puback(
            &fixture.attachment.key,
            fixture.attachment.generation,
            delivery.packet_id.unwrap(),
        )
        .unwrap();
}

fn run(count: usize, distribution: &str) {
    let setup = Instant::now();
    let mut case = fixture(count, distribution);
    println!(
        "SETUP,{distribution},{count},{:.3}",
        setup.elapsed().as_secs_f64()
    );
    let iterations = if count >= 50_000 { 100 } else { 200 };
    if distribution != "same_device" {
        measure(
            &format!("route_ack_{distribution}"),
            count,
            5,
            iterations,
            || {
                route_ack(&mut case);
            },
        );
    }
    measure(
        &format!("next_offline_empty_{distribution}"),
        count,
        5,
        iterations,
        || {
            black_box(
                case.broker
                    .next_offline(&case.attachment.key, case.attachment.generation)
                    .unwrap(),
            );
        },
    );
    measure(
        &format!("tick_unexpired_{distribution}"),
        count,
        5,
        iterations,
        || {
            case.broker.tick().unwrap();
        },
    );
    let filter = format!("{}/#", case.topic.rsplit_once('/').unwrap().0);
    measure(
        &format!("subscribe_unsubscribe_{distribution}"),
        count,
        5,
        iterations,
        || {
            case.broker
                .subscribe(&case.attachment.key, case.attachment.generation, &filter, 1)
                .unwrap();
            assert!(
                case.broker
                    .unsubscribe(&case.attachment.key, case.attachment.generation, &filter)
                    .unwrap()
            );
        },
    );
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn route_offline(case: &Fixture, index: usize, deadline: i64) {
    let identity = auth("tenant".to_owned(), format!("device-{index}"));
    let message = BrokerMessage {
        topic: topic(&identity.device_key, TopicKind::Down),
        payload: vec![7; 64],
        qos: 1,
        retain: false,
        properties: netbaiot_transports::mqtt::broker::PublishProperties {
            expires_at_ms: Some(deadline),
            ..Default::default()
        },
    };
    case.broker.route(&identity.device_key, message).unwrap();
}

fn run_expiry_extra(count: usize) {
    let setup = Instant::now();
    let case = fixture(count, "same_tenant");
    for index in 1..=1_001 {
        let identity = auth("tenant".to_owned(), format!("device-{index}"));
        let mut attachment = case
            .broker
            .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
            .unwrap();
        case.broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                &topic(&identity.device_key, TopicKind::Down),
                1,
            )
            .unwrap();
        attachment.detach().unwrap();
    }
    println!(
        "SETUP,expiry_extra,{count},{:.3}",
        setup.elapsed().as_secs_f64()
    );
    let mut index = 0;
    measure("queue_offline_1000_targets", count, 5, 200, || {
        index = index % 1_000 + 1;
        route_offline(&case, index, now_ms() + 3_600_000);
    });
    measure("tick_many_future_offline", count, 5, 100, || {
        case.broker.tick().unwrap();
    });
    let identity = auth("tenant".to_owned(), "device-1001".to_owned());
    measure("reconnect_promote_offline", count, 5, 100, || {
        route_offline(&case, 1_001, now_ms() + 3_600_000);
        let mut attachment = case
            .broker
            .attach_v5(&identity, "client-1001".to_owned(), false, 3_600, 32)
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected offline delivery on reconnect")
        };
        case.broker
            .puback(
                &attachment.key,
                attachment.generation,
                delivery.packet_id.unwrap(),
            )
            .unwrap();
        attachment.detach().unwrap();
    });
    for run in 1..=5 {
        for index in 1..=10 {
            route_offline(&case, index, now_ms() + 1_000);
        }
        thread::sleep(Duration::from_millis(1_050));
        let began = Instant::now();
        case.broker.tick().unwrap();
        let nanos = began.elapsed().as_nanos();
        println!("SCALE,tick_due_10_of_1000_offline,{count},{run},0,{nanos},{nanos},{nanos}");
    }
}

fn main() {
    let mode = std::env::var("NETBAIOT_MQTT_SCALE_BENCH").unwrap_or_default();
    if mode.is_empty() {
        println!("manual broker scale benchmark skipped");
        return;
    }
    println!("SCALE_HEADER,scenario,sessions,run,ops_per_sec,p50_ns,p95_ns,p99_ns");
    if mode == "expiry_extra" {
        run_expiry_extra(50_000);
        return;
    }
    for count in [100, 1_000, 10_000, 50_000] {
        run(count, "same_tenant");
    }
    for count in [10_000, 50_000] {
        run(count, "many_tenants");
    }
    for count in [100, 1_000, 10_000] {
        run(count, "same_device");
    }
}
