//! Serial allocator measurements: these are owned in-memory sessions, not sockets.
use super::*;
use std::mem::size_of;

#[test]
#[ignore = "isolated serial release allocation measurement"]
fn density_memory() {
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    println!("DENSITY_SIZE,StoredSession,{}", size_of::<StoredSession>());
    println!("DENSITY_SIZE,ActiveSession,{}", size_of::<ActiveSession>());
    println!("DENSITY_SIZE,SessionKey,{}", size_of::<SessionKey>());
    println!("DENSITY_SIZE,DeviceKey,{}", size_of::<DeviceKey>());
    println!("DENSITY_SIZE,Attachment,{}", size_of::<Attachment>());
    println!("DENSITY_SIZE,BrokerFrame,{}", size_of::<BrokerFrame>());
    println!(
        "DENSITY_SIZE,HashMap_empty,{}",
        size_of::<HashMap<u16, u16>>()
    );
    println!("DENSITY_SIZE,HashSet_empty,{}", size_of::<HashSet<u16>>());
    println!("DENSITY_SIZE,VecDeque_empty,{}", size_of::<VecDeque<u16>>());
    {
        let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
        let channel = frame_channel::channel(64);
        let stats = region.change();
        println!(
            "DENSITY_ALLOC,broker_channel,{},{},{}",
            stats.allocations, stats.bytes_allocated, stats.bytes_deallocated
        );
        drop(channel);
    }
    {
        let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
        let token = CancellationToken::new();
        let stats = region.change();
        println!(
            "DENSITY_ALLOC,cancel_token,{},{},{}",
            stats.allocations, stats.bytes_allocated, stats.bytes_deallocated
        );
        drop(token);
    }
    for count in [1_000, 10_000] {
        let limits = Arc::new(Limits {
            max_connections: count,
            max_connections_per_tenant: count,
            max_persistent_sessions: count,
            max_persistent_sessions_per_tenant: count,
            global_mqtt_session_bytes: 1024 * 1024 * 1024,
            max_mqtt_session_state_bytes_per_tenant: 1024 * 1024 * 1024,
            ..Limits::default()
        });
        let identities = (0..count)
            .map(|index| AuthenticatedDevice {
                device_key: DeviceKey {
                    tenant_id: TenantId::new("density-tenant").unwrap(),
                    product_id: ProductId::new("density-product").unwrap(),
                    device_id: DeviceId::new(format!("density-device-{index:07}")).unwrap(),
                },
                credential_version: 1,
                auth_generation: 1,
                codec_id: CodecId::new("json").unwrap(),
                codec_version: 1,
                permissions: Permissions {
                    publish: true,
                    commands: true,
                },
            })
            .collect::<Vec<_>>();
        let broker = MqttBroker::new(limits);
        let mut attachments = Vec::with_capacity(count);
        let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
        let started = Instant::now();
        for (index, auth) in identities.iter().enumerate() {
            attachments.push(
                broker
                    .attach(auth, format!("density-client-{index:07}"), false)
                    .unwrap(),
            );
        }
        let elapsed = started.elapsed();
        let stats = region.change();
        let state = broker.state.lock().unwrap();
        println!(
            "DENSITY_BROKER,{count},{},{},{},{},{:.3},{}",
            stats.allocations,
            stats.deallocations,
            stats.bytes_allocated,
            stats.bytes_deallocated,
            (stats.bytes_allocated - stats.bytes_deallocated) as f64 / count as f64,
            elapsed.as_micros()
        );
        println!(
            "DENSITY_CAPACITY,{count},{},{},{}",
            state.sessions.capacity(),
            state.active.capacity(),
            state.session_usage.capacity()
        );
        drop(state);
        drop(attachments);
    }
}
