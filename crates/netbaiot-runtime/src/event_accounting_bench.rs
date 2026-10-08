// Isolated subsystem evidence; setup and cleanup are outside the measured region.
#[test]
#[ignore = "serial release EventBus accounting measurement"]
fn engineering_eventbus_accounting_benchmark() {
    use crate::hotspot_bench::measure;
    for depth in [1, 64, 256, 1024, 4096, 16384] {
        let limits = Limits {
            global_event_max_count: depth + 1,
            event_queue_max_count_per_tenant: depth + 1,
            sink_queue_max_count: depth + 1,
            sink_delivery_concurrency: 1,
            ..Limits::default()
        };
        let bus = route_bus(limits, vec![route(None, &["a"])]).unwrap();
        let id = SinkId::new("a").unwrap();
        let definition = bus.state.lock().unwrap().sinks[&id].definition.clone();
        for _ in 0..depth {
            bus.publish(event(8)).unwrap();
        }
        // Park the backlog so each measured new event is selected independently.
        {
            let mut state = bus.state.lock().unwrap();
            let sink = state.sinks.get_mut(&id).unwrap();
            let future = Instant::now() + Duration::from_secs(3600);
            let records = std::mem::take(&mut sink.ready);
            sink.delayed.insert(future, records);
        }
        measure("engineering_usage", depth, 1000, || (), |_| bus.usage().unwrap(), |_| {});
        measure("engineering_publish", depth, 256, || event(8),
            |event| bus.publish(event).unwrap(),
            |_| {
                let record = bus.take_ready(&id).unwrap().unwrap();
                bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
            });
        measure("engineering_complete", depth, 256,
            || { bus.publish(event(8)).unwrap(); bus.take_ready(&id).unwrap().unwrap() },
            |record| bus.complete(&id, record, Ok(SinkAck), &definition).unwrap(), |_| {});
        measure("engineering_retry", depth, 256,
            || { bus.publish(event(8)).unwrap(); bus.take_ready(&id).unwrap().unwrap() },
            |record| bus.complete(&id, record, Err(SinkError::Retryable), &definition).unwrap(),
            |_| {
                let mut state = bus.state.lock().unwrap();
                let sink = state.sinks.get_mut(&id).unwrap();
                let (_, mut records) = sink.delayed.pop_first().unwrap();
                assert_eq!(records.len(), 1);
                sink.ready.push_front(records.pop_front().unwrap());
                drop(state);
                let record = bus.take_ready(&id).unwrap().unwrap();
                bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
            });
        measure("engineering_restore", depth, 256, || vec![audit_record()],
            |records| bus.restore(records).unwrap(),
            |_| {
                let record = bus.take_ready(&id).unwrap().unwrap();
                bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
            });
        assert_eq!(bus.usage().unwrap().pending_required, depth);
    }
}
