// Included within event::tests; setup and state restoration are untimed.
#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_simultaneous_due() {
    if std::env::var_os("NETBAIOT_SECOND_ROUND").is_none() { return; }
    if cfg!(debug_assertions) { panic!("release measurements only"); }
    for depth in [128, 1024, 4096, Limits::default().global_event_max_count] {
        for percent in [0, 10, 50, 100] {
            for buckets in ["same", "distinct"] {
                let limits = Arc::new(Limits {
                    sink_queue_max_count: depth,
                    ..Limits::default()
                });
                let id = SinkId::new("due").unwrap();
                let metrics = Arc::new(Metrics::with_lock_timing());
                let definition = SinkDefinition::bounded(id.clone(), SinkDeliveryMode::ConfirmedRequired, Arc::new(Ack), &limits);
                let bus = EventBus::new_paused(limits, metrics.clone(), vec![definition.clone()], vec![RouteDefinition { tenant: None, sinks: vec![id.clone()] }], 1).unwrap();
                for _ in 0..depth { bus.publish(event(8)).unwrap(); }
                let original: Vec<_> = {
                    let mut state = bus.state.lock().unwrap();
                    state.sinks.get_mut(&id).unwrap().ready.drain(..).collect()
                };
                let name = format!("due_{percent}_{buckets}");
                let mut hold = Vec::new();
                let mut wait = Vec::new();
                for run in 1..=3 {
                    let mut samples = Vec::with_capacity(256);
                    let (mut allocations, mut bytes) = (0usize, 0usize);
                    for _ in 0..256 {
                        {
                            let mut state = bus.state.lock().unwrap();
                            let sink = state.sinks.get_mut(&id).unwrap();
                            sink.ready.clear(); sink.delayed.clear(); sink.inflight = 0;
                            let due = Instant::now() - Duration::from_secs(1);
                            for (index, record) in original.iter().enumerate() {
                                let mut record = record.clone();
                                if index < depth * percent / 100 {
                                    record.next_attempt = due + Duration::from_nanos(if buckets == "same" { 0 } else { index as u64 });
                                    sink.delayed.entry(record.next_attempt).or_default().push_back(record);
                                } else { sink.ready.push_back(record); }
                            }
                        }
                        crate::hotspot_bench::enable_clock(true);
                        let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
                        let began = Instant::now();
                        let record = bus.take_ready(&id).unwrap().unwrap();
                        let elapsed = began.elapsed().as_nanos();
                        let stats = region.change();
                        let timing = crate::hotspot_bench::last_lock();
                        crate::hotspot_bench::enable_clock(false);
                        samples.push(elapsed);
                        wait.push(timing.0); hold.push(timing.1);
                        allocations += stats.allocations + stats.reallocations;
                        bytes += stats.bytes_allocated;
                        std::hint::black_box(&record);
                        {
                            let mut state = bus.state.lock().unwrap();
                            let sink = state.sinks.get_mut(&id).unwrap();
                            sink.inflight -= 1;
                            sink.ready.push_back(record);
                        }
                        if percent == 100 { assert_eq!(bus.next_ready_delay(&id).unwrap(), Some(Duration::ZERO)); }
                    }
                    samples.sort_unstable();
                    println!("SECOND_ROUND,{name},{depth},{run},{:.3},{},{},{},{},{:.3},{:.3}", samples.iter().sum::<u128>() as f64 / 256., samples[128], samples[243], samples[253], samples[255], allocations as f64 / 256., bytes as f64 / 256.);
                }
                wait.sort_unstable(); hold.sort_unstable();
                println!("SECOND_LOCK,{name},{depth},{:.3},{},{},{},{},{:.3},{},{},{},{}", wait.iter().sum::<u128>() as f64 / wait.len() as f64, wait[wait.len()/2], wait[wait.len()*95/100], wait[wait.len()*99/100], wait[wait.len()-1], hold.iter().sum::<u128>() as f64 / hold.len() as f64, hold[hold.len()/2], hold[hold.len()*95/100], hold[hold.len()*99/100], hold[hold.len()-1]);
                // A full drain exercises every due bucket, ordered promotion and throughput.
                let began = Instant::now();
                let mut count = 0;
                while let Some(record) = bus.take_ready(&id).unwrap() {
                    bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
                    count += 1;
                }
                println!("SECOND_DRAIN,{name},{depth},{count},{:.3}", began.elapsed().as_secs_f64());
            }
        }
    }
}
