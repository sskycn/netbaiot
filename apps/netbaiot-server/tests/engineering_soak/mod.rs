use super::*;
use std::collections::HashMap;

#[derive(Default)]
struct Observed {
    events: HashMap<String, (EventId, bool)>,
    requests: usize,
}

struct Receiver {
    address: std::net::SocketAddr,
    failing: Arc<AtomicBool>,
    observed: Arc<Mutex<Observed>>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Receiver {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let failing = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(Mutex::new(Observed::default()));
        let stop = CancellationToken::new();
        let task = {
            let failing = failing.clone();
            let observed = observed.clone();
            let stop = stop.clone();
            // One owned fixture task. Requests are sequential, size/deadline bounded.
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = tokio::select! {
                        _ = stop.cancelled() => break,
                        accepted = listener.accept() => accepted.unwrap(),
                    };
                    let request = async {
                        let mut bytes = Vec::new();
                        let body = loop {
                            let mut chunk = [0; 1024];
                            let n = socket.read(&mut chunk).await.unwrap();
                            if n == 0 {
                                return;
                            }
                            bytes.extend_from_slice(&chunk[..n]);
                            assert!(bytes.len() <= 8192);
                            let Some(at) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                                continue;
                            };
                            let start = at + 4;
                            let headers = String::from_utf8_lossy(&bytes[..start]);
                            let len = headers
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|v| v.trim().parse::<usize>().ok())
                                })
                                .unwrap();
                            assert!(len <= 4096);
                            if bytes.len() >= start + len {
                                break serde_json::from_slice::<serde_json::Value>(
                                    &bytes[start..start + len],
                                )
                                .unwrap();
                            }
                        };
                        let failed = failing.load(Ordering::Acquire);
                        let id: EventId = serde_json::from_value(body["event_id"].clone()).unwrap();
                        let source = body["source_message_id"].as_str().unwrap().to_owned();
                        {
                            let mut state = observed.lock().unwrap();
                            assert!(state.events.len() < 2048);
                            assert!(state.requests < 8192);
                            state.requests += 1;
                            let entry = state.events.entry(source).or_insert((id, false));
                            assert_eq!(entry.0, id, "retry changed stable event_id");
                            entry.1 |= !failed;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let response = if failed {
                            b"HTTP/1.1 503 Test\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice()
                        } else {
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                .as_slice()
                        };
                        let _ = socket.write_all(response).await;
                    };
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = tokio::time::timeout(Duration::from_secs(2), request) => {},
                    }
                }
            })
        };
        Self {
            address,
            failing,
            observed,
            stop,
            task,
        }
    }
    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}

async fn status(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    admin: &str,
) -> serde_json::Value {
    client
        .get(format!("https://{address}/api/v1/status"))
        .bearer_auth(admin)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn exercise_tls_outage(publishes: u16) {
    let task_baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let receiver = Receiver::start().await;
    let mut c = config();
    c.device_ingress = "127.0.0.1:0".parse().unwrap();
    c.management_http = "127.0.0.1:0".parse().unwrap();
    c.tls = Some(test_tls_files());
    c.delivery_url = Some(format!("http://{}/events", receiver.address));
    c.limits.global_event_max_count = 1024;
    c.limits.event_queue_max_count_per_tenant = 512;
    c.limits.sink_queue_max_count = 512;
    c.limits.sink_delivery_concurrency = 2;
    c.limits.sink_timeout_ms = 200;
    c.limits.retry_base_ms = 10;
    c.limits.retry_max_ms = 100;
    c.limits.shutdown_drain_timeout_ms = 5000;
    let directory = std::env::temp_dir().join(format!(
        "netbaiot-engineering-soak-{}",
        uuid::Uuid::new_v4()
    ));
    c.spool_directory = directory.clone();
    let limits = Arc::new(c.limits.clone());
    assert!(c.diagnostics().is_empty(), "{:?}", c.diagnostics());
    let admin = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let stop = CancellationToken::new();
    let (ready, bound) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(netbaiot_server::run_with_credentials_ready(
        c,
        stop.clone(),
        Some(admin.clone()),
        None,
        Some(ready),
    ));
    let bound = tokio::time::timeout(Duration::from_secs(20), bound)
        .await
        .unwrap()
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(test_tls_files().certificate).unwrap())
                .unwrap(),
        )
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let mut mqtt = device_socket(bound.device_ingress, true).await;
    mqtt.write_all(&mqtt_connect("engineering-soak", true))
        .await
        .unwrap();
    assert_eq!(mqtt_read(&mut *mqtt).await, (0x20, vec![0, 0]));
    let began = tokio::time::Instant::now();
    let mut ack_ns = Vec::with_capacity(usize::from(publishes));
    let mut outage_pending = 0;
    let mut outage_bytes = 0;
    for sequence in 1..=publishes {
        if sequence == publishes / 3 {
            receiver.failing.store(true, Ordering::Release);
        }
        if sequence == publishes * 2 / 3 {
            let state = status(&client, bound.management_http, &admin).await;
            outage_pending = state["pending_required"].as_u64().unwrap();
            outage_bytes = state["event_bytes"].as_u64().unwrap();
            assert!(outage_pending > 0);
            assert!(state["event_count"].as_u64().unwrap() <= 512);
            assert!(
                state["sink_diagnostics"][0]["failure_total"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            receiver.failing.store(false, Ordering::Release);
        }
        let payload = format!(
            r#"{{"schema_version":1,"source_message_id":"soak:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#
        );
        let started = tokio::time::Instant::now();
        mqtt.write_all(&mqtt_publish(
            sequence,
            "v1/t/demo/p/sensor/d/device-1/up",
            payload.as_bytes(),
            1,
            false,
        ))
        .await
        .unwrap();
        let mut ack = [0; 4];
        let result = tokio::time::timeout(Duration::from_secs(2), mqtt.read_exact(&mut ack)).await;
        assert!(
            matches!(result, Ok(Ok(_))),
            "sequence {sequence}: {result:?}"
        );
        assert_eq!(ack, [0x40, 2, (sequence >> 8) as u8, sequence as u8]);
        ack_ns.push(started.elapsed().as_nanos());
        // Ten publishes/second stays below the unchanged 16/device/second limit.
        tokio::time::sleep_until(began + Duration::from_millis(u64::from(sequence) * 100)).await;
    }
    mqtt.write_all(&[0xe0, 0]).await.unwrap();
    drop(mqtt);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = status(&client, bound.management_http, &admin).await;
            if state["pending_required"] == 0 {
                assert_eq!(state["event_count"], 0);
                assert_eq!(state["event_bytes"], 0);
                for sink in state["sink_diagnostics"].as_array().unwrap() {
                    assert_eq!(sink["queue_count"], 0);
                    assert_eq!(sink["queue_bytes"], 0);
                    assert_eq!(sink["inflight"], 0);
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let requests = {
        let observed = receiver.observed.lock().unwrap();
        assert_eq!(observed.events.len(), usize::from(publishes));
        for sequence in 1..=publishes {
            assert!(
                observed.events[&format!("soak:{sequence}")].1,
                "event lacks a successful sink ACK"
            );
        }
        observed.requests
    };
    let shutdown = tokio::time::Instant::now();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let shutdown_ms = shutdown.elapsed().as_millis();
    assert!(
        RestartSpool::new(directory.clone(), limits)
            .recover()
            .await
            .unwrap()
            .records
            .is_empty()
    );
    receiver.shutdown().await;
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            > task_baseline
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    ack_ns.sort_unstable();
    println!(
        "ENGINEERING_SOAK,tls_mqtt_qos1_http,{publishes},{requests},{outage_pending},{outage_bytes},{},{},{},{shutdown_ms}",
        began.elapsed().as_millis(),
        ack_ns[ack_ns.len() / 2],
        ack_ns[ack_ns.len() * 95 / 100]
    );
}

#[tokio::test]
async fn tls_qos1_http_outage_recovers_without_accounting_leaks() {
    exercise_tls_outage(30).await;
}

#[tokio::test]
#[ignore = "bounded 60-second TLS MQTT QoS1 confirmed HTTP outage soak"]
async fn engineering_tls_qos1_http_sixty_second_soak() {
    exercise_tls_outage(600).await;
}
