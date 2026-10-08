use super::*;
use std::sync::Mutex;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[derive(Clone)]
struct ResponseSpec {
    status: u16,
    retry_after: Option<String>,
    body_bytes: usize,
    chunked: bool,
    invalid: bool,
    delay: Duration,
}
impl ResponseSpec {
    fn status(status: u16) -> Self {
        Self {
            status,
            retry_after: None,
            body_bytes: 0,
            chunked: false,
            invalid: false,
            delay: Duration::ZERO,
        }
    }
}

struct HttpFixture {
    url: String,
    response: Arc<Mutex<ResponseSpec>>,
    events: Arc<Mutex<Vec<EventId>>>,
    stop: CancellationToken,
    worker: tokio::task::JoinHandle<()>,
}
impl HttpFixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/events", listener.local_addr().unwrap());
        let response = Arc::new(Mutex::new(ResponseSpec::status(200)));
        let events = Arc::new(Mutex::new(Vec::new()));
        let stop = CancellationToken::new();
        let worker = {
            let response = response.clone();
            let events = events.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = tokio::select! {
                        _ = stop.cancelled() => break,
                        accepted = listener.accept() => accepted.unwrap(),
                    };
                    let handle = async {
                        let mut bytes = Vec::new();
                        loop {
                            let mut chunk = [0; 4096];
                            let n = socket.read(&mut chunk).await.unwrap();
                            if n == 0 {
                                return;
                            }
                            bytes.extend_from_slice(&chunk[..n]);
                            assert!(bytes.len() <= 131_072);
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
                            assert!(len <= 65_536);
                            if bytes.len() < start + len {
                                continue;
                            }
                            let body: serde_json::Value =
                                serde_json::from_slice(&bytes[start..start + len]).unwrap();
                            let id: EventId =
                                serde_json::from_value(body["event_id"].clone()).unwrap();
                            {
                                let mut events = events.lock().unwrap();
                                assert!(events.len() < 8192);
                                events.push(id);
                            }
                            break;
                        }
                        let spec = response.lock().unwrap().clone();
                        tokio::time::sleep(spec.delay).await;
                        let result = if spec.invalid {
                            socket.write_all(b"not an HTTP response\r\n\r\n").await
                        } else {
                            let mut header =
                                format!("HTTP/1.1 {} Test\r\nConnection: close\r\n", spec.status);
                            if let Some(value) = spec.retry_after {
                                header.push_str(&format!("Retry-After: {value}\r\n"));
                            }
                            if spec.chunked {
                                header.push_str("Transfer-Encoding: chunked\r\n\r\n");
                                header.push_str(&format!(
                                    "{:x}\r\n{}\r\n0\r\n\r\n",
                                    spec.body_bytes,
                                    "x".repeat(spec.body_bytes)
                                ));
                            } else {
                                header.push_str(&format!(
                                    "Content-Length: {}\r\n\r\n{}",
                                    spec.body_bytes,
                                    "x".repeat(spec.body_bytes)
                                ));
                            }
                            socket.write_all(header.as_bytes()).await
                        };
                        let _ = result; // A timed-out client may already have closed its socket.
                    };
                    tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::timeout(Duration::from_secs(2), handle) => {} }
                }
            })
        };
        Self {
            url,
            response,
            events,
            stop,
            worker,
        }
    }
    async fn shutdown(self) {
        self.stop.cancel();
        self.worker.await.unwrap();
    }
}

fn envelope() -> DeliveryEnvelope {
    DeliveryEnvelope {
        event: Arc::new(super::tests::event(DeviceEventKind::Heartbeat(Heartbeat {
            sequence: 1,
        }))),
        sink_id: SinkId::new("http").unwrap(),
        attempt: 1,
        accepted_at: netbaiot_runtime::now_ms(),
    }
}

#[tokio::test]
async fn http_failure_classes_and_retry_after_are_bounded() {
    let fixture = HttpFixture::new().await;
    let limits = Limits {
        sink_timeout_ms: 50,
        retry_max_ms: 100,
        ..Limits::default()
    };
    let sink = HttpSink::new(&fixture.url, &limits).unwrap();
    for (status, reason, class) in [
        (500, SinkFailureReason::Http5xx, SinkError::Retryable),
        (429, SinkFailureReason::Http429, SinkError::Retryable),
        (503, SinkFailureReason::Http5xx, SinkError::Retryable),
        (401, SinkFailureReason::HttpAuth, SinkError::Permanent),
        (403, SinkFailureReason::HttpAuth, SinkError::Permanent),
        (404, SinkFailureReason::Http4xx, SinkError::Permanent),
        (
            302,
            SinkFailureReason::InvalidResponse,
            SinkError::Permanent,
        ),
    ] {
        *fixture.response.lock().unwrap() = ResponseSpec::status(status);
        let failure = sink.deliver_detailed(envelope()).await.unwrap_err();
        assert_eq!(failure.reason, reason);
        assert_eq!(failure.error, class);
    }
    for status in [429, 503] {
        for (header, expected) in [
            ("1", Some(Duration::from_millis(100))),
            ("18446744073709551615", Some(Duration::from_millis(100))),
            ("-1", None),
            ("bogus", None),
            ("0", Some(Duration::ZERO)),
        ] {
            let mut spec = ResponseSpec::status(status);
            spec.retry_after = Some(header.into());
            *fixture.response.lock().unwrap() = spec;
            assert_eq!(
                sink.deliver_detailed(envelope())
                    .await
                    .unwrap_err()
                    .retry_after,
                expected
            );
        }
    }
    for chunked in [false, true] {
        let mut spec = ResponseSpec::status(200);
        spec.body_bytes = 4097;
        spec.chunked = chunked;
        *fixture.response.lock().unwrap() = spec;
        assert_eq!(
            sink.deliver_detailed(envelope()).await.unwrap_err().reason,
            SinkFailureReason::ResponseTooLarge
        );
    }
    let mut spec = ResponseSpec::status(200);
    spec.invalid = true;
    *fixture.response.lock().unwrap() = spec;
    assert_eq!(
        sink.deliver_detailed(envelope()).await.unwrap_err().reason,
        SinkFailureReason::InvalidResponse
    );
    let mut spec = ResponseSpec::status(200);
    spec.delay = Duration::from_millis(100);
    *fixture.response.lock().unwrap() = spec;
    assert_eq!(
        sink.deliver_detailed(envelope()).await.unwrap_err().reason,
        SinkFailureReason::Timeout
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    *fixture.response.lock().unwrap() = ResponseSpec::status(200);
    sink.deliver_detailed(envelope()).await.unwrap();
    let url = fixture.url.clone();
    fixture.shutdown().await;
    // Winsock can take longer than the deliberately short body-timeout test
    // to report a refused loopback connection. Use a separate bounded deadline
    // so Network does not race that Timeout assertion.
    let sink = HttpSink::new(
        &url,
        &Limits {
            sink_timeout_ms: 2_000,
            ..limits
        },
    )
    .unwrap();
    assert_eq!(
        sink.deliver_detailed(envelope()).await.unwrap_err().reason,
        SinkFailureReason::Network
    );
}

#[tokio::test]
async fn required_http_outage_retains_spool_and_recovers_without_accounting_leak() {
    let fixture = HttpFixture::new().await;
    *fixture.response.lock().unwrap() = ResponseSpec::status(401);
    let limits = Arc::new(Limits {
        sink_delivery_concurrency: 2,
        retry_base_ms: 10,
        retry_max_ms: 100,
        sink_max_attempts: 2,
        sink_timeout_ms: 50,
        ..Limits::default()
    });
    let id = SinkId::new("http").unwrap();
    let bus = EventBus::new(
        limits.clone(),
        Arc::new(Metrics::default()),
        vec![SinkDefinition::bounded(
            id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(HttpSink::new(&fixture.url, &limits).unwrap()),
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![id.clone()],
        }],
        1,
    )
    .unwrap();
    let delivery = envelope();
    let event_id = delivery.event.event_id;
    for _ in 0..8 {
        let mut event = (*delivery.event).clone();
        event.event_id = EventId::generate();
        bus.publish(event).unwrap();
    }
    bus.publish((*delivery.event).clone()).unwrap();
    assert!(
        !bus.wait_required_drained(Duration::from_millis(250))
            .await
            .unwrap()
    );
    assert_eq!(bus.usage().unwrap().pending_required, 9);
    let before = bus.usage().unwrap();
    assert_eq!(bus.spool_records().unwrap().len(), 9);
    assert_eq!(bus.usage().unwrap(), before);
    assert!(bus.sink_diagnostics().unwrap()[0].paused);
    assert!(
        fixture.events.lock().unwrap().len() < 20,
        "outage caused an unbounded request burst"
    );
    *fixture.response.lock().unwrap() = ResponseSpec::status(200);
    assert!(
        bus.wait_required_drained(Duration::from_secs(3))
            .await
            .unwrap()
    );
    bus.close_admission().unwrap();
    bus.stop_workers().await.unwrap();
    assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
    let diagnostics = bus.sink_diagnostics().unwrap();
    assert_eq!(
        (
            diagnostics[0].queue_count,
            diagnostics[0].queue_bytes,
            diagnostics[0].inflight
        ),
        (0, 0, 0)
    );
    assert!(bus.spool_records().unwrap().is_empty());
    assert!(fixture.events.lock().unwrap().contains(&event_id));
    fixture.shutdown().await;
}
