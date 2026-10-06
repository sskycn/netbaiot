//! Development-only HTTP receiver and supervised real MQTT end-to-end demo.
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use netbaiot_client::NetbaIoTClient;
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, PublishQos, PublishResult};
use netbaiot_protocol::{DeviceUplink, DeviceUplinkKind, Heartbeat, SourceMessageId};
use netbaiot_server::{BoundAddresses, Config};
#[cfg(test)]
use std::path::PathBuf;
use std::{convert::Infallible, fmt, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct DemoError {
    stage: &'static str,
    cause: String,
}
impl DemoError {
    fn at(stage: &'static str, cause: impl fmt::Display) -> Self {
        Self {
            stage,
            cause: cause.to_string(),
        }
    }
}
impl fmt::Display for DemoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Demo failed at {}: {}", self.stage, self.cause)
    }
}
// The one supervisor owns temp storage, server and sink until graceful cleanup.
// Dropping the caller cancels it without detaching unowned network workers.
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct OwnedSink(tokio::task::JoinHandle<()>);
impl Drop for OwnedSink {
    fn drop(&mut self) {
        self.0.abort();
    }
}
const DEMO_QUEUE_BYTES: usize = 1_048_576;
struct DemoEvent {
    value: serde_json::Value,
    _reservation: OwnedSemaphorePermit,
}
#[derive(Clone)]
struct DemoQueue {
    sender: mpsc::Sender<DemoEvent>,
    bytes: Arc<Semaphore>,
}
#[derive(Default)]
struct SessionOptions {
    #[cfg(test)]
    temp_parent: Option<PathBuf>,
    #[cfg(test)]
    sink_address: Option<SocketAddr>,
    #[cfg(test)]
    invalid_start: bool,
    #[cfg(test)]
    invalid_credentials: bool,
    #[cfg(test)]
    prepared: Option<oneshot::Sender<(PathBuf, BoundAddresses)>>,
}

pub async fn run(once: bool) -> Result<(), DemoError> {
    run_with_options(once, SessionOptions::default()).await
}
async fn run_with_options(once: bool, options: SessionOptions) -> Result<(), DemoError> {
    let stop = CancellationToken::new();
    let _owner = CancelOnDrop(stop.clone());
    tokio::spawn(session(once, stop, options))
        .await
        .map_err(|e| DemoError::at("supervisor", e))?
}
async fn session(
    once: bool,
    stop: CancellationToken,
    options: SessionOptions,
) -> Result<(), DemoError> {
    #[cfg(not(test))]
    let _ = options;
    #[cfg(test)]
    let parent = options.temp_parent.clone();
    let temporary = tokio::task::spawn_blocking(move || {
        let mut builder = tempfile::Builder::new();
        builder.prefix("netbaiot-demo-");
        #[cfg(test)]
        if let Some(parent) = parent {
            return builder.tempdir_in(parent);
        }
        builder.tempdir()
    })
    .await
    .map_err(|e| DemoError::at("temporary storage", e))?
    .map_err(|e| DemoError::at("temporary storage", e.kind()))?;
    let sink_address: SocketAddr = "127.0.0.1:0"
        .parse()
        .map_err(|e| DemoError::at("sink address", e))?;
    #[cfg(test)]
    let sink_address = options.sink_address.unwrap_or(sink_address);
    let sink_listener = TcpListener::bind(sink_address)
        .await
        .map_err(|e| DemoError::at("demo sink bind", e.kind()))?;
    let sink_address = sink_listener
        .local_addr()
        .map_err(|e| DemoError::at("demo sink address", e.kind()))?;
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json")).map_err(|_| {
            DemoError::at("development config", "embedded configuration is invalid")
        })?;
    config.device_ingress.set_port(0);
    config.management_http.set_port(0);
    config.delivery_url = Some(format!("http://{sink_address}/events"));
    config.spool_directory = temporary.path().join("spool");
    config.limits.shutdown_drain_timeout_ms = 500;
    #[cfg(test)]
    if options.invalid_start {
        config.limits.max_connections = 0;
    }
    let credential = config
        .credentials
        .first()
        .cloned()
        .ok_or_else(|| DemoError::at("development config", "missing development credential"))?;
    let (sender, mut receiver) = mpsc::channel(16);
    let sender = DemoQueue {
        sender,
        bytes: Arc::new(Semaphore::new(DEMO_QUEUE_BYTES)),
    };
    let sink_stop = CancellationToken::new();
    let sink_cancel = sink_stop.clone();
    let mut sink = OwnedSink(tokio::spawn(async move {
        serve_sink(sink_listener, sender, sink_cancel).await;
    }));
    let (ready_tx, ready_rx) = oneshot::channel();
    let admin = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let server = netbaiot_server::run_with_credentials_ready(
        config,
        stop.clone(),
        Some(admin.clone()),
        None,
        Some(ready_tx),
    );
    tokio::pin!(server);
    let addresses = tokio::select! {
        result = &mut server => return Err(DemoError::at("gateway startup", result.err().unwrap_or(netbaiot_runtime::Error::Unavailable))),
        ready = tokio::time::timeout(Duration::from_secs(10), ready_rx) => match ready {
            Ok(Ok(addresses)) => addresses,
            _ => { stop.cancel(); let _ = server.await; return Err(DemoError::at("gateway startup", "readiness deadline exceeded")); }
        },
        _ = stop.cancelled() => { server.await.map_err(|e| DemoError::at("cancelled startup", e))?; return Ok(()); }
    };
    #[cfg(test)]
    if let Some(prepared) = options.prepared {
        let _ = prepared.send((temporary.path().to_path_buf(), addresses));
    }
    println!("Development demo only. Do not reuse these credentials in production.");
    println!(
        "Gateway started\nMQTT/TCP/UDP: {}\nManagement: {}",
        addresses.device_ingress, addresses.management_http
    );
    #[cfg(test)]
    let bad_credentials = options.invalid_credentials;
    #[cfg(not(test))]
    let bad_credentials = false;
    let outcome = {
        let sample = sample(addresses, admin, credential, &mut receiver, bad_credentials);
        tokio::pin!(sample);
        tokio::select! {
            result = &mut sample => result,
            result = &mut server => return Err(DemoError::at("gateway runtime", result.err().unwrap_or(netbaiot_runtime::Error::Unavailable))),
            _ = stop.cancelled() => Ok(()),
            _ = &mut sink.0 => Err(DemoError::at("demo sink", "receiver stopped unexpectedly")),
        }
    };
    if outcome.is_ok() && !once && !stop.is_cancelled() {
        println!("NetbaIoT demo ready. Press Ctrl-C to stop.");
        loop {
            tokio::select! {
                result = netbaiot_server::shutdown_signal() => {
                    if let Err(e) = result { stop.cancel(); let _ = server.await; return Err(DemoError::at("shutdown signal", e)); }
                    break;
                },
                _ = stop.cancelled() => break,
                event = receiver.recv() => {
                    if let Some(envelope) = event { let event = envelope.value; println!("Demo sink received event_id={} type={} device={}/{}/{}", event["event_id"], event["event_type"], event["tenant_id"], event["product_id"], event["device_id"]); }
                    else { stop.cancel(); let _ = server.await; return Err(DemoError::at("demo sink", "receiver channel closed")); }
                },
                result = &mut server => return result.map_err(|e| DemoError::at("gateway runtime", e)),
                _ = &mut sink.0 => { stop.cancel(); let _ = server.await; return Err(DemoError::at("demo sink", "receiver stopped unexpectedly")); }
            }
        }
    }

    stop.cancel();
    let shutdown = server
        .await
        .map_err(|e| DemoError::at("graceful shutdown", e));
    sink_stop.cancel();
    let _ = (&mut sink.0).await;
    shutdown?;
    outcome?;
    // Remove only this private temporary tree after both network owners have stopped.
    tokio::task::spawn_blocking(move || temporary.close())
        .await
        .map_err(|e| DemoError::at("temporary cleanup", e))?
        .map_err(|e| DemoError::at("temporary cleanup", e.kind()))?;
    println!("Shutdown completed\nDemo completed successfully.");
    Ok(())
}
async fn sample(
    addresses: BoundAddresses,
    admin: String,
    credential: netbaiot_runtime::Credential,
    events: &mut mpsc::Receiver<DemoEvent>,
    bad_credentials: bool,
) -> Result<(), DemoError> {
    let secret = if bad_credentials {
        "incorrect-demo-credential".to_owned()
    } else {
        credential.secret_hex.clone()
    };
    let device = DeviceClient::builder()
        .device(credential.identity.device_key.clone())
        .credentials(
            DeviceCredentials::new(&credential.credential_id, secret)
                .map_err(|e| DemoError::at("device credentials", e))?,
        )
        .mqtt_endpoint(format!("mqtt://{}", addresses.device_ingress))
        .client_id("netbaiot-built-in-demo")
        .mqtt_connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .map_err(|e| DemoError::at("device authentication", e))?;
    println!("Demo device authenticated");
    let source =
        SourceMessageId::new("netbaiot-demo:1").map_err(|e| DemoError::at("sample identity", e))?;
    let mut receipts = device.publish_receipts();
    device
        .publish(
            DeviceUplink::new(
                source.clone(),
                DeviceUplinkKind::Heartbeat(Heartbeat { sequence: 1 }),
            ),
            PublishQos::AtLeastOnce,
        )
        .await
        .map_err(|e| DemoError::at("sample publish", e))?;
    let receipt = tokio::time::timeout(Duration::from_secs(5), receipts.recv())
        .await
        .map_err(|_| DemoError::at("EventAccepted", "PUBACK deadline exceeded"))?
        .map_err(|e| DemoError::at("EventAccepted", e))?;
    if receipt.source_message_id != source || receipt.result != PublishResult::Puback {
        return Err(DemoError::at(
            "EventAccepted",
            "sample publish was not acknowledged with PUBACK",
        ));
    }
    println!("Heartbeat EventAccepted (MQTT QoS1 PUBACK)");
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .map_err(|_| DemoError::at("business sink", "delivery deadline exceeded"))?
        .ok_or_else(|| DemoError::at("business sink", "receiver closed"))?;
    let event = event.value;
    if event["source_message_id"] != source.as_str()
        || event["event_type"] != "heartbeat"
        || event["payload"]["data"]["sequence"] != 1
        || event["event_id"].as_str().is_none()
    {
        return Err(DemoError::at(
            "business sink",
            "unexpected normalized event",
        ));
    }
    let client = NetbaIoTClient::builder()
        .endpoint(format!("http://{}", addresses.management_http))
        .token(admin)
        .request_timeout(Duration::from_secs(1))
        .connect()
        .await
        .map_err(|e| DemoError::at("sink ACK status", e))?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            tick.tick().await;
            let status = client
                .runtime()
                .status()
                .await
                .map_err(|e| DemoError::at("sink ACK status", e))?;
            if status.pending_required == 0 {
                return Ok::<(), DemoError>(());
            }
        }
    })
    .await
    .map_err(|_| {
        DemoError::at(
            "business sink acknowledgement",
            "required delivery ACK deadline exceeded",
        )
    })??;
    device
        .shutdown_with_timeout(Duration::from_secs(3))
        .await
        .map_err(|e| DemoError::at("sample device shutdown", e))?;
    println!(
        "Business sink acknowledged\nEvent: heartbeat sequence=1 event_id={}",
        event["event_id"]
    );
    println!(
        "Try your own standard MQTT client (development credentials only):\nmosquitto_pub -h {} -p {} -V mqttv311 -u {} -P {} -i manual-demo -t v1/t/{}/p/{}/d/{}/up -q 1 -m '{{\"schema_version\":1,\"source_message_id\":\"manual-demo:1\",\"kind\":\"heartbeat\",\"data\":{{\"sequence\":2}}}}'",
        addresses.device_ingress.ip(),
        addresses.device_ingress.port(),
        credential.credential_id,
        credential.secret_hex,
        credential.identity.device_key.tenant_id,
        credential.identity.device_key.product_id,
        credential.identity.device_key.device_id
    );
    Ok(())
}
async fn serve_sink(listener: TcpListener, events: DemoQueue, stop: CancellationToken) {
    loop {
        let socket = tokio::select! { biased; _ = stop.cancelled() => break, result = listener.accept() => match result { Ok((socket, _)) => socket, Err(_) => break } };
        let events = events.clone();
        let connection = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .max_headers(16)
            .max_buf_size(16 * 1024)
            .timer(TokioTimer::new())
            .header_read_timeout(Duration::from_secs(2))
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |r| receive(r, events.clone())),
            );
        tokio::select! { biased; _ = stop.cancelled() => break, _ = tokio::time::timeout(Duration::from_secs(2), connection) => () }
    }
}
async fn receive(
    mut request: Request<Incoming>,
    events: DemoQueue,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut status = StatusCode::BAD_REQUEST;
    if request.method() == hyper::Method::POST && request.uri().path() == "/events" {
        let mut bytes = Vec::new();
        while let Some(frame) = request.body_mut().frame().await {
            let Ok(frame) = frame else {
                return Ok(response(StatusCode::BAD_REQUEST));
            };
            if let Ok(data) = frame.into_data() {
                if bytes.len().saturating_add(data.len()) > 65_536 {
                    return Ok(response(StatusCode::PAYLOAD_TOO_LARGE));
                }
                bytes.extend_from_slice(&data);
            }
        }
        if let Ok(event) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            let reservation = match events
                .bytes
                .clone()
                .try_acquire_many_owned(bytes.len() as u32)
            {
                Ok(reservation) => reservation,
                Err(_) => return Ok(response(StatusCode::SERVICE_UNAVAILABLE)),
            };
            status = if events
                .sender
                .try_send(DemoEvent {
                    value: event,
                    _reservation: reservation,
                })
                .is_ok()
            {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
        }
    }
    Ok(response(status))
}
fn response(status: StatusCode) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "current_thread")]
    async fn completed_demo_and_dropped_caller_leave_no_owned_tasks() {
        let parent = tempfile::tempdir().unwrap();
        let baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        run_with_options(
            true,
            SessionOptions {
                temp_parent: Some(parent.path().into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (prepared, ready) = oneshot::channel();
        let caller = tokio::spawn(run_with_options(
            false,
            SessionOptions {
                temp_parent: Some(parent.path().into()),
                prepared: Some(prepared),
                ..Default::default()
            },
        ));
        let (_, addresses) = ready.await.unwrap();
        caller.abort();
        let _ = caller.await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if std::fs::read_dir(parent.path()).unwrap().count() == 0
                    && tokio::runtime::Handle::current()
                        .metrics()
                        .num_alive_tasks()
                        == baseline
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(TcpListener::bind(addresses.device_ingress).await.is_ok());
        assert!(TcpListener::bind(addresses.management_http).await.is_ok());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn demo_failure_and_cancellation_release_resources() {
        for mode in 0..4 {
            let parent = tempfile::tempdir().unwrap();
            let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (tx, rx) = oneshot::channel();
            let stop = CancellationToken::new();
            let options = SessionOptions {
                temp_parent: Some(parent.path().into()),
                sink_address: (mode == 0).then(|| occupied.local_addr().unwrap()),
                invalid_start: mode == 1,
                invalid_credentials: mode == 2,
                prepared: (mode == 3).then_some(tx),
            };
            let token = stop.clone();
            let handle = tokio::spawn(session(false, token, options));
            let mut addresses = None;
            if mode == 3 {
                let (_, bound) = rx.await.unwrap();
                addresses = Some(bound);
                stop.cancel();
            }
            let result = tokio::time::timeout(Duration::from_secs(15), handle)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.is_ok(), mode == 3, "mode {mode}: {result:?}");
            assert_eq!(
                std::fs::read_dir(parent.path()).unwrap().count(),
                0,
                "temporary resources leaked in mode {mode}"
            );
            if let Some(bound) = addresses {
                assert!(TcpListener::bind(bound.device_ingress).await.is_ok());
                assert!(TcpListener::bind(bound.management_http).await.is_ok());
            }
        }
    }
}
