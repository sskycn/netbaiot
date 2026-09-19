use crate::common::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
type Sent = Arc<Mutex<Option<(DeviceKey, CommandId, u32)>>>;
fn response(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}
fn error(error: Error) -> Response<Full<Bytes>> {
    let code = match error {
        Error::Authentication => StatusCode::UNAUTHORIZED,
        Error::Forbidden => StatusCode::FORBIDDEN,
        Error::Conflict => StatusCode::CONFLICT,
        Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        Error::Timeout => StatusCode::GATEWAY_TIMEOUT,
        Error::Draining | Error::Storage | Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_REQUEST,
    };
    response(code, format!("{{\"error\":\"{error}\"}}").into_bytes())
}
async fn handle(
    req: Request<Incoming>,
    peer: SocketAddr,
    s: Arc<Services>,
    lease: Arc<Mutex<ConnectionLease>>,
    sent: Sent,
) -> Result<Response<Full<Bytes>>> {
    let _slot = s
        .http_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Overloaded)?;
    s.ingress.metrics.inc(Metric::HttpRequests);
    s.rates.take(peer.ip())?;
    let l = &s.ingress.limits;
    let size = req
        .headers()
        .iter()
        .try_fold(0usize, |n, (k, v)| {
            n.checked_add(k.as_str().len())?.checked_add(v.len() + 4)
        })
        .ok_or(Error::Invalid)?;
    if size > l.max_http_header_bytes || req.headers().len() > l.max_http_headers {
        return Err(Error::Invalid);
    }
    // This profile has no decompressor. Do not interpret encoded bytes as JSON.
    if req.headers().contains_key(hyper::header::CONTENT_ENCODING) {
        return Ok(response(StatusCode::UNSUPPORTED_MEDIA_TYPE, b"{}".to_vec()));
    }
    if req
        .headers()
        .get_all(hyper::header::AUTHORIZATION)
        .iter()
        .count()
        != 1
    {
        return Err(Error::Authentication);
    }
    let authorization = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or(Error::Authentication)?
        .to_owned();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    if path == "/v1/admin/commands" && method == hyper::Method::POST {
        let admin = s.admin.as_ref().ok_or(Error::Forbidden)?;
        admin.verify(authorization.as_bytes())?;
        let body = Limited::new(req.into_body(), l.max_command_bytes)
            .collect()
            .await
            .map_err(|_| Error::Invalid)?
            .to_bytes();
        let command: DeviceCommand = serde_json::from_slice(&body).map_err(|_| Error::Invalid)?;
        let auth = admin.identity(&command.device).ok_or(Error::Forbidden)?;
        let record = s.router.queue(auth, command).await?;
        return Ok(response(
            StatusCode::ACCEPTED,
            serde_json::to_vec(&record).map_err(|_| Error::Internal)?,
        ));
    }
    let (id, secret) = authorization.split_once(':').ok_or(Error::Authentication)?;
    if id.len() > l.max_username_bytes || secret.len() > l.max_password_bytes {
        return Err(Error::Authentication);
    }
    let auth = s
        .ingress
        .authenticate(AuthenticationRequest::Secret {
            credential_id: id,
            secret: secret.as_bytes(),
        })
        .await?;
    lock(&lease)?.authenticate(&auth.device_key)?;
    // Include slow bodies and command pulls in hierarchical request admission.
    let _request = s.protocol_admission.acquire(&auth.device_key, 0)?;
    if method == hyper::Method::GET && path == "/metrics" {
        let mut metrics = s.ingress.metrics.render();
        let counts = s.connections.active()?;
        for (name, count) in ["http", "mqtt", "tcp", "udp"].into_iter().zip(counts) {
            metrics.push_str(&format!(
                "netbaiot_active_connections{{transport=\"{name}\"}} {count}\n"
            ));
        }
        metrics.push_str(&format!(
            "netbaiot_queue_bytes {}\nnetbaiot_queue_depth {}\n",
            s.ingress.sessions.queued_bytes(),
            s.ingress.sessions.queued_messages()
        ));
        let (ingress_count, ingress_bytes) = s.ingress.admission.in_flight();
        let (ingress_waiters, ingress_wait_bytes) = s.ingress.admission.waiting();
        let (protocol_count, protocol_bytes) = s.protocol_admission.in_flight();
        let store = s.ingress.store.health();
        let degraded_workers = s
            .ingress
            .metrics
            .get(Metric::DependencyDegraded)
            .saturating_sub(s.ingress.metrics.get(Metric::DependencyRecovered));
        metrics.push_str(&format!(
            "netbaiot_ingress_inflight {ingress_count}\nnetbaiot_ingress_inflight_bytes {ingress_bytes}\nnetbaiot_ingress_waiters {ingress_waiters}\nnetbaiot_ingress_wait_bytes {ingress_wait_bytes}\nnetbaiot_protocol_inflight {protocol_count}\nnetbaiot_protocol_inflight_bytes {protocol_bytes}\nnetbaiot_database_pool_active {}\nnetbaiot_database_pool_idle {}\nnetbaiot_database_pool_waiters {}\nnetbaiot_dependency_degraded_workers {degraded_workers}\nnetbaiot_runtime_alive_tasks {}\n",
            store.pool_active,
            store.pool_idle,
            store.pool_waiters,
            tokio::runtime::Handle::current().metrics().num_alive_tasks()
        ));
        let (sessions, tenants, presence) = s.ingress.sessions.registry_counts()?;
        metrics.push_str(&format!(
            "netbaiot_registered_sessions {sessions}\nnetbaiot_session_tenant_entries {tenants}\nnetbaiot_presence_entries {presence}\nnetbaiot_subscription_entries {}\n",
            s.subscriptions.count()?
        ));
        let mut r = response(StatusCode::OK, metrics.into_bytes());
        r.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("text/plain; version=0.0.4"),
        );
        return Ok(r);
    }
    if method == hyper::Method::GET && path == "/v1/device/commands" {
        let command = s.router.pull(&auth).await?;
        s.ingress
            .sessions
            .touch(&auth.device_key, Transport::Http)?;
        return if let Some(record) = command {
            let command = record.command;
            *lock(&sent)? = Some((auth.device_key.clone(), command.command_id, record.attempts));
            Ok(response(
                StatusCode::OK,
                s.ingress
                    .codecs
                    .get(&auth)?
                    .encode(
                        &EncodeContext {
                            device: &auth.device_key,
                        },
                        &command,
                    )
                    .map_err(|_| Error::Codec)?,
            ))
        } else {
            Ok(response(StatusCode::NO_CONTENT, Vec::new()))
        };
    }
    if method != hyper::Method::POST
        || !matches!(
            path.as_str(),
            "/v1/device/messages" | "/v1/device/commands/ack"
        )
    {
        return Ok(response(StatusCode::NOT_FOUND, b"{}".to_vec()));
    }
    if req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > l.max_http_body_size as u64)
    {
        return Ok(response(StatusCode::PAYLOAD_TOO_LARGE, b"{}".to_vec()));
    }
    let body = match Limited::new(req.into_body(), l.max_http_body_size)
        .collect()
        .await
    {
        Ok(b) => b.to_bytes(),
        Err(_) => return Ok(response(StatusCode::PAYLOAD_TOO_LARGE, b"{}".to_vec())),
    };
    let acceptance = s
        .ingress
        .ingest(
            &auth,
            IngressEnvelope {
                transport: Transport::Http,
                payload: &body,
                require_command_ack: path.ends_with("/ack"),
                validated_at: std::time::Instant::now(),
                validation_us: 0,
            },
        )
        .await?;
    Ok(response(
        StatusCode::ACCEPTED,
        serde_json::to_vec(&acceptance.receipt).map_err(|_| Error::Internal)?,
    ))
}
pub async fn connection(
    stream: BoxStream,
    peer: SocketAddr,
    s: Arc<Services>,
    lease: ConnectionLease,
    stop: CancellationToken,
) -> Result<()> {
    let lease = Arc::new(Mutex::new(lease));
    let sent: Sent = Arc::new(Mutex::new(None));
    let handler_s = s.clone();
    let handler_sent = sent.clone();
    let service = service_fn(move |req| {
        let s = handler_s.clone();
        let lease = lease.clone();
        let sent = handler_sent.clone();
        async move {
            let result = deadline(
                s.ingress.limits.request_timeout_ms,
                handle(req, peer, s, lease, sent),
            )
            .await;
            Ok::<_, Infallible>(match result {
                Ok(r) => r,
                Err(e) => error(e),
            })
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .max_headers(s.ingress.limits.max_http_headers)
        .max_buf_size(s.ingress.limits.max_http_header_bytes)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_millis(s.ingress.limits.connect_timeout_ms));
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);
    // Bound slow response consumers as well as request/body processing.
    let result = tokio::select! {result=tokio::time::timeout(Duration::from_millis(s.ingress.limits.connect_timeout_ms+s.ingress.limits.request_timeout_ms+s.ingress.limits.write_timeout_ms),connection.as_mut())=>result.map_err(|_|Error::Timeout)?.map_err(|_|Error::Unavailable),_=stop.cancelled()=>{connection.as_mut().graceful_shutdown();tokio::time::timeout(Duration::from_millis(s.ingress.limits.shutdown_timeout_ms),connection).await.map_err(|_|Error::Timeout)?.map_err(|_|Error::Unavailable)}};
    result?;
    let delivered = lock(&sent)?.take();
    if let Some((device, id, attempt)) = delivered {
        s.router
            .state(&device, id, attempt, DeliveryState::Sent)
            .await?;
    }
    Ok(())
}
