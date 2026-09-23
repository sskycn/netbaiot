//! Bounded management HTTP only; device ingress never dispatches here.
use crate::common::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn response(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

fn error_code(error: &Error) -> ErrorCode {
    match error {
        Error::Authentication => ErrorCode::Unauthenticated,
        Error::Forbidden => ErrorCode::Forbidden,
        Error::Conflict => ErrorCode::Conflict,
        Error::Overloaded => ErrorCode::Overloaded,
        Error::Timeout => ErrorCode::Timeout,
        Error::Draining => ErrorCode::ServiceDraining,
        Error::Unavailable => ErrorCode::ServerUnavailable,
        Error::Storage | Error::IncompatibleSpool => ErrorCode::ServerUnavailable,
        Error::Internal => ErrorCode::Internal,
        Error::Configuration | Error::Invalid | Error::Codec => ErrorCode::InvalidRequest,
    }
}

fn api_error(
    status: StatusCode,
    code: ErrorCode,
    message: &str,
    request_id: &str,
) -> Response<Full<Bytes>> {
    let payload = ApiError {
        code,
        message: message.to_owned(),
        request_id: Some(request_id.to_owned()),
        required_scope: None,
    };
    let mut result = response(
        status,
        serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec()),
    );
    if let Ok(value) = hyper::header::HeaderValue::from_str(request_id) {
        result.headers_mut().insert("x-request-id", value);
    }
    result
}

fn error(error: Error, request_id: &str) -> Response<Full<Bytes>> {
    let status = match &error {
        Error::Authentication => StatusCode::UNAUTHORIZED,
        Error::Forbidden => StatusCode::FORBIDDEN,
        Error::Conflict => StatusCode::CONFLICT,
        Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        Error::Timeout => StatusCode::GATEWAY_TIMEOUT,
        Error::Draining | Error::Storage | Error::IncompatibleSpool | Error::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::BAD_REQUEST,
    };
    api_error(status, error_code(&error), &error.to_string(), request_id)
}

fn authorization(req: &Request<Incoming>) -> Result<String> {
    if req
        .headers()
        .get_all(hyper::header::AUTHORIZATION)
        .iter()
        .count()
        != 1
    {
        return Err(Error::Authentication);
    }
    req.headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::to_owned)
        .ok_or(Error::Authentication)
}

async fn body(req: Request<Incoming>, maximum: usize) -> Result<Bytes> {
    if req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(Error::Invalid);
    }
    Limited::new(req.into_body(), maximum)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|_| Error::Invalid)
}

async fn handle(
    req: Request<Incoming>,
    peer: SocketAddr,
    services: Arc<Services>,
) -> Result<Response<Full<Bytes>>> {
    let _slot = services
        .http_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Overloaded)?;
    services.ingress.metrics.inc(Metric::ManagementHttpRequests);
    services.rates.take(peer.ip())?;
    let limits = &services.ingress.limits;
    let header_bytes = req
        .headers()
        .iter()
        .try_fold(0usize, |total, (name, value)| {
            total
                .checked_add(name.as_str().len())?
                .checked_add(value.len() + 4)
        })
        .ok_or(Error::Invalid)?;
    if header_bytes > limits.max_http_header_bytes || req.headers().len() > limits.max_http_headers
    {
        return Err(Error::Invalid);
    }
    if req.headers().contains_key(hyper::header::CONTENT_ENCODING) {
        return Ok(response(StatusCode::UNSUPPORTED_MEDIA_TYPE, b"{}".to_vec()));
    }
    handle_management(req, services).await
}

async fn handle_management(
    req: Request<Incoming>,
    services: Arc<Services>,
) -> Result<Response<Full<Bytes>>> {
    let authorization = authorization(&req)?;
    services
        .admin
        .as_ref()
        .ok_or(Error::Forbidden)?
        .verify(authorization.as_bytes())?;
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    match (method, path.as_str()) {
        (hyper::Method::GET, "/api/v1/health") => {
            Ok(response(StatusCode::OK, b"{\"live\":true}".to_vec()))
        }
        (hyper::Method::GET, "/api/v1/ready") => {
            let ready = services.ingress.lifecycle.ready();
            Ok(response(
                if ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                format!("{{\"ready\":{ready}}}").into_bytes(),
            ))
        }
        (hyper::Method::GET, "/api/v1/status") => {
            let usage = services.ingress.events.usage()?;
            let (auth_entries, auth_bytes) = services.ingress.auth_cache.usage()?;
            let active = services.connections.active()?;
            let active_connections = ConnectionCounts {
                mqtt: active[0],
                tcp: active[1],
                udp: active[2],
            };
            Ok(response(
                StatusCode::OK,
                serde_json::to_vec(&serde_json::json!({
                    "lifecycle": services.ingress.lifecycle.state(),
                    "event_count": usage.events,
                    "event_bytes": usage.bytes,
                    "pending_required": usage.pending_required,
                    "auth_cache_entries": auth_entries,
                    "auth_cache_bytes": auth_bytes,
                    "runtime_tasks": tokio::runtime::Handle::current().metrics().num_alive_tasks(),
                    "active_connections": active_connections,
                }))
                .map_err(|_| Error::Internal)?,
            ))
        }
        (hyper::Method::GET, "/api/v1/metrics") => {
            let mut result = response(
                StatusCode::OK,
                services.ingress.metrics.render().into_bytes(),
            );
            result.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/plain; version=0.0.4"),
            );
            Ok(result)
        }
        (hyper::Method::GET, "/api/v1/connections") => {
            let query = req.uri().query().unwrap_or_default();
            let mut offset = 0usize;
            let mut limit = 100usize;
            for pair in query.split('&') {
                if let Some((key, value)) = pair.split_once('=') {
                    match key {
                        "offset" => offset = value.parse().map_err(|_| Error::Invalid)?,
                        "limit" => limit = value.parse().map_err(|_| Error::Invalid)?,
                        _ => return Err(Error::Invalid),
                    }
                }
            }
            Ok(response(
                StatusCode::OK,
                serde_json::to_vec(&services.ingress.sessions.list(offset, limit)?)
                    .map_err(|_| Error::Internal)?,
            ))
        }
        (hyper::Method::POST, "/api/v1/devices/commands") => {
            let maximum = services.ingress.limits.max_command_bytes;
            let command: DeviceCommand =
                serde_json::from_slice(&body(req, maximum).await?).map_err(|_| Error::Invalid)?;
            let result = match services.router.send(command) {
                Ok(result) => result,
                Err(Error::Unavailable) => {
                    let request_id = Uuid::new_v4().to_string();
                    return Ok(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        ErrorCode::DeviceOffline,
                        "device is not currently connected",
                        &request_id,
                    ));
                }
                Err(error) => return Err(error),
            };
            Ok(response(
                StatusCode::ACCEPTED,
                serde_json::to_vec(&result).map_err(|_| Error::Internal)?,
            ))
        }
        (hyper::Method::POST, "/api/v1/devices/connection") => {
            let device: DeviceKey = serde_json::from_slice(
                &body(req, services.ingress.limits.max_http_body_size).await?,
            )
            .map_err(|_| Error::Invalid)?;
            Ok(response(
                StatusCode::OK,
                serde_json::to_vec(&services.ingress.sessions.connection(&device)?)
                    .map_err(|_| Error::Internal)?,
            ))
        }
        (hyper::Method::POST, "/api/v1/auth/invalidate") => {
            let invalidation: AuthInvalidation = serde_json::from_slice(
                &body(req, services.ingress.limits.max_http_body_size).await?,
            )
            .map_err(|_| Error::Invalid)?;
            let mqtt = services.mqtt.clone();
            let (devices, disconnected, mqtt_invalidated) = services
                .ingress
                .invalidate_auth_with(&invalidation, || mqtt.invalidate_sessions(&invalidation))?;
            let invalidated = devices.len();
            Ok(response(
                StatusCode::OK,
                serde_json::to_vec(&InvalidationResult {
                    invalidated,
                    disconnected,
                    invalidated_cache_entries: invalidated,
                    disconnected_connections: disconnected,
                    invalidated_mqtt_sessions: mqtt_invalidated,
                })
                .map_err(|_| Error::Internal)?,
            ))
        }
        (hyper::Method::PUT, "/api/v1/control/snapshot") => {
            let snapshot: ControlSnapshot = serde_json::from_slice(
                &body(req, services.ingress.limits.max_http_body_size).await?,
            )
            .map_err(|_| Error::Invalid)?;
            let revision = snapshot.revision;
            let routes = snapshot.routes.clone();
            let _mutation = services.control_lock.lock().await;
            services
                .ingress
                .events
                .validate_route_update(revision, &routes)?;
            services.ingress.control.apply(snapshot)?;
            services.ingress.events.replace_routes(revision, routes)?;
            Ok(response(StatusCode::NO_CONTENT, Vec::new()))
        }
        (hyper::Method::PUT, "/api/v1/routes") => {
            let update: RoutesUpdate = serde_json::from_slice(
                &body(req, services.ingress.limits.max_http_body_size).await?,
            )
            .map_err(|_| Error::Invalid)?;
            let _mutation = services.control_lock.lock().await;
            services
                .ingress
                .events
                .validate_route_update(update.revision, &update.routes)?;
            services
                .ingress
                .control
                .replace_routes(update.revision, update.routes.clone())?;
            services
                .ingress
                .events
                .replace_routes(update.revision, update.routes)?;
            Ok(response(StatusCode::NO_CONTENT, Vec::new()))
        }
        (hyper::Method::POST, "/api/v1/drain") => {
            services.shutdown.cancel();
            Ok(response(
                StatusCode::ACCEPTED,
                b"{\"draining\":true}".to_vec(),
            ))
        }
        _ => Ok(response(StatusCode::NOT_FOUND, b"{}".to_vec())),
    }
}

pub async fn connection(
    stream: BoxStream,
    peer: SocketAddr,
    services: Arc<Services>,
    lease: ConnectionLease,
    stop: CancellationToken,
) -> Result<()> {
    let connect_remaining = lease
        .connect_deadline()
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or(Error::Timeout)?;
    let _lease = lease; // Retain the global/IP/byte permits through connection shutdown.
    let handler = services.clone();
    let service = service_fn(move |request| {
        let services = handler.clone();
        async move {
            let request_id = Uuid::new_v4().to_string();
            let result = deadline(
                services.ingress.limits.request_timeout_ms,
                handle(request, peer, services),
            )
            .await;
            Ok::<_, Infallible>(match result {
                Ok(response) => response,
                Err(error_value) => error(error_value, &request_id),
            })
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .max_headers(services.ingress.limits.max_http_headers)
        .max_buf_size(services.ingress.limits.max_http_header_bytes)
        .timer(TokioTimer::new())
        .header_read_timeout(connect_remaining);
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);
    tokio::select! {
        result = tokio::time::timeout(
            connect_remaining + Duration::from_millis(
                services.ingress.limits.request_timeout_ms
                    + services.ingress.limits.write_timeout_ms,
            ),
            connection.as_mut(),
        ) => result.map_err(|_| Error::Timeout)?.map_err(|_| Error::Unavailable),
        _ = stop.cancelled() => {
            connection.as_mut().graceful_shutdown();
            tokio::time::timeout(
                Duration::from_millis(services.ingress.limits.shutdown_timeout_ms),
                connection,
            )
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Unavailable)
        }
    }
}
