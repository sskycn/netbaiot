use super::*;
use crate::config::parse_hex_32;

pub async fn run(config: Config, stop: CancellationToken) -> Result<()> {
    run_with_credentials(
        config,
        stop,
        std::env::var("NETBAIOT_ADMIN_SECRET").ok(),
        std::env::var("NETBAIOT_BUSINESS_STREAM_TOKEN").ok(),
    )
    .await
}

/// Composition entry point for embedded/test hosts that inject secrets without
/// mutating process-global environment state.
// Tokio JoinHandle detaches on Drop; run owners must abort instead.
pub(crate) struct OwnedTask<T>(tokio::task::JoinHandle<T>);
impl<T> std::future::Future for OwnedTask<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}
impl<T> Drop for OwnedTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct RunCancellation(Vec<CancellationToken>);
impl Drop for RunCancellation {
    fn drop(&mut self) {
        for token in &self.0 {
            token.cancel();
        }
    }
}

pub async fn run_with_credentials(
    config: Config,
    stop: CancellationToken,
    admin_secret: Option<String>,
    business_stream_token: Option<String>,
) -> Result<()> {
    run_with_credentials_ready(config, stop, admin_secret, business_stream_token, None).await
}

/// Like run_with_credentials, with one bounded startup notification for embedded hosts.
/// No notification is sent until preparation and worker ownership are complete.
pub async fn run_with_credentials_ready(
    config: Config,
    stop: CancellationToken,
    admin_secret: Option<String>,
    business_stream_token: Option<String>,
    ready: Option<tokio::sync::oneshot::Sender<BoundAddresses>>,
) -> Result<()> {
    config.validate()?;
    let recovery_path = config.spool_directory.clone();
    let recovery_owner = Arc::new(
        tokio::task::spawn_blocking(move || {
            netbaiot_runtime::recovery_io::RecoveryDirectory::acquire(&recovery_path)
        })
        .await
        .map_err(|_| Error::Internal)??,
    );
    let limits = Arc::new(config.limits.clone());
    let metrics = Arc::new(
        if matches!(
            std::env::var("NETBAIOT_PERF_LOCK_METRICS").as_deref(),
            Ok("1")
        ) {
            Metrics::with_lock_timing()
        } else {
            Metrics::default()
        },
    );
    let lifecycle = Arc::new(Lifecycle::starting());
    let identities: HashMap<_, _> = config
        .credentials
        .iter()
        .map(|credential| {
            (
                credential.identity.device_key.clone(),
                credential.identity.clone(),
            )
        })
        .collect();
    let rpc_registry = if let Some(rpc) = &config.business_rpc {
        Some(BusinessRpcRegistry::new_with_metrics(
            rpc.auth_max_inflight,
            rpc.auth_max_inflight
                .checked_mul(32 * 1024)
                .ok_or(Error::Configuration)?,
            Duration::from_millis(limits.authentication_timeout_ms),
            metrics.clone(),
        )?)
    } else {
        None
    };
    let auth_source = config
        .device_auth
        .unwrap_or(if config.auth_provider_url.is_some() {
            DeviceAuthSource::Http
        } else {
            DeviceAuthSource::Static
        });
    let provider: Arc<dyn DeviceAuthenticator> = match auth_source {
        DeviceAuthSource::Static => StaticAuthenticator::new(config.credentials.clone(), &limits)?,
        DeviceAuthSource::Http => HttpAuthProvider::new(
            config
                .auth_provider_url
                .as_deref()
                .ok_or(Error::Configuration)?,
            &limits,
        )?,
        DeviceAuthSource::BusinessRpc => {
            BusinessRpcAuthProvider::new(rpc_registry.as_ref().ok_or(Error::Configuration)?.clone())
        }
    };
    let auth_cache = AuthCache::new(provider, limits.clone(), metrics.clone());
    let registry = crate::bootstrap::codec_registry(&limits)?;
    for auth in identities.values() {
        registry.get(auth)?;
    }

    let delivery_source = config
        .event_delivery
        .unwrap_or(if config.business_tcp.is_some() {
            EventDeliverySource::BusinessRpc
        } else if config.delivery_url.is_some() {
            EventDeliverySource::Http
        } else {
            EventDeliverySource::DevelopmentAudit
        });
    let business_sink = config.business_tcp.map(|_| BusinessRpcEventSink::new());
    let (sink_id, mut sink_definition) = match delivery_source {
        EventDeliverySource::BusinessRpc => {
            let id = SinkId::new("tcp-rpc").map_err(|_| Error::Configuration)?;
            let sink = business_sink.as_ref().ok_or(Error::Configuration)?.clone();
            let mut definition = SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink,
                &limits,
            );
            definition.concurrency = 1;
            (id, definition)
        }
        EventDeliverySource::Http => {
            let id = SinkId::new("webhook").map_err(|_| Error::Configuration)?;
            let sink = Arc::new(HttpSink::new(
                config.delivery_url.as_deref().ok_or(Error::Configuration)?,
                &limits,
            )?);
            (
                id.clone(),
                SinkDefinition::bounded(id, SinkDeliveryMode::ConfirmedRequired, sink, &limits),
            )
        }
        EventDeliverySource::DevelopmentAudit => {
            let id = SinkId::new("development-audit").map_err(|_| Error::Configuration)?;
            (
                id.clone(),
                SinkDefinition::bounded(
                    id,
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(AuditSink),
                    &limits,
                ),
            )
        }
    };
    if delivery_source == EventDeliverySource::BusinessRpc {
        sink_definition.timeout = Duration::from_millis(limits.sink_timeout_ms);
    }
    let snapshot = bootstrap_snapshot(&config, sink_id)?;
    let control = GatewayControl::empty(limits.clone());
    control.apply(snapshot.clone())?;
    let events = EventBus::new_paused(
        limits.clone(),
        metrics.clone(),
        vec![sink_definition],
        snapshot.routes,
        snapshot.revision,
    )?;
    let spool = RestartSpool::with_owner(
        config.spool_directory.clone(),
        limits.clone(),
        recovery_owner.clone(),
    );
    let mqtt_broker = MqttBroker::new_with_metrics(limits.clone(), metrics.clone());
    mqtt_broker.bind_recovery_owner(recovery_owner)?;
    mqtt_broker.recover_from(&config.spool_directory).await?;
    let recovery = spool.recover().await.inspect_err(|error| {
        // Display only our typed diagnostic, never the serialized record or serde error.
        tracing::error!(%error, "EventBus restart recovery failed; startup blocked");
    })?;
    let recovered_files = recovery.committed_files;
    let recovered_count = events.restore(recovery.records)?;
    metrics.add(Metric::RecoveryRecords, recovered_count as u64);
    let sessions = Sessions::new(limits.clone());
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        auth_cache,
        registry,
        events.clone(),
        control,
        metrics.clone(),
        sessions,
        lifecycle.clone(),
    ));
    let shutdown = stop.child_token();
    let mut base_services =
        Services::new_with_mqtt(ingress.clone(), shutdown.clone(), mqtt_broker.clone());
    if auth_source == DeviceAuthSource::BusinessRpc {
        Arc::get_mut(&mut base_services)
            .ok_or(Error::Internal)?
            .business_auth = rpc_registry.clone();
    }
    if let Some(secret) = admin_secret {
        let admin = Arc::new(AdminAccess::new(&secret, identities, &limits)?);
        Arc::get_mut(&mut base_services)
            .ok_or(Error::Internal)?
            .admin = Some(admin);
    }
    let management_auth = ManagementAuthService::new(
        config.management_auth.clone(),
        base_services.admin.clone(),
        limits.clone(),
    )?
    .with_metrics(metrics.clone());
    if !config.management_http.ip().is_loopback()
        && !(if config
            .management_tls
            .as_ref()
            .is_some_and(|tls| tls.require_client_certificate)
        {
            management_auth.has_mtls_provider()
        } else {
            management_auth.has_provider()
        })
    {
        return Err(Error::Configuration);
    }
    if management_auth.has_legacy_provider() && management_auth.has_usable_scoped_provider() {
        tracing::warn!(
            "legacy bootstrap management token remains enabled together with scoped management authentication providers"
        );
    }
    Arc::get_mut(&mut base_services)
        .ok_or(Error::Internal)?
        .management_auth = Some(Arc::new(management_auth));
    let tls = if let Some(files) = &config.tls {
        Some(tls_acceptor(files).await?)
    } else {
        None
    };
    let management_tls = if let Some(files) = &config.management_tls {
        Some(management_tls_acceptor(files).await?)
    } else if let Some(files) = &config.tls {
        // Legacy certificate configuration is loaded separately for the management listener.
        Some(tls_acceptor(files).await?)
    } else {
        None
    };

    let (device_ingress, udp) = bind_device_pair(config.device_ingress).await?;
    let device_address = device_ingress
        .local_addr()
        .map_err(|_| Error::Unavailable)?;
    let management_http = TcpListener::bind(config.management_http).await.map_err(|error| {
        tracing::error!(listener="management_tcp",address=%config.management_http,error_kind=?error.kind(),"listener bind failed"); Error::Unavailable
    })?;
    let management_address = management_http
        .local_addr()
        .map_err(|_| Error::Unavailable)?;
    let business = if let Some(address) = config.business_tcp {
        Some((
            TcpListener::bind(address)
                .await
                .map_err(|error| { tracing::error!(listener="business_tcp",%address,error_kind=?error.kind(),"listener bind failed"); Error::Unavailable })?,
            business_sink.ok_or(Error::Internal)?,
        ))
    } else {
        None
    };

    let business_address = business
        .as_ref()
        .map(|(listener, _)| listener.local_addr())
        .transpose()
        .map_err(|_| Error::Unavailable)?;
    let work_listeners = CancellationToken::new();
    let business_accept_stop = CancellationToken::new();
    let business_connection_stop = CancellationToken::new();
    let mut business_future: Option<
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>,
    > = None;
    if let Some((listener, sink)) = business {
        if let Some(rpc) = &config.business_rpc {
            let identity = if let Some(tls) = &rpc.tls {
                let identities = rpc
                    .identities
                    .iter()
                    .map(|configured| {
                        Ok((
                            parse_hex_32(&configured.certificate_sha256)?,
                            BusinessPrincipal {
                                id: configured.principal_id.clone(),
                                role: configured.role,
                                provider_id: configured.provider_id.clone(),
                                sink_id: configured.sink_id.clone(),
                                provide_methods: configured.provide_methods.clone(),
                                call_methods: configured.call_methods.clone(),
                                global: configured.global,
                                tenants: configured.tenants.clone(),
                                expires_at_ms: configured.expires_at_ms,
                            },
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let _ = tls;
                BusinessIdentity::Mtls { identities }
            } else {
                let token = rpc.development_token()?.ok_or(Error::Configuration)?;
                let role = rpc.development_role.unwrap_or(BusinessRole::Multiplexed);
                BusinessIdentity::Development {
                    token_hash: Sha256::digest(token.as_bytes()).into(),
                    principal: BusinessPrincipal {
                        id: "development".into(),
                        role,
                        provider_id: role.auth_control().then(|| "primary".into()),
                        sink_id: role.events().then(|| "tcp-rpc".into()),
                        provide_methods: if role.auth_control() {
                            vec![
                                "device.authenticate".into(),
                                "device.resolve_verifier".into(),
                            ]
                        } else {
                            Vec::new()
                        },
                        call_methods: if role.auth_control() {
                            vec!["auth.sync".into(), "auth.invalidate".into()]
                        } else if role.commands() {
                            vec!["device.command.send".into()]
                        } else {
                            Vec::new()
                        },
                        global: true,
                        tenants: Vec::new(),
                        expires_at_ms: None,
                    },
                }
            };
            let tls = if let Some(files) = &rpc.tls {
                Some(management_tls_acceptor(files).await?)
            } else {
                None
            };
            let transport = BusinessRpcTransportConfig {
                identity,
                tls,
                v3: rpc.v3.clone(),
                v3_send_ahead: rpc.v3_send_ahead,
                v3_experiment_socket_send_buffer_bytes: rpc.v3_experiment_socket_send_buffer_bytes,
                max_connections: rpc.max_connections,
                max_frame_bytes: 8 * 1024 * 1024,
                auth_max_inflight: rpc.auth_max_inflight,
                heartbeat_ms: 5_000,
                handshake_timeout: Duration::from_millis(limits.connect_timeout_ms),
                read_timeout: Duration::from_millis(limits.packet_read_timeout_ms.max(15_000)),
                write_timeout: Duration::from_millis(limits.write_timeout_ms),
                event_ack_timeout: Duration::from_millis(limits.sink_timeout_ms),
            };
            let services = Arc::new(BusinessRpcServices {
                registry: rpc_registry.as_ref().ok_or(Error::Internal)?.clone(),
                sink: sink.clone(),
                ingress: ingress.clone(),
                mqtt: mqtt_broker.clone(),
                commands: base_services.commands.clone(),
            });
            if rpc.allow_v1 {
                let secret = business_stream_token
                    .as_deref()
                    .ok_or(Error::Configuration)?;
                if secret.is_empty() || secret.len() > 256 {
                    return Err(Error::Configuration);
                }
                let legacy_hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
                business_future = Some(Box::pin(serve_business_mixed(
                    listener,
                    sink.clone(),
                    legacy_hash,
                    transport,
                    services,
                    limits.clone(),
                    (
                        business_accept_stop.child_token(),
                        business_connection_stop.child_token(),
                    ),
                )));
            } else {
                business_future = Some(Box::pin(business_rpc::serve(
                    listener,
                    transport,
                    services,
                    business_accept_stop.child_token(),
                    business_connection_stop.child_token(),
                )));
            }
        } else {
            let secret = business_stream_token.ok_or(Error::Configuration)?;
            let hash: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
            business_future = Some(Box::pin(serve_business_stream(
                listener,
                sink,
                hash,
                limits.clone(),
                work_listeners.child_token(),
            )));
        }
    }
    let event_workers = events.start_owned_workers()?;
    let _run_cancellation = RunCancellation(vec![
        shutdown.clone(),
        work_listeners.clone(),
        business_accept_stop.clone(),
        business_connection_stop.clone(),
    ]);
    lifecycle.mark_running()?;
    let management_listener = CancellationToken::new();
    let mut work_tasks = JoinSet::new();
    let maintenance_broker = mqtt_broker.clone();
    let maintenance_stop = work_listeners.child_token();
    work_tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = maintenance_stop.cancelled() => return Ok(()),
                _ = interval.tick() => maintenance_broker.tick()?,
            }
        }
    });
    if auth_source == DeviceAuthSource::BusinessRpc {
        let authority = rpc_registry.as_ref().ok_or(Error::Internal)?.clone();
        let ingress_for_offline = ingress.clone();
        let mqtt_for_offline = mqtt_broker.clone();
        let grace = Duration::from_millis(
            config
                .business_rpc
                .as_ref()
                .ok_or(Error::Internal)?
                .max_auth_control_offline_ms,
        );
        let offline_stop = work_listeners.child_token();
        work_tasks.spawn(watch_business_auth_offline(
            authority.subscribe_status(),
            grace,
            offline_stop,
            move |observed| {
                let admission = match ingress_for_offline.lifecycle.begin_admission() {
                    Ok(admission) => admission,
                    Err(Error::Draining) => return Ok(()),
                    Err(error) => return Err(error),
                };
                if authority
                    .invalidate_if_offline(observed, || {
                        let invalidate = AuthInvalidation::All;
                        ingress_for_offline.invalidate_auth_admitted_with(
                            &admission,
                            &invalidate,
                            || mqtt_for_offline.invalidate_sessions(&invalidate),
                        )?;
                        Ok(())
                    })?
                    .is_some()
                {
                    ingress_for_offline
                        .metrics
                        .inc(Metric::BusinessRpcOfflineGraceExpirations);
                }
                Ok(())
            },
        ));
    }
    work_tasks.spawn(serve_device_ingress(
        device_ingress,
        base_services.clone(),
        tls.clone(),
        work_listeners.child_token(),
    ));
    let mut management_task = OwnedTask(tokio::spawn(serve_management_http(
        management_http,
        base_services.clone(),
        management_tls,
        management_listener.child_token(),
    )));
    work_tasks.spawn(udp::serve(
        udp,
        base_services.clone(),
        work_listeners.child_token(),
    ));
    let mut business_task = if config.business_rpc.is_some() {
        business_future.map(|future| OwnedTask(tokio::spawn(future)))
    } else {
        if let Some(future) = business_future {
            work_tasks.spawn(future);
        }
        None
    };
    tracing::info!(device_ingress=%device_address,management_http=%config.management_http,business_tcp=?config.business_tcp,"runtime ready");
    if let Some(ready) = ready {
        let _ = ready.send(BoundAddresses {
            device_ingress: device_address,
            management_http: management_address,
            business_tcp: business_address,
        });
    }
    let mut management_running = true;
    let failure = tokio::select! {
        _ = shutdown.cancelled() => None,
        task = work_tasks.join_next() => Some(match task { Some(Ok(Err(error))) => error, _ => Error::Internal }),
        task = &mut management_task => {
            management_running = false;
            Some(match task { Ok(Err(error)) => error, _ => Error::Internal })
        },
    };
    lifecycle.begin_quiesce().await?;
    events.close_admission()?;
    work_listeners.cancel();
    business_accept_stop.cancel();
    while work_tasks.join_next().await.is_some() {}

    // All network owners have detached. Snapshot MQTT protocol state as one versioned,
    // fsynced image before claiming a successful planned shutdown. A storage failure blocks the
    // voluntary shutdown: the process stays alive and unready so an operator can repair storage.
    let retry_delay = Duration::from_millis(limits.retry_max_ms.min(1_000));
    let mut mqtt_structural_failure = None;
    loop {
        match mqtt_broker.commit_to(&config.spool_directory).await {
            Ok(_) => break,
            Err(error @ (Error::Overloaded | Error::Configuration | Error::Invalid)) => {
                // Limits validation proves that every admitted legal state fits the recovery
                // image. Retrying cannot repair a structural violation, but unrelated required
                // EventBus work must still be drained or spooled before exit is blocked.
                tracing::error!(error=%error, "MQTT recovery invariant violated");
                mqtt_structural_failure = Some(error);
                break;
            }
            Err(error) => {
                tracing::error!(error=%error, "MQTT recovery commit failed; shutdown remains blocked");
                tokio::time::sleep(retry_delay).await;
            }
        }
    }

    let drained = events
        .wait_required_drained(Duration::from_millis(limits.shutdown_drain_timeout_ms))
        .await?;
    if drained {
        events.stop_workers().await?;
        if !recovered_files.is_empty() {
            spool.remove_committed(recovered_files).await?;
        }
    } else {
        lifecycle.mark_spooling()?;
        loop {
            let pending = events.spool_records()?;
            if pending.is_empty() {
                events.stop_workers().await?;
                if !recovered_files.is_empty() {
                    spool.remove_committed(recovered_files.clone()).await?;
                }
                break;
            }
            let encoded_bytes = pending.iter().try_fold(0usize, |total, record| {
                total
                    .checked_add(record.encoded_len()?)
                    .ok_or(Error::Overloaded)
            })?;
            let pending_count = pending.len();
            match spool.commit(pending).await {
                Ok(_) => {
                    events.stop_workers().await?;
                    metrics.add(Metric::SpoolRecords, pending_count as u64);
                    metrics.add(Metric::SpoolBytes, encoded_bytes as u64);
                    break;
                }
                Err(error) => {
                    tracing::error!(error=%error, pending=pending_count, "event spool commit failed; shutdown remains blocked");
                    if events.wait_required_drained(retry_delay).await? {
                        events.stop_workers().await?;
                        if !recovered_files.is_empty() {
                            spool.remove_committed(recovered_files.clone()).await?;
                        }
                        break;
                    }
                }
            }
        }
    }
    if !shutdown_can_finish(mqtt_structural_failure.is_none(), true)
        && let Some(error) = mqtt_structural_failure
    {
        tracing::error!(error=%error, "critical MQTT recovery fault; required EventBus work is safe, process remains alive and unready");
        std::future::pending::<()>().await;
        return Err(error);
    }
    business_connection_stop.cancel();
    if let Some(task) = business_task.take() {
        let _ = task.await;
    }
    lifecycle.mark_drained()?;
    management_listener.cancel();
    if management_running {
        let _ = management_task.await;
    }
    event_workers.shutdown().await?;
    tracing::info!("shutdown complete");
    failure.map_or(Ok(()), Err)
}

pub(crate) async fn watch_business_auth_offline(
    mut status: tokio::sync::watch::Receiver<ProviderStatus>,
    grace: Duration,
    stop: CancellationToken,
    mut invalidate: impl FnMut(ProviderStatus) -> Result<()>,
) -> Result<()> {
    let mut offline_state = None;
    let mut invalidated = false;
    loop {
        let current = *status.borrow();
        if current.serving {
            offline_state = None;
            invalidated = false;
        } else if offline_state != Some((current.transition, current.changed_at)) {
            offline_state = Some((current.transition, current.changed_at));
            invalidated = false;
        }
        let deadline = offline_state.map(|(_, since)| since + grace);
        if let Some(expires) = deadline.filter(|_| !invalidated)
            && tokio::time::Instant::now() >= expires
        {
            // A newly synchronized generation wins an exact-deadline race.
            let observed = *status.borrow();
            if !observed.serving && observed.transition == current.transition {
                invalidate(observed)?;
            }
            invalidated = true;
            continue;
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            changed = status.changed() => {
                if changed.is_err() { return Ok(()); }
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)), if deadline.is_some() && !invalidated => {}
        }
    }
}

async fn serve_business_mixed(
    listener: TcpListener,
    sink: Arc<BusinessRpcEventSink>,
    legacy_token_hash: [u8; 32],
    transport: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    limits: Arc<Limits>,
    stops: (CancellationToken, CancellationToken),
) -> Result<()> {
    let (stop_accepting, stop_connections) = stops;
    transport.validate(listener.local_addr().map_err(|_| Error::Unavailable)?)?;
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = stop_accepting.cancelled() => break,
            completed = tasks.join_next(), if !tasks.is_empty() => { if let Some(Err(error)) = completed { tracing::warn!(%error, "business mixed connection failed"); } continue; },
            accepted = listener.accept() => accepted.map_err(|_| Error::Unavailable)?,
        };
        if tasks.len() >= transport.max_connections {
            drop(accepted.0);
            continue;
        }
        let (mut stream, _) = accepted;
        let sink = sink.clone();
        let services = services.clone();
        let transport = transport.clone();
        let limits = limits.clone();
        let stop = stop_connections.child_token();
        tasks.spawn(async move {
            let first =
                tokio::time::timeout(Duration::from_millis(limits.connect_timeout_ms), async {
                    let mut header = [0u8; 4];
                    stream
                        .read_exact(&mut header)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    let length =
                        usize::try_from(u32::from_be_bytes(header)).map_err(|_| Error::Invalid)?;
                    if length == 0 || length > limits.max_tcp_frame_size {
                        return Err(Error::Invalid);
                    }
                    let mut payload = vec![0u8; length];
                    stream
                        .read_exact(&mut payload)
                        .await
                        .map_err(|_| Error::Unavailable)?;
                    Ok::<_, Error>(payload)
                })
                .await
                .map_err(|_| Error::Timeout)??;
            let header: serde_json::Value =
                serde_json::from_slice(&first).map_err(|_| Error::Invalid)?;
            match header.get("version").and_then(|value| value.as_u64()) {
                Some(1) => {
                    serve_business_connection(
                        stream,
                        sink,
                        legacy_token_hash,
                        limits,
                        stop,
                        Some(first),
                    )
                    .await
                }
                Some(2 | 3) if first.len() <= BUSINESS_RPC_HELLO_MAX_BYTES => {
                    business_rpc::serve_accepted(stream, transport, services, stop, first).await
                }
                _ => Err(Error::Invalid),
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub(crate) fn shutdown_can_finish(
    mqtt_recovery_safe: bool,
    eventbus_required_work_safe: bool,
) -> bool {
    mqtt_recovery_safe && eventbus_required_work_safe
}

// Port-zero allocation is independent for TCP and UDP. Retry before starting
// any listeners/workers; explicit configured ports still fail immediately.
async fn bind_device_pair(address: SocketAddr) -> Result<(TcpListener, UdpSocket)> {
    for _ in 0..32 {
        let tcp = TcpListener::bind(address).await.map_err(|error| {
            tracing::error!(listener="device_tcp",%address,error_kind=?error.kind(),"listener bind failed"); Error::Unavailable
        })?;
        let bound = tcp.local_addr().map_err(|_| Error::Unavailable)?;
        match UdpSocket::bind(bound).await {
            Ok(udp) => return Ok((tcp, udp)),
            Err(error) if address.port() == 0 && error.kind() == std::io::ErrorKind::AddrInUse => {
                continue;
            }
            Err(error) => {
                tracing::error!(listener="device_udp",address=%bound,error_kind=?error.kind(),"listener bind failed");
                return Err(Error::Unavailable);
            }
        }
    }
    Err(Error::Unavailable)
}
