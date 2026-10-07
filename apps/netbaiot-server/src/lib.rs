use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    Services,
    business_rpc::{
        self, BusinessIdentity, BusinessPrincipal, BusinessRpcServices, BusinessRpcTransportConfig,
        V3SendAhead,
    },
    mqtt::broker::MqttBroker,
    serve_device_ingress, serve_management_http, udp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, UdpSocket},
    task::JoinSet,
};
use tokio_rustls::{TlsAcceptor, rustls};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use tokio::{io::AsyncWriteExt, sync::mpsc};

mod auth_provider;
mod bootstrap;
mod config;
mod delivery;
mod diagnostics;
mod entry;
mod server;
mod tls;

use auth_provider::HttpAuthProvider;
use bootstrap::bootstrap_snapshot;
pub use config::*;
use delivery::{AuditSink, HttpSink};
pub use diagnostics::*;
pub use entry::*;
pub use server::{run, run_with_credentials, run_with_credentials_ready};
#[cfg(test)]
use server::{shutdown_can_finish, watch_business_auth_offline};
pub use tls::{management_tls_acceptor, tls_acceptor};

#[cfg(test)]
mod reliability_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn business_rpc_v3_config_is_explicit_and_limits_are_checked() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
        config.business_tcp = Some("127.0.0.1:19002".parse().unwrap());
        assert!(
            serde_json::from_value::<BusinessRpcConfig>(serde_json::json!({
                "version":2,"tls":null,"development_token_env":"NETBAIOT_BUSINESS_RPC_TOKEN"
            }))
            .is_err()
        );
        config.business_rpc = Some(
            serde_json::from_value(serde_json::json!({
                "tls":null,"development_token_env":"NETBAIOT_BUSINESS_RPC_TOKEN"
            }))
            .unwrap(),
        );
        assert!(config.validate().is_ok());

        let mut limits = business_rpc_v3::V3Limits::default();
        config.business_rpc.as_mut().unwrap().limits = limits.clone();
        assert!(config.validate().is_ok());

        limits.max_frame_payload_bytes = 1024;
        config.business_rpc.as_mut().unwrap().limits = limits.clone();
        assert!(config.validate().is_err());
        limits.max_frame_payload_bytes = 8192;
        limits.max_concurrent_streams = 0;
        config.business_rpc.as_mut().unwrap().limits = limits.clone();
        assert!(config.validate().is_err());
        limits.max_concurrent_streams = 256;
        limits.initial_connection_window_bytes = limits.initial_stream_window_bytes - 1;
        config.business_rpc.as_mut().unwrap().limits = limits;
        assert!(config.validate().is_err());

        let rpc = config.business_rpc.as_mut().unwrap();
        rpc.limits = business_rpc_v3::V3Limits::default();
        rpc.send_ahead = Some(V3SendAhead {
            stream_bytes: 1,
            connection_bytes: 8192,
        });
        assert!(config.validate().is_err());
        config.business_rpc.as_mut().unwrap().send_ahead = Some(V3SendAhead {
            stream_bytes: 8192,
            connection_bytes: 131072,
        });
        assert!(config.validate().is_ok());
        config
            .business_rpc
            .as_mut()
            .unwrap()
            .experiment_socket_send_buffer_bytes = Some(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn business_rpc_command_roles_validate_without_changing_existing_roles() {
        let config_for = |role,
                          provider: Option<&str>,
                          sink: Option<&str>,
                          provide: Vec<&str>,
                          call: Vec<&str>| {
            let mut config: Config =
                serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
            config.business_tcp = Some("127.0.0.1:19002".parse().unwrap());
            config.business_rpc = Some(BusinessRpcConfig {
                limits: netbaiot_core::business_rpc_v3::V3Limits::default(),
                send_ahead: None,
                experiment_socket_send_buffer_bytes: None,
                tls: Some(ManagementTlsFiles {
                    certificate: "cert".into(),
                    private_key: "key".into(),
                    client_ca: Some("ca".into()),
                    require_client_certificate: true,
                }),
                identities: vec![BusinessRpcIdentityConfig {
                    certificate_sha256: "00".repeat(32),
                    principal_id: "test".into(),
                    role,
                    provider_id: provider.map(str::to_owned),
                    sink_id: sink.map(str::to_owned),
                    provide_methods: provide.into_iter().map(str::to_owned).collect(),
                    call_methods: call.into_iter().map(str::to_owned).collect(),
                    global: true,
                    tenants: Vec::new(),
                    expires_at_ms: None,
                }],
                development_token_env: None,
                development_role: None,
                max_connections: 8,
                auth_max_inflight: 16,
                max_auth_control_offline_ms: 30_000,
            });
            config.validate().is_ok()
        };
        assert!(config_for(
            BusinessRole::Commands,
            None,
            None,
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Commands,
            None,
            None,
            vec!["device.authenticate"],
            vec!["device.command.send"]
        ));
        assert!(config_for(
            BusinessRole::Application,
            None,
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Application,
            Some("primary"),
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::Events,
            None,
            Some("tcp-rpc"),
            vec![],
            vec!["device.command.send"]
        ));
        assert!(!config_for(
            BusinessRole::AuthControl,
            Some("primary"),
            None,
            vec!["device.authenticate", "device.resolve_verifier"],
            vec!["device.command.send"]
        ));
        assert!(config_for(
            BusinessRole::Multiplexed,
            Some("primary"),
            Some("tcp-rpc"),
            vec!["device.authenticate", "device.resolve_verifier"],
            vec!["auth.sync", "auth.invalidate"]
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn business_auth_zero_grace_invalidates_without_clock_advance() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::ZERO,
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn business_auth_grace_deadline_and_reconnect_are_generation_fenced() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::from_secs(30),
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(29_999)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        status.send_replace(ProviderStatus {
            epoch: 2,
            serving: true,
            transition: 3,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        status.send_replace(ProviderStatus {
            epoch: 2,
            serving: false,
            transition: 4,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        status.send_replace(ProviderStatus {
            epoch: 3,
            serving: true,
            transition: 5,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(21)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        status.send_replace(ProviderStatus {
            epoch: 3,
            serving: false,
            transition: 6,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        status.send_replace(ProviderStatus {
            epoch: 4,
            serving: true,
            transition: 7,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn collapsed_serving_disconnect_starts_grace_at_actual_disconnect() {
        let (status, receiver) = tokio::sync::watch::channel(ProviderStatus {
            epoch: 0,
            serving: false,
            transition: 0,
            changed_at: tokio::time::Instant::now(),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let stop = CancellationToken::new();
        let task = tokio::spawn(watch_business_auth_offline(
            receiver,
            Duration::from_secs(30),
            stop.clone(),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: true,
            transition: 1,
            changed_at: tokio::time::Instant::now(),
        });
        status.send_replace(ProviderStatus {
            epoch: 1,
            serving: false,
            transition: 2,
            changed_at: tokio::time::Instant::now(),
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[test]
    fn business_rpc_auth_and_event_delivery_are_independent() {
        let base: Config =
            serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
        for (auth, delivery) in [
            (
                DeviceAuthSource::BusinessRpc,
                EventDeliverySource::BusinessRpc,
            ),
            (DeviceAuthSource::BusinessRpc, EventDeliverySource::Http),
            (DeviceAuthSource::Http, EventDeliverySource::BusinessRpc),
            (DeviceAuthSource::Static, EventDeliverySource::BusinessRpc),
        ] {
            let mut value = serde_json::to_value(&base).unwrap();
            value["business_tcp"] = serde_json::json!("127.0.0.1:19002");
            value["business_rpc"] = serde_json::json!({
                "tls": null,
                "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN"
            });
            value["device_auth"] = serde_json::to_value(auth).unwrap();
            value["event_delivery"] = serde_json::to_value(delivery).unwrap();
            value["auth_provider_url"] = if auth == DeviceAuthSource::Http {
                serde_json::json!("http://127.0.0.1:19003")
            } else {
                serde_json::Value::Null
            };
            value["delivery_url"] = if delivery == EventDeliverySource::Http {
                serde_json::json!("http://127.0.0.1:19004")
            } else {
                serde_json::Value::Null
            };
            let config: Config = serde_json::from_value(value).unwrap();
            assert!(
                config.validate().is_ok(),
                "combination {:?} {:?}",
                auth as u8,
                delivery as u8
            );
        }
    }

    #[test]
    fn mqtt_recovery_structural_eventbus_safety_001() {
        assert!(!shutdown_can_finish(false, false));
        assert!(!shutdown_can_finish(false, true));
        assert!(!shutdown_can_finish(true, false));
        assert!(shutdown_can_finish(true, true));
    }

    fn delivery() -> DeliveryEnvelope {
        DeliveryEnvelope {
            event: Arc::new(DeviceEvent {
                event_id: EventId::generate(),
                source_message_id: SourceMessageId::new("business-filter").unwrap(),
                device: DeviceKey {
                    tenant_id: TenantId::new("tenant-a").unwrap(),
                    product_id: ProductId::new("product").unwrap(),
                    device_id: DeviceId::new("device").unwrap(),
                },
                received_at: 1,
                occurred_at: None,
                kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
            }),
            sink_id: SinkId::new("tcp-rpc").unwrap(),
            attempt: 1,
            accepted_at: 1,
        }
    }

    #[tokio::test]
    async fn eventbus_tcp_absence_filter_change_and_reconnect_preserve_isolation() {
        let limits = Arc::new(Limits::default());
        let metrics = Arc::new(Metrics::with_lock_timing());
        let tcp = BusinessRpcEventSink::new();
        let fast_id = SinkId::new("fast").unwrap();
        let tcp_id = SinkId::new("tcp").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![
                SinkDefinition::bounded(
                    fast_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(AuditSink),
                    &limits,
                ),
                SinkDefinition::bounded(
                    tcp_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    tcp.clone(),
                    &limits,
                ),
            ],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![fast_id, tcp_id],
            }],
            1,
        )
        .unwrap();
        let event = (*delivery().event).clone();
        let expected = event.event_id;
        bus.publish(event).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.get(Metric::SinkAcks) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bus.usage().unwrap().pending_required, 1);
        let (sender, mut mismatched) = mpsc::channel(1);
        let generation = tcp
            .claim(
                sender,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-b").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(mismatched.try_recv().is_err());
        assert_eq!(metrics.get(Metric::SinkAcks), 1);
        assert_eq!(metrics.get(Metric::SinkRetries), 0);
        assert!(
            metrics
                .render()
                .contains("netbaiot_event_bus_probe_wake_timer_total 0\n")
        );
        tcp.release(generation).unwrap();
        let (sender, mut requests) = mpsc::channel(1);
        let generation = tcp.claim(sender, EventFilter::default()).unwrap();
        let request = tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.delivery.event.event_id, expected);
        request.result.send(Ok(SinkAck)).unwrap();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        tcp.release(generation).unwrap();
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn active_subscriber_is_rejected_and_filter_mismatch_is_not_acknowledged() {
        let sink = BusinessRpcEventSink::new();
        let (sender, _receiver) = mpsc::channel(1);
        let generation = sink
            .claim(
                sender,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-b").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        let (other, _other_receiver) = mpsc::channel(1);
        assert!(matches!(
            sink.claim(other, EventFilter::default()),
            Err(Error::Conflict)
        ));
        let pending = delivery();
        let expected = pending.event.event_id;
        let sink_task = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.deliver(pending).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(20), async {
                while !sink_task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "a non-matching subscriber must not acknowledge required work"
        );
        sink.release(generation.wrapping_add(1)).unwrap();
        assert!(sink.has_owner().unwrap());
        sink.release(generation).unwrap();
        assert!(!sink.has_owner().unwrap());

        // The same already-accepted responsibility survives the filter revision and is ACKed
        // only after a later eligible subscriber explicitly confirms it.
        let (matching, mut requests) = mpsc::channel(1);
        let matching_generation = sink
            .claim(
                matching,
                EventFilter {
                    tenant: Some(TenantId::new("tenant-a").unwrap()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        let request = requests.recv().await.unwrap();
        assert_eq!(request.delivery.event.event_id, expected);
        request.result.send(Ok(SinkAck)).unwrap();
        assert!(matches!(sink_task.await.unwrap(), Ok(SinkAck)));
        sink.release(matching_generation).unwrap();
    }
}
