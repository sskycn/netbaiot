use super::*;
use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct UnusedAuthenticator;
#[async_trait]
impl DeviceAuthenticator for UnusedAuthenticator {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        panic!("bound command session must not authenticate again")
    }
    async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
        panic!("bound command session must not resolve credentials")
    }
}

struct UnusedSink;
#[async_trait]
impl EventSink for UnusedSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        panic!("command dispatch must not emit an event")
    }
}

fn fixture() -> (Arc<Services>, Arc<AuthenticatedDevice>) {
    let limits = Arc::new(Limits {
        max_pending_commands: 1,
        max_outbound_messages_per_connection: 1,
        ..Limits::default()
    });
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let sink_id = SinkId::new("unused").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(UnusedSink),
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
        1,
    )
    .unwrap();
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        AuthCache::new(
            Arc::new(UnusedAuthenticator),
            limits.clone(),
            metrics.clone(),
        ),
        CodecRegistry::new(vec![(
            CodecId::new("json").unwrap(),
            1,
            Arc::new(JsonV1::default()),
        )])
        .unwrap(),
        events,
        GatewayControl::empty(limits.clone()),
        metrics,
        Sessions::new(limits),
        lifecycle,
    ));
    let auth = Arc::new(AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    });
    (Services::new(ingress, CancellationToken::new()), auth)
}

fn attach(services: &Services, auth: &AuthenticatedDevice, v5: bool) -> broker::Attachment {
    let attachment = if v5 {
        services
            .mqtt
            .attach_v5(auth, "command".into(), false, 60, 8)
    } else {
        services.mqtt.attach(auth, "command".into(), false)
    }
    .unwrap();
    services
        .mqtt
        .subscribe(
            &attachment.key,
            attachment.generation,
            &topic(&auth.device_key, TopicKind::Down),
            0,
        )
        .unwrap();
    attachment
}

fn message(auth: &AuthenticatedDevice, expiry: Timestamp) -> BrokerMessage {
    BrokerMessage {
        topic: (topic(&auth.device_key, TopicKind::Down)).into(),
        payload: vec![1].into(),
        qos: 0,
        retain: false,
        properties: (broker::PublishProperties {
            expires_at_ms: Some(expiry),
            ..Default::default()
        })
        .into(),
    }
}

#[tokio::test]
async fn qos0_receiver_teardown_finishes_dedup_for_both_mqtt_versions() {
    for v5 in [false, true] {
        for expired in [false, true] {
            let (services, auth) = fixture();
            let (lease, mut commands) = services
                .ingress
                .sessions
                .register(auth.clone(), Transport::Mqtt)
                .unwrap();
            lease.set_command_ready(true).unwrap();
            let attachment = attach(&services, &auth, v5);
            let baseline = services.mqtt.usage().unwrap();
            let command = DeviceCommand {
                command_id: CommandId::generate(),
                device: auth.device_key.clone(),
                expires_at: None,
                payload: DeviceCommandPayload {
                    name: "set".into(),
                    arguments: Default::default(),
                },
            };
            assert_eq!(
                services.commands.send(command.clone()).await.unwrap().state,
                DeliveryState::Queued
            );
            let mut queued = commands.try_recv().unwrap();
            let expiry = queued.expires_at;
            let progress = queued.progress.take().unwrap();
            services
                .mqtt
                .send_live_tracked(
                    &attachment.key,
                    attachment.generation,
                    message(&auth, expiry),
                    Some(progress.clone()),
                )
                .unwrap();
            drop(queued);
            assert_eq!(services.ingress.sessions.queued_messages(), 0);
            assert_eq!(services.ingress.sessions.queued_bytes(), 0);
            assert_eq!(attachment.receiver.len(), 1);
            assert_eq!(services.mqtt.usage().unwrap(), baseline);
            assert_eq!(progress.state(), DeliveryState::Queued);
            if expired {
                assert!(progress.expire(expiry));
            }
            // No select timing or sleep: the queued frame has never reached a writer.
            drop(attachment);
            let expected = if expired {
                DeliveryState::Expired
            } else {
                DeliveryState::Failed
            };
            assert_eq!(progress.state(), expected);
            progress.abandon_unsent(); // A concurrent cleanup/expiry must count only once.
            progress.expire(expiry);
            progress.update(DeliveryState::Failed);
            assert_eq!(services.ingress.metrics.get(Metric::CommandFailed), 1);
            assert_eq!(
                services.commands.send(command.clone()).await.unwrap().state,
                expected
            );
            assert!(commands.try_recv().is_err());
            assert_eq!(Arc::strong_count(&progress), 2); // dedup entry plus this observer
            let resumed = attach(&services, &auth, v5);
            assert!(resumed.receiver.is_empty()); // QoS0 never becomes offline protocol work.
            assert_eq!(services.mqtt.usage().unwrap(), baseline);
            let mut next = command;
            next.command_id = CommandId::generate();
            services.commands.send(next).await.unwrap(); // the sole command slot was released
            drop(commands.try_recv().unwrap());
            drop(resumed);
        }
    }
}

// Assert the actual writer sees Dispatching, with a deterministic write outcome.
struct CommandWriter {
    progress: Arc<CommandProgress>,
    fail: bool,
}
impl AsyncRead for CommandWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for CommandWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        assert_eq!(self.progress.state(), DeliveryState::Dispatching);
        Poll::Ready(if self.fail {
            Err(io::ErrorKind::BrokenPipe.into())
        } else {
            Ok(bytes.len())
        })
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn qos0_socket_write_outcomes_for_both_mqtt_versions() {
    for v5 in [false, true] {
        for fail in [false, true] {
            let (services, auth) = fixture();
            let mut attachment = attach(&services, &auth, v5);
            let baseline = services.mqtt.usage().unwrap();
            let expiry = now_ms() + 60_000;
            let progress = CommandProgress::new(expiry, services.ingress.metrics.clone());
            services
                .mqtt
                .send_live_tracked(
                    &attachment.key,
                    attachment.generation,
                    message(&auth, expiry),
                    Some(progress.clone()),
                )
                .unwrap();
            assert_eq!(progress.state(), DeliveryState::Queued);
            let frame = attachment.receiver.try_recv().unwrap();
            let mut stream: BoxStream = Box::new(CommandWriter {
                progress: progress.clone(),
                fail,
            });
            let result = if v5 {
                v5_connection::send_frame(
                    &mut stream,
                    &services,
                    &attachment.key,
                    attachment.generation,
                    frame,
                    services.ingress.limits.max_mqtt_packet_size,
                    &mut false,
                )
                .await
            } else {
                send_broker_frame(
                    &mut stream,
                    &services,
                    &attachment.key,
                    attachment.generation,
                    frame,
                )
                .await
            };
            assert_eq!(result.is_err(), fail);
            let expected = if fail {
                DeliveryState::Failed
            } else {
                DeliveryState::Sent
            };
            assert_eq!(progress.state(), expected);
            drop(attachment);
            progress.abandon_unsent();
            assert!(!progress.expire(expiry));
            assert_eq!(progress.state(), expected);
            assert_eq!(
                services.ingress.metrics.get(Metric::CommandFailed),
                u64::from(fail)
            );
            assert_eq!(services.ingress.metrics.get(Metric::CommandReceived), 0);
            assert_eq!(Arc::strong_count(&progress), 2); // observer and mock writer only
            let resumed = attach(&services, &auth, v5);
            assert!(resumed.receiver.is_empty());
            assert_eq!(services.mqtt.usage().unwrap(), baseline);
        }
    }
}

#[tokio::test]
async fn qos0_prewrite_expiry_and_encoding_failure_for_both_mqtt_versions() {
    for v5 in [false, true] {
        for expired in [false, true] {
            let (services, auth) = fixture();
            let mut attachment = attach(&services, &auth, v5);
            let expiry = now_ms() + 60_000;
            let progress = CommandProgress::new(expiry, services.ingress.metrics.clone());
            let mut message = message(&auth, expiry);
            if !expired {
                message.payload = vec![0; services.ingress.limits.max_mqtt_packet_size].into();
            }
            services
                .mqtt
                .send_live_tracked(
                    &attachment.key,
                    attachment.generation,
                    message,
                    Some(progress.clone()),
                )
                .unwrap();
            if expired {
                assert!(progress.expire(expiry));
            }
            let frame = attachment.receiver.try_recv().unwrap();
            // Any socket write would fail the Dispatching assertion in this writer.
            let mut stream: BoxStream = Box::new(CommandWriter {
                progress: progress.clone(),
                fail: false,
            });
            let result = if v5 {
                v5_connection::send_frame(
                    &mut stream,
                    &services,
                    &attachment.key,
                    attachment.generation,
                    frame,
                    services.ingress.limits.max_mqtt_packet_size,
                    &mut false,
                )
                .await
            } else {
                send_broker_frame(
                    &mut stream,
                    &services,
                    &attachment.key,
                    attachment.generation,
                    frame,
                )
                .await
            };
            assert_eq!(result.is_err(), !v5 && !expired);
            drop(attachment);
            progress.abandon_unsent();
            assert_eq!(
                progress.state(),
                if expired {
                    DeliveryState::Expired
                } else {
                    DeliveryState::Failed
                }
            );
            assert_eq!(services.ingress.metrics.get(Metric::CommandFailed), 1);
            assert_eq!(services.ingress.metrics.get(Metric::CommandSent), 0);
        }
    }
}
