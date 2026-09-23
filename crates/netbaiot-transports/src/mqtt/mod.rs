pub mod broker;
pub mod packet;
pub mod topics;

use crate::common::*;
use broker::{BrokerFrame, BrokerMessage, InboundQos2Action, subscribe_acl};
use netbaiot_core::*;
use netbaiot_runtime::*;
use packet::*;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use topics::{TopicKind, publish_acl, topic};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Accepted,
    AwaitConnect,
    Authenticating,
    Connected,
    Draining,
    Closed,
}

pub struct StateMachine {
    pub state: ConnectionState,
}

impl StateMachine {
    pub fn transition(&mut self, next: ConnectionState) -> Result<()> {
        use ConnectionState::*;
        if !matches!(
            (self.state, next),
            (Accepted, AwaitConnect)
                | (AwaitConnect, Authenticating)
                | (Authenticating, Connected)
                | (Connected, Draining)
                | (_, Closed)
        ) {
            return Err(Error::Invalid);
        }
        self.state = next;
        Ok(())
    }
}

async fn next(
    reader: &mut Reader,
    stream: &mut BoxStream,
    limits: &Limits,
    idle: Instant,
) -> Result<(Packet, u64, Instant)> {
    loop {
        let validation_started = Instant::now();
        if let Some(packet) = decode(&mut reader.buffer, limits)? {
            let validation_us = validation_started.elapsed().as_micros() as u64;
            let validated_at = Instant::now();
            reader.consumed();
            return Ok((packet, validation_us, validated_at));
        }
        reader.read_more(stream, idle).await?;
    }
}

async fn send(stream: &mut BoxStream, services: &Services, bytes: &[u8]) -> Result<()> {
    write(stream, bytes, services.ingress.limits.write_timeout_ms).await?;
    services.ingress.metrics.inc(Metric::MqttPacketsSent);
    Ok(())
}

async fn send_broker_frame(
    stream: &mut BoxStream,
    services: &Services,
    frame: BrokerFrame,
) -> Result<()> {
    let bytes = match frame {
        BrokerFrame::Publish(delivery) => publish(
            &delivery.message.topic,
            &delivery.message.payload,
            delivery.message.qos,
            delivery.packet_id,
            delivery.message.retain,
            delivery.dup,
            &services.ingress.limits,
        )?,
        // PUBREL's MQTT 3.1.1 fixed-header flags are always 0010. Retransmission
        // is represented in broker state, not by setting a reserved header bit.
        BrokerFrame::Pubrel { packet_id, dup: _ } => ack(0x62, packet_id),
    };
    send(stream, services, &bytes).await
}

async fn accept_iot_publish(
    services: &Services,
    auth: &AuthenticatedDevice,
    message: &BrokerMessage,
    validated_at: Instant,
    validation_us: u64,
) -> Result<Option<IngressAcceptance>> {
    let kind = publish_acl(auth, &message.topic)?;
    if message.payload.is_empty() && message.retain {
        return Ok(None);
    }
    let acceptance = services
        .ingress
        .ingest(
            auth,
            IngressEnvelope {
                transport: Transport::Mqtt,
                payload: &message.payload,
                require_command_ack: kind == TopicKind::DownAck,

                validated_at: validated_at.into(),
                validation_us,
            },
        )
        .await?;
    Ok(Some(acceptance))
}

async fn process_publish(
    services: &Services,
    auth: &AuthenticatedDevice,
    message: &BrokerMessage,
    validated_at: Instant,
    validation_us: u64,
) -> Result<Option<IngressAcceptance>> {
    // Authorization must precede every broker-visible side effect. Otherwise an unauthorized
    // retained or routed publication could be observed before the IoT binding rejects it.
    publish_acl(auth, &message.topic)?;
    // Broker-side retained/routing admission is the last fallible MQTT responsibility before the
    // unified event crosses EventAccepted. A later broker error must never turn an accepted QoS1
    // DeviceEvent into a producer-visible failure and retransmission.
    services.mqtt.route(&auth.device_key, message.clone())?;
    let acceptance =
        accept_iot_publish(services, auth, message, validated_at, validation_us).await?;
    Ok(acceptance)
}

async fn bind_will(services: &Services, auth: &AuthenticatedDevice, message: &BrokerMessage) {
    // MQTT delivery is settled synchronously by WillGuard. The optional IoT binding cannot make
    // an already accepted Will disappear or be published twice.
    let _ = accept_iot_publish(services, auth, message, Instant::now(), 0).await;
}

pub async fn connection(
    mut stream: BoxStream,
    services: Arc<Services>,
    mut connection: ConnectionLease,
    stop: CancellationToken,
) -> Result<()> {
    let limits = &services.ingress.limits;
    let mut machine = StateMachine {
        state: ConnectionState::Accepted,
    };
    machine.transition(ConnectionState::AwaitConnect)?;
    let mut reader = Reader::new(limits.max_mqtt_packet_size, limits.packet_read_timeout_ms);
    let (first, _, _) = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        packet = next(&mut reader, &mut stream, limits, connection.connect_deadline()) => packet?,
    };
    services.ingress.metrics.inc(Metric::MqttPacketsReceived);
    let mut connect = match first {
        Packet::Connect(connect) => connect,
        Packet::UnsupportedVersion(_) => {
            send(&mut stream, &services, &connack(false, 1)).await?;
            return Err(Error::Invalid);
        }
        _ => {
            services.ingress.metrics.inc(Metric::MqttProtocolViolations);
            return Err(Error::Invalid);
        }
    };
    if connect.client_id.is_empty() && !connect.clean_session {
        send(&mut stream, &services, &connack(false, 2)).await?;
        return Err(Error::Invalid);
    }
    let Some(username) = connect.username.as_deref() else {
        send(&mut stream, &services, &connack(false, 4)).await?;
        return Err(Error::Authentication);
    };
    let Some(password) = connect.password.as_deref() else {
        send(&mut stream, &services, &connack(false, 4)).await?;
        return Err(Error::Authentication);
    };
    machine.transition(ConnectionState::Authenticating)?;
    let candidate = match authenticate_stream(
        &services,
        AuthenticationRequest::Secret {
            credential_id: username,
            secret: password,
        },
        &mut reader,
        &mut stream,
        &stop,
    )
    .await
    {
        Ok(candidate) => candidate,
        Err(error) => {
            services.ingress.metrics.inc(Metric::MqttConnectFailure);
            if matches!(error, Error::Authentication) {
                send(&mut stream, &services, &connack(false, 4)).await?;
            }
            return Err(error);
        }
    };
    if let Some(will) = &connect.will
        && publish_acl(&candidate.auth, &will.topic).is_err()
    {
        send(&mut stream, &services, &connack(false, 5)).await?;
        return Err(Error::Forbidden);
    }
    if let Err(error) = connection.authenticate(&candidate.auth.device_key) {
        send(&mut stream, &services, &connack(false, 3)).await?;
        return Err(error);
    }
    let auth = Arc::new(candidate.auth.clone());
    let requested_client_id = connect.client_id.clone();
    let clean_session = connect.clean_session;
    let mqtt = services.mqtt.clone();
    let (live_session, mut commands, (mut attachment, client_id)) = services
        .ingress
        .register_session_with(candidate, Transport::Mqtt, move |bound_auth, generation| {
            let client_id = if requested_client_id.is_empty() {
                format!("generated-{generation}")
            } else {
                requested_client_id
            };
            let attachment = mqtt.attach(bound_auth, client_id.clone(), clean_session)?;
            Ok((attachment, client_id))
        })?;
    connect.client_id = client_id;
    let mut will_guard = connect
        .will
        .take()
        .map(|will| {
            services.mqtt.reserve_will(
                auth.device_key.clone(),
                BrokerMessage {
                    topic: will.topic,
                    payload: will.payload.to_vec(),
                    qos: will.qos,
                    retain: will.retain,
                },
            )
        })
        .transpose()?;
    if let Some(will) = &mut will_guard {
        // From this point the broker has accepted CONNECT responsibility. Drop publishes the Will
        // on every early return, including a failed CONNACK write.
        will.arm();
    }
    send(
        &mut stream,
        &services,
        &connack(attachment.session_present, 0),
    )
    .await?;
    services.ingress.metrics.inc(Metric::MqttConnectSuccess);
    machine.transition(ConnectionState::Connected)?;
    let keepalive = if connect.keep_alive == 0 {
        None
    } else {
        Some(Duration::from_millis(u64::from(connect.keep_alive) * 1500))
    };
    let mut last = Instant::now();
    let mut normal_disconnect = false;
    let result = async {
        loop {
            let idle = last + keepalive.unwrap_or(Duration::from_millis(limits.idle_timeout_ms));
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = live_session.cancel.cancelled() => break,
                _ = attachment.cancel.cancelled() => break,
                _ = tokio::time::sleep_until(idle) => {
                    if keepalive.is_some() { services.ingress.metrics.inc(Metric::MqttKeepaliveDisconnects); }
                    return Err(Error::Timeout);
                }
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    if command.expires_at <= now_ms() { continue }
                    let down = topic(&auth.device_key, TopicKind::Down);
                    // Management commands target this authenticated live connection directly.
                    // A missing broker subscription must not turn an accepted command into a
                    // silent drop; QoS1 supplies transport receipt independently of execution ACK.
                    let qos = services
                        .mqtt
                        .subscription_qos(&attachment.key, &down)?
                        .unwrap_or(1);
                    services.mqtt.send_live(&attachment.key, BrokerMessage {
                        topic: down, payload: command.bytes.to_vec(), qos, retain: false,
                    })?;
                    services.router.transport_state(DeliveryState::Sent);
                }
                frame = attachment.receiver.recv() => {
                    let Some(frame) = frame else { break };
                    send_broker_frame(&mut stream, &services, frame).await?;
                }
                packet = next(&mut reader, &mut stream, limits, idle) => {
                    let (packet, validation_us, validated_at) = packet?;
                    if !matches!(&packet, Packet::Publish { .. }) {
                        let (wait, hold) = services.protocol_admission.check_rate(&auth.device_key)?;
                        services.ingress.metrics.observe(Histogram::AdmissionLockWait, wait);
                        services.ingress.metrics.observe(Histogram::AdmissionLockHold, hold);
                    }
                    last = Instant::now();
                    services.ingress.metrics.inc(Metric::MqttPacketsReceived);
                    match packet {
                        Packet::Connect(_) | Packet::UnsupportedVersion(_) | Packet::Connack { .. }
                        | Packet::Suback { .. } | Packet::Unsuback(_) | Packet::Pingresp => return Err(Error::Invalid),
                        Packet::Pingreq => send(&mut stream, &services, &[0xd0, 0]).await?,
                        Packet::Disconnect => { normal_disconnect = true; break }
                        Packet::Subscribe { packet_id, filters } => {
                            let mut body = packet_id.to_be_bytes().to_vec();
                            for (filter, qos) in filters {
                                let granted = if subscribe_acl(&auth, &filter, limits) {
                                    match services.mqtt.subscribe(&attachment.key, attachment.generation, &filter, qos) {
                                        Ok(qos) => { services.ingress.metrics.inc(Metric::MqttSubscriptions); qos }
                                        Err(_) => 0x80,
                                    }
                                } else { 0x80 };
                                body.push(granted);
                            }
                            send(&mut stream, &services, &encode(0x90, &body, limits.max_mqtt_packet_size)?).await?;
                        }
                        Packet::Unsubscribe { packet_id, filters } => {
                            for filter in filters { services.mqtt.unsubscribe(&attachment.key, attachment.generation, &filter)? }
                            send(&mut stream, &services, &ack(0xb0, packet_id)).await?;
                        }
                        Packet::Puback(id) => {
                            services.mqtt.puback(&attachment.key, attachment.generation, id)?;
                            services.router.transport_state(DeliveryState::Received);
                            services.ingress.metrics.inc(Metric::MqttPubacks);
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_broker_frame(&mut stream, &services, frame).await?;
                            }
                        }
                        Packet::Pubrec(id) => {
                            let frame = services.mqtt.pubrec(&attachment.key, attachment.generation, id)?;
                            send_broker_frame(&mut stream, &services, frame).await?;
                        }
                        Packet::Pubcomp(id) => {
                            services.mqtt.pubcomp(&attachment.key, attachment.generation, id)?;
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_broker_frame(&mut stream, &services, frame).await?;
                            }
                        }
                        Packet::Pubrel(id) => {
                            match services.mqtt.begin_inbound_qos2_delivery(
                                &attachment.key,
                                attachment.generation,
                                id,
                            )? {
                                InboundQos2Action::Deliver {
                                    message,
                                    session_incarnation,
                                    operation_id,
                                } => {
                                    if let Err(error) = accept_iot_publish(
                                        &services,
                                        &auth,
                                        &message,
                                        validated_at,
                                        validation_us,
                                    ).await {
                                        let _ = services.mqtt.abandon_inbound_qos2_delivery(
                                            &attachment.key,
                                            session_incarnation,
                                            id,
                                            operation_id,
                                        );
                                        return Err(error);
                                    }
                                    services.mqtt.finish_inbound_qos2_delivery(
                                        &attachment.key,
                                        session_incarnation,
                                        id,
                                        operation_id,
                                    )?;
                                    services.mqtt.route_inbound_qos2(
                                        &attachment.key,
                                        session_incarnation,
                                        id,
                                        operation_id,
                                        &auth.device_key,
                                    )?;
                                }
                                InboundQos2Action::EventAccepted {
                                    session_incarnation,
                                    operation_id,
                                } => {
                                    services.mqtt.route_inbound_qos2(
                                        &attachment.key,
                                        session_incarnation,
                                        id,
                                        operation_id,
                                        &auth.device_key,
                                    )?;
                                }
                                InboundQos2Action::DeliveryInProgress => continue,
                                InboundQos2Action::Unknown => {}
                            }
                            send(&mut stream, &services, &ack(0x70, id)).await?;
                        }
                        Packet::Publish { topic, payload, qos, packet_id, retain, dup: _ } => {
                            services.ingress.metrics.inc(Metric::MqttPublishes);
                            let message = BrokerMessage { topic, payload: payload.to_vec(), qos, retain };
                            // QoS2 acknowledges ownership with PUBREC, so authorization must be
                            // complete before storing the transaction or reserving retained state.
                            publish_acl(&auth, &message.topic)?;
                            if qos == 2 {
                                let id = packet_id.ok_or(Error::Invalid)?;
                                services.mqtt.inbound_qos2(&attachment.key, attachment.generation, id, message)?;
                                send(&mut stream, &services, &ack(0x50, id)).await?;
                            } else {
                                let _acceptance = process_publish(&services, &auth, &message, validated_at, validation_us).await?;
                                if let Some(id) = packet_id {
                                    let started = Instant::now();
                                    send(&mut stream, &services, &ack(0x40, id)).await?;
                                    services.ingress.metrics.observe(Histogram::PubackWrite, started.elapsed().as_micros() as u64);
                                    services.ingress.metrics.inc(Metric::MqttPubacks);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    attachment.detach()?;
    if let Some(will) = &mut will_guard {
        if normal_disconnect {
            will.suppress()?;
        } else {
            let message = will.publish()?;
            bind_will(&services, &auth, &message).await;
        }
    }
    machine.transition(ConnectionState::Draining)?;
    machine.transition(ConnectionState::Closed)?;
    if matches!(result, Err(Error::Invalid | Error::Forbidden)) {
        services.ingress.metrics.inc(Metric::MqttProtocolViolations);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn state_rejects_double_connect() {
        let mut state = StateMachine {
            state: ConnectionState::Accepted,
        };
        assert!(state.transition(ConnectionState::Connected).is_err());
        state.transition(ConnectionState::AwaitConnect).unwrap();
        state.transition(ConnectionState::Authenticating).unwrap();
        state.transition(ConnectionState::Connected).unwrap();
        assert!(state.transition(ConnectionState::Authenticating).is_err());
    }
}
