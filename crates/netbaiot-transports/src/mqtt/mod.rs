pub mod broker;
pub mod codec;
pub mod packet;
pub mod topics;
mod v5_connection;

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
    key: &broker::SessionKey,
    generation: u64,
    frame: BrokerFrame,
) -> Result<()> {
    let mut pending = Some(frame);
    while let Some(frame) = pending.take() {
        let (bytes, budget, command) = match frame {
            BrokerFrame::Publish(delivery) => {
                if delivery.packet_id.is_none() && delivery.message.expired(now_ms()) {
                    return Ok(());
                }
                let bytes = publish(
                    &delivery.message.topic,
                    &delivery.message.payload,
                    delivery.message.qos,
                    delivery.packet_id,
                    delivery.message.retain,
                    delivery.dup,
                    &services.ingress.limits,
                )?;
                if delivery.packet_id.is_some()
                    && !services
                        .mqtt
                        .begin_outbound_transfer(key, generation, &delivery)?
                {
                    pending = services.mqtt.next_offline(key, generation)?;
                    continue;
                }
                (bytes, delivery._budget, delivery.command)
            }
            // PUBREL's MQTT 3.1.1 fixed-header flags are always 0010.
            BrokerFrame::Pubrel { packet_id, dup: _ } => (ack(0x62, packet_id), Vec::new(), false),
        };
        let sent = send(stream, services, &bytes).await;
        let charged = !budget.is_empty();
        drop(budget);
        if charged {
            services.mqtt.outbound_bytes_released()?;
        }
        sent?;
        if command {
            services.router.transport_state(DeliveryState::Sent);
        }
    }
    Ok(())
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
    origin: &broker::SessionKey,
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
    services.mqtt.route_from_session(origin, message.clone())?;
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
    loop {
        if let Some((first, header, total)) =
            codec::common::fixed_header(&reader.buffer, limits.max_mqtt_packet_size)?
            && reader.buffer.len() >= total
        {
            if first != 0x10 || total < header + 7 {
                return Err(Error::Invalid);
            }
            if &reader.buffer[header..header + 6] == b"\0\x04MQTT" && reader.buffer[header + 6] == 5
            {
                return v5_connection::connection(stream, services, connection, stop, reader).await;
            }
            break;
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            result = reader.read_more(&mut stream, connection.connect_deadline()) => result?,
        }
    }
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
    let (live_session, mut commands, (mut attachment, client_id)) =
        services.ingress.register_session_with(
            candidate,
            Transport::Mqtt,
            move |bound_auth, _generation| {
                let attachment = if requested_client_id.is_empty() {
                    mqtt.attach_generated(bound_auth)?
                } else {
                    mqtt.attach(bound_auth, requested_client_id, clean_session)?
                };
                let client_id = attachment.key.client_id.clone();
                Ok((attachment, client_id))
            },
        )?;
    connect.client_id = client_id;
    let mut will_guard = connect
        .will
        .take()
        .map(|will| {
            services.mqtt.reserve_will_for_session(
                attachment.key.clone(),
                BrokerMessage {
                    topic: will.topic,
                    payload: will.payload.to_vec(),
                    qos: will.qos,
                    retain: will.retain,
                    properties: Default::default(),
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
    let down_topic = topic(&auth.device_key, TopicKind::Down);
    live_session.set_command_ready(
        services
            .mqtt
            .subscription_qos(&attachment.key, &down_topic)?
            .is_some(),
    )?;
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
                    let Some(qos) = services.mqtt.subscription_qos(&attachment.key, &down)? else {
                        services.router.transport_state(DeliveryState::Failed);
                        continue;
                    };
                    if services.mqtt.send_live(&attachment.key, attachment.generation, BrokerMessage {
                        topic: down, payload: command.bytes.to_vec(), qos, retain: false,
                        properties: Default::default(),
                    }).is_err() {
                        services.router.transport_state(DeliveryState::Failed);
                    }
                }
                frame = attachment.receiver.recv() => {
                    let Some(frame) = frame else { break };
                    send_broker_frame(&mut stream, &services, &attachment.key, attachment.generation, frame).await?;
                }
                packet = next(&mut reader, &mut stream, limits, idle) => {
                    let (packet, validation_us, validated_at) = packet?;
                    let control = matches!(&packet, Packet::Puback(_) | Packet::Pubrec(_)
                        | Packet::Pubrel(_) | Packet::Pubcomp(_) | Packet::Pingreq | Packet::Disconnect);
                    let units = match &packet {
                        Packet::Publish { topic, payload, .. } =>
                            1usize.saturating_add(topic.len().saturating_add(payload.len()) / 4096),
                        _ => 1,
                    };
                    let admission = if control { &services.protocol_control_admission }
                        else { &services.protocol_admission };
                    let (wait, hold) = admission.check_rate_weighted(&auth.device_key, units)?;
                    services.ingress.metrics.observe(Histogram::AdmissionLockWait, wait);
                    services.ingress.metrics.observe(Histogram::AdmissionLockHold, hold);
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
                            live_session.set_command_ready(
                                services.mqtt.subscription_qos(&attachment.key, &down_topic)?.is_some()
                            )?;
                            send(&mut stream, &services, &encode(0x90, &body, limits.max_mqtt_packet_size)?).await?;
                        }
                        Packet::Unsubscribe { packet_id, filters } => {
                            live_session.set_command_ready(false)?;
                            for filter in filters { services.mqtt.unsubscribe(&attachment.key, attachment.generation, &filter)?; }
                            live_session.set_command_ready(
                                services.mqtt.subscription_qos(&attachment.key, &down_topic)?.is_some()
                            )?;
                            send(&mut stream, &services, &ack(0xb0, packet_id)).await?;
                        }
                        Packet::Puback(id) => {
                            if services.mqtt.puback(&attachment.key, attachment.generation, id)? {
                                services.router.transport_state(DeliveryState::Received);
                            }
                            services.ingress.metrics.inc(Metric::MqttPubacks);
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_broker_frame(&mut stream, &services, &attachment.key, attachment.generation, frame).await?;
                            }
                        }
                        Packet::Pubrec(id) => {
                            let frame = services.mqtt.pubrec(&attachment.key, attachment.generation, id)?;
                            send_broker_frame(&mut stream, &services, &attachment.key, attachment.generation, frame).await?;
                        }
                        Packet::Pubcomp(id) => {
                            if services.mqtt.pubcomp(&attachment.key, attachment.generation, id)? {
                                services.router.transport_state(DeliveryState::Received);
                            }
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_broker_frame(&mut stream, &services, &attachment.key, attachment.generation, frame).await?;
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
                            if qos == 2 {
                                let id = packet_id.ok_or(Error::Invalid)?;
                                match services.mqtt.classify_inbound_qos2_publish(
                                    &attachment.key, attachment.generation, id)? {
                                    broker::InboundQos2PublishState::ExistingTransaction => {
                                        send(&mut stream, &services, &ack(0x50, id)).await?;
                                        continue;
                                    }
                                    broker::InboundQos2PublishState::IdentifierInUse => return Err(Error::Invalid),
                                    broker::InboundQos2PublishState::NeedsNewMessageAdmission => {}
                                }
                            }
                            let message = BrokerMessage { topic, payload: payload.to_vec(), qos, retain,
    properties: Default::default(),
};
                            // QoS2 acknowledges ownership with PUBREC, so authorization must be
                            // complete before storing the transaction or reserving retained state.
                            let kind = publish_acl(&auth, &message.topic)?;
                            if qos == 2 {
                                let id = packet_id.ok_or(Error::Invalid)?;
                                if !(message.retain && message.payload.is_empty()) {
                                    services.ingress.validate_mqtt_qos2_payload(
                                        &auth, &message.payload, kind == TopicKind::DownAck)?;
                                }
                                services.mqtt.inbound_qos2(&attachment.key, attachment.generation, id, message)?;
                                send(&mut stream, &services, &ack(0x50, id)).await?;
                            } else {
                                let _acceptance = process_publish(&services, &auth, &attachment.key, &message, validated_at, validation_us).await?;
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
