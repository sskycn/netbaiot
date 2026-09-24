use super::*;
use broker::PublishProperties;
use codec::v5::{self, Packet};

fn authorize_inbound_qos2_publish(
    mqtt: &broker::MqttBroker,
    auth: &AuthenticatedDevice,
    key: &broker::SessionKey,
    generation: u64,
    packet_id: u16,
    topic: &str,
) -> Result<broker::InboundQos2PublishState> {
    let state = mqtt.classify_inbound_qos2_publish(key, generation, packet_id)?;
    if state == broker::InboundQos2PublishState::NeedsNewMessageAdmission {
        publish_acl(auth, topic)?;
    }
    Ok(state)
}

async fn next_v5(
    reader: &mut Reader,
    stream: &mut BoxStream,
    limits: &Limits,
    idle: Instant,
) -> std::result::Result<(Packet, u64, Instant), (Error, v5::DisconnectReason)> {
    loop {
        let started = Instant::now();
        match v5::decode(&mut reader.buffer, limits) {
            Ok(Some(packet)) => {
                let elapsed = started.elapsed().as_micros() as u64;
                let validated = Instant::now();
                reader.consumed();
                return Ok((packet, elapsed, validated));
            }
            Ok(None) => {}
            Err(error) => {
                return Err((
                    Error::Invalid,
                    v5::DisconnectReason::from_decode(error.reason),
                ));
            }
        }
        reader.read_more(stream, idle).await.map_err(|error| {
            let reason = v5::disconnect_reason(&error);
            (error, reason)
        })?;
    }
}

async fn send_v5(
    stream: &mut BoxStream,
    services: &Services,
    bytes: &[u8],
    maximum: usize,
    disconnected: Option<&mut bool>,
) -> Result<()> {
    if bytes.len() > maximum {
        if let Ok(reply) = v5::disconnect(v5::DisconnectReason::PacketTooLarge, maximum) {
            if let Some(disconnected) = disconnected {
                *disconnected = true;
            }
            let _ = send(stream, services, &reply).await;
        }
        return Err(Error::Overloaded);
    }
    send(stream, services, bytes).await
}

async fn outbound_ack_result<T>(
    result: Result<T>,
    stream: &mut BoxStream,
    services: &Services,
    maximum: usize,
    disconnected: &mut bool,
) -> Result<T> {
    if matches!(result, Err(Error::Invalid))
        && let Ok(bytes) = v5::disconnect(v5::DisconnectReason::ProtocolError, maximum)
    {
        *disconnected = true;
        let _ = send_v5(stream, services, &bytes, maximum, Some(disconnected)).await;
    }
    result
}

async fn fail_with_reason(
    stream: &mut BoxStream,
    services: &Services,
    maximum: usize,
    disconnected: &mut bool,
    reason: v5::DisconnectReason,
    error: Error,
) -> Result<()> {
    if let Ok(bytes) = v5::disconnect(reason, maximum) {
        *disconnected = true;
        let _ = send_v5(stream, services, &bytes, maximum, Some(disconnected)).await;
    }
    Err(error)
}

async fn send_frame(
    stream: &mut BoxStream,
    services: &Services,
    key: &broker::SessionKey,
    generation: u64,
    frame: BrokerFrame,
    maximum: usize,
    disconnected: &mut bool,
) -> Result<()> {
    let mut pending = Some(frame);
    while let Some(frame) = pending.take() {
        let bytes = match frame {
            BrokerFrame::Publish(delivery) => {
                if delivery.packet_id.is_none() && delivery.message.expired(now_ms()) {
                    return Ok(());
                }
                if delivery.packet_id.is_some()
                    && !services
                        .mqtt
                        .begin_outbound_transfer(key, generation, &delivery)?
                {
                    pending = services.mqtt.next_offline(key, generation)?;
                    continue;
                }
                let properties = &delivery.message.properties;
                let mut wire = v5::Properties {
                    payload_format: properties.payload_format,
                    content_type: properties.content_type.clone(),
                    response_topic: properties.response_topic.clone(),
                    correlation_data: properties
                        .correlation_data
                        .as_ref()
                        .map(|data| data.clone().into()),
                    user_properties: properties.user_properties.clone(),
                    ..Default::default()
                };
                if let Some(expiry) = properties.expires_at_ms {
                    let remaining = expiry.saturating_sub(now_ms());
                    wire.message_expiry =
                        Some(u32::try_from((remaining.max(0) + 999) / 1_000).unwrap_or(u32::MAX));
                }
                match v5::publish(
                    v5::OutboundPublish {
                        topic: &delivery.message.topic,
                        payload: &delivery.message.payload,
                        qos: delivery.message.qos,
                        packet_id: delivery.packet_id,
                        retain: delivery.message.retain,
                        dup: delivery.dup,
                        properties: &wire,
                    },
                    maximum.min(services.ingress.limits.max_mqtt_packet_size),
                ) {
                    Ok(bytes) => bytes,
                    Err(Error::Overloaded) => {
                        if delivery.packet_id.is_some() {
                            services.mqtt.discard_outbound(key, generation, &delivery)?;
                            pending = services.mqtt.next_offline(key, generation)?;
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            BrokerFrame::Pubrel { packet_id, .. } => v5::ack(
                v5::AckReason::Pubrel(v5::PubrelReason::Success),
                packet_id,
                maximum,
            )?,
        };
        send_v5(stream, services, &bytes, maximum, Some(disconnected)).await?;
    }
    Ok(())
}

async fn reject_connect(
    stream: &mut BoxStream,
    services: &Services,
    reason: v5::ConnackReason,
    client_maximum: usize,
) -> Result<()> {
    match v5::connack(
        false,
        reason,
        &services.ingress.limits,
        None,
        client_maximum,
    ) {
        Ok(bytes) => send(stream, services, &bytes).await,
        // A legal CONNACK needs five bytes. A peer advertising less cannot
        // receive even a failure CONNACK, so the connection closes silently.
        Err(Error::Overloaded) => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) async fn connection(
    mut stream: BoxStream,
    services: Arc<Services>,
    mut connection: ConnectionLease,
    stop: CancellationToken,
    mut reader: Reader,
) -> Result<()> {
    let limits = &services.ingress.limits;
    let (first, _, _) = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        packet = next_v5(&mut reader, &mut stream, limits, connection.connect_deadline()) =>
            match packet { Ok(value) => value, Err((error, _)) => return Err(error) },
    };
    services.ingress.metrics.inc(Metric::MqttPacketsReceived);
    let Packet::Connect(mut connect) = first else {
        return Err(Error::Invalid);
    };
    let client_maximum = connect.properties.maximum_packet_size.unwrap_or(u32::MAX) as usize;
    if connect.client_id.is_empty() && !connect.clean_start {
        reject_connect(
            &mut stream,
            &services,
            v5::ConnackReason::ClientIdentifierInvalid,
            client_maximum,
        )
        .await?;
        return Err(Error::Invalid);
    }
    if connect.properties.authentication_method.is_some() {
        reject_connect(
            &mut stream,
            &services,
            v5::ConnackReason::BadAuthenticationMethod,
            client_maximum,
        )
        .await?;
        return Err(Error::Authentication);
    }
    let Some(username) = connect.username.as_deref() else {
        reject_connect(
            &mut stream,
            &services,
            v5::ConnackReason::BadCredentials,
            client_maximum,
        )
        .await?;
        return Err(Error::Authentication);
    };
    let Some(password) = connect.password.as_deref() else {
        reject_connect(
            &mut stream,
            &services,
            v5::ConnackReason::BadCredentials,
            client_maximum,
        )
        .await?;
        return Err(Error::Authentication);
    };
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
            reject_connect(
                &mut stream,
                &services,
                v5::connect_reason(&error),
                client_maximum,
            )
            .await?;
            return Err(error);
        }
    };
    if let Some(will) = &connect.will
        && publish_acl(&candidate.auth, &will.topic).is_err()
    {
        reject_connect(
            &mut stream,
            &services,
            v5::ConnackReason::NotAuthorized,
            client_maximum,
        )
        .await?;
        return Err(Error::Forbidden);
    }
    if let Err(error) = connection.authenticate(&candidate.auth.device_key) {
        reject_connect(
            &mut stream,
            &services,
            v5::connect_reason(&error),
            client_maximum,
        )
        .await?;
        return Err(error);
    }
    let auth = Arc::new(candidate.auth.clone());
    let requested_client_id = connect.client_id.clone();
    let assigned = requested_client_id.is_empty();
    let clean_start = connect.clean_start;
    let session_expiry = connect.properties.session_expiry.unwrap_or(0);
    let client_receive_maximum = connect.properties.receive_maximum.unwrap_or(u16::MAX);
    let mqtt = services.mqtt.clone();
    let (live_session, mut commands, (mut attachment, client_id)) = services
        .ingress
        .register_session_with(candidate, Transport::Mqtt, move |bound_auth, generation| {
            let client_id = if requested_client_id.is_empty() {
                format!("generated-{generation}")
            } else {
                requested_client_id
            };
            v5::connack(
                false,
                v5::ConnackReason::Success,
                limits,
                assigned.then_some(client_id.as_str()),
                client_maximum,
            )?;
            let attachment = mqtt.attach_v5(
                bound_auth,
                client_id.clone(),
                clean_start,
                session_expiry,
                client_receive_maximum,
            )?;
            Ok((attachment, client_id))
        })?;
    connect.client_id = client_id.clone();
    let mut will_guard = connect
        .will
        .take()
        .map(|will| {
            let delay = will.properties.will_delay.unwrap_or(0);
            let message_expiry = will.properties.message_expiry;
            let mut properties = PublishProperties::from_wire(&will.properties);
            // Keep the expiry metadata charged from CONNECT. The absolute deadline starts
            // only when the Will is published after its delay.
            properties.expires_at_ms = message_expiry.map(|_| i64::MAX);
            let mut guard = services.mqtt.reserve_will(
                auth.device_key.clone(),
                BrokerMessage {
                    topic: will.topic,
                    payload: will.payload.to_vec(),
                    qos: will.qos,
                    retain: will.retain,
                    properties,
                },
            )?;
            guard.arm_v5(
                attachment.key.clone(),
                attachment.session_incarnation,
                attachment.generation,
                delay,
                session_expiry,
                message_expiry,
            );
            Ok::<_, Error>(guard)
        })
        .transpose()?;
    let response = v5::connack(
        attachment.session_present,
        v5::ConnackReason::Success,
        limits,
        assigned.then_some(client_id.as_str()),
        client_maximum,
    )?;
    send_v5(&mut stream, &services, &response, client_maximum, None).await?;
    services.ingress.metrics.inc(Metric::MqttConnectSuccess);
    let keepalive = if connect.keep_alive == 0 {
        None
    } else {
        Some(Duration::from_millis(u64::from(connect.keep_alive) * 1_500))
    };
    let mut last = Instant::now();
    let mut suppress_will = false;
    let mut error_disconnect_sent = false;
    let result = async {
        loop {
            let idle = last + keepalive.unwrap_or(Duration::from_millis(limits.idle_timeout_ms));
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = attachment.cancel.cancelled() => {
                    let bytes = v5::disconnect(v5::DisconnectReason::SessionTakenOver, client_maximum)?;
                    let _ = send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await;
                    break;
                }
                _ = live_session.cancel.cancelled() => break,
                _ = tokio::time::sleep_until(idle) => return Err(Error::Timeout),
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    if command.expires_at <= now_ms() { continue; }
                    let down = topic(&auth.device_key, TopicKind::Down);
                    let qos = services.mqtt.subscription_qos(&attachment.key, &down)?.unwrap_or(1);
                    services.mqtt.send_live(&attachment.key, BrokerMessage {
                        topic: down, payload: command.bytes.to_vec(), qos, retain: false,
                        properties: PublishProperties {
                            expires_at_ms: Some(command.expires_at),
                            ..Default::default()
                        },
                    })?;
                    services.router.transport_state(DeliveryState::Sent);
                }
                frame = attachment.receiver.recv() => {
                    let Some(frame) = frame else { break };
                    send_frame(&mut stream, &services, &attachment.key, attachment.generation, frame, client_maximum, &mut error_disconnect_sent).await?;
                }
                packet = next_v5(&mut reader, &mut stream, limits, idle) => {
                    let (packet, validation_us, validated_at) = match packet {
                        Ok(packet) => packet,
                        Err((error, reason)) => {
                            if let Ok(bytes) = v5::disconnect(reason, client_maximum) {
                                error_disconnect_sent = true;
                                let _ = send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await;
                            }
                            return Err(error);
                        }
                    };
                    if !matches!(&packet, Packet::Publish { .. }) {
                        let (wait, hold) = services.protocol_admission.check_rate(&auth.device_key)?;
                        services.ingress.metrics.observe(Histogram::AdmissionLockWait, wait);
                        services.ingress.metrics.observe(Histogram::AdmissionLockHold, hold);
                    }
                    last = Instant::now();
                    services.ingress.metrics.inc(Metric::MqttPacketsReceived);
                    match packet {
                        Packet::Connect(_) => return Err(Error::Invalid),
                        Packet::Pingreq => send_v5(&mut stream, &services, &[0xd0, 0], client_maximum, Some(&mut error_disconnect_sent)).await?,
                        Packet::Disconnect { reason, properties } => {
                            if let Some(interval) = properties.session_expiry {
                                services.mqtt.set_v5_disconnect_expiry(&attachment.key, attachment.generation, interval)?;
                                if let Some(will) = &mut will_guard { will.set_v5_session_expiry(interval); }
                            }
                            suppress_will = reason == 0;
                            break;
                        }
                        Packet::Subscribe { packet_id, filters, .. } => {
                            let mut reasons = Vec::with_capacity(filters.len());
                            for (filter, options) in filters {
                                let reason = if subscribe_acl(&auth, &filter, limits) {
                                    match services.mqtt.subscribe_v5(&attachment.key, attachment.generation, &filter, options) {
                                        Ok(qos) => { services.ingress.metrics.inc(Metric::MqttSubscriptions); v5::SubackReason::granted(qos)? }
                                        Err(error) => v5::subscription_reason(&error),
                                    }
                                } else { v5::SubackReason::NotAuthorized };
                                reasons.push(reason);
                            }
                            let bytes = v5::suback(packet_id, &reasons, limits.max_mqtt_packet_size)?;
                            send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                        }
                        Packet::Unsubscribe { packet_id, filters, .. } => {
                            let mut reasons = Vec::with_capacity(filters.len());
                            for filter in filters {
                                reasons.push(if services.mqtt.unsubscribe(&attachment.key, attachment.generation, &filter)? { v5::UnsubackReason::Success } else { v5::UnsubackReason::NoSubscriptionExisted });
                            }
                            let bytes = v5::unsuback(packet_id, &reasons, limits.max_mqtt_packet_size)?;
                            send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                        }
                        Packet::Puback { packet_id, reason: _ } => {
                            outbound_ack_result(services.mqtt.puback(&attachment.key, attachment.generation, packet_id), &mut stream, &services, client_maximum, &mut error_disconnect_sent).await?;
                            services.router.transport_state(DeliveryState::Received);
                            services.ingress.metrics.inc(Metric::MqttPubacks);
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_frame(&mut stream, &services, &attachment.key, attachment.generation, frame, client_maximum, &mut error_disconnect_sent).await?;
                            }
                        }
                        Packet::Pubrec { packet_id, reason } => {
                            if reason >= 0x80 {
                                let result = services.mqtt.pubrec_rejected(&attachment.key, attachment.generation, packet_id);
                                if matches!(result, Err(Error::Invalid)) {
                                    let bytes = v5::ack(v5::AckReason::Pubrel(v5::PubrelReason::PacketIdentifierNotFound), packet_id, limits.max_mqtt_packet_size)?;
                                    send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                                    continue;
                                }
                                result?;
                            } else {
                                let result = services.mqtt.pubrec(&attachment.key, attachment.generation, packet_id);
                                if matches!(result, Err(Error::Invalid)) {
                                    let bytes = v5::ack(v5::AckReason::Pubrel(v5::PubrelReason::PacketIdentifierNotFound), packet_id, limits.max_mqtt_packet_size)?;
                                    send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                                    continue;
                                }
                                let frame = result?;
                                send_frame(&mut stream, &services, &attachment.key, attachment.generation, frame, client_maximum, &mut error_disconnect_sent).await?;
                            }
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_frame(&mut stream, &services, &attachment.key, attachment.generation, frame, client_maximum, &mut error_disconnect_sent).await?;
                            }
                        }
                        Packet::Pubcomp { packet_id, reason: _ } => {
                            outbound_ack_result(services.mqtt.pubcomp(&attachment.key, attachment.generation, packet_id), &mut stream, &services, client_maximum, &mut error_disconnect_sent).await?;
                            if let Some(frame) = services.mqtt.next_offline(&attachment.key, attachment.generation)? {
                                send_frame(&mut stream, &services, &attachment.key, attachment.generation, frame, client_maximum, &mut error_disconnect_sent).await?;
                            }
                        }
                        Packet::Pubrel { packet_id, .. } => {
                            let unknown = match services.mqtt.begin_inbound_qos2_delivery(&attachment.key, attachment.generation, packet_id)? {
                                InboundQos2Action::Deliver { message, session_incarnation, operation_id } => {
                                    if let Err(error) = accept_iot_publish(&services, &auth, &message, validated_at, validation_us).await {
                                        let _ = services.mqtt.abandon_inbound_qos2_delivery(&attachment.key, session_incarnation, packet_id, operation_id);
                                        return Err(error);
                                    }
                                    services.mqtt.finish_inbound_qos2_delivery(&attachment.key, session_incarnation, packet_id, operation_id)?;
                                    services.mqtt.route_inbound_qos2(&attachment.key, session_incarnation, packet_id, operation_id, &auth.device_key)?;
                                    false
                                }
                                InboundQos2Action::EventAccepted { session_incarnation, operation_id } => {
                                    services.mqtt.route_inbound_qos2(&attachment.key, session_incarnation, packet_id, operation_id, &auth.device_key)?;
                                    false
                                }
                                InboundQos2Action::DeliveryInProgress => continue,
                                InboundQos2Action::Unknown => true,
                            };
                            let bytes = v5::ack(v5::AckReason::Pubcomp(if unknown { v5::PubcompReason::PacketIdentifierNotFound } else { v5::PubcompReason::Success }), packet_id, limits.max_mqtt_packet_size)?;
                            send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                            services.mqtt.finish_inbound_pubcomp(&attachment.key, attachment.generation, packet_id)?;
                        }
                        Packet::Publish { topic, payload, qos, packet_id, retain, dup: _, properties } => {
                            services.ingress.metrics.inc(Metric::MqttPublishes);
                            if qos == 2 {
                                let id = packet_id.ok_or(Error::Invalid)?;
                                if authorize_inbound_qos2_publish(&services.mqtt, &auth, &attachment.key, attachment.generation, id, &topic)?
                                    == broker::InboundQos2PublishState::ExistingTransaction {
                                    let bytes = v5::ack(v5::AckReason::Pubrec(v5::PubrecReason::Success), id, limits.max_mqtt_packet_size)?;
                                    send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                                    continue;
                                }
                            } else {
                                publish_acl(&auth, &topic)?;
                            }
                            let message = BrokerMessage { topic, payload: payload.to_vec(), qos, retain,
                                properties: PublishProperties::from_wire(&properties) };
                            if qos > 0 && !services.mqtt.inbound_receive_available(&attachment.key, attachment.generation, qos, packet_id.ok_or(Error::Invalid)?)? {
                                fail_with_reason(&mut stream, &services, client_maximum, &mut error_disconnect_sent, v5::DisconnectReason::ReceiveMaximumExceeded, Error::Overloaded).await?;
                            }
                            if qos == 2 {
                                let id = packet_id.ok_or(Error::Invalid)?;
                                match services.mqtt.inbound_qos2(&attachment.key, attachment.generation, id, message) {
                                    Ok(_) => {}
                                    Err(Error::Invalid) => {
                                        let bytes = v5::ack(v5::AckReason::Pubrec(v5::PubrecReason::PacketIdentifierInUse), id, limits.max_mqtt_packet_size)?;
                                        send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                                        continue;
                                    }
                                    Err(error) => return Err(error),
                                }
                                let bytes = v5::ack(v5::AckReason::Pubrec(v5::PubrecReason::Success), id, limits.max_mqtt_packet_size)?;
                                send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
                            } else {
                                let _acceptance = process_publish(&services, &auth, &attachment.key, &message, validated_at, validation_us).await?;
                                if let Some(id) = packet_id {
                                    let started = Instant::now();
                                    let bytes = v5::ack(v5::AckReason::Puback(v5::PubackReason::Success), id, limits.max_mqtt_packet_size)?;
                                    send_v5(&mut stream, &services, &bytes, client_maximum, Some(&mut error_disconnect_sent)).await?;
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
    }.await;
    if let Err(error) = &result
        && !error_disconnect_sent
        && let Ok(bytes) = v5::disconnect(v5::disconnect_reason(error), client_maximum)
    {
        let _ = send_v5(&mut stream, &services, &bytes, client_maximum, None).await;
    }
    attachment.detach()?;
    if let Some(will) = &mut will_guard {
        if suppress_will {
            will.suppress()?;
        } else if let Some(message) = will.publish_v5()? {
            bind_will(&services, &auth, &message).await;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_qos2_only_skips_duplicate_acl_until_identifier_is_released() {
        let auth = AuthenticatedDevice {
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
                commands: false,
            },
        };
        let mqtt = broker::MqttBroker::new(Arc::new(Limits::default()));
        let mut attachment = mqtt
            .attach_v5(&auth, "client".into(), false, 60, 2)
            .unwrap();
        let forbidden = "v1/t/t/p/p/d/another/up";
        let allowed = "v1/t/t/p/p/d/d/up";
        assert!(matches!(
            authorize_inbound_qos2_publish(
                &mqtt,
                &auth,
                &attachment.key,
                attachment.generation,
                7,
                forbidden
            ),
            Err(Error::Forbidden)
        ));
        let original = broker::BrokerMessage {
            topic: allowed.into(),
            payload: b"first".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        assert!(
            mqtt.inbound_qos2(&attachment.key, attachment.generation, 7, original)
                .unwrap()
        );
        assert_eq!(
            authorize_inbound_qos2_publish(
                &mqtt,
                &auth,
                &attachment.key,
                attachment.generation,
                7,
                forbidden
            )
            .unwrap(),
            broker::InboundQos2PublishState::ExistingTransaction
        );
        assert!(matches!(
            authorize_inbound_qos2_publish(
                &mqtt,
                &auth,
                &attachment.key,
                attachment.generation,
                8,
                forbidden
            ),
            Err(Error::Forbidden)
        ));
        let broker::InboundQos2Action::Deliver {
            session_incarnation,
            operation_id,
            ..
        } = mqtt
            .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
            .unwrap()
        else {
            panic!("PUBREL must start original delivery")
        };
        assert!(matches!(
            authorize_inbound_qos2_publish(
                &mqtt,
                &auth,
                &attachment.key,
                attachment.generation,
                7,
                forbidden
            ),
            Err(Error::Forbidden)
        ));
        mqtt.finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 7, operation_id)
            .unwrap();
        mqtt.complete_inbound_qos2(&attachment.key, attachment.generation, 7)
            .unwrap();
        mqtt.finish_inbound_pubcomp(&attachment.key, attachment.generation, 7)
            .unwrap();
        assert!(matches!(
            authorize_inbound_qos2_publish(
                &mqtt,
                &auth,
                &attachment.key,
                attachment.generation,
                7,
                forbidden
            ),
            Err(Error::Forbidden)
        ));
        attachment.detach().unwrap();
    }
}
