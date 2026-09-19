pub mod packet;
pub mod topics;
use crate::common::*;
use netbaiot_core::*;
use netbaiot_runtime::*;
use packet::*;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use topics::*;
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
struct Pending {
    command: Option<QueuedCommand>,
    _ack_bytes: Option<BytesPermit>,
    sent_at: Instant,
}
struct PacketIds {
    next: u16,
    entries: HashMap<u16, Pending>,
    maximum: usize,
}
impl PacketIds {
    fn new(maximum: usize) -> Self {
        Self {
            next: 1,
            entries: HashMap::new(),
            maximum,
        }
    }
    fn allocate(&mut self, pending: Pending) -> Result<u16> {
        if self.entries.len() >= self.maximum {
            return Err(Error::Overloaded);
        }
        for _ in 0..u16::MAX {
            let id = self.next;
            self.next = if id == u16::MAX { 1 } else { id + 1 };
            if let std::collections::hash_map::Entry::Vacant(e) = self.entries.entry(id) {
                e.insert(pending);
                return Ok(id);
            }
        }
        Err(Error::Overloaded)
    }
    fn deadline(&self, timeout: Duration) -> Option<Instant> {
        self.entries.values().map(|p| p.sent_at + timeout).min()
    }
}
async fn next(
    reader: &mut Reader,
    stream: &mut BoxStream,
    l: &Limits,
    idle: Instant,
) -> Result<Packet> {
    loop {
        if let Some(packet) = decode(&mut reader.buffer, l)? {
            reader.consumed();
            return Ok(packet);
        }
        reader.read_more(stream, idle).await?;
    }
}
async fn send(stream: &mut BoxStream, s: &Services, bytes: &[u8]) -> Result<()> {
    write(stream, bytes, s.ingress.limits.write_timeout_ms).await?;
    s.ingress.metrics.inc(Metric::MqttPacketsSent);
    Ok(())
}
pub async fn connection(
    mut stream: BoxStream,
    s: Arc<Services>,
    mut connection: ConnectionLease,
    stop: CancellationToken,
) -> Result<()> {
    let l = &s.ingress.limits;
    let mut machine = StateMachine {
        state: ConnectionState::Accepted,
    };
    machine.transition(ConnectionState::AwaitConnect)?;
    let mut reader = Reader::new(l.max_mqtt_packet_size, l.packet_read_timeout_ms);
    let first = tokio::select! {_=stop.cancelled()=>return Ok(()),packet=next(&mut reader,&mut stream,l,Instant::now()+Duration::from_millis(l.connect_timeout_ms))=>packet?};
    s.ingress.metrics.inc(Metric::MqttPacketsReceived);
    let connect = match first {
        Packet::Connect(c) => c,
        Packet::UnsupportedVersion(v) => {
            if v == 5 {
                send(&mut stream, &s, &[0x20, 3, 0, 0x84, 0]).await?;
            } else {
                send(&mut stream, &s, &connack(1)).await?;
            }
            return Err(Error::Invalid);
        }
        _ => {
            s.ingress.metrics.inc(Metric::MqttProtocolViolations);
            return Err(Error::Invalid);
        }
    };
    if !connect.clean_session || connect.has_will {
        send(&mut stream, &s, &connack(5)).await?;
        return Err(Error::Forbidden);
    }
    // One provisioned client ID per device prevents identity alias takeover.
    if connect.client_id.is_empty() || connect.client_id != connect.username {
        send(&mut stream, &s, &connack(2)).await?;
        return Err(Error::Authentication);
    }
    machine.transition(ConnectionState::Authenticating)?;
    let auth = match s
        .ingress
        .authenticate(AuthenticationRequest::Secret {
            credential_id: &connect.username,
            secret: &connect.password,
        })
        .await
    {
        Ok(auth) => auth,
        Err(e) => {
            s.ingress.metrics.inc(Metric::MqttConnectFailure);
            send(&mut stream, &s, &connack(4)).await?;
            return Err(e);
        }
    };
    if let Err(e) = connection.authenticate(&auth.device_key) {
        send(&mut stream, &s, &connack(3)).await?;
        return Err(e);
    }
    let (session, mut outbound) = s
        .ingress
        .sessions
        .register(&auth.device_key, Transport::Mqtt)?;
    let _subscriptions = SubscriptionLease {
        registry: s.subscriptions.clone(),
        device: auth.device_key.clone(),
        generation: session.generation,
    };
    send(&mut stream, &s, &connack(0)).await?;
    s.ingress.metrics.inc(Metric::MqttConnectSuccess);
    machine.transition(ConnectionState::Connected)?;
    let keepalive = if connect.keep_alive == 0 {
        None
    } else {
        Some(Duration::from_millis(u64::from(connect.keep_alive) * 1500))
    };
    let mut last = Instant::now();
    let mut ids = PacketIds::new(l.max_inflight_qos1_per_connection);
    // Receipt QoS1 entries retain only protocol identity, bounded by this budget.
    let receipt_bytes = ByteBudget::new(l.max_outbound_bytes_per_connection);
    let result = async {
        loop {
            let idle = last + keepalive.unwrap_or(Duration::from_millis(l.idle_timeout_ms));
            let protocol_deadline = ids.deadline(Duration::from_millis(l.idle_timeout_ms))
                .unwrap_or(idle).min(idle);
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                _ = session.cancel.cancelled() => break,
                _ = tokio::time::sleep_until(protocol_deadline) => {
                    if keepalive.is_some() && Instant::now() >= idle {
                        s.ingress.metrics.inc(Metric::MqttKeepaliveDisconnects);
                    }
                    return Err(Error::Timeout);
                }
                item = outbound.recv() => {
                    let Some(item) = item else { break };
                    if item.expires_at <= now_ms() { continue; }
                    let down = topic(&auth.device_key, TopicKind::Down);
                    let Some(qos) = s.subscriptions.lookup(&down, session.generation)? else {
                        // The durable command lease permits a later retry after SUBSCRIBE.
                        continue;
                    };
                    let command_id = item.command_id;
                    if qos == 1 {
                        let id = ids.allocate(Pending {
                            command: Some(item), _ack_bytes: None, sent_at: Instant::now(),
                        })?;
                        let pending = ids.entries.get(&id)
                            .and_then(|p| p.command.as_ref()).ok_or(Error::Internal)?;
                        let frame = publish(&down, &pending.bytes, Some(id), l)?;
                        send(&mut stream, &s, &frame).await?;
                    } else {
                        let frame = publish(&down, &item.bytes, None, l)?;
                        send(&mut stream, &s, &frame).await?;
                    }
                    s.router.state(&auth.device_key, command_id, DeliveryState::Sent).await?;
                }
                packet = next(&mut reader, &mut stream, l, protocol_deadline) => {
                    let packet = packet?;
                    let _protocol = s.protocol_admission.acquire(&auth.device_key, 1)?;
                    last = Instant::now();
                    s.ingress.metrics.inc(Metric::MqttPacketsReceived);
                    match packet {
                        Packet::Connect(_) | Packet::UnsupportedVersion(_) => return Err(Error::Invalid),
                        Packet::Pingreq => send(&mut stream, &s, &[0xd0, 0]).await?,
                        Packet::Disconnect => break,
                        Packet::Subscribe { packet_id, filters } => {
                            let mut body = packet_id.to_be_bytes().to_vec();
                            for (filter, qos) in filters {
                                let granted = match s.subscriptions.subscribe(&auth, session.generation, &filter, qos) {
                                    Ok(q) => { s.ingress.metrics.inc(Metric::MqttSubscriptions); q }
                                    Err(_) => 0x80,
                                };
                                body.push(granted);
                            }
                            send(&mut stream, &s, &encode(0x90, &body, l.max_mqtt_packet_size)?).await?;
                        }
                        Packet::Unsubscribe { packet_id, filters } => {
                            for filter in filters {
                                s.subscriptions.unsubscribe(&filter, session.generation)?;
                            }
                            send(&mut stream, &s, &ack(0xb0, packet_id)).await?;
                        }
                        Packet::Puback(id) => {
                            s.ingress.metrics.inc(Metric::MqttPubacks);
                            if let Some(pending) = ids.entries.remove(&id)
                                && let Some(command) = pending.command {
                                s.router.state(&auth.device_key, command.command_id, DeliveryState::Received).await?;
                            }
                        }
                        Packet::Publish { topic: requested, payload, packet_id, retain, .. } => {
                            if retain { return Err(Error::Forbidden); }
                            let kind = publish_acl(&auth, &requested)?;
                            s.ingress.metrics.inc(Metric::MqttPublishes);
                            let receipt = s.ingress.ingest(&auth, IngressEnvelope {
                                transport: Transport::Mqtt,
                                payload: &payload,
                                require_command_ack: kind == TopicKind::DownAck,
                            }).await?;
                            if let Some(id) = packet_id {
                                send(&mut stream, &s, &ack(0x40, id)).await?;
                                s.ingress.metrics.inc(Metric::MqttPubacks);
                            }
                            let up_ack = topic(&auth.device_key, TopicKind::UpAck);
                            if let Some(qos) = s.subscriptions.lookup(&up_ack, session.generation)? {
                                let bytes = serde_json::to_vec(&receipt).map_err(|_| Error::Internal)?;
                                let id = if qos == 1 {
                                    Some(ids.allocate(Pending {
                                        command: None,
                                        _ack_bytes: Some(receipt_bytes.reserve(bytes.len())?),
                                        sent_at: Instant::now(),
                                    })?)
                                } else { None };
                                send(&mut stream, &s, &publish(&up_ack, &bytes, id, l)?).await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }.await;
    machine.transition(ConnectionState::Draining)?;
    machine.transition(ConnectionState::Closed)?;
    if matches!(result, Err(Error::Invalid | Error::Forbidden)) {
        s.ingress.metrics.inc(Metric::MqttProtocolViolations);
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packet_ids_reuse_only_after_ack() {
        let mut ids = PacketIds::new(1);
        let pending = || Pending {
            command: None,
            _ack_bytes: None,
            sent_at: Instant::now(),
        };
        let first = ids.allocate(pending()).unwrap();
        assert!(ids.allocate(pending()).is_err());
        ids.entries.remove(&first);
        assert!(ids.allocate(pending()).is_ok());
    }
    #[test]
    fn state_rejects_double_connect() {
        let mut s = StateMachine {
            state: ConnectionState::Accepted,
        };
        assert!(s.transition(ConnectionState::Connected).is_err());
        s.transition(ConnectionState::AwaitConnect).unwrap();
        s.transition(ConnectionState::Authenticating).unwrap();
        s.transition(ConnectionState::Connected).unwrap();
        assert!(s.transition(ConnectionState::Authenticating).is_err());
    }
}
