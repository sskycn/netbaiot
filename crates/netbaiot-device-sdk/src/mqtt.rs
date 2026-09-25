use crate::profile::{
    Admission, CommandEnvelope, DeviceReconnectPolicy, DeviceSdkError, Metrics,
    MqttConnectionState, MqttProtocolVersion, Outbound, PublishQos, PublishReceipt, PublishResult,
    device_reconnect_delay, terminal_connect_error, topic,
};
use bytes::BytesMut;
use netbaiot_mqtt_wire::client::{self as wire, Limits, Packet, Properties, Version};
use netbaiot_protocol::{DeviceCommand, DeviceKey, EventId};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Semaphore, broadcast, mpsc, watch},
    time::Instant,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};
use tokio_util::sync::CancellationToken;

pub(super) struct DriverConfig {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub ca_pem: Option<PathBuf>,
    pub device: DeviceKey,
    pub client_id: String,
    pub username: String,
    pub password: Vec<u8>,
    pub protocol_version: MqttProtocolVersion,
    pub session_expiry: u32,
    pub connect_timeout: Duration,
    pub max_packet_bytes: usize,
    pub max_payload_bytes: usize,
    pub command_bytes: usize,
    pub inflight_items: usize,
    pub reconnect: DeviceReconnectPolicy,
}
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Socket = Box<dyn Io>;
struct Connection {
    io: Socket,
    buffer: BytesMut,
    properties: Properties,
    keep_alive: Duration,
    send_maximum: usize,
    version: Version,
}
struct Inflight {
    outbound: Outbound,
    sent_at: Instant,
    order: u64,
    active: bool,
}

fn protocol(error: netbaiot_mqtt_wire::WireError) -> DeviceSdkError {
    match error {
        netbaiot_mqtt_wire::WireError::Invalid => {
            DeviceSdkError::Protocol("invalid MQTT packet".into())
        }
        netbaiot_mqtt_wire::WireError::TooLarge => DeviceSdkError::Overloaded,
    }
}
fn version(config: &DriverConfig) -> Version {
    match config.protocol_version {
        MqttProtocolVersion::V311 => Version::V311,
        MqttProtocolVersion::V5 => Version::V5,
    }
}
fn limits(config: &DriverConfig) -> Limits {
    Limits {
        packet: config.max_packet_bytes,
        payload: config.max_payload_bytes,
        ..Limits::default()
    }
}
async fn write(io: &mut Socket, bytes: &[u8], deadline: Duration) -> Result<(), DeviceSdkError> {
    tokio::time::timeout(deadline, io.write_all(bytes))
        .await
        .map_err(|_| DeviceSdkError::Timeout)?
        .map_err(|_| DeviceSdkError::Transport("MQTT socket write failed".into()))
}
async fn read_packet(
    connection: &mut Connection,
    limits: Limits,
) -> Result<Packet, DeviceSdkError> {
    loop {
        if let Some(packet) =
            wire::decode(&mut connection.buffer, connection.version, limits).map_err(protocol)?
        {
            if connection.buffer.is_empty() && connection.buffer.capacity() > limits.packet / 2 {
                connection.buffer = BytesMut::with_capacity(4096);
            }
            return Ok(packet);
        }
        if connection.buffer.len() >= limits.packet {
            return Err(DeviceSdkError::Overloaded);
        }
        let mut chunk = [0u8; 4096];
        let size = connection
            .io
            .read(&mut chunk)
            .await
            .map_err(|_| DeviceSdkError::Transport("MQTT socket read failed".into()))?;
        if size == 0 {
            return Err(DeviceSdkError::ServerUnavailable);
        }
        if connection
            .buffer
            .len()
            .checked_add(size)
            .is_none_or(|n| n > limits.packet + 4096)
        {
            return Err(DeviceSdkError::Overloaded);
        }
        connection.buffer.extend_from_slice(&chunk[..size]);
    }
}
fn terminal(error: &DeviceSdkError) -> bool {
    matches!(
        error,
        DeviceSdkError::Unauthenticated
            | DeviceSdkError::Forbidden
            | DeviceSdkError::InvalidConfiguration(_)
            | DeviceSdkError::Protocol(_)
    )
}
async fn socket(config: &DriverConfig) -> Result<Socket, DeviceSdkError> {
    let stream = TcpStream::connect((config.host.as_str(), config.port))
        .await
        .map_err(|_| DeviceSdkError::ServerUnavailable)?;
    stream
        .set_nodelay(true)
        .map_err(|_| DeviceSdkError::ServerUnavailable)?;
    if !config.tls {
        return Ok(Box::new(stream));
    }
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = &config.ca_pem {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|_| DeviceSdkError::InvalidConfiguration("cannot read MQTT CA file".into()))?;
        let mut pem = Vec::new();
        file.take(65_537)
            .read_to_end(&mut pem)
            .await
            .map_err(|_| DeviceSdkError::InvalidConfiguration("cannot read MQTT CA file".into()))?;
        if pem.len() > 65_536 {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT CA file exceeds 64 KiB".into(),
            ));
        }
        let mut added = 0usize;
        for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
            roots
                .add(cert.map_err(|_| {
                    DeviceSdkError::InvalidConfiguration("invalid MQTT CA PEM".into())
                })?)
                .map_err(|_| {
                    DeviceSdkError::InvalidConfiguration("invalid MQTT CA certificate".into())
                })?;
            added += 1;
        }
        if added == 0 {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT CA file has no certificates".into(),
            ));
        }
    }
    let tls = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(config.host.clone())
        .map_err(|_| DeviceSdkError::InvalidConfiguration("invalid MQTT TLS server name".into()))?;
    let stream = TlsConnector::from(Arc::new(tls))
        .connect(name, stream)
        .await
        .map_err(|_| DeviceSdkError::Unauthenticated)?;
    Ok(Box::new(stream))
}
fn next_id(next: &mut u16, inflight: &BTreeMap<u16, Inflight>) -> Result<u16, DeviceSdkError> {
    for _ in 0..65_535 {
        *next = if *next == 65_535 { 1 } else { *next + 1 };
        if !inflight.contains_key(next) {
            return Ok(*next);
        }
    }
    Err(DeviceSdkError::Overloaded)
}
fn replay_order(inflight: &BTreeMap<u16, Inflight>) -> Vec<u16> {
    let mut entries = inflight
        .iter()
        .map(|(&id, entry)| (entry.order, id))
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries.into_iter().map(|(_, id)| id).collect()
}
#[allow(clippy::too_many_arguments)]
async fn command(
    connection: &mut Connection,
    config: &DriverConfig,
    topic_name: String,
    payload: bytes::Bytes,
    qos: u8,
    packet_id: Option<u16>,
    sender: &mpsc::Sender<CommandEnvelope>,
    bytes: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
) -> Result<(), DeviceSdkError> {
    if topic_name != topic(&config.device, "down") {
        return Err(DeviceSdkError::Forbidden);
    }
    let command: DeviceCommand = serde_json::from_slice(&payload)
        .map_err(|_| DeviceSdkError::Protocol("invalid device command".into()))?;
    if command.device != config.device {
        return Err(DeviceSdkError::Forbidden);
    }
    let charge = u32::try_from(
        payload
            .len()
            .checked_add(128)
            .ok_or(DeviceSdkError::Overloaded)?,
    )
    .map_err(|_| DeviceSdkError::Overloaded)?;
    let permit = bytes
        .clone()
        .try_acquire_many_owned(charge)
        .map_err(|_| DeviceSdkError::Overloaded)?;
    sender
        .try_send(CommandEnvelope {
            command,
            _permit: permit,
        })
        .map_err(|_| DeviceSdkError::Overloaded)?;
    metrics.commands_received.fetch_add(1, Ordering::Relaxed);
    if qos == 1 {
        let id = packet_id
            .ok_or_else(|| DeviceSdkError::Protocol("QoS1 command missing packet ID".into()))?;
        write(
            &mut connection.io,
            &wire::puback(connection.version, id).map_err(protocol)?,
            config.connect_timeout,
        )
        .await?;
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn establish(
    config: &DriverConfig,
    inflight: &BTreeMap<u16, Inflight>,
    next: &mut u16,
    clean_start: bool,
    has_local_session: bool,
    command_sender: &mpsc::Sender<CommandEnvelope>,
    command_bytes: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
    stop: &CancellationToken,
) -> Result<(Connection, bool), DeviceSdkError> {
    let deadline = Instant::now() + config.connect_timeout;
    tokio::select! { _ = stop.cancelled() => Err(DeviceSdkError::ServerUnavailable), result = tokio::time::timeout_at(deadline, async {
        let io = socket(config).await?;
        let mut connection = Connection { io, buffer: BytesMut::with_capacity(4096), properties: Properties::default(),
            keep_alive: Duration::from_secs(15), send_maximum: config.max_packet_bytes, version: version(config) };
        let connect = wire::connect(wire::Connect { version: connection.version, client_id: &config.client_id,
            username: &config.username, password: &config.password, keep_alive: 15, clean_start,
            session_expiry: config.session_expiry, receive_maximum: 16,
            maximum_packet_size: u32::try_from(config.max_packet_bytes).map_err(|_| DeviceSdkError::Overloaded)? }, config.max_packet_bytes).map_err(protocol)?;
        write(&mut connection.io, &connect, config.connect_timeout).await?;
        let (present, properties) = match read_packet(&mut connection, limits(config)).await? {
            Packet::Connack { session_present, reason: 0, properties } => (session_present, properties),
            Packet::Connack { reason, .. } => return Err(terminal_connect_error(reason, config.protocol_version).unwrap_or(DeviceSdkError::ServerUnavailable)),
            _ => return Err(DeviceSdkError::Protocol("expected CONNACK".into())),
        };
        if present && !has_local_session {
            return Err(DeviceSdkError::SessionStateMismatch);
        }
        if properties.maximum_qos == Some(0) { return Err(DeviceSdkError::Protocol("broker does not support QoS1".into())); }
        if let Some(maximum) = properties.maximum_packet_size { connection.send_maximum = connection.send_maximum.min(maximum as usize); }
        if let Some(seconds) = properties.server_keep_alive {
            // Zero disables the protocol obligation; keep an optional 15 s liveness probe.
            connection.keep_alive = Duration::from_secs(u64::from(if seconds == 0 { 15 } else { seconds }));
        }
        connection.properties = properties;
        let subscribe_id = next_id(next, inflight)?;
        let subscribe = wire::subscribe(connection.version, subscribe_id, &topic(&config.device, "down"), connection.send_maximum)
            .map_err(|_| DeviceSdkError::Protocol("broker Maximum Packet Size is below required SUBSCRIBE".into()))?;
        write(&mut connection.io, &subscribe, config.connect_timeout).await?;
        loop {
            match read_packet(&mut connection, limits(config)).await? {
                Packet::Suback { packet_id, reasons } if packet_id == subscribe_id && reasons.as_slice() == [1] => break,
                Packet::Suback { packet_id, reasons } if packet_id == subscribe_id && reasons.as_slice() == [0] => return Err(DeviceSdkError::Protocol("broker granted QoS0 for command subscription".into())),
                Packet::Suback { packet_id, .. } if packet_id == subscribe_id => return Err(DeviceSdkError::Forbidden),
                Packet::Publish { topic, payload, qos, packet_id, .. } => command(&mut connection, config, topic, payload, qos, packet_id, command_sender, command_bytes, metrics).await?,
                Packet::Disconnect { reason } => return Err(disconnect_error(reason)),
                _ => return Err(DeviceSdkError::Protocol("unexpected packet before SUBACK".into())),
            }
        }
        Ok((connection, present))
    }) => result.map_err(|_| DeviceSdkError::Timeout)? }
}
fn disconnect_error(reason: u8) -> DeviceSdkError {
    match reason {
        0x87 => DeviceSdkError::Forbidden,
        0x8e => DeviceSdkError::ServerUnavailable,
        0x89 | 0x8b | 0x97 => DeviceSdkError::ServerUnavailable,
        0x81 | 0x82 | 0x94 | 0x95 => {
            DeviceSdkError::Protocol("broker disconnected for protocol error".into())
        }
        _ => DeviceSdkError::ServerUnavailable,
    }
}
fn notify(
    receipts: &broadcast::Sender<PublishReceipt>,
    outbound: &Outbound,
    result: PublishResult,
) {
    let _ = receipts.send(PublishReceipt {
        source_message_id: outbound.source_message_id.clone(),
        result,
    });
}
async fn send_outbound(
    connection: &mut Connection,
    config: &DriverConfig,
    outbound: &Outbound,
    id: Option<u16>,
    dup: bool,
) -> Result<(), DeviceSdkError> {
    let elapsed = Instant::now()
        .saturating_duration_since(outbound.admitted)
        .as_secs();
    let expiry = outbound
        .expiry
        .map(|seconds| seconds.saturating_sub(u32::try_from(elapsed).unwrap_or(u32::MAX)));
    let packet = wire::publish(
        connection.version,
        &topic(&config.device, outbound.suffix),
        &outbound.payload,
        match outbound.qos {
            PublishQos::AtMostOnce => 0,
            PublishQos::AtLeastOnce => 1,
        },
        id,
        dup,
        expiry,
        connection.send_maximum,
    )
    .map_err(protocol)?;
    write(&mut connection.io, &packet, config.connect_timeout).await
}
#[allow(clippy::too_many_arguments)]
async fn online(
    mut connection: Connection,
    config: &DriverConfig,
    input: &mut mpsc::Receiver<Outbound>,
    inflight: &mut BTreeMap<u16, Inflight>,
    next: &mut u16,
    next_order: &mut u64,
    command_sender: &mpsc::Sender<CommandEnvelope>,
    command_bytes: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
    receipts: &broadcast::Sender<PublishReceipt>,
    stop: &CancellationToken,
) -> Result<(), DeviceSdkError> {
    let mut last_write = Instant::now();
    let mut ping_deadline: Option<Instant> = None;
    let ack_timeout = config
        .connect_timeout
        .saturating_mul(3)
        .max(Duration::from_secs(1));
    loop {
        let window = config.inflight_items.min(usize::from(
            connection.properties.receive_maximum.unwrap_or(u16::MAX),
        ));
        let active = inflight.values().filter(|entry| entry.active).count();
        if active < window
            && active < inflight.len()
            && let Some(id) = replay_order(inflight)
                .into_iter()
                .find(|id| inflight.get(id).is_some_and(|entry| !entry.active))
        {
            let entry = inflight
                .get_mut(&id)
                .ok_or(DeviceSdkError::ServerUnavailable)?;
            send_outbound(&mut connection, config, &entry.outbound, Some(id), true).await?;
            entry.sent_at = Instant::now();
            entry.active = true;
            last_write = Instant::now();
            continue;
        }
        let ack_deadline = inflight
            .values()
            .filter(|entry| entry.active)
            .map(|entry| entry.sent_at + ack_timeout)
            .min();
        if ack_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(DeviceSdkError::Timeout);
        }
        let wake = ack_deadline.map_or(
            ping_deadline.unwrap_or(last_write + connection.keep_alive),
            |deadline| deadline.min(ping_deadline.unwrap_or(last_write + connection.keep_alive)),
        );
        if Instant::now() >= wake {
            if ack_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(DeviceSdkError::Timeout);
            }
            if ping_deadline.is_some() {
                return Err(DeviceSdkError::Timeout);
            }
            write(&mut connection.io, &wire::pingreq(), config.connect_timeout).await?;
            last_write = Instant::now();
            ping_deadline = Some(last_write + connection.keep_alive);
            continue;
        }
        tokio::select! {
            _ = stop.cancelled() => { let _ = write(&mut connection.io, &wire::disconnect(), Duration::from_millis(500)).await; return Ok(()); }
            result = read_packet(&mut connection, limits(config)) => {
                match result? {
                    Packet::Puback { packet_id, reason } => {
                        if !inflight.get(&packet_id).is_some_and(|entry| entry.active) {
                            return Err(DeviceSdkError::Protocol("unknown PUBACK".into()));
                        }
                        let outbound = inflight.remove(&packet_id).ok_or(DeviceSdkError::ServerUnavailable)?.outbound;
                        notify(receipts, &outbound, if reason < 0x80 { PublishResult::Puback } else { PublishResult::Rejected(reason) });
                    }
                    Packet::Publish { topic, payload, qos, packet_id, .. } => {
                        if let Err(error) = command(&mut connection, config, topic, payload, qos, packet_id, command_sender, command_bytes, metrics).await {
                            let _ = write(&mut connection.io, &wire::disconnect(), Duration::from_millis(500)).await;
                            return Err(error);
                        }
                        if qos == 1 { last_write = Instant::now(); }
                    }
                    Packet::Pingresp if ping_deadline.take().is_some() => {},
                    Packet::Disconnect { reason } => return Err(disconnect_error(reason)),
                    _ => return Err(DeviceSdkError::Protocol("unexpected MQTT packet".into())),
                }
            }
            outbound = input.recv(), if inflight.len() < config.inflight_items && active < window => {
                let Some(outbound) = outbound else { return Ok(()); };
                if outbound.expiry.is_some_and(|expiry| Instant::now().saturating_duration_since(outbound.admitted).as_secs() >= u64::from(expiry)) {
                    notify(receipts, &outbound, PublishResult::Expired); continue;
                }
                let id = if outbound.qos == PublishQos::AtLeastOnce { Some(next_id(next, inflight)?) } else { None };
                if let Some(id) = id {
                    *next_order = next_order.checked_add(1).ok_or(DeviceSdkError::Overloaded)?;
                    inflight.insert(id, Inflight { outbound, sent_at: Instant::now(), order: *next_order, active: true });
                    match send_outbound(&mut connection, config, &inflight.get(&id).ok_or(DeviceSdkError::ServerUnavailable)?.outbound, Some(id), false).await {
                        Err(DeviceSdkError::Overloaded) => { if let Some(old) = inflight.remove(&id) { notify(receipts, &old.outbound, PublishResult::Rejected(0x95)); } continue; }
                        other => other?,
                    }
                } else {
                    if let Err(error) = send_outbound(&mut connection, config, &outbound, None, false).await {
                        notify(receipts, &outbound, if matches!(error, DeviceSdkError::Overloaded) { PublishResult::Rejected(0x95) } else { PublishResult::Uncertain });
                        if matches!(error, DeviceSdkError::Overloaded) { continue; }
                        return Err(error);
                    }
                    notify(receipts, &outbound, PublishResult::Written);
                }
                last_write = Instant::now();
            }
            _ = tokio::time::sleep_until(wake) => {
                if ack_deadline.is_some_and(|deadline| Instant::now() >= deadline) { return Err(DeviceSdkError::Timeout); }
                if ping_deadline.is_some() { return Err(DeviceSdkError::Timeout); }
                write(&mut connection.io, &wire::pingreq(), config.connect_timeout).await?;
                last_write = Instant::now();
                ping_deadline = Some(last_write + connection.keep_alive);
            }
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    config: DriverConfig,
    mut input: mpsc::Receiver<Outbound>,
    command_sender: mpsc::Sender<CommandEnvelope>,
    state: watch::Sender<MqttConnectionState>,
    admission: Arc<Mutex<Admission>>,
    stop: CancellationToken,
    metrics: Arc<Metrics>,
    receipts: broadcast::Sender<PublishReceipt>,
) {
    let command_bytes = Arc::new(Semaphore::new(config.command_bytes));
    let seed = EventId::generate().0.as_u128() as u64;
    let mut attempt = 0u32;
    let mut connected_once = false;
    let mut clean_start_next = false;
    let mut v311_reset_pending = false;
    let mut next = 0u16;
    let mut next_order = 0u64;
    let mut ended_terminal = false;
    let mut inflight = BTreeMap::<u16, Inflight>::new();
    loop {
        if stop.is_cancelled() {
            break;
        }
        for entry in inflight.values_mut() {
            entry.active = false;
        }
        let established = establish(
            &config,
            &inflight,
            &mut next,
            clean_start_next,
            connected_once || !inflight.is_empty(),
            &command_sender,
            &command_bytes,
            &metrics,
            &stop,
        )
        .await;
        match established {
            Ok((mut connection, session_present)) => {
                if v311_reset_pending {
                    let _ = write(
                        &mut connection.io,
                        &wire::disconnect(),
                        Duration::from_millis(500),
                    )
                    .await;
                    v311_reset_pending = false;
                    clean_start_next = false;
                    continue;
                }
                clean_start_next = false;
                if connected_once {
                    metrics.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                }
                connected_once = true;
                attempt = 0;
                if config.protocol_version == MqttProtocolVersion::V5 && !session_present {
                    for (_, old) in std::mem::take(&mut inflight) {
                        notify(&receipts, &old.outbound, PublishResult::SessionLost);
                    }
                }
                if let Ok(mut gate) = admission.lock() {
                    gate.ready = !gate.closing;
                }
                let _ = state.send(MqttConnectionState::Connected);
                let result = online(
                    connection,
                    &config,
                    &mut input,
                    &mut inflight,
                    &mut next,
                    &mut next_order,
                    &command_sender,
                    &command_bytes,
                    &metrics,
                    &receipts,
                    &stop,
                )
                .await;
                if let Ok(mut gate) = admission.lock() {
                    gate.ready = false;
                }
                let _ = state.send(MqttConnectionState::Connecting);
                if stop.is_cancelled() {
                    break;
                }
                if let Err(error) = result
                    && terminal(&error)
                {
                    let _ = state.send(MqttConnectionState::Terminal(error));
                    ended_terminal = true;
                    break;
                }
            }
            Err(error) => {
                if stop.is_cancelled() {
                    break;
                }
                if matches!(error, DeviceSdkError::SessionStateMismatch) && !clean_start_next {
                    clean_start_next = true;
                    v311_reset_pending = config.protocol_version == MqttProtocolVersion::V311;
                } else if matches!(error, DeviceSdkError::SessionStateMismatch) {
                    let _ = state.send(MqttConnectionState::Terminal(error));
                    ended_terminal = true;
                    break;
                }
                if terminal(&error) {
                    let _ = state.send(MqttConnectionState::Terminal(error));
                    ended_terminal = true;
                    break;
                }
            }
        }
        attempt = attempt.saturating_add(1);
        let delay = device_reconnect_delay(config.reconnect, attempt, seed);
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
    }
    if let Ok(mut gate) = admission.lock() {
        gate.ready = false;
    }
    for (_, old) in inflight {
        notify(&receipts, &old.outbound, PublishResult::Uncertain);
    }
    while let Ok(old) = input.try_recv() {
        notify(&receipts, &old, PublishResult::Uncertain);
    }
    if !ended_terminal {
        let _ = state.send(MqttConnectionState::Closed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_protocol::SourceMessageId;

    #[test]
    fn replay_follows_send_order_across_packet_id_wrap() {
        let bytes = Arc::new(Semaphore::new(2));
        let outbound = |name| Outbound {
            payload: vec![1],
            qos: PublishQos::AtLeastOnce,
            suffix: "up",
            source_message_id: SourceMessageId::new(name).unwrap(),
            expiry: None,
            admitted: Instant::now(),
            _permit: bytes.clone().try_acquire_owned().unwrap(),
        };
        let mut inflight = BTreeMap::new();
        inflight.insert(
            65_535,
            Inflight {
                outbound: outbound("before-wrap"),
                sent_at: Instant::now(),
                order: 1,
                active: true,
            },
        );
        inflight.insert(
            1,
            Inflight {
                outbound: outbound("after-wrap"),
                sent_at: Instant::now(),
                order: 2,
                active: true,
            },
        );
        assert_eq!(replay_order(&inflight), [65_535, 1]);
        let mut next = 65_535;
        assert_eq!(next_id(&mut next, &inflight).unwrap(), 2);
    }

    #[tokio::test]
    async fn slow_socket_reader_cannot_hold_writer_forever() {
        let (writer, _reader) = tokio::io::duplex(1);
        let mut socket: Socket = Box::new(writer);
        assert!(matches!(
            write(&mut socket, &[0; 64], Duration::from_millis(25)).await,
            Err(DeviceSdkError::Timeout)
        ));
    }
}
