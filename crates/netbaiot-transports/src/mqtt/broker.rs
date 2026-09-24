use super::{
    codec::{MqttVersion, v5},
    packet::valid_topic,
};
use netbaiot_core::{
    AuthInvalidation, AuthenticatedDevice, CodecId, DeviceId, DeviceKey, Permissions, ProductId,
    TenantId,
};
use netbaiot_runtime::{Error, Histogram, Limits, Metrics, Result, lock, now_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs,
    hash::Hash,
    io::{BufReader, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const STATE_OVERHEAD: usize = 64;
const RECOVERY_MAGIC: &[u8; 4] = b"NBMQ";
const RECOVERY_VERSION_V1: u32 = 1;
const RECOVERY_VERSION_V2: u32 = 2;
const RECOVERY_VERSION_V3: u32 = 3;
const RECOVERY_VERSION_V4: u32 = 4;
const RECOVERY_VERSION: u32 = 5;
const LEGACY_V1_RECOVERY_READ_MAX: usize = 1_342_177_280;
const RECOVERY_FILE: &str = "mqtt-runtime.state";
const HOT_MAINTENANCE_BUDGET: usize = 64;
const TICK_MAINTENANCE_BUDGET: usize = 1_024;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionKey {
    pub device: DeviceKey,
    pub client_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Subscription {
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    /// 0: replay always; 1: only for a new subscription; 2: never replay.
    pub retain_handling: u8,
}

impl Subscription {
    fn v311(qos: u8) -> Self {
        Self {
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }
    }
}

impl<'de> Deserialize<'de> for Subscription {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Format {
            Legacy(u8),
            Current {
                qos: u8,
                no_local: bool,
                retain_as_published: bool,
                retain_handling: u8,
            },
        }
        Ok(match Format::deserialize(deserializer)? {
            Format::Legacy(qos) => Self::v311(qos),
            Format::Current {
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            } => Self {
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    #[serde(default)]
    pub properties: PublishProperties,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishProperties {
    pub payload_format: Option<u8>,
    pub expires_at_ms: Option<i64>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
}

impl PublishProperties {
    pub fn from_wire(properties: &v5::Properties) -> Self {
        Self {
            payload_format: properties.payload_format,
            expires_at_ms: properties
                .message_expiry
                .map(|seconds| now_ms().saturating_add(i64::from(seconds) * 1_000)),
            content_type: properties.content_type.clone(),
            response_topic: properties.response_topic.clone(),
            correlation_data: properties
                .correlation_data
                .as_ref()
                .map(|value| value.to_vec()),
            user_properties: properties.user_properties.clone(),
        }
    }

    fn bytes(&self) -> usize {
        if self == &Self::default() {
            return 0;
        }
        128usize
            .saturating_add(self.content_type.as_ref().map_or(0, String::len))
            .saturating_add(self.response_topic.as_ref().map_or(0, String::len))
            .saturating_add(self.correlation_data.as_ref().map_or(0, Vec::len))
            .saturating_add(
                self.user_properties
                    .iter()
                    .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(64))
                    .sum::<usize>(),
            )
    }

    fn valid(&self, limits: &Limits) -> bool {
        if self.payload_format.is_some_and(|value| value > 1)
            || self.expires_at_ms.is_some_and(|value| value < 0)
            || self.content_type.as_ref().is_some_and(|value| {
                value.len() > limits.max_mqtt_content_type_bytes
                    || super::packet::valid_utf8(value.as_bytes()).is_err()
            })
            || self.response_topic.as_ref().is_some_and(|value| {
                value.len() > limits.max_mqtt_response_topic_bytes
                    || !valid_topic(value, limits, false)
            })
            || self
                .correlation_data
                .as_ref()
                .is_some_and(|value| value.len() > limits.max_mqtt_correlation_data_bytes)
            || self.user_properties.len() > limits.max_mqtt_user_properties
        {
            return false;
        }
        let mut total = 0usize;
        let mut user_total = 0usize;
        for (key, value) in &self.user_properties {
            if super::packet::valid_utf8(key.as_bytes()).is_err()
                || super::packet::valid_utf8(value.as_bytes()).is_err()
            {
                return false;
            }
            let Some(pair_bytes) = key
                .len()
                .checked_add(value.len())
                .and_then(|n| n.checked_add(5))
            else {
                return false;
            };
            let Some(next) = user_total.checked_add(pair_bytes) else {
                return false;
            };
            user_total = next;
            let Some(next) = total.checked_add(pair_bytes) else {
                return false;
            };
            total = next;
        }
        if user_total > limits.max_mqtt_user_property_bytes {
            return false;
        }
        for value in [
            self.payload_format.map(|_| 2),
            self.expires_at_ms.map(|_| 5),
            self.content_type.as_ref().map(|value| value.len() + 3),
            self.response_topic.as_ref().map(|value| value.len() + 3),
            self.correlation_data.as_ref().map(|value| value.len() + 3),
        ]
        .into_iter()
        .flatten()
        {
            let Some(next) = total.checked_add(value) else {
                return false;
            };
            total = next;
        }
        total <= limits.max_mqtt_property_bytes
    }
}

impl BrokerMessage {
    fn bytes(&self) -> usize {
        self.topic
            .len()
            .saturating_add(self.payload.len())
            .saturating_add(STATE_OVERHEAD)
            .saturating_add(self.properties.bytes())
    }

    pub(crate) fn expired(&self, now: i64) -> bool {
        self.properties
            .expires_at_ms
            .is_some_and(|expires| expires <= now)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerDelivery {
    pub message: BrokerMessage,
    pub packet_id: Option<u16>,
    pub dup: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerFrame {
    Publish(BrokerDelivery),
    Pubrel { packet_id: u16, dup: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboundQos2State {
    AwaitPubrel(BrokerMessage),
    Delivering {
        message: BrokerMessage,
        operation_id: u64,
    },
    EventAccepted(BrokerMessage),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboundQos2Action {
    Deliver {
        message: BrokerMessage,
        session_incarnation: u64,
        operation_id: u64,
    },
    EventAccepted {
        session_incarnation: u64,
        operation_id: u64,
    },
    DeliveryInProgress,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundQos2PublishState {
    ExistingTransaction,
    NeedsNewMessageAdmission,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutboundAck {
    Puback,
    Pubcomp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboundState {
    AwaitPuback(BrokerMessage),
    AwaitPubrec(BrokerMessage),
    AwaitPubcomp(BrokerMessage),
}

impl OutboundState {
    fn bytes(&self) -> usize {
        match self {
            Self::AwaitPuback(message)
            | Self::AwaitPubrec(message)
            | Self::AwaitPubcomp(message) => message.bytes(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSession {
    key: SessionKey,
    #[serde(default)]
    version: MqttVersion,
    #[serde(default)]
    session_expiry_interval: u32,
    #[serde(default)]
    expires_at_ms: Option<i64>,
    #[serde(default)]
    incarnation: u64,
    #[serde(default)]
    authorization: Option<SessionAuthorization>,
    subscriptions: HashMap<String, Subscription>,
    offline: VecDeque<BrokerMessage>,
    offline_bytes: usize,
    inbound_qos2: HashMap<u16, InboundQos2State>,
    #[serde(skip)]
    inbound_operations: HashMap<u16, u64>,
    #[serde(default)]
    inbound_reservations: HashMap<u16, RetainedReservation>,
    outbound: HashMap<u16, OutboundState>,
    /// Original transmission order for reconnect retransmission (MQTT-4.6.0-1).
    #[serde(default)]
    outbound_order: VecDeque<u16>,
    next_packet_id: u16,
    state_bytes: usize,
    last_seen_ms: i64,
    #[serde(skip)]
    active_generation: Option<u64>,
    #[serde(skip)]
    send_quota: u16,
    #[serde(skip)]
    sent: HashSet<u16>,
    /// QoS PUBLISH packets occupying this network connection's Receive Maximum.
    /// A successful QoS2 PUBREC does not release the slot; PUBCOMP does.
    #[serde(skip)]
    send_window: HashSet<u16>,
    /// Inbound QoS2 PUBLISH packets admitted on this network connection and
    /// not yet answered with PUBCOMP. Persistent QoS2 state is separate.
    #[serde(skip)]
    inbound_window: HashSet<u16>,
    /// A QoS PUBLISH whose first transfer has begun must finish its ACK exchange
    /// even after Message Expiry. This state survives disconnect and NBMQ recovery.
    #[serde(default)]
    started_outbound: HashSet<u16>,
}

impl StoredSession {
    fn new(key: SessionKey, incarnation: u64, authorization: SessionAuthorization) -> Self {
        let state_bytes = key.client_id.len()
            + key.device.tenant_id.as_str().len()
            + key.device.product_id.as_str().len()
            + key.device.device_id.as_str().len()
            + STATE_OVERHEAD;
        Self {
            key,
            version: MqttVersion::V311,
            session_expiry_interval: 0,
            expires_at_ms: None,
            incarnation,
            authorization: Some(authorization),
            subscriptions: HashMap::new(),
            offline: VecDeque::new(),
            offline_bytes: 0,
            inbound_qos2: HashMap::new(),
            inbound_operations: HashMap::new(),
            inbound_reservations: HashMap::new(),
            outbound: HashMap::new(),
            outbound_order: VecDeque::new(),
            next_packet_id: 1,
            state_bytes,
            last_seen_ms: now_ms(),
            active_generation: None,
            send_quota: u16::MAX,
            sent: HashSet::new(),
            send_window: HashSet::new(),
            inbound_window: HashSet::new(),
            started_outbound: HashSet::new(),
        }
    }

    fn allocate_packet_id(&mut self) -> Result<u16> {
        for _ in 0..u16::MAX {
            let id = self.next_packet_id;
            self.next_packet_id = if id == u16::MAX { 1 } else { id + 1 };
            if !self.outbound.contains_key(&id) {
                return Ok(id);
            }
        }
        Err(Error::Overloaded)
    }

    fn has_outbound_capacity(&self, qos: u8, limits: &Limits) -> bool {
        let outbound = self
            .outbound
            .values()
            .filter(|state| match qos {
                1 => matches!(state, OutboundState::AwaitPuback(_)),
                2 => !matches!(state, OutboundState::AwaitPuback(_)),
                _ => false,
            })
            .count();
        let used = if qos == 2 {
            outbound.saturating_add(self.inbound_qos2.len())
        } else {
            outbound
        };
        used < if qos == 1 {
            limits.max_inflight_qos1_per_session
        } else {
            limits.max_inflight_qos2_per_session
        }
    }

    fn has_send_quota(&self) -> bool {
        self.send_window.len() < usize::from(self.send_quota)
    }

    fn insert_outbound(&mut self, packet_id: u16, state: OutboundState) {
        if !self.outbound.contains_key(&packet_id) {
            self.outbound_order.push_back(packet_id);
        }
        self.outbound.insert(packet_id, state);
    }

    fn remove_outbound(&mut self, packet_id: u16) -> Option<OutboundState> {
        self.sent.remove(&packet_id);
        self.send_window.remove(&packet_id);
        self.started_outbound.remove(&packet_id);
        let removed = self.outbound.remove(&packet_id);
        if removed.is_some() {
            self.outbound_order
                .retain(|candidate| *candidate != packet_id);
        }
        removed
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SessionAuthorization {
    credential_version: u32,
    auth_generation: u64,
    permissions: Permissions,
    #[serde(default)]
    codec_id: Option<CodecId>,
    #[serde(default)]
    codec_version: Option<u16>,
}

impl From<&AuthenticatedDevice> for SessionAuthorization {
    fn from(auth: &AuthenticatedDevice) -> Self {
        Self {
            credential_version: auth.credential_version,
            auth_generation: auth.auth_generation,
            permissions: auth.permissions.clone(),
            codec_id: Some(auth.codec_id.clone()),
            codec_version: Some(auth.codec_version),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RetainedReservation {
    global_count: usize,
    global_bytes: usize,
    tenant_count: usize,
    tenant_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingWill {
    owner: DeviceKey,
    message: BrokerMessage,
    #[serde(default)]
    due_at_ms: Option<i64>,
    #[serde(default)]
    cancel_on_resume: Option<(SessionKey, u64)>,
    #[serde(default)]
    message_expiry_interval: Option<u32>,
    #[serde(skip)]
    retained_reservation: RetainedReservation,
}

#[derive(Clone)]
struct ActiveSession {
    generation: u64,
    sender: mpsc::Sender<BrokerFrame>,
    cancel: CancellationToken,
}

#[derive(Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    subscribers: HashMap<SessionKey, Subscription>,
}

#[derive(Default)]
struct SubscriptionTrie {
    root: TrieNode,
}

impl SubscriptionTrie {
    fn insert(&mut self, filter: &str, key: SessionKey, subscription: Subscription) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.to_owned()).or_default();
        }
        node.subscribers.insert(key, subscription);
    }

    fn remove(&mut self, filter: &str, key: &SessionKey) {
        let levels = filter.split('/').collect::<Vec<_>>();
        Self::remove_at(&mut self.root, &levels, 0, key);
    }

    fn remove_at(node: &mut TrieNode, levels: &[&str], at: usize, key: &SessionKey) -> bool {
        if at == levels.len() {
            node.subscribers.remove(key);
        } else if let Some(child) = node.children.get_mut(levels[at])
            && Self::remove_at(child, levels, at + 1, key)
        {
            node.children.remove(levels[at]);
        }
        node.subscribers.is_empty() && node.children.is_empty()
    }

    fn matching(&self, topic: &str) -> HashMap<SessionKey, Subscription> {
        let levels = topic.split('/').collect::<Vec<_>>();
        let mut matches = HashMap::new();
        Self::match_at(
            &self.root,
            &levels,
            0,
            levels.first().is_some_and(|level| level.starts_with('$')),
            &mut matches,
        );
        matches
    }

    fn match_at(
        node: &TrieNode,
        levels: &[&str],
        at: usize,
        dollar_root: bool,
        output: &mut HashMap<SessionKey, Subscription>,
    ) {
        if !(at == 0 && dollar_root)
            && let Some(hash) = node.children.get("#")
        {
            merge_subscribers(output, &hash.subscribers);
        }
        if at == levels.len() {
            merge_subscribers(output, &node.subscribers);
            return;
        }
        if let Some(exact) = node.children.get(levels[at]) {
            Self::match_at(exact, levels, at + 1, dollar_root, output);
        }
        if !(at == 0 && dollar_root)
            && let Some(plus) = node.children.get("+")
        {
            Self::match_at(plus, levels, at + 1, dollar_root, output);
        }
    }
}

fn merge_subscribers(
    output: &mut HashMap<SessionKey, Subscription>,
    subscribers: &HashMap<SessionKey, Subscription>,
) {
    for (key, subscription) in subscribers {
        output
            .entry(key.clone())
            .and_modify(|existing| {
                existing.qos = existing.qos.max(subscription.qos);
                existing.no_local &= subscription.no_local;
                existing.retain_as_published |= subscription.retain_as_published;
            })
            .or_insert(*subscription);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetainedMessage {
    tenant_id: TenantId,
    message: BrokerMessage,
    #[serde(default)]
    origin: Option<SessionKey>,
}

impl RetainedMessage {
    fn bytes(&self) -> usize {
        retained_charge(&self.message, self.origin.as_ref())
    }
}

fn retained_charge(message: &BrokerMessage, origin: Option<&SessionKey>) -> usize {
    message.bytes().saturating_add(origin.map_or(0, |key| {
        key.client_id.len()
            + key.device.tenant_id.as_str().len()
            + key.device.product_id.as_str().len()
            + key.device.device_id.as_str().len()
            + STATE_OVERHEAD
    }))
}

/// One current deadline per owned key. Replacements remove the old bucket entry,
/// so historical traffic cannot accumulate stale heap nodes.
struct DeadlineIndex<K> {
    by_deadline: BTreeMap<i64, HashSet<K>>,
    by_key: HashMap<K, i64>,
}

impl<K> Default for DeadlineIndex<K> {
    fn default() -> Self {
        Self {
            by_deadline: BTreeMap::new(),
            by_key: HashMap::new(),
        }
    }
}

impl<K: Clone + Eq + Hash> DeadlineIndex<K> {
    fn has_due(&self, now: i64) -> bool {
        self.by_deadline
            .first_key_value()
            .is_some_and(|(&deadline, _)| deadline <= now)
    }

    fn update(&mut self, key: K, deadline: Option<i64>) {
        if self.by_key.get(&key).copied() == deadline {
            return;
        }
        if let Some(old) = self.by_key.remove(&key)
            && let Some(bucket) = self.by_deadline.get_mut(&old)
        {
            bucket.remove(&key);
            if bucket.is_empty() {
                self.by_deadline.remove(&old);
            }
        }
        if let Some(deadline) = deadline {
            self.by_deadline
                .entry(deadline)
                .or_default()
                .insert(key.clone());
            self.by_key.insert(key, deadline);
        }
    }

    fn pop_due(&mut self, now: i64) -> Option<K> {
        let (&deadline, bucket) = self.by_deadline.first_key_value()?;
        if deadline > now {
            return None;
        }
        let key = bucket.iter().next()?.clone();
        self.update(key.clone(), None);
        Some(key)
    }
}

struct BrokerState {
    sessions: HashMap<SessionKey, StoredSession>,
    /// Derived from sessions and rebuilt after recovery; never serialized.
    session_usage: HashMap<SessionKey, SessionUsage>,
    tenant_usage: HashMap<TenantId, TenantUsage>,
    device_subscription_count: HashMap<DeviceKey, usize>,
    session_expiry: DeadlineIndex<SessionKey>,
    message_expiry: DeadlineIndex<SessionKey>,
    retained_expiry: DeadlineIndex<String>,
    session_idle_ttl_ms: i64,
    active: HashMap<SessionKey, ActiveSession>,
    trie: SubscriptionTrie,
    retained: HashMap<String, RetainedMessage>,
    generation: u64,
    operation_id: u64,
    subscription_count: usize,
    session_bytes: usize,
    offline_count: usize,
    offline_bytes: usize,
    retained_bytes: usize,
    retained_reserved_count: usize,
    retained_reserved_bytes: usize,
    retained_reserved_tenants: HashMap<TenantId, (usize, usize)>,
    pending_wills: VecDeque<PendingWill>,
    will_responsibility_count: usize,
    will_responsibility_bytes: usize,
    will_responsibility_tenants: HashMap<TenantId, (usize, usize)>,
    pending_by_tenant: HashMap<TenantId, VecDeque<SessionKey>>,
    pending_sessions: HashSet<SessionKey>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SessionUsage {
    state_bytes: usize,
    subscriptions: usize,
    offline_count: usize,
    offline_bytes: usize,
    qos1_inflight: usize,
    qos2_inflight: usize,
}

impl SessionUsage {
    fn from_session(session: &StoredSession) -> Self {
        let mut qos1_inflight = 0;
        let mut qos2_inflight = session.inbound_qos2.len();
        for outbound in session.outbound.values() {
            if matches!(outbound, OutboundState::AwaitPuback(_)) {
                qos1_inflight += 1;
            } else {
                qos2_inflight += 1;
            }
        }
        Self {
            state_bytes: session.state_bytes,
            subscriptions: session.subscriptions.len(),
            offline_count: session.offline.len(),
            offline_bytes: session.offline_bytes,
            qos1_inflight,
            qos2_inflight,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TenantUsage {
    session_count: usize,
    session_bytes: usize,
    subscription_count: usize,
    offline_count: usize,
    offline_bytes: usize,
    qos1_inflight: usize,
    qos2_inflight: usize,
}

fn apply_usage_delta(value: &mut usize, before: usize, after: usize) -> Result<()> {
    *value = if after >= before {
        value.checked_add(after - before)
    } else {
        value.checked_sub(before - after)
    }
    .ok_or(Error::Internal)?;
    Ok(())
}

fn next_message_expiry(session: &StoredSession) -> Option<i64> {
    session
        .offline
        .iter()
        .filter_map(|message| message.properties.expires_at_ms)
        .chain(
            session
                .outbound
                .iter()
                .filter_map(|(id, outbound)| match outbound {
                    OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message)
                        if !session.started_outbound.contains(id) =>
                    {
                        message.properties.expires_at_ms
                    }
                    OutboundState::AwaitPubcomp(message) => message.properties.expires_at_ms,
                    _ => None,
                }),
        )
        .min()
}

fn session_expiry_deadline(state: &BrokerState, key: &SessionKey) -> Option<i64> {
    if state.active.contains_key(key) {
        return None;
    }
    let session = state.sessions.get(key)?;
    if session.version == MqttVersion::V5 {
        session.expires_at_ms
    } else {
        Some(
            session
                .last_seen_ms
                .saturating_add(state.session_idle_ttl_ms)
                .saturating_add(1),
        )
    }
}

fn message_expiry_due(state: &BrokerState, key: &SessionKey, now: i64) -> bool {
    state
        .message_expiry
        .by_key
        .get(key)
        .is_some_and(|&deadline| deadline <= now)
}

/// Reconcile one changed authoritative session while the broker mutex is held.
/// The cached per-session value makes all callers independent of unrelated sessions.
fn sync_session_usage(state: &mut BrokerState, key: &SessionKey) -> Result<()> {
    let before = state.session_usage.get(key).copied();
    let after = state.sessions.get(key).map(SessionUsage::from_session);
    let message_deadline = state.sessions.get(key).and_then(next_message_expiry);
    let expiry_deadline = session_expiry_deadline(state, key);
    state.message_expiry.update(key.clone(), message_deadline);
    state.session_expiry.update(key.clone(), expiry_deadline);
    if before == after {
        return Ok(());
    }
    let old = before.unwrap_or_default();
    let new = after.unwrap_or_default();
    let tenant = &key.device.tenant_id;
    let mut tenant_usage = state.tenant_usage.get(tenant).copied().unwrap_or_default();
    apply_usage_delta(
        &mut tenant_usage.session_count,
        usize::from(before.is_some()),
        usize::from(after.is_some()),
    )?;
    apply_usage_delta(
        &mut tenant_usage.session_bytes,
        old.state_bytes,
        new.state_bytes,
    )?;
    apply_usage_delta(
        &mut tenant_usage.subscription_count,
        old.subscriptions,
        new.subscriptions,
    )?;
    apply_usage_delta(
        &mut tenant_usage.offline_count,
        old.offline_count,
        new.offline_count,
    )?;
    apply_usage_delta(
        &mut tenant_usage.offline_bytes,
        old.offline_bytes,
        new.offline_bytes,
    )?;
    apply_usage_delta(
        &mut tenant_usage.qos1_inflight,
        old.qos1_inflight,
        new.qos1_inflight,
    )?;
    apply_usage_delta(
        &mut tenant_usage.qos2_inflight,
        old.qos2_inflight,
        new.qos2_inflight,
    )?;
    let mut device_subscriptions = state
        .device_subscription_count
        .get(&key.device)
        .copied()
        .unwrap_or_default();
    apply_usage_delta(
        &mut device_subscriptions,
        old.subscriptions,
        new.subscriptions,
    )?;
    if let Some(after) = after {
        state.session_usage.insert(key.clone(), after);
    } else {
        state.session_usage.remove(key);
    }
    if tenant_usage.session_count == 0 {
        state.tenant_usage.remove(tenant);
    } else {
        state.tenant_usage.insert(tenant.clone(), tenant_usage);
    }
    if device_subscriptions == 0 {
        state.device_subscription_count.remove(&key.device);
    } else {
        state
            .device_subscription_count
            .insert(key.device.clone(), device_subscriptions);
    }
    Ok(())
}

fn tenant_session_bytes(state: &BrokerState, tenant: &TenantId) -> usize {
    state
        .tenant_usage
        .get(tenant)
        .map_or(0, |usage| usage.session_bytes)
}

fn total_session_bytes(state: &BrokerState) -> usize {
    state
        .session_bytes
        .saturating_add(state.will_responsibility_bytes)
}

fn tenant_total_session_bytes(state: &BrokerState, tenant: &TenantId) -> usize {
    tenant_session_bytes(state, tenant).saturating_add(
        state
            .will_responsibility_tenants
            .get(tenant)
            .map_or(0, |usage| usage.1),
    )
}

fn tenant_inflight(state: &BrokerState, tenant: &TenantId, qos: u8) -> usize {
    state.tenant_usage.get(tenant).map_or(0, |usage| {
        if qos == 1 {
            usage.qos1_inflight
        } else {
            usage.qos2_inflight
        }
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MqttRecoverySnapshot {
    pub format_version: u32,
    pub snapshot_generation: u64,
    sessions: Vec<StoredSession>,
    retained: Vec<(String, RetainedMessage)>,
    #[serde(default)]
    pending_wills: Vec<PendingWill>,
}

pub struct Attachment {
    pub key: SessionKey,
    pub generation: u64,
    pub session_incarnation: u64,
    pub session_present: bool,
    pub receiver: mpsc::Receiver<BrokerFrame>,
    pub cancel: CancellationToken,
    broker: Arc<MqttBroker>,
    clean_session: bool,
    attached: bool,
}

pub struct WillGuard {
    broker: Arc<MqttBroker>,
    owner: DeviceKey,
    message: BrokerMessage,
    reservation: RetainedReservation,
    delay: Option<(u32, u32, SessionKey, u64, u64)>,
    message_expiry_interval: Option<u32>,
    armed: bool,
    finished: bool,
}

enum WillSchedule {
    Delayed,
    Suppressed,
    PublishNow,
}

impl WillGuard {
    pub fn arm(&mut self) {
        self.armed = true;
    }

    pub(super) fn arm_v5(
        &mut self,
        key: SessionKey,
        incarnation: u64,
        generation: u64,
        delay: u32,
        session_expiry: u32,
        message_expiry: Option<u32>,
    ) {
        self.delay = Some((delay, session_expiry, key, incarnation, generation));
        self.message_expiry_interval = message_expiry;
        self.armed = true;
    }

    pub fn set_v5_session_expiry(&mut self, interval: u32) {
        if let Some((_, expiry, _, _, _)) = &mut self.delay {
            *expiry = interval;
        }
    }

    fn settle(&mut self) -> Result<Option<BrokerMessage>> {
        if self.finished {
            return Err(Error::Conflict);
        }
        if let Some((delay, session_expiry, key, incarnation, generation)) = &self.delay
            && *delay > 0
            && *session_expiry > 0
        {
            let due_at_ms =
                now_ms().saturating_add(i64::from((*delay).min(*session_expiry)) * 1_000);
            let outcome = self.broker.schedule_reserved_will(
                PendingWill {
                    owner: self.owner.clone(),
                    message: self.message.clone(),
                    due_at_ms: Some(due_at_ms),
                    cancel_on_resume: Some((key.clone(), *incarnation)),
                    message_expiry_interval: self.message_expiry_interval,
                    retained_reservation: self.reservation,
                },
                *generation,
            )?;
            match outcome {
                WillSchedule::Delayed | WillSchedule::Suppressed => {
                    self.finished = true;
                    return Ok(None);
                }
                WillSchedule::PublishNow => {}
            }
        }
        let mut message = self.message.clone();
        if let Some(expiry) = self.message_expiry_interval {
            message.properties.expires_at_ms =
                Some(now_ms().saturating_add(i64::from(expiry) * 1_000));
        }
        let result = self.broker.publish_reserved_will(
            &self.owner,
            &message,
            self.reservation,
            self.message_expiry_interval,
        );
        self.finished = true;
        Ok(result?.map(|_| message))
    }

    pub fn publish_v5(&mut self) -> Result<Option<BrokerMessage>> {
        self.settle()
    }

    pub fn publish(&mut self) -> Result<BrokerMessage> {
        if self.finished {
            return Err(Error::Conflict);
        }
        self.settle()?.ok_or(Error::Internal)
    }

    pub fn suppress(&mut self) -> Result<()> {
        if !self.finished {
            self.broker.release_will_reservation(
                &self.owner.tenant_id,
                self.message.bytes(),
                self.reservation,
            )?;
            self.finished = true;
        }
        Ok(())
    }
}

impl Drop for WillGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let result = if self.armed {
            self.settle().map(|_| ())
        } else {
            self.broker.release_will_reservation(
                &self.owner.tenant_id,
                self.message.bytes(),
                self.reservation,
            )
        };
        if let Err(error) = result {
            tracing::error!(%error, "failed to settle accepted MQTT Will responsibility");
        }
        self.finished = true;
    }
}

impl Attachment {
    pub fn detach(&mut self) -> Result<()> {
        if self.attached {
            self.broker
                .detach(&self.key, self.generation, self.clean_session)?;
            self.attached = false;
        }
        Ok(())
    }
}

impl Drop for Attachment {
    fn drop(&mut self) {
        if self.attached {
            if let Err(error) = self
                .broker
                .detach(&self.key, self.generation, self.clean_session)
            {
                tracing::error!(%error, "failed to release MQTT attachment");
            }
            self.attached = false;
        }
    }
}

pub struct MqttBroker {
    limits: Arc<Limits>,
    metrics: Option<Arc<Metrics>>,
    /// Derived from `BrokerState::subscription_count`. Broker state remains authoritative; this
    /// hint only lets a non-retained route with no possible target linearize without the mutex.
    subscription_count: AtomicUsize,
    state: Mutex<BrokerState>,
}

impl MqttBroker {
    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Self::new_inner(limits, None)
    }

    pub fn new_with_metrics(limits: Arc<Limits>, metrics: Arc<Metrics>) -> Arc<Self> {
        Self::new_inner(limits, Some(metrics))
    }

    fn new_inner(limits: Arc<Limits>, metrics: Option<Arc<Metrics>>) -> Arc<Self> {
        let session_idle_ttl_ms =
            i64::try_from(limits.mqtt_session_idle_ttl_ms).unwrap_or(i64::MAX);
        Arc::new(Self {
            limits,
            metrics,
            subscription_count: AtomicUsize::new(0),
            state: Mutex::new(BrokerState {
                sessions: HashMap::new(),
                session_usage: HashMap::new(),
                tenant_usage: HashMap::new(),
                device_subscription_count: HashMap::new(),
                session_expiry: DeadlineIndex::default(),
                message_expiry: DeadlineIndex::default(),
                retained_expiry: DeadlineIndex::default(),
                session_idle_ttl_ms,
                active: HashMap::new(),
                trie: SubscriptionTrie::default(),
                retained: HashMap::new(),
                generation: 0,
                operation_id: 0,
                subscription_count: 0,
                session_bytes: 0,
                offline_count: 0,
                offline_bytes: 0,
                retained_bytes: 0,
                retained_reserved_count: 0,
                retained_reserved_bytes: 0,
                retained_reserved_tenants: HashMap::new(),
                pending_wills: VecDeque::new(),
                will_responsibility_count: 0,
                will_responsibility_bytes: 0,
                will_responsibility_tenants: HashMap::new(),
                pending_by_tenant: HashMap::new(),
                pending_sessions: HashSet::new(),
            }),
        })
    }

    pub fn attach(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        client_id: String,
        clean_session: bool,
    ) -> Result<Attachment> {
        self.attach_profile(
            auth,
            client_id,
            clean_session,
            MqttVersion::V311,
            0,
            u16::MAX,
        )
    }

    pub fn attach_v5(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        client_id: String,
        clean_start: bool,
        session_expiry_interval: u32,
        receive_maximum: u16,
    ) -> Result<Attachment> {
        self.attach_profile(
            auth,
            client_id,
            clean_start,
            MqttVersion::V5,
            session_expiry_interval,
            receive_maximum,
        )
    }

    fn attach_profile(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        client_id: String,
        clean_session: bool,
        version: MqttVersion,
        session_expiry_interval: u32,
        receive_maximum: u16,
    ) -> Result<Attachment> {
        if receive_maximum == 0 {
            return Err(Error::Invalid);
        }
        let key = SessionKey {
            device: auth.device_key.clone(),
            client_id,
        };
        let mut state = lock(&self.state)?;
        self.prune_expired(&mut state, HOT_MAINTENANCE_BUDGET)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        if session_expiry_deadline(&state, &key).is_some_and(|deadline| deadline <= now_ms()) {
            remove_session(&mut state, &key)?;
        }
        if message_expiry_due(&state, &key, now_ms()) {
            prune_expired_messages_for_session(&mut state, &key, now_ms())?;
        }
        let authorization = SessionAuthorization::from(auth);
        if clean_session {
            for pending in &mut state.pending_wills {
                if pending
                    .cancel_on_resume
                    .as_ref()
                    .is_some_and(|(owner, _)| owner == &key)
                {
                    pending.due_at_ms = None;
                    pending.cancel_on_resume = None;
                }
            }
            retry_pending_wills(&mut state, &self.limits);
        }
        if clean_session {
            remove_session(&mut state, &key)?;
        } else if state.sessions.get(&key).is_some_and(|session| {
            session.version != version || session.authorization.as_ref() != Some(&authorization)
        }) {
            // A persistent session is valid only under the authorization profile that created
            // it. Reauthentication with changed provenance starts a fresh MQTT session.
            remove_session(&mut state, &key)?;
        }
        let session_present = !clean_session && state.sessions.contains_key(&key);
        if !state.sessions.contains_key(&key) {
            state.generation = state.generation.wrapping_add(1).max(1);
            let mut session = StoredSession::new(key.clone(), state.generation, authorization);
            session.version = version;
            session.session_expiry_interval = session_expiry_interval;
            self.check_new_session(&state, &key, session.state_bytes)?;
            state.session_bytes = state.session_bytes.saturating_add(session.state_bytes);
            state.sessions.insert(key.clone(), session);
            sync_session_usage(&mut state, &key)?;
        }
        retry_pending_wills(&mut state, &self.limits);
        let incarnation = state.sessions.get(&key).ok_or(Error::Internal)?.incarnation;
        let mut keep_wills = VecDeque::with_capacity(state.pending_wills.len());
        while let Some(pending) = state.pending_wills.pop_front() {
            if pending
                .cancel_on_resume
                .as_ref()
                .is_some_and(|(owner, prior)| owner == &key && *prior == incarnation)
            {
                release_retained_reservation(
                    &mut state,
                    &pending.owner.tenant_id,
                    pending.retained_reservation,
                );
                release_will_capacity(
                    &mut state,
                    &pending.owner.tenant_id,
                    pending.message.bytes(),
                );
            } else {
                keep_wills.push_back(pending);
            }
        }
        state.pending_wills = keep_wills;
        if let Some(old) = state.active.remove(&key) {
            old.cancel.cancel();
        }
        state.generation = state.generation.wrapping_add(1).max(1);
        let generation = state.generation;
        let capacity = self.limits.max_outbound_messages_per_connection;
        let (sender, receiver) = mpsc::channel(capacity);
        let cancel = CancellationToken::new();
        state.active.insert(
            key.clone(),
            ActiveSession {
                generation,
                sender: sender.clone(),
                cancel: cancel.clone(),
            },
        );
        let available_qos1 = self
            .limits
            .max_inflight_qos1_per_tenant
            .saturating_sub(tenant_inflight(&state, &key.device.tenant_id, 1));
        let available_qos2 = self
            .limits
            .max_inflight_qos2_per_tenant
            .saturating_sub(tenant_inflight(&state, &key.device.tenant_id, 2));
        let resumed = {
            let session = state.sessions.get_mut(&key).ok_or(Error::Internal)?;
            session.active_generation = Some(generation);
            session.last_seen_ms = now_ms();
            session.expires_at_ms = None;
            session.session_expiry_interval = session_expiry_interval;
            session.send_quota = receive_maximum;
            session.sent.clear();
            session.send_window.clear();
            session.inbound_window.clear();
            resume_frames(session, &self.limits, available_qos1, available_qos2)
        };
        let (resumed, resumed_count, resumed_bytes) = match resumed {
            Ok(resumed) => resumed,
            Err(error) => {
                state.active.remove(&key);
                if clean_session {
                    remove_session(&mut state, &key)?;
                } else if let Some(session) = state.sessions.get_mut(&key) {
                    session.active_generation = None;
                }
                sync_session_usage(&mut state, &key)?;
                return Err(error);
            }
        };
        state.offline_count = state.offline_count.saturating_sub(resumed_count);
        state.offline_bytes = state.offline_bytes.saturating_sub(resumed_bytes);
        sync_session_usage(&mut state, &key)?;
        if state
            .sessions
            .get(&key)
            .is_some_and(|session| !session.offline.is_empty())
        {
            mark_pending(&mut state, &key);
        }
        let session_incarnation = state.sessions.get(&key).ok_or(Error::Internal)?.incarnation;
        self.publish_subscription_count(&state);
        drop(state);
        let attachment = Attachment {
            key: key.clone(),
            generation,
            session_incarnation,
            session_present,
            receiver,
            cancel,
            broker: self.clone(),
            clean_session,
            attached: true,
        };
        for frame in resumed.into_iter().take(capacity) {
            sender.try_send(frame).map_err(|_| Error::Overloaded)?;
        }
        // The guard owns cleanup for every post-attachment early return.
        Ok(attachment)
    }

    pub fn detach(&self, key: &SessionKey, generation: u64, clean_session: bool) -> Result<()> {
        let mut state = lock(&self.state)?;
        if state
            .active
            .get(key)
            .is_none_or(|active| active.generation != generation)
        {
            return Ok(());
        }
        state.active.remove(key);
        unmark_pending(&mut state, key);
        let clear_session = state.sessions.get(key).is_some_and(|session| {
            if session.version == MqttVersion::V5 {
                session.session_expiry_interval == 0
            } else {
                clean_session
            }
        });
        if clear_session {
            remove_session(&mut state, key)?;
        } else if let Some(session) = state.sessions.get_mut(key) {
            session.active_generation = None;
            session.sent.clear();
            session.send_window.clear();
            session.inbound_window.clear();
            session.last_seen_ms = now_ms();
            if session.version == MqttVersion::V5 && session.session_expiry_interval != u32::MAX {
                session.expires_at_ms = Some(
                    now_ms().saturating_add(i64::from(session.session_expiry_interval) * 1_000),
                );
            }
        }
        sync_session_usage(&mut state, key)?;
        self.publish_subscription_count(&state);
        Ok(())
    }

    pub fn set_v5_disconnect_expiry(
        &self,
        key: &SessionKey,
        generation: u64,
        interval: u32,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.version != MqttVersion::V5
            || (session.session_expiry_interval == 0 && interval != 0)
        {
            return Err(Error::Invalid);
        }
        session.session_expiry_interval = interval;
        Ok(())
    }

    pub fn subscribe(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        qos: u8,
    ) -> Result<u8> {
        self.subscribe_options(key, generation, filter, Subscription::v311(qos))
    }

    pub fn subscribe_v5(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        options: v5::SubscriptionOptions,
    ) -> Result<u8> {
        self.subscribe_options(
            key,
            generation,
            filter,
            Subscription {
                qos: options.qos,
                no_local: options.no_local,
                retain_as_published: options.retain_as_published,
                retain_handling: options.retain_handling,
            },
        )
    }

    fn subscribe_options(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        subscription: Subscription,
    ) -> Result<u8> {
        if subscription.qos > 2
            || subscription.retain_handling > 2
            || !valid_topic(filter, &self.limits, true)
        {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        check_owner(&state, key, generation)?;
        if message_expiry_due(&state, key, now_ms()) {
            prune_expired_messages_for_session(&mut state, key, now_ms())?;
        }
        let replacement = state
            .sessions
            .get(key)
            .is_some_and(|session| session.subscriptions.contains_key(filter));
        let now = now_ms();
        let retained = state
            .retained
            .values()
            .filter(|retained| {
                !retained.message.expired(now)
                    && topic_matches(filter, &retained.message.topic)
                    && !(subscription.no_local && retained.origin.as_ref() == Some(key))
                    && match subscription.retain_handling {
                        0 => true,
                        1 => !replacement,
                        _ => false,
                    }
            })
            .map(|retained| BrokerMessage {
                qos: retained.message.qos.min(subscription.qos),
                retain: true,
                ..retained.message.clone()
            })
            .collect::<Vec<_>>();
        if !replacement {
            let tenant_count = state
                .tenant_usage
                .get(&key.device.tenant_id)
                .map_or(0, |usage| usage.subscription_count);
            let device_count = state
                .device_subscription_count
                .get(&key.device)
                .copied()
                .unwrap_or_default();
            let session = state.sessions.get(key).ok_or(Error::Internal)?;
            if session.subscriptions.len() >= self.limits.max_subscriptions_per_session
                || device_count >= self.limits.max_subscriptions_per_device
                || state.subscription_count >= self.limits.max_subscriptions
                || tenant_count >= self.limits.max_subscriptions_per_tenant
                || session
                    .state_bytes
                    .saturating_add(filter.len() + STATE_OVERHEAD)
                    > self.limits.max_mqtt_session_state_bytes
            {
                return Err(Error::Overloaded);
            }
        }
        // Simulate every retained enqueue against target-session accounting before exposing the
        // subscription in either the session map or trie. This avoids copying global MQTT state.
        let subscription_charge = if replacement {
            0
        } else {
            filter.len() + STATE_OVERHEAD
        };
        let live_frames =
            preflight_retained_replay(&state, key, &retained, subscription_charge, &self.limits)?;
        if state
            .active
            .get(key)
            .is_some_and(|active| active.sender.capacity() < live_frames)
        {
            return Err(Error::Overloaded);
        }
        let before_session = state.sessions.get(key).cloned().ok_or(Error::Internal)?;
        let before_subscription_count = state.subscription_count;
        let before_session_bytes = state.session_bytes;
        let before_offline_count = state.offline_count;
        let before_offline_bytes = state.offline_bytes;
        if !replacement {
            let charge = filter.len() + STATE_OVERHEAD;
            if tenant_total_session_bytes(&state, &key.device.tenant_id).saturating_add(charge)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
                || total_session_bytes(&state).saturating_add(charge)
                    > self.limits.global_mqtt_session_bytes
            {
                return Err(Error::Overloaded);
            }
            state
                .sessions
                .get_mut(key)
                .ok_or(Error::Internal)?
                .state_bytes += charge;
            state.session_bytes += charge;
            state.subscription_count += 1;
        }
        state
            .sessions
            .get_mut(key)
            .ok_or(Error::Internal)?
            .subscriptions
            .insert(filter.to_owned(), subscription);
        state.trie.insert(filter, key.clone(), subscription);
        sync_session_usage(&mut state, key)?;
        for message in retained {
            if let Err(error) = enqueue(&mut state, key, message, &self.limits) {
                // A concurrently closed receiver is the only expected post-preflight failure.
                // Restore all broker metadata; frames queued to a now-closed receiver are dropped
                // with that receiver and cannot create a hidden live subscription.
                state.sessions.insert(key.clone(), before_session.clone());
                state.subscription_count = before_subscription_count;
                state.session_bytes = before_session_bytes;
                state.offline_count = before_offline_count;
                state.offline_bytes = before_offline_bytes;
                state.trie.remove(filter, key);
                if let Some(previous_qos) = before_session.subscriptions.get(filter) {
                    state.trie.insert(filter, key.clone(), *previous_qos);
                }
                sync_session_usage(&mut state, key)?;
                return Err(error);
            }
        }
        self.publish_subscription_count(&state);
        Ok(subscription.qos)
    }

    pub fn unsubscribe(&self, key: &SessionKey, generation: u64, filter: &str) -> Result<bool> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let removed = state
            .sessions
            .get_mut(key)
            .and_then(|session| session.subscriptions.remove(filter));
        if removed.is_some() {
            let charge = filter.len() + STATE_OVERHEAD;
            if let Some(session) = state.sessions.get_mut(key) {
                session.state_bytes = session.state_bytes.saturating_sub(charge);
            }
            state.session_bytes = state.session_bytes.saturating_sub(charge);
            state.subscription_count = state.subscription_count.saturating_sub(1);
            state.trie.remove(filter, key);
            sync_session_usage(&mut state, key)?;
            retry_pending_wills(&mut state, &self.limits);
            self.publish_subscription_count(&state);
        }
        Ok(removed.is_some())
    }

    pub fn route(&self, owner: &DeviceKey, message: BrokerMessage) -> Result<usize> {
        self.route_with_origin(owner, None, message)
    }

    pub fn route_from_session(&self, key: &SessionKey, message: BrokerMessage) -> Result<usize> {
        self.route_with_origin(&key.device, Some(key), message)
    }

    fn route_with_origin(
        &self,
        owner: &DeviceKey,
        origin: Option<&SessionKey>,
        message: BrokerMessage,
    ) -> Result<usize> {
        if !valid_broker_message(&message, &self.limits) {
            return Err(Error::Invalid);
        }
        if message.expired(now_ms()) {
            return Ok(0);
        }
        // Stores happen while holding the broker mutex after the matching trie mutation. A zero
        // observed here therefore linearizes this route before a concurrent subscribe or after a
        // concurrent final unsubscribe. Retained messages still need the mutex for their update.
        if !message.retain && self.subscription_count.load(Ordering::Acquire) == 0 {
            return Ok(0);
        }
        let lock_started = self
            .metrics
            .as_ref()
            .filter(|metrics| metrics.lock_timing_enabled())
            .map(|_| Instant::now());
        let mut state = lock(&self.state)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        let lock_wait_us = lock_started.map(|started| started.elapsed().as_micros() as u64);
        let hold_started = lock_started.map(|_| Instant::now());
        let result = route_locked(&mut state, owner, origin, &message, &self.limits);
        let lock_hold_us = hold_started.map(|started| started.elapsed().as_micros() as u64);
        drop(state);
        if let (Some(metrics), Some(wait), Some(hold)) = (&self.metrics, lock_wait_us, lock_hold_us)
        {
            metrics.observe(Histogram::BrokerLockWait, wait);
            metrics.observe(Histogram::BrokerLockHold, hold);
        }
        result
    }

    pub fn reserve_will(
        self: &Arc<Self>,
        owner: DeviceKey,
        message: BrokerMessage,
    ) -> Result<WillGuard> {
        if !valid_broker_message(&message, &self.limits)
            || message.payload.len() > self.limits.max_will_payload_bytes
        {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        reserve_will_capacity(&mut state, &owner.tenant_id, message.bytes(), &self.limits)?;
        let reservation = if message.retain && !message.payload.is_empty() {
            match reserve_retained(&mut state, &owner.tenant_id, None, &message, &self.limits) {
                Ok(reservation) => reservation,
                Err(error) => {
                    release_will_capacity(&mut state, &owner.tenant_id, message.bytes());
                    return Err(error);
                }
            }
        } else {
            RetainedReservation::default()
        };
        drop(state);
        Ok(WillGuard {
            broker: self.clone(),
            owner,
            message,
            reservation,
            delay: None,
            message_expiry_interval: None,
            armed: false,
            finished: false,
        })
    }

    fn release_will_reservation(
        &self,
        tenant: &TenantId,
        bytes: usize,
        reservation: RetainedReservation,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        if reservation != RetainedReservation::default() {
            release_retained_reservation(&mut state, tenant, reservation);
        }
        // Every accepted Will reserves one bounded broker-owned responsibility slot, even before
        // it becomes pending.
        release_will_capacity(&mut state, tenant, bytes);
        Ok(())
    }

    fn publish_reserved_will(
        &self,
        owner: &DeviceKey,
        message: &BrokerMessage,
        reservation: RetainedReservation,
        message_expiry_interval: Option<u32>,
    ) -> Result<Option<usize>> {
        let mut state = lock(&self.state)?;
        // Only a Will in the delayed cancellation window may be suppressed by a
        // resumed Session. An immediate Will belongs to the closing connection.
        if reservation != RetainedReservation::default() {
            release_retained_reservation(&mut state, &owner.tenant_id, reservation);
        }
        match route_locked(&mut state, owner, None, message, &self.limits) {
            Ok(delivered) => {
                release_will_capacity(&mut state, &owner.tenant_id, message.bytes());
                Ok(Some(delivered))
            }
            Err(error) => {
                // CONNECT already transferred Will ownership to the broker. Preserve it under the
                // capacity reserved at CONNECT and retry only when another broker operation frees
                // resources; never create a task or spin.
                add_retained_reservation(&mut state, &owner.tenant_id, reservation);
                state.pending_wills.push_back(PendingWill {
                    owner: owner.clone(),
                    message: message.clone(),
                    due_at_ms: None,
                    cancel_on_resume: None,
                    message_expiry_interval,
                    retained_reservation: reservation,
                });
                tracing::warn!(%error, "MQTT Will publication deferred under bounded pressure");
                Ok(Some(0))
            }
        }
    }

    fn schedule_reserved_will(
        &self,
        pending: PendingWill,
        old_generation: u64,
    ) -> Result<WillSchedule> {
        let mut state = lock(&self.state)?;
        if let Some((key, incarnation)) = &pending.cancel_on_resume {
            if state
                .sessions
                .get(key)
                .is_none_or(|session| session.incarnation != *incarnation)
            {
                return Ok(WillSchedule::PublishNow);
            }
            if state
                .active
                .get(key)
                .is_some_and(|active| active.generation != old_generation)
            {
                release_retained_reservation(
                    &mut state,
                    &pending.owner.tenant_id,
                    pending.retained_reservation,
                );
                release_will_capacity(
                    &mut state,
                    &pending.owner.tenant_id,
                    pending.message.bytes(),
                );
                return Ok(WillSchedule::Suppressed);
            }
        }
        // The CONNECT reservation already owns the global and tenant capacity for this entry.
        state.pending_wills.push_back(pending);
        Ok(WillSchedule::Delayed)
    }

    pub fn pending_will_count(&self) -> Result<usize> {
        Ok(lock(&self.state)?.pending_wills.len())
    }

    /// MQTT 5 Receive Maximum is scoped to this connection. QoS 1 is processed
    /// serially by the connection; QoS 2 retains a slot until PUBCOMP is sent.
    pub fn inbound_receive_available(
        &self,
        key: &SessionKey,
        generation: u64,
        qos: u8,
        packet_id: u16,
    ) -> Result<bool> {
        let state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get(key).ok_or(Error::Internal)?;
        if qos == 2 && session.inbound_qos2.contains_key(&packet_id) {
            return Ok(true);
        }
        let limit = self
            .limits
            .max_inflight_qos1_per_session
            .min(self.limits.max_inflight_qos2_per_session);
        Ok(session.inbound_window.len() < limit)
    }

    /// Classify a MQTT 5 QoS 2 PUBLISH before authorizing its topic. Only a
    /// transaction still awaiting PUBREL can bypass the second packet's ACL.
    /// The connection processes packets serially; `inbound_qos2` rechecks
    /// ownership and inserts under the broker lock after new-message admission.
    pub(crate) fn classify_inbound_qos2_publish(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<InboundQos2PublishState> {
        if packet_id == 0 {
            return Err(Error::Invalid);
        }
        let state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get(key).ok_or(Error::Internal)?;
        if session.version != MqttVersion::V5 {
            return Err(Error::Invalid);
        }
        Ok(
            if matches!(
                session.inbound_qos2.get(&packet_id),
                Some(InboundQos2State::AwaitPubrel(_))
            ) {
                InboundQos2PublishState::ExistingTransaction
            } else {
                InboundQos2PublishState::NeedsNewMessageAdmission
            },
        )
    }

    pub fn inbound_qos2(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        message: BrokerMessage,
    ) -> Result<bool> {
        if message.qos != 2 {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session_state_bytes = {
            let session = state.sessions.get(key).ok_or(Error::Internal)?;
            if let Some(existing) = session.inbound_qos2.get(&packet_id) {
                if session.version == MqttVersion::V5 {
                    // The accepted Packet Identifier owns the original message until
                    // PUBREL. Repeated PUBLISH contents and DUP do not replace it.
                    return Ok(false);
                }
                return match existing {
                    InboundQos2State::AwaitPubrel(existing)
                    | InboundQos2State::Delivering {
                        message: existing, ..
                    }
                    | InboundQos2State::EventAccepted(existing)
                        if existing == &message =>
                    {
                        Ok(false)
                    }
                    _ => Err(Error::Invalid),
                };
            }
            if !valid_broker_message(&message, &self.limits) {
                return Err(Error::Invalid);
            }
            if session.inbound_qos2.len()
                + session
                    .outbound
                    .values()
                    .filter(|entry| !matches!(entry, OutboundState::AwaitPuback(_)))
                    .count()
                >= self.limits.max_inflight_qos2_per_session
            {
                return Err(Error::Overloaded);
            }
            if session.version == MqttVersion::V5
                && session.inbound_window.len()
                    >= self
                        .limits
                        .max_inflight_qos1_per_session
                        .min(self.limits.max_inflight_qos2_per_session)
            {
                return Err(Error::Overloaded);
            }
            session.state_bytes
        };
        let charge = message.bytes();
        let reservation = reserve_retained(
            &mut state,
            &key.device.tenant_id,
            Some(key),
            &message,
            &self.limits,
        )?;
        if session_state_bytes.saturating_add(charge) > self.limits.max_mqtt_session_state_bytes
            || tenant_inflight(&state, &key.device.tenant_id, 2)
                >= self.limits.max_inflight_qos2_per_tenant
            || tenant_total_session_bytes(&state, &key.device.tenant_id).saturating_add(charge)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            || total_session_bytes(&state).saturating_add(charge)
                > self.limits.global_mqtt_session_bytes
        {
            release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
            return Err(Error::Overloaded);
        }
        {
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes += charge;
            session
                .inbound_qos2
                .insert(packet_id, InboundQos2State::AwaitPubrel(message));
            if session.version == MqttVersion::V5 {
                session.inbound_window.insert(packet_id);
            }
            if reservation != RetainedReservation::default() {
                session.inbound_reservations.insert(packet_id, reservation);
            }
        }
        state.session_bytes += charge;
        sync_session_usage(&mut state, key)?;
        Ok(true)
    }

    pub fn finish_inbound_pubcomp(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        state
            .sessions
            .get_mut(key)
            .ok_or(Error::Internal)?
            .inbound_window
            .remove(&packet_id);
        Ok(())
    }

    pub fn begin_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<InboundQos2Action> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        state.operation_id = state.operation_id.wrapping_add(1).max(1);
        let operation_id = state.operation_id;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let session_incarnation = session.incarnation;
        let Some(entry) = session.inbound_qos2.get_mut(&packet_id) else {
            return Ok(InboundQos2Action::Unknown);
        };
        Ok(match entry {
            InboundQos2State::AwaitPubrel(message) => {
                let message = message.clone();
                *entry = InboundQos2State::Delivering {
                    message: message.clone(),
                    operation_id,
                };
                InboundQos2Action::Deliver {
                    message,
                    session_incarnation,
                    operation_id,
                }
            }
            InboundQos2State::Delivering { .. } => InboundQos2Action::DeliveryInProgress,
            InboundQos2State::EventAccepted(_) => {
                let operation_id = *session
                    .inbound_operations
                    .entry(packet_id)
                    .or_insert(operation_id);
                InboundQos2Action::EventAccepted {
                    session_incarnation,
                    operation_id,
                }
            }
        })
    }

    #[cfg(test)]
    fn inbound_qos2_message(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<Option<(BrokerMessage, bool)>> {
        let state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        Ok(state
            .sessions
            .get(key)
            .and_then(|session| session.inbound_qos2.get(&packet_id))
            .map(|entry| match entry {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. } => (message.clone(), false),
                InboundQos2State::EventAccepted(message) => (message.clone(), true),
            }))
    }

    pub fn finish_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.incarnation != session_incarnation {
            return Err(Error::Conflict);
        }
        let entry = session
            .inbound_qos2
            .get_mut(&packet_id)
            .ok_or(Error::Invalid)?;
        match entry {
            InboundQos2State::Delivering {
                message,
                operation_id: current,
            } if *current == operation_id => {
                *entry = InboundQos2State::EventAccepted(message.clone());
                session.inbound_operations.insert(packet_id, operation_id);
                Ok(())
            }
            InboundQos2State::EventAccepted(_)
                if session.inbound_operations.get(&packet_id) == Some(&operation_id) =>
            {
                Ok(())
            }
            _ => Err(Error::Conflict),
        }
    }

    pub fn abandon_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.incarnation != session_incarnation {
            return Err(Error::Conflict);
        }
        let entry = session
            .inbound_qos2
            .get_mut(&packet_id)
            .ok_or(Error::Invalid)?;
        if let InboundQos2State::Delivering {
            message,
            operation_id: current,
        } = entry
            && *current == operation_id
        {
            *entry = InboundQos2State::AwaitPubrel(message.clone());
            return Ok(());
        }
        Err(Error::Conflict)
    }

    pub fn complete_inbound_qos2(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let message = state
            .sessions
            .get_mut(key)
            .ok_or(Error::Internal)?
            .inbound_qos2
            .remove(&packet_id)
            .map(|state| match state {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. }
                | InboundQos2State::EventAccepted(message) => message,
            });
        if let Some(session) = state.sessions.get_mut(key) {
            session.inbound_operations.remove(&packet_id);
        }
        if let Some(message) = &message {
            let reservation = state
                .sessions
                .get_mut(key)
                .and_then(|session| session.inbound_reservations.remove(&packet_id))
                .unwrap_or_default();
            release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
            let charge = message.bytes();
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
        sync_session_usage(&mut state, key)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(())
    }

    pub fn route_inbound_qos2(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
        owner: &DeviceKey,
    ) -> Result<usize> {
        let mut state = lock(&self.state)?;
        let message = match state
            .sessions
            .get(key)
            .filter(|session| session.incarnation == session_incarnation)
            .and_then(|session| session.inbound_qos2.get(&packet_id))
        {
            Some(InboundQos2State::EventAccepted(message))
                if state
                    .sessions
                    .get(key)
                    .and_then(|session| session.inbound_operations.get(&packet_id))
                    == Some(&operation_id) =>
            {
                message.clone()
            }
            _ => return Err(Error::Conflict),
        };
        let reservation = state
            .sessions
            .get_mut(key)
            .and_then(|session| session.inbound_reservations.remove(&packet_id))
            .unwrap_or_default();
        release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
        let delivered = match route_locked(&mut state, owner, Some(key), &message, &self.limits) {
            Ok(delivered) => delivered,
            Err(error) => {
                let restored = reserve_retained(
                    &mut state,
                    &key.device.tenant_id,
                    Some(key),
                    &message,
                    &self.limits,
                )?;
                if restored != RetainedReservation::default() {
                    state
                        .sessions
                        .get_mut(key)
                        .ok_or(Error::Internal)?
                        .inbound_reservations
                        .insert(packet_id, restored);
                }
                return Err(error);
            }
        };
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.inbound_qos2.remove(&packet_id).is_some() {
            session.inbound_operations.remove(&packet_id);
            let charge = message.bytes();
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
        sync_session_usage(&mut state, key)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(delivered)
    }

    pub fn subscription_qos(&self, key: &SessionKey, topic: &str) -> Result<Option<u8>> {
        let state = lock(&self.state)?;
        Ok(state.sessions.get(key).and_then(|session| {
            session
                .subscriptions
                .iter()
                .filter(|(filter, _)| topic_matches(filter, topic))
                .map(|(_, subscription)| subscription.qos)
                .max()
        }))
    }

    pub fn send_live(&self, key: &SessionKey, message: BrokerMessage) -> Result<()> {
        let mut state = lock(&self.state)?;
        if !state.active.contains_key(key) {
            return Err(Error::Unavailable);
        }
        enqueue(&mut state, key, message, &self.limits)
    }

    pub fn next_offline(&self, key: &SessionKey, generation: u64) -> Result<Option<BrokerFrame>> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        if message_expiry_due(&state, key, now_ms())
            && prune_expired_messages_for_session(&mut state, key, now_ms())?
        {
            wake_tenant_pending(&mut state, &key.device.tenant_id, 1, &self.limits)?;
            wake_tenant_pending(&mut state, &key.device.tenant_id, 2, &self.limits)?;
        }
        if let Some(session) = state.sessions.get_mut(key) {
            for id in session.outbound_order.clone() {
                if session.sent.contains(&id) {
                    continue;
                }
                let Some(outbound) = session.outbound.get(&id) else {
                    continue;
                };
                if !matches!(outbound, OutboundState::AwaitPubcomp(_)) && !session.has_send_quota()
                {
                    break;
                }
                let frame = match outbound {
                    OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message) => {
                        BrokerFrame::Publish(BrokerDelivery {
                            message: message.clone(),
                            packet_id: Some(id),
                            dup: true,
                        })
                    }
                    OutboundState::AwaitPubcomp(_) => BrokerFrame::Pubrel {
                        packet_id: id,
                        dup: true,
                    },
                };
                session.sent.insert(id);
                if !matches!(outbound, OutboundState::AwaitPubcomp(_)) {
                    session.send_window.insert(id);
                }
                return Ok(Some(frame));
            }
        }
        let qos = state
            .sessions
            .get(key)
            .and_then(|session| session.offline.front())
            .map(|message| message.qos);
        if qos.is_some_and(|qos| {
            tenant_inflight(&state, &key.device.tenant_id, qos)
                >= if qos == 1 {
                    self.limits.max_inflight_qos1_per_tenant
                } else {
                    self.limits.max_inflight_qos2_per_tenant
                }
        }) {
            mark_pending(&mut state, key);
            return Ok(None);
        }
        let frame = promote_offline(&mut state, key, &self.limits)?;
        mark_pending(&mut state, key);
        retry_pending_wills(&mut state, &self.limits);
        Ok(frame)
    }

    pub fn puback(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<()> {
        self.complete_outbound(key, generation, packet_id, OutboundAck::Puback)
    }

    /// Atomically crosses the first-transfer boundary for a queued MQTT 5 PUBLISH.
    /// A stale or expired unsent frame is skipped without consuming its Packet Identifier.
    pub fn begin_outbound_transfer(
        &self,
        key: &SessionKey,
        generation: u64,
        delivery: &BrokerDelivery,
    ) -> Result<bool> {
        let packet_id = delivery.packet_id.ok_or(Error::Invalid)?;
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let matching = matches!(
            session.outbound.get(&packet_id),
            Some(OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message))
                if message == &delivery.message
        );
        if !matching {
            return Ok(false);
        }
        if delivery.message.expired(now_ms()) && !session.started_outbound.contains(&packet_id) {
            let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
            let charge = outbound.bytes();
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
            sync_session_usage(&mut state, key)?;
            wake_tenant_pending(
                &mut state,
                &key.device.tenant_id,
                delivery.message.qos,
                &self.limits,
            )?;
            return Ok(false);
        }
        session.started_outbound.insert(packet_id);
        sync_session_usage(&mut state, key)?;
        Ok(true)
    }

    /// Settle one subscriber copy locally when the peer's Maximum Packet Size
    /// cannot carry it. This is not a fabricated protocol acknowledgement.
    pub fn discard_outbound(
        &self,
        key: &SessionKey,
        generation: u64,
        delivery: &BrokerDelivery,
    ) -> Result<bool> {
        let packet_id = delivery.packet_id.ok_or(Error::Invalid)?;
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let matching = matches!(
            session.outbound.get(&packet_id),
            Some(OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message))
                if message == &delivery.message
        );
        if !matching {
            return Ok(false);
        }
        let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(&mut state, key)?;
        wake_tenant_pending(
            &mut state,
            &key.device.tenant_id,
            delivery.message.qos,
            &self.limits,
        )?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(true)
    }

    pub fn pubrec(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<BrokerFrame> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let frame = match session.outbound.get_mut(&packet_id) {
            Some(state @ OutboundState::AwaitPubrec(_)) => {
                let OutboundState::AwaitPubrec(message) = state.clone() else {
                    return Err(Error::Internal);
                };
                *state = OutboundState::AwaitPubcomp(message);
                session.started_outbound.insert(packet_id);
                Ok(BrokerFrame::Pubrel {
                    packet_id,
                    dup: false,
                })
            }
            Some(OutboundState::AwaitPubcomp(_)) => Ok(BrokerFrame::Pubrel {
                packet_id,
                dup: true,
            }),
            Some(OutboundState::AwaitPuback(_)) => Err(Error::Conflict),
            None => Err(Error::Invalid),
        }?;
        sync_session_usage(&mut state, key)?;
        Ok(frame)
    }

    pub fn pubrec_rejected(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        match session.outbound.get(&packet_id) {
            Some(OutboundState::AwaitPubrec(_)) => {}
            Some(_) => return Err(Error::Conflict),
            None => return Err(Error::Invalid),
        }
        let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(&mut state, key)?;
        wake_tenant_pending(&mut state, &key.device.tenant_id, 2, &self.limits)?;
        Ok(())
    }

    pub fn pubcomp(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<()> {
        self.complete_outbound(key, generation, packet_id, OutboundAck::Pubcomp)
    }

    fn complete_outbound(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        ack: OutboundAck,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let Some(outbound) = session.outbound.get(&packet_id) else {
            return Err(Error::Invalid);
        };
        let expected = matches!(
            (outbound, ack),
            (OutboundState::AwaitPuback(_), OutboundAck::Puback)
                | (OutboundState::AwaitPubcomp(_), OutboundAck::Pubcomp)
        );
        if !expected {
            return Err(Error::Invalid);
        }
        let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(&mut state, key)?;
        wake_tenant_pending(
            &mut state,
            &key.device.tenant_id,
            if ack == OutboundAck::Puback { 1 } else { 2 },
            &self.limits,
        )?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<MqttRecoverySnapshot> {
        let state = lock(&self.state)?;
        Ok(MqttRecoverySnapshot {
            format_version: RECOVERY_VERSION,
            snapshot_generation: state.generation,
            sessions: state
                .sessions
                .values()
                .cloned()
                .map(|mut session| {
                    session.active_generation = None;
                    session.inbound_operations.clear();
                    session.inbound_reservations.clear();
                    session
                })
                .collect(),
            retained: state
                .retained
                .iter()
                .map(|(topic, retained)| (topic.clone(), retained.clone()))
                .collect(),
            pending_wills: state.pending_wills.iter().cloned().collect(),
        })
    }

    pub async fn recover_from(&self, directory: &Path) -> Result<bool> {
        let directory = directory.to_path_buf();
        let limits = self.limits.clone();
        let snapshot = tokio::task::spawn_blocking(move || read_recovery(&directory, &limits))
            .await
            .map_err(|_| Error::Internal)??;
        if let Some(snapshot) = snapshot {
            self.restore(snapshot)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn commit_to(self: &Arc<Self>, directory: &Path) -> Result<PathBuf> {
        let directory = directory.to_path_buf();
        let limits = self.limits.clone();
        let broker = self.clone();
        tokio::task::spawn_blocking(move || write_recovery(&directory, &limits, &broker))
            .await
            .map_err(|_| Error::Internal)?
    }

    pub fn restore(&self, snapshot: MqttRecoverySnapshot) -> Result<()> {
        if !matches!(
            snapshot.format_version,
            RECOVERY_VERSION_V1
                | RECOVERY_VERSION_V2
                | RECOVERY_VERSION_V3
                | RECOVERY_VERSION_V4
                | RECOVERY_VERSION
        ) || snapshot.sessions.len() > self.limits.max_persistent_sessions
            || snapshot.retained.len() > self.limits.max_retained_messages
        {
            return Err(Error::Configuration);
        }
        let mut replacement = BrokerState {
            sessions: HashMap::new(),
            session_usage: HashMap::new(),
            tenant_usage: HashMap::new(),
            device_subscription_count: HashMap::new(),
            session_expiry: DeadlineIndex::default(),
            message_expiry: DeadlineIndex::default(),
            retained_expiry: DeadlineIndex::default(),
            session_idle_ttl_ms: i64::try_from(self.limits.mqtt_session_idle_ttl_ms)
                .unwrap_or(i64::MAX),
            active: HashMap::new(),
            trie: SubscriptionTrie::default(),
            retained: HashMap::new(),
            generation: snapshot.snapshot_generation,
            operation_id: 0,
            subscription_count: 0,
            session_bytes: 0,
            offline_count: 0,
            offline_bytes: 0,
            retained_bytes: 0,
            retained_reserved_count: 0,
            retained_reserved_bytes: 0,
            retained_reserved_tenants: HashMap::new(),
            pending_wills: VecDeque::new(),
            will_responsibility_count: 0,
            will_responsibility_bytes: 0,
            will_responsibility_tenants: HashMap::new(),
            pending_by_tenant: HashMap::new(),
            pending_sessions: HashSet::new(),
        };
        for mut session in snapshot.sessions {
            if snapshot.format_version < RECOVERY_VERSION_V4 && session.version != MqttVersion::V311
            {
                return Err(Error::Invalid);
            }
            if session.version == MqttVersion::V5 {
                if session.session_expiry_interval == 0 {
                    continue;
                }
                if session.session_expiry_interval != u32::MAX {
                    let expiry = session.expires_at_ms.ok_or(Error::Invalid)?;
                    if expiry <= now_ms() {
                        continue;
                    }
                } else if session.expires_at_ms.is_some() {
                    return Err(Error::Invalid);
                }
            } else if session.expires_at_ms.is_some() || session.session_expiry_interval != 0 {
                return Err(Error::Invalid);
            }
            session.active_generation = None;
            session.inbound_operations.clear();
            session.inbound_reservations.clear();
            if session.incarnation == 0 {
                replacement.generation = replacement.generation.wrapping_add(1).max(1);
                session.incarnation = replacement.generation;
            }
            for entry in session.inbound_qos2.values_mut() {
                if let InboundQos2State::Delivering { message, .. } = entry {
                    *entry = InboundQos2State::AwaitPubrel(message.clone());
                }
            }
            if session.outbound_order.is_empty() && !session.outbound.is_empty() {
                let mut packet_ids = session.outbound.keys().copied().collect::<Vec<_>>();
                packet_ids.sort_unstable();
                session.outbound_order = packet_ids.into();
            }
            let ordered_ids = session
                .outbound_order
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>();
            if ordered_ids.len() != session.outbound_order.len()
                || ordered_ids.len() != session.outbound.len()
                || !session
                    .outbound
                    .keys()
                    .all(|packet_id| ordered_ids.contains(packet_id))
            {
                return Err(Error::Invalid);
            }
            if session.next_packet_id == 0
                || session.offline.iter().any(|message| {
                    !matches!(message.qos, 1 | 2) || !valid_broker_message(message, &self.limits)
                })
                || session.inbound_qos2.iter().any(|(packet_id, inbound)| {
                    *packet_id == 0
                        || match inbound {
                            InboundQos2State::AwaitPubrel(message)
                            | InboundQos2State::Delivering { message, .. }
                            | InboundQos2State::EventAccepted(message) => {
                                message.qos != 2 || !valid_broker_message(message, &self.limits)
                            }
                        }
                })
                || session.outbound.iter().any(|(packet_id, outbound)| {
                    *packet_id == 0
                        || match outbound {
                            OutboundState::AwaitPuback(message) => {
                                message.qos != 1 || !valid_broker_message(message, &self.limits)
                            }
                            OutboundState::AwaitPubrec(message)
                            | OutboundState::AwaitPubcomp(message) => {
                                message.qos != 2 || !valid_broker_message(message, &self.limits)
                            }
                        }
                })
            {
                return Err(Error::Invalid);
            }
            if let Some(authorization) = &session.authorization
                && (session.offline.iter().any(|message| {
                    !session_delivery_acl(
                        &session.key.device,
                        &authorization.permissions,
                        &message.topic,
                        &self.limits,
                    )
                }) || session.outbound.values().any(|outbound| {
                    let message = match outbound {
                        OutboundState::AwaitPuback(message)
                        | OutboundState::AwaitPubrec(message)
                        | OutboundState::AwaitPubcomp(message) => message,
                    };
                    !session_delivery_acl(
                        &session.key.device,
                        &authorization.permissions,
                        &message.topic,
                        &self.limits,
                    )
                }) || session.inbound_qos2.values().any(|inbound| {
                    let message = match inbound {
                        InboundQos2State::AwaitPubrel(message)
                        | InboundQos2State::Delivering { message, .. }
                        | InboundQos2State::EventAccepted(message) => message,
                    };
                    !device_publish_topic(
                        &session.key.device,
                        &message.topic,
                        Some(&authorization.permissions),
                    )
                }))
            {
                return Err(Error::Invalid);
            }
            if replacement.sessions.contains_key(&session.key) {
                return Err(Error::Invalid);
            }
            let recorded_offline_bytes =
                session.offline.iter().try_fold(0usize, |total, message| {
                    total.checked_add(message.bytes()).ok_or(Error::Overloaded)
                })?;
            if recorded_offline_bytes != session.offline_bytes
                || recorded_offline_bytes > self.limits.max_offline_bytes_per_session
            {
                return Err(Error::Invalid);
            }
            let now = now_ms();
            session.offline.retain(|message| !message.expired(now));
            session.offline_bytes = session.offline.iter().try_fold(0usize, |total, message| {
                total.checked_add(message.bytes()).ok_or(Error::Overloaded)
            })?;
            if !session
                .started_outbound
                .iter()
                .all(|id| session.outbound.contains_key(id))
            {
                return Err(Error::Invalid);
            }
            let expired_outbound = session
                .outbound
                .iter()
                .filter_map(|(id, outbound)| match outbound {
                    OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message)
                        if message.expired(now) && !session.started_outbound.contains(id) =>
                    {
                        Some(*id)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            for id in expired_outbound {
                session.remove_outbound(id);
            }
            let tenant_sessions = replacement
                .sessions
                .keys()
                .filter(|key| key.device.tenant_id == session.key.device.tenant_id)
                .count();
            let session_qos1 = session
                .outbound
                .values()
                .filter(|state| matches!(state, OutboundState::AwaitPuback(_)))
                .count();
            let session_qos2 = session.inbound_qos2.len()
                + session
                    .outbound
                    .values()
                    .filter(|state| !matches!(state, OutboundState::AwaitPuback(_)))
                    .count();
            if tenant_sessions >= self.limits.max_persistent_sessions_per_tenant
                || session.subscriptions.len() > self.limits.max_subscriptions_per_session
                || session.offline.len() > self.limits.max_offline_messages_per_session
                || session_qos1 > self.limits.max_inflight_qos1_per_session
                || session_qos2 > self.limits.max_inflight_qos2_per_session
                || tenant_inflight(&replacement, &session.key.device.tenant_id, 1)
                    .saturating_add(session_qos1)
                    > self.limits.max_inflight_qos1_per_tenant
                || tenant_inflight(&replacement, &session.key.device.tenant_id, 2)
                    .saturating_add(session_qos2)
                    > self.limits.max_inflight_qos2_per_tenant
            {
                return Err(Error::Overloaded);
            }
            let base = session.key.client_id.len()
                + session.key.device.tenant_id.as_str().len()
                + session.key.device.product_id.as_str().len()
                + session.key.device.device_id.as_str().len()
                + STATE_OVERHEAD;
            let subscription_bytes =
                session
                    .subscriptions
                    .keys()
                    .try_fold(0usize, |total, filter| {
                        total
                            .checked_add(filter.len() + STATE_OVERHEAD)
                            .ok_or(Error::Overloaded)
                    })?;
            let inbound_bytes =
                session
                    .inbound_qos2
                    .values()
                    .try_fold(0usize, |total, inbound| match inbound {
                        InboundQos2State::AwaitPubrel(message)
                        | InboundQos2State::Delivering { message, .. }
                        | InboundQos2State::EventAccepted(message) => {
                            total.checked_add(message.bytes()).ok_or(Error::Overloaded)
                        }
                    })?;
            let outbound_bytes = session
                .outbound
                .values()
                .try_fold(0usize, |total, outbound| {
                    total.checked_add(outbound.bytes()).ok_or(Error::Overloaded)
                })?;
            session.state_bytes = base
                .checked_add(subscription_bytes)
                .and_then(|value| value.checked_add(session.offline_bytes))
                .and_then(|value| value.checked_add(inbound_bytes))
                .and_then(|value| value.checked_add(outbound_bytes))
                .ok_or(Error::Overloaded)?;
            if session.state_bytes > self.limits.max_mqtt_session_state_bytes {
                return Err(Error::Overloaded);
            }
            if tenant_session_bytes(&replacement, &session.key.device.tenant_id)
                .saturating_add(session.state_bytes)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            {
                return Err(Error::Overloaded);
            }
            replacement.subscription_count = replacement
                .subscription_count
                .checked_add(session.subscriptions.len())
                .ok_or(Error::Overloaded)?;
            replacement.offline_count = replacement
                .offline_count
                .checked_add(session.offline.len())
                .ok_or(Error::Overloaded)?;
            replacement.offline_bytes = replacement
                .offline_bytes
                .checked_add(session.offline_bytes)
                .ok_or(Error::Overloaded)?;
            replacement.session_bytes = replacement
                .session_bytes
                .checked_add(session.state_bytes)
                .ok_or(Error::Overloaded)?;
            for (filter, subscription) in &session.subscriptions {
                if !valid_topic(filter, &self.limits, true)
                    || subscription.qos > 2
                    || subscription.retain_handling > 2
                    || session.authorization.as_ref().is_some_and(|authorization| {
                        !session_subscribe_acl(
                            &session.key.device,
                            &authorization.permissions,
                            filter,
                            &self.limits,
                        )
                    })
                {
                    return Err(Error::Invalid);
                }
                if session
                    .authorization
                    .as_ref()
                    .is_some_and(authorization_complete)
                {
                    replacement
                        .trie
                        .insert(filter, session.key.clone(), *subscription);
                }
            }
            let key = session.key.clone();
            replacement.sessions.insert(key.clone(), session);
            sync_session_usage(&mut replacement, &key)?;
        }
        for (topic, retained) in snapshot.retained {
            if topic != retained.message.topic
                || !valid_broker_message(&retained.message, &self.limits)
                || !retained_topic_owner_acl(&retained.tenant_id, &topic)
                || retained.origin.as_ref().is_some_and(|origin| {
                    origin.device.tenant_id != retained.tenant_id
                        || origin.client_id.len() > self.limits.max_client_id_bytes
                        || !device_publish_topic(&origin.device, &topic, None)
                })
                || !retained.message.retain
                || retained.message.payload.is_empty()
                || retained.message.payload.len() > self.limits.max_retained_message_bytes
            {
                return Err(Error::Invalid);
            }
            if retained.message.expired(now_ms()) {
                continue;
            }
            let tenant_count = replacement
                .retained
                .values()
                .filter(|entry| entry.tenant_id == retained.tenant_id)
                .count();
            let tenant_bytes = replacement
                .retained
                .values()
                .filter(|entry| entry.tenant_id == retained.tenant_id)
                .map(RetainedMessage::bytes)
                .sum::<usize>();
            if tenant_count >= self.limits.max_retained_messages_per_tenant
                || tenant_bytes.saturating_add(retained.bytes())
                    > self.limits.max_retained_bytes_per_tenant
            {
                return Err(Error::Overloaded);
            }
            replacement.retained_bytes = replacement
                .retained_bytes
                .checked_add(retained.bytes())
                .ok_or(Error::Overloaded)?;
            let deadline = retained.message.properties.expires_at_ms;
            replacement.retained.insert(topic.clone(), retained);
            replacement.retained_expiry.update(topic, deadline);
        }
        for mut pending in snapshot.pending_wills {
            if !valid_broker_message(&pending.message, &self.limits)
                || pending.message.payload.len() > self.limits.max_will_payload_bytes
                || !device_publish_topic(&pending.owner, &pending.message.topic, None)
                || pending.due_at_ms.is_some() != pending.cancel_on_resume.is_some()
                || pending
                    .cancel_on_resume
                    .as_ref()
                    .is_some_and(|(key, incarnation)| {
                        key.device != pending.owner
                            || *incarnation == 0
                            || key.client_id.len() > self.limits.max_client_id_bytes
                    })
            {
                return Err(Error::Invalid);
            }
            if pending.message.expired(now_ms()) {
                continue;
            }
            reserve_will_capacity(
                &mut replacement,
                &pending.owner.tenant_id,
                pending.message.bytes(),
                &self.limits,
            )?;
            pending.retained_reservation = reserve_retained(
                &mut replacement,
                &pending.owner.tenant_id,
                None,
                &pending.message,
                &self.limits,
            )?;
            replacement.pending_wills.push_back(pending);
        }
        let reservations = replacement
            .sessions
            .values()
            .flat_map(|session| {
                session
                    .inbound_qos2
                    .keys()
                    .map(|packet_id| (session.key.clone(), *packet_id))
            })
            .collect::<Vec<_>>();
        for (key, packet_id) in reservations {
            let message = match replacement
                .sessions
                .get(&key)
                .and_then(|session| session.inbound_qos2.get(&packet_id))
                .ok_or(Error::Internal)?
            {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. }
                | InboundQos2State::EventAccepted(message) => message.clone(),
            };
            let reservation = reserve_retained(
                &mut replacement,
                &key.device.tenant_id,
                Some(&key),
                &message,
                &self.limits,
            )?;
            if reservation != RetainedReservation::default() {
                replacement
                    .sessions
                    .get_mut(&key)
                    .ok_or(Error::Internal)?
                    .inbound_reservations
                    .insert(packet_id, reservation);
            }
        }
        if replacement.subscription_count > self.limits.max_subscriptions
            || replacement.offline_count > self.limits.max_offline_messages
            || replacement.offline_bytes > self.limits.max_offline_bytes
            || replacement.session_bytes > self.limits.global_mqtt_session_bytes
            || replacement.retained_bytes > self.limits.max_retained_bytes
        {
            return Err(Error::Overloaded);
        }
        retry_pending_wills(&mut replacement, &self.limits);
        let mut state = lock(&self.state)?;
        *state = replacement;
        self.publish_subscription_count(&state);
        Ok(())
    }

    pub fn usage(&self) -> Result<(usize, usize, usize, usize, usize)> {
        let state = lock(&self.state)?;
        Ok((
            state.sessions.len(),
            state.session_bytes,
            state.subscription_count,
            state.retained.len(),
            state.retained_bytes,
        ))
    }

    /// One bounded maintenance pass. The server owns a single periodic task for this broker.
    pub fn tick(&self) -> Result<()> {
        let mut state = lock(&self.state)?;
        self.prune_expired(&mut state, TICK_MAINTENANCE_BUDGET)?;
        let released_tenants =
            prune_expired_messages(&mut state, now_ms(), TICK_MAINTENANCE_BUDGET)?;
        for tenant in released_tenants {
            wake_tenant_pending(&mut state, &tenant, 1, &self.limits)?;
            wake_tenant_pending(&mut state, &tenant, 2, &self.limits)?;
        }
        retry_pending_wills(&mut state, &self.limits);
        self.publish_subscription_count(&state);
        Ok(())
    }

    /// Invalidates bounded persistent MQTT state together with the authentication cache/session
    /// boundary. No credentials are retained; only authorization provenance is matched.
    pub fn invalidate_sessions(&self, invalidation: &AuthInvalidation) -> Result<usize> {
        let mut state = lock(&self.state)?;
        let keys = state
            .sessions
            .values()
            .filter(|session| match invalidation {
                AuthInvalidation::Device { device } => &session.key.device == device,
                AuthInvalidation::Product {
                    tenant_id,
                    product_id,
                } => {
                    &session.key.device.tenant_id == tenant_id
                        && &session.key.device.product_id == product_id
                }
                AuthInvalidation::Tenant { tenant_id } => {
                    &session.key.device.tenant_id == tenant_id
                }
                AuthInvalidation::CredentialVersion { version } => session
                    .authorization
                    .as_ref()
                    .is_none_or(|authorization| authorization.credential_version == *version),
                AuthInvalidation::AuthGeneration { generation } => session
                    .authorization
                    .as_ref()
                    .is_none_or(|authorization| authorization.auth_generation == *generation),
                AuthInvalidation::All => true,
            })
            .map(|session| session.key.clone())
            .collect::<Vec<_>>();
        for key in &keys {
            remove_session(&mut state, key)?;
        }
        retry_pending_wills(&mut state, &self.limits);
        self.publish_subscription_count(&state);
        Ok(keys.len())
    }

    /// Read-only diagnostics used by benchmarks and operational capacity probes.
    pub fn matching_subscription_count(&self, topic: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        Ok(state.trie.matching(topic).len())
    }

    pub fn matching_retained_count(&self, filter: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        let now = now_ms();
        Ok(state
            .retained
            .values()
            .filter(|entry| {
                !entry.message.expired(now) && topic_matches(filter, &entry.message.topic)
            })
            .count())
    }

    pub fn has_retained_topic(&self, topic: &str) -> Result<bool> {
        Ok(lock(&self.state)?
            .retained
            .get(topic)
            .is_some_and(|entry| !entry.message.expired(now_ms())))
    }

    fn check_new_session(
        &self,
        state: &BrokerState,
        key: &SessionKey,
        state_bytes: usize,
    ) -> Result<()> {
        let tenant = state
            .tenant_usage
            .get(&key.device.tenant_id)
            .map_or(0, |usage| usage.session_count);
        if state.sessions.len() >= self.limits.max_persistent_sessions
            || tenant >= self.limits.max_persistent_sessions_per_tenant
            || tenant_total_session_bytes(state, &key.device.tenant_id).saturating_add(state_bytes)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            || total_session_bytes(state).saturating_add(state_bytes)
                > self.limits.global_mqtt_session_bytes
        {
            return Err(Error::Overloaded);
        }
        Ok(())
    }

    fn publish_subscription_count(&self, state: &BrokerState) {
        self.subscription_count
            .store(state.subscription_count, Ordering::Release);
    }

    fn prune_expired(&self, state: &mut BrokerState, budget: usize) -> Result<()> {
        let now = now_ms();
        for _ in 0..budget {
            let Some(key) = state.session_expiry.pop_due(now) else {
                break;
            };
            if session_expiry_deadline(state, &key).is_some_and(|deadline| deadline <= now) {
                remove_session(state, &key)?;
            } else {
                sync_session_usage(state, &key)?;
            }
        }
        Ok(())
    }
}

fn check_owner(state: &BrokerState, key: &SessionKey, generation: u64) -> Result<()> {
    if state
        .active
        .get(key)
        .is_some_and(|active| active.generation == generation)
    {
        Ok(())
    } else {
        Err(Error::Conflict)
    }
}

fn mark_pending(state: &mut BrokerState, key: &SessionKey) {
    let eligible = state.active.contains_key(key)
        && state
            .sessions
            .get(key)
            .is_some_and(|session| !session.offline.is_empty());
    if eligible && state.pending_sessions.insert(key.clone()) {
        state
            .pending_by_tenant
            .entry(key.device.tenant_id.clone())
            .or_default()
            .push_back(key.clone());
    }
}

fn unmark_pending(state: &mut BrokerState, key: &SessionKey) {
    if state.pending_sessions.remove(key)
        && let Some(queue) = state.pending_by_tenant.get_mut(&key.device.tenant_id)
    {
        queue.retain(|candidate| candidate != key);
        if queue.is_empty() {
            state.pending_by_tenant.remove(&key.device.tenant_id);
        }
    }
}

fn promote_offline(
    state: &mut BrokerState,
    key: &SessionKey,
    limits: &Limits,
) -> Result<Option<BrokerFrame>> {
    let (frame, bytes) = {
        let session = state.sessions.get_mut(key).ok_or(Error::Unavailable)?;
        let Some(message) = session.offline.front().cloned() else {
            return Ok(None);
        };
        if !session.has_outbound_capacity(message.qos, limits) || !session.has_send_quota() {
            return Ok(None);
        }
        let message = session.offline.pop_front().ok_or(Error::Internal)?;
        let bytes = message.bytes();
        session.offline_bytes = session.offline_bytes.saturating_sub(bytes);
        let id = session.allocate_packet_id()?;
        let outbound = if message.qos == 1 {
            OutboundState::AwaitPuback(message.clone())
        } else {
            OutboundState::AwaitPubrec(message.clone())
        };
        session.insert_outbound(id, outbound);
        session.sent.insert(id);
        session.send_window.insert(id);
        (
            BrokerFrame::Publish(BrokerDelivery {
                message,
                packet_id: Some(id),
                dup: false,
            }),
            bytes,
        )
    };
    state.offline_count = state.offline_count.saturating_sub(1);
    state.offline_bytes = state.offline_bytes.saturating_sub(bytes);
    sync_session_usage(state, key)?;
    Ok(Some(frame))
}

fn wake_tenant_pending(
    state: &mut BrokerState,
    tenant: &TenantId,
    qos: u8,
    limits: &Limits,
) -> Result<()> {
    let mut pending = state.pending_by_tenant.remove(tenant).unwrap_or_default();
    let attempts = pending.len();
    for _ in 0..attempts {
        let Some(key) = pending.pop_front() else {
            break;
        };
        state.pending_sessions.remove(&key);
        let tenant_limit = if qos == 1 {
            limits.max_inflight_qos1_per_tenant
        } else {
            limits.max_inflight_qos2_per_tenant
        };
        if message_expiry_due(state, &key, now_ms()) {
            prune_expired_messages_for_session(state, &key, now_ms())?;
        }
        let next_qos = state
            .sessions
            .get(&key)
            .and_then(|session| session.offline.front())
            .map(|message| message.qos);
        let sender = state.active.get(&key).map(|active| active.sender.clone());
        if next_qos != Some(qos)
            || sender.as_ref().is_none_or(|sender| sender.capacity() == 0)
            || tenant_inflight(state, tenant, qos) >= tenant_limit
        {
            mark_pending(state, &key);
            continue;
        }
        let Some(frame) = promote_offline(state, &key, limits)? else {
            mark_pending(state, &key);
            continue;
        };
        if sender.ok_or(Error::Internal)?.try_send(frame).is_err() {
            // The capacity check and send happen while holding the broker lock, so failure can
            // only mean the receiver closed. Cancel it; the durable outbound state will resume on
            // the next attachment.
            if let Some(active) = state.active.get(&key) {
                active.cancel.cancel();
            }
        }
        mark_pending(state, &key);
    }
    Ok(())
}

fn remove_session(state: &mut BrokerState, key: &SessionKey) -> Result<()> {
    unmark_pending(state, key);
    if let Some(active) = state.active.remove(key) {
        active.cancel.cancel()
    }
    if let Some(session) = state.sessions.remove(key) {
        for reservation in session.inbound_reservations.values() {
            release_retained_reservation(state, &key.device.tenant_id, *reservation);
        }
        for filter in session.subscriptions.keys() {
            state.trie.remove(filter, key)
        }
        state.subscription_count = state
            .subscription_count
            .saturating_sub(session.subscriptions.len());
        state.offline_count = state.offline_count.saturating_sub(session.offline.len());
        state.offline_bytes = state.offline_bytes.saturating_sub(session.offline_bytes);
        state.session_bytes = state.session_bytes.saturating_sub(session.state_bytes);
    }
    sync_session_usage(state, key)
}

fn prune_expired_messages(
    state: &mut BrokerState,
    now: i64,
    budget: usize,
) -> Result<HashSet<TenantId>> {
    let mut released_tenants = HashSet::new();
    for _ in 0..budget {
        let Some(key) = state.message_expiry.pop_due(now) else {
            break;
        };
        if prune_expired_messages_for_session(state, &key, now)? {
            released_tenants.insert(key.device.tenant_id.clone());
        }
    }
    for _ in 0..budget {
        let Some(topic) = state.retained_expiry.pop_due(now) else {
            break;
        };
        let Some(retained) = state.retained.get(&topic) else {
            continue;
        };
        if !retained.message.expired(now) {
            state
                .retained_expiry
                .update(topic, retained.message.properties.expires_at_ms);
            continue;
        }
        let remaining = state
            .retained_bytes
            .checked_sub(retained.bytes())
            .ok_or(Error::Internal)?;
        state.retained.remove(&topic);
        state.retained_bytes = remaining;
    }
    Ok(released_tenants)
}

/// Expiry work for one session. ACK-driven promotion calls this without scanning
/// unrelated persistent sessions.
fn prune_expired_messages_for_session(
    state: &mut BrokerState,
    key: &SessionKey,
    now: i64,
) -> Result<bool> {
    let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
    let (count, offline_bytes, session_bytes, released_outbound) =
        prune_session_messages(session, now);
    let empty = session.offline.is_empty();
    state.offline_count = state.offline_count.saturating_sub(count);
    state.offline_bytes = state.offline_bytes.saturating_sub(offline_bytes);
    state.session_bytes = state.session_bytes.saturating_sub(session_bytes);
    sync_session_usage(state, key)?;
    if empty {
        unmark_pending(state, key);
    }
    Ok(released_outbound)
}

/// Returns released offline count/bytes, all released state bytes, and whether
/// an unsent outbound QoS exchange freed tenant inflight capacity.
fn prune_session_messages(session: &mut StoredSession, now: i64) -> (usize, usize, usize, bool) {
    let before_count = session.offline.len();
    let mut offline_bytes = 0usize;
    session.offline.retain(|message| {
        if message.expired(now) {
            offline_bytes = offline_bytes.saturating_add(message.bytes());
            false
        } else {
            true
        }
    });
    session.offline_bytes = session.offline_bytes.saturating_sub(offline_bytes);
    let count = before_count.saturating_sub(session.offline.len());
    let mut session_bytes = offline_bytes;
    let expired_ids = session
        .outbound
        .iter()
        .filter_map(|(id, outbound)| match outbound {
            OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message)
                if message.expired(now) && !session.started_outbound.contains(id) =>
            {
                Some(*id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let released_outbound = !expired_ids.is_empty();
    for id in expired_ids {
        if let Some(outbound) = session.remove_outbound(id) {
            session_bytes = session_bytes.saturating_add(outbound.bytes());
        }
    }
    for outbound in session.outbound.values_mut() {
        if let OutboundState::AwaitPubcomp(message) = outbound
            && message.expired(now)
        {
            let before = message.bytes();
            message.payload.clear();
            message.properties = PublishProperties::default();
            session_bytes = session_bytes.saturating_add(before.saturating_sub(message.bytes()));
        }
    }
    session.state_bytes = session.state_bytes.saturating_sub(session_bytes);
    (count, offline_bytes, session_bytes, released_outbound)
}

fn resume_frames(
    session: &mut StoredSession,
    limits: &Limits,
    available_qos1: usize,
    available_qos2: usize,
) -> Result<(Vec<BrokerFrame>, usize, usize)> {
    let mut frames = Vec::new();
    let mut resumed_count = 0usize;
    let mut resumed_bytes = 0usize;
    let mut promoted_qos1 = 0usize;
    let mut promoted_qos2 = 0usize;
    for packet_id in &session.outbound_order {
        if frames.len() >= limits.max_outbound_messages_per_connection {
            break;
        }
        let state = session.outbound.get(packet_id).ok_or(Error::Internal)?;
        if !matches!(state, OutboundState::AwaitPubcomp(_)) && !session.has_send_quota() {
            continue;
        }
        frames.push(match state {
            OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message) => {
                BrokerFrame::Publish(BrokerDelivery {
                    message: message.clone(),
                    packet_id: Some(*packet_id),
                    dup: true,
                })
            }
            OutboundState::AwaitPubcomp(_) => BrokerFrame::Pubrel {
                packet_id: *packet_id,
                dup: true,
            },
        });
        session.sent.insert(*packet_id);
        if !matches!(state, OutboundState::AwaitPubcomp(_)) {
            session.send_window.insert(*packet_id);
        }
    }
    while frames.len() < limits.max_outbound_messages_per_connection {
        if !session.has_send_quota() {
            break;
        }
        let Some(message) = session.offline.pop_front() else {
            break;
        };
        let bytes = message.bytes();
        session.offline_bytes = session.offline_bytes.saturating_sub(bytes);
        resumed_count += 1;
        resumed_bytes += bytes;
        let tenant_capacity = if message.qos == 1 {
            promoted_qos1 < available_qos1
        } else {
            promoted_qos2 < available_qos2
        };
        if !session.has_outbound_capacity(message.qos, limits) || !tenant_capacity {
            session.offline.push_front(message);
            session.offline_bytes = session.offline_bytes.saturating_add(bytes);
            resumed_count = resumed_count.saturating_sub(1);
            resumed_bytes = resumed_bytes.saturating_sub(bytes);
            break;
        }
        let id = session.allocate_packet_id()?;
        if message.qos == 1 {
            promoted_qos1 += 1;
        } else {
            promoted_qos2 += 1;
        }
        let state = if message.qos == 1 {
            OutboundState::AwaitPuback(message.clone())
        } else {
            OutboundState::AwaitPubrec(message.clone())
        };
        session.insert_outbound(id, state);
        session.sent.insert(id);
        session.send_window.insert(id);
        frames.push(BrokerFrame::Publish(BrokerDelivery {
            message,
            packet_id: Some(id),
            dup: false,
        }));
    }
    Ok((frames, resumed_count, resumed_bytes))
}

fn enqueue(
    state: &mut BrokerState,
    key: &SessionKey,
    message: BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.expired(now_ms()) {
        return Ok(());
    }
    let active = state.active.get(key).cloned();
    if active.is_none() {
        return queue_offline(state, key, message, limits);
    }
    let active = active.ok_or(Error::Internal)?;
    if message.qos > 0 {
        let tenant_inflight_limit = if message.qos == 1 {
            limits.max_inflight_qos1_per_tenant
        } else {
            limits.max_inflight_qos2_per_tenant
        };
        if tenant_inflight(state, &key.device.tenant_id, message.qos) >= tenant_inflight_limit {
            return queue_offline(state, key, message, limits);
        }
    }
    let tenant_bytes = tenant_total_session_bytes(state, &key.device.tenant_id);
    let global_bytes = total_session_bytes(state);
    let (frame, packet_id, charge) = {
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if message.qos == 0 {
            (
                BrokerFrame::Publish(BrokerDelivery {
                    message,
                    packet_id: None,
                    dup: false,
                }),
                None,
                0,
            )
        } else {
            if !session.has_outbound_capacity(message.qos, limits) || !session.has_send_quota() {
                return queue_offline(state, key, message, limits);
            }
            let id = session.allocate_packet_id()?;
            let charge = message.bytes();
            if session.state_bytes.saturating_add(charge) > limits.max_mqtt_session_state_bytes
                || tenant_bytes.saturating_add(charge)
                    > limits.max_mqtt_session_state_bytes_per_tenant
                || global_bytes.saturating_add(charge) > limits.global_mqtt_session_bytes
            {
                return Err(Error::Overloaded);
            }
            let outbound = if message.qos == 1 {
                OutboundState::AwaitPuback(message.clone())
            } else {
                OutboundState::AwaitPubrec(message.clone())
            };
            session.insert_outbound(id, outbound);
            session.sent.insert(id);
            session.send_window.insert(id);
            session.state_bytes += charge;
            state.session_bytes += charge;
            sync_session_usage(state, key)?;
            (
                BrokerFrame::Publish(BrokerDelivery {
                    message,
                    packet_id: Some(id),
                    dup: false,
                }),
                Some(id),
                charge,
            )
        }
    };
    if active.sender.try_send(frame.clone()).is_ok() {
        return Ok(());
    }
    active.cancel.cancel();
    if packet_id.is_some() {
        // The protocol state is already durable in this session. A receiver can close between
        // preflight and try_send; retaining the outbound entry preserves responsibility for a
        // persistent reconnect and avoids a second fallible queue transition.
        return Ok(());
    }
    if let Some(id) = packet_id
        && let Some(session) = state.sessions.get_mut(key)
    {
        session.remove_outbound(id);
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(state, key)?;
    }
    match frame {
        BrokerFrame::Publish(delivery) if delivery.message.qos > 0 => {
            queue_offline(state, key, delivery.message, limits)
        }
        _ => Err(Error::Overloaded),
    }
}

/// Applies the same bounded-state decisions as `enqueue` to a target-session accounting copy,
/// without touching the global broker or an active connection channel.
fn preflight_retained_replay(
    state: &BrokerState,
    key: &SessionKey,
    messages: &[BrokerMessage],
    subscription_charge: usize,
    limits: &Limits,
) -> Result<usize> {
    let mut session = state.sessions.get(key).cloned().ok_or(Error::Internal)?;
    session.state_bytes = session
        .state_bytes
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    let mut tenant_state_bytes = tenant_total_session_bytes(state, &key.device.tenant_id)
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    let mut global_state_bytes = total_session_bytes(state)
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    if session.state_bytes > limits.max_mqtt_session_state_bytes
        || tenant_state_bytes > limits.max_mqtt_session_state_bytes_per_tenant
        || global_state_bytes > limits.global_mqtt_session_bytes
    {
        return Err(Error::Overloaded);
    }
    let mut tenant_qos1 = tenant_inflight(state, &key.device.tenant_id, 1);
    let mut tenant_qos2 = tenant_inflight(state, &key.device.tenant_id, 2);
    let mut tenant_offline_count = state
        .tenant_usage
        .get(&key.device.tenant_id)
        .map_or(0, |usage| usage.offline_count);
    let mut tenant_offline_bytes = state
        .tenant_usage
        .get(&key.device.tenant_id)
        .map_or(0, |usage| usage.offline_bytes);
    let mut global_offline_count = state.offline_count;
    let mut global_offline_bytes = state.offline_bytes;
    let mut live_frames = 0usize;
    for message in messages {
        if message.qos == 0 {
            live_frames = live_frames.checked_add(1).ok_or(Error::Overloaded)?;
            continue;
        }
        let current_tenant_inflight = if message.qos == 1 {
            tenant_qos1
        } else {
            tenant_qos2
        };
        let tenant_limit = if message.qos == 1 {
            limits.max_inflight_qos1_per_tenant
        } else {
            limits.max_inflight_qos2_per_tenant
        };
        let use_offline = current_tenant_inflight >= tenant_limit
            || !session.has_outbound_capacity(message.qos, limits)
            || !session.has_send_quota();
        let charge = message.bytes();
        if session.state_bytes.saturating_add(charge) > limits.max_mqtt_session_state_bytes
            || tenant_state_bytes.saturating_add(charge)
                > limits.max_mqtt_session_state_bytes_per_tenant
            || global_state_bytes.saturating_add(charge) > limits.global_mqtt_session_bytes
        {
            return Err(Error::Overloaded);
        }
        if use_offline {
            if session.offline.len() >= limits.max_offline_messages_per_session
                || session.offline_bytes.saturating_add(charge)
                    > limits.max_offline_bytes_per_session
                || tenant_offline_count >= limits.max_offline_messages_per_tenant
                || tenant_offline_bytes.saturating_add(charge) > limits.max_offline_bytes_per_tenant
                || global_offline_count >= limits.max_offline_messages
                || global_offline_bytes.saturating_add(charge) > limits.max_offline_bytes
            {
                return Err(Error::Overloaded);
            }
            session.offline.push_back(message.clone());
            session.offline_bytes += charge;
            tenant_offline_count += 1;
            tenant_offline_bytes += charge;
            global_offline_count += 1;
            global_offline_bytes += charge;
        } else {
            let packet_id = session.allocate_packet_id()?;
            session.insert_outbound(
                packet_id,
                if message.qos == 1 {
                    OutboundState::AwaitPuback(message.clone())
                } else {
                    OutboundState::AwaitPubrec(message.clone())
                },
            );
            session.sent.insert(packet_id);
            session.send_window.insert(packet_id);
            if message.qos == 1 {
                tenant_qos1 += 1;
            } else {
                tenant_qos2 += 1;
            }
            live_frames = live_frames.checked_add(1).ok_or(Error::Overloaded)?;
        }
        session.state_bytes += charge;
        tenant_state_bytes += charge;
        global_state_bytes += charge;
    }
    Ok(live_frames)
}

fn queue_offline(
    state: &mut BrokerState,
    key: &SessionKey,
    message: BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.qos == 0 {
        return Ok(());
    }
    let bytes = message.bytes();
    let tenant_id = key.device.tenant_id.clone();
    let tenant_count = state
        .tenant_usage
        .get(&tenant_id)
        .map_or(0, |usage| usage.offline_count);
    let tenant_bytes = state
        .tenant_usage
        .get(&tenant_id)
        .map_or(0, |usage| usage.offline_bytes);
    let tenant_state_bytes = tenant_total_session_bytes(state, &key.device.tenant_id);
    let global_state_bytes = total_session_bytes(state);
    let session = state.sessions.get_mut(key).ok_or(Error::Unavailable)?;
    if session.offline.len() >= limits.max_offline_messages_per_session
        || session.offline_bytes.saturating_add(bytes) > limits.max_offline_bytes_per_session
        || tenant_count >= limits.max_offline_messages_per_tenant
        || tenant_bytes.saturating_add(bytes) > limits.max_offline_bytes_per_tenant
        || state.offline_count >= limits.max_offline_messages
        || state.offline_bytes.saturating_add(bytes) > limits.max_offline_bytes
        || session.state_bytes.saturating_add(bytes) > limits.max_mqtt_session_state_bytes
        || tenant_state_bytes.saturating_add(bytes) > limits.max_mqtt_session_state_bytes_per_tenant
        || global_state_bytes.saturating_add(bytes) > limits.global_mqtt_session_bytes
    {
        return Err(Error::Overloaded);
    }
    session.offline.push_back(message);
    session.offline_bytes += bytes;
    session.state_bytes += bytes;
    state.offline_count += 1;
    state.offline_bytes += bytes;
    state.session_bytes += bytes;
    sync_session_usage(state, key)?;
    mark_pending(state, key);
    Ok(())
}

fn route_locked(
    state: &mut BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<usize> {
    let now = now_ms();
    if message.expired(now) {
        return Ok(0);
    }
    if state.message_expiry.has_due(now) {
        for key in state.trie.matching(&message.topic).into_keys() {
            prune_expired_messages_for_session(state, &key, now)?;
        }
    }
    let plan = preflight_route(state, owner, origin, message, limits)?;
    if message.retain {
        update_retained(state, owner, origin, message, limits)?;
    }
    let mut delivered = 0usize;
    for target in plan.targets {
        let routed = BrokerMessage {
            qos: target.qos,
            retain: target.retain_as_published && message.retain,
            ..message.clone()
        };
        match target.mode {
            PlannedRouteMode::Qos0 => {
                let Some(active) = state.active.get(&target.key).cloned() else {
                    continue;
                };
                let frame = BrokerFrame::Publish(BrokerDelivery {
                    message: routed,
                    packet_id: None,
                    dup: false,
                });
                if active.sender.try_send(frame).is_ok() {
                    delivered += 1;
                } else if active.sender.is_closed() {
                    active.cancel.cancel();
                }
            }
            PlannedRouteMode::Live { packet_id } => {
                let charge = routed.bytes();
                let session = state.sessions.get_mut(&target.key).ok_or(Error::Internal)?;
                session.next_packet_id = if packet_id == u16::MAX {
                    1
                } else {
                    packet_id + 1
                };
                session.insert_outbound(
                    packet_id,
                    if target.qos == 1 {
                        OutboundState::AwaitPuback(routed.clone())
                    } else {
                        OutboundState::AwaitPubrec(routed.clone())
                    },
                );
                session.sent.insert(packet_id);
                session.send_window.insert(packet_id);
                session.state_bytes += charge;
                state.session_bytes += charge;
                sync_session_usage(state, &target.key)?;
                let frame = BrokerFrame::Publish(BrokerDelivery {
                    message: routed,
                    packet_id: Some(packet_id),
                    dup: false,
                });
                if let Some(active) = state.active.get(&target.key)
                    && active.sender.try_send(frame).is_err()
                {
                    active.cancel.cancel();
                }
                delivered += 1;
            }
            PlannedRouteMode::Offline => {
                let charge = routed.bytes();
                let session = state.sessions.get_mut(&target.key).ok_or(Error::Internal)?;
                session.offline.push_back(routed);
                session.offline_bytes += charge;
                session.state_bytes += charge;
                state.offline_count += 1;
                state.offline_bytes += charge;
                state.session_bytes += charge;
                sync_session_usage(state, &target.key)?;
                mark_pending(state, &target.key);
                delivered += 1;
            }
        }
    }
    Ok(delivered)
}

#[derive(Clone, Copy)]
enum PlannedRouteMode {
    Qos0,
    Live { packet_id: u16 },
    Offline,
}

struct PlannedRouteTarget {
    key: SessionKey,
    qos: u8,
    retain_as_published: bool,
    mode: PlannedRouteMode,
}

struct RoutePlan {
    targets: Vec<PlannedRouteTarget>,
}

impl RoutePlan {
    #[cfg(test)]
    fn temporary_bytes(&self) -> usize {
        self.targets
            .capacity()
            .saturating_mul(std::mem::size_of::<PlannedRouteTarget>())
            .saturating_add(self.targets.iter().fold(0usize, |total, target| {
                total
                    .saturating_add(target.key.client_id.len())
                    .saturating_add(target.key.device.tenant_id.as_str().len())
                    .saturating_add(target.key.device.product_id.as_str().len())
                    .saturating_add(target.key.device.device_id.as_str().len())
            }))
    }
}

#[derive(Clone, Copy, Default)]
struct TenantRouteUsage {
    session_bytes: usize,
    offline_count: usize,
    offline_bytes: usize,
    qos1_inflight: usize,
    qos2_inflight: usize,
}

fn retry_pending_wills(state: &mut BrokerState, limits: &Limits) -> usize {
    let attempts = state.pending_wills.len();
    let mut settled = 0usize;
    for _ in 0..attempts {
        let Some(mut pending) = state.pending_wills.pop_front() else {
            break;
        };
        if pending.due_at_ms.is_some_and(|due| due > now_ms()) {
            state.pending_wills.push_back(pending);
            continue;
        }
        pending.due_at_ms = None;
        pending.cancel_on_resume = None;
        if let Some(expiry) = pending.message_expiry_interval.take() {
            pending.message.properties.expires_at_ms =
                Some(now_ms().saturating_add(i64::from(expiry) * 1_000));
        }
        release_retained_reservation(
            state,
            &pending.owner.tenant_id,
            pending.retained_reservation,
        );
        match route_locked(state, &pending.owner, None, &pending.message, limits) {
            Ok(_) => {
                release_will_capacity(state, &pending.owner.tenant_id, pending.message.bytes());
                settled += 1;
            }
            Err(error) => {
                add_retained_reservation(
                    state,
                    &pending.owner.tenant_id,
                    pending.retained_reservation,
                );
                state.pending_wills.push_back(pending);
                tracing::debug!(%error, "pending MQTT Will remains blocked by bounded pressure");
            }
        }
    }
    settled
}

fn preflight_route(
    state: &BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<RoutePlan> {
    if message.retain {
        check_retained_update(state, owner, origin, message, limits)?;
    }
    let matches = state.trie.matching(&message.topic);
    let relevant_tenants = matches
        .keys()
        .map(|key| key.device.tenant_id.clone())
        .collect::<HashSet<_>>();
    let mut tenant_usage = relevant_tenants
        .into_iter()
        .map(|tenant| {
            let stored = state.tenant_usage.get(&tenant).copied().unwrap_or_default();
            (
                tenant,
                TenantRouteUsage {
                    session_bytes: stored.session_bytes,
                    offline_count: stored.offline_count,
                    offline_bytes: stored.offline_bytes,
                    qos1_inflight: stored.qos1_inflight,
                    qos2_inflight: stored.qos2_inflight,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    for (tenant, usage) in &state.will_responsibility_tenants {
        if let Some(projected) = tenant_usage.get_mut(tenant) {
            projected.session_bytes = projected.session_bytes.saturating_add(usage.1);
        }
    }
    let mut global_state_bytes = total_session_bytes(state);
    let mut global_offline_count = state.offline_count;
    let mut global_offline_bytes = state.offline_bytes;
    let mut plan = RoutePlan {
        targets: Vec::with_capacity(matches.len()),
    };

    for (key, subscription) in matches {
        if subscription.no_local && origin == Some(&key) {
            continue;
        }
        let qos = message.qos.min(subscription.qos);
        if qos == 0 {
            plan.targets.push(PlannedRouteTarget {
                key,
                qos,
                retain_as_published: subscription.retain_as_published,
                mode: PlannedRouteMode::Qos0,
            });
            continue;
        }
        let session = state.sessions.get(&key).ok_or(Error::Internal)?;
        let charge = message.bytes();
        let tenant = key.device.tenant_id.clone();
        let usage = tenant_usage.get_mut(&tenant).ok_or(Error::Internal)?;
        let active_live = state
            .active
            .get(&key)
            .is_some_and(|active| !active.sender.is_closed() && active.sender.capacity() > 0);
        let inflight = if qos == 1 {
            usage.qos1_inflight
        } else {
            usage.qos2_inflight
        };
        let inflight_limit = if qos == 1 {
            limits.max_inflight_qos1_per_tenant
        } else {
            limits.max_inflight_qos2_per_tenant
        };
        let use_live = active_live
            && inflight < inflight_limit
            && session.has_outbound_capacity(qos, limits)
            && session.has_send_quota();
        if session.state_bytes.saturating_add(charge) > limits.max_mqtt_session_state_bytes
            || usage.session_bytes.saturating_add(charge)
                > limits.max_mqtt_session_state_bytes_per_tenant
            || global_state_bytes.saturating_add(charge) > limits.global_mqtt_session_bytes
        {
            return Err(Error::Overloaded);
        }
        let mode = if use_live {
            let packet_id = projected_packet_id(session)?;
            if qos == 1 {
                usage.qos1_inflight += 1;
            } else {
                usage.qos2_inflight += 1;
            }
            PlannedRouteMode::Live { packet_id }
        } else {
            if session.offline.len() >= limits.max_offline_messages_per_session
                || session.offline_bytes.saturating_add(charge)
                    > limits.max_offline_bytes_per_session
                || usage.offline_count >= limits.max_offline_messages_per_tenant
                || usage.offline_bytes.saturating_add(charge) > limits.max_offline_bytes_per_tenant
                || global_offline_count >= limits.max_offline_messages
                || global_offline_bytes.saturating_add(charge) > limits.max_offline_bytes
            {
                return Err(Error::Overloaded);
            }
            global_offline_count += 1;
            global_offline_bytes += charge;
            usage.offline_count += 1;
            usage.offline_bytes += charge;
            PlannedRouteMode::Offline
        };
        usage.session_bytes += charge;
        global_state_bytes += charge;
        plan.targets.push(PlannedRouteTarget {
            key,
            qos,
            retain_as_published: subscription.retain_as_published,
            mode,
        });
    }
    Ok(plan)
}

fn projected_packet_id(session: &StoredSession) -> Result<u16> {
    let mut candidate = session.next_packet_id;
    for _ in 0..u16::MAX {
        if !session.outbound.contains_key(&candidate) {
            return Ok(candidate);
        }
        candidate = if candidate == u16::MAX {
            1
        } else {
            candidate + 1
        };
    }
    Err(Error::Overloaded)
}

fn check_retained_update(
    state: &BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.payload.is_empty() {
        return Ok(());
    }
    if message.payload.len() > limits.max_retained_message_bytes {
        return Err(Error::Overloaded);
    }
    let existing = state.retained.get(&message.topic);
    let old_bytes = existing.map_or(0, RetainedMessage::bytes);
    let old_same_tenant = existing.is_some_and(|old| old.tenant_id == owner.tenant_id);
    let tenant_count = state
        .retained
        .values()
        .filter(|entry| entry.tenant_id == owner.tenant_id)
        .count();
    let tenant_bytes = state
        .retained
        .values()
        .filter(|entry| entry.tenant_id == owner.tenant_id)
        .map(RetainedMessage::bytes)
        .sum::<usize>();
    let new_bytes = retained_charge(message, origin);
    let (reserved_count, reserved_bytes) = state
        .retained_reserved_tenants
        .get(&owner.tenant_id)
        .copied()
        .unwrap_or_default();
    if (existing.is_none()
        && state
            .retained
            .len()
            .saturating_add(state.retained_reserved_count)
            >= limits.max_retained_messages)
        || (!old_same_tenant
            && tenant_count.saturating_add(reserved_count)
                >= limits.max_retained_messages_per_tenant)
        || state
            .retained_bytes
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes)
            .saturating_add(state.retained_reserved_bytes)
            > limits.max_retained_bytes
        || tenant_bytes
            .saturating_sub(if old_same_tenant { old_bytes } else { 0 })
            .saturating_add(new_bytes)
            .saturating_add(reserved_bytes)
            > limits.max_retained_bytes_per_tenant
    {
        return Err(Error::Overloaded);
    }
    Ok(())
}

fn update_retained(
    state: &mut BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.payload.is_empty() {
        if let Some(old) = state.retained.remove(&message.topic) {
            state.retained_bytes = state.retained_bytes.saturating_sub(old.bytes());
        }
        state.retained_expiry.update(message.topic.clone(), None);
        return Ok(());
    }
    check_retained_update(state, owner, origin, message, limits)?;
    let old_bytes = state
        .retained
        .get(&message.topic)
        .map_or(0, RetainedMessage::bytes);
    let new_bytes = retained_charge(message, origin);
    state.retained_bytes = state
        .retained_bytes
        .saturating_sub(old_bytes)
        .saturating_add(new_bytes);
    state.retained.insert(
        message.topic.clone(),
        RetainedMessage {
            tenant_id: owner.tenant_id.clone(),
            message: message.clone(),
            origin: origin.cloned(),
        },
    );
    state
        .retained_expiry
        .update(message.topic.clone(), message.properties.expires_at_ms);
    Ok(())
}

fn reserve_retained(
    state: &mut BrokerState,
    tenant: &TenantId,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<RetainedReservation> {
    if !message.retain || message.payload.is_empty() {
        return Ok(RetainedReservation::default());
    }
    if message.payload.len() > limits.max_retained_message_bytes {
        return Err(Error::Overloaded);
    }
    let bytes = retained_charge(message, origin);
    let existing = state.retained.get(&message.topic);
    let old_bytes = existing.map_or(0, RetainedMessage::bytes);
    let same_tenant = existing.is_some_and(|entry| &entry.tenant_id == tenant);
    let reservation = RetainedReservation {
        global_count: usize::from(existing.is_none()),
        global_bytes: bytes.saturating_sub(old_bytes),
        tenant_count: usize::from(existing.is_none() || !same_tenant),
        tenant_bytes: if same_tenant {
            bytes.saturating_sub(old_bytes)
        } else {
            bytes
        },
    };
    let tenant_count = state
        .retained
        .values()
        .filter(|entry| &entry.tenant_id == tenant)
        .count();
    let tenant_bytes = state
        .retained
        .values()
        .filter(|entry| &entry.tenant_id == tenant)
        .map(RetainedMessage::bytes)
        .sum::<usize>();
    let reserved = state
        .retained_reserved_tenants
        .get(tenant)
        .copied()
        .unwrap_or_default();
    if state
        .retained
        .len()
        .saturating_add(state.retained_reserved_count)
        .saturating_add(reservation.global_count)
        > limits.max_retained_messages
        || state
            .retained_bytes
            .saturating_add(state.retained_reserved_bytes)
            .saturating_add(reservation.global_bytes)
            > limits.max_retained_bytes
        || tenant_count
            .saturating_add(reserved.0)
            .saturating_add(reservation.tenant_count)
            > limits.max_retained_messages_per_tenant
        || tenant_bytes
            .saturating_add(reserved.1)
            .saturating_add(reservation.tenant_bytes)
            > limits.max_retained_bytes_per_tenant
    {
        return Err(Error::Overloaded);
    }
    state.retained_reserved_count += reservation.global_count;
    state.retained_reserved_bytes += reservation.global_bytes;
    let tenant_reserved = state
        .retained_reserved_tenants
        .entry(tenant.clone())
        .or_default();
    tenant_reserved.0 += reservation.tenant_count;
    tenant_reserved.1 += reservation.tenant_bytes;
    Ok(reservation)
}

fn add_retained_reservation(
    state: &mut BrokerState,
    tenant: &TenantId,
    reservation: RetainedReservation,
) {
    if reservation == RetainedReservation::default() {
        return;
    }
    state.retained_reserved_count = state
        .retained_reserved_count
        .saturating_add(reservation.global_count);
    state.retained_reserved_bytes = state
        .retained_reserved_bytes
        .saturating_add(reservation.global_bytes);
    let tenant_reserved = state
        .retained_reserved_tenants
        .entry(tenant.clone())
        .or_default();
    tenant_reserved.0 = tenant_reserved.0.saturating_add(reservation.tenant_count);
    tenant_reserved.1 = tenant_reserved.1.saturating_add(reservation.tenant_bytes);
}

fn reserve_will_capacity(
    state: &mut BrokerState,
    tenant: &TenantId,
    bytes: usize,
    limits: &Limits,
) -> Result<()> {
    let tenant_usage = state
        .will_responsibility_tenants
        .get(tenant)
        .copied()
        .unwrap_or_default();
    if state.will_responsibility_count >= limits.max_connections
        || state
            .session_bytes
            .saturating_add(state.will_responsibility_bytes)
            .saturating_add(bytes)
            > limits.global_mqtt_session_bytes
        || tenant_usage.0 >= limits.max_connections_per_tenant
        || tenant_session_bytes(state, tenant)
            .saturating_add(tenant_usage.1)
            .saturating_add(bytes)
            > limits.max_mqtt_session_state_bytes_per_tenant
    {
        return Err(Error::Overloaded);
    }
    state.will_responsibility_count += 1;
    state.will_responsibility_bytes += bytes;
    let tenant_usage = state
        .will_responsibility_tenants
        .entry(tenant.clone())
        .or_default();
    tenant_usage.0 += 1;
    tenant_usage.1 += bytes;
    Ok(())
}

fn release_will_capacity(state: &mut BrokerState, tenant: &TenantId, bytes: usize) {
    debug_assert!(state.will_responsibility_count > 0);
    debug_assert!(state.will_responsibility_bytes >= bytes);
    state.will_responsibility_count = state.will_responsibility_count.saturating_sub(1);
    state.will_responsibility_bytes = state.will_responsibility_bytes.saturating_sub(bytes);
    if let Some(tenant_usage) = state.will_responsibility_tenants.get_mut(tenant) {
        debug_assert!(tenant_usage.0 > 0);
        debug_assert!(tenant_usage.1 >= bytes);
        tenant_usage.0 = tenant_usage.0.saturating_sub(1);
        tenant_usage.1 = tenant_usage.1.saturating_sub(bytes);
        if *tenant_usage == (0, 0) {
            state.will_responsibility_tenants.remove(tenant);
        }
    }
}

fn release_retained_reservation(
    state: &mut BrokerState,
    tenant: &TenantId,
    reservation: RetainedReservation,
) {
    if reservation == RetainedReservation::default() {
        return;
    }
    debug_assert!(state.retained_reserved_count >= reservation.global_count);
    debug_assert!(state.retained_reserved_bytes >= reservation.global_bytes);
    state.retained_reserved_count = state
        .retained_reserved_count
        .saturating_sub(reservation.global_count);
    state.retained_reserved_bytes = state
        .retained_reserved_bytes
        .saturating_sub(reservation.global_bytes);
    if let Some(reserved) = state.retained_reserved_tenants.get_mut(tenant) {
        debug_assert!(reserved.0 >= reservation.tenant_count);
        debug_assert!(reserved.1 >= reservation.tenant_bytes);
        reserved.0 = reserved.0.saturating_sub(reservation.tenant_count);
        reserved.1 = reserved.1.saturating_sub(reservation.tenant_bytes);
        if *reserved == (0, 0) {
            state.retained_reserved_tenants.remove(tenant);
        }
    }
}

pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter == "#" || filter == "+" || filter.starts_with("+/")) {
        return false;
    }
    let filters = filter.split('/').collect::<Vec<_>>();
    let topics = topic.split('/').collect::<Vec<_>>();
    let mut at = 0usize;
    while at < filters.len() {
        match filters[at] {
            "#" => return at + 1 == filters.len(),
            "+" if at < topics.len() => {}
            level if at < topics.len() && level == topics[at] => {}
            _ => return false,
        }
        at += 1;
    }
    at == topics.len()
}

fn valid_broker_message(message: &BrokerMessage, limits: &Limits) -> bool {
    message.qos <= 2
        && message.payload.len() <= limits.max_mqtt_packet_size
        && valid_topic(&message.topic, limits, false)
        && message.properties.valid(limits)
        && (message.properties.payload_format != Some(1)
            || std::str::from_utf8(&message.payload).is_ok())
}

pub fn subscribe_acl(auth: &AuthenticatedDevice, filter: &str, limits: &Limits) -> bool {
    session_subscribe_acl(&auth.device_key, &auth.permissions, filter, limits)
}

fn device_topic_root(device: &DeviceKey) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/",
        device.tenant_id.as_str(),
        device.product_id.as_str(),
        device.device_id.as_str()
    )
}

fn session_subscribe_acl(
    device: &DeviceKey,
    permissions: &Permissions,
    filter: &str,
    limits: &Limits,
) -> bool {
    if !valid_topic(filter, limits, true) || !permissions.commands {
        return false;
    }
    let root = device_topic_root(device);
    filter
        .strip_prefix(&root)
        .is_some_and(|suffix| !suffix.is_empty())
}

fn session_delivery_acl(
    device: &DeviceKey,
    permissions: &Permissions,
    topic: &str,
    limits: &Limits,
) -> bool {
    if !valid_topic(topic, limits, false) || !permissions.commands {
        return false;
    }
    let root = device_topic_root(device);
    topic
        .strip_prefix(&root)
        .is_some_and(|suffix| !suffix.is_empty())
}

fn device_publish_topic(
    device: &DeviceKey,
    topic: &str,
    permissions: Option<&Permissions>,
) -> bool {
    let root = device_topic_root(device);
    let Some(suffix) = topic.strip_prefix(&root) else {
        return false;
    };
    match suffix {
        "up" => permissions.is_none_or(|value| value.publish),
        "down_ack" => permissions.is_none_or(|value| value.publish && value.commands),
        _ => false,
    }
}

fn retained_topic_owner_acl(tenant: &TenantId, topic: &str) -> bool {
    let levels = topic.split('/').collect::<Vec<_>>();
    levels.len() == 8
        && levels[0] == "v1"
        && levels[1] == "t"
        && levels[2] == tenant.as_str()
        && levels[3] == "p"
        && ProductId::new(levels[4]).is_ok()
        && levels[5] == "d"
        && DeviceId::new(levels[6]).is_ok()
        && matches!(levels[7], "up" | "down_ack")
}

fn authorization_complete(authorization: &SessionAuthorization) -> bool {
    authorization.codec_id.is_some() && authorization.codec_version.is_some_and(|value| value > 0)
}

const RECORD_SESSION: u8 = 1;
const RECORD_SUBSCRIPTION: u8 = 2;
const RECORD_OFFLINE: u8 = 3;
const RECORD_INBOUND_QOS2: u8 = 4;
const RECORD_OUTBOUND: u8 = 5;
const RECORD_RETAINED: u8 = 6;
const RECORD_PENDING_WILL: u8 = 7;
const RECOVERY_PREFIX_BYTES: usize = 16;
const RECOVERY_HEADER_BYTES: usize = RECOVERY_PREFIX_BYTES + 32;
const RECORD_HEADER_BYTES: usize = 5;
const RECORD_CHECKSUM_BYTES: usize = 32;
const RECOVERY_TRAILER_MAGIC: &[u8; 4] = b"NEND";
const RECOVERY_TRAILER_BYTES: usize = 4 + 8 + 8 + 32;

struct RecoveryWriteState {
    written: usize,
    record_count: u64,
    record_bytes: u64,
    stream_hash: Sha256,
}

fn write_recovery(directory: &Path, limits: &Limits, broker: &MqttBroker) -> Result<PathBuf> {
    fs::create_dir_all(directory).map_err(|_| Error::Storage)?;
    set_directory_permissions(directory)?;
    let temporary = directory.join(format!(".{RECOVERY_FILE}.tmp"));
    let committed = directory.join(RECOVERY_FILE);
    let mut file = open_private_replace(&temporary)?;
    let state = lock(&broker.state)?;
    let mut header = [0u8; RECOVERY_PREFIX_BYTES];
    header[..4].copy_from_slice(RECOVERY_MAGIC);
    header[4..8].copy_from_slice(&RECOVERY_VERSION.to_be_bytes());
    header[8..16].copy_from_slice(&state.generation.to_be_bytes());
    let mut recovery = RecoveryWriteState {
        written: RECOVERY_HEADER_BYTES,
        record_count: 0,
        record_bytes: 0,
        stream_hash: Sha256::new(),
    };
    recovery.stream_hash.update(header);
    file.write_all(&header)
        .and_then(|_| file.write_all(&Sha256::digest(header)))
        .map_err(|_| Error::Storage)?;
    if state.will_responsibility_count != state.pending_wills.len() {
        // Planned shutdown must detach every connection first, transferring every armed Will to
        // either settled or broker-owned pending state before a coherent image is committed.
        return Err(Error::Conflict);
    }
    let mut sessions = state.sessions.values().collect::<Vec<_>>();
    sessions.sort_by(|left, right| {
        (
            left.key.device.tenant_id.as_str(),
            left.key.device.product_id.as_str(),
            left.key.device.device_id.as_str(),
            left.key.client_id.as_str(),
        )
            .cmp(&(
                right.key.device.tenant_id.as_str(),
                right.key.device.product_id.as_str(),
                right.key.device.device_id.as_str(),
                right.key.client_id.as_str(),
            ))
    });
    for session in sessions {
        let mut record = Vec::with_capacity(384);
        encode_session_meta(&mut record, session)?;
        write_record(&mut file, RECORD_SESSION, &record, limits, &mut recovery)?;
        let mut subscriptions = session.subscriptions.iter().collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| left.0.cmp(right.0));
        for (filter, subscription) in subscriptions {
            record.clear();
            put_string(&mut record, filter)?;
            record.push(subscription.qos);
            record.push(
                u8::from(subscription.no_local) << 2
                    | u8::from(subscription.retain_as_published) << 3
                    | subscription.retain_handling << 4,
            );
            write_record(
                &mut file,
                RECORD_SUBSCRIPTION,
                &record,
                limits,
                &mut recovery,
            )?;
        }
        for message in &session.offline {
            record.clear();
            encode_message(&mut record, message)?;
            write_record(&mut file, RECORD_OFFLINE, &record, limits, &mut recovery)?;
        }
        let mut inbound = session.inbound_qos2.iter().collect::<Vec<_>>();
        inbound.sort_by_key(|(packet_id, _)| **packet_id);
        for (packet_id, inbound) in inbound {
            record.clear();
            record.extend_from_slice(&packet_id.to_be_bytes());
            match inbound {
                InboundQos2State::EventAccepted(message) => {
                    record.push(1);
                    encode_message(&mut record, message)?;
                }
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. } => {
                    // Delivery ownership is process-local; restart safely retries PUBREL work.
                    record.push(0);
                    encode_message(&mut record, message)?;
                }
            }
            write_record(
                &mut file,
                RECORD_INBOUND_QOS2,
                &record,
                limits,
                &mut recovery,
            )?;
        }
        for packet_id in &session.outbound_order {
            let outbound = session.outbound.get(packet_id).ok_or(Error::Invalid)?;
            record.clear();
            record.extend_from_slice(&packet_id.to_be_bytes());
            let (kind, message) = match outbound {
                OutboundState::AwaitPuback(message) => (0, message),
                OutboundState::AwaitPubrec(message) => (1, message),
                OutboundState::AwaitPubcomp(message) => (2, message),
            };
            record.push(kind);
            record.push(u8::from(session.started_outbound.contains(packet_id)));
            encode_message(&mut record, message)?;
            write_record(&mut file, RECORD_OUTBOUND, &record, limits, &mut recovery)?;
        }
    }
    for pending in &state.pending_wills {
        let mut record = Vec::with_capacity(pending.message.payload.len().saturating_add(384));
        put_string(&mut record, pending.owner.tenant_id.as_str())?;
        put_string(&mut record, pending.owner.product_id.as_str())?;
        put_string(&mut record, pending.owner.device_id.as_str())?;
        encode_message(&mut record, &pending.message)?;
        if let Some((key, incarnation)) = &pending.cancel_on_resume {
            record.push(1);
            put_string(&mut record, &key.client_id)?;
            record.extend_from_slice(&incarnation.to_be_bytes());
            record.extend_from_slice(&pending.due_at_ms.ok_or(Error::Invalid)?.to_be_bytes());
        } else {
            record.push(0);
        }
        if let Some(expiry) = pending.message_expiry_interval {
            record.push(1);
            record.extend_from_slice(&expiry.to_be_bytes());
        } else {
            record.push(0);
        }
        write_record(
            &mut file,
            RECORD_PENDING_WILL,
            &record,
            limits,
            &mut recovery,
        )?;
    }
    let mut retained = state.retained.values().collect::<Vec<_>>();
    retained.sort_by(|left, right| left.message.topic.cmp(&right.message.topic));
    for retained in retained {
        let mut record = Vec::with_capacity(retained.message.payload.len().saturating_add(384));
        put_string(&mut record, retained.tenant_id.as_str())?;
        encode_message(&mut record, &retained.message)?;
        if let Some(origin) = &retained.origin {
            record.push(1);
            put_string(&mut record, origin.device.tenant_id.as_str())?;
            put_string(&mut record, origin.device.product_id.as_str())?;
            put_string(&mut record, origin.device.device_id.as_str())?;
            put_string(&mut record, &origin.client_id)?;
        } else {
            record.push(0);
        }
        write_record(&mut file, RECORD_RETAINED, &record, limits, &mut recovery)?;
    }
    drop(state);
    recovery.written = recovery
        .written
        .checked_add(RECOVERY_TRAILER_BYTES)
        .ok_or(Error::Overloaded)?;
    if recovery.written > limits.mqtt_recovery_max_bytes {
        return Err(Error::Overloaded);
    }
    let mut trailer = [0u8; RECOVERY_TRAILER_BYTES];
    trailer[..4].copy_from_slice(RECOVERY_TRAILER_MAGIC);
    trailer[4..12].copy_from_slice(&recovery.record_count.to_be_bytes());
    trailer[12..20].copy_from_slice(&recovery.record_bytes.to_be_bytes());
    trailer[20..].copy_from_slice(&recovery.stream_hash.finalize());
    file.write_all(&trailer).map_err(|_| Error::Storage)?;
    file.sync_all().map_err(|_| Error::Storage)?;
    fs::rename(&temporary, &committed).map_err(|_| Error::Storage)?;
    sync_directory(directory)?;
    Ok(committed)
}

fn read_recovery(directory: &Path, limits: &Limits) -> Result<Option<MqttRecoverySnapshot>> {
    let path = directory.join(RECOVERY_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let size = usize::try_from(fs::metadata(&path).map_err(|_| Error::Storage)?.len())
        .map_err(|_| Error::Overloaded)?;
    if size < RECOVERY_PREFIX_BYTES {
        return Err(Error::Invalid);
    }
    let file = fs::File::open(path).map_err(|_| Error::Storage)?;
    decode_recovery_reader(BufReader::new(file), size, limits).map(Some)
}

/// Pure, bounded decoder used by fuzzing. Production uses the same decoder over a buffered file.
pub fn decode_mqtt_recovery(input: &[u8], limits: &Limits) -> Result<MqttRecoverySnapshot> {
    decode_recovery_reader(Cursor::new(input), input.len(), limits)
}

fn decode_recovery_reader(
    mut reader: impl Read,
    size: usize,
    limits: &Limits,
) -> Result<MqttRecoverySnapshot> {
    let mut header = [0u8; RECOVERY_PREFIX_BYTES];
    reader.read_exact(&mut header).map_err(|_| Error::Invalid)?;
    if &header[..4] != RECOVERY_MAGIC {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().map_err(|_| Error::Invalid)?);
    let generation = u64::from_be_bytes(header[8..16].try_into().map_err(|_| Error::Invalid)?);
    let maximum = if version == RECOVERY_VERSION_V1 {
        LEGACY_V1_RECOVERY_READ_MAX
    } else {
        limits.mqtt_recovery_max_bytes
    };
    if size > maximum {
        return Err(Error::Invalid);
    }
    if version == RECOVERY_VERSION_V1 {
        return decode_v1(reader, size, generation, limits);
    }
    if !matches!(
        version,
        RECOVERY_VERSION_V2 | RECOVERY_VERSION_V3 | RECOVERY_VERSION_V4 | RECOVERY_VERSION
    ) {
        return Err(Error::Invalid);
    }
    let mut header_checksum = [0u8; 32];
    reader
        .read_exact(&mut header_checksum)
        .map_err(|_| Error::Invalid)?;
    if Sha256::digest(header).as_slice() != header_checksum {
        return Err(Error::Invalid);
    }
    let mut consumed = RECOVERY_HEADER_BYTES;
    let records_end = if version >= RECOVERY_VERSION_V3 {
        size.checked_sub(RECOVERY_TRAILER_BYTES)
            .filter(|end| *end >= RECOVERY_HEADER_BYTES)
            .ok_or(Error::Invalid)?
    } else {
        size
    };
    let mut stream_hash = Sha256::new();
    stream_hash.update(header);
    let mut record_count = 0u64;
    let mut total_record_bytes = 0u64;
    let mut snapshot = MqttRecoverySnapshot {
        format_version: version,
        snapshot_generation: generation,
        sessions: Vec::new(),
        retained: Vec::new(),
        pending_wills: Vec::new(),
    };
    let mut retained_phase = false;
    while consumed < records_end {
        let mut record_header = [0u8; RECORD_HEADER_BYTES];
        reader
            .read_exact(&mut record_header)
            .map_err(|_| Error::Invalid)?;
        consumed = consumed
            .checked_add(RECORD_HEADER_BYTES)
            .ok_or(Error::Invalid)?;
        let kind = record_header[0];
        if kind == RECORD_RETAINED {
            retained_phase = true;
        } else if retained_phase {
            return Err(Error::Invalid);
        }
        let length = usize::try_from(u32::from_be_bytes(
            record_header[1..5].try_into().map_err(|_| Error::Invalid)?,
        ))
        .map_err(|_| Error::Invalid)?;
        if length > recovery_record_max(limits)?
            || consumed
                .checked_add(length)
                .and_then(|value| value.checked_add(RECORD_CHECKSUM_BYTES))
                .is_none_or(|end| end > records_end)
        {
            return Err(Error::Invalid);
        }
        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .map_err(|_| Error::Invalid)?;
        let mut checksum = [0u8; RECORD_CHECKSUM_BYTES];
        reader
            .read_exact(&mut checksum)
            .map_err(|_| Error::Invalid)?;
        if Sha256::digest(&payload).as_slice() != checksum {
            return Err(Error::Invalid);
        }
        let wire_bytes = RECORD_HEADER_BYTES + length + RECORD_CHECKSUM_BYTES;
        consumed += length + RECORD_CHECKSUM_BYTES;
        if version >= RECOVERY_VERSION_V3 {
            stream_hash.update(record_header);
            stream_hash.update(&payload);
            stream_hash.update(checksum);
            record_count = record_count.checked_add(1).ok_or(Error::Invalid)?;
            total_record_bytes = total_record_bytes
                .checked_add(u64::try_from(wire_bytes).map_err(|_| Error::Invalid)?)
                .ok_or(Error::Invalid)?;
        }
        decode_record(kind, &payload, &mut snapshot, limits, version)?;
    }
    if consumed != records_end {
        return Err(Error::Invalid);
    }
    if version >= RECOVERY_VERSION_V3 {
        let mut trailer = [0u8; RECOVERY_TRAILER_BYTES];
        reader
            .read_exact(&mut trailer)
            .map_err(|_| Error::Invalid)?;
        if &trailer[..4] != RECOVERY_TRAILER_MAGIC
            || u64::from_be_bytes(trailer[4..12].try_into().map_err(|_| Error::Invalid)?)
                != record_count
            || u64::from_be_bytes(trailer[12..20].try_into().map_err(|_| Error::Invalid)?)
                != total_record_bytes
            || stream_hash.finalize().as_slice() != &trailer[20..]
        {
            return Err(Error::Invalid);
        }
    }
    Ok(snapshot)
}

fn decode_v1(
    mut reader: impl Read,
    size: usize,
    generation: u64,
    _limits: &Limits,
) -> Result<MqttRecoverySnapshot> {
    if size < 52 {
        return Err(Error::Invalid);
    }
    let mut length_bytes = [0u8; 4];
    reader
        .read_exact(&mut length_bytes)
        .map_err(|_| Error::Invalid)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| Error::Invalid)?;
    if length.saturating_add(52) != size || length > LEGACY_V1_RECOVERY_READ_MAX {
        return Err(Error::Invalid);
    }
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|_| Error::Invalid)?;
    let mut checksum = [0u8; 32];
    reader
        .read_exact(&mut checksum)
        .map_err(|_| Error::Invalid)?;
    if Sha256::digest(&payload).as_slice() != checksum {
        return Err(Error::Invalid);
    }
    let snapshot: MqttRecoverySnapshot =
        serde_json::from_slice(&payload).map_err(|_| Error::Invalid)?;
    if snapshot.snapshot_generation != generation || snapshot.format_version != version_one() {
        return Err(Error::Invalid);
    }
    Ok(snapshot)
}

const fn version_one() -> u32 {
    RECOVERY_VERSION_V1
}

fn recovery_record_max(limits: &Limits) -> Result<usize> {
    limits
        .max_mqtt_packet_size
        .checked_add(limits.max_topic_bytes.saturating_mul(2))
        .and_then(|value| value.checked_add(limits.max_mqtt_property_bytes))
        .and_then(|value| value.checked_add(2_048))
        .ok_or(Error::Configuration)
}

fn write_record(
    file: &mut fs::File,
    kind: u8,
    payload: &[u8],
    limits: &Limits,
    recovery: &mut RecoveryWriteState,
) -> Result<()> {
    if payload.len() > recovery_record_max(limits)? {
        return Err(Error::Overloaded);
    }
    let record_bytes = RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(RECORD_CHECKSUM_BYTES))
        .ok_or(Error::Overloaded)?;
    recovery.written = recovery
        .written
        .checked_add(record_bytes)
        .ok_or(Error::Overloaded)?;
    if recovery.written.saturating_add(RECOVERY_TRAILER_BYTES) > limits.mqtt_recovery_max_bytes {
        return Err(Error::Overloaded);
    }
    let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
    let mut header = [0u8; RECORD_HEADER_BYTES];
    header[0] = kind;
    header[1..].copy_from_slice(&length.to_be_bytes());
    let checksum = Sha256::digest(payload);
    file.write_all(&header)
        .and_then(|_| file.write_all(payload))
        .and_then(|_| file.write_all(&checksum))
        .map_err(|_| Error::Storage)?;
    recovery.stream_hash.update(header);
    recovery.stream_hash.update(payload);
    recovery.stream_hash.update(checksum);
    recovery.record_count = recovery
        .record_count
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    recovery.record_bytes = recovery
        .record_bytes
        .checked_add(u64::try_from(record_bytes).map_err(|_| Error::Overloaded)?)
        .ok_or(Error::Overloaded)?;
    Ok(())
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u16::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn encode_message(output: &mut Vec<u8>, message: &BrokerMessage) -> Result<()> {
    put_string(output, &message.topic)?;
    put_bytes(output, &message.payload)?;
    output.push(message.qos);
    output.push(u8::from(message.retain));
    let properties = &message.properties;
    output.push(properties.payload_format.unwrap_or(2));
    output.extend_from_slice(&properties.expires_at_ms.unwrap_or(-1).to_be_bytes());
    put_optional_string(output, properties.content_type.as_deref())?;
    put_optional_string(output, properties.response_topic.as_deref())?;
    match &properties.correlation_data {
        Some(value) => {
            output.push(1);
            put_bytes(output, value)?;
        }
        None => output.push(0),
    }
    output.extend_from_slice(
        &u16::try_from(properties.user_properties.len())
            .map_err(|_| Error::Overloaded)?
            .to_be_bytes(),
    );
    for (key, value) in &properties.user_properties {
        put_string(output, key)?;
        put_string(output, value)?;
    }
    Ok(())
}

fn put_optional_string(output: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            output.push(1);
            put_string(output, value)?;
        }
        None => output.push(0),
    }
    Ok(())
}

fn encode_session_meta(output: &mut Vec<u8>, session: &StoredSession) -> Result<()> {
    put_string(output, session.key.device.tenant_id.as_str())?;
    put_string(output, session.key.device.product_id.as_str())?;
    put_string(output, session.key.device.device_id.as_str())?;
    put_string(output, &session.key.client_id)?;
    output.extend_from_slice(&session.incarnation.to_be_bytes());
    if let Some(authorization) = &session.authorization {
        output.push(1);
        output.extend_from_slice(&authorization.credential_version.to_be_bytes());
        output.extend_from_slice(&authorization.auth_generation.to_be_bytes());
        output.push(u8::from(authorization.permissions.publish));
        output.push(u8::from(authorization.permissions.commands));
        let codec_id = authorization.codec_id.as_ref().ok_or(Error::Invalid)?;
        put_string(output, codec_id.as_str())?;
        output.extend_from_slice(
            &authorization
                .codec_version
                .ok_or(Error::Invalid)?
                .to_be_bytes(),
        );
    } else {
        output.push(0);
    }
    output.extend_from_slice(&session.next_packet_id.to_be_bytes());
    output.extend_from_slice(&session.last_seen_ms.to_be_bytes());
    output.push(match session.version {
        MqttVersion::V311 => 4,
        MqttVersion::V5 => 5,
    });
    output.extend_from_slice(&session.session_expiry_interval.to_be_bytes());
    output.extend_from_slice(&session.expires_at_ms.unwrap_or(-1).to_be_bytes());
    Ok(())
}

struct RecordReader<'a> {
    input: &'a [u8],
    at: usize,
}

impl<'a> RecordReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, at: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(length).ok_or(Error::Invalid)?;
        let value = self.input.get(self.at..end).ok_or(Error::Invalid)?;
        self.at = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().ok_or(Error::Invalid)?)
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    fn string(&mut self, maximum: usize) -> Result<String> {
        let length = usize::from(self.u16()?);
        if length == 0 || length > maximum {
            return Err(Error::Invalid);
        }
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| Error::Invalid)
    }

    fn optional_string(&mut self, maximum: usize) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let length = usize::from(self.u16()?);
                if length > maximum {
                    return Err(Error::Invalid);
                }
                let bytes = self.take(length)?;
                Ok(Some(super::packet::valid_utf8(bytes)?.to_owned()))
            }
            _ => Err(Error::Invalid),
        }
    }

    fn string_allow_empty(&mut self, maximum: usize) -> Result<String> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(Error::Invalid);
        }
        Ok(super::packet::valid_utf8(self.take(length)?)?.to_owned())
    }

    fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>> {
        let length = usize::try_from(self.u32()?).map_err(|_| Error::Invalid)?;
        if length > maximum {
            return Err(Error::Invalid);
        }
        Ok(self.take(length)?.to_vec())
    }

    fn finish(self) -> Result<()> {
        if self.at == self.input.len() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }
}

fn decode_message(
    reader: &mut RecordReader<'_>,
    limits: &Limits,
    format_version: u32,
) -> Result<BrokerMessage> {
    let topic = reader.string(limits.max_topic_bytes)?;
    let payload = reader.bytes(limits.max_mqtt_packet_size)?;
    let qos = reader.u8()?;
    let retain = match reader.u8()? {
        0 => false,
        1 => true,
        _ => return Err(Error::Invalid),
    };
    if qos > 2 || !valid_topic(&topic, limits, false) {
        return Err(Error::Invalid);
    }
    let properties = if format_version >= RECOVERY_VERSION_V4 {
        let payload_format = match reader.u8()? {
            value @ (0 | 1) => Some(value),
            2 => None,
            _ => return Err(Error::Invalid),
        };
        let expires_at_ms = match reader.i64()? {
            -1 => None,
            value if value >= 0 => Some(value),
            _ => return Err(Error::Invalid),
        };
        let content_type = reader.optional_string(limits.max_mqtt_content_type_bytes)?;
        let response_topic = reader.optional_string(limits.max_mqtt_response_topic_bytes)?;
        let correlation_data = match reader.u8()? {
            0 => None,
            1 => Some(reader.bytes(limits.max_mqtt_correlation_data_bytes)?),
            _ => return Err(Error::Invalid),
        };
        let count = usize::from(reader.u16()?);
        if count > limits.max_mqtt_user_properties {
            return Err(Error::Overloaded);
        }
        let mut user_properties = Vec::with_capacity(count);
        for _ in 0..count {
            let key = reader.string_allow_empty(limits.max_mqtt_user_property_bytes)?;
            let value = reader.string_allow_empty(limits.max_mqtt_user_property_bytes)?;
            user_properties.push((key, value));
        }
        PublishProperties {
            payload_format,
            expires_at_ms,
            content_type,
            response_topic,
            correlation_data,
            user_properties,
        }
    } else {
        PublishProperties::default()
    };
    Ok(BrokerMessage {
        topic,
        payload,
        qos,
        retain,
        properties,
    })
}

fn decode_record(
    kind: u8,
    payload: &[u8],
    snapshot: &mut MqttRecoverySnapshot,
    limits: &Limits,
    format_version: u32,
) -> Result<()> {
    let mut reader = RecordReader::new(payload);
    match kind {
        RECORD_SESSION => {
            if snapshot.sessions.len() >= limits.max_persistent_sessions {
                return Err(Error::Overloaded);
            }
            let tenant_id = TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let product_id = ProductId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let device_id = DeviceId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let client_id = reader.string(limits.max_client_id_bytes)?;
            let incarnation = reader.u64()?;
            if incarnation == 0 {
                return Err(Error::Invalid);
            }
            let authorization = match reader.u8()? {
                0 => None,
                1 => {
                    let credential_version = reader.u32()?;
                    let auth_generation = reader.u64()?;
                    let permissions = Permissions {
                        publish: match reader.u8()? {
                            0 => false,
                            1 => true,
                            _ => return Err(Error::Invalid),
                        },
                        commands: match reader.u8()? {
                            0 => false,
                            1 => true,
                            _ => return Err(Error::Invalid),
                        },
                    };
                    let (codec_id, codec_version) = if format_version >= RECOVERY_VERSION_V3 {
                        (
                            Some(CodecId::new(reader.string(64)?).map_err(|_| Error::Invalid)?),
                            Some(reader.u16()?),
                        )
                    } else {
                        (None, None)
                    };
                    if codec_version == Some(0) {
                        return Err(Error::Invalid);
                    }
                    Some(SessionAuthorization {
                        credential_version,
                        auth_generation,
                        permissions,
                        codec_id,
                        codec_version,
                    })
                }
                _ => return Err(Error::Invalid),
            };
            let next_packet_id = reader.u16()?;
            if next_packet_id == 0 {
                return Err(Error::Invalid);
            }
            let last_seen_ms = reader.i64()?;
            let (version, session_expiry_interval, expires_at_ms) =
                if format_version >= RECOVERY_VERSION_V4 {
                    let version = match reader.u8()? {
                        4 => MqttVersion::V311,
                        5 => MqttVersion::V5,
                        _ => return Err(Error::Invalid),
                    };
                    let interval = reader.u32()?;
                    let expiry = reader.i64()?;
                    (
                        version,
                        interval,
                        if expiry == -1 { None } else { Some(expiry) },
                    )
                } else {
                    (MqttVersion::V311, 0, None)
                };
            reader.finish()?;
            let key = SessionKey {
                device: DeviceKey {
                    tenant_id,
                    product_id,
                    device_id,
                },
                client_id,
            };
            let mut session = StoredSession::new(
                key,
                incarnation,
                authorization.clone().unwrap_or(SessionAuthorization {
                    credential_version: 0,
                    auth_generation: 0,
                    permissions: Permissions {
                        publish: false,
                        commands: false,
                    },
                    codec_id: None,
                    codec_version: None,
                }),
            );
            session.authorization = authorization;
            session.version = version;
            session.session_expiry_interval = session_expiry_interval;
            session.expires_at_ms = expires_at_ms;
            session.next_packet_id = next_packet_id;
            session.last_seen_ms = last_seen_ms;
            snapshot.sessions.push(session);
        }
        RECORD_SUBSCRIPTION => {
            let filter = reader.string(limits.max_topic_bytes)?;
            let qos = reader.u8()?;
            let options = if format_version >= RECOVERY_VERSION_V4 {
                reader.u8()?
            } else {
                0
            };
            reader.finish()?;
            if qos > 2
                || options & 0xc3 != 0
                || (options >> 4) & 3 > 2
                || !valid_topic(&filter, limits, true)
            {
                return Err(Error::Invalid);
            }
            let subscription = Subscription {
                qos,
                no_local: options & 4 != 0,
                retain_as_published: options & 8 != 0,
                retain_handling: (options >> 4) & 3,
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.subscriptions.len() >= limits.max_subscriptions_per_session {
                return Err(Error::Overloaded);
            }
            if session.subscriptions.insert(filter, subscription).is_some() {
                return Err(Error::Invalid);
            }
        }
        RECORD_OFFLINE => {
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if !matches!(message.qos, 1 | 2) {
                return Err(Error::Invalid);
            }
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.offline.len() >= limits.max_offline_messages_per_session {
                return Err(Error::Overloaded);
            }
            session.offline_bytes = session
                .offline_bytes
                .checked_add(message.bytes())
                .ok_or(Error::Overloaded)?;
            session.offline.push_back(message);
        }
        RECORD_INBOUND_QOS2 => {
            let packet_id = reader.u16()?;
            let stage = reader.u8()?;
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if packet_id == 0 || message.qos != 2 {
                return Err(Error::Invalid);
            }
            let state = match stage {
                0 => InboundQos2State::AwaitPubrel(message),
                1 => InboundQos2State::EventAccepted(message),
                _ => return Err(Error::Invalid),
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.inbound_qos2.len()
                + session
                    .outbound
                    .values()
                    .filter(|state| !matches!(state, OutboundState::AwaitPuback(_)))
                    .count()
                >= limits.max_inflight_qos2_per_session
            {
                return Err(Error::Overloaded);
            }
            if session.inbound_qos2.insert(packet_id, state).is_some() {
                return Err(Error::Invalid);
            }
        }
        RECORD_OUTBOUND => {
            let packet_id = reader.u16()?;
            let stage = reader.u8()?;
            let started = if format_version >= RECOVERY_VERSION {
                match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::Invalid),
                }
            } else {
                // v1-v4 did not record the transfer boundary. Conservatively retain
                // protocol responsibility for every recovered outbound exchange.
                true
            };
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if packet_id == 0
                || (stage == 0 && message.qos != 1)
                || (matches!(stage, 1 | 2) && message.qos != 2)
            {
                return Err(Error::Invalid);
            }
            let state = match stage {
                0 => OutboundState::AwaitPuback(message),
                1 => OutboundState::AwaitPubrec(message),
                2 => OutboundState::AwaitPubcomp(message),
                _ => return Err(Error::Invalid),
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            let same_qos = session
                .outbound
                .values()
                .filter(|state| {
                    if stage == 0 {
                        matches!(state, OutboundState::AwaitPuback(_))
                    } else {
                        !matches!(state, OutboundState::AwaitPuback(_))
                    }
                })
                .count();
            let limit = if stage == 0 {
                limits.max_inflight_qos1_per_session
            } else {
                limits.max_inflight_qos2_per_session
            };
            if same_qos >= limit {
                return Err(Error::Overloaded);
            }
            if session.outbound.insert(packet_id, state).is_some() {
                return Err(Error::Invalid);
            }
            if started {
                session.started_outbound.insert(packet_id);
            }
            session.outbound_order.push_back(packet_id);
        }
        RECORD_PENDING_WILL => {
            if snapshot.pending_wills.len() >= limits.max_connections {
                return Err(Error::Overloaded);
            }
            let owner = DeviceKey {
                tenant_id: TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
                product_id: ProductId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
                device_id: DeviceId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
            };
            let message = decode_message(&mut reader, limits, format_version)?;
            let (due_at_ms, cancel_on_resume, message_expiry_interval) =
                if format_version >= RECOVERY_VERSION_V4 {
                    let (due_at_ms, cancel_on_resume) = match reader.u8()? {
                        0 => (None, None),
                        1 => {
                            let client_id = reader.string(limits.max_client_id_bytes)?;
                            let incarnation = reader.u64()?;
                            let due = reader.i64()?;
                            if incarnation == 0 || due < 0 {
                                return Err(Error::Invalid);
                            }
                            (
                                Some(due),
                                Some((
                                    SessionKey {
                                        device: owner.clone(),
                                        client_id,
                                    },
                                    incarnation,
                                )),
                            )
                        }
                        _ => return Err(Error::Invalid),
                    };
                    let expiry = match reader.u8()? {
                        0 => None,
                        1 => Some(reader.u32()?),
                        _ => return Err(Error::Invalid),
                    };
                    (due_at_ms, cancel_on_resume, expiry)
                } else {
                    (None, None, None)
                };
            reader.finish()?;
            if message.payload.len() > limits.max_will_payload_bytes {
                return Err(Error::Invalid);
            }
            snapshot.pending_wills.push(PendingWill {
                owner,
                message,
                due_at_ms,
                cancel_on_resume,
                message_expiry_interval,
                retained_reservation: RetainedReservation::default(),
            });
        }
        RECORD_RETAINED => {
            if snapshot.retained.len() >= limits.max_retained_messages {
                return Err(Error::Overloaded);
            }
            let tenant = TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let message = decode_message(&mut reader, limits, format_version)?;
            let origin = if format_version >= RECOVERY_VERSION_V4 {
                match reader.u8()? {
                    0 => None,
                    1 => Some(SessionKey {
                        device: DeviceKey {
                            tenant_id: TenantId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                            product_id: ProductId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                            device_id: DeviceId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                        },
                        client_id: reader.string(limits.max_client_id_bytes)?,
                    }),
                    _ => return Err(Error::Invalid),
                }
            } else {
                None
            };
            reader.finish()?;
            if !message.retain || message.payload.is_empty() {
                return Err(Error::Invalid);
            }
            let topic = message.topic.clone();
            if snapshot
                .retained
                .iter()
                .any(|(existing, _)| existing == &topic)
            {
                return Err(Error::Invalid);
            }
            snapshot.retained.push((
                topic,
                RetainedMessage {
                    tenant_id: tenant,
                    message,
                    origin,
                },
            ));
        }
        _ => return Err(Error::Invalid),
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_replace(path: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let _ = fs::remove_file(path);
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| Error::Storage)
}

#[cfg(not(unix))]
fn open_private_replace(path: &Path) -> Result<fs::File> {
    let _ = fs::remove_file(path);
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|_| Error::Storage)
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| Error::Storage)
}

#[cfg(not(unix))]
fn set_directory_permissions(_: &Path) -> Result<()> {
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;
    use std::time::Duration;

    fn assert_deadline_index<K: Clone + Debug + Eq + Hash>(
        index: &DeadlineIndex<K>,
        expected: &HashMap<K, i64>,
    ) {
        assert_eq!(&index.by_key, expected);
        assert_eq!(
            index.by_deadline.values().map(HashSet::len).sum::<usize>(),
            expected.len()
        );
        for (&deadline, keys) in &index.by_deadline {
            for key in keys {
                assert_eq!(expected.get(key), Some(&deadline));
            }
        }
    }

    fn assert_accounting_consistent(state: &BrokerState) {
        let mut sessions = HashMap::new();
        let mut tenants = HashMap::<TenantId, TenantUsage>::new();
        let mut device_subscriptions = HashMap::<DeviceKey, usize>::new();
        let mut global_bytes = 0;
        let mut global_subscriptions = 0;
        let mut global_offline_count = 0;
        let mut global_offline_bytes = 0;
        let mut reserved_count = 0;
        let mut reserved_bytes = 0;
        let mut reserved_tenants = HashMap::<TenantId, (usize, usize)>::new();
        let mut session_expiry = HashMap::new();
        let mut message_expiry = HashMap::new();
        for (key, session) in &state.sessions {
            let usage = SessionUsage::from_session(session);
            sessions.insert(key.clone(), usage);
            let tenant = tenants.entry(key.device.tenant_id.clone()).or_default();
            tenant.session_count += 1;
            tenant.session_bytes += usage.state_bytes;
            tenant.subscription_count += usage.subscriptions;
            tenant.offline_count += usage.offline_count;
            tenant.offline_bytes += usage.offline_bytes;
            tenant.qos1_inflight += usage.qos1_inflight;
            tenant.qos2_inflight += usage.qos2_inflight;
            *device_subscriptions.entry(key.device.clone()).or_default() += usage.subscriptions;
            global_bytes += usage.state_bytes;
            global_subscriptions += usage.subscriptions;
            global_offline_count += usage.offline_count;
            global_offline_bytes += usage.offline_bytes;
            if let Some(deadline) = next_message_expiry(session) {
                message_expiry.insert(key.clone(), deadline);
            }
            if !state.active.contains_key(key) {
                let deadline = if session.version == MqttVersion::V5 {
                    session.expires_at_ms
                } else {
                    Some(
                        session
                            .last_seen_ms
                            .saturating_add(state.session_idle_ttl_ms)
                            .saturating_add(1),
                    )
                };
                if let Some(deadline) = deadline {
                    session_expiry.insert(key.clone(), deadline);
                }
            }
            for reservation in session.inbound_reservations.values() {
                reserved_count += reservation.global_count;
                reserved_bytes += reservation.global_bytes;
                let tenant = reserved_tenants
                    .entry(key.device.tenant_id.clone())
                    .or_default();
                tenant.0 += reservation.tenant_count;
                tenant.1 += reservation.tenant_bytes;
            }
        }
        device_subscriptions.retain(|_, count| *count != 0);
        for pending in &state.pending_wills {
            let reservation = pending.retained_reservation;
            reserved_count += reservation.global_count;
            reserved_bytes += reservation.global_bytes;
            let tenant = reserved_tenants
                .entry(pending.owner.tenant_id.clone())
                .or_default();
            tenant.0 += reservation.tenant_count;
            tenant.1 += reservation.tenant_bytes;
        }
        reserved_tenants.retain(|_, usage| *usage != (0, 0));
        assert_eq!(state.session_usage, sessions);
        assert_eq!(state.tenant_usage, tenants);
        assert_eq!(state.device_subscription_count, device_subscriptions);
        assert_eq!(state.session_bytes, global_bytes);
        assert_eq!(state.subscription_count, global_subscriptions);
        assert_eq!(state.offline_count, global_offline_count);
        assert_eq!(state.offline_bytes, global_offline_bytes);
        assert_eq!(
            state.retained_bytes,
            state
                .retained
                .values()
                .map(RetainedMessage::bytes)
                .sum::<usize>()
        );
        assert_eq!(state.retained_reserved_count, reserved_count);
        assert_eq!(state.retained_reserved_bytes, reserved_bytes);
        assert_eq!(state.retained_reserved_tenants, reserved_tenants);
        let retained_expiry = state
            .retained
            .iter()
            .filter_map(|(topic, retained)| {
                retained
                    .message
                    .properties
                    .expires_at_ms
                    .map(|deadline| (topic.clone(), deadline))
            })
            .collect::<HashMap<_, _>>();
        assert_deadline_index(&state.session_expiry, &session_expiry);
        assert_deadline_index(&state.message_expiry, &message_expiry);
        assert_deadline_index(&state.retained_expiry, &retained_expiry);
    }

    fn assert_broker_accounting(broker: &MqttBroker) {
        let state = lock(&broker.state).unwrap();
        assert_accounting_consistent(&state);
    }

    #[test]
    fn derived_accounting_matches_authoritative_mutation_sequence() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let identity = auth("accounting-sequence");
        let down = "v1/t/t/p/p/d/accounting-sequence/down";
        let up = "v1/t/t/p/p/d/accounting-sequence/up";
        let mut attachment = broker
            .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, down, 2)
            .unwrap();
        assert_broker_accounting(&broker);
        for step in 0..1_000u16 {
            let qos = if step % 2 == 0 { 1 } else { 2 };
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: down.into(),
                        payload: step.to_be_bytes().to_vec(),
                        qos,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert_broker_accounting(&broker);
            let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                panic!("expected routed delivery")
            };
            let packet_id = delivery.packet_id.unwrap();
            if qos == 1 {
                broker
                    .puback(&attachment.key, attachment.generation, packet_id)
                    .unwrap();
            } else {
                broker
                    .pubrec(&attachment.key, attachment.generation, packet_id)
                    .unwrap();
                assert_broker_accounting(&broker);
                broker
                    .pubcomp(&attachment.key, attachment.generation, packet_id)
                    .unwrap();
            }
            assert_broker_accounting(&broker);
            if step % 7 == 0 {
                let inbound_id = step + 1;
                let inbound = BrokerMessage {
                    topic: up.into(),
                    payload: step.to_be_bytes().to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                };
                assert!(
                    broker
                        .inbound_qos2(
                            &attachment.key,
                            attachment.generation,
                            inbound_id,
                            inbound.clone(),
                        )
                        .unwrap()
                );
                assert_broker_accounting(&broker);
                assert!(
                    !broker
                        .inbound_qos2(&attachment.key, attachment.generation, inbound_id, inbound,)
                        .unwrap()
                );
                assert_broker_accounting(&broker);
                broker
                    .complete_inbound_qos2(&attachment.key, attachment.generation, inbound_id)
                    .unwrap();
                assert_broker_accounting(&broker);
            }
            if step % 11 == 0 {
                broker
                    .subscribe(&attachment.key, attachment.generation, down, 1)
                    .unwrap();
                assert_broker_accounting(&broker);
                broker
                    .unsubscribe(&attachment.key, attachment.generation, down)
                    .unwrap();
                assert_broker_accounting(&broker);
                broker
                    .subscribe(&attachment.key, attachment.generation, down, 2)
                    .unwrap();
                assert_broker_accounting(&broker);
            }
            if step % 19 == 0 {
                broker
                    .route(
                        &identity.device_key,
                        BrokerMessage {
                            topic: up.into(),
                            payload: vec![1, 2, 3],
                            qos: 0,
                            retain: true,
                            properties: Default::default(),
                        },
                    )
                    .unwrap();
                assert_broker_accounting(&broker);
                broker
                    .route(
                        &identity.device_key,
                        BrokerMessage {
                            topic: up.into(),
                            payload: Vec::new(),
                            qos: 0,
                            retain: true,
                            properties: Default::default(),
                        },
                    )
                    .unwrap();
                assert_broker_accounting(&broker);
            }
            if step % 23 == 0 {
                attachment.detach().unwrap();
                assert_broker_accounting(&broker);
                broker
                    .route(
                        &identity.device_key,
                        BrokerMessage {
                            topic: down.into(),
                            payload: vec![4, 5, 6],
                            qos: 1,
                            retain: false,
                            properties: Default::default(),
                        },
                    )
                    .unwrap();
                assert_broker_accounting(&broker);
                attachment = broker
                    .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
                    .unwrap();
                assert_broker_accounting(&broker);
                let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                    panic!("expected resumed offline delivery")
                };
                broker
                    .puback(
                        &attachment.key,
                        attachment.generation,
                        delivery.packet_id.unwrap(),
                    )
                    .unwrap();
                assert_broker_accounting(&broker);
            }
            if step % 101 == 0 {
                attachment.detach().unwrap();
                assert_broker_accounting(&broker);
                let recovered = MqttBroker::new(Arc::new(Limits::default()));
                recovered.restore(broker.snapshot().unwrap()).unwrap();
                assert_broker_accounting(&recovered);
                attachment = broker
                    .attach_v5(&identity, "accounting".into(), false, 3_600, 32)
                    .unwrap();
                assert_broker_accounting(&broker);
            }
            if step % 173 == 0 {
                attachment.detach().unwrap();
                attachment = broker
                    .attach_v5(&identity, "accounting".into(), true, 3_600, 32)
                    .unwrap();
                broker
                    .subscribe(&attachment.key, attachment.generation, down, 2)
                    .unwrap();
                assert_broker_accounting(&broker);
            }
        }
    }

    #[test]
    fn derived_accounting_tracks_tenant_device_and_restore() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let first = auth("shared-device");
        let mut other_tenant = auth("other-device");
        other_tenant.device_key.tenant_id = TenantId::new("other").unwrap();
        let down = "v1/t/t/p/p/d/shared-device/down";
        let mut a = broker.attach(&first, "a".into(), false).unwrap();
        let mut b = broker.attach(&first, "b".into(), false).unwrap();
        let mut c = broker.attach(&other_tenant, "c".into(), false).unwrap();
        assert_broker_accounting(&broker);
        broker.subscribe(&a.key, a.generation, down, 1).unwrap();
        broker.subscribe(&b.key, b.generation, down, 1).unwrap();
        broker
            .subscribe(
                &c.key,
                c.generation,
                "v1/t/other/p/p/d/other-device/down",
                1,
            )
            .unwrap();
        assert_broker_accounting(&broker);
        b.detach().unwrap();
        assert_broker_accounting(&broker);
        broker
            .route(
                &first.device_key,
                BrokerMessage {
                    topic: down.into(),
                    payload: b"first".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert_broker_accounting(&broker);
        let BrokerFrame::Publish(live) = a.receiver.try_recv().unwrap() else {
            panic!("expected live copy")
        };
        broker
            .puback(&a.key, a.generation, live.packet_id.unwrap())
            .unwrap();
        assert_broker_accounting(&broker);
        b = broker.attach(&first, "b".into(), false).unwrap();
        assert_broker_accounting(&broker);
        let BrokerFrame::Publish(resumed) = b.receiver.try_recv().unwrap() else {
            panic!("expected resumed copy")
        };
        broker
            .puback(&b.key, b.generation, resumed.packet_id.unwrap())
            .unwrap();
        assert_broker_accounting(&broker);
        let inbound = BrokerMessage {
            topic: "v1/t/t/p/p/d/shared-device/up".into(),
            payload: b"inbound".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        assert!(
            broker
                .inbound_qos2(&a.key, a.generation, 77, inbound.clone())
                .unwrap()
        );
        assert!(
            !broker
                .inbound_qos2(&a.key, a.generation, 77, inbound)
                .unwrap()
        );
        assert_broker_accounting(&broker);
        broker
            .complete_inbound_qos2(&a.key, a.generation, 77)
            .unwrap();
        assert_broker_accounting(&broker);
        b.detach().unwrap();
        let mut replaced = broker.attach(&first, "b".into(), true).unwrap();
        assert_broker_accounting(&broker);
        a.detach().unwrap();
        c.detach().unwrap();
        replaced.detach().unwrap();
        let restored = MqttBroker::new(Arc::new(Limits::default()));
        restored.restore(broker.snapshot().unwrap()).unwrap();
        assert_broker_accounting(&restored);
    }

    #[test]
    fn derived_accounting_survives_local_discard_expiry_and_limit_rejection() {
        let limits = Arc::new(Limits {
            max_subscriptions_per_connection: 1,
            max_subscriptions_per_session: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let identity = auth("accounting-faults");
        let down = "v1/t/t/p/p/d/accounting-faults/down";
        let mut attachment = broker
            .attach_v5(&identity, "faults".into(), false, 3_600, 32)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, down, 2)
            .unwrap();
        assert!(matches!(
            broker.subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/accounting-faults/up",
                1,
            ),
            Err(Error::Overloaded)
        ));
        assert_broker_accounting(&broker);
        for qos in [1, 2] {
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: down.into(),
                        payload: vec![1; 64],
                        qos,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
                panic!("expected outbound publish")
            };
            assert_broker_accounting(&broker);
            if qos == 1 {
                assert!(
                    broker
                        .discard_outbound(&attachment.key, attachment.generation, &delivery)
                        .unwrap()
                );
            } else {
                broker
                    .pubrec_rejected(
                        &attachment.key,
                        attachment.generation,
                        delivery.packet_id.unwrap(),
                    )
                    .unwrap();
            }
            assert_broker_accounting(&broker);
        }
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: down.into(),
                    payload: vec![2; 64],
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected expiring publish")
        };
        std::thread::sleep(Duration::from_millis(15));
        assert!(
            !broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                .unwrap()
        );
        assert_broker_accounting(&broker);
        attachment.detach().unwrap();
        broker
            .route(
                &identity.device_key,
                BrokerMessage {
                    topic: down.into(),
                    payload: vec![3; 64],
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        assert_broker_accounting(&broker);
        std::thread::sleep(Duration::from_millis(15));
        broker.tick().unwrap();
        assert_broker_accounting(&broker);
    }

    #[test]
    fn message_deadline_budget_cleans_only_due_sessions_and_reindexes() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut keys = Vec::new();
        for index in 0..5 {
            let identity = auth(&format!("expiry-index-{index}"));
            let down = format!("v1/t/t/p/p/d/expiry-index-{index}/down");
            let mut attachment = broker
                .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
                .unwrap();
            broker
                .subscribe(&attachment.key, attachment.generation, &down, 1)
                .unwrap();
            attachment.detach().unwrap();
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: down,
                        payload: vec![index as u8],
                        qos: 1,
                        retain: false,
                        properties: PublishProperties {
                            expires_at_ms: Some(now_ms() + 10_000),
                            ..Default::default()
                        },
                    },
                )
                .unwrap();
            keys.push(attachment.key.clone());
        }
        let mut state = lock(&broker.state).unwrap();
        for key in &keys {
            state
                .sessions
                .get_mut(key)
                .unwrap()
                .offline
                .front_mut()
                .unwrap()
                .properties
                .expires_at_ms = Some(now_ms() - 1);
            sync_session_usage(&mut state, key).unwrap();
        }
        drop(state);
        assert_broker_accounting(&broker);
        {
            let mut state = lock(&broker.state).unwrap();
            prune_expired_messages(&mut state, now_ms(), 3).unwrap();
            assert_eq!(state.offline_count, 2);
            assert_accounting_consistent(&state);
            prune_expired_messages(&mut state, now_ms(), 3).unwrap();
            assert_eq!(state.offline_count, 0);
            assert_accounting_consistent(&state);
        }
    }

    #[test]
    fn target_session_expiry_is_checked_after_bounded_global_maintenance() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut identities = Vec::new();
        for index in 0..=HOT_MAINTENANCE_BUDGET {
            let identity = auth(&format!("session-deadline-{index}"));
            let mut attachment = broker
                .attach_v5(&identity, format!("client-{index}"), false, 3_600, 32)
                .unwrap();
            let key = attachment.key.clone();
            attachment.detach().unwrap();
            identities.push((identity, key));
        }
        let past = now_ms() - 1;
        for (index, (_, key)) in identities.iter().enumerate() {
            let mut state = lock(&broker.state).unwrap();
            state.sessions.get_mut(key).unwrap().expires_at_ms =
                Some(if index == HOT_MAINTENANCE_BUDGET {
                    past
                } else {
                    past - 1
                });
            sync_session_usage(&mut state, key).unwrap();
        }
        assert_broker_accounting(&broker);
        let identity = &identities[HOT_MAINTENANCE_BUDGET].0;
        let attachment = broker
            .attach_v5(
                identity,
                format!("client-{HOT_MAINTENANCE_BUDGET}"),
                false,
                3_600,
                32,
            )
            .unwrap();
        assert!(!attachment.session_present);
        assert_broker_accounting(&broker);
    }

    #[test]
    fn expired_retained_is_not_replayed_while_maintenance_is_budgeted() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut topics = Vec::new();
        for index in 0..=(HOT_MAINTENANCE_BUDGET * 2) {
            let identity = auth(&format!("retained-deadline-{index}"));
            let topic = format!("v1/t/t/p/p/d/retained-deadline-{index}/up");
            broker
                .route(
                    &identity.device_key,
                    BrokerMessage {
                        topic: topic.clone(),
                        payload: vec![1],
                        qos: 1,
                        retain: true,
                        properties: PublishProperties {
                            expires_at_ms: Some(now_ms() + 10_000),
                            ..Default::default()
                        },
                    },
                )
                .unwrap();
            topics.push((identity, topic));
        }
        let past = now_ms() - 1;
        {
            let mut state = lock(&broker.state).unwrap();
            for (index, (_, topic)) in topics.iter().enumerate() {
                let deadline = if index == HOT_MAINTENANCE_BUDGET * 2 {
                    past
                } else {
                    past - 1
                };
                state
                    .retained
                    .get_mut(topic)
                    .unwrap()
                    .message
                    .properties
                    .expires_at_ms = Some(deadline);
                state.retained_expiry.update(topic.clone(), Some(deadline));
            }
        }
        let (identity, topic) = &topics[HOT_MAINTENANCE_BUDGET * 2];
        let attachment = broker
            .attach_v5(identity, "retained-reader".into(), false, 3_600, 32)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        assert!(attachment.receiver.is_empty());
        assert!(!broker.has_retained_topic(topic).unwrap());
        assert_broker_accounting(&broker);
    }

    #[test]
    #[ignore = "manual baseline for future delayed-Will retry cost"]
    fn benchmark_future_pending_will_retry() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let owner = auth("future-will").device_key;
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/future-will/up".into(),
            payload: vec![1; 64],
            qos: 1,
            retain: false,
            properties: Default::default(),
        };
        let mut state = lock(&broker.state).unwrap();
        for _ in 0..10_000 {
            state.pending_wills.push_back(PendingWill {
                owner: owner.clone(),
                message: message.clone(),
                due_at_ms: Some(now_ms() + 3_600_000),
                cancel_on_resume: None,
                message_expiry_interval: None,
                retained_reservation: RetainedReservation::default(),
            });
        }
        for run in 1..=5 {
            let mut samples = Vec::with_capacity(100);
            for _ in 0..100 {
                let started = std::time::Instant::now();
                assert_eq!(retry_pending_wills(&mut state, &broker.limits), 0);
                samples.push(started.elapsed().as_nanos());
            }
            samples.sort_unstable();
            println!(
                "WILL,future_10000,{run},{},{},{}",
                samples[50], samples[95], samples[99]
            );
        }
    }

    fn auth(device: &str) -> AuthenticatedDevice {
        AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new(device).unwrap(),
            },
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        }
    }

    #[tokio::test]
    async fn v5_delayed_will_is_bounded_cancelled_on_resume_and_recovers() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let device = auth("will-delay");
        let topic = "v1/t/t/p/p/d/will-delay/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"delayed".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties {
                        expires_at_ms: Some(i64::MAX),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        will.arm_v5(
            attachment.key.clone(),
            attachment.session_incarnation,
            attachment.generation,
            2,
            60,
            Some(5),
        );
        attachment.detach().unwrap();
        assert!(will.publish_v5().unwrap().is_none());
        assert_eq!(broker.pending_will_count().unwrap(), 1);
        assert!(!broker.has_retained_topic(topic).unwrap());
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        assert!(resumed.session_present);
        assert_eq!(broker.pending_will_count().unwrap(), 0);
        resumed.detach().unwrap();

        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"recover".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties {
                        expires_at_ms: Some(i64::MAX),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        will.arm_v5(
            resumed.key.clone(),
            resumed.session_incarnation,
            resumed.generation,
            2,
            60,
            Some(5),
        );
        assert!(will.publish_v5().unwrap().is_none());
        let root = std::env::temp_dir().join(format!("netbaiot-v5-will-{}", uuid::Uuid::new_v4()));
        broker.commit_to(&root).await.unwrap();
        let recovered = MqttBroker::new(limits);
        assert!(recovered.recover_from(&root).await.unwrap());
        assert_eq!(recovered.pending_will_count().unwrap(), 1);
        assert!(!recovered.has_retained_topic(topic).unwrap());
        {
            let mut state = recovered.state.lock().unwrap();
            state.pending_wills.front_mut().unwrap().due_at_ms = Some(now_ms() - 1);
        }
        recovered.tick().unwrap();
        assert_eq!(recovered.pending_will_count().unwrap(), 0);
        assert!(recovered.has_retained_topic(topic).unwrap());
        let state = recovered.state.lock().unwrap();
        assert!(
            state
                .retained
                .get(topic)
                .unwrap()
                .message
                .properties
                .expires_at_ms
                .unwrap()
                > now_ms()
        );
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn takeover_same_session_positive_delay_suppresses_will() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("will-takeover");
        let topic = "v1/t/t/p/p/d/will-takeover/up";
        let mut old = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"old".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm_v5(
            old.key.clone(),
            old.session_incarnation,
            old.generation,
            30,
            60,
            None,
        );
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        assert_eq!(resumed.session_incarnation, old.session_incarnation);
        old.detach().unwrap();
        assert!(will.publish_v5().unwrap().is_none());
        assert_eq!(broker.pending_will_count().unwrap(), 0);
        assert!(!broker.has_retained_topic(topic).unwrap());
        resumed.detach().unwrap();
    }

    #[test]
    fn takeover_clean_start_publishes_old_will() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("will-clean-takeover");
        let topic = "v1/t/t/p/p/d/will-clean-takeover/up";
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"ended".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm_v5(
            resumed.key.clone(),
            resumed.session_incarnation,
            resumed.generation,
            30,
            60,
            None,
        );
        let mut fresh = broker
            .attach_v5(&device, "client".into(), true, 60, 4)
            .unwrap();
        assert!(!fresh.session_present);
        resumed.detach().unwrap();
        assert!(will.publish_v5().unwrap().is_some());
        assert!(broker.has_retained_topic(topic).unwrap());
        fresh.detach().unwrap();
    }

    #[test]
    fn takeover_same_session_zero_delay_publishes_will() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("immediate-will-takeover");
        let topic = "v1/t/t/p/p/d/immediate-will-takeover/up";
        let mut old = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"old".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm_v5(
            old.key.clone(),
            old.session_incarnation,
            old.generation,
            0,
            60,
            None,
        );
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        old.detach().unwrap();
        assert!(will.publish_v5().unwrap().is_some());
        assert!(broker.has_retained_topic(topic).unwrap());
        resumed.detach().unwrap();
    }

    #[test]
    fn takeover_clean_start_zero_delay_publishes_old_will() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("immediate-will-takeover");
        let topic = "v1/t/t/p/p/d/immediate-will-takeover/up";
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"new".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm_v5(
            resumed.key.clone(),
            resumed.session_incarnation,
            resumed.generation,
            0,
            60,
            None,
        );
        let mut fresh = broker
            .attach_v5(&device, "client".into(), true, 60, 4)
            .unwrap();
        resumed.detach().unwrap();
        assert!(will.publish_v5().unwrap().is_some());
        assert!(broker.has_retained_topic(topic).unwrap());
        fresh.detach().unwrap();
    }

    #[tokio::test]
    async fn v5_client_receive_maximum_defers_second_publish_until_ack() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("receive-max");
        let topic = "v1/t/t/p/p/d/receive-max/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe_v5(
                &attachment.key,
                attachment.generation,
                topic,
                v5::SubscriptionOptions {
                    qos: 1,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: 0,
                },
            )
            .unwrap();
        for payload in [b"first".to_vec(), b"second".to_vec()] {
            broker
                .route_from_session(
                    &attachment.key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload,
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first PUBLISH")
        };
        assert_eq!(first.message.payload, b"first");
        assert!(attachment.receiver.try_recv().is_err());
        assert_eq!(broker.state.lock().unwrap().offline_count, 1);
        broker
            .puback(
                &attachment.key,
                attachment.generation,
                first.packet_id.unwrap(),
            )
            .unwrap();
        let second = match attachment.receiver.try_recv() {
            Ok(frame) => frame,
            Err(_) => broker
                .next_offline(&attachment.key, attachment.generation)
                .unwrap()
                .unwrap(),
        };
        let BrokerFrame::Publish(second) = second else {
            panic!("expected deferred PUBLISH")
        };
        assert_eq!(second.message.payload, b"second");
        assert_eq!(broker.state.lock().unwrap().offline_count, 0);
        attachment.detach().unwrap();
    }

    #[tokio::test]
    async fn receive_maximum_1_qos2_waits_for_pubcomp() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("qos2-window");
        let topic = "v1/t/t/p/p/d/qos2-window/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 2)
            .unwrap();
        for payload in [b"first".to_vec(), b"second".to_vec()] {
            broker
                .route_from_session(
                    &attachment.key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload,
                        qos: 2,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first QoS2 PUBLISH")
        };
        let id = first.packet_id.unwrap();
        assert_eq!(first.message.payload, b"first");
        assert!(attachment.receiver.try_recv().is_err());
        assert!(matches!(
            broker.pubrec(&attachment.key, attachment.generation, id).unwrap(),
            BrokerFrame::Pubrel { packet_id, dup: false } if packet_id == id
        ));
        assert!(
            broker
                .next_offline(&attachment.key, attachment.generation)
                .unwrap()
                .is_none()
        );
        assert!(attachment.receiver.try_recv().is_err());
        assert!(matches!(
            broker.pubrec(&attachment.key, attachment.generation, id).unwrap(),
            BrokerFrame::Pubrel { packet_id, dup: true } if packet_id == id
        ));
        assert!(!broker.state.lock().unwrap().sessions[&attachment.key].has_send_quota());
        broker
            .pubcomp(&attachment.key, attachment.generation, id)
            .unwrap();
        assert!(matches!(
            broker.pubcomp(&attachment.key, attachment.generation, id),
            Err(Error::Invalid)
        ));
        let second = match attachment.receiver.try_recv() {
            Ok(frame) => frame,
            Err(_) => broker
                .next_offline(&attachment.key, attachment.generation)
                .unwrap()
                .unwrap(),
        };
        let BrokerFrame::Publish(second) = second else {
            panic!("expected second QoS2 PUBLISH")
        };
        assert_eq!(second.message.payload, b"second");
        attachment.detach().unwrap();
    }

    #[tokio::test]
    async fn qos2_negative_pubrec_releases_send_quota() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("qos2-rejected");
        let topic = "v1/t/t/p/p/d/qos2-rejected/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 2)
            .unwrap();
        for payload in [b"first".to_vec(), b"second".to_vec()] {
            broker
                .route_from_session(
                    &attachment.key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload,
                        qos: 2,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first PUBLISH")
        };
        broker
            .pubrec_rejected(
                &attachment.key,
                attachment.generation,
                first.packet_id.unwrap(),
            )
            .unwrap();
        let second = broker
            .next_offline(&attachment.key, attachment.generation)
            .unwrap()
            .or_else(|| attachment.receiver.try_recv().ok())
            .unwrap();
        assert!(
            matches!(second, BrokerFrame::Publish(delivery) if delivery.message.payload == b"second")
        );
        attachment.detach().unwrap();
    }

    #[tokio::test]
    async fn reconnect_await_pubcomp_does_not_consume_new_send_window() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("qos2-resume-window");
        let topic = "v1/t/t/p/p/d/qos2-resume-window/up";
        let mut old = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&old.key, old.generation, topic, 2)
            .unwrap();
        for payload in [b"first".to_vec(), b"second".to_vec()] {
            broker
                .route_from_session(
                    &old.key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload,
                        qos: 2,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = old.receiver.try_recv().unwrap() else {
            panic!("expected first PUBLISH")
        };
        let first_id = first.packet_id.unwrap();
        broker.pubrec(&old.key, old.generation, first_id).unwrap();
        old.detach().unwrap();
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        assert!(resumed.session_present);
        assert!(matches!(
            resumed.receiver.try_recv().unwrap(),
            BrokerFrame::Pubrel { packet_id, dup: true } if packet_id == first_id
        ));
        let second = resumed.receiver.try_recv().unwrap();
        assert!(matches!(second, BrokerFrame::Publish(_)));
        resumed.detach().unwrap();
    }

    #[test]
    fn v5_server_receive_maximum_counts_qos1_against_pending_qos2() {
        let limits = Limits {
            max_inflight_qos1_per_session: 2,
            max_inflight_qos2_per_session: 2,
            ..Default::default()
        };
        let broker = MqttBroker::new(Arc::new(limits));
        let device = auth("server-receive-max");
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/server-receive-max/up".into(),
            payload: b"data".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        for id in [1, 2] {
            assert!(
                broker
                    .inbound_receive_available(&attachment.key, attachment.generation, 2, id)
                    .unwrap()
            );
            broker
                .inbound_qos2(&attachment.key, attachment.generation, id, message.clone())
                .unwrap();
        }
        assert!(
            broker
                .inbound_receive_available(&attachment.key, attachment.generation, 2, 1)
                .unwrap()
        );
        assert!(
            !broker
                .inbound_receive_available(&attachment.key, attachment.generation, 1, 3)
                .unwrap()
        );
        assert!(
            !broker
                .inbound_receive_available(&attachment.key, attachment.generation, 2, 3)
                .unwrap()
        );
        attachment.detach().unwrap();
    }

    #[test]
    fn persistent_inbound_qos2_does_not_consume_new_connection_receive_window() {
        let limits = Limits {
            max_inflight_qos1_per_session: 1,
            max_inflight_qos2_per_session: 2,
            ..Default::default()
        };
        let broker = MqttBroker::new(Arc::new(limits));
        let device = auth("inbound-window-reconnect");
        let mut first = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        broker
            .inbound_qos2(
                &first.key,
                first.generation,
                7,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/inbound-window-reconnect/up".into(),
                    payload: b"old".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert!(
            !broker
                .inbound_receive_available(&first.key, first.generation, 1, 8)
                .unwrap()
        );
        first.detach().unwrap();
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        assert!(resumed.session_present);
        assert!(
            broker
                .inbound_receive_available(&resumed.key, resumed.generation, 1, 8)
                .unwrap()
        );
        assert!(
            broker
                .inbound_receive_available(&resumed.key, resumed.generation, 2, 7)
                .unwrap()
        );
        resumed.detach().unwrap();
    }

    #[test]
    fn v5_qos2_retransmission_keeps_first_message_expiry_deadline() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("qos2-expiry");
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        let deadline = now_ms() + 5_000;
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/qos2-expiry/up".into(),
            payload: b"first".to_vec(),
            qos: 2,
            retain: false,
            properties: PublishProperties {
                expires_at_ms: Some(deadline),
                ..Default::default()
            },
        };
        assert!(
            broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
                .unwrap()
        );
        let mut retransmit = message.clone();
        retransmit.properties.expires_at_ms = Some(deadline + 1_000);
        assert!(
            !broker
                .inbound_qos2(
                    &attachment.key,
                    attachment.generation,
                    7,
                    retransmit.clone()
                )
                .unwrap()
        );
        let stored = broker
            .inbound_qos2_message(&attachment.key, attachment.generation, 7)
            .unwrap()
            .unwrap();
        assert_eq!(stored.0.properties.expires_at_ms, Some(deadline));
        let accounting = transaction_accounting(&broker, &attachment.key);
        retransmit.payload = b"different".to_vec();
        assert!(
            !broker
                .inbound_qos2(
                    &attachment.key,
                    attachment.generation,
                    7,
                    retransmit.clone()
                )
                .unwrap()
        );
        retransmit = message.clone();
        retransmit.topic = "v1/t/t/p/p/d/qos2-expiry/down".into();
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, retransmit)
                .unwrap()
        );
        retransmit = message.clone();
        retransmit.properties.content_type = Some("other".into());
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, retransmit)
                .unwrap()
        );
        assert_eq!(transaction_accounting(&broker, &attachment.key), accounting);
        attachment.detach().unwrap();
    }

    fn qos2_duplicate_case() -> (Arc<MqttBroker>, Attachment, BrokerMessage) {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("qos2-duplicate");
        let attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/qos2-duplicate/up".into(),
            payload: b"original".to_vec(),
            qos: 2,
            retain: true,
            properties: Default::default(),
        };
        assert!(
            broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
                .unwrap()
        );
        (broker, attachment, message)
    }

    #[test]
    fn inbound_qos2_classification_is_read_only_and_generation_fenced() {
        let (broker, mut old, _) = qos2_duplicate_case();
        let before = transaction_accounting(&broker, &old.key);
        assert_eq!(
            broker
                .classify_inbound_qos2_publish(&old.key, old.generation, 7)
                .unwrap(),
            InboundQos2PublishState::ExistingTransaction
        );
        assert_eq!(
            broker
                .classify_inbound_qos2_publish(&old.key, old.generation, 8)
                .unwrap(),
            InboundQos2PublishState::NeedsNewMessageAdmission
        );
        assert_eq!(transaction_accounting(&broker, &old.key), before);

        let device = auth("qos2-duplicate");
        let mut replacement = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        assert!(matches!(
            broker.classify_inbound_qos2_publish(&old.key, old.generation, 7),
            Err(Error::Conflict)
        ));
        let resumed = transaction_accounting(&broker, &replacement.key);
        assert_eq!(
            broker
                .classify_inbound_qos2_publish(&replacement.key, replacement.generation, 7)
                .unwrap(),
            InboundQos2PublishState::ExistingTransaction
        );
        assert_eq!(transaction_accounting(&broker, &replacement.key), resumed);
        old.detach().unwrap();
        replacement.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_repeated_publish_before_pubrel_repeats_pubrec() {
        let (broker, mut attachment, message) = qos2_duplicate_case();
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message)
                .unwrap()
        );
        assert!(matches!(
            broker.begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7),
            Ok(InboundQos2Action::Deliver { .. })
        ));
        attachment.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_repeated_publish_does_not_redeliver() {
        let (broker, mut attachment, message) = qos2_duplicate_case();
        let InboundQos2Action::Deliver {
            session_incarnation,
            operation_id,
            ..
        } = broker
            .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
            .unwrap()
        else {
            panic!("original must be delivered")
        };
        broker
            .finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 7, operation_id)
            .unwrap();
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message)
                .unwrap()
        );
        assert!(matches!(
            broker.begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7),
            Ok(InboundQos2Action::EventAccepted { .. })
        ));
        attachment.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_repeated_publish_does_not_change_accounting() {
        let (broker, mut attachment, mut message) = qos2_duplicate_case();
        let before = transaction_accounting(&broker, &attachment.key);
        message.payload = b"different-and-larger".to_vec();
        message.retain = false;
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, message)
                .unwrap()
        );
        assert_eq!(transaction_accounting(&broker, &attachment.key), before);
        attachment.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_dup_flag_does_not_create_second_transaction() {
        let (broker, mut attachment, mut message) = qos2_duplicate_case();
        // The connection supplies identical broker state transitions for both wire DUP values.
        for _dup in [false, true] {
            message.payload.push(b'x');
            assert!(
                !broker
                    .inbound_qos2(&attachment.key, attachment.generation, 7, message.clone())
                    .unwrap()
            );
        }
        assert_eq!(
            transaction_accounting(&broker, &attachment.key).inbound_qos2_count,
            1
        );
        attachment.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_original_message_remains_authoritative_until_pubrel() {
        let (broker, mut attachment, message) = qos2_duplicate_case();
        let mut changed = message.clone();
        changed.payload = b"replacement".to_vec();
        changed.properties.content_type = Some("changed".into());
        assert!(
            !broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, changed)
                .unwrap()
        );
        let InboundQos2Action::Deliver {
            message: delivered, ..
        } = broker
            .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 7)
            .unwrap()
        else {
            panic!("original must be delivered")
        };
        assert_eq!(delivered, message);
        attachment.detach().unwrap();
    }

    #[test]
    fn inbound_qos2_pubcomp_releases_identifier_for_new_message() {
        let (broker, mut attachment, message) = qos2_duplicate_case();
        broker
            .complete_inbound_qos2(&attachment.key, attachment.generation, 7)
            .unwrap();
        broker
            .finish_inbound_pubcomp(&attachment.key, attachment.generation, 7)
            .unwrap();
        let mut next = message;
        next.payload = b"next".to_vec();
        assert!(
            broker
                .inbound_qos2(&attachment.key, attachment.generation, 7, next.clone())
                .unwrap()
        );
        assert_eq!(
            broker
                .inbound_qos2_message(&attachment.key, attachment.generation, 7)
                .unwrap()
                .unwrap()
                .0,
            next
        );
        attachment.detach().unwrap();
    }

    #[test]
    fn qos1_started_publish_survives_message_expiry_until_puback() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("started-qos1");
        let topic = "v1/t/t/p/p/d/started-qos1/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        broker
            .route_from_session(
                &attachment.key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"data".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected PUBLISH")
        };
        let id = delivery.packet_id.unwrap();
        assert!(
            broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                .unwrap()
        );
        {
            let mut state = broker.state.lock().unwrap();
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
                panic!("expected AwaitPuback")
            };
            message.properties.expires_at_ms = Some(now_ms() - 1);
            sync_session_usage(&mut state, &attachment.key).unwrap();
        }
        broker.tick().unwrap();
        assert!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .contains_key(&id)
        );
        broker
            .puback(&attachment.key, attachment.generation, id)
            .unwrap();
        assert!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .is_empty()
        );
        attachment.detach().unwrap();
    }

    #[test]
    fn unsent_expired_message_is_dropped() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("unsent-expiry");
        let topic = "v1/t/t/p/p/d/unsent-expiry/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        broker
            .route_from_session(
                &attachment.key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"data".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected PUBLISH")
        };
        let id = delivery.packet_id.unwrap();
        {
            let mut state = broker.state.lock().unwrap();
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
                panic!("expected AwaitPuback")
            };
            message.properties.expires_at_ms = Some(now_ms() - 1);
            sync_session_usage(&mut state, &attachment.key).unwrap();
        }
        broker.tick().unwrap();
        assert!(
            !broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                .unwrap()
        );
        assert!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .is_empty()
        );
        attachment.detach().unwrap();
    }

    fn oversized_delivery_is_settled_locally(qos: u8) {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth(if qos == 1 {
            "oversized-qos1"
        } else {
            "oversized-qos2"
        });
        let topic = format!("v1/t/t/p/p/d/{}/up", device.device_key.device_id);
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, &topic, qos)
            .unwrap();
        for payload in [vec![b'x'; 128], b"small".to_vec()] {
            broker
                .route_from_session(
                    &attachment.key,
                    BrokerMessage {
                        topic: topic.clone(),
                        payload,
                        qos,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first PUBLISH")
        };
        let first_id = first.packet_id.unwrap();
        assert!(
            broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &first)
                .unwrap()
        );
        assert!(
            broker
                .discard_outbound(&attachment.key, attachment.generation, &first)
                .unwrap()
        );
        assert!(
            !broker
                .discard_outbound(&attachment.key, attachment.generation, &first)
                .unwrap()
        );
        assert!(
            !broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .contains_key(&first_id)
        );
        let next = attachment.receiver.try_recv().ok().or_else(|| {
            broker
                .next_offline(&attachment.key, attachment.generation)
                .unwrap()
        });
        let BrokerFrame::Publish(second) = next.unwrap() else {
            panic!("expected following PUBLISH")
        };
        assert_eq!(second.message.payload, b"small");
        assert_eq!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .send_window
                .len(),
            1
        );
        attachment.detach().unwrap();
        let mut resumed = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        assert!(resumed.session_present);
        let BrokerFrame::Publish(resumed_delivery) = resumed.receiver.try_recv().unwrap() else {
            panic!("expected following PUBLISH on reconnect")
        };
        assert_eq!(resumed_delivery.message.payload, b"small");
        assert!(resumed.receiver.try_recv().is_err());
        resumed.detach().unwrap();
    }

    #[test]
    fn oversized_qos1_delivery_is_settled_locally() {
        oversized_delivery_is_settled_locally(1);
    }

    #[test]
    fn oversized_qos2_delivery_is_settled_locally() {
        oversized_delivery_is_settled_locally(2);
    }

    #[test]
    fn command_expires_while_waiting_for_send_quota() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("command-expiry");
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        let deadline = now_ms() + 10_000;
        for payload in [b"first".as_slice(), b"expiring".as_slice()] {
            broker
                .send_live(
                    &attachment.key,
                    BrokerMessage {
                        topic: "v1/t/t/p/p/d/command-expiry/down".into(),
                        payload: payload.to_vec(),
                        qos: 1,
                        retain: false,
                        properties: PublishProperties {
                            expires_at_ms: Some(deadline),
                            ..Default::default()
                        },
                    },
                )
                .unwrap();
        }
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first command")
        };
        assert_eq!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .offline
                .len(),
            1
        );
        {
            let mut state = broker.state.lock().unwrap();
            state
                .sessions
                .get_mut(&attachment.key)
                .unwrap()
                .offline
                .front_mut()
                .unwrap()
                .properties
                .expires_at_ms = Some(now_ms() - 1);
            sync_session_usage(&mut state, &attachment.key).unwrap();
        }
        broker.tick().unwrap();
        assert!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .offline
                .is_empty()
        );
        broker
            .puback(
                &attachment.key,
                attachment.generation,
                first.packet_id.unwrap(),
            )
            .unwrap();
        assert!(
            broker
                .next_offline(&attachment.key, attachment.generation)
                .unwrap()
                .is_none()
        );
        attachment.detach().unwrap();
    }

    #[tokio::test]
    async fn command_expiry_survives_snapshot_restore() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let device = auth("command-recovery");
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        let deadline = now_ms() + 30_000;
        broker
            .send_live(
                &attachment.key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/command-recovery/down".into(),
                    payload: b"command".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(deadline),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        attachment.detach().unwrap();
        let root = std::env::temp_dir().join(format!(
            "netbaiot-command-recovery-{}",
            uuid::Uuid::new_v4()
        ));
        broker.commit_to(&root).await.unwrap();
        let recovered = MqttBroker::new(limits);
        assert!(recovered.recover_from(&root).await.unwrap());
        let state = recovered.state.lock().unwrap();
        let session = state.sessions.get(&attachment.key).unwrap();
        let message = session.outbound.values().next().unwrap();
        assert_eq!(
            match message {
                OutboundState::AwaitPuback(message) => message.properties.expires_at_ms,
                _ => None,
            },
            Some(deadline)
        );
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn qos2_started_publish_survives_message_expiry_until_pubcomp() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("started-qos2");
        let topic = "v1/t/t/p/p/d/started-qos2/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 1)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 2)
            .unwrap();
        broker
            .route_from_session(
                &attachment.key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"data".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected PUBLISH")
        };
        let id = delivery.packet_id.unwrap();
        assert!(
            broker
                .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
                .unwrap()
        );
        {
            let mut state = broker.state.lock().unwrap();
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            let OutboundState::AwaitPubrec(message) = session.outbound.get_mut(&id).unwrap() else {
                panic!("expected AwaitPubrec")
            };
            message.properties.expires_at_ms = Some(now_ms() - 1);
        }
        broker.tick().unwrap();
        assert!(matches!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .get(&id),
            Some(OutboundState::AwaitPubrec(_))
        ));
        broker
            .pubrec(&attachment.key, attachment.generation, id)
            .unwrap();
        broker.tick().unwrap();
        assert!(matches!(
            broker.state.lock().unwrap().sessions[&attachment.key].outbound.get(&id),
            Some(OutboundState::AwaitPubcomp(message)) if message.payload.is_empty()
        ));
        broker
            .pubcomp(&attachment.key, attachment.generation, id)
            .unwrap();
        assert!(
            broker.state.lock().unwrap().sessions[&attachment.key]
                .outbound
                .is_empty()
        );
        attachment.detach().unwrap();
    }

    #[tokio::test]
    async fn reconnect_preserves_started_qos_state_after_message_expiry() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let device = auth("recovered-started");
        let topic = "v1/t/t/p/p/d/recovered-started/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        broker
            .route_from_session(
                &attachment.key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"first".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 10_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected PUBLISH")
        };
        let id = delivery.packet_id.unwrap();
        broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &delivery)
            .unwrap();
        {
            let mut state = broker.state.lock().unwrap();
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            let OutboundState::AwaitPuback(message) = session.outbound.get_mut(&id).unwrap() else {
                panic!("expected AwaitPuback")
            };
            message.properties.expires_at_ms = Some(now_ms() - 1);
        }
        attachment.detach().unwrap();
        let directory =
            std::env::temp_dir().join(format!("netbaiot-started-expiry-{}", uuid::Uuid::new_v4()));
        broker.commit_to(&directory).await.unwrap();
        let recovered = MqttBroker::new(limits);
        recovered.recover_from(&directory).await.unwrap();
        let mut resumed = recovered
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        let BrokerFrame::Publish(retransmit) = resumed.receiver.try_recv().unwrap() else {
            panic!("expected retransmitted PUBLISH")
        };
        assert_eq!(retransmit.packet_id, Some(id));
        assert!(retransmit.dup);
        assert!(
            recovered
                .begin_outbound_transfer(&resumed.key, resumed.generation, &retransmit)
                .unwrap()
        );
        assert!(
            recovered.state.lock().unwrap().sessions[&resumed.key]
                .started_outbound
                .contains(&id)
        );
        recovered
            .puback(&resumed.key, resumed.generation, id)
            .unwrap();
        resumed.detach().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn packet_identifier_not_reused_before_qos_exchange_finishes() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("packet-id-expiry");
        let topic = "v1/t/t/p/p/d/packet-id-expiry/up";
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 2)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        let message = BrokerMessage {
            topic: topic.into(),
            payload: b"data".to_vec(),
            qos: 1,
            retain: false,
            properties: PublishProperties {
                expires_at_ms: Some(now_ms() + 10_000),
                ..Default::default()
            },
        };
        broker
            .route_from_session(&attachment.key, message.clone())
            .unwrap();
        let BrokerFrame::Publish(first) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected first PUBLISH")
        };
        let first_id = first.packet_id.unwrap();
        broker
            .begin_outbound_transfer(&attachment.key, attachment.generation, &first)
            .unwrap();
        {
            let mut state = broker.state.lock().unwrap();
            let session = state.sessions.get_mut(&attachment.key).unwrap();
            session.next_packet_id = first_id;
            let OutboundState::AwaitPuback(stored) = session.outbound.get_mut(&first_id).unwrap()
            else {
                panic!("expected AwaitPuback")
            };
            stored.properties.expires_at_ms = Some(now_ms() - 1);
        }
        broker.tick().unwrap();
        broker.route_from_session(&attachment.key, message).unwrap();
        let BrokerFrame::Publish(second) = attachment.receiver.try_recv().unwrap() else {
            panic!("expected second PUBLISH")
        };
        assert_ne!(second.packet_id, Some(first_id));
        attachment.detach().unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TransactionAccounting {
        session_bytes: usize,
        broker_session_bytes: usize,
        offline_count: usize,
        offline_bytes: usize,
        retained_reserved_count: usize,
        retained_reserved_bytes: usize,
        inbound_qos2_count: usize,
        inbound_window_count: usize,
        tenant_qos2_inflight: usize,
        outbound_order: VecDeque<u16>,
        outbound: HashMap<u16, OutboundState>,
    }

    fn transaction_accounting(broker: &MqttBroker, key: &SessionKey) -> TransactionAccounting {
        let state = broker.state.lock().unwrap();
        let session = state.sessions.get(key).unwrap();
        TransactionAccounting {
            session_bytes: session.state_bytes,
            broker_session_bytes: state.session_bytes,
            offline_count: state.offline_count,
            offline_bytes: state.offline_bytes,
            retained_reserved_count: state.retained_reserved_count,
            retained_reserved_bytes: state.retained_reserved_bytes,
            inbound_qos2_count: session.inbound_qos2.len(),
            inbound_window_count: session.inbound_window.len(),
            tenant_qos2_inflight: tenant_inflight(&state, &key.device.tenant_id, 2),
            outbound_order: session.outbound_order.clone(),
            outbound: session.outbound.clone(),
        }
    }

    #[test]
    fn v5_clean_start_expiry_and_cross_version_sessions() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let auth = auth("a");
        let mut first = broker
            .attach_v5(&auth, "client".into(), true, 60, u16::MAX)
            .unwrap();
        assert!(!first.session_present);
        first.detach().unwrap();
        let mut resumed = broker
            .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
            .unwrap();
        assert!(resumed.session_present);
        broker
            .set_v5_disconnect_expiry(&resumed.key, resumed.generation, 0)
            .unwrap();
        resumed.detach().unwrap();
        let mut fresh = broker
            .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
            .unwrap();
        assert!(!fresh.session_present);
        fresh.detach().unwrap();
        let mut v311 = broker.attach(&auth, "client".into(), false).unwrap();
        assert!(!v311.session_present);
        v311.detach().unwrap();
        let mut v5 = broker
            .attach_v5(&auth, "client".into(), false, 60, u16::MAX)
            .unwrap();
        assert!(!v5.session_present);
        v5.detach().unwrap();
    }

    #[test]
    fn v5_expired_session_releases_all_accounting_before_reconnect() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let auth = auth("a");
        let mut attachment = broker
            .attach_v5(&auth, "client".into(), false, 1, u16::MAX)
            .unwrap();
        let key = attachment.key.clone();
        let filter = format!("v1/t/t/p/p/d/{}/up", auth.device_key.device_id.as_str());
        broker
            .subscribe(&key, attachment.generation, &filter, 1)
            .unwrap();
        attachment.detach().unwrap();
        {
            let mut state = broker.state.lock().unwrap();
            state.sessions.get_mut(&key).unwrap().expires_at_ms = Some(now_ms() - 1);
        }
        let mut fresh = broker
            .attach_v5(&auth, "client".into(), false, 1, u16::MAX)
            .unwrap();
        assert!(!fresh.session_present);
        assert_eq!(broker.usage().unwrap().2, 0);
        fresh.detach().unwrap();
    }

    #[tokio::test]
    async fn v4_recovery_preserves_v5_session_and_v3_remains_readable() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let auth = auth("a");
        let mut attachment = broker
            .attach_v5(&auth, "v5".into(), false, 60, u16::MAX)
            .unwrap();
        attachment.detach().unwrap();
        let directory =
            std::env::temp_dir().join(format!("netbaiot-mqtt-v4-{}", uuid::Uuid::new_v4()));
        broker.commit_to(&directory).await.unwrap();
        let restored = MqttBroker::new(Arc::new(Limits::default()));
        assert!(restored.recover_from(&directory).await.unwrap());
        let mut resumed = restored
            .attach_v5(&auth, "v5".into(), false, 60, u16::MAX)
            .unwrap();
        assert!(resumed.session_present);
        resumed.detach().unwrap();
        fs::remove_dir_all(&directory).unwrap();

        let v311 = MqttBroker::new(Arc::new(Limits::default()));
        let mut old = v311.attach(&auth, "old".into(), false).unwrap();
        old.detach().unwrap();
        let state = v311.state.lock().unwrap();
        let session = state.sessions.values().next().unwrap();
        let mut payload = Vec::new();
        encode_session_meta(&mut payload, session).unwrap();
        payload.truncate(payload.len() - 13);
        let mut header = Vec::new();
        header.extend_from_slice(RECOVERY_MAGIC);
        header.extend_from_slice(&RECOVERY_VERSION_V3.to_be_bytes());
        header.extend_from_slice(&state.generation.to_be_bytes());
        let mut record = vec![RECORD_SESSION];
        record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&Sha256::digest(&payload));
        let mut digest = Sha256::new();
        digest.update(&header);
        digest.update(&record);
        let mut image = header.clone();
        image.extend_from_slice(&Sha256::digest(&header));
        image.extend_from_slice(&record);
        image.extend_from_slice(RECOVERY_TRAILER_MAGIC);
        image.extend_from_slice(&1u64.to_be_bytes());
        image.extend_from_slice(&(record.len() as u64).to_be_bytes());
        image.extend_from_slice(&digest.finalize());
        drop(state);
        let snapshot = decode_mqtt_recovery(&image, &Limits::default()).unwrap();
        assert_eq!(snapshot.format_version, RECOVERY_VERSION_V3);
        let recovered = MqttBroker::new(Arc::new(Limits::default()));
        recovered.restore(snapshot).unwrap();
        let mut resumed = recovered.attach(&auth, "old".into(), false).unwrap();
        assert!(resumed.session_present);
        resumed.detach().unwrap();
    }

    #[test]
    fn v4_outbound_record_remains_readable_without_started_flag() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let device = auth("v4-outbound");
        let mut attachment = broker
            .attach_v5(&device, "client".into(), false, 60, 4)
            .unwrap();
        attachment.detach().unwrap();
        let mut snapshot = broker.snapshot().unwrap();
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/v4-outbound/down".into(),
            payload: b"legacy".to_vec(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        };
        let mut record = vec![0, 7, 0];
        encode_message(&mut record, &message).unwrap();
        decode_record(
            RECORD_OUTBOUND,
            &record,
            &mut snapshot,
            &limits,
            RECOVERY_VERSION_V4,
        )
        .unwrap();
        assert!(snapshot.sessions[0].started_outbound.contains(&7));
        assert!(snapshot.sessions[0].outbound.contains_key(&7));
    }

    #[tokio::test]
    async fn v4_recovery_keeps_publish_properties_and_byte_accounting() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let auth = auth("a");
        let topic = "v1/t/t/p/p/d/a/up";
        let mut attachment = broker
            .attach_v5(&auth, "metadata".into(), false, 60, u16::MAX)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        attachment.detach().unwrap();
        let message = BrokerMessage {
            topic: topic.into(),
            payload: b"value".to_vec(),
            qos: 1,
            retain: true,
            properties: PublishProperties {
                payload_format: Some(1),
                expires_at_ms: Some(now_ms() + 30_000),
                content_type: Some("text/plain".into()),
                response_topic: Some(topic.into()),
                correlation_data: Some(vec![0, 1, 2]),
                user_properties: vec![("key".into(), "value".into())],
            },
        };
        broker.route(&auth.device_key, message.clone()).unwrap();
        let original = broker.usage().unwrap();
        assert!(original.4 >= message.bytes());
        let directory =
            std::env::temp_dir().join(format!("netbaiot-mqtt-v4-props-{}", uuid::Uuid::new_v4()));
        broker.commit_to(&directory).await.unwrap();
        let restored = MqttBroker::new(limits);
        restored.recover_from(&directory).await.unwrap();
        assert_eq!(restored.usage().unwrap(), original);
        {
            let state = restored.state.lock().unwrap();
            assert_eq!(
                state.retained.get(topic).unwrap().message.properties,
                message.properties
            );
            assert_eq!(
                state
                    .sessions
                    .values()
                    .next()
                    .unwrap()
                    .offline
                    .front()
                    .unwrap()
                    .properties,
                message.properties
            );
        }
        fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn v5_message_expiry_cleans_offline_retained_and_recovery_state() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let auth = auth("a");
        let topic = "v1/t/t/p/p/d/a/up";
        let mut attachment = broker
            .attach_v5(&auth, "expiry".into(), false, 60, u16::MAX)
            .unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();
        attachment.detach().unwrap();
        broker
            .route(
                &auth.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"value".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: PublishProperties {
                        expires_at_ms: Some(now_ms() + 30_000),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        assert_eq!(broker.usage().unwrap().3, 1);
        {
            let mut state = broker.state.lock().unwrap();
            state
                .retained
                .get_mut(topic)
                .unwrap()
                .message
                .properties
                .expires_at_ms = Some(now_ms() - 1);
            state
                .retained_expiry
                .update(topic.to_owned(), Some(now_ms() - 1));
            state
                .sessions
                .get_mut(&attachment.key)
                .unwrap()
                .offline
                .front_mut()
                .unwrap()
                .properties
                .expires_at_ms = Some(now_ms() - 1);
            sync_session_usage(&mut state, &attachment.key).unwrap();
        }
        let directory =
            std::env::temp_dir().join(format!("netbaiot-mqtt-v4-expiry-{}", uuid::Uuid::new_v4()));
        broker.commit_to(&directory).await.unwrap();
        let restored = MqttBroker::new(limits);
        restored.recover_from(&directory).await.unwrap();
        assert_eq!(restored.usage().unwrap().3, 0);
        assert_eq!(restored.usage().unwrap().4, 0);
        assert_eq!(restored.state.lock().unwrap().offline_count, 0);
        fs::remove_dir_all(&directory).unwrap();
        broker.tick().unwrap();
        assert_eq!(broker.usage().unwrap().3, 0);
        assert_eq!(broker.state.lock().unwrap().offline_count, 0);
    }

    #[test]
    fn v5_no_local_uses_client_id_and_retain_options_control_replay() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let auth = auth("a");
        let topic = "v1/t/t/p/p/d/a/up";
        let mut a = broker
            .attach_v5(&auth, "client-a".into(), false, 60, u16::MAX)
            .unwrap();
        let mut b = broker
            .attach_v5(&auth, "client-b".into(), false, 60, u16::MAX)
            .unwrap();
        let options = v5::SubscriptionOptions {
            qos: 1,
            no_local: true,
            retain_as_published: true,
            retain_handling: 0,
        };
        broker
            .subscribe_v5(&a.key, a.generation, topic, options)
            .unwrap();
        broker
            .subscribe_v5(&b.key, b.generation, topic, options)
            .unwrap();
        broker
            .route_from_session(
                &a.key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"value".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert!(a.receiver.try_recv().is_err());
        let BrokerFrame::Publish(delivery) = b.receiver.try_recv().unwrap() else {
            panic!("expected PUBLISH")
        };
        assert!(delivery.message.retain);
        broker
            .puback(&b.key, b.generation, delivery.packet_id.unwrap())
            .unwrap();
        broker
            .subscribe_v5(
                &b.key,
                b.generation,
                topic,
                v5::SubscriptionOptions {
                    retain_handling: 1,
                    ..options
                },
            )
            .unwrap();
        assert!(b.receiver.try_recv().is_err());
        broker
            .subscribe_v5(
                &b.key,
                b.generation,
                topic,
                v5::SubscriptionOptions {
                    retain_handling: 0,
                    ..options
                },
            )
            .unwrap();
        assert!(matches!(b.receiver.try_recv(), Ok(BrokerFrame::Publish(_))));
        let mut c = broker
            .attach_v5(&auth, "client-c".into(), false, 60, u16::MAX)
            .unwrap();
        broker
            .subscribe_v5(
                &c.key,
                c.generation,
                topic,
                v5::SubscriptionOptions {
                    retain_handling: 2,
                    ..options
                },
            )
            .unwrap();
        assert!(c.receiver.try_recv().is_err());
        a.detach().unwrap();
        b.detach().unwrap();
        c.detach().unwrap();
    }
    #[test]
    fn wildcard_trie_and_dollar_rules() {
        assert!(topic_matches("sport/+/player1", "sport/team/player1"));
        assert!(topic_matches("sport/#", "sport"));
        assert!(topic_matches("sport/#", "sport/a/b"));
        assert!(!topic_matches("#", "$SYS/status"));
        assert!(!topic_matches("+", "$SYS"));
        assert!(!topic_matches("+/status", "$SYS/status"));
        assert!(topic_matches("$SYS/#", "$SYS/status"));
        let mut trie = SubscriptionTrie::default();
        let a = SessionKey {
            device: auth("a").device_key,
            client_id: "a".into(),
        };
        let b = SessionKey {
            device: auth("b").device_key,
            client_id: "b".into(),
        };
        trie.insert("sport/+", a.clone(), Subscription::v311(1));
        trie.insert("sport/#", b.clone(), Subscription::v311(2));
        let found = trie.matching("sport/tennis");
        assert_eq!(found.get(&a).map(|value| value.qos), Some(1));
        assert_eq!(found.get(&b).map(|value| value.qos), Some(2));
        trie.remove("sport/+", &a);
        assert!(!trie.matching("sport/tennis").contains_key(&a));
        let authorized = auth("a");
        let limits = Limits::default();
        assert!(subscribe_acl(&authorized, "v1/t/t/p/p/d/a/#", &limits));
        assert!(subscribe_acl(&authorized, "v1/t/t/p/p/d/a/up", &limits));
        for escaped in ["#", "v1/t/t/p/p/d/+/up", "v1/t/t/p/p/d/b/#"] {
            assert!(!subscribe_acl(&authorized, escaped, &limits));
        }
    }
    #[test]
    fn ownership_clean_session_qos2_and_snapshot() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits);
        let a = auth("a");
        let b = auth("b");
        let first = broker.attach(&a, "same".into(), false).unwrap();
        broker
            .subscribe(&first.key, first.generation, "v1/t/t/p/p/d/a/#", 2)
            .unwrap();
        broker.detach(&first.key, first.generation, false).unwrap();
        let resumed = broker.attach(&a, "same".into(), false).unwrap();
        assert!(resumed.session_present);
        let other = broker.attach(&b, "same".into(), false).unwrap();
        assert!(!other.session_present);
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/a/up".into(),
            payload: b"x".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        assert!(
            broker
                .inbound_qos2(&resumed.key, resumed.generation, 7, message.clone())
                .unwrap()
        );
        assert!(
            !broker
                .inbound_qos2(&resumed.key, resumed.generation, 7, message.clone())
                .unwrap()
        );
        assert_eq!(
            broker
                .inbound_qos2_message(&resumed.key, resumed.generation, 7)
                .unwrap(),
            Some((message, false))
        );
        broker
            .complete_inbound_qos2(&resumed.key, resumed.generation, 7)
            .unwrap();
        assert_eq!(
            broker
                .inbound_qos2_message(&resumed.key, resumed.generation, 7)
                .unwrap(),
            None
        );
        let snapshot = broker.snapshot().unwrap();
        let restored = MqttBroker::new(Arc::new(Limits::default()));
        restored.restore(snapshot).unwrap();
        assert!(
            restored
                .attach(&a, "same".into(), false)
                .unwrap()
                .session_present
        );
        let clean = restored.attach(&a, "same".into(), true).unwrap();
        assert!(!clean.session_present);
        assert_eq!(
            restored
                .subscription_qos(&clean.key, "v1/t/t/p/p/d/a/up")
                .unwrap(),
            None
        );
    }

    #[test]
    fn tenant_session_bytes_and_qos2_inflight_are_hard_bounded() {
        let limits = Arc::new(Limits {
            max_mqtt_session_state_bytes_per_tenant: 100,
            max_inflight_qos2_per_session: 1,
            max_inflight_qos2_per_tenant: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let a = auth("a");
        let first = broker.attach(&a, "a".into(), false).unwrap();
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/a/up".into(),
            payload: b"x".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        assert!(
            broker
                .inbound_qos2(&first.key, first.generation, 1, message)
                .is_err(),
            "the message charge must not exceed the tenant byte ceiling"
        );
        assert!(
            broker.attach(&auth("b"), "b".into(), false).is_err(),
            "a second session must not exceed the tenant byte ceiling"
        );

        let limits = Arc::new(Limits {
            max_inflight_qos2_per_session: 1,
            max_inflight_qos2_per_tenant: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let first = broker.attach(&a, "a".into(), false).unwrap();
        let b = auth("b");
        let second = broker.attach(&b, "b".into(), false).unwrap();
        let message = |device: &str| BrokerMessage {
            topic: format!("v1/t/t/p/p/d/{device}/up"),
            payload: b"x".to_vec(),
            qos: 2,
            retain: false,
            properties: Default::default(),
        };
        broker
            .inbound_qos2(&first.key, first.generation, 1, message("a"))
            .unwrap();
        assert!(
            broker
                .inbound_qos2(&second.key, second.generation, 1, message("b"))
                .is_err()
        );
    }

    #[tokio::test]
    async fn mqtt_outbound_ack_state_matrix_preserves_transaction_on_wrong_ack() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("ack-matrix");
        let mut attachment = broker.attach(&device, "ack-matrix".into(), false).unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, "matrix/#", 2)
            .unwrap();

        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: "matrix/qos2".into(),
                    payload: vec![2],
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(qos2) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected QoS2 publish")
        };
        let qos2_id = qos2.packet_id.unwrap();
        let await_pubrec = transaction_accounting(&broker, &attachment.key);
        assert!(
            broker
                .puback(&attachment.key, attachment.generation, qos2_id)
                .is_err()
        );
        assert_eq!(
            transaction_accounting(&broker, &attachment.key),
            await_pubrec
        );
        assert!(
            broker
                .pubcomp(&attachment.key, attachment.generation, qos2_id)
                .is_err()
        );
        assert_eq!(
            transaction_accounting(&broker, &attachment.key),
            await_pubrec
        );
        assert!(matches!(
            broker
                .pubrec(&attachment.key, attachment.generation, qos2_id)
                .unwrap(),
            BrokerFrame::Pubrel { dup: false, .. }
        ));
        let await_pubcomp = transaction_accounting(&broker, &attachment.key);
        assert!(
            broker
                .puback(&attachment.key, attachment.generation, qos2_id)
                .is_err()
        );
        assert_eq!(
            transaction_accounting(&broker, &attachment.key),
            await_pubcomp
        );
        broker
            .pubcomp(&attachment.key, attachment.generation, qos2_id)
            .unwrap();

        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: "matrix/qos1".into(),
                    payload: vec![1],
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(qos1) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected QoS1 publish")
        };
        let qos1_id = qos1.packet_id.unwrap();
        let await_puback = transaction_accounting(&broker, &attachment.key);
        assert!(
            broker
                .pubrec(&attachment.key, attachment.generation, qos1_id)
                .is_err()
        );
        assert_eq!(
            transaction_accounting(&broker, &attachment.key),
            await_puback
        );
        assert!(
            broker
                .pubcomp(&attachment.key, attachment.generation, qos1_id)
                .is_err()
        );
        assert_eq!(
            transaction_accounting(&broker, &attachment.key),
            await_puback
        );
        broker
            .puback(&attachment.key, attachment.generation, qos1_id)
            .unwrap();
    }

    #[test]
    fn mqtt_inbound_qos2_takeover_can_finish_event_accepted_handoff() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("takeover");
        let old = broker.attach(&device, "takeover".into(), false).unwrap();
        broker
            .inbound_qos2(
                &old.key,
                old.generation,
                7,
                BrokerMessage {
                    topic: "takeover/up".into(),
                    payload: vec![7],
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let InboundQos2Action::Deliver {
            session_incarnation,
            operation_id,
            ..
        } = broker
            .begin_inbound_qos2_delivery(&old.key, old.generation, 7)
            .unwrap()
        else {
            panic!("old connection must own delivery")
        };
        let replacement = broker.attach(&device, "takeover".into(), false).unwrap();
        assert_eq!(old.session_incarnation, replacement.session_incarnation);
        broker
            .finish_inbound_qos2_delivery(&old.key, session_incarnation, 7, operation_id)
            .unwrap();
        let InboundQos2Action::EventAccepted {
            session_incarnation,
            operation_id,
        } = broker
            .begin_inbound_qos2_delivery(&replacement.key, replacement.generation, 7)
            .unwrap()
        else {
            panic!("takeover must observe accepted transaction")
        };
        broker
            .route_inbound_qos2(
                &replacement.key,
                session_incarnation,
                7,
                operation_id,
                &device.device_key,
            )
            .unwrap();
        assert!(
            broker
                .route_inbound_qos2(
                    &replacement.key,
                    session_incarnation,
                    7,
                    operation_id,
                    &device.device_key,
                )
                .is_err()
        );
        let state = broker.state.lock().unwrap();
        assert!(state.sessions[&replacement.key].inbound_qos2.is_empty());
    }

    #[test]
    fn qos2_clean_session_incarnation_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("clean-incarnation");
        let old = broker.attach(&device, "same".into(), false).unwrap();
        broker
            .inbound_qos2(
                &old.key,
                old.generation,
                7,
                BrokerMessage {
                    topic: "clean/incarnation".into(),
                    payload: b"old".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let InboundQos2Action::Deliver {
            session_incarnation: old_incarnation,
            operation_id: old_operation,
            ..
        } = broker
            .begin_inbound_qos2_delivery(&old.key, old.generation, 7)
            .unwrap()
        else {
            panic!("old delivery must start")
        };
        let replacement = broker.attach(&device, "same".into(), true).unwrap();
        assert_ne!(old_incarnation, replacement.session_incarnation);
        broker
            .inbound_qos2(
                &replacement.key,
                replacement.generation,
                7,
                BrokerMessage {
                    topic: "clean/incarnation".into(),
                    payload: b"new".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let InboundQos2Action::Deliver {
            session_incarnation: new_incarnation,
            operation_id: new_operation,
            ..
        } = broker
            .begin_inbound_qos2_delivery(&replacement.key, replacement.generation, 7)
            .unwrap()
        else {
            panic!("new delivery must start")
        };
        assert!(
            broker
                .finish_inbound_qos2_delivery(&old.key, old_incarnation, 7, old_operation,)
                .is_err()
        );
        broker
            .finish_inbound_qos2_delivery(&replacement.key, new_incarnation, 7, new_operation)
            .unwrap();
        broker
            .route_inbound_qos2(
                &replacement.key,
                new_incarnation,
                7,
                new_operation,
                &device.device_key,
            )
            .unwrap();
    }

    #[test]
    fn mqtt_persistent_overload_qos1_001_is_atomic() {
        persistent_route_overload_is_atomic(1);
    }

    #[test]
    fn mqtt_persistent_overload_qos2_001_is_atomic() {
        persistent_route_overload_is_atomic(2);
    }

    fn persistent_route_overload_is_atomic(qos: u8) {
        let limits = Arc::new(Limits {
            max_offline_messages_per_session: 1,
            max_offline_messages_per_tenant: 8,
            max_offline_messages: 8,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let device = auth("atomic-route");
        let a = broker.attach(&device, "a".into(), false).unwrap();
        let b = broker.attach(&device, "b".into(), false).unwrap();
        broker
            .subscribe(&a.key, a.generation, "atomic/shared", qos)
            .unwrap();
        broker
            .subscribe(&b.key, b.generation, "atomic/shared", qos)
            .unwrap();
        broker
            .subscribe(&b.key, b.generation, "atomic/b-only", qos)
            .unwrap();
        broker.detach(&a.key, a.generation, false).unwrap();
        broker.detach(&b.key, b.generation, false).unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: "atomic/b-only".into(),
                    payload: vec![1],
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert!(matches!(
            broker.route(
                &device.device_key,
                BrokerMessage {
                    topic: "atomic/shared".into(),
                    payload: vec![2],
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            ),
            Err(Error::Overloaded)
        ));
        let state = broker.state.lock().unwrap();
        assert!(state.sessions[&a.key].offline.is_empty());
        assert_eq!(state.sessions[&b.key].offline.len(), 1);
    }

    fn route_projection_fixture(targets: usize, stored_payload_bytes: usize) -> Arc<MqttBroker> {
        let limits = Arc::new(Limits {
            max_persistent_sessions: targets + 1,
            max_persistent_sessions_per_tenant: targets + 1,
            max_subscriptions_per_device: targets + 1,
            max_subscriptions_per_tenant: targets + 1,
            max_subscriptions: targets + 1,
            max_offline_messages_per_tenant: targets.saturating_mul(2).max(1),
            max_offline_messages: targets.saturating_mul(2).max(1),
            max_offline_bytes_per_tenant: 96 * 1024 * 1024,
            max_offline_bytes: 96 * 1024 * 1024,
            max_mqtt_session_state_bytes_per_tenant: 96 * 1024 * 1024,
            global_mqtt_session_bytes: 128 * 1024 * 1024,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let device = auth("route-plan");
        for index in 0..targets {
            let attachment = broker
                .attach(&device, format!("client-{index}"), false)
                .unwrap();
            broker
                .subscribe(
                    &attachment.key,
                    attachment.generation,
                    "route/plan/shared",
                    1,
                )
                .unwrap();
            broker
                .detach(&attachment.key, attachment.generation, false)
                .unwrap();
        }
        if stored_payload_bytes > 0 {
            let mut state = broker.state.lock().unwrap();
            let mut total = 0usize;
            for session in state.sessions.values_mut() {
                let stored = BrokerMessage {
                    topic: "route/plan/stored".into(),
                    payload: vec![0x5a; stored_payload_bytes],
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                };
                let bytes = stored.bytes();
                session.offline.push_back(stored);
                session.offline_bytes += bytes;
                session.state_bytes += bytes;
                total += bytes;
            }
            state.offline_count += targets;
            state.offline_bytes += total;
            state.session_bytes += total;
        }
        broker
    }

    #[test]
    fn mqtt_route_preflight_bounded_memory_001() {
        let broker = route_projection_fixture(1_000, 8 * 1024);
        let state = broker.state.lock().unwrap();
        let stored_payload_bytes = state.offline_bytes;
        let plan = preflight_route(
            &state,
            &auth("route-plan").device_key,
            None,
            &BrokerMessage {
                topic: "route/plan/shared".into(),
                payload: b"one-message".to_vec(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
            &broker.limits,
        )
        .unwrap();
        assert_eq!(plan.targets.len(), 1_000);
        assert!(stored_payload_bytes > 8_000_000);
        assert!(plan.temporary_bytes() < 512 * 1024);
        assert!(plan.temporary_bytes() < stored_payload_bytes / 16);
    }

    #[test]
    fn mqtt_empty_subscription_route_hint_tracks_authoritative_state_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("route-hint");
        let mut attachment = broker
            .attach(&device, "route-hint-client".into(), false)
            .unwrap();
        let topic = "v1/t/t/p/p/d/route-hint/down";
        let message = BrokerMessage {
            topic: topic.into(),
            payload: b"payload".to_vec(),
            qos: 0,
            retain: false,
            properties: Default::default(),
        };

        assert_eq!(broker.subscription_count.load(Ordering::Acquire), 0);
        assert_eq!(
            broker.route(&device.device_key, message.clone()).unwrap(),
            0
        );
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 0)
            .unwrap();
        assert_eq!(broker.subscription_count.load(Ordering::Acquire), 1);
        assert_eq!(
            broker.route(&device.device_key, message.clone()).unwrap(),
            1
        );
        assert!(attachment.receiver.try_recv().is_ok());

        broker
            .unsubscribe(&attachment.key, attachment.generation, topic)
            .unwrap();
        assert_eq!(broker.subscription_count.load(Ordering::Acquire), 0);
        assert_eq!(broker.route(&device.device_key, message).unwrap(), 0);
    }

    #[test]
    fn mqtt_empty_subscription_route_hint_is_restored_and_cleaned_001() {
        let source = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("route-hint-restore");
        let mut attachment = source.attach(&device, "persistent".into(), false).unwrap();
        source
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/route-hint-restore/down",
                1,
            )
            .unwrap();
        attachment.detach().unwrap();

        let recovered = MqttBroker::new(Arc::new(Limits::default()));
        recovered.restore(source.snapshot().unwrap()).unwrap();
        assert_eq!(recovered.subscription_count.load(Ordering::Acquire), 1);
        let clean = recovered
            .attach(&device, "persistent".into(), true)
            .unwrap();
        assert_eq!(recovered.subscription_count.load(Ordering::Acquire), 0);
        drop(clean);
    }

    #[test]
    #[ignore = "manual 100/1000/configured-max route preflight benchmark"]
    fn mqtt_route_preflight_benchmark_manual() {
        for targets in [100usize, 1_000, 2_000] {
            let broker = route_projection_fixture(targets, 0);
            let state = broker.state.lock().unwrap();
            let started = std::time::Instant::now();
            let plan = preflight_route(
                &state,
                &auth("route-plan").device_key,
                None,
                &BrokerMessage {
                    topic: "route/plan/shared".into(),
                    payload: b"benchmark".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
                &broker.limits,
            )
            .unwrap();
            println!(
                "route-targets={targets} planning_us={} temporary_plan_bytes={}",
                started.elapsed().as_micros(),
                plan.temporary_bytes()
            );
        }
    }

    #[test]
    fn mqtt_authz_persistent_reset_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let mut allowed = auth("auth-reset");
        let first = broker.attach(&allowed, "persistent".into(), false).unwrap();
        broker
            .subscribe(
                &first.key,
                first.generation,
                "v1/t/t/p/p/d/auth-reset/down",
                1,
            )
            .unwrap();
        broker.detach(&first.key, first.generation, false).unwrap();
        allowed.permissions.commands = false;
        let replacement = broker.attach(&allowed, "persistent".into(), false).unwrap();
        assert!(!replacement.session_present);
        assert_eq!(
            broker
                .subscription_qos(&replacement.key, "v1/t/t/p/p/d/auth-reset/down")
                .unwrap(),
            None
        );
        drop(replacement);
        let mut rotated = allowed.clone();
        rotated.credential_version += 1;
        let credential_reset = broker.attach(&rotated, "persistent".into(), false).unwrap();
        assert!(!credential_reset.session_present);
        drop(credential_reset);
        rotated.auth_generation += 1;
        let generation_reset = broker.attach(&rotated, "persistent".into(), false).unwrap();
        assert!(!generation_reset.session_present);
        drop(generation_reset);
        let unrelated_auth = auth("auth-unrelated");
        let unrelated = broker
            .attach(&unrelated_auth, "persistent".into(), false)
            .unwrap();
        drop(unrelated);
        assert_eq!(
            broker
                .invalidate_sessions(&AuthInvalidation::Device {
                    device: allowed.device_key.clone(),
                })
                .unwrap(),
            1
        );
        assert!(
            broker
                .attach(&unrelated_auth, "persistent".into(), false)
                .unwrap()
                .session_present
        );
    }

    #[test]
    fn mqtt_persistent_codec_provenance_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let original = auth("codec-profile");
        let first = broker
            .attach(&original, "persistent".into(), false)
            .unwrap();
        broker
            .inbound_qos2(
                &first.key,
                first.generation,
                9,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/codec-profile/up".into(),
                    payload: b"v1".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        broker.detach(&first.key, first.generation, false).unwrap();

        let exact = broker
            .attach(&original, "persistent".into(), false)
            .unwrap();
        assert!(exact.session_present);
        broker.detach(&exact.key, exact.generation, false).unwrap();

        let mut changed_id = original.clone();
        changed_id.codec_id = CodecId::new("other-codec").unwrap();
        let reset = broker
            .attach(&changed_id, "persistent".into(), false)
            .unwrap();
        assert!(!reset.session_present);
        broker.detach(&reset.key, reset.generation, false).unwrap();

        let mut changed_version = changed_id.clone();
        changed_version.codec_version += 1;
        let reset = broker
            .attach(&changed_version, "persistent".into(), false)
            .unwrap();
        assert!(!reset.session_present);
    }

    #[test]
    fn auth_invalidation_counting_001() {
        let limits = Arc::new(Limits::default());
        let device = auth("counting");
        let sessions = netbaiot_runtime::Sessions::new(limits.clone());
        let (_lease, _commands) = sessions
            .register(Arc::new(device.clone()), netbaiot_core::Transport::Mqtt)
            .unwrap();
        let broker = MqttBroker::new(limits);
        let _active = broker.attach(&device, "active".into(), false).unwrap();
        let offline = broker.attach(&device, "offline".into(), false).unwrap();
        broker
            .detach(&offline.key, offline.generation, false)
            .unwrap();
        let invalidation = AuthInvalidation::Device {
            device: device.device_key.clone(),
        };
        let disconnected_connections = sessions.disconnect_matching(&invalidation).unwrap();
        let invalidated_mqtt_sessions = broker.invalidate_sessions(&invalidation).unwrap();
        let result = netbaiot_core::InvalidationResult {
            invalidated: 1,
            disconnected: disconnected_connections,
            invalidated_cache_entries: 1,
            disconnected_connections,
            invalidated_mqtt_sessions,
        };
        assert_eq!(result.disconnected_connections, 1);
        assert_eq!(result.invalidated_mqtt_sessions, 2);
        assert_eq!(
            result.disconnected, 1,
            "legacy field remains a connection count"
        );
        let decoded: netbaiot_core::InvalidationResult =
            serde_json::from_slice(&serde_json::to_vec(&result).unwrap()).unwrap();
        assert_eq!(decoded, result);
    }

    #[test]
    fn retain_replacement_reservation_001() {
        let limits = Arc::new(Limits {
            max_retained_messages: 1,
            max_retained_messages_per_tenant: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let device = auth("retain-replace");
        let topic = "retained/replacement";
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"one".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"qos1-replacement".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let attachment = broker.attach(&device, "qos2".into(), false).unwrap();
        broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                9,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"two".to_vec(),
                    qos: 2,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let InboundQos2Action::Deliver {
            session_incarnation,
            operation_id,
            ..
        } = broker
            .begin_inbound_qos2_delivery(&attachment.key, attachment.generation, 9)
            .unwrap()
        else {
            panic!("QoS2 replacement must be deliverable")
        };
        broker
            .finish_inbound_qos2_delivery(&attachment.key, session_incarnation, 9, operation_id)
            .unwrap();
        broker
            .route_inbound_qos2(
                &attachment.key,
                session_incarnation,
                9,
                operation_id,
                &device.device_key,
            )
            .unwrap();
        let mut guard = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"will".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        guard.publish().unwrap();
        assert!(broker.has_retained_topic(topic).unwrap());
        let state = broker.state.lock().unwrap();
        assert_eq!(state.retained_reserved_count, 0);
        assert_eq!(state.retained_reserved_bytes, 0);
    }

    #[test]
    fn mqtt_attachment_guard_cleans_clean_and_preserves_persistent_session() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("guard");
        drop(broker.attach(&device, "clean".into(), true).unwrap());
        let clean_probe = broker.attach(&device, "clean".into(), false).unwrap();
        assert!(!clean_probe.session_present);
        drop(clean_probe);

        drop(broker.attach(&device, "persistent".into(), false).unwrap());
        let persistent_probe = broker.attach(&device, "persistent".into(), false).unwrap();
        assert!(persistent_probe.session_present);
    }

    #[test]
    fn persistent_unsubscribe_commits_session_trie_and_offline_removal() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("persistent-unsub");
        let topic = "v1/t/t/p/p/d/persistent-unsub/up";
        let first = broker.attach(&device, "client".into(), false).unwrap();
        broker
            .subscribe(&first.key, first.generation, topic, 1)
            .unwrap();
        broker.detach(&first.key, first.generation, false).unwrap();

        let resumed = broker.attach(&device, "client".into(), false).unwrap();
        assert!(resumed.session_present);
        broker
            .unsubscribe(&resumed.key, resumed.generation, topic)
            .unwrap();
        {
            let state = broker.state.lock().unwrap();
            let session = state.sessions.get(&resumed.key).unwrap();
            assert!(!session.subscriptions.contains_key(topic));
            assert!(state.trie.matching(topic).is_empty());
        }
        broker
            .detach(&resumed.key, resumed.generation, false)
            .unwrap();

        assert_eq!(
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: b"stale".to_vec(),
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap(),
            0
        );
        {
            let state = broker.state.lock().unwrap();
            let session = state.sessions.get(&resumed.key).unwrap();
            assert!(session.offline.is_empty());
            assert_eq!(state.offline_count, 0);
        }

        let verify = broker.attach(&device, "client".into(), false).unwrap();
        assert!(verify.session_present);
        assert!(verify.receiver.is_empty());
        broker
            .subscribe(&verify.key, verify.generation, topic, 1)
            .unwrap();
        broker
            .detach(&verify.key, verify.generation, false)
            .unwrap();
        assert_eq!(
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: b"fresh".to_vec(),
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap(),
            1
        );
        let state = broker.state.lock().unwrap();
        assert_eq!(state.sessions.get(&verify.key).unwrap().offline.len(), 1);
    }

    #[test]
    fn mqtt_accepted_will_reservation_survives_later_retained_pressure() {
        let limits = Arc::new(Limits {
            max_retained_messages: 1,
            max_retained_messages_per_tenant: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let device = auth("will");
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: "will/reserved".into(),
                    payload: vec![0xff; 16],
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        {
            let state = broker.state.lock().unwrap();
            assert_eq!(state.retained_reserved_count, 1);
            assert!(state.retained_reserved_bytes > 0);
        }
        assert!(
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: "will/competitor".into(),
                        payload: vec![1],
                        qos: 1,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .is_err()
        );
        will.arm();
        will.publish().unwrap();
        assert!(broker.has_retained_topic("will/reserved").unwrap());
        let state = broker.state.lock().unwrap();
        assert_eq!(state.retained_reserved_count, 0);
        assert_eq!(state.retained_reserved_bytes, 0);
    }

    #[tokio::test]
    async fn will_subscriber_pressure_001() {
        let limits = Arc::new(Limits {
            max_offline_messages_per_session: 1,
            max_offline_messages_per_tenant: 8,
            max_offline_messages: 8,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let device = auth("will-pressure");
        let will_topic = "v1/t/t/p/p/d/will-pressure/up";
        let fill_topic = "v1/t/t/p/p/d/will-pressure/fill";
        let mut a = broker.attach(&device, "a".into(), false).unwrap();
        let b = broker.attach(&device, "b".into(), false).unwrap();
        broker
            .subscribe(&a.key, a.generation, will_topic, 1)
            .unwrap();
        broker
            .subscribe(&b.key, b.generation, will_topic, 1)
            .unwrap();
        broker
            .subscribe(&b.key, b.generation, fill_topic, 1)
            .unwrap();
        broker.detach(&b.key, b.generation, false).unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: fill_topic.into(),
                    payload: b"fill".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: will_topic.into(),
                    payload: b"will".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm();
        will.publish().unwrap();
        assert_eq!(broker.pending_will_count().unwrap(), 1);
        assert!(
            a.receiver.try_recv().is_err(),
            "A must not observe a partial route"
        );

        let mut resumed_b = broker.attach(&device, "b".into(), false).unwrap();
        let BrokerFrame::Publish(fill) = resumed_b.receiver.recv().await.unwrap() else {
            panic!("expected queued fill")
        };
        broker
            .puback(
                &resumed_b.key,
                resumed_b.generation,
                fill.packet_id.unwrap(),
            )
            .unwrap();
        let BrokerFrame::Publish(a_will) = a.receiver.recv().await.unwrap() else {
            panic!("expected Will for A")
        };
        let BrokerFrame::Publish(b_will) = resumed_b.receiver.recv().await.unwrap() else {
            panic!("expected Will for B")
        };
        assert_eq!(a_will.message.payload, b"will");
        assert_eq!(b_will.message.payload, b"will");
        assert_eq!(broker.pending_will_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn will_pending_restart_001() {
        let limits = Arc::new(Limits {
            max_offline_messages_per_session: 1,
            max_offline_messages_per_tenant: 8,
            max_offline_messages: 8,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits.clone());
        let device = auth("will-restart");
        let topic = "v1/t/t/p/p/d/will-restart/up";
        let subscriber = broker.attach(&device, "persistent".into(), false).unwrap();
        broker
            .subscribe(&subscriber.key, subscriber.generation, topic, 1)
            .unwrap();
        broker
            .detach(&subscriber.key, subscriber.generation, false)
            .unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"fill".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let mut will = broker
            .reserve_will(
                device.device_key.clone(),
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"restart-will".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        will.arm();
        will.publish().unwrap();
        assert_eq!(broker.pending_will_count().unwrap(), 1);

        let directory = std::env::temp_dir().join(format!(
            "netbaiot-will-pending-{}-{}",
            std::process::id(),
            now_ms()
        ));
        broker.commit_to(&directory).await.unwrap();
        let recovered = MqttBroker::new(limits);
        recovered.recover_from(&directory).await.unwrap();
        assert_eq!(recovered.pending_will_count().unwrap(), 1);
        let mut resumed = recovered
            .attach(&device, "persistent".into(), false)
            .unwrap();
        let BrokerFrame::Publish(fill) = resumed.receiver.recv().await.unwrap() else {
            panic!("expected recovered fill")
        };
        recovered
            .puback(&resumed.key, resumed.generation, fill.packet_id.unwrap())
            .unwrap();
        let BrokerFrame::Publish(will) = resumed.receiver.recv().await.unwrap() else {
            panic!("expected recovered pending Will")
        };
        assert_eq!(will.message.payload, b"restart-will");
        assert_eq!(recovered.pending_will_count().unwrap(), 0);
        fs::remove_dir_all(directory).unwrap();
    }

    async fn tenant_capacity_wakes_other_session(qos: u8) {
        let limits = Arc::new(Limits {
            max_inflight_qos1_per_tenant: 1,
            max_inflight_qos2_per_tenant: 1,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let mut a = broker.attach(&auth("wake-a"), "a".into(), false).unwrap();
        let mut b = broker.attach(&auth("wake-b"), "b".into(), false).unwrap();
        for attachment in [&a, &b] {
            broker
                .subscribe(&attachment.key, attachment.generation, "wake/topic", qos)
                .unwrap();
        }
        broker
            .route(
                &a.key.device,
                BrokerMessage {
                    topic: "wake/topic".into(),
                    payload: vec![qos],
                    qos,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let (live_key, live_generation, packet_id, waiting) =
            if let Ok(BrokerFrame::Publish(delivery)) = a.receiver.try_recv() {
                (
                    a.key.clone(),
                    a.generation,
                    delivery.packet_id.unwrap(),
                    &mut b.receiver,
                )
            } else if let Ok(BrokerFrame::Publish(delivery)) = b.receiver.try_recv() {
                (
                    b.key.clone(),
                    b.generation,
                    delivery.packet_id.unwrap(),
                    &mut a.receiver,
                )
            } else {
                panic!("one tenant session must receive the initial frame")
            };
        assert!(waiting.try_recv().is_err());
        if qos == 1 {
            broker
                .puback(&live_key, live_generation, packet_id)
                .unwrap();
        } else {
            broker
                .pubrec(&live_key, live_generation, packet_id)
                .unwrap();
            assert!(
                waiting.try_recv().is_err(),
                "PUBREC does not release QoS2 capacity"
            );
            broker
                .pubcomp(&live_key, live_generation, packet_id)
                .unwrap();
        }
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), waiting.recv()).await,
            Ok(Some(BrokerFrame::Publish(_)))
        ));
    }

    #[tokio::test]
    async fn mqtt_tenant_qos1_capacity_release_wakes_other_session() {
        tenant_capacity_wakes_other_session(1).await;
    }

    #[tokio::test]
    async fn mqtt_tenant_qos2_capacity_release_wakes_other_session() {
        tenant_capacity_wakes_other_session(2).await;
    }
    #[tokio::test]
    async fn retained_offline_and_outbound_qos_lifecycle_are_bounded() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let a = auth("a");
        let mut attachment = broker.attach(&a, "a".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/a/#",
                2,
            )
            .unwrap();
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/a/up".into(),
            payload: b"retained".to_vec(),
            qos: 2,
            retain: true,
            properties: Default::default(),
        };
        broker.route(&a.device_key, message).unwrap();
        let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
            panic!()
        };
        let id = delivery.packet_id.unwrap();
        assert!(matches!(
            broker
                .pubrec(&attachment.key, attachment.generation, id)
                .unwrap(),
            BrokerFrame::Pubrel { dup: false, .. }
        ));
        assert!(matches!(
            broker
                .pubrec(&attachment.key, attachment.generation, id)
                .unwrap(),
            BrokerFrame::Pubrel { dup: true, .. }
        ));
        broker
            .pubcomp(&attachment.key, attachment.generation, id)
            .unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/a/up".into(),
                    payload: b"offline".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let mut resumed = broker.attach(&a, "a".into(), false).unwrap();
        assert!(matches!(
            resumed.receiver.recv().await,
            Some(BrokerFrame::Publish(_))
        ));
    }

    #[tokio::test]
    async fn persistent_reconnect_retransmits_outbound_in_original_order() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("a");
        let topic = "v1/t/t/p/p/d/a/up";
        let mut attachment = broker.attach(&device, "ordered".into(), false).unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 1)
            .unwrap();

        let mut expected = Vec::new();
        for sequence in 0u8..16 {
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: vec![sequence],
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
                panic!("expected publish")
            };
            expected.push((delivery.packet_id.unwrap(), delivery.message.payload));
        }
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();

        let mut resumed = broker.attach(&device, "ordered".into(), false).unwrap();
        assert!(resumed.session_present);
        for (packet_id, payload) in expected {
            let BrokerFrame::Publish(delivery) = resumed.receiver.recv().await.unwrap() else {
                panic!("expected retransmitted publish")
            };
            assert_eq!(delivery.packet_id, Some(packet_id));
            assert_eq!(delivery.message.payload, payload);
            assert!(delivery.dup);
        }
    }

    #[test]
    fn retained_replay_capacity_failure_does_not_commit_subscription_or_trie() {
        let broker = MqttBroker::new(Arc::new(Limits {
            max_outbound_messages_per_connection: 1,
            ..Limits::default()
        }));
        let owner = auth("publisher");
        for suffix in ["one", "two"] {
            broker
                .route(
                    &owner.device_key,
                    BrokerMessage {
                        topic: format!("v1/t/t/p/p/d/publisher/{suffix}"),
                        payload: suffix.as_bytes().to_vec(),
                        qos: 0,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let subscriber = auth("subscriber");
        let mut attachment = broker
            .attach(&subscriber, "transactional-subscribe".into(), true)
            .unwrap();
        let filter = "v1/t/t/p/p/d/publisher/#";
        assert!(
            broker
                .subscribe(&attachment.key, attachment.generation, filter, 0)
                .is_err()
        );
        assert_eq!(
            broker.subscription_qos(&attachment.key, filter).unwrap(),
            None
        );
        broker
            .route(
                &owner.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/publisher/future".into(),
                    payload: b"future".to_vec(),
                    qos: 0,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert!(attachment.receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn routing_uses_minimum_qos_and_retained_replace_delete_semantics() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let a = auth("matrix");
        let mut attachment = broker.attach(&a, "matrix".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/matrix/+",
                1,
            )
            .unwrap();
        for publish_qos in 0..=2 {
            broker
                .route(
                    &a.device_key,
                    BrokerMessage {
                        topic: "v1/t/t/p/p/d/matrix/up".into(),
                        payload: vec![publish_qos],
                        qos: publish_qos,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            let BrokerFrame::Publish(delivery) = attachment.receiver.recv().await.unwrap() else {
                panic!("expected routed publish")
            };
            assert_eq!(delivery.message.qos, publish_qos.min(1));
            assert!(!delivery.message.retain);
            if let Some(packet_id) = delivery.packet_id
                && delivery.message.qos == 1
            {
                broker
                    .puback(&attachment.key, attachment.generation, packet_id)
                    .unwrap();
            }
        }
        assert_eq!(broker.usage().unwrap().3, 1, "retained value is replaced");
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/matrix/up".into(),
                    payload: Vec::new(),
                    qos: 0,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(
            broker.usage().unwrap().3,
            0,
            "zero payload deletes retained"
        );
    }

    #[tokio::test]
    async fn recovery_file_round_trip_preserves_session_retained_and_inflight() {
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-recovery-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&directory).unwrap();

        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let a = auth("recovery");
        let mut attachment = broker.attach(&a, "persistent".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/recovery/#",
                1,
            )
            .unwrap();
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/recovery/up".into(),
                    payload: b"durable".to_vec(),
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(first) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected publish")
        };
        let packet_id = first.packet_id.unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
        broker.commit_to(&directory).await.unwrap();

        let recovered = MqttBroker::new(Arc::new(Limits::default()));
        assert!(recovered.recover_from(&directory).await.unwrap());
        let mut resumed = recovered.attach(&a, "persistent".into(), false).unwrap();
        assert!(resumed.session_present);
        let BrokerFrame::Publish(replayed) = resumed.receiver.recv().await.unwrap() else {
            panic!("expected replayed publish")
        };
        assert_eq!(replayed.packet_id, Some(packet_id));
        assert!(replayed.dup);
        assert!(!replayed.message.retain);
        let mut retained_subscriber = recovered
            .attach(&a, "retained-reader".into(), true)
            .unwrap();
        recovered
            .subscribe(
                &retained_subscriber.key,
                retained_subscriber.generation,
                "v1/t/t/p/p/d/recovery/#",
                1,
            )
            .unwrap();
        let BrokerFrame::Publish(retained) = retained_subscriber.receiver.recv().await.unwrap()
        else {
            panic!("expected retained publish")
        };
        assert!(retained.message.retain);

        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn mqtt_recovery_v1_compat_001() {
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-v1-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("v1-compatible");
        let attachment = broker.attach(&device, "persistent".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/v1-compatible/#",
                1,
            )
            .unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
        let mut snapshot = broker.snapshot().unwrap();
        snapshot.format_version = RECOVERY_VERSION_V1;
        let payload = serde_json::to_vec(&snapshot).unwrap();
        let mut image = Vec::new();
        image.extend_from_slice(RECOVERY_MAGIC);
        image.extend_from_slice(&RECOVERY_VERSION_V1.to_be_bytes());
        image.extend_from_slice(&snapshot.snapshot_generation.to_be_bytes());
        image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        image.extend_from_slice(&payload);
        image.extend_from_slice(&Sha256::digest(&payload));
        fs::write(directory.join(RECOVERY_FILE), image).unwrap();
        let restored = MqttBroker::new(Arc::new(Limits::default()));
        assert!(restored.recover_from(&directory).await.unwrap());
        assert!(
            restored
                .attach(&device, "persistent".into(), false)
                .unwrap()
                .session_present
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn mqtt_recovery_v1_large_compat_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("v1-large");
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/v1-large/up".into(),
                    payload: vec![0xa5; 8 * 1024],
                    qos: 1,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let mut snapshot = broker.snapshot().unwrap();
        snapshot.format_version = RECOVERY_VERSION_V1;
        let mut legacy = serde_json::to_value(&snapshot).unwrap();
        let object = legacy.as_object_mut().unwrap();
        object.remove("pending_wills");
        if let Some(sessions) = object
            .get_mut("sessions")
            .and_then(|value| value.as_array_mut())
        {
            for session in sessions {
                let session = session.as_object_mut().unwrap();
                session.remove("incarnation");
                session.remove("authorization");
                session.remove("inbound_reservations");
            }
        }
        let payload = serde_json::to_vec(&legacy).unwrap();
        let mut image = Vec::new();
        image.extend_from_slice(RECOVERY_MAGIC);
        image.extend_from_slice(&RECOVERY_VERSION_V1.to_be_bytes());
        image.extend_from_slice(&snapshot.snapshot_generation.to_be_bytes());
        image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        image.extend_from_slice(&payload);
        image.extend_from_slice(&Sha256::digest(&payload));
        let lowered_v3_limits = Limits {
            mqtt_recovery_max_bytes: 1_024,
            ..Limits::default()
        };
        assert!(image.len() > lowered_v3_limits.mqtt_recovery_max_bytes);
        let decoded = decode_mqtt_recovery(&image, &lowered_v3_limits).unwrap();
        assert_eq!(decoded.format_version, RECOVERY_VERSION_V1);
        assert_eq!(decoded.retained.len(), 1);
    }

    #[test]
    fn mqtt_recovery_v2_compat_001() {
        let device = auth("v2-compatible");
        let mut payload = Vec::new();
        put_string(&mut payload, device.device_key.tenant_id.as_str()).unwrap();
        put_string(&mut payload, device.device_key.product_id.as_str()).unwrap();
        put_string(&mut payload, device.device_key.device_id.as_str()).unwrap();
        put_string(&mut payload, "persistent").unwrap();
        payload.extend_from_slice(&1u64.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&device.credential_version.to_be_bytes());
        payload.extend_from_slice(&device.auth_generation.to_be_bytes());
        payload.push(1);
        payload.push(1);
        payload.extend_from_slice(&1u16.to_be_bytes());
        payload.extend_from_slice(&now_ms().to_be_bytes());
        let mut header = Vec::new();
        header.extend_from_slice(RECOVERY_MAGIC);
        header.extend_from_slice(&RECOVERY_VERSION_V2.to_be_bytes());
        header.extend_from_slice(&1u64.to_be_bytes());
        let mut image = header.clone();
        image.extend_from_slice(&Sha256::digest(&header));
        image.push(RECORD_SESSION);
        image.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        image.extend_from_slice(&payload);
        image.extend_from_slice(&Sha256::digest(&payload));
        let decoded = decode_mqtt_recovery(&image, &Limits::default()).unwrap();
        assert_eq!(decoded.format_version, RECOVERY_VERSION_V2);
        let restored = MqttBroker::new(Arc::new(Limits::default()));
        restored.restore(decoded).unwrap();
        // v2 did not carry codec provenance, so it is readable but conservatively reset at attach.
        assert!(
            !restored
                .attach(&device, "persistent".into(), false)
                .unwrap()
                .session_present
        );
    }

    #[test]
    fn mqtt_recovery_semantic_invalid_001() {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let device = auth("semantic-invalid");
        let attachment = broker.attach(&device, "persistent".into(), false).unwrap();
        broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                5,
                BrokerMessage {
                    topic: "semantic/invalid".into(),
                    payload: vec![1],
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let mut snapshot = broker.snapshot().unwrap();
        snapshot.sessions[0].inbound_qos2.insert(
            5,
            InboundQos2State::AwaitPubrel(BrokerMessage {
                topic: "semantic/invalid".into(),
                payload: vec![1],
                qos: 1,
                retain: false,
                properties: Default::default(),
            }),
        );
        assert!(matches!(broker.restore(snapshot), Err(Error::Invalid)));
    }

    #[test]
    fn mqtt_recovery_acl_ownership_001() {
        let limits = Arc::new(Limits::default());
        let source = MqttBroker::new(limits.clone());
        let device = auth("acl-owner");
        let attachment = source.attach(&device, "persistent".into(), false).unwrap();
        source
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/acl-owner/#",
                1,
            )
            .unwrap();
        let base = source.snapshot().unwrap();
        let foreign = BrokerMessage {
            topic: "v1/t/t/p/p/d/other/up".into(),
            payload: b"foreign".to_vec(),
            qos: 1,
            retain: false,
            properties: Default::default(),
        };

        let mut invalid = base.clone();
        invalid.sessions[0]
            .subscriptions
            .insert("v1/t/t/p/p/d/other/#".into(), Subscription::v311(1));
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut invalid = base.clone();
        invalid.sessions[0].offline.push_back(foreign.clone());
        invalid.sessions[0].offline_bytes = foreign.bytes();
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut invalid = base.clone();
        invalid.sessions[0]
            .outbound
            .insert(1, OutboundState::AwaitPuback(foreign.clone()));
        invalid.sessions[0].outbound_order.push_back(1);
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut invalid = base.clone();
        invalid.sessions[0].inbound_qos2.insert(
            2,
            InboundQos2State::AwaitPubrel(BrokerMessage { qos: 2, ..foreign }),
        );
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut invalid = base.clone();
        let retained_message = BrokerMessage {
            topic: "v1/t/t/p/p/d/acl-owner/up".into(),
            payload: b"retained".to_vec(),
            qos: 1,
            retain: true,
            properties: Default::default(),
        };
        invalid.retained.push((
            retained_message.topic.clone(),
            RetainedMessage {
                tenant_id: TenantId::new("other-tenant").unwrap(),
                message: retained_message,
                origin: None,
            },
        ));
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut invalid = base.clone();
        invalid.pending_wills.push(PendingWill {
            owner: device.device_key.clone(),
            message: BrokerMessage {
                topic: "v1/t/t/p/p/d/other/up".into(),
                payload: b"foreign-will".to_vec(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
            due_at_ms: None,
            cancel_on_resume: None,
            message_expiry_interval: None,
            retained_reservation: RetainedReservation::default(),
        });
        assert!(MqttBroker::new(limits.clone()).restore(invalid).is_err());

        let mut legacy = base;
        legacy.sessions[0].authorization = None;
        let restored = MqttBroker::new(limits);
        restored.restore(legacy).unwrap();
        assert_eq!(
            restored
                .matching_subscription_count("v1/t/t/p/p/d/acl-owner/up")
                .unwrap(),
            0
        );
        assert!(
            !restored
                .attach(&device, "persistent".into(), false)
                .unwrap()
                .session_present
        );
    }

    #[tokio::test]
    async fn mqtt_recovery_v3_raw_binary_round_trip_bound() {
        let limits = Arc::new(Limits::default());
        limits.validate().unwrap();
        let broker = MqttBroker::new(limits.clone());
        for (index, byte) in [0_u8, 0x7f, 0x80, 0xff].into_iter().enumerate() {
            let device = auth(&format!("binary-recovery-{index}"));
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: format!("v1/t/t/p/p/d/binary-recovery-{index}/up"),
                        payload: vec![byte; 1024],
                        qos: 1,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-binary-recovery-{}-{}",
            std::process::id(),
            now_ms()
        ));
        broker.commit_to(&directory).await.unwrap();
        let encoded = fs::read(directory.join(RECOVERY_FILE)).unwrap();
        assert!(encoded.len() <= limits.mqtt_recovery_upper_bound().unwrap());
        assert!(
            encoded
                .windows(1024)
                .any(|window| window.iter().all(|byte| *byte == 0xff))
        );
        let recovered = MqttBroker::new(limits);
        assert!(recovered.recover_from(&directory).await.unwrap());
        assert_eq!(recovered.usage().unwrap().3, 4);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn mqtt_recovery_whole_image_integrity_001() {
        let limits = Arc::new(Limits::default());
        let broker = MqttBroker::new(limits.clone());
        let device = auth("integrity");
        let topic = "v1/t/t/p/p/d/integrity/up";
        let mut attachment = broker.attach(&device, "persistent".into(), false).unwrap();
        broker
            .subscribe(&attachment.key, attachment.generation, topic, 2)
            .unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"outbound".to_vec(),
                    qos: 2,
                    retain: true,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let _ = attachment.receiver.recv().await.unwrap();
        broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                77,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"inbound".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
        broker
            .route(
                &device.device_key,
                BrokerMessage {
                    topic: topic.into(),
                    payload: b"offline".to_vec(),
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-integrity-{}-{}",
            std::process::id(),
            now_ms()
        ));
        broker.commit_to(&directory).await.unwrap();
        let image = fs::read(directory.join(RECOVERY_FILE)).unwrap();
        let records_end = image.len() - RECOVERY_TRAILER_BYTES;
        let mut records = Vec::new();
        let mut at = RECOVERY_HEADER_BYTES;
        while at < records_end {
            let kind = image[at];
            let length = usize::try_from(u32::from_be_bytes(
                image[at + 1..at + 5].try_into().unwrap(),
            ))
            .unwrap();
            let end = at + RECORD_HEADER_BYTES + length + RECORD_CHECKSUM_BYTES;
            records.push((kind, at, end));
            at = end;
        }
        assert_eq!(at, records_end);
        for kind in [RECORD_OFFLINE, RECORD_OUTBOUND, RECORD_INBOUND_QOS2] {
            let (_, start, end) = records
                .iter()
                .copied()
                .find(|record| record.0 == kind)
                .unwrap();
            let mut mutated = image.clone();
            mutated.drain(start..end);
            assert!(decode_mqtt_recovery(&mutated, &limits).is_err());
        }
        assert!(decode_mqtt_recovery(&image[..records_end], &limits).is_err());

        let (_, first_start, first_end) = records[1];
        let (_, second_start, second_end) = records[2];
        let mut reordered = Vec::with_capacity(image.len());
        reordered.extend_from_slice(&image[..first_start]);
        reordered.extend_from_slice(&image[second_start..second_end]);
        reordered.extend_from_slice(&image[first_end..second_start]);
        reordered.extend_from_slice(&image[first_start..first_end]);
        reordered.extend_from_slice(&image[second_end..]);
        assert!(decode_mqtt_recovery(&reordered, &limits).is_err());

        let mut unknown = Vec::with_capacity(image.len() + RECORD_HEADER_BYTES + 32);
        unknown.extend_from_slice(&image[..records_end]);
        unknown.push(0xff);
        unknown.extend_from_slice(&0u32.to_be_bytes());
        unknown.extend_from_slice(&Sha256::digest([]));
        unknown.extend_from_slice(&image[records_end..]);
        assert!(decode_mqtt_recovery(&unknown, &limits).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn qos2_recovery_resumes_each_protocol_stage_without_reallocation() {
        let limits = Arc::new(Limits::default());
        let a = auth("qos2-recovery");
        let broker = MqttBroker::new(limits.clone());
        let mut attachment = broker.attach(&a, "qos2".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/qos2-recovery/#",
                2,
            )
            .unwrap();
        broker
            .route(
                &a.device_key,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/qos2-recovery/up".into(),
                    payload: b"outbound".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();
        let BrokerFrame::Publish(first) = attachment.receiver.recv().await.unwrap() else {
            panic!("expected qos2 publish")
        };
        let packet_id = first.packet_id.unwrap();
        broker
            .inbound_qos2(
                &attachment.key,
                attachment.generation,
                77,
                BrokerMessage {
                    topic: "v1/t/t/p/p/d/qos2-recovery/up".into(),
                    payload: b"inbound".to_vec(),
                    qos: 2,
                    retain: false,
                    properties: Default::default(),
                },
            )
            .unwrap();

        let before_pubrec = MqttBroker::new(limits.clone());
        before_pubrec.restore(broker.snapshot().unwrap()).unwrap();
        let mut resumed = before_pubrec.attach(&a, "qos2".into(), false).unwrap();
        let BrokerFrame::Publish(retry) = resumed.receiver.recv().await.unwrap() else {
            panic!("expected qos2 publish retry")
        };
        assert_eq!(retry.packet_id, Some(packet_id));
        assert!(retry.dup);
        assert!(
            before_pubrec
                .inbound_qos2_message(&resumed.key, resumed.generation, 77)
                .unwrap()
                .is_some()
        );

        before_pubrec
            .pubrec(&resumed.key, resumed.generation, packet_id)
            .unwrap();
        let before_pubcomp = MqttBroker::new(limits);
        before_pubcomp
            .restore(before_pubrec.snapshot().unwrap())
            .unwrap();
        let mut resumed = before_pubcomp.attach(&a, "qos2".into(), false).unwrap();
        assert!(matches!(
            resumed.receiver.recv().await,
            Some(BrokerFrame::Pubrel {
                packet_id: id,
                dup: true
            }) if id == packet_id
        ));

        let InboundQos2Action::Deliver {
            session_incarnation,
            operation_id,
            ..
        } = before_pubcomp
            .begin_inbound_qos2_delivery(&resumed.key, resumed.generation, 77)
            .unwrap()
        else {
            panic!("expected inbound QoS2 delivery ownership")
        };
        before_pubcomp
            .finish_inbound_qos2_delivery(&resumed.key, session_incarnation, 77, operation_id)
            .unwrap();
        let accepted_stage = MqttBroker::new(Arc::new(Limits::default()));
        accepted_stage
            .restore(before_pubcomp.snapshot().unwrap())
            .unwrap();
        let accepted_owner = accepted_stage.attach(&a, "qos2".into(), false).unwrap();
        assert!(matches!(
            accepted_stage
                .inbound_qos2_message(&accepted_owner.key, accepted_owner.generation, 77)
                .unwrap(),
            Some((_, true))
        ));
    }

    #[tokio::test]
    async fn recovery_file_supports_legal_state_larger_than_event_spool_record() {
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-large-recovery-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let limits = Arc::new(Limits {
            max_offline_bytes_per_session: 2_097_152,
            max_mqtt_session_state_bytes: 4_194_304,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits.clone());
        let a = auth("large-recovery");
        let attachment = broker.attach(&a, "persistent".into(), false).unwrap();
        broker
            .subscribe(
                &attachment.key,
                attachment.generation,
                "v1/t/t/p/p/d/large-recovery/#",
                1,
            )
            .unwrap();
        broker
            .detach(&attachment.key, attachment.generation, false)
            .unwrap();
        for sequence in 0..20u8 {
            broker
                .route(
                    &a.device_key,
                    BrokerMessage {
                        topic: "v1/t/t/p/p/d/large-recovery/up".into(),
                        payload: vec![sequence; 60_000],
                        qos: 1,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
        }
        broker.commit_to(&directory).await.unwrap();
        assert!(fs::metadata(directory.join(RECOVERY_FILE)).unwrap().len() > 1_048_576);
        let recovered = MqttBroker::new(limits);
        assert!(recovered.recover_from(&directory).await.unwrap());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    #[ignore = "manual 10/50/100 MiB recovery benchmark"]
    async fn mqtt_recovery_streaming_benchmark_manual() {
        let limits = Arc::new(Limits {
            max_retained_messages: 3_000,
            max_retained_messages_per_tenant: 3_000,
            max_retained_bytes: 125_829_120,
            max_retained_bytes_per_tenant: 125_829_120,
            mqtt_recovery_max_bytes: 335_544_320,
            ..Limits::default()
        });
        limits.validate().unwrap();
        for logical_mib in [10usize, 50, 100] {
            let directory = std::env::temp_dir().join(format!(
                "netbaiot-mqtt-bench-{}-{}-{logical_mib}",
                std::process::id(),
                now_ms()
            ));
            let broker = MqttBroker::new(limits.clone());
            let target = logical_mib * 1_048_576;
            let mut payload_bytes = 0usize;
            let mut index = 0usize;
            while payload_bytes < target {
                let length = (target - payload_bytes).min(60_000);
                let device = auth(&format!("bench-{index}"));
                broker
                    .route(
                        &device.device_key,
                        BrokerMessage {
                            topic: format!("v1/t/t/p/p/d/bench-{index}/up"),
                            payload: vec![index as u8; length],
                            qos: 0,
                            retain: true,
                            properties: Default::default(),
                        },
                    )
                    .unwrap();
                payload_bytes += length;
                index += 1;
            }
            let encode_started = std::time::Instant::now();
            broker.commit_to(&directory).await.unwrap();
            let encode_elapsed = encode_started.elapsed();
            let file_bytes = fs::metadata(directory.join(RECOVERY_FILE)).unwrap().len();
            let restored = MqttBroker::new(limits.clone());
            let decode_started = std::time::Instant::now();
            assert!(restored.recover_from(&directory).await.unwrap());
            let decode_elapsed = decode_started.elapsed();
            println!(
                "logical_mib={logical_mib} logical_bytes={payload_bytes} file_bytes={file_bytes} encode_ms={} decode_ms={} max_record_bytes={}",
                encode_elapsed.as_millis(),
                decode_elapsed.as_millis(),
                recovery_record_max(&limits).unwrap()
            );
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn recovery_file_rejects_corruption_and_unknown_version() {
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-corrupt-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        broker.commit_to(&directory).await.unwrap();
        let path = directory.join(RECOVERY_FILE);

        let mut corrupt = fs::read(&path).unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x80;
        fs::write(&path, &corrupt).unwrap();
        assert!(matches!(
            broker.recover_from(&directory).await,
            Err(Error::Invalid)
        ));

        broker.commit_to(&directory).await.unwrap();
        let mut unknown_version = fs::read(&path).unwrap();
        unknown_version[4..8].copy_from_slice(&(RECOVERY_VERSION + 1).to_be_bytes());
        fs::write(&path, &unknown_version).unwrap();
        assert!(matches!(
            broker.recover_from(&directory).await,
            Err(Error::Invalid)
        ));

        fs::remove_dir_all(directory).unwrap();
    }
}
