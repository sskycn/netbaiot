use super::{
    codec::{MqttVersion, v5},
    packet::valid_topic,
};
use netbaiot_core::{
    AuthInvalidation, AuthenticatedDevice, CodecId, DeviceId, DeviceKey, Permissions, ProductId,
    TenantId,
};
use netbaiot_runtime::recovery_io;
use netbaiot_runtime::{
    BrokerProbe, ByteBudget, BytesPermit, Error, Histogram, Limits, Metrics, Result,
    WeakByteBudget, lock, now_ms,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
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
const RECOVERY_VERSION_V5: u32 = 5;
const RECOVERY_VERSION: u32 = 6;
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
    pub topic: Arc<str>,
    #[serde(with = "payload_bytes")]
    pub payload: bytes::Bytes,
    pub qos: u8,
    pub retain: bool,
    #[serde(default)]
    pub properties: SharedPublishProperties,
}

// Keep historical JSON recovery payloads as arrays of byte values.
mod payload_bytes {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        payload: &bytes::Bytes,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        payload.as_ref().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<bytes::Bytes, D::Error> {
        Vec::<u8>::deserialize(deserializer).map(bytes::Bytes::from)
    }
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

/// Cloneable broker metadata with copy-on-write mutation. Empty MQTT 3.1.1
/// properties need no allocation or shared global reference counter. Recovery
/// serialization remains exactly the historical PublishProperties object.
#[derive(Clone, Default)]
pub struct SharedPublishProperties(Option<Arc<PublishProperties>>);
static EMPTY_PUBLISH_PROPERTIES: PublishProperties = PublishProperties {
    payload_format: None,
    expires_at_ms: None,
    content_type: None,
    response_topic: None,
    correlation_data: None,
    user_properties: Vec::new(),
};
impl From<PublishProperties> for SharedPublishProperties {
    fn from(value: PublishProperties) -> Self {
        if value == PublishProperties::default() {
            Self(None)
        } else {
            Self(Some(Arc::new(value)))
        }
    }
}
impl std::ops::Deref for SharedPublishProperties {
    type Target = PublishProperties;
    fn deref(&self) -> &Self::Target {
        self.0.as_deref().unwrap_or(&EMPTY_PUBLISH_PROPERTIES)
    }
}
impl std::ops::DerefMut for SharedPublishProperties {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(
            self.0
                .get_or_insert_with(|| Arc::new(PublishProperties::default())),
        )
    }
}
impl std::fmt::Debug for SharedPublishProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}
impl PartialEq for SharedPublishProperties {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl Eq for SharedPublishProperties {}
impl Serialize for SharedPublishProperties {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        (**self).serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SharedPublishProperties {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        PublishProperties::deserialize(deserializer).map(Self::from)
    }
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

#[derive(Debug)]
pub struct BrokerDelivery {
    pub message: BrokerMessage,
    pub packet_id: Option<u16>,
    pub dup: bool,
    pub command: bool,
    pub progress: Option<Arc<netbaiot_runtime::CommandProgress>>,
    unsent_command: UnsentCommandGuard,
    /// Includes the connection, tenant and process charge while this physical
    /// outbound copy waits in the channel or is being written to the socket.
    pub(super) _budget: Vec<BytesPermit>,
}

#[derive(Debug, Default)]
struct UnsentCommandGuard(Option<Arc<netbaiot_runtime::CommandProgress>>);

impl Drop for UnsentCommandGuard {
    fn drop(&mut self) {
        // QoS0 has no protocol ACK or persistent responsibility to finish the
        // command after this physical frame is lost during connection teardown.
        if let Some(progress) = &self.0 {
            progress.abandon_unsent();
        }
    }
}

impl BrokerDelivery {
    pub(super) fn begin_qos0_transfer(&mut self) -> bool {
        if let Some(progress) = &self.progress
            && !progress.begin_transfer()
        {
            return false;
        }
        // Encoding succeeded; the transport now owns the socket write outcome.
        self.unsent_command.0.take();
        true
    }
}

#[derive(Debug)]
pub enum BrokerFrame {
    Publish(Box<BrokerDelivery>),
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
    IdentifierInUse,
}

type ClientIdPreflight<'a> = dyn Fn(&str) -> Result<()> + 'a;

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
    #[serde(skip)]
    command_outbound: HashSet<u16>,
    #[serde(skip)]
    command_progress: HashMap<u16, Arc<netbaiot_runtime::CommandProgress>>,
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
    #[serde(default)]
    origin: Option<SessionKey>,
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

impl PendingWill {
    fn bytes(&self) -> usize {
        will_charge(&self.message, self.origin.as_ref())
    }
}

#[derive(Clone)]
struct ActiveSession {
    generation: u64,
    sender: mpsc::Sender<BrokerFrame>,
    cancel: CancellationToken,
    connection_bytes: ByteBudget,
    tenant_bytes: ByteBudget,
    global_bytes: ByteBudget,
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
    tenant_outbound_bytes: HashMap<TenantId, WeakByteBudget>,
    trie: SubscriptionTrie,
    retained: HashMap<String, RetainedMessage>,
    retained_tenant_usage: HashMap<TenantId, (usize, usize)>,
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
    /// Ready Wills only. Future delayed Wills live in `future_wills` until due.
    pending_wills: VecDeque<PendingWill>,
    future_wills: BTreeMap<i64, BTreeMap<u64, PendingWill>>,
    /// Derived owner lookup for delayed Wills; rebuilt from the recovery records.
    future_wills_by_session: HashMap<SessionKey, BTreeSet<(i64, u64)>>,
    next_will_token: u64,
    will_responsibility_count: usize,
    will_responsibility_bytes: usize,
    will_responsibility_tenants: HashMap<TenantId, (usize, usize)>,
    pending_by_tenant: HashMap<(TenantId, u8), BTreeMap<u64, SessionKey>>,
    pending_global: BTreeMap<u64, SessionKey>,
    pending_sessions: HashMap<SessionKey, (u8, u64)>,
    next_pending_token: u64,
    /// Capacity releases awaiting indexed promotion. Drained under the broker lock.
    capacity_wakes: BTreeSet<(TenantId, u8)>,
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
    origin: Option<SessionKey>,
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

pub struct MqttBroker {
    limits: Arc<Limits>,
    global_outbound_bytes: ByteBudget,
    metrics: Option<Arc<Metrics>>,
    /// Derived from `BrokerState::subscription_count`. Broker state remains authoritative; this
    /// hint only lets a non-retained route with no possible target linearize without the mutex.
    subscription_count: AtomicUsize,
    state: Mutex<BrokerState>,
    recovery_owner: Mutex<Option<Arc<recovery_io::RecoveryDirectory>>>,
    #[cfg(test)]
    replay_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
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
            global_outbound_bytes: ByteBudget::new(limits.max_outbound_bytes),
            limits,
            metrics,
            subscription_count: AtomicUsize::new(0),
            recovery_owner: Mutex::new(None),
            #[cfg(test)]
            replay_hook: Mutex::new(None),
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
                tenant_outbound_bytes: HashMap::new(),
                trie: SubscriptionTrie::default(),
                retained: HashMap::new(),
                retained_tenant_usage: HashMap::new(),
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
                future_wills: BTreeMap::new(),
                future_wills_by_session: HashMap::new(),
                next_will_token: 0,
                will_responsibility_count: 0,
                will_responsibility_bytes: 0,
                will_responsibility_tenants: HashMap::new(),
                pending_by_tenant: HashMap::new(),
                pending_global: BTreeMap::new(),
                pending_sessions: HashMap::new(),
                next_pending_token: 0,
                capacity_wakes: BTreeSet::new(),
            }),
        })
    }
}

/// Applies the same bounded-state decisions as `enqueue` to a target-session accounting copy,
/// without touching the global broker or an active connection channel.
enum RetainedReplayAdmission {
    Live(Vec<BytesPermit>),
    Offline,
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
    budget: Vec<BytesPermit>,
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

const fn version_one() -> u32 {
    RECOVERY_VERSION_V1
}

#[cfg(test)]
#[path = "../broker_hotspot_bench.rs"]
mod hotspot_bench;

mod expiry;
use expiry::*;
mod inbound_qos2;
mod maintenance;
mod outbound;
use outbound::*;
mod recovery;
#[cfg(test)]
use recovery::*;
mod retained;
use retained::*;
mod routing;
use routing::*;
mod session;
use session::*;
mod state;
use state::*;
mod subscription;
mod will;
use will::*;

pub use recovery::decode_mqtt_recovery;
pub use routing::{subscribe_acl, topic_matches};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod second_round_bench;

mod profiling;
