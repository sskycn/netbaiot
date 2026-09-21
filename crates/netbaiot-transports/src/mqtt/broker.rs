use super::packet::valid_topic;
use netbaiot_core::{
    AuthInvalidation, AuthenticatedDevice, CodecId, DeviceId, DeviceKey, Permissions, ProductId,
    TenantId,
};
use netbaiot_runtime::{Error, Limits, Result, lock, now_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{BufReader, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const STATE_OVERHEAD: usize = 64;
const RECOVERY_MAGIC: &[u8; 4] = b"NBMQ";
const RECOVERY_VERSION_V1: u32 = 1;
const RECOVERY_VERSION_V2: u32 = 2;
const RECOVERY_VERSION: u32 = 3;
const LEGACY_V1_RECOVERY_READ_MAX: usize = 1_342_177_280;
const RECOVERY_FILE: &str = "mqtt-runtime.state";

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionKey {
    pub device: DeviceKey,
    pub client_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

impl BrokerMessage {
    fn bytes(&self) -> usize {
        self.topic.len() + self.payload.len() + STATE_OVERHEAD
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
    incarnation: u64,
    #[serde(default)]
    authorization: Option<SessionAuthorization>,
    subscriptions: HashMap<String, u8>,
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

    fn insert_outbound(&mut self, packet_id: u16, state: OutboundState) {
        if !self.outbound.contains_key(&packet_id) {
            self.outbound_order.push_back(packet_id);
        }
        self.outbound.insert(packet_id, state);
    }

    fn remove_outbound(&mut self, packet_id: u16) -> Option<OutboundState> {
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
    subscribers: HashMap<SessionKey, u8>,
}

#[derive(Default)]
struct SubscriptionTrie {
    root: TrieNode,
}

impl SubscriptionTrie {
    fn insert(&mut self, filter: &str, key: SessionKey, qos: u8) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.to_owned()).or_default();
        }
        node.subscribers.insert(key, qos);
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

    fn matching(&self, topic: &str) -> HashMap<SessionKey, u8> {
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
        output: &mut HashMap<SessionKey, u8>,
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

fn merge_subscribers(output: &mut HashMap<SessionKey, u8>, subscribers: &HashMap<SessionKey, u8>) {
    for (key, qos) in subscribers {
        output
            .entry(key.clone())
            .and_modify(|existing| *existing = (*existing).max(*qos))
            .or_insert(*qos);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetainedMessage {
    tenant_id: TenantId,
    message: BrokerMessage,
}

struct BrokerState {
    sessions: HashMap<SessionKey, StoredSession>,
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

fn tenant_session_bytes(state: &BrokerState, tenant: &TenantId) -> usize {
    state
        .sessions
        .values()
        .filter(|session| &session.key.device.tenant_id == tenant)
        .map(|session| session.state_bytes)
        .sum()
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
    state
        .sessions
        .values()
        .filter(|session| &session.key.device.tenant_id == tenant)
        .map(|session| {
            if qos == 1 {
                session
                    .outbound
                    .values()
                    .filter(|entry| matches!(entry, OutboundState::AwaitPuback(_)))
                    .count()
            } else {
                session.inbound_qos2.len()
                    + session
                        .outbound
                        .values()
                        .filter(|entry| !matches!(entry, OutboundState::AwaitPuback(_)))
                        .count()
            }
        })
        .sum()
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
    armed: bool,
    finished: bool,
}

impl WillGuard {
    pub fn arm(&mut self) {
        self.armed = true;
    }

    pub fn publish(&mut self) -> Result<BrokerMessage> {
        if self.finished {
            return Err(Error::Conflict);
        }
        let result =
            self.broker
                .publish_reserved_will(&self.owner, &self.message, self.reservation);
        // Never retry publication from Drop after a partially observed routing failure.
        self.finished = true;
        result?;
        Ok(self.message.clone())
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
            self.broker
                .publish_reserved_will(&self.owner, &self.message, self.reservation)
                .map(|_| ())
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
    state: Mutex<BrokerState>,
}

impl MqttBroker {
    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(BrokerState {
                sessions: HashMap::new(),
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
        let key = SessionKey {
            device: auth.device_key.clone(),
            client_id,
        };
        let mut state = lock(&self.state)?;
        self.prune_expired(&mut state);
        let authorization = SessionAuthorization::from(auth);
        if clean_session {
            remove_session(&mut state, &key);
        } else if state
            .sessions
            .get(&key)
            .is_some_and(|session| session.authorization.as_ref() != Some(&authorization))
        {
            // A persistent session is valid only under the authorization profile that created
            // it. Reauthentication with changed provenance starts a fresh MQTT session.
            remove_session(&mut state, &key);
        }
        let session_present = !clean_session && state.sessions.contains_key(&key);
        if !state.sessions.contains_key(&key) {
            state.generation = state.generation.wrapping_add(1).max(1);
            let session = StoredSession::new(key.clone(), state.generation, authorization);
            self.check_new_session(&state, &key, session.state_bytes)?;
            state.session_bytes = state.session_bytes.saturating_add(session.state_bytes);
            state.sessions.insert(key.clone(), session);
        }
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
            resume_frames(session, &self.limits, available_qos1, available_qos2)
        };
        let (resumed, resumed_count, resumed_bytes) = match resumed {
            Ok(resumed) => resumed,
            Err(error) => {
                state.active.remove(&key);
                if clean_session {
                    remove_session(&mut state, &key);
                } else if let Some(session) = state.sessions.get_mut(&key) {
                    session.active_generation = None;
                }
                return Err(error);
            }
        };
        state.offline_count = state.offline_count.saturating_sub(resumed_count);
        state.offline_bytes = state.offline_bytes.saturating_sub(resumed_bytes);
        if state
            .sessions
            .get(&key)
            .is_some_and(|session| !session.offline.is_empty())
        {
            mark_pending(&mut state, &key);
        }
        let session_incarnation = state.sessions.get(&key).ok_or(Error::Internal)?.incarnation;
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
        if clean_session {
            remove_session(&mut state, key);
        } else if let Some(session) = state.sessions.get_mut(key) {
            session.active_generation = None;
            session.last_seen_ms = now_ms();
        }
        Ok(())
    }

    pub fn subscribe(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        qos: u8,
    ) -> Result<u8> {
        if qos > 2 || !valid_topic(filter, &self.limits, true) {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let retained = state
            .retained
            .values()
            .filter(|retained| topic_matches(filter, &retained.message.topic))
            .map(|retained| BrokerMessage {
                qos: retained.message.qos.min(qos),
                retain: true,
                ..retained.message.clone()
            })
            .collect::<Vec<_>>();
        let replacement = state
            .sessions
            .get(key)
            .is_some_and(|session| session.subscriptions.contains_key(filter));
        if !replacement {
            let tenant_count = state
                .sessions
                .values()
                .filter(|session| session.key.device.tenant_id == key.device.tenant_id)
                .map(|session| session.subscriptions.len())
                .sum::<usize>();
            let device_count = state
                .sessions
                .values()
                .filter(|session| session.key.device == key.device)
                .map(|session| session.subscriptions.len())
                .sum::<usize>();
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
            .insert(filter.to_owned(), qos);
        state.trie.insert(filter, key.clone(), qos);
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
                return Err(error);
            }
        }
        Ok(qos)
    }

    pub fn unsubscribe(&self, key: &SessionKey, generation: u64, filter: &str) -> Result<()> {
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
            retry_pending_wills(&mut state, &self.limits);
        }
        Ok(())
    }

    pub fn route(&self, owner: &DeviceKey, message: BrokerMessage) -> Result<usize> {
        if message.qos > 2 || !valid_topic(&message.topic, &self.limits, false) {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        route_locked(&mut state, owner, &message, &self.limits)
    }

    pub fn reserve_will(
        self: &Arc<Self>,
        owner: DeviceKey,
        message: BrokerMessage,
    ) -> Result<WillGuard> {
        if message.qos > 2
            || message.payload.len() > self.limits.max_will_payload_bytes
            || !valid_topic(&message.topic, &self.limits, false)
        {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        reserve_will_capacity(&mut state, &owner.tenant_id, message.bytes(), &self.limits)?;
        let reservation = if message.retain && !message.payload.is_empty() {
            match reserve_retained(&mut state, &owner.tenant_id, &message, &self.limits) {
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
    ) -> Result<usize> {
        let mut state = lock(&self.state)?;
        if reservation != RetainedReservation::default() {
            release_retained_reservation(&mut state, &owner.tenant_id, reservation);
        }
        match route_locked(&mut state, owner, message, &self.limits) {
            Ok(delivered) => {
                release_will_capacity(&mut state, &owner.tenant_id, message.bytes());
                Ok(delivered)
            }
            Err(error) => {
                // CONNECT already transferred Will ownership to the broker. Preserve it under the
                // capacity reserved at CONNECT and retry only when another broker operation frees
                // resources; never create a task or spin.
                add_retained_reservation(&mut state, &owner.tenant_id, reservation);
                state.pending_wills.push_back(PendingWill {
                    owner: owner.clone(),
                    message: message.clone(),
                    retained_reservation: reservation,
                });
                tracing::warn!(%error, "MQTT Will publication deferred under bounded pressure");
                Ok(0)
            }
        }
    }

    pub fn pending_will_count(&self) -> Result<usize> {
        Ok(lock(&self.state)?.pending_wills.len())
    }

    pub fn inbound_qos2(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        message: BrokerMessage,
    ) -> Result<bool> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session_state_bytes = {
            let session = state.sessions.get(key).ok_or(Error::Internal)?;
            if let Some(existing) = session.inbound_qos2.get(&packet_id) {
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
            session.state_bytes
        };
        let charge = message.bytes();
        let reservation =
            reserve_retained(&mut state, &key.device.tenant_id, &message, &self.limits)?;
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
            if reservation != RetainedReservation::default() {
                session.inbound_reservations.insert(packet_id, reservation);
            }
        }
        state.session_bytes += charge;
        Ok(true)
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
        let delivered = match route_locked(&mut state, owner, &message, &self.limits) {
            Ok(delivered) => delivered,
            Err(error) => {
                let restored =
                    reserve_retained(&mut state, &key.device.tenant_id, &message, &self.limits)?;
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
                .map(|(_, qos)| *qos)
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

    pub fn pubrec(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<BrokerFrame> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        match session.outbound.get_mut(&packet_id) {
            Some(state @ OutboundState::AwaitPubrec(_)) => {
                let OutboundState::AwaitPubrec(message) = state.clone() else {
                    return Err(Error::Internal);
                };
                *state = OutboundState::AwaitPubcomp(message);
                Ok(BrokerFrame::Pubrel {
                    packet_id,
                    dup: false,
                })
            }
            Some(OutboundState::AwaitPubcomp(_)) => Ok(BrokerFrame::Pubrel {
                packet_id,
                dup: true,
            }),
            Some(OutboundState::AwaitPuback(_)) => Err(Error::Invalid),
            None => Err(Error::Invalid),
        }
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
            RECOVERY_VERSION_V1 | RECOVERY_VERSION_V2 | RECOVERY_VERSION
        ) || snapshot.sessions.len() > self.limits.max_persistent_sessions
            || snapshot.retained.len() > self.limits.max_retained_messages
        {
            return Err(Error::Configuration);
        }
        let mut replacement = BrokerState {
            sessions: HashMap::new(),
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
            let actual_offline_bytes =
                session.offline.iter().try_fold(0usize, |total, message| {
                    total.checked_add(message.bytes()).ok_or(Error::Overloaded)
                })?;
            if actual_offline_bytes != session.offline_bytes
                || actual_offline_bytes > self.limits.max_offline_bytes_per_session
            {
                return Err(Error::Invalid);
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
                .and_then(|value| value.checked_add(actual_offline_bytes))
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
            for (filter, qos) in &session.subscriptions {
                if !valid_topic(filter, &self.limits, true)
                    || *qos > 2
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
                    replacement.trie.insert(filter, session.key.clone(), *qos);
                }
            }
            replacement.sessions.insert(session.key.clone(), session);
        }
        for (topic, retained) in snapshot.retained {
            if topic != retained.message.topic
                || !valid_broker_message(&retained.message, &self.limits)
                || !retained_topic_owner_acl(&retained.tenant_id, &topic)
                || !retained.message.retain
                || retained.message.payload.is_empty()
                || retained.message.payload.len() > self.limits.max_retained_message_bytes
            {
                return Err(Error::Invalid);
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
                .map(|entry| entry.message.bytes())
                .sum::<usize>();
            if tenant_count >= self.limits.max_retained_messages_per_tenant
                || tenant_bytes.saturating_add(retained.message.bytes())
                    > self.limits.max_retained_bytes_per_tenant
            {
                return Err(Error::Overloaded);
            }
            replacement.retained_bytes = replacement
                .retained_bytes
                .checked_add(retained.message.bytes())
                .ok_or(Error::Overloaded)?;
            replacement.retained.insert(topic, retained);
        }
        for mut pending in snapshot.pending_wills {
            if !valid_broker_message(&pending.message, &self.limits)
                || pending.message.payload.len() > self.limits.max_will_payload_bytes
                || !device_publish_topic(&pending.owner, &pending.message.topic, None)
            {
                return Err(Error::Invalid);
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
        *lock(&self.state)? = replacement;
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
            remove_session(&mut state, key);
        }
        retry_pending_wills(&mut state, &self.limits);
        Ok(keys.len())
    }

    /// Read-only diagnostics used by benchmarks and operational capacity probes.
    pub fn matching_subscription_count(&self, topic: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        Ok(state.trie.matching(topic).len())
    }

    pub fn matching_retained_count(&self, filter: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        Ok(state
            .retained
            .values()
            .filter(|entry| topic_matches(filter, &entry.message.topic))
            .count())
    }

    pub fn has_retained_topic(&self, topic: &str) -> Result<bool> {
        Ok(lock(&self.state)?.retained.contains_key(topic))
    }

    fn check_new_session(
        &self,
        state: &BrokerState,
        key: &SessionKey,
        state_bytes: usize,
    ) -> Result<()> {
        let tenant = state
            .sessions
            .keys()
            .filter(|candidate| candidate.device.tenant_id == key.device.tenant_id)
            .count();
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

    fn prune_expired(&self, state: &mut BrokerState) {
        let cutoff = now_ms().saturating_sub(
            i64::try_from(self.limits.mqtt_session_idle_ttl_ms).unwrap_or(i64::MAX),
        );
        let expired = state
            .sessions
            .iter()
            .filter(|(key, session)| {
                !state.active.contains_key(*key) && session.last_seen_ms < cutoff
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            remove_session(state, &key)
        }
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
        if !session.has_outbound_capacity(message.qos, limits) {
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

fn remove_session(state: &mut BrokerState, key: &SessionKey) {
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
        let state = session.outbound.get(packet_id).ok_or(Error::Internal)?;
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
    }
    while frames.len() < limits.max_outbound_messages_per_connection {
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
            if !session.has_outbound_capacity(message.qos, limits) {
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
            session.state_bytes += charge;
            state.session_bytes += charge;
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
        .sessions
        .values()
        .filter(|candidate| candidate.key.device.tenant_id == key.device.tenant_id)
        .map(|candidate| candidate.offline.len())
        .sum::<usize>();
    let mut tenant_offline_bytes = state
        .sessions
        .values()
        .filter(|candidate| candidate.key.device.tenant_id == key.device.tenant_id)
        .map(|candidate| candidate.offline_bytes)
        .sum::<usize>();
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
            || !session.has_outbound_capacity(message.qos, limits);
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
        .sessions
        .values()
        .filter(|session| session.key.device.tenant_id == tenant_id)
        .map(|session| session.offline.len())
        .sum::<usize>();
    let tenant_bytes = state
        .sessions
        .values()
        .filter(|session| session.key.device.tenant_id == tenant_id)
        .map(|session| session.offline_bytes)
        .sum::<usize>();
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
    mark_pending(state, key);
    Ok(())
}

fn route_locked(
    state: &mut BrokerState,
    owner: &DeviceKey,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<usize> {
    let plan = preflight_route(state, owner, message, limits)?;
    if message.retain {
        update_retained(state, owner, message, limits)?;
    }
    let mut delivered = 0usize;
    for target in plan.targets {
        let routed = BrokerMessage {
            qos: target.qos,
            retain: false,
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
                session.state_bytes += charge;
                state.session_bytes += charge;
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
        let Some(pending) = state.pending_wills.pop_front() else {
            break;
        };
        release_retained_reservation(
            state,
            &pending.owner.tenant_id,
            pending.retained_reservation,
        );
        match route_locked(state, &pending.owner, &pending.message, limits) {
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
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<RoutePlan> {
    if message.retain {
        check_retained_update(state, owner, message, limits)?;
    }
    let matches = state.trie.matching(&message.topic);
    let relevant_tenants = matches
        .keys()
        .map(|key| key.device.tenant_id.clone())
        .collect::<HashSet<_>>();
    let mut tenant_usage = relevant_tenants
        .into_iter()
        .map(|tenant| (tenant, TenantRouteUsage::default()))
        .collect::<HashMap<_, _>>();
    // Compute tenant totals once. Planning memory contains only compact counters and one compact
    // target entry per match; no StoredSession or payload data is cloned.
    for session in state.sessions.values() {
        let Some(usage) = tenant_usage.get_mut(&session.key.device.tenant_id) else {
            continue;
        };
        usage.session_bytes = usage.session_bytes.saturating_add(session.state_bytes);
        usage.offline_count = usage.offline_count.saturating_add(session.offline.len());
        usage.offline_bytes = usage.offline_bytes.saturating_add(session.offline_bytes);
        usage.qos1_inflight = usage.qos1_inflight.saturating_add(
            session
                .outbound
                .values()
                .filter(|entry| matches!(entry, OutboundState::AwaitPuback(_)))
                .count(),
        );
        usage.qos2_inflight = usage
            .qos2_inflight
            .saturating_add(session.inbound_qos2.len())
            .saturating_add(
                session
                    .outbound
                    .values()
                    .filter(|entry| !matches!(entry, OutboundState::AwaitPuback(_)))
                    .count(),
            );
    }
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

    for (key, subscription_qos) in matches {
        let qos = message.qos.min(subscription_qos);
        if qos == 0 {
            plan.targets.push(PlannedRouteTarget {
                key,
                qos,
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
        let use_live =
            active_live && inflight < inflight_limit && session.has_outbound_capacity(qos, limits);
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
        plan.targets.push(PlannedRouteTarget { key, qos, mode });
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
    let old_bytes = existing.map_or(0, |old| old.message.bytes());
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
        .map(|entry| entry.message.bytes())
        .sum::<usize>();
    let new_bytes = message.bytes();
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
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.payload.is_empty() {
        if let Some(old) = state.retained.remove(&message.topic) {
            state.retained_bytes = state.retained_bytes.saturating_sub(old.message.bytes());
        }
        return Ok(());
    }
    check_retained_update(state, owner, message, limits)?;
    let old_bytes = state
        .retained
        .get(&message.topic)
        .map_or(0, |old| old.message.bytes());
    let new_bytes = message.bytes();
    state.retained_bytes = state
        .retained_bytes
        .saturating_sub(old_bytes)
        .saturating_add(new_bytes);
    state.retained.insert(
        message.topic.clone(),
        RetainedMessage {
            tenant_id: owner.tenant_id.clone(),
            message: message.clone(),
        },
    );
    Ok(())
}

fn reserve_retained(
    state: &mut BrokerState,
    tenant: &TenantId,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<RetainedReservation> {
    if !message.retain || message.payload.is_empty() {
        return Ok(RetainedReservation::default());
    }
    if message.payload.len() > limits.max_retained_message_bytes {
        return Err(Error::Overloaded);
    }
    let bytes = message.bytes();
    let existing = state.retained.get(&message.topic);
    let old_bytes = existing.map_or(0, |entry| entry.message.bytes());
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
        .map(|entry| entry.message.bytes())
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
        for (filter, qos) in subscriptions {
            record.clear();
            put_string(&mut record, filter)?;
            record.push(*qos);
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
    if !matches!(version, RECOVERY_VERSION_V2 | RECOVERY_VERSION) {
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
    let records_end = if version == RECOVERY_VERSION {
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
        if version == RECOVERY_VERSION {
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
    if version == RECOVERY_VERSION {
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
        .and_then(|value| value.checked_add(1_024))
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

fn decode_message(reader: &mut RecordReader<'_>, limits: &Limits) -> Result<BrokerMessage> {
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
    Ok(BrokerMessage {
        topic,
        payload,
        qos,
        retain,
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
                    let (codec_id, codec_version) = if format_version >= RECOVERY_VERSION {
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
            session.next_packet_id = next_packet_id;
            session.last_seen_ms = last_seen_ms;
            snapshot.sessions.push(session);
        }
        RECORD_SUBSCRIPTION => {
            let filter = reader.string(limits.max_topic_bytes)?;
            let qos = reader.u8()?;
            reader.finish()?;
            if qos > 2 || !valid_topic(&filter, limits, true) {
                return Err(Error::Invalid);
            }
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.subscriptions.len() >= limits.max_subscriptions_per_session {
                return Err(Error::Overloaded);
            }
            if session.subscriptions.insert(filter, qos).is_some() {
                return Err(Error::Invalid);
            }
        }
        RECORD_OFFLINE => {
            let message = decode_message(&mut reader, limits)?;
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
            let message = decode_message(&mut reader, limits)?;
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
            let message = decode_message(&mut reader, limits)?;
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
            let message = decode_message(&mut reader, limits)?;
            reader.finish()?;
            if message.payload.len() > limits.max_will_payload_bytes {
                return Err(Error::Invalid);
            }
            snapshot.pending_wills.push(PendingWill {
                owner,
                message,
                retained_reservation: RetainedReservation::default(),
            });
        }
        RECORD_RETAINED => {
            if snapshot.retained.len() >= limits.max_retained_messages {
                return Err(Error::Overloaded);
            }
            let tenant = TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let message = decode_message(&mut reader, limits)?;
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
    use std::time::Duration;
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

    #[derive(Debug, PartialEq, Eq)]
    struct TransactionAccounting {
        session_bytes: usize,
        broker_session_bytes: usize,
        offline_count: usize,
        offline_bytes: usize,
        retained_reserved_count: usize,
        retained_reserved_bytes: usize,
        inbound_qos2_count: usize,
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
            outbound_order: session.outbound_order.clone(),
            outbound: session.outbound.clone(),
        }
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
        trie.insert("sport/+", a.clone(), 1);
        trie.insert("sport/#", b.clone(), 2);
        let found = trie.matching("sport/tennis");
        assert_eq!(found.get(&a), Some(&1));
        assert_eq!(found.get(&b), Some(&2));
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
            &BrokerMessage {
                topic: "route/plan/shared".into(),
                payload: b"one-message".to_vec(),
                qos: 1,
                retain: false,
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
    #[ignore = "manual 100/1000/configured-max route preflight benchmark"]
    fn mqtt_route_preflight_benchmark_manual() {
        for targets in [100usize, 1_000, 2_000] {
            let broker = route_projection_fixture(targets, 0);
            let state = broker.state.lock().unwrap();
            let started = std::time::Instant::now();
            let plan = preflight_route(
                &state,
                &auth("route-plan").device_key,
                &BrokerMessage {
                    topic: "route/plan/shared".into(),
                    payload: b"benchmark".to_vec(),
                    qos: 1,
                    retain: false,
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
        };

        let mut invalid = base.clone();
        invalid.sessions[0]
            .subscriptions
            .insert("v1/t/t/p/p/d/other/#".into(), 1);
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
        };
        invalid.retained.push((
            retained_message.topic.clone(),
            RetainedMessage {
                tenant_id: TenantId::new("other-tenant").unwrap(),
                message: retained_message,
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
            },
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
