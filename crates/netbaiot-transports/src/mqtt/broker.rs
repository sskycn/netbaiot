use super::packet::valid_topic;
use netbaiot_core::{AuthenticatedDevice, DeviceKey, TenantId};
use netbaiot_runtime::{Error, Limits, Result, lock, now_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const STATE_OVERHEAD: usize = 64;
const RECOVERY_MAGIC: &[u8; 4] = b"NBMQ";
const RECOVERY_VERSION: u32 = 1;
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
        operation_id: u64,
    },
    EventAccepted,
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
    subscriptions: HashMap<String, u8>,
    offline: VecDeque<BrokerMessage>,
    offline_bytes: usize,
    inbound_qos2: HashMap<u16, InboundQos2State>,
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
    fn new(key: SessionKey) -> Self {
        let state_bytes = key.client_id.len()
            + key.device.tenant_id.as_str().len()
            + key.device.product_id.as_str().len()
            + key.device.device_id.as_str().len()
            + STATE_OVERHEAD;
        Self {
            key,
            subscriptions: HashMap::new(),
            offline: VecDeque::new(),
            offline_bytes: 0,
            inbound_qos2: HashMap::new(),
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
}

pub struct Attachment {
    pub key: SessionKey,
    pub generation: u64,
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
    reserved: bool,
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
        let result = self
            .broker
            .publish_reserved_will(&self.owner, &self.message, self.reserved);
        // Never retry publication from Drop after a partially observed routing failure.
        self.finished = true;
        result?;
        Ok(self.message.clone())
    }

    pub fn suppress(&mut self) -> Result<()> {
        if !self.finished {
            self.broker.release_will_reservation(
                &self.owner.tenant_id,
                &self.message,
                self.reserved,
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
                .publish_reserved_will(&self.owner, &self.message, self.reserved)
                .map(|_| ())
        } else {
            self.broker.release_will_reservation(
                &self.owner.tenant_id,
                &self.message,
                self.reserved,
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
        if clean_session {
            remove_session(&mut state, &key);
        }
        let session_present = !clean_session && state.sessions.contains_key(&key);
        if !state.sessions.contains_key(&key) {
            let session = StoredSession::new(key.clone());
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
        drop(state);
        let attachment = Attachment {
            key: key.clone(),
            generation,
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
            if tenant_session_bytes(&state, &key.device.tenant_id).saturating_add(charge)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
                || state.session_bytes.saturating_add(charge)
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
        if message.qos > 2 || !valid_topic(&message.topic, &self.limits, false) {
            return Err(Error::Invalid);
        }
        let reserved = message.retain && !message.payload.is_empty();
        if reserved {
            let mut state = lock(&self.state)?;
            reserve_retained(&mut state, &owner.tenant_id, &message, &self.limits)?;
        }
        Ok(WillGuard {
            broker: self.clone(),
            owner,
            message,
            reserved,
            armed: false,
            finished: false,
        })
    }

    fn release_will_reservation(
        &self,
        tenant: &TenantId,
        message: &BrokerMessage,
        reserved: bool,
    ) -> Result<()> {
        if reserved {
            let mut state = lock(&self.state)?;
            release_retained_reservation(&mut state, tenant, message);
        }
        Ok(())
    }

    fn publish_reserved_will(
        &self,
        owner: &DeviceKey,
        message: &BrokerMessage,
        reserved: bool,
    ) -> Result<usize> {
        let mut state = lock(&self.state)?;
        if reserved {
            release_retained_reservation(&mut state, &owner.tenant_id, message);
        }
        route_locked(&mut state, owner, message, &self.limits)
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
        reserve_retained(&mut state, &key.device.tenant_id, &message, &self.limits)?;
        if session_state_bytes.saturating_add(charge) > self.limits.max_mqtt_session_state_bytes
            || tenant_inflight(&state, &key.device.tenant_id, 2)
                >= self.limits.max_inflight_qos2_per_tenant
            || tenant_session_bytes(&state, &key.device.tenant_id).saturating_add(charge)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            || state.session_bytes.saturating_add(charge) > self.limits.global_mqtt_session_bytes
        {
            release_retained_reservation(&mut state, &key.device.tenant_id, &message);
            return Err(Error::Overloaded);
        }
        {
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes += charge;
            session
                .inbound_qos2
                .insert(packet_id, InboundQos2State::AwaitPubrel(message));
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
                    operation_id,
                }
            }
            InboundQos2State::Delivering { .. } => InboundQos2Action::DeliveryInProgress,
            InboundQos2State::EventAccepted(_) => InboundQos2Action::EventAccepted,
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
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
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
                Ok(())
            }
            InboundQos2State::EventAccepted(_) => Ok(()),
            _ => Err(Error::Conflict),
        }
    }

    pub fn abandon_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
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
        if let Some(message) = &message {
            release_retained_reservation(&mut state, &key.device.tenant_id, message);
            let charge = message.bytes();
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
        Ok(())
    }

    pub fn route_inbound_qos2(
        &self,
        key: &SessionKey,
        packet_id: u16,
        owner: &DeviceKey,
    ) -> Result<usize> {
        let mut state = lock(&self.state)?;
        let message = match state
            .sessions
            .get(key)
            .and_then(|session| session.inbound_qos2.get(&packet_id))
        {
            Some(InboundQos2State::EventAccepted(message)) => message.clone(),
            _ => return Err(Error::Conflict),
        };
        release_retained_reservation(&mut state, &key.device.tenant_id, &message);
        if message.retain {
            update_retained(&mut state, owner, &message, &self.limits)?;
        }
        let matches = state.trie.matching(&message.topic);
        let mut delivered = 0usize;
        for (subscriber, subscription_qos) in matches {
            let qos = message.qos.min(subscription_qos);
            match enqueue(
                &mut state,
                &subscriber,
                BrokerMessage {
                    qos,
                    retain: false,
                    ..message.clone()
                },
                &self.limits,
            ) {
                Ok(()) => delivered += 1,
                Err(Error::Overloaded | Error::Unavailable) => {}
                Err(error) => return Err(error),
            }
        }
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.inbound_qos2.remove(&packet_id).is_some() {
            let charge = message.bytes();
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
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
        Ok(())
    }

    pub fn snapshot(&self) -> Result<MqttRecoverySnapshot> {
        let state = lock(&self.state)?;
        Ok(MqttRecoverySnapshot {
            format_version: 1,
            snapshot_generation: state.generation,
            sessions: state
                .sessions
                .values()
                .cloned()
                .map(|mut session| {
                    session.active_generation = None;
                    session
                })
                .collect(),
            retained: state
                .retained
                .iter()
                .map(|(topic, retained)| (topic.clone(), retained.clone()))
                .collect(),
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

    pub async fn commit_to(&self, directory: &Path) -> Result<PathBuf> {
        let snapshot = self.snapshot()?;
        let directory = directory.to_path_buf();
        let limits = self.limits.clone();
        tokio::task::spawn_blocking(move || write_recovery(&directory, &limits, &snapshot))
            .await
            .map_err(|_| Error::Internal)?
    }

    pub fn restore(&self, snapshot: MqttRecoverySnapshot) -> Result<()> {
        if snapshot.format_version != 1
            || snapshot.sessions.len() > self.limits.max_persistent_sessions
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
            pending_by_tenant: HashMap::new(),
            pending_sessions: HashSet::new(),
        };
        for mut session in snapshot.sessions {
            session.active_generation = None;
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
                if !valid_topic(filter, &self.limits, true) || *qos > 2 {
                    return Err(Error::Invalid);
                }
                replacement.trie.insert(filter, session.key.clone(), *qos);
            }
            replacement.sessions.insert(session.key.clone(), session);
        }
        for (topic, retained) in snapshot.retained {
            if topic != retained.message.topic || !valid_topic(&topic, &self.limits, false) {
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
        let reservations = replacement
            .sessions
            .values()
            .flat_map(|session| {
                session.inbound_qos2.values().map(|entry| {
                    let message = match entry {
                        InboundQos2State::AwaitPubrel(message)
                        | InboundQos2State::Delivering { message, .. }
                        | InboundQos2State::EventAccepted(message) => message.clone(),
                    };
                    (session.key.device.tenant_id.clone(), message)
                })
            })
            .collect::<Vec<_>>();
        for (tenant, message) in reservations {
            reserve_retained(&mut replacement, &tenant, &message, &self.limits)?;
        }
        if replacement.subscription_count > self.limits.max_subscriptions
            || replacement.offline_count > self.limits.max_offline_messages
            || replacement.offline_bytes > self.limits.max_offline_bytes
            || replacement.session_bytes > self.limits.global_mqtt_session_bytes
            || replacement.retained_bytes > self.limits.max_retained_bytes
        {
            return Err(Error::Overloaded);
        }
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
            || tenant_session_bytes(state, &key.device.tenant_id).saturating_add(state_bytes)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            || state.session_bytes.saturating_add(state_bytes)
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
        for entry in session.inbound_qos2.values() {
            let message = match entry {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. }
                | InboundQos2State::EventAccepted(message) => message,
            };
            release_retained_reservation(state, &key.device.tenant_id, message);
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
    let tenant_bytes = tenant_session_bytes(state, &key.device.tenant_id);
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
                || state.session_bytes.saturating_add(charge) > limits.global_mqtt_session_bytes
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
    let mut tenant_state_bytes = tenant_session_bytes(state, &key.device.tenant_id)
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    let mut global_state_bytes = state
        .session_bytes
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
    let tenant_state_bytes = tenant_session_bytes(state, &key.device.tenant_id);
    let session = state.sessions.get_mut(key).ok_or(Error::Unavailable)?;
    if session.offline.len() >= limits.max_offline_messages_per_session
        || session.offline_bytes.saturating_add(bytes) > limits.max_offline_bytes_per_session
        || tenant_count >= limits.max_offline_messages_per_tenant
        || tenant_bytes.saturating_add(bytes) > limits.max_offline_bytes_per_tenant
        || state.offline_count >= limits.max_offline_messages
        || state.offline_bytes.saturating_add(bytes) > limits.max_offline_bytes
        || session.state_bytes.saturating_add(bytes) > limits.max_mqtt_session_state_bytes
        || tenant_state_bytes.saturating_add(bytes) > limits.max_mqtt_session_state_bytes_per_tenant
        || state.session_bytes.saturating_add(bytes) > limits.global_mqtt_session_bytes
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
    if message.retain {
        update_retained(state, owner, message, limits)?;
    }
    let matches = state.trie.matching(&message.topic);
    let mut delivered = 0usize;
    for (key, subscription_qos) in matches {
        let qos = message.qos.min(subscription_qos);
        match enqueue(
            state,
            &key,
            BrokerMessage {
                qos,
                retain: false,
                ..message.clone()
            },
            limits,
        ) {
            Ok(()) => delivered += 1,
            // A bounded slow/offline subscriber is isolated and shed. It must not make a
            // publication fail after another subscriber already received it.
            Err(Error::Overloaded | Error::Unavailable) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(delivered)
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
    if message.payload.len() > limits.max_retained_message_bytes {
        return Err(Error::Overloaded);
    }
    let old_bytes = state
        .retained
        .get(&message.topic)
        .map_or(0, |old| old.message.bytes());
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
    let replacing = old_bytes != 0;
    let (tenant_reserved_count, tenant_reserved_bytes) = state
        .retained_reserved_tenants
        .get(&owner.tenant_id)
        .copied()
        .unwrap_or_default();
    if (!replacing
        && (state
            .retained
            .len()
            .saturating_add(state.retained_reserved_count)
            >= limits.max_retained_messages
            || tenant_count.saturating_add(tenant_reserved_count)
                >= limits.max_retained_messages_per_tenant))
        || state
            .retained_bytes
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes)
            .saturating_add(state.retained_reserved_bytes)
            > limits.max_retained_bytes
        || tenant_bytes
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes)
            .saturating_add(tenant_reserved_bytes)
            > limits.max_retained_bytes_per_tenant
    {
        return Err(Error::Overloaded);
    }
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
) -> Result<()> {
    if !message.retain || message.payload.is_empty() {
        return Ok(());
    }
    if message.payload.len() > limits.max_retained_message_bytes {
        return Err(Error::Overloaded);
    }
    let bytes = message.bytes();
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
        >= limits.max_retained_messages
        || state
            .retained_bytes
            .saturating_add(state.retained_reserved_bytes)
            .saturating_add(bytes)
            > limits.max_retained_bytes
        || tenant_count.saturating_add(reserved.0) >= limits.max_retained_messages_per_tenant
        || tenant_bytes
            .saturating_add(reserved.1)
            .saturating_add(bytes)
            > limits.max_retained_bytes_per_tenant
    {
        return Err(Error::Overloaded);
    }
    state.retained_reserved_count += 1;
    state.retained_reserved_bytes += bytes;
    let tenant_reserved = state
        .retained_reserved_tenants
        .entry(tenant.clone())
        .or_default();
    tenant_reserved.0 += 1;
    tenant_reserved.1 += bytes;
    Ok(())
}

fn release_retained_reservation(
    state: &mut BrokerState,
    tenant: &TenantId,
    message: &BrokerMessage,
) {
    if !message.retain || message.payload.is_empty() {
        return;
    }
    let bytes = message.bytes();
    state.retained_reserved_count = state.retained_reserved_count.saturating_sub(1);
    state.retained_reserved_bytes = state.retained_reserved_bytes.saturating_sub(bytes);
    if let Some(reserved) = state.retained_reserved_tenants.get_mut(tenant) {
        reserved.0 = reserved.0.saturating_sub(1);
        reserved.1 = reserved.1.saturating_sub(bytes);
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

pub fn subscribe_acl(auth: &AuthenticatedDevice, filter: &str, limits: &Limits) -> bool {
    if !valid_topic(filter, limits, true) {
        return false;
    }
    let root = format!(
        "v1/t/{}/p/{}/d/{}/",
        auth.device_key.tenant_id.as_str(),
        auth.device_key.product_id.as_str(),
        auth.device_key.device_id.as_str()
    );
    if !filter.starts_with(&root) {
        return false;
    }
    let suffix = &filter[root.len()..];
    auth.permissions.commands && !suffix.is_empty()
}

fn write_recovery(
    directory: &Path,
    limits: &Limits,
    snapshot: &MqttRecoverySnapshot,
) -> Result<PathBuf> {
    fs::create_dir_all(directory).map_err(|_| Error::Storage)?;
    set_directory_permissions(directory)?;
    let payload = serde_json::to_vec(snapshot).map_err(|_| Error::Internal)?;
    if payload.len().saturating_add(52) > limits.mqtt_recovery_max_bytes {
        return Err(Error::Overloaded);
    }
    let temporary = directory.join(format!(".{RECOVERY_FILE}.tmp"));
    let committed = directory.join(RECOVERY_FILE);
    let mut file = open_private_replace(&temporary)?;
    file.write_all(RECOVERY_MAGIC)
        .and_then(|_| file.write_all(&RECOVERY_VERSION.to_be_bytes()))
        .and_then(|_| file.write_all(&snapshot.snapshot_generation.to_be_bytes()))
        .and_then(|_| {
            let length =
                u32::try_from(payload.len()).map_err(|_| std::io::ErrorKind::InvalidData)?;
            file.write_all(&length.to_be_bytes())
        })
        .and_then(|_| file.write_all(&payload))
        .and_then(|_| file.write_all(&Sha256::digest(&payload)))
        .map_err(|_| Error::Storage)?;
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
    if size < 52 || size > limits.mqtt_recovery_max_bytes {
        return Err(Error::Invalid);
    }
    let mut bytes = Vec::with_capacity(size);
    fs::File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|_| Error::Storage)?;
    if bytes.len() != size || bytes.get(..4) != Some(RECOVERY_MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != RECOVERY_VERSION {
        return Err(Error::Invalid);
    }
    let generation = u64::from_be_bytes(bytes[8..16].try_into().map_err(|_| Error::Invalid)?);
    let length = usize::try_from(u32::from_be_bytes(
        bytes[16..20].try_into().map_err(|_| Error::Invalid)?,
    ))
    .map_err(|_| Error::Invalid)?;
    if length.saturating_add(52) != size {
        return Err(Error::Invalid);
    }
    let payload_end = 20usize.checked_add(length).ok_or(Error::Invalid)?;
    let payload = bytes.get(20..payload_end).ok_or(Error::Invalid)?;
    let checksum = bytes
        .get(payload_end..payload_end + 32)
        .ok_or(Error::Invalid)?;
    if Sha256::digest(payload).as_slice() != checksum {
        return Err(Error::Invalid);
    }
    let snapshot: MqttRecoverySnapshot =
        serde_json::from_slice(payload).map_err(|_| Error::Invalid)?;
    if snapshot.snapshot_generation != generation || snapshot.format_version != version {
        return Err(Error::Invalid);
    }
    Ok(Some(snapshot))
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
    use netbaiot_core::*;
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
        let InboundQos2Action::Deliver { operation_id, .. } = broker
            .begin_inbound_qos2_delivery(&old.key, old.generation, 7)
            .unwrap()
        else {
            panic!("old connection must own delivery")
        };
        let replacement = broker.attach(&device, "takeover".into(), false).unwrap();
        broker
            .finish_inbound_qos2_delivery(&old.key, 7, operation_id)
            .unwrap();
        assert_eq!(
            broker
                .begin_inbound_qos2_delivery(&replacement.key, replacement.generation, 7)
                .unwrap(),
            InboundQos2Action::EventAccepted
        );
        broker
            .route_inbound_qos2(&replacement.key, 7, &device.device_key)
            .unwrap();
        assert!(
            broker
                .route_inbound_qos2(&replacement.key, 7, &device.device_key)
                .is_err()
        );
        let state = broker.state.lock().unwrap();
        assert!(state.sessions[&replacement.key].inbound_qos2.is_empty());
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
    async fn mqtt_recovery_bound_accepts_worst_case_binary_json_expansion() {
        let limits = Arc::new(Limits::default());
        limits.validate().unwrap();
        let broker = MqttBroker::new(limits.clone());
        let device = auth("binary-recovery");
        for (index, byte) in [0_u8, 0x7f, 0x80, 0xff].into_iter().enumerate() {
            broker
                .route(
                    &device.device_key,
                    BrokerMessage {
                        topic: format!("recovery/binary/{index}"),
                        payload: vec![byte; 1024],
                        qos: 1,
                        retain: true,
                    },
                )
                .unwrap();
        }
        let snapshot = broker.snapshot().unwrap();
        let encoded = serde_json::to_vec(&snapshot).unwrap();
        assert!(encoded.len().saturating_add(52) <= limits.mqtt_recovery_upper_bound().unwrap());
        assert!(encoded.windows(3).any(|window| window == b"255"));
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-mqtt-binary-recovery-{}-{}",
            std::process::id(),
            now_ms()
        ));
        broker.commit_to(&directory).await.unwrap();
        let recovered = MqttBroker::new(limits);
        assert!(recovered.recover_from(&directory).await.unwrap());
        assert_eq!(recovered.usage().unwrap().3, 4);
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

        let InboundQos2Action::Deliver { operation_id, .. } = before_pubcomp
            .begin_inbound_qos2_delivery(&resumed.key, resumed.generation, 77)
            .unwrap()
        else {
            panic!("expected inbound QoS2 delivery ownership")
        };
        before_pubcomp
            .finish_inbound_qos2_delivery(&resumed.key, 77, operation_id)
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
