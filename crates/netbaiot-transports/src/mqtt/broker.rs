use super::packet::valid_topic;
use netbaiot_core::{AuthenticatedDevice, DeviceKey, TenantId};
use netbaiot_runtime::{Error, Limits, Result, lock, now_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
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
    EventAccepted(BrokerMessage),
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
    subscription_count: usize,
    session_bytes: usize,
    offline_count: usize,
    offline_bytes: usize,
    retained_bytes: usize,
    retained_reserved_count: usize,
    retained_reserved_bytes: usize,
    retained_reserved_tenants: HashMap<TenantId, (usize, usize)>,
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
                subscription_count: 0,
                session_bytes: 0,
                offline_count: 0,
                offline_bytes: 0,
                retained_bytes: 0,
                retained_reserved_count: 0,
                retained_reserved_bytes: 0,
                retained_reserved_tenants: HashMap::new(),
            }),
        })
    }

    pub fn attach(
        &self,
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
        let session = state.sessions.get_mut(&key).ok_or(Error::Internal)?;
        session.active_generation = Some(generation);
        session.last_seen_ms = now_ms();
        let (resumed, resumed_count, resumed_bytes) =
            resume_frames(session, &self.limits, available_qos1, available_qos2)?;
        state.offline_count = state.offline_count.saturating_sub(resumed_count);
        state.offline_bytes = state.offline_bytes.saturating_sub(resumed_bytes);
        for frame in resumed.into_iter().take(capacity) {
            sender.try_send(frame).map_err(|_| Error::Overloaded)?;
        }
        Ok(Attachment {
            key,
            generation,
            session_present,
            receiver,
            cancel,
        })
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
        let retained = state
            .retained
            .values()
            .filter(|retained| topic_matches(filter, &retained.message.topic))
            .map(|retained| retained.message.clone())
            .collect::<Vec<_>>();
        for message in retained {
            let outgoing = message.qos.min(qos);
            enqueue(
                &mut state,
                key,
                BrokerMessage {
                    qos: outgoing,
                    retain: true,
                    ..message
                },
                &self.limits,
            )?;
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
        if message.retain {
            update_retained(&mut state, owner, &message, &self.limits)?;
        }
        let matches = state.trie.matching(&message.topic);
        let mut delivered = 0usize;
        for (key, subscription_qos) in matches {
            let qos = message.qos.min(subscription_qos);
            let result = enqueue(
                &mut state,
                &key,
                BrokerMessage {
                    qos,
                    retain: false,
                    ..message.clone()
                },
                &self.limits,
            );
            match result {
                Ok(()) => delivered += 1,
                // A bounded slow/offline subscriber is isolated and shed. It must not make a
                // publish partially fail and then be replayed to already-enqueued subscribers.
                Err(Error::Overloaded | Error::Unavailable) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(delivered)
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

    pub fn inbound_qos2_message(
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
            .map(|state| match state {
                InboundQos2State::AwaitPubrel(message) => (message.clone(), false),
                InboundQos2State::EventAccepted(message) => (message.clone(), true),
            }))
    }

    pub fn mark_inbound_qos2_event_accepted(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let entry = session
            .inbound_qos2
            .get_mut(&packet_id)
            .ok_or(Error::Invalid)?;
        if let InboundQos2State::AwaitPubrel(message) = entry {
            *entry = InboundQos2State::EventAccepted(message.clone());
        }
        Ok(())
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
        generation: u64,
        packet_id: u16,
        owner: &DeviceKey,
    ) -> Result<usize> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
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
            return Ok(None);
        }
        let (frame, bytes) = {
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            let Some(message) = session.offline.pop_front() else {
                return Ok(None);
            };
            let bytes = message.bytes();
            session.offline_bytes = session.offline_bytes.saturating_sub(bytes);
            if !session.has_outbound_capacity(message.qos, &self.limits) {
                session.offline.push_front(message);
                session.offline_bytes = session.offline_bytes.saturating_add(bytes);
                return Ok(None);
            }
            let id = session.allocate_packet_id()?;
            let outbound = if message.qos == 1 {
                OutboundState::AwaitPuback(message.clone())
            } else {
                OutboundState::AwaitPubrec(message.clone())
            };
            session.outbound.insert(id, outbound);
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

    pub fn puback(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<()> {
        self.complete_outbound(key, generation, packet_id, false)
    }

    pub fn pubrec(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<BrokerFrame> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        match session.outbound.remove(&packet_id) {
            Some(OutboundState::AwaitPubrec(message)) => {
                session
                    .outbound
                    .insert(packet_id, OutboundState::AwaitPubcomp(message));
                Ok(BrokerFrame::Pubrel {
                    packet_id,
                    dup: false,
                })
            }
            Some(state @ OutboundState::AwaitPubcomp(_)) => {
                session.outbound.insert(packet_id, state);
                Ok(BrokerFrame::Pubrel {
                    packet_id,
                    dup: true,
                })
            }
            Some(other) => {
                session.outbound.insert(packet_id, other);
                Err(Error::Invalid)
            }
            None => Err(Error::Invalid),
        }
    }

    pub fn pubcomp(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<()> {
        self.complete_outbound(key, generation, packet_id, true)
    }

    fn complete_outbound(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        qos2: bool,
    ) -> Result<()> {
        let mut state = lock(&self.state)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let Some(outbound) = session.outbound.remove(&packet_id) else {
            return Err(Error::Invalid);
        };
        let expected = matches!(outbound, OutboundState::AwaitPubcomp(_)) == qos2;
        if !expected {
            session.outbound.insert(packet_id, outbound);
            return Err(Error::Invalid);
        }
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
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
            subscription_count: 0,
            session_bytes: 0,
            offline_count: 0,
            offline_bytes: 0,
            retained_bytes: 0,
            retained_reserved_count: 0,
            retained_reserved_bytes: 0,
            retained_reserved_tenants: HashMap::new(),
        };
        for mut session in snapshot.sessions {
            session.active_generation = None;
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

fn remove_session(state: &mut BrokerState, key: &SessionKey) {
    if let Some(active) = state.active.remove(key) {
        active.cancel.cancel()
    }
    if let Some(session) = state.sessions.remove(key) {
        for entry in session.inbound_qos2.values() {
            let message = match entry {
                InboundQos2State::AwaitPubrel(message)
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
    for (packet_id, state) in &session.outbound {
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
        session.outbound.insert(id, state);
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
            session.outbound.insert(id, outbound);
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
        session.outbound.remove(&id);
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
        && state
            .retained
            .len()
            .saturating_add(state.retained_reserved_count)
            >= limits.max_retained_messages)
        || (!replacing
            && tenant_count.saturating_add(tenant_reserved_count)
                >= limits.max_retained_messages_per_tenant)
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
    if topic.starts_with('$') && (filter == "#" || filter.starts_with("+/")) {
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
    #[test]
    fn wildcard_trie_and_dollar_rules() {
        assert!(topic_matches("sport/+/player1", "sport/team/player1"));
        assert!(topic_matches("sport/#", "sport"));
        assert!(topic_matches("sport/#", "sport/a/b"));
        assert!(!topic_matches("#", "$SYS/status"));
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

        before_pubcomp
            .mark_inbound_qos2_event_accepted(&resumed.key, resumed.generation, 77)
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
