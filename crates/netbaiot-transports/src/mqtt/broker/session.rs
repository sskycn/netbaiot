//! session responsibilities under the single broker mutex.
use super::*;

pub(super) fn check_owner(state: &BrokerState, key: &SessionKey, generation: u64) -> Result<()> {
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

pub(super) fn remove_session(state: &mut BrokerState, key: &SessionKey) -> Result<()> {
    unmark_pending(state, key);
    if let Some(active) = state.active.remove(key) {
        active.cancel.cancel()
    }
    if let Some(session) = state.sessions.remove(key) {
        for progress in session.command_progress.values() {
            progress.abandon_unsent();
        }
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

impl StoredSession {
    pub(super) fn new(
        key: SessionKey,
        incarnation: u64,
        authorization: SessionAuthorization,
    ) -> Self {
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
            command_outbound: HashSet::new(),
            command_progress: HashMap::new(),
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

    pub(super) fn allocate_packet_id(&mut self) -> Result<u16> {
        for _ in 0..u16::MAX {
            let id = self.next_packet_id;
            self.next_packet_id = if id == u16::MAX { 1 } else { id + 1 };
            if !self.outbound.contains_key(&id) {
                return Ok(id);
            }
        }
        Err(Error::Overloaded)
    }

    pub(super) fn has_outbound_capacity(&self, qos: u8, limits: &Limits) -> bool {
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

    pub(super) fn has_send_quota(&self) -> bool {
        self.send_window.len() < usize::from(self.send_quota)
    }

    pub(super) fn insert_outbound(&mut self, packet_id: u16, state: OutboundState) {
        if !self.outbound.contains_key(&packet_id) {
            self.outbound_order.push_back(packet_id);
        }
        self.outbound.insert(packet_id, state);
    }

    pub(super) fn remove_outbound(&mut self, packet_id: u16) -> Option<OutboundState> {
        self.command_outbound.remove(&packet_id);
        if let Some(progress) = self.command_progress.remove(&packet_id) {
            progress.abandon_unsent();
        }
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

impl MqttBroker {
    pub fn attach(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        client_id: String,
        clean_session: bool,
    ) -> Result<Attachment> {
        self.attach_profile(
            auth,
            Some(client_id),
            clean_session,
            MqttVersion::V311,
            0,
            u16::MAX,
            None,
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
            Some(client_id),
            clean_start,
            MqttVersion::V5,
            session_expiry_interval,
            receive_maximum,
            None,
        )
    }

    pub fn attach_generated(self: &Arc<Self>, auth: &AuthenticatedDevice) -> Result<Attachment> {
        self.attach_profile(auth, None, true, MqttVersion::V311, 0, u16::MAX, None)
    }

    pub fn attach_generated_v5(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        session_expiry_interval: u32,
        receive_maximum: u16,
        preflight: &dyn Fn(&str) -> Result<()>,
    ) -> Result<Attachment> {
        self.attach_profile(
            auth,
            None,
            true,
            MqttVersion::V5,
            session_expiry_interval,
            receive_maximum,
            Some(preflight),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn attach_profile(
        self: &Arc<Self>,
        auth: &AuthenticatedDevice,
        client_id: Option<String>,
        clean_session: bool,
        version: MqttVersion,
        session_expiry_interval: u32,
        receive_maximum: u16,
        preflight: Option<&ClientIdPreflight<'_>>,
    ) -> Result<Attachment> {
        if receive_maximum == 0 {
            return Err(Error::Invalid);
        }
        let mut state = lock(&self.state)?;
        self.prune_expired(&mut state, HOT_MAINTENANCE_BUDGET)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        let client_id = if let Some(client_id) = client_id {
            client_id
        } else {
            (1..=1_024_u64)
                .filter_map(|offset| {
                    let candidate = format!("generated-{}", state.generation.wrapping_add(offset));
                    (candidate.len() <= self.limits.max_client_id_bytes
                        && !state.sessions.keys().any(|key| key.client_id == candidate))
                    .then_some(candidate)
                })
                .next()
                .ok_or(Error::Overloaded)?
        };
        if let Some(preflight) = preflight {
            preflight(&client_id)?;
        }
        let key = SessionKey {
            device: auth.device_key.clone(),
            client_id,
        };
        if session_expiry_deadline(&state, &key).is_some_and(|deadline| deadline <= now_ms()) {
            remove_session(&mut state, &key)?;
        }
        if message_expiry_due(&state, &key, now_ms()) {
            prune_expired_messages_for_session(&mut state, &key, now_ms())?;
        }
        let authorization = SessionAuthorization::from(auth);
        if clean_session {
            release_clean_start_delays(&mut state, &key);
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
        cancel_resumed_wills(&mut state, &key, incarnation);
        if let Some(old) = state.active.remove(&key) {
            old.cancel.cancel();
        }
        state.generation = state.generation.wrapping_add(1).max(1);
        let generation = state.generation;
        let capacity = self.limits.max_outbound_messages_per_connection;
        let (sender, receiver) = mpsc::channel(capacity);
        let cancel = CancellationToken::new();
        state
            .tenant_outbound_bytes
            .retain(|_, budget| budget.upgrade().is_some());
        let tenant_bytes = state
            .tenant_outbound_bytes
            .entry(key.device.tenant_id.clone())
            .or_default()
            .get_or_create(self.limits.max_outbound_bytes_per_tenant);
        let active = ActiveSession {
            generation,
            sender: sender.clone(),
            cancel: cancel.clone(),
            connection_bytes: ByteBudget::new(self.limits.max_outbound_bytes_per_connection),
            tenant_bytes,
            global_bytes: self.global_outbound_bytes.clone(),
        };
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
            resume_frames(
                session,
                &active,
                &self.limits,
                available_qos1,
                available_qos2,
            )
        };
        let (resumed, resumed_count, resumed_bytes) = match resumed {
            Ok(resumed) => resumed,
            Err(error) => {
                if clean_session {
                    remove_session(&mut state, &key)?;
                } else if let Some(session) = state.sessions.get_mut(&key) {
                    session.active_generation = None;
                }
                sync_session_usage(&mut state, &key)?;
                drive_capacity_wakes(&mut state, &self.limits)?;
                return Err(error);
            }
        };
        // Publish the active generation only after its recovery frames are queued.
        // Concurrent routes hold the same broker lock and cannot overtake replay.
        for frame in resumed.into_iter().take(capacity) {
            sender.try_send(frame).map_err(|_| Error::Internal)?;
        }
        #[cfg(test)]
        if let Some(hook) = self
            .replay_hook
            .lock()
            .map_err(|_| Error::Internal)?
            .clone()
        {
            hook();
        }
        state.active.insert(key.clone(), active);
        state.offline_count = state.offline_count.saturating_sub(resumed_count);
        state.offline_bytes = state.offline_bytes.saturating_sub(resumed_bytes);
        sync_session_usage(&mut state, &key)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        mark_pending(&mut state, &key);
        let session_incarnation = state.sessions.get(&key).ok_or(Error::Internal)?.incarnation;
        self.publish_subscription_count(&state);
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
        drive_capacity_wakes(&mut state, &self.limits)?;
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
}
