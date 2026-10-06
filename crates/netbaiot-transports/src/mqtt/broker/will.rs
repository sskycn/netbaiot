//! will responsibilities under the single broker mutex.
use super::*;

pub(super) fn will_charge(message: &BrokerMessage, origin: Option<&SessionKey>) -> usize {
    message
        .bytes()
        .saturating_add(origin.map_or(0, |key| key.client_id.len().saturating_add(2)))
}

pub(super) fn all_pending_wills(state: &BrokerState) -> impl Iterator<Item = &PendingWill> {
    state
        .pending_wills
        .iter()
        .chain(state.future_wills.values().flat_map(|queue| queue.values()))
}

pub(super) fn pending_will_count(state: &BrokerState) -> usize {
    state.pending_wills.len()
        + state
            .future_wills
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
}

pub(super) fn insert_pending_will(state: &mut BrokerState, pending: PendingWill) {
    if let Some(deadline) = pending.due_at_ms {
        let bucket = state.future_wills.entry(deadline).or_default();
        // A token is derived runtime metadata, never part of the recovery format.
        let token = loop {
            state.next_will_token = state.next_will_token.wrapping_add(1);
            if !bucket.contains_key(&state.next_will_token) {
                break state.next_will_token;
            }
        };
        if let Some((owner, _)) = &pending.cancel_on_resume {
            state
                .future_wills_by_session
                .entry(owner.clone())
                .or_default()
                .insert((deadline, token));
        }
        bucket.insert(token, pending);
    } else {
        state.pending_wills.push_back(pending);
    }
}

pub(super) fn take_owned_future_wills(
    state: &mut BrokerState,
    key: &SessionKey,
) -> Vec<PendingWill> {
    let mut owned = Vec::new();
    let Some(locations) = state.future_wills_by_session.remove(key) else {
        return owned;
    };
    for (deadline, token) in locations {
        if let Some(bucket) = state.future_wills.get_mut(&deadline) {
            if let Some(pending) = bucket.remove(&token) {
                owned.push(pending);
            }
            if bucket.is_empty() {
                state.future_wills.remove(&deadline);
            }
        }
    }
    owned
}

pub(super) fn release_clean_start_delays(state: &mut BrokerState, key: &SessionKey) {
    for mut pending in take_owned_future_wills(state, key) {
        pending.due_at_ms = None;
        pending.cancel_on_resume = None;
        state.pending_wills.push_back(pending);
    }
}

pub(super) fn cancel_resumed_wills(state: &mut BrokerState, key: &SessionKey, incarnation: u64) {
    let now = now_ms();
    for mut will in take_owned_future_wills(state, key) {
        let matches = will
            .cancel_on_resume
            .as_ref()
            .is_some_and(|(owner, prior)| owner == key && *prior == incarnation);
        if !matches {
            insert_pending_will(state, will);
        } else if will.due_at_ms.is_some_and(|deadline| deadline <= now) {
            // A due Will was already eligible before the resume. Keep its broker-owned
            // responsibility even when a bounded maintenance pass has not reached it yet.
            will.due_at_ms = None;
            will.cancel_on_resume = None;
            state.pending_wills.push_back(will);
        } else {
            release_retained_reservation(state, &will.owner.tenant_id, will.retained_reservation);
            release_will_capacity(state, &will.owner.tenant_id, will.bytes());
        }
    }
}

pub(super) fn promote_due_wills(state: &mut BrokerState, now: i64, mut budget: usize) {
    while budget > 0 {
        let Some((&deadline, _)) = state.future_wills.first_key_value() else {
            break;
        };
        if deadline > now {
            break;
        }
        let Some(mut bucket) = state.future_wills.remove(&deadline) else {
            break;
        };
        while budget > 0 {
            let Some((token, mut pending)) = bucket.pop_first() else {
                break;
            };
            if let Some((owner, _)) = &pending.cancel_on_resume
                && let Some(locations) = state.future_wills_by_session.get_mut(owner)
            {
                locations.remove(&(deadline, token));
                if locations.is_empty() {
                    state.future_wills_by_session.remove(owner);
                }
            }
            pending.due_at_ms = None;
            pending.cancel_on_resume = None;
            state.pending_wills.push_back(pending);
            budget -= 1;
        }
        if !bucket.is_empty() {
            state.future_wills.insert(deadline, bucket);
        }
    }
}

pub(super) fn retry_pending_wills(state: &mut BrokerState, limits: &Limits) -> usize {
    retry_pending_wills_bounded(state, limits, HOT_MAINTENANCE_BUDGET)
}

pub(super) fn retry_pending_wills_bounded(
    state: &mut BrokerState,
    limits: &Limits,
    budget: usize,
) -> usize {
    promote_due_wills(state, now_ms(), budget);
    let attempts = state.pending_wills.len().min(budget);
    let mut settled = 0usize;
    for _ in 0..attempts {
        let Some(mut pending) = state.pending_wills.pop_front() else {
            break;
        };
        if pending.due_at_ms.is_some_and(|due| due > now_ms()) {
            insert_pending_will(state, pending);
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
        match route_locked(
            state,
            &pending.owner,
            pending.origin.as_ref(),
            &pending.message,
            limits,
        ) {
            Ok(_) => {
                release_will_capacity(state, &pending.owner.tenant_id, pending.bytes());
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

pub(super) fn reserve_will_capacity(
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

pub(super) fn release_will_capacity(state: &mut BrokerState, tenant: &TenantId, bytes: usize) {
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

impl WillGuard {
    pub fn arm(&mut self) {
        self.armed = true;
    }

    pub(in crate::mqtt) fn arm_v5(
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

    pub(super) fn settle(&mut self) -> Result<Option<BrokerMessage>> {
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
                    origin: self.origin.clone(),
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
            self.origin.as_ref(),
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
                will_charge(&self.message, self.origin.as_ref()),
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
                will_charge(&self.message, self.origin.as_ref()),
                self.reservation,
            )
        };
        if let Err(error) = result {
            tracing::error!(%error, "failed to settle accepted MQTT Will responsibility");
        }
        self.finished = true;
    }
}

impl MqttBroker {
    pub fn reserve_will(
        self: &Arc<Self>,
        owner: DeviceKey,
        message: BrokerMessage,
    ) -> Result<WillGuard> {
        self.reserve_will_with_origin(owner, None, message)
    }

    pub fn reserve_will_for_session(
        self: &Arc<Self>,
        origin: SessionKey,
        message: BrokerMessage,
    ) -> Result<WillGuard> {
        self.reserve_will_with_origin(origin.device.clone(), Some(origin), message)
    }

    pub(super) fn reserve_will_with_origin(
        self: &Arc<Self>,
        owner: DeviceKey,
        origin: Option<SessionKey>,
        message: BrokerMessage,
    ) -> Result<WillGuard> {
        if !valid_broker_message(&message, &self.limits)
            || message.payload.len() > self.limits.max_will_payload_bytes
            || origin.as_ref().is_some_and(|key| {
                key.device != owner || key.client_id.len() > self.limits.max_client_id_bytes
            })
        {
            return Err(Error::Invalid);
        }
        let charge = will_charge(&message, origin.as_ref());
        let mut state = lock(&self.state)?;
        reserve_will_capacity(&mut state, &owner.tenant_id, charge, &self.limits)?;
        let reservation = if message.retain && !message.payload.is_empty() {
            match reserve_retained(
                &mut state,
                &owner.tenant_id,
                origin.as_ref(),
                &message,
                &self.limits,
            ) {
                Ok(reservation) => reservation,
                Err(error) => {
                    release_will_capacity(&mut state, &owner.tenant_id, charge);
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
            origin,
            message,
            reservation,
            delay: None,
            message_expiry_interval: None,
            armed: false,
            finished: false,
        })
    }

    pub(super) fn release_will_reservation(
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

    pub(super) fn publish_reserved_will(
        &self,
        owner: &DeviceKey,
        origin: Option<&SessionKey>,
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
        match route_locked(&mut state, owner, origin, message, &self.limits) {
            Ok(delivered) => {
                release_will_capacity(&mut state, &owner.tenant_id, will_charge(message, origin));
                Ok(Some(delivered))
            }
            Err(error) => {
                // CONNECT already transferred Will ownership to the broker. Preserve it under the
                // capacity reserved at CONNECT and retry only when another broker operation frees
                // resources; never create a task or spin.
                add_retained_reservation(&mut state, &owner.tenant_id, reservation);
                state.pending_wills.push_back(PendingWill {
                    owner: owner.clone(),
                    origin: origin.cloned(),
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

    pub(super) fn schedule_reserved_will(
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
                release_will_capacity(&mut state, &pending.owner.tenant_id, pending.bytes());
                return Ok(WillSchedule::Suppressed);
            }
        }
        // The CONNECT reservation already owns the global and tenant capacity for this entry.
        insert_pending_will(&mut state, pending);
        Ok(WillSchedule::Delayed)
    }

    pub fn pending_will_count(&self) -> Result<usize> {
        let state = lock(&self.state)?;
        Ok(pending_will_count(&state))
    }
}
