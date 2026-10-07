//! outbound responsibilities under the single broker mutex.
use super::*;

pub(super) fn unsent_outbound_qos(session: &StoredSession) -> Option<u8> {
    session.outbound_order.iter().find_map(|id| {
        if session.sent.contains(id) {
            return None;
        }
        match session.outbound.get(id) {
            Some(OutboundState::AwaitPuback(_)) => Some(1),
            Some(OutboundState::AwaitPubrec(_) | OutboundState::AwaitPubcomp(_)) => Some(2),
            None => None,
        }
    })
}

pub(super) fn next_unsent_frame(
    state: &mut BrokerState,
    key: &SessionKey,
) -> Result<Option<BrokerFrame>> {
    let active = state.active.get(key).cloned().ok_or(Error::Unavailable)?;
    let session = state.sessions.get_mut(key).ok_or(Error::Unavailable)?;
    for id in &session.outbound_order {
        if session.sent.contains(id) {
            continue;
        }
        let Some(outbound) = session.outbound.get(id) else {
            continue;
        };
        if !matches!(outbound, OutboundState::AwaitPubcomp(_)) && !session.has_send_quota() {
            return Ok(None);
        }
        let frame = match outbound {
            OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message) => {
                let Ok(budget) = active.reserve_frame(message.bytes()) else {
                    return Ok(None);
                };
                BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message: message.clone(),
                    packet_id: Some(*id),
                    dup: true,
                    command: session.command_outbound.contains(id),
                    progress: session.command_progress.get(id).cloned(),
                    unsent_command: UnsentCommandGuard::default(),
                    _budget: budget,
                }))
            }
            OutboundState::AwaitPubcomp(_) => BrokerFrame::Pubrel {
                packet_id: *id,
                dup: true,
            },
        };
        session.sent.insert(*id);
        if !matches!(outbound, OutboundState::AwaitPubcomp(_)) {
            session.send_window.insert(*id);
        }
        return Ok(Some(frame));
    }
    Ok(None)
}

pub(super) fn mark_pending(state: &mut BrokerState, key: &SessionKey) {
    let qos = state
        .active
        .contains_key(key)
        .then(|| {
            state.sessions.get(key).and_then(|session| {
                unsent_outbound_qos(session)
                    .or_else(|| session.offline.front().map(|message| message.qos))
            })
        })
        .flatten();
    if state.pending_sessions.get(key).map(|(qos, _)| *qos) == qos {
        return;
    }
    unmark_pending(state, key);
    if let Some(qos) = qos {
        let token = loop {
            state.next_pending_token = state.next_pending_token.wrapping_add(1);
            if !state.pending_global.contains_key(&state.next_pending_token) {
                break state.next_pending_token;
            }
        };
        state
            .pending_by_tenant
            .entry((key.device.tenant_id.clone(), qos))
            .or_default()
            .insert(token, key.clone());
        state.pending_global.insert(token, key.clone());
        state.pending_sessions.insert(key.clone(), (qos, token));
    }
}

pub(super) fn unmark_pending(state: &mut BrokerState, key: &SessionKey) {
    let Some((qos, token)) = state.pending_sessions.remove(key) else {
        return;
    };
    state.pending_global.remove(&token);
    let queue_key = (key.device.tenant_id.clone(), qos);
    if let Some(queue) = state.pending_by_tenant.get_mut(&queue_key) {
        queue.remove(&token);
        if queue.is_empty() {
            state.pending_by_tenant.remove(&queue_key);
        }
    }
}

pub(super) fn promote_offline(
    state: &mut BrokerState,
    key: &SessionKey,
    limits: &Limits,
) -> Result<Option<BrokerFrame>> {
    let budget = {
        let Some(message) = state
            .sessions
            .get(key)
            .and_then(|session| session.offline.front())
        else {
            return Ok(None);
        };
        let active = state.active.get(key).ok_or(Error::Unavailable)?;
        let Ok(budget) = active.reserve_frame(message.bytes()) else {
            return Ok(None);
        };
        budget
    };
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
            BrokerFrame::Publish(Box::new(BrokerDelivery {
                message,
                packet_id: Some(id),
                dup: false,
                command: false,
                progress: None,
                unsent_command: UnsentCommandGuard::default(),
                _budget: budget,
            })),
            bytes,
        )
    };
    state.offline_count = state.offline_count.saturating_sub(1);
    state.offline_bytes = state.offline_bytes.saturating_sub(bytes);
    sync_session_usage(state, key)?;
    Ok(Some(frame))
}

pub(super) fn wake_tenant_pending(
    state: &mut BrokerState,
    tenant: &TenantId,
    qos: u8,
    limits: &Limits,
) -> Result<()> {
    let tenant_limit = if qos == 1 {
        limits.max_inflight_qos1_per_tenant
    } else {
        limits.max_inflight_qos2_per_tenant
    };
    if tenant_inflight(state, tenant, qos) >= tenant_limit {
        return Ok(());
    }
    let queue_key = (tenant.clone(), qos);
    let mut pending = state
        .pending_by_tenant
        .remove(&queue_key)
        .unwrap_or_default();
    let attempts = pending.len();
    for _ in 0..attempts {
        if tenant_inflight(state, tenant, qos) >= tenant_limit {
            break;
        }
        let Some((_, key)) = pending.pop_first() else {
            break;
        };
        if let Some((_, token)) = state.pending_sessions.remove(&key) {
            state.pending_global.remove(&token);
        }
        if message_expiry_due(state, &key, now_ms()) {
            prune_expired_messages_for_session(state, &key, now_ms())?;
        }
        let next_qos = state.sessions.get(&key).and_then(|session| {
            unsent_outbound_qos(session)
                .or_else(|| session.offline.front().map(|message| message.qos))
        });
        let sender = state.active.get(&key).map(|active| active.sender.clone());
        if next_qos != Some(qos)
            || sender.as_ref().is_none_or(|sender| sender.capacity() == 0)
            || tenant_inflight(state, tenant, qos) >= tenant_limit
        {
            mark_pending(state, &key);
            continue;
        }
        let frame = if state
            .sessions
            .get(&key)
            .and_then(unsent_outbound_qos)
            .is_some()
        {
            next_unsent_frame(state, &key)?
        } else {
            promote_offline(state, &key, limits)?
        };
        let Some(frame) = frame else {
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
    if !pending.is_empty() {
        if let Some(mut requeued) = state.pending_by_tenant.remove(&queue_key) {
            pending.append(&mut requeued);
        }
        state.pending_by_tenant.insert(queue_key, pending);
    }
    Ok(())
}

/// Release accounting is recorded by `sync_session_usage`. Drain only tenants
/// whose inflight count actually fell; promotions may themselves expire an
/// unsent exchange and enqueue another bounded wake without recursion.
pub(super) fn drive_capacity_wakes(state: &mut BrokerState, limits: &Limits) -> Result<()> {
    for _ in 0..HOT_MAINTENANCE_BUDGET {
        let Some((tenant, qos)) = state.capacity_wakes.pop_first() else {
            break;
        };
        if let Err(error) = wake_tenant_pending(state, &tenant, qos, limits) {
            state.capacity_wakes.insert((tenant, qos));
            return Err(error);
        }
    }
    Ok(())
}

/// A completed socket write releases active-frame byte permits. Retry a fixed
/// number of indexed pending sessions across tenants so a global-byte release
/// can advance a different tenant without waiting for another device packet.
pub(super) fn wake_global_byte_pending(state: &mut BrokerState, limits: &Limits) -> Result<()> {
    let attempts = state.pending_global.len().min(HOT_MAINTENANCE_BUDGET);
    for _ in 0..attempts {
        let Some((_, key)) = state.pending_global.pop_first() else {
            break;
        };
        unmark_pending(state, &key);
        if message_expiry_due(state, &key, now_ms()) {
            prune_expired_messages_for_session(state, &key, now_ms())?;
        }
        let qos = state.sessions.get(&key).and_then(|session| {
            unsent_outbound_qos(session)
                .or_else(|| session.offline.front().map(|message| message.qos))
        });
        let sender = state.active.get(&key).map(|active| active.sender.clone());
        if let Some(qos) = qos
            && sender.as_ref().is_some_and(|sender| sender.capacity() > 0)
            && (state
                .sessions
                .get(&key)
                .and_then(unsent_outbound_qos)
                .is_some()
                || tenant_inflight(state, &key.device.tenant_id, qos)
                    < if qos == 1 {
                        limits.max_inflight_qos1_per_tenant
                    } else {
                        limits.max_inflight_qos2_per_tenant
                    })
            && let Some(frame) = if state
                .sessions
                .get(&key)
                .and_then(unsent_outbound_qos)
                .is_some()
            {
                next_unsent_frame(state, &key)?
            } else {
                promote_offline(state, &key, limits)?
            }
            && sender.ok_or(Error::Internal)?.try_send(frame).is_err()
            && let Some(active) = state.active.get(&key)
        {
            active.cancel.cancel();
        }
        mark_pending(state, &key);
    }
    drive_capacity_wakes(state, limits)
}

pub(super) fn resume_frames(
    session: &mut StoredSession,
    active: &ActiveSession,
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
        let frame = match state {
            OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message) => {
                let Ok(budget) = active.reserve_frame(message.bytes()) else {
                    break;
                };
                BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message: message.clone(),
                    packet_id: Some(*packet_id),
                    dup: true,
                    command: session.command_outbound.contains(packet_id),
                    progress: session.command_progress.get(packet_id).cloned(),
                    unsent_command: UnsentCommandGuard::default(),
                    _budget: budget,
                }))
            }
            OutboundState::AwaitPubcomp(_) => BrokerFrame::Pubrel {
                packet_id: *packet_id,
                dup: true,
            },
        };
        frames.push(frame);
        session.sent.insert(*packet_id);
        if !matches!(state, OutboundState::AwaitPubcomp(_)) {
            session.send_window.insert(*packet_id);
        }
    }
    while frames.len() < limits.max_outbound_messages_per_connection {
        if !session.has_send_quota() {
            break;
        }
        let Some(message) = session.offline.front() else {
            break;
        };
        let Ok(budget) = active.reserve_frame(message.bytes()) else {
            break;
        };
        let message = session.offline.pop_front().ok_or(Error::Internal)?;
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
        frames.push(BrokerFrame::Publish(Box::new(BrokerDelivery {
            message,
            packet_id: Some(id),
            dup: false,
            command: false,
            progress: None,
            unsent_command: UnsentCommandGuard::default(),
            _budget: budget,
        })));
    }
    Ok((frames, resumed_count, resumed_bytes))
}

pub(super) fn enqueue(
    state: &mut BrokerState,
    key: &SessionKey,
    message: BrokerMessage,
    limits: &Limits,
    pre_budget: Option<Vec<BytesPermit>>,
    command: bool,
    progress: Option<Arc<netbaiot_runtime::CommandProgress>>,
) -> Result<()> {
    let now = now_ms();
    if message.expired(now) {
        if let Some(progress) = progress {
            progress.expire(now);
        }
        return Ok(());
    }
    let active = state.active.get(key).cloned();
    if active.is_none() {
        return queue_offline(state, key, message, limits);
    }
    let active = active.ok_or(Error::Internal)?;
    if message.bytes() > limits.max_outbound_bytes_per_connection {
        return Err(Error::Overloaded);
    }
    if active.sender.capacity() == 0 {
        return queue_offline(state, key, message, limits);
    }
    let budget = if let Some(budget) = pre_budget {
        budget
    } else {
        match active.reserve_frame(message.bytes()) {
            Ok(budget) => budget,
            Err(_) => return queue_offline(state, key, message, limits),
        }
    };
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
                BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message,
                    packet_id: None,
                    dup: false,
                    command,
                    unsent_command: UnsentCommandGuard(progress.clone()),
                    progress,
                    _budget: budget,
                })),
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
            if command {
                session.command_outbound.insert(id);
                if let Some(progress) = &progress {
                    session.command_progress.insert(id, progress.clone());
                }
            }
            session.sent.insert(id);
            session.send_window.insert(id);
            session.state_bytes += charge;
            state.session_bytes += charge;
            sync_session_usage(state, key)?;
            (
                BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message,
                    packet_id: Some(id),
                    dup: false,
                    command,
                    unsent_command: UnsentCommandGuard::default(),
                    progress,
                    _budget: budget,
                })),
                Some(id),
                charge,
            )
        }
    };
    let frame = match active.sender.try_send(frame) {
        Ok(()) => return Ok(()),
        Err(error) => error.into_inner(),
    };
    active.cancel.cancel();
    if packet_id.is_some() && !command {
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
    if command {
        return Err(Error::Unavailable);
    }
    match frame {
        BrokerFrame::Publish(delivery) if delivery.message.qos > 0 => {
            queue_offline(state, key, delivery.message, limits)
        }
        _ => Err(Error::Overloaded),
    }
}

pub(super) fn queue_offline(
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

impl ActiveSession {
    pub(super) fn reserve_frame(&self, bytes: usize) -> Result<Vec<BytesPermit>> {
        Ok(vec![
            self.connection_bytes.reserve(bytes)?,
            self.tenant_bytes.reserve(bytes)?,
            self.global_bytes.reserve(bytes)?,
        ])
    }
}

impl MqttBroker {
    pub fn send_live(
        &self,
        key: &SessionKey,
        generation: u64,
        message: BrokerMessage,
    ) -> Result<()> {
        self.send_live_tracked(key, generation, message, None)
    }

    pub fn send_live_tracked(
        &self,
        key: &SessionKey,
        generation: u64,
        message: BrokerMessage,
        progress: Option<Arc<netbaiot_runtime::CommandProgress>>,
    ) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Outbound)?;
        check_owner(&state, key, generation).map_err(|_| Error::Unavailable)?;
        let subscribed = state.sessions.get(key).is_some_and(|session| {
            session
                .subscriptions
                .keys()
                .any(|filter| topic_matches(filter, &message.topic))
        });
        let now = now_ms();
        if message.expired(now) {
            if let Some(progress) = progress {
                progress.expire(now);
                return Ok(());
            }
            return Err(Error::Unavailable);
        }
        if !subscribed {
            return Err(Error::Unavailable);
        }
        let active = state.active.get(key).ok_or(Error::Unavailable)?;
        if active.sender.capacity() == 0
            || message.bytes() > self.limits.max_outbound_bytes_per_connection
        {
            return Err(Error::Overloaded);
        }
        let budget = active.reserve_frame(message.bytes())?;
        if message.qos > 0 {
            let tenant_limit = if message.qos == 1 {
                self.limits.max_inflight_qos1_per_tenant
            } else {
                self.limits.max_inflight_qos2_per_tenant
            };
            let session = state.sessions.get(key).ok_or(Error::Internal)?;
            if tenant_inflight(&state, &key.device.tenant_id, message.qos) >= tenant_limit
                || !session.has_outbound_capacity(message.qos, &self.limits)
                || !session.has_send_quota()
            {
                return Err(Error::Overloaded);
            }
        }
        enqueue(
            &mut state,
            key,
            message,
            &self.limits,
            Some(budget),
            true,
            progress,
        )
    }

    pub fn next_offline(&self, key: &SessionKey, generation: u64) -> Result<Option<BrokerFrame>> {
        let mut state = self.lock_state(BrokerProbe::Outbound)?;
        check_owner(&state, key, generation)?;
        if message_expiry_due(&state, key, now_ms()) {
            prune_expired_messages_for_session(&mut state, key, now_ms())?;
            drive_capacity_wakes(&mut state, &self.limits)?;
        }
        if let Some(frame) = next_unsent_frame(&mut state, key)? {
            mark_pending(&mut state, key);
            return Ok(Some(frame));
        }
        if state
            .sessions
            .get(key)
            .and_then(unsent_outbound_qos)
            .is_some()
        {
            mark_pending(&mut state, key);
            return Ok(None);
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

    pub fn outbound_bytes_released(&self) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Outbound)?;
        wake_global_byte_pending(&mut state, &self.limits)
    }

    pub fn puback(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<bool> {
        self.puback_result(key, generation, packet_id, true)
    }

    pub fn puback_result(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        success: bool,
    ) -> Result<bool> {
        self.complete_outbound(key, generation, packet_id, OutboundAck::Puback, success)
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
        let mut state = self.lock_state(BrokerProbe::Outbound)?;
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
        if !session.started_outbound.contains(&packet_id)
            && (delivery.message.expired(now_ms())
                || delivery
                    .progress
                    .as_ref()
                    .is_some_and(|progress| !progress.begin_transfer()))
        {
            let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
            let charge = outbound.bytes();
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
            sync_session_usage(&mut state, key)?;
            drive_capacity_wakes(&mut state, &self.limits)?;
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
        let mut state = self.lock_state(BrokerProbe::Outbound)?;
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
        drive_capacity_wakes(&mut state, &self.limits)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(true)
    }

    pub fn pubrec(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<BrokerFrame> {
        let mut state = self.lock_state(BrokerProbe::Pubrec)?;
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

    pub fn pubrec_rejected(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<bool> {
        let mut state = self.lock_state(BrokerProbe::Pubrec)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        match session.outbound.get(&packet_id) {
            Some(OutboundState::AwaitPubrec(_)) => {}
            Some(_) => return Err(Error::Conflict),
            None => return Err(Error::Invalid),
        }
        if let Some(progress) = session.command_progress.get(&packet_id) {
            progress.update(netbaiot_core::DeliveryState::Failed);
        }
        let command = session.command_outbound.contains(&packet_id);
        let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(&mut state, key)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        Ok(command)
    }

    pub fn pubcomp(&self, key: &SessionKey, generation: u64, packet_id: u16) -> Result<bool> {
        self.pubcomp_result(key, generation, packet_id, true)
    }

    pub fn pubcomp_result(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        success: bool,
    ) -> Result<bool> {
        self.complete_outbound(key, generation, packet_id, OutboundAck::Pubcomp, success)
    }

    pub(super) fn complete_outbound(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        ack: OutboundAck,
        success: bool,
    ) -> Result<bool> {
        let mut state = self.lock_state(match ack {
            OutboundAck::Puback => BrokerProbe::Puback,
            OutboundAck::Pubcomp => BrokerProbe::Pubcomp,
        })?;
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
        if let Some(progress) = session.command_progress.get(&packet_id) {
            progress.update(if success {
                netbaiot_core::DeliveryState::Received
            } else {
                netbaiot_core::DeliveryState::Failed
            });
        }
        let command = session.command_outbound.contains(&packet_id);
        let outbound = session.remove_outbound(packet_id).ok_or(Error::Internal)?;
        let charge = outbound.bytes();
        session.state_bytes = session.state_bytes.saturating_sub(charge);
        state.session_bytes = state.session_bytes.saturating_sub(charge);
        sync_session_usage(&mut state, key)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(command)
    }
}
