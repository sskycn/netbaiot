//! retained responsibilities under the single broker mutex.
use super::*;

pub(super) fn retained_charge(message: &BrokerMessage, origin: Option<&SessionKey>) -> usize {
    message.bytes().saturating_add(origin.map_or(0, |key| {
        key.client_id.len()
            + key.device.tenant_id.as_str().len()
            + key.device.product_id.as_str().len()
            + key.device.device_id.as_str().len()
            + STATE_OVERHEAD
    }))
}

pub(super) fn preflight_retained_replay(
    state: &BrokerState,
    key: &SessionKey,
    messages: &[BrokerMessage],
    subscription_charge: usize,
    limits: &Limits,
) -> Result<Vec<RetainedReplayAdmission>> {
    let session = state.sessions.get(key).ok_or(Error::Internal)?;
    let session_state_bytes = session
        .state_bytes
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    let mut tenant_state_bytes = tenant_total_session_bytes(state, &key.device.tenant_id)
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    let mut global_state_bytes = total_session_bytes(state)
        .checked_add(subscription_charge)
        .ok_or(Error::Overloaded)?;
    if session_state_bytes > limits.max_mqtt_session_state_bytes
        || tenant_state_bytes > limits.max_mqtt_session_state_bytes_per_tenant
        || global_state_bytes > limits.global_mqtt_session_bytes
    {
        return Err(Error::Overloaded);
    }
    if messages.is_empty() {
        return Ok(Vec::new());
    }
    let active = state.active.get(key).ok_or(Error::Conflict)?;
    let mut session = session.clone();
    session.state_bytes = session_state_bytes;
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
    let mut admissions = Vec::with_capacity(messages.len());
    for message in messages {
        if message.qos == 0 {
            if live_frames >= active.sender.capacity() {
                return Err(Error::Overloaded);
            }
            admissions.push(RetainedReplayAdmission::Live(
                active.reserve_frame(message.bytes())?,
            ));
            live_frames = live_frames.checked_add(1).ok_or(Error::Overloaded)?;
            continue;
        }
        if message.bytes() > limits.max_outbound_bytes_per_connection {
            return Err(Error::Overloaded);
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
        let can_live = current_tenant_inflight < tenant_limit
            && live_frames < active.sender.capacity()
            && session.has_outbound_capacity(message.qos, limits)
            && session.has_send_quota();
        let live_budget = can_live
            .then(|| active.reserve_frame(message.bytes()).ok())
            .flatten();
        let use_offline = live_budget.is_none();
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
            admissions.push(RetainedReplayAdmission::Offline);
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
            admissions.push(RetainedReplayAdmission::Live(
                live_budget.ok_or(Error::Internal)?,
            ));
        }
        session.state_bytes += charge;
        tenant_state_bytes += charge;
        global_state_bytes += charge;
    }
    Ok(admissions)
}

pub(super) fn retained_usage_add(
    state: &mut BrokerState,
    retained: &RetainedMessage,
) -> Result<()> {
    let current = state
        .retained_tenant_usage
        .get(&retained.tenant_id)
        .copied()
        .unwrap_or_default();
    let next = (
        current.0.checked_add(1).ok_or(Error::Overloaded)?,
        current
            .1
            .checked_add(retained.bytes())
            .ok_or(Error::Overloaded)?,
    );
    state
        .retained_tenant_usage
        .insert(retained.tenant_id.clone(), next);
    Ok(())
}

pub(super) fn retained_usage_remove(
    state: &mut BrokerState,
    retained: &RetainedMessage,
) -> Result<()> {
    let usage = state
        .retained_tenant_usage
        .get_mut(&retained.tenant_id)
        .ok_or(Error::Internal)?;
    usage.0 = usage.0.checked_sub(1).ok_or(Error::Internal)?;
    usage.1 = usage
        .1
        .checked_sub(retained.bytes())
        .ok_or(Error::Internal)?;
    if usage.0 == 0 {
        state.retained_tenant_usage.remove(&retained.tenant_id);
    }
    Ok(())
}

pub(super) fn check_retained_update(
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
    let (tenant_count, tenant_bytes) = state
        .retained_tenant_usage
        .get(&owner.tenant_id)
        .copied()
        .unwrap_or_default();
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

pub(super) fn update_retained(
    state: &mut BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<()> {
    if message.payload.is_empty() {
        if let Some(old) = state.retained.remove(&message.topic) {
            state.retained_bytes = state.retained_bytes.saturating_sub(old.bytes());
            retained_usage_remove(state, &old)?;
        }
        state.retained_expiry.update(message.topic.clone(), None);
        return Ok(());
    }
    check_retained_update(state, owner, origin, message, limits)?;
    let old = state.retained.remove(&message.topic);
    let old_bytes = old.as_ref().map_or(0, RetainedMessage::bytes);
    if let Some(old) = &old {
        retained_usage_remove(state, old)?;
    }
    let new_bytes = retained_charge(message, origin);
    state.retained_bytes = state
        .retained_bytes
        .saturating_sub(old_bytes)
        .saturating_add(new_bytes);
    let retained = RetainedMessage {
        tenant_id: owner.tenant_id.clone(),
        message: message.clone(),
        origin: origin.cloned(),
    };
    retained_usage_add(state, &retained)?;
    state.retained.insert(message.topic.clone(), retained);
    state
        .retained_expiry
        .update(message.topic.clone(), message.properties.expires_at_ms);
    Ok(())
}

pub(super) fn reserve_retained(
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
    // A PUBREC (or accepted Will) promises a future retained update. The current
    // value may expire, be deleted, or be replaced before that update commits, so
    // it cannot serve as credit for the promise. Reserve the full future slot and
    // byte charge until the transaction is settled.
    let reservation = RetainedReservation {
        global_count: 1,
        global_bytes: bytes,
        tenant_count: 1,
        tenant_bytes: bytes,
    };
    let (tenant_count, tenant_bytes) = state
        .retained_tenant_usage
        .get(tenant)
        .copied()
        .unwrap_or_default();
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

pub(super) fn add_retained_reservation(
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

pub(super) fn release_retained_reservation(
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
