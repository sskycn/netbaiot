//! state responsibilities under the single broker mutex.
use super::*;

pub(super) fn apply_usage_delta(value: &mut usize, before: usize, after: usize) -> Result<()> {
    *value = if after >= before {
        value.checked_add(after - before)
    } else {
        value.checked_sub(before - after)
    }
    .ok_or(Error::Internal)?;
    Ok(())
}

/// Reconcile one changed authoritative session while the broker mutex is held.
/// The cached per-session value makes all callers independent of unrelated sessions.
pub(super) fn sync_session_usage(state: &mut BrokerState, key: &SessionKey) -> Result<()> {
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
    if tenant_usage.qos1_inflight
        < state
            .tenant_usage
            .get(tenant)
            .map_or(0, |usage| usage.qos1_inflight)
    {
        state.capacity_wakes.insert((tenant.clone(), 1));
    }
    if tenant_usage.qos2_inflight
        < state
            .tenant_usage
            .get(tenant)
            .map_or(0, |usage| usage.qos2_inflight)
    {
        state.capacity_wakes.insert((tenant.clone(), 2));
    }
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

pub(super) fn tenant_session_bytes(state: &BrokerState, tenant: &TenantId) -> usize {
    state
        .tenant_usage
        .get(tenant)
        .map_or(0, |usage| usage.session_bytes)
}

pub(super) fn total_session_bytes(state: &BrokerState) -> usize {
    state
        .session_bytes
        .saturating_add(state.will_responsibility_bytes)
}

pub(super) fn tenant_total_session_bytes(state: &BrokerState, tenant: &TenantId) -> usize {
    tenant_session_bytes(state, tenant).saturating_add(
        state
            .will_responsibility_tenants
            .get(tenant)
            .map_or(0, |usage| usage.1),
    )
}

pub(super) fn tenant_inflight(state: &BrokerState, tenant: &TenantId, qos: u8) -> usize {
    state.tenant_usage.get(tenant).map_or(0, |usage| {
        if qos == 1 {
            usage.qos1_inflight
        } else {
            usage.qos2_inflight
        }
    })
}

impl SessionUsage {
    pub(super) fn from_session(session: &StoredSession) -> Self {
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
