//! expiry responsibilities under the single broker mutex.
use super::*;

pub(super) fn next_message_expiry(session: &StoredSession) -> Option<i64> {
    session
        .offline
        .iter()
        .filter_map(|message| message.properties.expires_at_ms)
        .chain(
            session
                .outbound
                .iter()
                .filter_map(|(id, outbound)| match outbound {
                    OutboundState::AwaitPuback(message) | OutboundState::AwaitPubrec(message) => {
                        // Most publishes have no expiry. Only consult transfer
                        // ownership when a deadline actually needs exclusion.
                        message
                            .properties
                            .expires_at_ms
                            .filter(|_| !session.started_outbound.contains(id))
                    }
                    OutboundState::AwaitPubcomp(message) => message.properties.expires_at_ms,
                }),
        )
        .min()
}

pub(super) fn session_expiry_deadline(state: &BrokerState, key: &SessionKey) -> Option<i64> {
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

pub(super) fn message_expiry_due(state: &BrokerState, key: &SessionKey, now: i64) -> bool {
    state
        .message_expiry
        .by_key
        .get(key)
        .is_some_and(|&deadline| deadline <= now)
}

pub(super) fn prune_expired_messages(
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
        let removed = state.retained.remove(&topic).ok_or(Error::Internal)?;
        retained_usage_remove(state, &removed)?;
        state.retained_bytes = remaining;
    }
    Ok(released_tenants)
}

/// Expiry work for one session. ACK-driven promotion calls this without scanning
/// unrelated persistent sessions.
pub(super) fn prune_expired_messages_for_session(
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
pub(super) fn prune_session_messages(
    session: &mut StoredSession,
    now: i64,
) -> (usize, usize, usize, bool) {
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

impl<K: Clone + Eq + Hash> DeadlineIndex<K> {
    pub(super) fn has_due(&self, now: i64) -> bool {
        self.by_deadline
            .first_key_value()
            .is_some_and(|(&deadline, _)| deadline <= now)
    }

    pub(super) fn update(&mut self, key: K, deadline: Option<i64>) {
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

    pub(super) fn pop_due(&mut self, now: i64) -> Option<K> {
        let (&deadline, bucket) = self.by_deadline.first_key_value()?;
        if deadline > now {
            return None;
        }
        let key = bucket.iter().next()?.clone();
        self.update(key.clone(), None);
        Some(key)
    }
}
