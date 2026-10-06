//! maintenance responsibilities under the single broker mutex.
use super::*;

impl MqttBroker {
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

    /// One bounded maintenance pass. The server owns a single periodic task for this broker.
    pub fn tick(&self) -> Result<()> {
        let mut state = lock(&self.state)?;
        self.prune_expired(&mut state, TICK_MAINTENANCE_BUDGET)?;
        prune_expired_messages(&mut state, now_ms(), TICK_MAINTENANCE_BUDGET)?;
        wake_global_byte_pending(&mut state, &self.limits)?;
        retry_pending_wills_bounded(&mut state, &self.limits, TICK_MAINTENANCE_BUDGET);
        self.publish_subscription_count(&state);
        Ok(())
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
            remove_session(&mut state, key)?;
        }
        drive_capacity_wakes(&mut state, &self.limits)?;
        retry_pending_wills(&mut state, &self.limits);
        self.publish_subscription_count(&state);
        Ok(keys.len())
    }

    /// Read-only diagnostics used by benchmarks and operational capacity probes.
    pub fn matching_subscription_count(&self, topic: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        Ok(state.trie.matching(topic).len())
    }

    pub fn matching_retained_count(&self, filter: &str) -> Result<usize> {
        let state = lock(&self.state)?;
        let now = now_ms();
        Ok(state
            .retained
            .values()
            .filter(|entry| {
                !entry.message.expired(now) && topic_matches(filter, &entry.message.topic)
            })
            .count())
    }

    pub fn has_retained_topic(&self, topic: &str) -> Result<bool> {
        Ok(lock(&self.state)?
            .retained
            .get(topic)
            .is_some_and(|entry| !entry.message.expired(now_ms())))
    }

    pub(super) fn check_new_session(
        &self,
        state: &BrokerState,
        key: &SessionKey,
        state_bytes: usize,
    ) -> Result<()> {
        let tenant = state
            .tenant_usage
            .get(&key.device.tenant_id)
            .map_or(0, |usage| usage.session_count);
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

    pub(super) fn publish_subscription_count(&self, state: &BrokerState) {
        self.subscription_count
            .store(state.subscription_count, Ordering::Release);
    }

    pub(super) fn prune_expired(&self, state: &mut BrokerState, budget: usize) -> Result<()> {
        let now = now_ms();
        for _ in 0..budget {
            let Some(key) = state.session_expiry.pop_due(now) else {
                break;
            };
            if session_expiry_deadline(state, &key).is_some_and(|deadline| deadline <= now) {
                remove_session(state, &key)?;
            } else {
                sync_session_usage(state, &key)?;
            }
        }
        Ok(())
    }
}
