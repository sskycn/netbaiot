//! subscription responsibilities under the single broker mutex.
use super::*;

pub(super) fn merge_subscribers(
    output: &mut HashMap<SessionKey, Subscription>,
    subscribers: &HashMap<SessionKey, Subscription>,
) {
    for (key, subscription) in subscribers {
        output
            .entry(key.clone())
            .and_modify(|existing| {
                existing.qos = existing.qos.max(subscription.qos);
                existing.no_local &= subscription.no_local;
                existing.retain_as_published |= subscription.retain_as_published;
            })
            .or_insert(*subscription);
    }
}

impl SubscriptionTrie {
    pub(super) fn insert(&mut self, filter: &str, key: SessionKey, subscription: Subscription) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            node = node.children.entry(level.to_owned()).or_default();
        }
        node.subscribers.insert(key, subscription);
    }

    pub(super) fn remove(&mut self, filter: &str, key: &SessionKey) {
        let levels = filter.split('/').collect::<Vec<_>>();
        Self::remove_at(&mut self.root, &levels, 0, key);
    }

    pub(super) fn remove_at(
        node: &mut TrieNode,
        levels: &[&str],
        at: usize,
        key: &SessionKey,
    ) -> bool {
        if at == levels.len() {
            node.subscribers.remove(key);
        } else if let Some(child) = node.children.get_mut(levels[at])
            && Self::remove_at(child, levels, at + 1, key)
        {
            node.children.remove(levels[at]);
        }
        node.subscribers.is_empty() && node.children.is_empty()
    }

    pub(super) fn matching(&self, topic: &str) -> HashMap<SessionKey, Subscription> {
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

    pub(super) fn match_at(
        node: &TrieNode,
        levels: &[&str],
        at: usize,
        dollar_root: bool,
        output: &mut HashMap<SessionKey, Subscription>,
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

impl MqttBroker {
    pub fn subscribe(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        qos: u8,
    ) -> Result<u8> {
        self.subscribe_options(key, generation, filter, Subscription::v311(qos))
    }

    pub fn subscribe_v5(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        options: v5::SubscriptionOptions,
    ) -> Result<u8> {
        self.subscribe_options(
            key,
            generation,
            filter,
            Subscription {
                qos: options.qos,
                no_local: options.no_local,
                retain_as_published: options.retain_as_published,
                retain_handling: options.retain_handling,
            },
        )
    }

    pub(super) fn subscribe_options(
        &self,
        key: &SessionKey,
        generation: u64,
        filter: &str,
        subscription: Subscription,
    ) -> Result<u8> {
        if subscription.qos > 2
            || subscription.retain_handling > 2
            || !valid_topic(filter, &self.limits, true)
        {
            return Err(Error::Invalid);
        }
        let mut state = self.lock_state(BrokerProbe::Subscribe)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        check_owner(&state, key, generation)?;
        if message_expiry_due(&state, key, now_ms()) {
            prune_expired_messages_for_session(&mut state, key, now_ms())?;
        }
        let replacement = state
            .sessions
            .get(key)
            .is_some_and(|session| session.subscriptions.contains_key(filter));
        if !state.retained.is_empty() {
            state.classify(BrokerProbe::RetainedReplay);
        }
        let now = now_ms();
        let retained_allowed = |retained: &RetainedMessage| {
            !(retained.message.expired(now)
                || (subscription.no_local && retained.origin.as_ref() == Some(key)))
                && match subscription.retain_handling {
                    0 => true,
                    1 => !replacement,
                    _ => false,
                }
        };
        let replay_message = |retained: &RetainedMessage| BrokerMessage {
            qos: retained.message.qos.min(subscription.qos),
            retain: true,
            ..retained.message.clone()
        };
        let retained = if filter.contains(['+', '#']) {
            state
                .retained
                .values()
                .filter(|retained| {
                    topic_matches(filter, &retained.message.topic) && retained_allowed(retained)
                })
                .map(replay_message)
                .collect::<Vec<_>>()
        } else {
            state
                .retained
                .get(filter)
                .filter(|retained| retained_allowed(retained))
                .map(replay_message)
                .into_iter()
                .collect::<Vec<_>>()
        };
        if !replacement {
            let tenant_count = state
                .tenant_usage
                .get(&key.device.tenant_id)
                .map_or(0, |usage| usage.subscription_count);
            let device_count = state
                .device_subscription_count
                .get(&key.device)
                .copied()
                .unwrap_or_default();
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
        let replay_plan =
            preflight_retained_replay(&state, key, &retained, subscription_charge, &self.limits)?;
        // Retained replay can fail after the subscription mutation and needs a full rollback.
        // An empty replay cannot enter that failure path, so avoid copying the entire target
        // Session (including its bounded offline queue and inflight payloads) for it.
        let before_session = if retained.is_empty() {
            None
        } else {
            Some(state.sessions.get(key).cloned().ok_or(Error::Internal)?)
        };
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
            .insert(filter.to_owned(), subscription);
        state.trie.insert(filter, key.clone(), subscription);
        sync_session_usage(&mut state, key)?;
        for (message, admission) in retained.into_iter().zip(replay_plan) {
            let result = match admission {
                RetainedReplayAdmission::Live(budget) => enqueue(
                    &mut state,
                    key,
                    message,
                    &self.limits,
                    Some(budget),
                    false,
                    None,
                ),
                RetainedReplayAdmission::Offline => {
                    queue_offline(&mut state, key, message, &self.limits)
                }
            };
            if let Err(error) = result {
                let before_session = before_session.as_ref().ok_or(Error::Internal)?;
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
                sync_session_usage(&mut state, key)?;
                return Err(error);
            }
        }
        self.publish_subscription_count(&state);
        Ok(subscription.qos)
    }

    pub fn unsubscribe(&self, key: &SessionKey, generation: u64, filter: &str) -> Result<bool> {
        let mut state = self.lock_state(BrokerProbe::Unsubscribe)?;
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
            sync_session_usage(&mut state, key)?;
            retry_pending_wills(&mut state, &self.limits);
            self.publish_subscription_count(&state);
        }
        Ok(removed.is_some())
    }

    pub fn subscription_qos(&self, key: &SessionKey, topic: &str) -> Result<Option<u8>> {
        let state = self.lock_state(BrokerProbe::Read)?;
        Ok(state.sessions.get(key).and_then(|session| {
            session
                .subscriptions
                .iter()
                .filter(|(filter, _)| topic_matches(filter, topic))
                .map(|(_, subscription)| subscription.qos)
                .max()
        }))
    }
}
