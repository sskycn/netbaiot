//! routing responsibilities under the single broker mutex.
use super::*;

pub(super) fn route_locked(
    state: &mut BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
) -> Result<usize> {
    let now = now_ms();
    if message.expired(now) {
        return Ok(0);
    }
    if state.message_expiry.has_due(now) {
        for key in state.trie.matching(&message.topic).into_keys() {
            prune_expired_messages_for_session(state, &key, now)?;
        }
    }
    // QoS and retain differ per delivery, but neither changes the logical charge.
    let charge = message.bytes();
    let plan = preflight_route(state, owner, origin, message, limits, charge)?;
    if message.retain {
        update_retained(state, owner, origin, message, limits)?;
    }
    let mut delivered = 0usize;
    for target in plan.targets {
        let routed = BrokerMessage {
            qos: target.qos,
            retain: target.retain_as_published && message.retain,
            ..message.clone()
        };
        match target.mode {
            PlannedRouteMode::Qos0 => {
                let Some(active) = state.active.get(&target.key).cloned() else {
                    continue;
                };
                let frame = BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message: routed,
                    packet_id: None,
                    dup: false,
                    command: false,
                    progress: None,
                    unsent_command: UnsentCommandGuard::default(),
                    _budget: target.budget,
                }));
                if active.sender.try_send(frame).is_ok() {
                    delivered += 1;
                } else if active.sender.is_closed() {
                    active.cancel.cancel();
                }
            }
            PlannedRouteMode::Live { packet_id } => {
                let session = state.sessions.get_mut(&target.key).ok_or(Error::Internal)?;
                session.next_packet_id = if packet_id == u16::MAX {
                    1
                } else {
                    packet_id + 1
                };
                session.insert_outbound(
                    packet_id,
                    if target.qos == 1 {
                        OutboundState::AwaitPuback(routed.clone())
                    } else {
                        OutboundState::AwaitPubrec(routed.clone())
                    },
                );
                session.sent.insert(packet_id);
                session.send_window.insert(packet_id);
                session.state_bytes += charge;
                state.session_bytes += charge;
                sync_session_usage(state, &target.key)?;
                let frame = BrokerFrame::Publish(Box::new(BrokerDelivery {
                    message: routed,
                    packet_id: Some(packet_id),
                    dup: false,
                    command: false,
                    progress: None,
                    unsent_command: UnsentCommandGuard::default(),
                    _budget: target.budget,
                }));
                if let Some(active) = state.active.get(&target.key)
                    && active.sender.try_send(frame).is_err()
                {
                    active.cancel.cancel();
                }
                delivered += 1;
            }
            PlannedRouteMode::Offline => {
                let session = state.sessions.get_mut(&target.key).ok_or(Error::Internal)?;
                session.offline.push_back(routed);
                session.offline_bytes += charge;
                session.state_bytes += charge;
                state.offline_count += 1;
                state.offline_bytes += charge;
                state.session_bytes += charge;
                sync_session_usage(state, &target.key)?;
                mark_pending(state, &target.key);
                delivered += 1;
            }
        }
    }
    Ok(delivered)
}

pub(super) fn preflight_route(
    state: &BrokerState,
    owner: &DeviceKey,
    origin: Option<&SessionKey>,
    message: &BrokerMessage,
    limits: &Limits,
    charge: usize,
) -> Result<RoutePlan> {
    if message.retain {
        check_retained_update(state, owner, origin, message, limits)?;
    }
    let matches = state.trie.matching(&message.topic);
    let tenant_capacity_hint = matches.len().min(state.tenant_usage.len());
    let mut tenant_usage = HashMap::<TenantId, TenantRouteUsage>::new();
    let mut global_state_bytes = total_session_bytes(state);
    let mut global_offline_count = state.offline_count;
    let mut global_offline_bytes = state.offline_bytes;
    let mut plan = RoutePlan {
        targets: Vec::with_capacity(matches.len()),
    };

    for (key, subscription) in matches {
        if subscription.no_local && origin == Some(&key) {
            continue;
        }
        let qos = message.qos.min(subscription.qos);
        let active = state
            .active
            .get(&key)
            .filter(|active| !active.sender.is_closed() && active.sender.capacity() > 0);
        if qos == 0 {
            let Some(budget) = active.and_then(|active| active.reserve_frame(charge).ok()) else {
                continue;
            };
            plan.targets.push(PlannedRouteTarget {
                key,
                qos,
                retain_as_published: subscription.retain_as_published,
                mode: PlannedRouteMode::Qos0,
                budget,
            });
            continue;
        }
        if charge > limits.max_outbound_bytes_per_connection {
            return Err(Error::Overloaded);
        }
        let session = state.sessions.get(&key).ok_or(Error::Internal)?;
        let tenant = &key.device.tenant_id;
        if tenant_usage.is_empty() {
            // Allocate only for required work. This O(1) hint avoids repeated
            // map growth without allocating an intermediate distinct-tenant set.
            tenant_usage.reserve(tenant_capacity_hint);
        }
        let usage = tenant_usage.entry(tenant.clone()).or_insert_with(|| {
            let stored = state.tenant_usage.get(tenant).copied().unwrap_or_default();
            TenantRouteUsage {
                session_bytes: stored.session_bytes.saturating_add(
                    state
                        .will_responsibility_tenants
                        .get(tenant)
                        .map_or(0, |usage| usage.1),
                ),
                offline_count: stored.offline_count,
                offline_bytes: stored.offline_bytes,
                qos1_inflight: stored.qos1_inflight,
                qos2_inflight: stored.qos2_inflight,
            }
        });
        let live_budget = active.and_then(|active| active.reserve_frame(charge).ok());
        let inflight = if qos == 1 {
            usage.qos1_inflight
        } else {
            usage.qos2_inflight
        };
        let inflight_limit = if qos == 1 {
            limits.max_inflight_qos1_per_tenant
        } else {
            limits.max_inflight_qos2_per_tenant
        };
        let use_live = live_budget.is_some()
            && inflight < inflight_limit
            && session.has_outbound_capacity(qos, limits)
            && session.has_send_quota();
        if session.state_bytes.saturating_add(charge) > limits.max_mqtt_session_state_bytes
            || usage.session_bytes.saturating_add(charge)
                > limits.max_mqtt_session_state_bytes_per_tenant
            || global_state_bytes.saturating_add(charge) > limits.global_mqtt_session_bytes
        {
            return Err(Error::Overloaded);
        }
        let mode = if use_live {
            let packet_id = projected_packet_id(session)?;
            if qos == 1 {
                usage.qos1_inflight += 1;
            } else {
                usage.qos2_inflight += 1;
            }
            PlannedRouteMode::Live { packet_id }
        } else {
            if session.offline.len() >= limits.max_offline_messages_per_session
                || session.offline_bytes.saturating_add(charge)
                    > limits.max_offline_bytes_per_session
                || usage.offline_count >= limits.max_offline_messages_per_tenant
                || usage.offline_bytes.saturating_add(charge) > limits.max_offline_bytes_per_tenant
                || global_offline_count >= limits.max_offline_messages
                || global_offline_bytes.saturating_add(charge) > limits.max_offline_bytes
            {
                return Err(Error::Overloaded);
            }
            global_offline_count += 1;
            global_offline_bytes += charge;
            usage.offline_count += 1;
            usage.offline_bytes += charge;
            PlannedRouteMode::Offline
        };
        usage.session_bytes += charge;
        global_state_bytes += charge;
        plan.targets.push(PlannedRouteTarget {
            key,
            qos,
            retain_as_published: subscription.retain_as_published,
            mode,
            budget: if use_live {
                live_budget.ok_or(Error::Internal)?
            } else {
                Vec::new()
            },
        });
    }
    Ok(plan)
}

pub(super) fn projected_packet_id(session: &StoredSession) -> Result<u16> {
    let mut candidate = session.next_packet_id;
    for _ in 0..u16::MAX {
        if !session.outbound.contains_key(&candidate) {
            return Ok(candidate);
        }
        candidate = if candidate == u16::MAX {
            1
        } else {
            candidate + 1
        };
    }
    Err(Error::Overloaded)
}

pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter == "#" || filter == "+" || filter.starts_with("+/")) {
        return false;
    }
    let mut filters = filter.split('/');
    let mut topics = topic.split('/');
    while let Some(filter) = filters.next() {
        match filter {
            "#" => return filters.next().is_none(),
            "+" if topics.next().is_some() => {}
            level if topics.next() == Some(level) => {}
            _ => return false,
        }
    }
    topics.next().is_none()
}

pub(super) fn valid_broker_message(message: &BrokerMessage, limits: &Limits) -> bool {
    message.qos <= 2
        && message.payload.len() <= limits.max_mqtt_packet_size
        && valid_topic(&message.topic, limits, false)
        && message.properties.valid(limits)
        && (message.properties.payload_format != Some(1)
            || std::str::from_utf8(&message.payload).is_ok())
}

pub fn subscribe_acl(auth: &AuthenticatedDevice, filter: &str, limits: &Limits) -> bool {
    session_subscribe_acl(&auth.device_key, &auth.permissions, filter, limits)
}

pub(super) fn device_topic_root(device: &DeviceKey) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/",
        device.tenant_id.as_str(),
        device.product_id.as_str(),
        device.device_id.as_str()
    )
}

pub(super) fn session_subscribe_acl(
    device: &DeviceKey,
    permissions: &Permissions,
    filter: &str,
    limits: &Limits,
) -> bool {
    if !valid_topic(filter, limits, true) || !permissions.commands {
        return false;
    }
    let root = device_topic_root(device);
    filter
        .strip_prefix(&root)
        .is_some_and(|suffix| !suffix.is_empty())
}

pub(super) fn session_delivery_acl(
    device: &DeviceKey,
    permissions: &Permissions,
    topic: &str,
    limits: &Limits,
) -> bool {
    if !valid_topic(topic, limits, false) || !permissions.commands {
        return false;
    }
    let root = device_topic_root(device);
    topic
        .strip_prefix(&root)
        .is_some_and(|suffix| !suffix.is_empty())
}

pub(super) fn device_publish_topic(
    device: &DeviceKey,
    topic: &str,
    permissions: Option<&Permissions>,
) -> bool {
    let root = device_topic_root(device);
    let Some(suffix) = topic.strip_prefix(&root) else {
        return false;
    };
    match suffix {
        "up" => permissions.is_none_or(|value| value.publish),
        "down_ack" => permissions.is_none_or(|value| value.publish && value.commands),
        _ => false,
    }
}

pub(super) fn retained_topic_owner_acl(tenant: &TenantId, topic: &str) -> bool {
    let levels = topic.split('/').collect::<Vec<_>>();
    levels.len() == 8
        && levels[0] == "v1"
        && levels[1] == "t"
        && levels[2] == tenant.as_str()
        && levels[3] == "p"
        && ProductId::new(levels[4]).is_ok()
        && levels[5] == "d"
        && DeviceId::new(levels[6]).is_ok()
        && matches!(levels[7], "up" | "down_ack")
}

pub(super) fn authorization_complete(authorization: &SessionAuthorization) -> bool {
    authorization.codec_version > 0
}

impl MqttBroker {
    pub fn route(&self, owner: &DeviceKey, message: BrokerMessage) -> Result<usize> {
        self.route_with_origin(owner, None, &message)
    }

    pub fn route_from_session(&self, key: &SessionKey, message: &BrokerMessage) -> Result<usize> {
        self.route_with_origin(&key.device, Some(key), message)
    }

    pub(super) fn route_with_origin(
        &self,
        owner: &DeviceKey,
        origin: Option<&SessionKey>,
        message: &BrokerMessage,
    ) -> Result<usize> {
        if !valid_broker_message(message, &self.limits) {
            return Err(Error::Invalid);
        }
        if message.expired(now_ms()) {
            return Ok(0);
        }
        // Stores happen while holding the broker mutex after the matching trie mutation. A zero
        // observed here therefore linearizes this route before a concurrent subscribe or after a
        // concurrent final unsubscribe. Retained messages still need the mutex for their update.
        if !message.retain && self.subscription_count.load(Ordering::Acquire) == 0 {
            return Ok(0);
        }
        let mut state = self.lock_state(BrokerProbe::Route)?;
        prune_expired_messages(&mut state, now_ms(), HOT_MAINTENANCE_BUDGET)?;
        let result = route_locked(&mut state, owner, origin, message, &self.limits);
        if let Err(error) = drive_capacity_wakes(&mut state, &self.limits) {
            tracing::error!(%error, "failed to promote MQTT work after capacity release");
        }
        result
    }
}
