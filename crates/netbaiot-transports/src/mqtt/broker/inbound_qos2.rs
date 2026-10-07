//! inbound qos2 responsibilities under the single broker mutex.
use super::*;

impl MqttBroker {
    /// MQTT 5 Receive Maximum is scoped to this connection. QoS 1 is processed
    /// serially by the connection; QoS 2 retains a slot until PUBCOMP is sent.
    pub fn inbound_receive_available(
        &self,
        key: &SessionKey,
        generation: u64,
        qos: u8,
        packet_id: u16,
    ) -> Result<bool> {
        let state = self.lock_state(BrokerProbe::Inbound)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get(key).ok_or(Error::Internal)?;
        if qos == 2 && session.inbound_qos2.contains_key(&packet_id) {
            return Ok(true);
        }
        let limit = self
            .limits
            .max_inflight_qos1_per_session
            .min(self.limits.max_inflight_qos2_per_session);
        Ok(session.inbound_window.len() < limit)
    }

    /// A retransmitted PUBLISH on a new network connection consumes that
    /// connection's receive slot once, while retaining the Session transaction.
    pub fn begin_inbound_qos2_retransmission(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<bool> {
        let mut state = self.lock_state(BrokerProbe::Inbound)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if !matches!(
            session.inbound_qos2.get(&packet_id),
            Some(InboundQos2State::AwaitPubrel(_))
        ) {
            return Err(Error::Conflict);
        }
        if session.inbound_window.contains(&packet_id) {
            return Ok(true);
        }
        let limit = self
            .limits
            .max_inflight_qos1_per_session
            .min(self.limits.max_inflight_qos2_per_session);
        if session.inbound_window.len() >= limit {
            return Ok(false);
        }
        session.inbound_window.insert(packet_id);
        Ok(true)
    }

    /// Classify a MQTT 5 QoS 2 PUBLISH before authorizing its topic. Only a
    /// transaction still awaiting PUBREL can bypass the second packet's ACL.
    /// The connection processes packets serially; `inbound_qos2` rechecks
    /// ownership and inserts under the broker lock after new-message admission.
    pub(crate) fn classify_inbound_qos2_publish(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<InboundQos2PublishState> {
        if packet_id == 0 {
            return Err(Error::Invalid);
        }
        let state = self.lock_state(BrokerProbe::Inbound)?;
        check_owner(&state, key, generation)?;
        let session = state.sessions.get(key).ok_or(Error::Internal)?;
        Ok(match session.inbound_qos2.get(&packet_id) {
            Some(InboundQos2State::AwaitPubrel(_)) => InboundQos2PublishState::ExistingTransaction,
            Some(_) => InboundQos2PublishState::IdentifierInUse,
            None => InboundQos2PublishState::NeedsNewMessageAdmission,
        })
    }

    pub fn inbound_qos2(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
        message: BrokerMessage,
    ) -> Result<bool> {
        if message.qos != 2 {
            return Err(Error::Invalid);
        }
        let mut state = self.lock_state(BrokerProbe::Inbound)?;
        check_owner(&state, key, generation)?;
        let session_state_bytes = {
            let session = state.sessions.get(key).ok_or(Error::Internal)?;
            if let Some(existing) = session.inbound_qos2.get(&packet_id) {
                if session.version == MqttVersion::V5 {
                    // The accepted Packet Identifier owns the original message until
                    // PUBREL. Repeated PUBLISH contents and DUP do not replace it.
                    return Ok(false);
                }
                return match existing {
                    InboundQos2State::AwaitPubrel(existing)
                    | InboundQos2State::Delivering {
                        message: existing, ..
                    }
                    | InboundQos2State::EventAccepted(existing)
                        if existing == &message =>
                    {
                        Ok(false)
                    }
                    _ => Err(Error::Invalid),
                };
            }
            if !valid_broker_message(&message, &self.limits) {
                return Err(Error::Invalid);
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
            if session.version == MqttVersion::V5
                && session.inbound_window.len()
                    >= self
                        .limits
                        .max_inflight_qos1_per_session
                        .min(self.limits.max_inflight_qos2_per_session)
            {
                return Err(Error::Overloaded);
            }
            session.state_bytes
        };
        let charge = message.bytes();
        let reservation = reserve_retained(
            &mut state,
            &key.device.tenant_id,
            Some(key),
            &message,
            &self.limits,
        )?;
        if session_state_bytes.saturating_add(charge) > self.limits.max_mqtt_session_state_bytes
            || tenant_inflight(&state, &key.device.tenant_id, 2)
                >= self.limits.max_inflight_qos2_per_tenant
            || tenant_total_session_bytes(&state, &key.device.tenant_id).saturating_add(charge)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            || total_session_bytes(&state).saturating_add(charge)
                > self.limits.global_mqtt_session_bytes
        {
            release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
            return Err(Error::Overloaded);
        }
        {
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes += charge;
            session
                .inbound_qos2
                .insert(packet_id, InboundQos2State::AwaitPubrel(message));
            if session.version == MqttVersion::V5 {
                session.inbound_window.insert(packet_id);
            }
            if reservation != RetainedReservation::default() {
                session.inbound_reservations.insert(packet_id, reservation);
            }
        }
        state.session_bytes += charge;
        sync_session_usage(&mut state, key)?;
        Ok(true)
    }

    pub fn finish_inbound_pubcomp(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Pubcomp)?;
        check_owner(&state, key, generation)?;
        state
            .sessions
            .get_mut(key)
            .ok_or(Error::Internal)?
            .inbound_window
            .remove(&packet_id);
        Ok(())
    }

    pub fn begin_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<InboundQos2Action> {
        let mut state = self.lock_state(BrokerProbe::Pubrel)?;
        check_owner(&state, key, generation)?;
        state.operation_id = state.operation_id.wrapping_add(1).max(1);
        let operation_id = state.operation_id;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        let session_incarnation = session.incarnation;
        let Some(entry) = session.inbound_qos2.get_mut(&packet_id) else {
            return Ok(InboundQos2Action::Unknown);
        };
        Ok(match entry {
            InboundQos2State::AwaitPubrel(message) => {
                let message = message.clone();
                *entry = InboundQos2State::Delivering {
                    message: message.clone(),
                    operation_id,
                };
                InboundQos2Action::Deliver {
                    message,
                    session_incarnation,
                    operation_id,
                }
            }
            InboundQos2State::Delivering { .. } => InboundQos2Action::DeliveryInProgress,
            InboundQos2State::EventAccepted(_) => {
                let operation_id = *session
                    .inbound_operations
                    .entry(packet_id)
                    .or_insert(operation_id);
                InboundQos2Action::EventAccepted {
                    session_incarnation,
                    operation_id,
                }
            }
        })
    }

    #[cfg(test)]
    pub(super) fn inbound_qos2_message(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<Option<(BrokerMessage, bool)>> {
        let state = self.lock_state(BrokerProbe::Inbound)?;
        check_owner(&state, key, generation)?;
        Ok(state
            .sessions
            .get(key)
            .and_then(|session| session.inbound_qos2.get(&packet_id))
            .map(|entry| match entry {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. } => (message.clone(), false),
                InboundQos2State::EventAccepted(message) => (message.clone(), true),
            }))
    }

    pub fn finish_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Pubrel)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.incarnation != session_incarnation {
            return Err(Error::Conflict);
        }
        let entry = session
            .inbound_qos2
            .get_mut(&packet_id)
            .ok_or(Error::Invalid)?;
        match entry {
            InboundQos2State::Delivering {
                message,
                operation_id: current,
            } if *current == operation_id => {
                *entry = InboundQos2State::EventAccepted(message.clone());
                session.inbound_operations.insert(packet_id, operation_id);
                Ok(())
            }
            InboundQos2State::EventAccepted(_)
                if session.inbound_operations.get(&packet_id) == Some(&operation_id) =>
            {
                Ok(())
            }
            _ => Err(Error::Conflict),
        }
    }

    pub fn abandon_inbound_qos2_delivery(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
    ) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Pubrel)?;
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.incarnation != session_incarnation {
            return Err(Error::Conflict);
        }
        let entry = session
            .inbound_qos2
            .get_mut(&packet_id)
            .ok_or(Error::Invalid)?;
        if let InboundQos2State::Delivering {
            message,
            operation_id: current,
        } = entry
            && *current == operation_id
        {
            *entry = InboundQos2State::AwaitPubrel(message.clone());
            return Ok(());
        }
        Err(Error::Conflict)
    }

    pub fn complete_inbound_qos2(
        &self,
        key: &SessionKey,
        generation: u64,
        packet_id: u16,
    ) -> Result<()> {
        let mut state = self.lock_state(BrokerProbe::Pubrel)?;
        check_owner(&state, key, generation)?;
        let message = state
            .sessions
            .get_mut(key)
            .ok_or(Error::Internal)?
            .inbound_qos2
            .remove(&packet_id)
            .map(|state| match state {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. }
                | InboundQos2State::EventAccepted(message) => message,
            });
        if let Some(session) = state.sessions.get_mut(key) {
            session.inbound_operations.remove(&packet_id);
        }
        if let Some(message) = &message {
            let reservation = state
                .sessions
                .get_mut(key)
                .and_then(|session| session.inbound_reservations.remove(&packet_id))
                .unwrap_or_default();
            release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
            let charge = message.bytes();
            let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
        sync_session_usage(&mut state, key)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(())
    }

    pub fn route_inbound_qos2(
        &self,
        key: &SessionKey,
        session_incarnation: u64,
        packet_id: u16,
        operation_id: u64,
        owner: &DeviceKey,
    ) -> Result<usize> {
        let mut state = self.lock_state(BrokerProbe::Pubrel)?;
        let message = match state
            .sessions
            .get(key)
            .filter(|session| session.incarnation == session_incarnation)
            .and_then(|session| session.inbound_qos2.get(&packet_id))
        {
            Some(InboundQos2State::EventAccepted(message))
                if state
                    .sessions
                    .get(key)
                    .and_then(|session| session.inbound_operations.get(&packet_id))
                    == Some(&operation_id) =>
            {
                message.clone()
            }
            _ => return Err(Error::Conflict),
        };
        let reservation = state
            .sessions
            .get_mut(key)
            .and_then(|session| session.inbound_reservations.remove(&packet_id))
            .unwrap_or_default();
        release_retained_reservation(&mut state, &key.device.tenant_id, reservation);
        let delivered = match route_locked(&mut state, owner, Some(key), &message, &self.limits) {
            Ok(delivered) => delivered,
            Err(error) => {
                let restored = reserve_retained(
                    &mut state,
                    &key.device.tenant_id,
                    Some(key),
                    &message,
                    &self.limits,
                )?;
                if restored != RetainedReservation::default() {
                    state
                        .sessions
                        .get_mut(key)
                        .ok_or(Error::Internal)?
                        .inbound_reservations
                        .insert(packet_id, restored);
                }
                return Err(error);
            }
        };
        let session = state.sessions.get_mut(key).ok_or(Error::Internal)?;
        if session.inbound_qos2.remove(&packet_id).is_some() {
            session.inbound_operations.remove(&packet_id);
            let charge = message.bytes();
            session.state_bytes = session.state_bytes.saturating_sub(charge);
            state.session_bytes = state.session_bytes.saturating_sub(charge);
        }
        sync_session_usage(&mut state, key)?;
        drive_capacity_wakes(&mut state, &self.limits)?;
        retry_pending_wills(&mut state, &self.limits);
        Ok(delivered)
    }
}
