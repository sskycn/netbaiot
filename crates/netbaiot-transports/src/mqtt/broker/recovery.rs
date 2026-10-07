//! recovery responsibilities under the single broker mutex.
use super::*;

pub(super) fn write_recovery(
    directory: &Path,
    limits: &Limits,
    broker: &MqttBroker,
) -> Result<PathBuf> {
    recovery_io::prepare_directory(directory)?;
    recovery_io::ensure_temporary_capacity(directory, limits.spool_max_records)?;
    let temporary = directory.join(format!(".{RECOVERY_FILE}.{}.tmp", uuid::Uuid::new_v4()));
    let committed = directory.join(RECOVERY_FILE);
    let mut file = recovery_io::create_private(&temporary)?;
    let _temporary_cleanup = TemporaryRecovery(temporary.clone());
    // Shutdown has fenced control mutations and joined device/maintenance owners.
    // Keep one coherent view while streaming bounded records: cloning payload state
    // would increase peak memory by up to the entire admitted broker state. This
    // lock can span slow disk I/O, but commit runs on spawn_blocking, off Tokio
    // workers. Prefer bounded memory/coherence over shorter lock hold until a
    // measured ownership-transfer design proves Will/QoS/recovery equivalence.
    let state = lock(&broker.state)?;
    let mut header = [0u8; RECOVERY_PREFIX_BYTES];
    header[..4].copy_from_slice(RECOVERY_MAGIC);
    header[4..8].copy_from_slice(&RECOVERY_VERSION.to_be_bytes());
    header[8..16].copy_from_slice(&state.generation.to_be_bytes());
    let mut recovery = RecoveryWriteState {
        written: RECOVERY_HEADER_BYTES,
        record_count: 0,
        record_bytes: 0,
        stream_hash: Sha256::new(),
    };
    recovery.stream_hash.update(header);
    file.write_all(&header)
        .and_then(|_| file.write_all(&Sha256::digest(header)))
        .map_err(|_| Error::Storage)?;
    if state.will_responsibility_count != pending_will_count(&state) {
        // Planned shutdown must detach every connection first, transferring every armed Will to
        // either settled or broker-owned pending state before a coherent image is committed.
        return Err(Error::Conflict);
    }
    let mut sessions = state.sessions.values().collect::<Vec<_>>();
    sessions.sort_by(|left, right| {
        (
            left.key.device.tenant_id.as_str(),
            left.key.device.product_id.as_str(),
            left.key.device.device_id.as_str(),
            left.key.client_id.as_str(),
        )
            .cmp(&(
                right.key.device.tenant_id.as_str(),
                right.key.device.product_id.as_str(),
                right.key.device.device_id.as_str(),
                right.key.client_id.as_str(),
            ))
    });
    for session in sessions {
        let mut record = Vec::with_capacity(384);
        encode_session_meta(&mut record, session)?;
        write_record(&mut file, RECORD_SESSION, &record, limits, &mut recovery)?;
        let mut subscriptions = session.subscriptions.iter().collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| left.0.cmp(right.0));
        for (filter, subscription) in subscriptions {
            record.clear();
            put_string(&mut record, filter)?;
            record.push(subscription.qos);
            record.push(
                u8::from(subscription.no_local) << 2
                    | u8::from(subscription.retain_as_published) << 3
                    | subscription.retain_handling << 4,
            );
            write_record(
                &mut file,
                RECORD_SUBSCRIPTION,
                &record,
                limits,
                &mut recovery,
            )?;
        }
        for message in &session.offline {
            record.clear();
            encode_message(&mut record, message)?;
            write_record(&mut file, RECORD_OFFLINE, &record, limits, &mut recovery)?;
        }
        let mut inbound = session.inbound_qos2.iter().collect::<Vec<_>>();
        inbound.sort_by_key(|(packet_id, _)| **packet_id);
        for (packet_id, inbound) in inbound {
            record.clear();
            record.extend_from_slice(&packet_id.to_be_bytes());
            match inbound {
                InboundQos2State::EventAccepted(message) => {
                    record.push(1);
                    encode_message(&mut record, message)?;
                }
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. } => {
                    // Delivery ownership is process-local; restart safely retries PUBREL work.
                    record.push(0);
                    encode_message(&mut record, message)?;
                }
            }
            write_record(
                &mut file,
                RECORD_INBOUND_QOS2,
                &record,
                limits,
                &mut recovery,
            )?;
        }
        for packet_id in &session.outbound_order {
            let outbound = session.outbound.get(packet_id).ok_or(Error::Invalid)?;
            record.clear();
            record.extend_from_slice(&packet_id.to_be_bytes());
            let (kind, message) = match outbound {
                OutboundState::AwaitPuback(message) => (0, message),
                OutboundState::AwaitPubrec(message) => (1, message),
                OutboundState::AwaitPubcomp(message) => (2, message),
            };
            record.push(kind);
            record.push(u8::from(session.started_outbound.contains(packet_id)));
            encode_message(&mut record, message)?;
            write_record(&mut file, RECORD_OUTBOUND, &record, limits, &mut recovery)?;
        }
    }
    for pending in all_pending_wills(&state) {
        let mut record = Vec::with_capacity(pending.message.payload.len().saturating_add(384));
        put_string(&mut record, pending.owner.tenant_id.as_str())?;
        put_string(&mut record, pending.owner.product_id.as_str())?;
        put_string(&mut record, pending.owner.device_id.as_str())?;
        encode_message(&mut record, &pending.message)?;
        if let Some((key, incarnation)) = &pending.cancel_on_resume {
            record.push(1);
            put_string(&mut record, &key.client_id)?;
            record.extend_from_slice(&incarnation.to_be_bytes());
            record.extend_from_slice(&pending.due_at_ms.ok_or(Error::Invalid)?.to_be_bytes());
        } else {
            record.push(0);
        }
        if let Some(expiry) = pending.message_expiry_interval {
            record.push(1);
            record.extend_from_slice(&expiry.to_be_bytes());
        } else {
            record.push(0);
        }
        if let Some(origin) = &pending.origin {
            record.push(1);
            put_string(&mut record, &origin.client_id)?;
        } else {
            record.push(0);
        }
        write_record(
            &mut file,
            RECORD_PENDING_WILL,
            &record,
            limits,
            &mut recovery,
        )?;
    }
    let mut retained = state.retained.values().collect::<Vec<_>>();
    retained.sort_by(|left, right| left.message.topic.cmp(&right.message.topic));
    for retained in retained {
        let mut record = Vec::with_capacity(retained.message.payload.len().saturating_add(384));
        put_string(&mut record, retained.tenant_id.as_str())?;
        encode_message(&mut record, &retained.message)?;
        if let Some(origin) = &retained.origin {
            record.push(1);
            put_string(&mut record, origin.device.tenant_id.as_str())?;
            put_string(&mut record, origin.device.product_id.as_str())?;
            put_string(&mut record, origin.device.device_id.as_str())?;
            put_string(&mut record, &origin.client_id)?;
        } else {
            record.push(0);
        }
        write_record(&mut file, RECORD_RETAINED, &record, limits, &mut recovery)?;
    }
    drop(state);
    recovery.written = recovery
        .written
        .checked_add(RECOVERY_TRAILER_BYTES)
        .ok_or(Error::Overloaded)?;
    if recovery.written > limits.mqtt_recovery_max_bytes {
        return Err(Error::Overloaded);
    }
    let mut trailer = [0u8; RECOVERY_TRAILER_BYTES];
    trailer[..4].copy_from_slice(RECOVERY_TRAILER_MAGIC);
    trailer[4..12].copy_from_slice(&recovery.record_count.to_be_bytes());
    trailer[12..20].copy_from_slice(&recovery.record_bytes.to_be_bytes());
    trailer[20..].copy_from_slice(&recovery.stream_hash.finalize());
    file.write_all(&trailer).map_err(|_| Error::Storage)?;
    file.sync_all().map_err(|_| Error::Storage)?;
    drop(file);
    recovery_io::replace_synced(&temporary, &committed)?;
    Ok(committed)
}

pub(super) fn read_recovery(
    directory: &Path,
    limits: &Limits,
) -> Result<Option<MqttRecoverySnapshot>> {
    let path = directory.join(RECOVERY_FILE);
    if !recovery_io::directory_present(directory)? {
        return Ok(None);
    }
    let Some(file) = recovery_io::open_snapshot(&path)? else {
        return Ok(None);
    };
    let size = usize::try_from(file.metadata().map_err(|_| Error::Storage)?.len())
        .map_err(|_| Error::Overloaded)?;
    if size < RECOVERY_PREFIX_BYTES {
        return Err(Error::Invalid);
    }
    // The decoder allocates one checked record at a time; actual reads are also
    // capped, even if the open file grows after metadata. Probe exact EOF below.
    let ceiling = limits
        .mqtt_recovery_max_bytes
        .max(LEGACY_V1_RECOVERY_READ_MAX)
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    read_storage_snapshot(file, size, ceiling, limits)
}

pub(super) fn read_storage_snapshot(
    reader: impl Read,
    size: usize,
    ceiling: usize,
    limits: &Limits,
) -> Result<Option<MqttRecoverySnapshot>> {
    let mut source = recovery_io::StorageReader::new(
        reader.take(u64::try_from(ceiling).map_err(|_| Error::Overloaded)?),
    );
    let result = (|| {
        let mut reader = BufReader::new(&mut source);
        let snapshot = decode_recovery_reader(&mut reader, size, limits)?;
        let mut extra = [0];
        if reader.read(&mut extra).map_err(|_| Error::Storage)? != 0 {
            return Err(Error::Invalid);
        }
        Ok(Some(snapshot))
    })();
    if source.failed {
        Err(Error::Storage)
    } else {
        result
    }
}

/// Pure, bounded decoder used by fuzzing. Production uses the same decoder over a buffered file.
pub fn decode_mqtt_recovery(input: &[u8], limits: &Limits) -> Result<MqttRecoverySnapshot> {
    decode_recovery_reader(Cursor::new(input), input.len(), limits)
}

pub(super) fn decode_recovery_reader(
    mut reader: impl Read,
    size: usize,
    limits: &Limits,
) -> Result<MqttRecoverySnapshot> {
    let mut header = [0u8; RECOVERY_PREFIX_BYTES];
    reader.read_exact(&mut header).map_err(|_| Error::Invalid)?;
    if &header[..4] != RECOVERY_MAGIC {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().map_err(|_| Error::Invalid)?);
    let generation = u64::from_be_bytes(header[8..16].try_into().map_err(|_| Error::Invalid)?);
    let maximum = if version == RECOVERY_VERSION_V1 {
        LEGACY_V1_RECOVERY_READ_MAX
    } else {
        limits.mqtt_recovery_max_bytes
    };
    if size > maximum {
        return Err(Error::Invalid);
    }
    if version == RECOVERY_VERSION_V1 {
        return decode_v1(reader, size, generation, limits);
    }
    if !matches!(
        version,
        RECOVERY_VERSION_V2
            | RECOVERY_VERSION_V3
            | RECOVERY_VERSION_V4
            | RECOVERY_VERSION_V5
            | RECOVERY_VERSION
    ) {
        return Err(Error::Invalid);
    }
    let mut header_checksum = [0u8; 32];
    reader
        .read_exact(&mut header_checksum)
        .map_err(|_| Error::Invalid)?;
    if Sha256::digest(header).as_slice() != header_checksum {
        return Err(Error::Invalid);
    }
    let mut consumed = RECOVERY_HEADER_BYTES;
    let records_end = if version >= RECOVERY_VERSION_V3 {
        size.checked_sub(RECOVERY_TRAILER_BYTES)
            .filter(|end| *end >= RECOVERY_HEADER_BYTES)
            .ok_or(Error::Invalid)?
    } else {
        size
    };
    let mut stream_hash = Sha256::new();
    stream_hash.update(header);
    let mut record_count = 0u64;
    let mut total_record_bytes = 0u64;
    let mut snapshot = MqttRecoverySnapshot {
        format_version: version,
        snapshot_generation: generation,
        sessions: Vec::new(),
        retained: Vec::new(),
        pending_wills: Vec::new(),
    };
    let mut retained_phase = false;
    while consumed < records_end {
        let mut record_header = [0u8; RECORD_HEADER_BYTES];
        reader
            .read_exact(&mut record_header)
            .map_err(|_| Error::Invalid)?;
        consumed = consumed
            .checked_add(RECORD_HEADER_BYTES)
            .ok_or(Error::Invalid)?;
        let kind = record_header[0];
        if kind == RECORD_RETAINED {
            retained_phase = true;
        } else if retained_phase {
            return Err(Error::Invalid);
        }
        let length = usize::try_from(u32::from_be_bytes(
            record_header[1..5].try_into().map_err(|_| Error::Invalid)?,
        ))
        .map_err(|_| Error::Invalid)?;
        if length > recovery_record_max(limits)?
            || consumed
                .checked_add(length)
                .and_then(|value| value.checked_add(RECORD_CHECKSUM_BYTES))
                .is_none_or(|end| end > records_end)
        {
            return Err(Error::Invalid);
        }
        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .map_err(|_| Error::Invalid)?;
        let mut checksum = [0u8; RECORD_CHECKSUM_BYTES];
        reader
            .read_exact(&mut checksum)
            .map_err(|_| Error::Invalid)?;
        if Sha256::digest(&payload).as_slice() != checksum {
            return Err(Error::Invalid);
        }
        let wire_bytes = RECORD_HEADER_BYTES + length + RECORD_CHECKSUM_BYTES;
        consumed += length + RECORD_CHECKSUM_BYTES;
        if version >= RECOVERY_VERSION_V3 {
            stream_hash.update(record_header);
            stream_hash.update(&payload);
            stream_hash.update(checksum);
            record_count = record_count.checked_add(1).ok_or(Error::Invalid)?;
            total_record_bytes = total_record_bytes
                .checked_add(u64::try_from(wire_bytes).map_err(|_| Error::Invalid)?)
                .ok_or(Error::Invalid)?;
        }
        decode_record(kind, &payload, &mut snapshot, limits, version)?;
    }
    if consumed != records_end {
        return Err(Error::Invalid);
    }
    if version >= RECOVERY_VERSION_V3 {
        let mut trailer = [0u8; RECOVERY_TRAILER_BYTES];
        reader
            .read_exact(&mut trailer)
            .map_err(|_| Error::Invalid)?;
        if &trailer[..4] != RECOVERY_TRAILER_MAGIC
            || u64::from_be_bytes(trailer[4..12].try_into().map_err(|_| Error::Invalid)?)
                != record_count
            || u64::from_be_bytes(trailer[12..20].try_into().map_err(|_| Error::Invalid)?)
                != total_record_bytes
            || stream_hash.finalize().as_slice() != &trailer[20..]
        {
            return Err(Error::Invalid);
        }
    }
    Ok(snapshot)
}

pub(super) fn decode_v1(
    mut reader: impl Read,
    size: usize,
    generation: u64,
    _limits: &Limits,
) -> Result<MqttRecoverySnapshot> {
    if size < 52 {
        return Err(Error::Invalid);
    }
    let mut length_bytes = [0u8; 4];
    reader
        .read_exact(&mut length_bytes)
        .map_err(|_| Error::Invalid)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| Error::Invalid)?;
    if length.saturating_add(52) != size || length > LEGACY_V1_RECOVERY_READ_MAX {
        return Err(Error::Invalid);
    }
    let mut payload = vec![0u8; length];
    reader
        .read_exact(&mut payload)
        .map_err(|_| Error::Invalid)?;
    let mut checksum = [0u8; 32];
    reader
        .read_exact(&mut checksum)
        .map_err(|_| Error::Invalid)?;
    if Sha256::digest(&payload).as_slice() != checksum {
        return Err(Error::Invalid);
    }
    let snapshot: MqttRecoverySnapshot =
        serde_json::from_slice(&payload).map_err(|_| Error::Invalid)?;
    if snapshot.snapshot_generation != generation || snapshot.format_version != version_one() {
        return Err(Error::Invalid);
    }
    Ok(snapshot)
}

pub(super) fn recovery_record_max(limits: &Limits) -> Result<usize> {
    limits
        .max_mqtt_packet_size
        .checked_add(limits.max_topic_bytes.saturating_mul(2))
        .and_then(|value| value.checked_add(limits.max_mqtt_property_bytes))
        // A delayed v6 Will may contain both a cancellation and an origin ClientId.
        .and_then(|value| value.checked_add(limits.max_client_id_bytes.saturating_mul(2)))
        .and_then(|value| value.checked_add(2_048))
        .ok_or(Error::Configuration)
}

pub(super) fn write_record(
    file: &mut fs::File,
    kind: u8,
    payload: &[u8],
    limits: &Limits,
    recovery: &mut RecoveryWriteState,
) -> Result<()> {
    if payload.len() > recovery_record_max(limits)? {
        return Err(Error::Overloaded);
    }
    let record_bytes = RECORD_HEADER_BYTES
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(RECORD_CHECKSUM_BYTES))
        .ok_or(Error::Overloaded)?;
    recovery.written = recovery
        .written
        .checked_add(record_bytes)
        .ok_or(Error::Overloaded)?;
    if recovery.written.saturating_add(RECOVERY_TRAILER_BYTES) > limits.mqtt_recovery_max_bytes {
        return Err(Error::Overloaded);
    }
    let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
    let mut header = [0u8; RECORD_HEADER_BYTES];
    header[0] = kind;
    header[1..].copy_from_slice(&length.to_be_bytes());
    let checksum = Sha256::digest(payload);
    file.write_all(&header)
        .and_then(|_| file.write_all(payload))
        .and_then(|_| file.write_all(&checksum))
        .map_err(|_| Error::Storage)?;
    recovery.stream_hash.update(header);
    recovery.stream_hash.update(payload);
    recovery.stream_hash.update(checksum);
    recovery.record_count = recovery
        .record_count
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    recovery.record_bytes = recovery
        .record_bytes
        .checked_add(u64::try_from(record_bytes).map_err(|_| Error::Overloaded)?)
        .ok_or(Error::Overloaded)?;
    Ok(())
}

pub(super) fn put_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u16::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

pub(super) fn encode_message(output: &mut Vec<u8>, message: &BrokerMessage) -> Result<()> {
    put_string(output, &message.topic)?;
    put_bytes(output, &message.payload)?;
    output.push(message.qos);
    output.push(u8::from(message.retain));
    let properties = &message.properties;
    output.push(properties.payload_format.unwrap_or(2));
    output.extend_from_slice(&properties.expires_at_ms.unwrap_or(-1).to_be_bytes());
    put_optional_string(output, properties.content_type.as_deref())?;
    put_optional_string(output, properties.response_topic.as_deref())?;
    match &properties.correlation_data {
        Some(value) => {
            output.push(1);
            put_bytes(output, value)?;
        }
        None => output.push(0),
    }
    output.extend_from_slice(
        &u16::try_from(properties.user_properties.len())
            .map_err(|_| Error::Overloaded)?
            .to_be_bytes(),
    );
    for (key, value) in &properties.user_properties {
        put_string(output, key)?;
        put_string(output, value)?;
    }
    Ok(())
}

pub(super) fn put_optional_string(output: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            output.push(1);
            put_string(output, value)?;
        }
        None => output.push(0),
    }
    Ok(())
}

pub(super) fn encode_session_meta(output: &mut Vec<u8>, session: &StoredSession) -> Result<()> {
    put_string(output, session.key.device.tenant_id.as_str())?;
    put_string(output, session.key.device.product_id.as_str())?;
    put_string(output, session.key.device.device_id.as_str())?;
    put_string(output, &session.key.client_id)?;
    output.extend_from_slice(&session.incarnation.to_be_bytes());
    if let Some(authorization) = session
        .authorization
        .as_ref()
        .filter(|profile| profile.codec_id.is_some() && profile.codec_version.is_some())
    {
        output.push(1);
        output.extend_from_slice(&authorization.credential_version.to_be_bytes());
        output.extend_from_slice(&authorization.auth_generation.to_be_bytes());
        output.push(u8::from(authorization.permissions.publish));
        output.push(u8::from(authorization.permissions.commands));
        let codec_id = authorization.codec_id.as_ref().ok_or(Error::Invalid)?;
        put_string(output, codec_id.as_str())?;
        output.extend_from_slice(
            &authorization
                .codec_version
                .ok_or(Error::Invalid)?
                .to_be_bytes(),
        );
    } else {
        // NBMQ v2 had no codec provenance. Mark it unknown in v6 so attach still
        // resets the session instead of inventing a profile or blocking shutdown.
        output.push(0);
    }
    output.extend_from_slice(&session.next_packet_id.to_be_bytes());
    output.extend_from_slice(&session.last_seen_ms.to_be_bytes());
    output.push(match session.version {
        MqttVersion::V311 => 4,
        MqttVersion::V5 => 5,
    });
    output.extend_from_slice(&session.session_expiry_interval.to_be_bytes());
    output.extend_from_slice(&session.expires_at_ms.unwrap_or(-1).to_be_bytes());
    Ok(())
}

pub(super) fn decode_message(
    reader: &mut RecordReader<'_>,
    limits: &Limits,
    format_version: u32,
) -> Result<BrokerMessage> {
    let topic = reader.string(limits.max_topic_bytes)?;
    let payload = reader.bytes(limits.max_mqtt_packet_size)?;
    let qos = reader.u8()?;
    let retain = match reader.u8()? {
        0 => false,
        1 => true,
        _ => return Err(Error::Invalid),
    };
    if qos > 2 || !valid_topic(&topic, limits, false) {
        return Err(Error::Invalid);
    }
    let properties = if format_version >= RECOVERY_VERSION_V4 {
        let payload_format = match reader.u8()? {
            value @ (0 | 1) => Some(value),
            2 => None,
            _ => return Err(Error::Invalid),
        };
        let expires_at_ms = match reader.i64()? {
            -1 => None,
            value if value >= 0 => Some(value),
            _ => return Err(Error::Invalid),
        };
        let content_type = reader.optional_string(limits.max_mqtt_content_type_bytes)?;
        let response_topic = reader.optional_string(limits.max_mqtt_response_topic_bytes)?;
        let correlation_data = match reader.u8()? {
            0 => None,
            1 => Some(reader.bytes(limits.max_mqtt_correlation_data_bytes)?),
            _ => return Err(Error::Invalid),
        };
        let count = usize::from(reader.u16()?);
        if count > limits.max_mqtt_user_properties {
            return Err(Error::Overloaded);
        }
        let mut user_properties = Vec::with_capacity(count);
        for _ in 0..count {
            let key = reader.string_allow_empty(limits.max_mqtt_user_property_bytes)?;
            let value = reader.string_allow_empty(limits.max_mqtt_user_property_bytes)?;
            user_properties.push((key, value));
        }
        PublishProperties {
            payload_format,
            expires_at_ms,
            content_type,
            response_topic,
            correlation_data,
            user_properties,
        }
    } else {
        PublishProperties::default()
    };
    Ok(BrokerMessage {
        topic: topic.into(),
        payload: payload.into(),
        qos,
        retain,
        properties: properties.into(),
    })
}

pub(super) fn decode_record(
    kind: u8,
    payload: &[u8],
    snapshot: &mut MqttRecoverySnapshot,
    limits: &Limits,
    format_version: u32,
) -> Result<()> {
    let mut reader = RecordReader::new(payload);
    match kind {
        RECORD_SESSION => {
            if snapshot.sessions.len() >= limits.max_persistent_sessions {
                return Err(Error::Overloaded);
            }
            let tenant_id = TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let product_id = ProductId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let device_id = DeviceId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let client_id = reader.string(limits.max_client_id_bytes)?;
            let incarnation = reader.u64()?;
            if incarnation == 0 {
                return Err(Error::Invalid);
            }
            let authorization = match reader.u8()? {
                0 => None,
                1 => {
                    let credential_version = reader.u32()?;
                    let auth_generation = reader.u64()?;
                    let permissions = Permissions {
                        publish: match reader.u8()? {
                            0 => false,
                            1 => true,
                            _ => return Err(Error::Invalid),
                        },
                        commands: match reader.u8()? {
                            0 => false,
                            1 => true,
                            _ => return Err(Error::Invalid),
                        },
                    };
                    let (codec_id, codec_version) = if format_version >= RECOVERY_VERSION_V3 {
                        (
                            Some(CodecId::new(reader.string(64)?).map_err(|_| Error::Invalid)?),
                            Some(reader.u16()?),
                        )
                    } else {
                        (None, None)
                    };
                    if codec_version == Some(0) {
                        return Err(Error::Invalid);
                    }
                    Some(SessionAuthorization {
                        credential_version,
                        auth_generation,
                        permissions,
                        codec_id,
                        codec_version,
                    })
                }
                _ => return Err(Error::Invalid),
            };
            let next_packet_id = reader.u16()?;
            if next_packet_id == 0 {
                return Err(Error::Invalid);
            }
            let last_seen_ms = reader.i64()?;
            let (version, session_expiry_interval, expires_at_ms) =
                if format_version >= RECOVERY_VERSION_V4 {
                    let version = match reader.u8()? {
                        4 => MqttVersion::V311,
                        5 => MqttVersion::V5,
                        _ => return Err(Error::Invalid),
                    };
                    let interval = reader.u32()?;
                    let expiry = reader.i64()?;
                    (
                        version,
                        interval,
                        if expiry == -1 { None } else { Some(expiry) },
                    )
                } else {
                    (MqttVersion::V311, 0, None)
                };
            reader.finish()?;
            let key = SessionKey {
                device: DeviceKey {
                    tenant_id,
                    product_id,
                    device_id,
                },
                client_id,
            };
            let mut session = StoredSession::new(
                key,
                incarnation,
                authorization.clone().unwrap_or(SessionAuthorization {
                    credential_version: 0,
                    auth_generation: 0,
                    permissions: Permissions {
                        publish: false,
                        commands: false,
                    },
                    codec_id: None,
                    codec_version: None,
                }),
            );
            session.authorization = authorization;
            session.version = version;
            session.session_expiry_interval = session_expiry_interval;
            session.expires_at_ms = expires_at_ms;
            session.next_packet_id = next_packet_id;
            session.last_seen_ms = last_seen_ms;
            snapshot.sessions.push(session);
        }
        RECORD_SUBSCRIPTION => {
            let filter = reader.string(limits.max_topic_bytes)?;
            let qos = reader.u8()?;
            let options = if format_version >= RECOVERY_VERSION_V4 {
                reader.u8()?
            } else {
                0
            };
            reader.finish()?;
            if qos > 2
                || options & 0xc3 != 0
                || (options >> 4) & 3 > 2
                || !valid_topic(&filter, limits, true)
            {
                return Err(Error::Invalid);
            }
            let subscription = Subscription {
                qos,
                no_local: options & 4 != 0,
                retain_as_published: options & 8 != 0,
                retain_handling: (options >> 4) & 3,
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.subscriptions.len() >= limits.max_subscriptions_per_session {
                return Err(Error::Overloaded);
            }
            if session.subscriptions.insert(filter, subscription).is_some() {
                return Err(Error::Invalid);
            }
        }
        RECORD_OFFLINE => {
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if !matches!(message.qos, 1 | 2) {
                return Err(Error::Invalid);
            }
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.offline.len() >= limits.max_offline_messages_per_session {
                return Err(Error::Overloaded);
            }
            session.offline_bytes = session
                .offline_bytes
                .checked_add(message.bytes())
                .ok_or(Error::Overloaded)?;
            session.offline.push_back(message);
        }
        RECORD_INBOUND_QOS2 => {
            let packet_id = reader.u16()?;
            let stage = reader.u8()?;
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if packet_id == 0 || message.qos != 2 {
                return Err(Error::Invalid);
            }
            let state = match stage {
                0 => InboundQos2State::AwaitPubrel(message),
                1 => InboundQos2State::EventAccepted(message),
                _ => return Err(Error::Invalid),
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            if session.inbound_qos2.len()
                + session
                    .outbound
                    .values()
                    .filter(|state| !matches!(state, OutboundState::AwaitPuback(_)))
                    .count()
                >= limits.max_inflight_qos2_per_session
            {
                return Err(Error::Overloaded);
            }
            if session.inbound_qos2.insert(packet_id, state).is_some() {
                return Err(Error::Invalid);
            }
        }
        RECORD_OUTBOUND => {
            let packet_id = reader.u16()?;
            let stage = reader.u8()?;
            let started = if format_version >= RECOVERY_VERSION_V5 {
                match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::Invalid),
                }
            } else {
                // v1-v4 did not record the transfer boundary. Conservatively retain
                // protocol responsibility for every recovered outbound exchange.
                true
            };
            let message = decode_message(&mut reader, limits, format_version)?;
            reader.finish()?;
            if packet_id == 0
                || (stage == 0 && message.qos != 1)
                || (matches!(stage, 1 | 2) && message.qos != 2)
            {
                return Err(Error::Invalid);
            }
            let state = match stage {
                0 => OutboundState::AwaitPuback(message),
                1 => OutboundState::AwaitPubrec(message),
                2 => OutboundState::AwaitPubcomp(message),
                _ => return Err(Error::Invalid),
            };
            let session = snapshot.sessions.last_mut().ok_or(Error::Invalid)?;
            let same_qos = session
                .outbound
                .values()
                .filter(|state| {
                    if stage == 0 {
                        matches!(state, OutboundState::AwaitPuback(_))
                    } else {
                        !matches!(state, OutboundState::AwaitPuback(_))
                    }
                })
                .count();
            let limit = if stage == 0 {
                limits.max_inflight_qos1_per_session
            } else {
                limits.max_inflight_qos2_per_session
            };
            if same_qos >= limit {
                return Err(Error::Overloaded);
            }
            if session.outbound.insert(packet_id, state).is_some() {
                return Err(Error::Invalid);
            }
            if started {
                session.started_outbound.insert(packet_id);
            }
            session.outbound_order.push_back(packet_id);
        }
        RECORD_PENDING_WILL => {
            if snapshot.pending_wills.len() >= limits.max_connections {
                return Err(Error::Overloaded);
            }
            let owner = DeviceKey {
                tenant_id: TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
                product_id: ProductId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
                device_id: DeviceId::new(reader.string(64)?).map_err(|_| Error::Invalid)?,
            };
            let message = decode_message(&mut reader, limits, format_version)?;
            let (due_at_ms, cancel_on_resume, message_expiry_interval) =
                if format_version >= RECOVERY_VERSION_V4 {
                    let (due_at_ms, cancel_on_resume) = match reader.u8()? {
                        0 => (None, None),
                        1 => {
                            let client_id = reader.string(limits.max_client_id_bytes)?;
                            let incarnation = reader.u64()?;
                            let due = reader.i64()?;
                            if incarnation == 0 || due < 0 {
                                return Err(Error::Invalid);
                            }
                            (
                                Some(due),
                                Some((
                                    SessionKey {
                                        device: owner.clone(),
                                        client_id,
                                    },
                                    incarnation,
                                )),
                            )
                        }
                        _ => return Err(Error::Invalid),
                    };
                    let expiry = match reader.u8()? {
                        0 => None,
                        1 => Some(reader.u32()?),
                        _ => return Err(Error::Invalid),
                    };
                    (due_at_ms, cancel_on_resume, expiry)
                } else {
                    (None, None, None)
                };
            let origin = if format_version >= RECOVERY_VERSION {
                match reader.u8()? {
                    0 => None,
                    1 => Some(SessionKey {
                        device: owner.clone(),
                        client_id: reader.string(limits.max_client_id_bytes)?,
                    }),
                    _ => return Err(Error::Invalid),
                }
            } else {
                cancel_on_resume.as_ref().map(|(key, _)| key.clone())
            };
            reader.finish()?;
            if message.payload.len() > limits.max_will_payload_bytes {
                return Err(Error::Invalid);
            }
            snapshot.pending_wills.push(PendingWill {
                owner,
                origin,
                message,
                due_at_ms,
                cancel_on_resume,
                message_expiry_interval,
                retained_reservation: RetainedReservation::default(),
            });
        }
        RECORD_RETAINED => {
            if snapshot.retained.len() >= limits.max_retained_messages {
                return Err(Error::Overloaded);
            }
            let tenant = TenantId::new(reader.string(64)?).map_err(|_| Error::Invalid)?;
            let message = decode_message(&mut reader, limits, format_version)?;
            let origin = if format_version >= RECOVERY_VERSION_V4 {
                match reader.u8()? {
                    0 => None,
                    1 => Some(SessionKey {
                        device: DeviceKey {
                            tenant_id: TenantId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                            product_id: ProductId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                            device_id: DeviceId::new(reader.string(64)?)
                                .map_err(|_| Error::Invalid)?,
                        },
                        client_id: reader.string(limits.max_client_id_bytes)?,
                    }),
                    _ => return Err(Error::Invalid),
                }
            } else {
                None
            };
            reader.finish()?;
            if !message.retain || message.payload.is_empty() {
                return Err(Error::Invalid);
            }
            let topic = message.topic.to_string();
            if snapshot
                .retained
                .iter()
                .any(|(existing, _)| existing == &topic)
            {
                return Err(Error::Invalid);
            }
            snapshot.retained.push((
                topic,
                RetainedMessage {
                    tenant_id: tenant,
                    message,
                    origin,
                },
            ));
        }
        _ => return Err(Error::Invalid),
    }
    Ok(())
}

impl Drop for TemporaryRecovery {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

impl<'a> RecordReader<'a> {
    pub(super) fn new(input: &'a [u8]) -> Self {
        Self { input, at: 0 }
    }

    pub(super) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(length).ok_or(Error::Invalid)?;
        let value = self.input.get(self.at..end).ok_or(Error::Invalid)?;
        self.at = end;
        Ok(value)
    }

    pub(super) fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().ok_or(Error::Invalid)?)
    }

    pub(super) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    pub(super) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    pub(super) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    pub(super) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Invalid)?,
        ))
    }

    pub(super) fn string(&mut self, maximum: usize) -> Result<String> {
        let length = usize::from(self.u16()?);
        if length == 0 || length > maximum {
            return Err(Error::Invalid);
        }
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| Error::Invalid)
    }

    pub(super) fn optional_string(&mut self, maximum: usize) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let length = usize::from(self.u16()?);
                if length > maximum {
                    return Err(Error::Invalid);
                }
                let bytes = self.take(length)?;
                Ok(Some(crate::mqtt::packet::valid_utf8(bytes)?.to_owned()))
            }
            _ => Err(Error::Invalid),
        }
    }

    pub(super) fn string_allow_empty(&mut self, maximum: usize) -> Result<String> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(Error::Invalid);
        }
        Ok(crate::mqtt::packet::valid_utf8(self.take(length)?)?.to_owned())
    }

    pub(super) fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>> {
        let length = usize::try_from(self.u32()?).map_err(|_| Error::Invalid)?;
        if length > maximum {
            return Err(Error::Invalid);
        }
        Ok(self.take(length)?.to_vec())
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.at == self.input.len() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }
}

pub(super) struct RecoveryWriteState {
    pub(super) written: usize,
    pub(super) record_count: u64,
    pub(super) record_bytes: u64,
    pub(super) stream_hash: Sha256,
}

pub(super) struct RecordReader<'a> {
    pub(super) input: &'a [u8],
    pub(super) at: usize,
}

pub(super) struct TemporaryRecovery(PathBuf);

impl MqttBroker {
    pub fn snapshot(&self) -> Result<MqttRecoverySnapshot> {
        let state = lock(&self.state)?;
        Ok(MqttRecoverySnapshot {
            format_version: RECOVERY_VERSION,
            snapshot_generation: state.generation,
            sessions: state
                .sessions
                .values()
                .cloned()
                .map(|mut session| {
                    session.active_generation = None;
                    session.inbound_operations.clear();
                    session.inbound_reservations.clear();
                    session
                })
                .collect(),
            retained: state
                .retained
                .iter()
                .map(|(topic, retained)| (topic.clone(), retained.clone()))
                .collect(),
            pending_wills: all_pending_wills(&state).cloned().collect(),
        })
    }

    pub fn bind_recovery_owner(&self, owner: Arc<recovery_io::RecoveryDirectory>) -> Result<()> {
        let mut slot = lock(&self.recovery_owner)?;
        if slot.is_some() {
            return Err(Error::Conflict);
        }
        *slot = Some(owner);
        Ok(())
    }

    pub async fn recover_from(&self, directory: &Path) -> Result<bool> {
        let directory = directory.to_path_buf();
        let limits = self.limits.clone();
        let owner = lock(&self.recovery_owner)?.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            let _owner = owner;
            read_recovery(&directory, &limits)
        })
        .await
        .map_err(|_| Error::Internal)??;
        if let Some(snapshot) = snapshot {
            self.restore(snapshot)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn commit_to(self: &Arc<Self>, directory: &Path) -> Result<PathBuf> {
        let directory = directory.to_path_buf();
        let limits = self.limits.clone();
        let broker = self.clone();
        tokio::task::spawn_blocking(move || write_recovery(&directory, &limits, &broker))
            .await
            .map_err(|_| Error::Internal)?
    }

    pub fn restore(&self, snapshot: MqttRecoverySnapshot) -> Result<()> {
        if !matches!(
            snapshot.format_version,
            RECOVERY_VERSION_V1
                | RECOVERY_VERSION_V2
                | RECOVERY_VERSION_V3
                | RECOVERY_VERSION_V4
                | RECOVERY_VERSION_V5
                | RECOVERY_VERSION
        ) || snapshot.sessions.len() > self.limits.max_persistent_sessions
            || snapshot.retained.len() > self.limits.max_retained_messages
        {
            return Err(Error::Configuration);
        }
        let mut replacement = BrokerState {
            sessions: HashMap::new(),
            session_usage: HashMap::new(),
            tenant_usage: HashMap::new(),
            device_subscription_count: HashMap::new(),
            session_expiry: DeadlineIndex::default(),
            message_expiry: DeadlineIndex::default(),
            retained_expiry: DeadlineIndex::default(),
            session_idle_ttl_ms: i64::try_from(self.limits.mqtt_session_idle_ttl_ms)
                .unwrap_or(i64::MAX),
            active: HashMap::new(),
            tenant_outbound_bytes: HashMap::new(),
            trie: SubscriptionTrie::default(),
            retained: HashMap::new(),
            retained_tenant_usage: HashMap::new(),
            generation: snapshot.snapshot_generation,
            operation_id: 0,
            subscription_count: 0,
            session_bytes: 0,
            offline_count: 0,
            offline_bytes: 0,
            retained_bytes: 0,
            retained_reserved_count: 0,
            retained_reserved_bytes: 0,
            retained_reserved_tenants: HashMap::new(),
            pending_wills: VecDeque::new(),
            future_wills: BTreeMap::new(),
            future_wills_by_session: HashMap::new(),
            next_will_token: 0,
            will_responsibility_count: 0,
            will_responsibility_bytes: 0,
            will_responsibility_tenants: HashMap::new(),
            pending_by_tenant: HashMap::new(),
            pending_global: BTreeMap::new(),
            pending_sessions: HashMap::new(),
            next_pending_token: 0,
            capacity_wakes: BTreeSet::new(),
        };
        for mut session in snapshot.sessions {
            if snapshot.format_version < RECOVERY_VERSION_V4 && session.version != MqttVersion::V311
            {
                return Err(Error::Invalid);
            }
            if session.version == MqttVersion::V5 {
                if session.session_expiry_interval == 0 {
                    continue;
                }
                if session.session_expiry_interval != u32::MAX {
                    let expiry = session.expires_at_ms.ok_or(Error::Invalid)?;
                    if expiry <= now_ms() {
                        continue;
                    }
                } else if session.expires_at_ms.is_some() {
                    return Err(Error::Invalid);
                }
            } else if session.expires_at_ms.is_some() || session.session_expiry_interval != 0 {
                return Err(Error::Invalid);
            }
            session.active_generation = None;
            session.inbound_operations.clear();
            session.inbound_reservations.clear();
            if session.incarnation == 0 {
                replacement.generation = replacement.generation.wrapping_add(1).max(1);
                session.incarnation = replacement.generation;
            }
            for entry in session.inbound_qos2.values_mut() {
                if let InboundQos2State::Delivering { message, .. } = entry {
                    *entry = InboundQos2State::AwaitPubrel(message.clone());
                }
            }
            if session.outbound_order.is_empty() && !session.outbound.is_empty() {
                let mut packet_ids = session.outbound.keys().copied().collect::<Vec<_>>();
                packet_ids.sort_unstable();
                session.outbound_order = packet_ids.into();
            }
            let ordered_ids = session
                .outbound_order
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>();
            if ordered_ids.len() != session.outbound_order.len()
                || ordered_ids.len() != session.outbound.len()
                || !session
                    .outbound
                    .keys()
                    .all(|packet_id| ordered_ids.contains(packet_id))
            {
                return Err(Error::Invalid);
            }
            if session.next_packet_id == 0
                || session.offline.iter().any(|message| {
                    !matches!(message.qos, 1 | 2) || !valid_broker_message(message, &self.limits)
                })
                || session.inbound_qos2.iter().any(|(packet_id, inbound)| {
                    *packet_id == 0
                        || match inbound {
                            InboundQos2State::AwaitPubrel(message)
                            | InboundQos2State::Delivering { message, .. }
                            | InboundQos2State::EventAccepted(message) => {
                                message.qos != 2 || !valid_broker_message(message, &self.limits)
                            }
                        }
                })
                || session.outbound.iter().any(|(packet_id, outbound)| {
                    *packet_id == 0
                        || match outbound {
                            OutboundState::AwaitPuback(message) => {
                                message.qos != 1 || !valid_broker_message(message, &self.limits)
                            }
                            OutboundState::AwaitPubrec(message)
                            | OutboundState::AwaitPubcomp(message) => {
                                message.qos != 2 || !valid_broker_message(message, &self.limits)
                            }
                        }
                })
            {
                return Err(Error::Invalid);
            }
            if let Some(authorization) = &session.authorization
                && (session.offline.iter().any(|message| {
                    !session_delivery_acl(
                        &session.key.device,
                        &authorization.permissions,
                        &message.topic,
                        &self.limits,
                    )
                }) || session.outbound.values().any(|outbound| {
                    let message = match outbound {
                        OutboundState::AwaitPuback(message)
                        | OutboundState::AwaitPubrec(message)
                        | OutboundState::AwaitPubcomp(message) => message,
                    };
                    !session_delivery_acl(
                        &session.key.device,
                        &authorization.permissions,
                        &message.topic,
                        &self.limits,
                    )
                }) || session.inbound_qos2.values().any(|inbound| {
                    let message = match inbound {
                        InboundQos2State::AwaitPubrel(message)
                        | InboundQos2State::Delivering { message, .. }
                        | InboundQos2State::EventAccepted(message) => message,
                    };
                    !device_publish_topic(
                        &session.key.device,
                        &message.topic,
                        Some(&authorization.permissions),
                    )
                }))
            {
                return Err(Error::Invalid);
            }
            if replacement.sessions.contains_key(&session.key) {
                return Err(Error::Invalid);
            }
            let recorded_offline_bytes =
                session.offline.iter().try_fold(0usize, |total, message| {
                    total.checked_add(message.bytes()).ok_or(Error::Overloaded)
                })?;
            if recorded_offline_bytes != session.offline_bytes
                || recorded_offline_bytes > self.limits.max_offline_bytes_per_session
            {
                return Err(Error::Invalid);
            }
            let now = now_ms();
            session.offline.retain(|message| !message.expired(now));
            session.offline_bytes = session.offline.iter().try_fold(0usize, |total, message| {
                total.checked_add(message.bytes()).ok_or(Error::Overloaded)
            })?;
            if !session
                .started_outbound
                .iter()
                .all(|id| session.outbound.contains_key(id))
            {
                return Err(Error::Invalid);
            }
            let expired_outbound = session
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
            for id in expired_outbound {
                session.remove_outbound(id);
            }
            let tenant_sessions = replacement
                .sessions
                .keys()
                .filter(|key| key.device.tenant_id == session.key.device.tenant_id)
                .count();
            let session_qos1 = session
                .outbound
                .values()
                .filter(|state| matches!(state, OutboundState::AwaitPuback(_)))
                .count();
            let session_qos2 = session.inbound_qos2.len()
                + session
                    .outbound
                    .values()
                    .filter(|state| !matches!(state, OutboundState::AwaitPuback(_)))
                    .count();
            if tenant_sessions >= self.limits.max_persistent_sessions_per_tenant
                || session.subscriptions.len() > self.limits.max_subscriptions_per_session
                || session.offline.len() > self.limits.max_offline_messages_per_session
                || session_qos1 > self.limits.max_inflight_qos1_per_session
                || session_qos2 > self.limits.max_inflight_qos2_per_session
                || tenant_inflight(&replacement, &session.key.device.tenant_id, 1)
                    .saturating_add(session_qos1)
                    > self.limits.max_inflight_qos1_per_tenant
                || tenant_inflight(&replacement, &session.key.device.tenant_id, 2)
                    .saturating_add(session_qos2)
                    > self.limits.max_inflight_qos2_per_tenant
            {
                return Err(Error::Overloaded);
            }
            let base = session.key.client_id.len()
                + session.key.device.tenant_id.as_str().len()
                + session.key.device.product_id.as_str().len()
                + session.key.device.device_id.as_str().len()
                + STATE_OVERHEAD;
            let subscription_bytes =
                session
                    .subscriptions
                    .keys()
                    .try_fold(0usize, |total, filter| {
                        total
                            .checked_add(filter.len() + STATE_OVERHEAD)
                            .ok_or(Error::Overloaded)
                    })?;
            let inbound_bytes =
                session
                    .inbound_qos2
                    .values()
                    .try_fold(0usize, |total, inbound| match inbound {
                        InboundQos2State::AwaitPubrel(message)
                        | InboundQos2State::Delivering { message, .. }
                        | InboundQos2State::EventAccepted(message) => {
                            total.checked_add(message.bytes()).ok_or(Error::Overloaded)
                        }
                    })?;
            let outbound_bytes = session
                .outbound
                .values()
                .try_fold(0usize, |total, outbound| {
                    total.checked_add(outbound.bytes()).ok_or(Error::Overloaded)
                })?;
            session.state_bytes = base
                .checked_add(subscription_bytes)
                .and_then(|value| value.checked_add(session.offline_bytes))
                .and_then(|value| value.checked_add(inbound_bytes))
                .and_then(|value| value.checked_add(outbound_bytes))
                .ok_or(Error::Overloaded)?;
            if session.state_bytes > self.limits.max_mqtt_session_state_bytes {
                return Err(Error::Overloaded);
            }
            if tenant_session_bytes(&replacement, &session.key.device.tenant_id)
                .saturating_add(session.state_bytes)
                > self.limits.max_mqtt_session_state_bytes_per_tenant
            {
                return Err(Error::Overloaded);
            }
            replacement.subscription_count = replacement
                .subscription_count
                .checked_add(session.subscriptions.len())
                .ok_or(Error::Overloaded)?;
            replacement.offline_count = replacement
                .offline_count
                .checked_add(session.offline.len())
                .ok_or(Error::Overloaded)?;
            replacement.offline_bytes = replacement
                .offline_bytes
                .checked_add(session.offline_bytes)
                .ok_or(Error::Overloaded)?;
            replacement.session_bytes = replacement
                .session_bytes
                .checked_add(session.state_bytes)
                .ok_or(Error::Overloaded)?;
            for (filter, subscription) in &session.subscriptions {
                if !valid_topic(filter, &self.limits, true)
                    || subscription.qos > 2
                    || subscription.retain_handling > 2
                    || session.authorization.as_ref().is_some_and(|authorization| {
                        !session_subscribe_acl(
                            &session.key.device,
                            &authorization.permissions,
                            filter,
                            &self.limits,
                        )
                    })
                {
                    return Err(Error::Invalid);
                }
                if session
                    .authorization
                    .as_ref()
                    .is_some_and(authorization_complete)
                {
                    replacement
                        .trie
                        .insert(filter, session.key.clone(), *subscription);
                }
            }
            let key = session.key.clone();
            replacement.sessions.insert(key.clone(), session);
            sync_session_usage(&mut replacement, &key)?;
        }
        for (topic, retained) in snapshot.retained {
            if topic.as_str() != retained.message.topic.as_ref()
                || !valid_broker_message(&retained.message, &self.limits)
                || !retained_topic_owner_acl(&retained.tenant_id, &topic)
                || retained.origin.as_ref().is_some_and(|origin| {
                    origin.device.tenant_id != retained.tenant_id
                        || origin.client_id.len() > self.limits.max_client_id_bytes
                        || !device_publish_topic(&origin.device, &topic, None)
                })
                || !retained.message.retain
                || retained.message.payload.is_empty()
                || retained.message.payload.len() > self.limits.max_retained_message_bytes
            {
                return Err(Error::Invalid);
            }
            if retained.message.expired(now_ms()) {
                continue;
            }
            let (tenant_count, tenant_bytes) = replacement
                .retained_tenant_usage
                .get(&retained.tenant_id)
                .copied()
                .unwrap_or_default();
            if tenant_count >= self.limits.max_retained_messages_per_tenant
                || tenant_bytes.saturating_add(retained.bytes())
                    > self.limits.max_retained_bytes_per_tenant
            {
                return Err(Error::Overloaded);
            }
            replacement.retained_bytes = replacement
                .retained_bytes
                .checked_add(retained.bytes())
                .ok_or(Error::Overloaded)?;
            retained_usage_add(&mut replacement, &retained)?;
            let deadline = retained.message.properties.expires_at_ms;
            replacement.retained.insert(topic.clone(), retained);
            replacement.retained_expiry.update(topic, deadline);
        }
        for mut pending in snapshot.pending_wills {
            if !valid_broker_message(&pending.message, &self.limits)
                || pending.message.payload.len() > self.limits.max_will_payload_bytes
                || !device_publish_topic(&pending.owner, &pending.message.topic, None)
                || pending.origin.as_ref().is_some_and(|key| {
                    key.device != pending.owner
                        || key.client_id.len() > self.limits.max_client_id_bytes
                })
                || pending.due_at_ms.is_some() != pending.cancel_on_resume.is_some()
                || pending
                    .cancel_on_resume
                    .as_ref()
                    .is_some_and(|(key, incarnation)| {
                        key.device != pending.owner
                            || *incarnation == 0
                            || key.client_id.len() > self.limits.max_client_id_bytes
                    })
            {
                return Err(Error::Invalid);
            }
            if pending.message.expired(now_ms()) {
                continue;
            }
            reserve_will_capacity(
                &mut replacement,
                &pending.owner.tenant_id,
                pending.bytes(),
                &self.limits,
            )?;
            pending.retained_reservation = reserve_retained(
                &mut replacement,
                &pending.owner.tenant_id,
                pending.origin.as_ref(),
                &pending.message,
                &self.limits,
            )?;
            insert_pending_will(&mut replacement, pending);
        }
        let reservations = replacement
            .sessions
            .values()
            .flat_map(|session| {
                session
                    .inbound_qos2
                    .keys()
                    .map(|packet_id| (session.key.clone(), *packet_id))
            })
            .collect::<Vec<_>>();
        for (key, packet_id) in reservations {
            let message = match replacement
                .sessions
                .get(&key)
                .and_then(|session| session.inbound_qos2.get(&packet_id))
                .ok_or(Error::Internal)?
            {
                InboundQos2State::AwaitPubrel(message)
                | InboundQos2State::Delivering { message, .. }
                | InboundQos2State::EventAccepted(message) => message.clone(),
            };
            let reservation = reserve_retained(
                &mut replacement,
                &key.device.tenant_id,
                Some(&key),
                &message,
                &self.limits,
            )?;
            if reservation != RetainedReservation::default() {
                replacement
                    .sessions
                    .get_mut(&key)
                    .ok_or(Error::Internal)?
                    .inbound_reservations
                    .insert(packet_id, reservation);
            }
        }
        if replacement.subscription_count > self.limits.max_subscriptions
            || replacement.offline_count > self.limits.max_offline_messages
            || replacement.offline_bytes > self.limits.max_offline_bytes
            || replacement.session_bytes > self.limits.global_mqtt_session_bytes
            || replacement.retained_bytes > self.limits.max_retained_bytes
        {
            return Err(Error::Overloaded);
        }
        retry_pending_wills(&mut replacement, &self.limits);
        let mut state = lock(&self.state)?;
        *state = replacement;
        self.publish_subscription_count(&state);
        Ok(())
    }
}
