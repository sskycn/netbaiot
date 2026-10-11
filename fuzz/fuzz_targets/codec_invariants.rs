use netbaiot_core::*;

fn text(value: &str, limit: usize) {
    assert!(value.len() <= limit);
    assert!(!value.chars().any(char::is_control));
}

fn scalar(value: &Scalar, limit: usize) {
    match value {
        Scalar::Number(n) => assert!(n.is_finite()),
        Scalar::Text(s) => text(s, limit),
        Scalar::Boolean(_) => {}
    }
}

pub fn check(codec: &dyn DeviceCodec, limits: &CodecLimits, data: &[u8]) {
    let key = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext { device: &key, received_at: 1234 };
    let decoded = codec.decode(&ctx, data);
    let validated = codec.validate_payload(&ctx, data);
    assert_eq!(decoded.is_ok(), validated.is_ok());
    if let Ok(events) = decoded {
        let kinds = validated.unwrap();
        assert_eq!(events.len(), 1);
        assert!(events.len() <= limits.output_messages);
        assert_eq!(kinds.len(), events.len());
        for (event, kind) in events.iter().zip(kinds) {
            assert_eq!(event.device, key);
            assert_eq!(event.received_at, ctx.received_at);
            assert!(event.occurred_at.is_none_or(|t| t >= 0));
            assert!(!event.source_message_id.as_str().is_empty());
            text(event.source_message_id.as_str(), 64);
            assert_eq!(event.kind, kind);
            match &event.kind {
                DeviceEventKind::Telemetry(fields) => {
                    assert!(!fields.is_empty());
                    assert!(fields.len() <= limits.fields);
                    for (name, value) in fields {
                        assert!(!name.is_empty());
                        text(name, limits.field_bytes);
                        scalar(value, limits.field_bytes);
                    }
                }
                DeviceEventKind::DeviceEvent(e) => {
                    assert!(!e.name.is_empty());
                    text(&e.name, limits.field_bytes);
                    if let Some(v) = &e.value { scalar(v, limits.field_bytes); }
                }
                DeviceEventKind::Heartbeat(_) => {}
                DeviceEventKind::CommandAck(a) => assert_ne!(a.execution, ExecutionState::Unknown),
            }
        }
        // A second parse preserves semantic fields but creates a fresh gateway ID.
        let again = codec.decode(&ctx, data).unwrap();
        assert_ne!(again[0].event_id, events[0].event_id);
        assert_eq!(again[0].source_message_id, events[0].source_message_id);
        assert_eq!(again[0].occurred_at, events[0].occurred_at);
        assert_eq!(again[0].kind, events[0].kind);
    }
}
