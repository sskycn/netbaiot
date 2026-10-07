use super::*;

fn shared_fixture() -> BrokerMessage {
    BrokerMessage {
        topic: "v1/t/t/p/p/d/d/up".into(),
        payload: vec![0, 255].into(),
        qos: 2,
        retain: true,
        properties: PublishProperties {
            payload_format: Some(0),
            expires_at_ms: Some(42),
            content_type: Some("application/json".into()),
            response_topic: Some("v1/t/t/p/p/d/d/down_ack".into()),
            correlation_data: Some(vec![1, 2]),
            user_properties: vec![("k".into(), "v".into()), ("k".into(), "two".into())],
        }
        .into(),
    }
}

#[test]
fn shared_metadata_preserves_historical_json_and_compact_message_bytes() {
    let message = shared_fixture();
    let expected = r#"{"topic":"v1/t/t/p/p/d/d/up","payload":[0,255],"qos":2,"retain":true,"properties":{"payload_format":0,"expires_at_ms":42,"content_type":"application/json","response_topic":"v1/t/t/p/p/d/d/down_ack","correlation_data":[1,2],"user_properties":[["k","v"],["k","two"]]}}"#;
    assert_eq!(serde_json::to_string(&message).unwrap(), expected);
    assert_eq!(
        serde_json::from_str::<BrokerMessage>(expected).unwrap(),
        message
    );
    let mut encoded = Vec::new();
    encode_message(&mut encoded, &message).unwrap();
    assert_eq!(
        Sha256::digest(&encoded).as_slice(),
        &[
            243, 166, 69, 165, 216, 109, 191, 195, 25, 48, 85, 181, 6, 156, 76, 241, 201, 108, 100,
            187, 165, 109, 22, 212, 219, 128, 212, 133, 226, 168, 6, 65
        ]
    );
}

#[test]
fn metadata_sharing_preserves_logical_charge_and_copy_on_write_isolation() {
    let original = shared_fixture();
    let mut copy = original.clone();
    assert!(Arc::ptr_eq(&original.topic, &copy.topic));
    assert!(Arc::ptr_eq(
        original.properties.0.as_ref().unwrap(),
        copy.properties.0.as_ref().unwrap()
    ));
    assert_eq!(original.bytes(), copy.bytes());
    copy.properties
        .user_properties
        .push(("new".into(), "value".into()));
    copy.properties.expires_at_ms = None;
    assert_eq!(original.properties.user_properties.len(), 2);
    assert_eq!(original.properties.expires_at_ms, Some(42));
    assert!(copy.bytes() > original.bytes());
    let empty = SharedPublishProperties::default();
    assert!(empty.0.is_none());
    let mut allocated_empty = empty.clone();
    allocated_empty.expires_at_ms = None;
    assert_eq!(empty, allocated_empty);
    assert_eq!(
        serde_json::to_vec(&empty).unwrap(),
        serde_json::to_vec(&PublishProperties::default()).unwrap()
    );
}
