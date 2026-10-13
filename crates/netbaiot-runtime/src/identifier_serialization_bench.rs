// Included within event::tests. Payload construction and output storage are
// outside measurement; the sink counts bytes without an output allocation.
#[test]
fn event_json_accounting_matches_wire_bytes_and_enforces_the_limit() {
    for payload in [8, 64, 8_192] {
        let event = event(payload);
        let expected = serde_json::to_vec(&event).unwrap();
        assert_eq!(
            bounded_json_bytes(&event, expected.len()).unwrap(),
            expected.len()
        );
        assert!(matches!(
            bounded_json_bytes(&event, expected.len() - 1),
            Err(Error::Overloaded)
        ));
    }
}

#[test]
#[ignore = "isolated serial release identifier serialization allocation measurement"]
fn identifier_serialization_hotspot() {
    use crate::hotspot_bench::measure;
    for length in [1, 16, MAX_IDENTIFIER_BYTES] {
        let identifier = SinkId::new("a".repeat(length)).unwrap();
        measure(
            "identifier_json_count",
            length,
            4_000,
            || (),
            |_| bounded_json_bytes(&identifier, usize::MAX).unwrap(),
            |_| {},
        );
    }
    for payload in [8, 64, 8_192] {
        let event = event(payload);
        measure(
            "event_json_count",
            payload,
            4_000,
            || (),
            |_| bounded_json_bytes(&event, usize::MAX).unwrap(),
            |_| {},
        );
        let ids = [
            SinkId::new("required-a").unwrap(),
            SinkId::new("required-b").unwrap(),
        ];
        let record = SpoolRecord {
            event,
            pending_sinks: ids.to_vec(),
            routing_revision: 1,
            accepted_at: 1,
            attempts: BTreeMap::from([(ids[0].clone(), 1), (ids[1].clone(), 3)]),
        };
        measure(
            "spool_json_count",
            payload,
            4_000,
            || (),
            |_| record.encoded_len().unwrap(),
            |_| {},
        );
    }
}
