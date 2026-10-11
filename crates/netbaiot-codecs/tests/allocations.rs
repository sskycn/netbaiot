//! Single-threaded isolated allocation matrix. Counts cumulative heap allocation,
//! not peak live heap, stack usage, allocator overhead or process RSS.
use netbaiot_codecs::{CborV1, JsonV1, MsgpackV1, ProtobufV1, protobuf::v1::wire};
use netbaiot_core::*;
use prost::Message;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
fn measured_binary_structure_matrix_and_early_rejection() {
    let key = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext {
        device: &key,
        received_at: 1,
    };
    let limits = CodecLimits::default();
    let codecs: [(&str, Box<dyn DeviceCodec>); 3] = [
        ("cbor", Box::new(CborV1::default())),
        ("msgpack", Box::new(MsgpackV1::default())),
        ("protobuf", Box::new(ProtobufV1::default())),
    ];
    println!("codec,fields,text_bytes,result,allocations,reallocations,cumulative_allocated_bytes");
    for count in [1, 2, 16, 64, 65] {
        for width in [0, 16, 128, 256, 257] {
            let fields: serde_json::Map<String, serde_json::Value> = (0..count)
                .map(|i| {
                    (
                        format!("f{i}"),
                        if width == 0 {
                            serde_json::json!(1.5)
                        } else {
                            serde_json::json!("x".repeat(width))
                        },
                    )
                })
                .collect();
            let value = serde_json::json!({"schema_version":1,"source_message_id":"allocation:1","kind":"telemetry","data":fields});
            let mut cbor = Vec::new();
            ciborium::into_writer(&value, &mut cbor).unwrap();
            let msgpack = rmp_serde::to_vec_named(&value).unwrap();
            let protobuf = wire::Uplink {
                schema_version: 1,
                source_message_id: "allocation:1".into(),
                occurred_at: None,
                kind: Some(wire::uplink::Kind::Telemetry(wire::Telemetry {
                    fields: (0..count)
                        .map(|i| wire::Field {
                            name: format!("f{i}"),
                            value: Some(wire::Scalar {
                                value: Some(if width == 0 {
                                    wire::scalar::Value::Number(1.5)
                                } else {
                                    wire::scalar::Value::Text("x".repeat(width))
                                }),
                            }),
                        })
                        .collect(),
                })),
            }
            .encode_to_vec();
            for ((name, codec), input) in codecs.iter().zip([cbor, msgpack, protobuf]) {
                let region = Region::new(ALLOCATOR);
                let result = codec.decode(&ctx, &input);
                let stats = region.change();
                println!(
                    "{name},{count},{width},{},{},{},{}",
                    result.is_ok(),
                    stats.allocations,
                    stats.reallocations,
                    stats.bytes_allocated
                );
                if let Ok(events) = result {
                    assert!(count <= limits.fields && width <= limits.field_bytes);
                    assert_eq!(events.len(), 1);
                    // Evidence for this matrix, not a universal allocator/RSS proof.
                    assert!(stats.bytes_allocated <= limits.decoded_bytes);
                }
            }
        }
    }
    for ((_, codec), invalid) in codecs.iter().zip([
        vec![0xbb, 255, 255, 255, 255, 255, 255, 255, 255],
        vec![0xdf, 255, 255, 255, 255],
        vec![0x12, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255],
    ]) {
        let region = Region::new(ALLOCATOR);
        assert!(codec.decode(&ctx, &invalid).is_err());
        assert_eq!(
            region.change().allocations,
            0,
            "length overflow must fail before owned decoding"
        );
    }
    // Output is stopped within the configured bound even if individual strings fit.
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: key.clone(),
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: (0..64)
                .map(|i| (format!("f{i}"), Scalar::Text("x".repeat(256))))
                .collect(),
        },
    };
    let codec = JsonV1::new(CodecLimits {
        decoded_bytes: 256,
        ..limits
    });
    let region = Region::new(ALLOCATOR);
    assert!(
        codec
            .encode(&EncodeContext { device: &key }, &command)
            .is_err()
    );
    let stats = region.change();
    assert!(
        stats.bytes_allocated < 2048,
        "must not allocate the complete 16 KiB command"
    );
}
