//! Ignored, serial release-only codec safety-scan measurements.
use super::*;
use std::{hint::black_box, time::Instant};

#[test]
#[ignore = "serial second-round release measurement"]
fn second_round_json() {
    if std::env::var_os("NETBAIOT_SECOND_ROUND").is_none() {
        return;
    }
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    let codec = JsonV1::default();
    for size in [
        64,
        256,
        1024,
        4096,
        16384,
        CodecLimits::default().input_bytes,
    ] {
        for shape in ["flat", "deep", "many_members", "invalid"] {
            let data = match shape {
                "deep" => "{\"k\":{\"k\":{\"value\":1}}}".to_owned(),
                "many_members" => format!(
                    "{{{}}}",
                    (0..64)
                        .map(|index| format!("\"k{index}\":{index}"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                _ => "{\"value\":1}".to_owned(),
            };
            let mut payload = format!("{{\"schema_version\":1,\"source_message_id\":\"bench\",\"kind\":\"telemetry\",\"data\":{data}}}").into_bytes();
            // 64B cannot hold this complete envelope. Truncations are labelled by
            // actual codec acceptance; padding exercises the legal input ceiling.
            payload.resize(size, b' ');
            if shape == "invalid" {
                payload[0] = 0xff;
            }
            let valid = codec.parse_payload(&payload).is_ok();
            for operation in ["depth", "members", "guards", "serde", "codec"] {
                for run in 1..=3 {
                    let mut samples = Vec::with_capacity(256);
                    for _ in 0..256 {
                        let began = Instant::now();
                        for _ in 0..8 {
                            let input = black_box(payload.as_slice());
                            black_box(match operation {
                                "depth" => {
                                    check_json_depth(input, codec.limits.nesting_depth).is_ok()
                                }
                                "members" => {
                                    check_members(input, codec.limits.fields.saturating_add(8))
                                        .is_ok()
                                }
                                "guards" => check_payload_bounds(
                                    input,
                                    codec.limits.nesting_depth,
                                    codec.limits.fields.saturating_add(8),
                                )
                                .is_ok(),
                                "serde" => serde_json::from_slice::<WireMessage<'_>>(input).is_ok(),
                                _ => codec.parse_payload(input).is_ok(),
                            });
                        }
                        samples.push(began.elapsed().as_nanos() / 8);
                    }
                    samples.sort_unstable();
                    println!(
                        "SECOND_JSON,{shape},{size},{operation},{run},{:.3},{},{},{},{},{valid}",
                        samples.iter().sum::<u128>() as f64 / 256.,
                        samples[128],
                        samples[243],
                        samples[253],
                        samples[255]
                    );
                }
            }
        }
    }
}
