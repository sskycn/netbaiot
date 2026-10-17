pub mod common;
pub mod v311;
pub mod v5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MqttVersion {
    #[default]
    V311,
    V5,
}

/// Command payload capacity for any negotiated subscription QoS. Reserve the
/// packet identifier even at QoS0: queued commands may outlive a resubscription.
/// MQTT 5 commands carry one Message Expiry property (five bytes plus length).
/// No payload allocation or encoding is needed to establish the session limit.
pub(super) fn command_payload_limit(
    topic: &str,
    maximum: usize,
    version: MqttVersion,
    limits: &netbaiot_runtime::Limits,
) -> usize {
    if u16::try_from(topic.len()).is_err() || !common::valid_topic(topic, limits, false) {
        return 0;
    }
    let Some(overhead) = topic
        .len()
        .checked_add(if version == MqttVersion::V5 { 10 } else { 4 })
    else {
        return 0;
    };
    let mut low = 0;
    let mut high = maximum.min(268_435_455).saturating_sub(overhead);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if common::encoded_size(overhead + mid).is_ok_and(|size| size <= maximum) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

#[cfg(test)]
mod command_capacity_tests {
    use super::*;
    use netbaiot_runtime::Limits;

    #[test]
    fn command_topic_limits_are_checked_before_payload_admission() {
        let limits = Limits {
            max_topic_bytes: 8,
            ..Default::default()
        };
        for version in [MqttVersion::V311, MqttVersion::V5] {
            assert_eq!(
                command_payload_limit("too/long/topic", 65536, version, &limits),
                0
            );
            assert_eq!(command_payload_limit("a/+", 65536, version, &limits), 0);
            assert_eq!(command_payload_limit("a", 0, version, &limits), 0);
        }
    }

    #[test]
    fn payload_capacity_matches_wire_encoder_at_remaining_length_boundaries() {
        let topic = "v1/t/t/p/acme/d/mqtt/down";
        for version in [MqttVersion::V311, MqttVersion::V5] {
            for maximum in [
                31, 32, 63, 127, 128, 129, 130, 255, 256, 16_383, 16_384, 16_385, 65_536,
            ] {
                let capacity = command_payload_limit(topic, maximum, version, &Limits::default());
                for size in [capacity, capacity + 1] {
                    let payload = vec![0; size];
                    let encoded = match version {
                        MqttVersion::V311 => v311::publish(
                            topic,
                            &payload,
                            1,
                            Some(1),
                            false,
                            false,
                            &Limits {
                                max_mqtt_packet_size: maximum,
                                ..Default::default()
                            },
                        ),
                        MqttVersion::V5 => v5::publish(
                            v5::OutboundPublish {
                                topic,
                                payload: &payload,
                                qos: 1,
                                packet_id: Some(1),
                                retain: false,
                                dup: false,
                                properties: &v5::Properties {
                                    message_expiry: Some(1),
                                    ..Default::default()
                                },
                            },
                            maximum,
                        ),
                    };
                    if capacity > 0 {
                        assert_eq!(
                            encoded.is_ok(),
                            size == capacity,
                            "{version:?} {maximum} {size}"
                        );
                    } else if size > 0 {
                        assert!(encoded.is_err());
                    }
                }
            }
        }
    }
}
