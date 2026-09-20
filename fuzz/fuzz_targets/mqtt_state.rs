#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::*;
use netbaiot_runtime::Limits;
use netbaiot_transports::mqtt::broker::{BrokerMessage, MqttBroker};
use std::sync::Arc;

fn auth() -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}

fuzz_target!(|data: &[u8]| {
    let limits = Arc::new(Limits::default());
    let device = auth();
    let broker = MqttBroker::new(limits.clone());
    let mut attachment = match broker.attach(&device, "client".into(), false) {
        Ok(value) => value,
        Err(_) => return,
    };
    for chunk in data.chunks(4).take(256) {
        let operation = chunk.first().copied().unwrap_or(0) % 9;
        let packet_id = u16::from_be_bytes([
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(1),
        ])
        .max(1);
        let qos = chunk.get(3).copied().unwrap_or(0) % 3;
        let message = BrokerMessage {
            topic: "v1/t/t/p/p/d/d/up".into(),
            payload: chunk.to_vec(),
            qos,
            retain: operation == 8,
        };
        match operation {
            0 => {
                let _ = broker.subscribe(
                    &attachment.key,
                    attachment.generation,
                    "v1/t/t/p/p/d/d/#",
                    qos,
                );
            }
            1 => {
                let _ = broker.unsubscribe(
                    &attachment.key,
                    attachment.generation,
                    "v1/t/t/p/p/d/d/#",
                );
            }
            2 => {
                let _ = broker.inbound_qos2(
                    &attachment.key,
                    attachment.generation,
                    packet_id,
                    message,
                );
            }
            3 => {
                let _ = broker.inbound_qos2_message(
                    &attachment.key,
                    attachment.generation,
                    packet_id,
                );
                let _ = broker.complete_inbound_qos2(
                    &attachment.key,
                    attachment.generation,
                    packet_id,
                );
            }
            4 | 8 => {
                let _ = broker.route(&device.device_key, message);
            }
            5 => {
                while let Ok(frame) = attachment.receiver.try_recv() {
                    if let netbaiot_transports::mqtt::broker::BrokerFrame::Publish(delivery) = frame
                        && let Some(id) = delivery.packet_id
                    {
                        if delivery.message.qos == 1 {
                            let _ = broker.puback(&attachment.key, attachment.generation, id);
                        } else {
                            let _ = broker.pubrec(&attachment.key, attachment.generation, id);
                            let _ = broker.pubcomp(&attachment.key, attachment.generation, id);
                        }
                    }
                }
            }
            6 => {
                let _ = broker.detach(&attachment.key, attachment.generation, false);
                if let Ok(next) = broker.attach(&device, "client".into(), false) {
                    attachment = next;
                }
            }
            _ => {
                if let Ok(snapshot) = broker.snapshot() {
                    let restored = MqttBroker::new(limits.clone());
                    let _ = restored.restore(snapshot);
                }
            }
        }
        if let Ok((sessions, bytes, subscriptions, retained, retained_bytes)) = broker.usage() {
            assert!(sessions <= limits.max_persistent_sessions);
            assert!(bytes <= limits.global_mqtt_session_bytes);
            assert!(subscriptions <= limits.max_subscriptions);
            assert!(retained <= limits.max_retained_messages);
            assert!(retained_bytes <= limits.max_retained_bytes);
        }
    }
});
