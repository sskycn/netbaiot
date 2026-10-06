use super::*;

pub(crate) fn bootstrap_snapshot(config: &Config, sink_id: SinkId) -> Result<ControlSnapshot> {
    let mut products = HashMap::new();
    for credential in &config.credentials {
        let identity = &credential.identity;
        products
            .entry((
                identity.device_key.tenant_id.clone(),
                identity.device_key.product_id.clone(),
            ))
            .or_insert(ProductRuntimeConfig {
                tenant_id: identity.device_key.tenant_id.clone(),
                product_id: identity.device_key.product_id.clone(),
                codec_id: identity.codec_id.clone(),
                codec_version: identity.codec_version,
                revision: 1,
            });
    }
    Ok(ControlSnapshot {
        revision: 1,
        products: products.into_values().collect(),
        routes: vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
    })
}

pub(crate) fn codec_registry(limits: &Limits) -> Result<CodecRegistry> {
    let codec_limits = CodecLimits {
        input_bytes: limits
            .max_http_body_size
            .max(limits.max_mqtt_packet_size)
            .max(limits.max_tcp_frame_size),
        decoded_bytes: limits
            .max_http_body_size
            .max(limits.max_mqtt_packet_size)
            .max(limits.max_tcp_frame_size),
        ..CodecLimits::default()
    };
    CodecRegistry::new(vec![(
        CodecId::new("netbaiot-json").map_err(|_| Error::Configuration)?,
        1,
        Arc::new(JsonV1::new(codec_limits)),
    )])
}
