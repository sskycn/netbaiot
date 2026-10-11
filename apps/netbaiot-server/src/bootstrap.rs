use super::*;

pub(crate) fn bootstrap_snapshot(config: &Config, sink_id: SinkId) -> Result<ControlSnapshot> {
    let mut products = HashMap::new();
    for credential in &config.credentials {
        let identity = &credential.identity;
        let key = (
            identity.device_key.tenant_id.clone(),
            identity.device_key.product_id.clone(),
        );
        if let Some(existing) = products.get(&key) {
            let existing: &ProductRuntimeConfig = existing;
            if existing.codec_id != identity.codec_id
                || existing.codec_version != identity.codec_version
            {
                return Err(Error::Configuration);
            }
        } else {
            products.insert(
                key,
                ProductRuntimeConfig {
                    tenant_id: identity.device_key.tenant_id.clone(),
                    product_id: identity.device_key.product_id.clone(),
                    codec_id: identity.codec_id.clone(),
                    codec_version: identity.codec_version,
                    revision: 1,
                },
            );
        }
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
    CodecRegistry::new(netbaiot_codecs::builtins(codec_limits).map_err(|_| Error::Configuration)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_and_diagnostics_share_all_builtins_and_reject_product_conflicts() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../configs/multi-codec.json")).unwrap();
        config.validate().unwrap();
        let registry = codec_registry(&config.limits).unwrap();
        for credential in &config.credentials {
            assert!(registry.get(&credential.identity).is_ok());
        }
        assert_eq!(
            bootstrap_snapshot(&config, SinkId::new("sink").unwrap())
                .unwrap()
                .products
                .len(),
            4
        );
        config.credentials[1].identity.device_key.product_id =
            config.credentials[0].identity.device_key.product_id.clone();
        assert!(config.validate().is_err());
        assert!(bootstrap_snapshot(&config, SinkId::new("sink").unwrap()).is_err());
        let diagnostics = config.diagnostics();
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("conflicting codec"))
        );
    }
}
