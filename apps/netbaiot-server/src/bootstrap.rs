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

#[cfg(test)]
pub(crate) fn codec_registry(limits: &Limits) -> Result<CodecRegistry> {
    configured_codec_registry(limits, &[], &[])
}

pub(crate) fn configured_codec_registry(
    limits: &Limits,
    plans: &[netbaiot_codecs::vendor::declarative::DeclarativeConfig],
    native: &[netbaiot_codecs::CodecRegistration],
) -> Result<CodecRegistry> {
    use netbaiot_codecs::vendor::{declarative::DeclarativeCodec, registration, vendor_id};
    if plans.len().saturating_add(native.len()) > 60 {
        return Err(Error::Configuration);
    }
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
    let mut entries =
        netbaiot_codecs::builtins(codec_limits.clone()).map_err(|_| Error::Configuration)?;
    for (id, version, codec) in native {
        vendor_id(id.as_str(), *version).map_err(|_| Error::Configuration)?;
        entries.push((id.clone(), *version, codec.clone()));
    }
    for plan in plans {
        let codec = DeclarativeCodec::compile(plan, codec_limits.clone())
            .map_err(|_| Error::Configuration)?;
        entries.push(
            registration(&plan.id, plan.version, Arc::new(codec))
                .map_err(|_| Error::Configuration)?,
        );
    }
    CodecRegistry::new(entries)
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

#[cfg(test)]
mod vendor_tests {
    use super::*;
    use netbaiot_codecs::vendor::{demo::AcmeV1, registration};
    #[test]
    fn vendor_profiles_validate_and_native_versions_are_explicit() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        config.validate().unwrap();
        let registry = configured_codec_registry(
            &config.limits,
            &config.payload_codecs,
            &config.native_codecs,
        )
        .unwrap();
        assert!(registry.get(&config.credentials[0].identity).is_ok());
        let mut unsupported = config.credentials[0].identity.clone();
        unsupported.codec_version = 2;
        assert!(registry.get(&unsupported).is_err());
        let native = registration(
            "vendor-acme",
            2,
            Arc::new(AcmeV1::new(CodecLimits::default()).unwrap()),
        )
        .unwrap();
        config.native_codecs.push(native.clone());
        config.validate().unwrap();
        config.credentials[0].identity.codec_version = 2;
        config.validate().unwrap();
        config.native_codecs.push(native);
        assert!(config.validate().is_err());
        config.native_codecs.pop();
        config.payload_codecs[0].messages[0].size = 1;
        assert!(
            config
                .diagnostics()
                .iter()
                .any(|d| d.path == "payload_codecs")
        );
    }
    #[test]
    fn declarations_cannot_replace_builtins_or_load_native_code() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        config.payload_codecs[0].id = "netbaiot-json".into();
        assert!(config.validate().is_err());
        let mut json = serde_json::to_value(&config).unwrap();
        json["native_codecs"] = serde_json::json!([]);
        assert!(serde_json::from_value::<Config>(json).is_err());
        config.payload_codecs[0].id = "vendor-acme".into();
        config.payload_codecs = vec![config.payload_codecs[0].clone(); 61];
        assert!(config.validate().is_err());
    }
}
