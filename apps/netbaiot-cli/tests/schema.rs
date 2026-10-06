#[test]
fn generated_schema_matches_examples_and_rejects_unknown_types_and_invalid_limits() {
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../docs/schema/netbaiot-config.schema.json"
    ))
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs");
    let mut count = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if value.get("device_ingress").is_none() {
            continue;
        } // resource-limits.json is a Limits reference, not a Config
        assert!(
            validator.is_valid(&value),
            "schema rejects {}",
            path.display()
        );
        count += 1;
    }
    assert!(count >= 2);
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    config["unknown"] = true.into();
    assert!(!validator.is_valid(&config));
    config.as_object_mut().unwrap().remove("unknown");
    config["development"] = 7.into();
    assert!(!validator.is_valid(&config));
    config["development"] = true.into();
    config["limits"]["max_connections"] = 0.into();
    assert!(!validator.is_valid(&config));
    config["limits"]["max_connections"] = 256.into();
    config.as_object_mut().unwrap().remove("device_ingress");
    assert!(!validator.is_valid(&config));
    assert_eq!(
        schema.pointer("/$defs/Limits/properties/max_connections/default"),
        Some(&serde_json::json!(
            netbaiot_runtime::Limits::default().max_connections
        ))
    );
}
