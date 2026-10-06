fn main() -> std::process::ExitCode {
    let mut schema = schemars::schema_for!(netbaiot_server::Config).to_value();
    if let Some(properties) = schema
        .pointer_mut("/$defs/Limits/properties")
        .and_then(serde_json::Value::as_object_mut)
    {
        for value in properties.values_mut() {
            if let Some(object) = value.as_object_mut() {
                object.insert("minimum".into(), netbaiot_runtime::LIMIT_SCALAR_MIN.into());
                object.insert("maximum".into(), netbaiot_runtime::LIMIT_SCALAR_MAX.into());
            }
        }
    }
    match serde_json::to_string_pretty(&schema) {
        Ok(json) => {
            use std::io::Write;
            if writeln!(std::io::stdout().lock(), "{json}").is_ok() {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::FAILURE
            }
        }
        Err(_) => {
            eprintln!("Cannot serialize generated configuration schema");
            std::process::ExitCode::FAILURE
        }
    }
}
