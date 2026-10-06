use std::{path::Path, time::Duration};
use tokio::process::Command;
async fn cli(args: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(15),
        Command::new(env!("CARGO_BIN_EXE_netbaiot"))
            .args(args)
            .env_remove("NETBAIOT_ADMIN_SECRET")
            .env_remove("NETBAIOT_BUSINESS_STREAM_TOKEN")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}
#[tokio::test]
async fn init_check_doctor_schema_and_production_skeleton_use_actual_cli() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("development");
    let dir = directory.to_str().unwrap();
    assert!(cli(&["init", dir]).await.status.success());
    let path = directory.join("netbaiot.json");
    let result = cli(&[
        "config",
        "check",
        "--config",
        path.to_str().unwrap(),
        "--output",
        "json",
    ])
    .await;
    assert!(result.status.success());
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["valid"], true);
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["device_ingress"] = "127.0.0.1:0".into();
    config["management_http"] = "127.0.0.1:0".into();
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let result = cli(&[
        "doctor",
        "--config",
        path.to_str().unwrap(),
        "--output",
        "json",
    ])
    .await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["ok"], true);
    assert!(
        report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == "business_sink_network" && c["status"] == "skip")
    );
    assert!(!cli(&["init", dir]).await.status.success());
    let production = root.path().join("production");
    assert!(
        cli(&["init", "--production", production.to_str().unwrap()])
            .await
            .status
            .success()
    );
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(production.join("netbaiot.json")).unwrap()).unwrap();
    assert_eq!(value["development"], false);
    assert_eq!(value["credentials"], serde_json::json!([]));
    assert!(
        !cli(&[
            "config",
            "check",
            "--config",
            production.join("netbaiot.json").to_str().unwrap()
        ])
        .await
        .status
        .success()
    );
    let schema = cli(&["config", "schema"]).await;
    assert!(schema.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&schema.stdout).unwrap(),
        serde_json::from_slice::<serde_json::Value>(include_bytes!(
            "../../../docs/schema/netbaiot-config.schema.json"
        ))
        .unwrap()
    );
    assert_eq!(
        std::fs::read_to_string(production.join(".env.example"))
            .unwrap()
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
            .filter(|l| !l.ends_with('='))
            .count(),
        0
    );
    assert!(Path::new(dir).join("README.md").is_file());
}
