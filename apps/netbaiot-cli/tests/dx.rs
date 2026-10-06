#[cfg(unix)]
use std::process::Stdio;
use std::time::Duration;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::{net::TcpListener, process::Command};

fn cli() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_netbaiot"));
    for name in [
        "NETBAIOT_ENDPOINT",
        "NETBAIOT_TOKEN",
        "NETBAIOT_API_KEY",
        "NETBAIOT_EVENT_ADDRESS",
        "NETBAIOT_ADMIN_SECRET",
        "NETBAIOT_BUSINESS_STREAM_TOKEN",
    ] {
        c.env_remove(name);
    }
    c.kill_on_drop(true);
    c
}
async fn output(args: &[&str]) -> std::process::Output {
    tokio::time::timeout(Duration::from_secs(15), cli().args(args).output())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn local_commands_help_diagnostics_and_limits_need_no_management_credentials() {
    for args in [
        vec!["--help"],
        vec!["serve", "--help"],
        vec!["demo", "--help"],
        vec!["config", "--help"],
        vec!["config", "check", "--help"],
        vec!["version"],
    ] {
        let result = output(&args).await;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.json");
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap();
    config["spool_directory"] = serde_json::json!(root.path().join("must-not-exist"));
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let path = path.to_str().unwrap();
    let result = output(&["config", "check", "--config", path, "--output", "json"]).await;
    assert!(result.status.success());
    assert!(result.stderr.is_empty());
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["valid"], true);
    assert!(!root.path().join("must-not-exist").exists());
    for (json, code) in [
        ("{malformed", "NBI-CFG-001"),
        (r#"{"secret-never-log":"secret-never-log"}"#, "NBI-CFG-002"),
        (r#"{"device_ingress":"secret-never-log"}"#, "NBI-CFG-003"),
    ] {
        std::fs::write(path, json).unwrap();
        let result = output(&["--output", "json", "config", "check", "--config", path]).await;
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stderr.is_empty());
        let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["diagnostics"][0]["code"], code);
        assert!(!String::from_utf8_lossy(&result.stdout).contains("secret-never-log"));
    }
    let limits = output(&["config", "limits"]).await;
    let actual: serde_json::Value = serde_json::from_slice(&limits.stdout).unwrap();
    assert_eq!(
        actual,
        serde_json::to_value(netbaiot_runtime::Limits::default()).unwrap()
    );
    for args in [
        vec!["unknown"],
        vec!["config", "check", "--config", "missing-config.json"],
        vec!["serve", "--bogus"],
        vec!["server", "drain", "--yes", "--bogus"],
    ] {
        assert_eq!(output(&args).await.status.code(), Some(2));
    }
}
#[tokio::test]
async fn actual_demo_once_uses_real_mqtt_and_cleans_private_directory() {
    let temp = tempfile::tempdir().unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        cli()
            .args(["demo", "--once"])
            .env("TMPDIR", temp.path())
            .env("TMP", temp.path())
            .env("TEMP", temp.path())
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let text = String::from_utf8(result.stdout).unwrap();
    for stage in [
        "Gateway started",
        "Demo device authenticated",
        "Heartbeat EventAccepted",
        "Business sink acknowledged",
        "Shutdown completed",
    ] {
        assert!(text.contains(stage), "missing {stage}: {text}");
    }
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    for prefix in ["MQTT/TCP/UDP: ", "Management: "] {
        let address = text.lines().find_map(|l| l.strip_prefix(prefix)).unwrap();
        assert!(TcpListener::bind(address).await.is_ok());
    }
}
#[cfg(unix)]
#[tokio::test]
async fn demo_sigint_and_sigterm_gracefully_stop_and_clean() {
    for signal in ["-INT", "-TERM"] {
        let temp = tempfile::tempdir().unwrap();
        let mut child = cli()
            .arg("demo")
            .env("TMPDIR", temp.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut text = String::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).await.unwrap() > 0);
                text.push_str(&line);
                if line.contains("demo ready") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            Command::new("kill")
                .args([signal, &pid.to_string()])
                .status()
                .await
                .unwrap()
                .success()
        );
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        let mut line = String::new();
        while reader.read_line(&mut line).await.unwrap() > 0 {
            text.push_str(&line);
            line.clear();
        }
        assert!(text.contains("Shutdown completed"));
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}
