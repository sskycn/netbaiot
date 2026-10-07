mod filesystem;
mod network;
mod tls;
use netbaiot_server::{Config, ConfigReport};
use serde::Serialize;
use std::path::Path;
#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Skip,
}
#[derive(Serialize)]
pub struct Check {
    pub id: &'static str,
    pub status: Status,
    pub code: &'static str,
    pub message: String,
}
impl Check {
    pub(super) fn new(
        id: &'static str,
        status: Status,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            id,
            status,
            code,
            message: message.into(),
        }
    }
}
#[derive(Serialize)]
pub struct Report {
    pub ok: bool,
    pub checks: Vec<Check>,
    pub configuration: ConfigReport,
}
impl Report {
    pub fn human(&self) -> String {
        let mut text = String::from("NetbaIoT doctor\n");
        for c in &self.checks {
            let status = match c.status {
                Status::Pass => "PASS",
                Status::Warn => "WARN",
                Status::Fail => "FAIL",
                Status::Skip => "SKIP",
            };
            text.push_str(&format!(
                "{:<24} {:<4} {} {}\n",
                c.id, status, c.code, c.message
            ));
        }
        if !self.configuration.valid {
            text.push_str(&self.configuration.human());
        }
        text.push_str(if self.ok { "No blocking problems found. Binding probes are available-now checks; serve rechecks ownership and ports.\n" } else { "Blocking problems found.\n" });
        text
    }
}
pub async fn inspect(path: &Path, external: bool) -> Report {
    let config = netbaiot_server::load_config_diagnostic(path).await;
    let (config, configuration) = match config {
        Ok(config) => {
            let report = netbaiot_server::check_config(
                &config,
                std::env::var("NETBAIOT_ADMIN_SECRET").ok().as_deref(),
                None,
            )
            .await;
            (Some(config), report)
        }
        Err(report) => (None, report),
    };
    let mut checks = vec![Check::new(
        "configuration",
        if configuration.valid {
            Status::Pass
        } else {
            Status::Fail
        },
        "NBI-CFG",
        if configuration.valid {
            "Configuration valid"
        } else {
            "Configuration diagnostics below"
        },
    )];
    if let Some(config) = config {
        checks.extend(network::bindings(&config).await);
        checks.push(filesystem::inspect(&config).await);
        checks.extend(tls::inspect(&config).await);
        for (id, url) in [
            ("auth_provider_network", config.auth_provider_url.as_deref()),
            ("business_sink_network", config.delivery_url.as_deref()),
        ] {
            checks.push(if !external { Check::new(id,Status::Skip,"NBI-DOC-003","Use --network for bounded DNS/TCP/TLS reachability; no HTTP/business messages are sent") } else if let Some(url)=url { network::probe(id,url).await } else { Check::new(id,Status::Skip,"NBI-DOC-003","Endpoint not configured") });
        }
        checks.push(Check::new("business_acknowledgement",Status::Skip,"NBI-DOC-003","Business acknowledgement semantics NOT TESTED; no synthetic events or commands are submitted"));
        checks.push(Check::new("authentication_configuration",if configuration.valid {Status::Pass}else{Status::Skip},"NBI-CFG","Uses the same local provider/secret-source validation; device authentication semantics are not probed"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.as_secs().to_string())
        .unwrap_or_else(|_| "unavailable (clock precedes Unix epoch)".into());
    checks.push(Check::new(
        "system_time",
        Status::Skip,
        "NBI-DOC-004",
        format!(
            "Current UTC Unix seconds: {now}; synchronization with a trusted clock is NOT CHECKED"
        ),
    ));
    let ok = checks.iter().all(|c| c.status != Status::Fail);
    Report {
        ok,
        checks,
        configuration,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn local_doctor_does_not_contact_configured_sink_and_reports_occupied_port() {
        let root = tempfile::tempdir().unwrap();
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../../../configs/development.json")).unwrap();
        config.device_ingress = occupied.local_addr().unwrap();
        config.management_http = "127.0.0.1:0".parse().unwrap();
        config.delivery_url = Some(format!("http://{}/events", occupied.local_addr().unwrap()));
        config.spool_directory = root.path().join("var");
        let path = root.path().join("config.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let report = inspect(&path, false).await;
        assert!(!report.ok);
        assert!(
            report
                .checks
                .iter()
                .any(|c| c.id == "device_port" && c.status == Status::Fail)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|c| c.id == "business_sink_network" && c.status == Status::Skip)
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), occupied.accept())
                .await
                .is_err()
        );
    }
}
