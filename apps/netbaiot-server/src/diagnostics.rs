//! Configuration-boundary diagnostics. Never expose serde values or secret sources.
use super::*;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct ConfigDiagnostic {
    pub code: &'static str,
    pub path: String,
    pub message: &'static str,
    pub help: &'static str,
}
impl ConfigDiagnostic {
    fn new(code: &'static str, path: &str, message: &'static str, help: &'static str) -> Self {
        Self {
            code,
            path: path.into(),
            message,
            help,
        }
    }
}
#[derive(Debug, Serialize)]
pub struct ConfigReport {
    pub valid: bool,
    pub diagnostics: Vec<ConfigDiagnostic>,
}
impl ConfigReport {
    fn from_diagnostics(diagnostics: Vec<ConfigDiagnostic>) -> Self {
        Self {
            valid: diagnostics.is_empty(),
            diagnostics,
        }
    }
    pub fn human(&self) -> String {
        if self.valid {
            return "Configuration valid\n".into();
        }
        let mut text = format!(
            "Configuration invalid: {} problem(s)\n",
            self.diagnostics.len()
        );
        for d in &self.diagnostics {
            text.push_str(&format!(
                "\n{} {}\n{}\nHelp: {}\n",
                d.code, d.path, d.message, d.help
            ));
        }
        text
    }
}

pub(crate) fn safe_http_url(url: &reqwest::Url) -> bool {
    url.username().is_empty()
        && url.password().is_none()
        && url.host_str().is_some()
        && (url.scheme() == "https"
            || (url.scheme() == "http"
                && url.host_str().is_some_and(|h| {
                    h == "localhost"
                        || h.trim_matches(['[', ']'])
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                })))
}
impl Config {
    /// The same static checks used by every server entry point. No I/O or workers.
    pub fn diagnostics(&self) -> Vec<ConfigDiagnostic> {
        let mut out = Vec::new();
        let mut issue = |bad: bool, code, path, message, help| {
            if bad {
                out.push(ConfigDiagnostic::new(code, path, message, help));
            }
        };
        issue(
            self.limits.validate().is_err(),
            "NBI-CFG-010",
            "limits",
            "Resource limits are invalid or inconsistent.",
            "Use netbaiot config limits as the default reference; all configured limits must remain bounded.",
        );
        issue(
            self.development
                && [self.device_ingress, self.management_http]
                    .iter()
                    .any(|a| !a.ip().is_loopback()),
            "NBI-CFG-003",
            "development",
            "Development listeners must use loopback addresses.",
            "Use 127.0.0.1 or ::1, or configure authenticated TLS production listeners.",
        );
        issue(
            !self.device_ingress.ip().is_loopback() && self.tls.is_none(),
            "NBI-CFG-004",
            "device_ingress",
            "Non-loopback device ingress requires TLS.",
            "Configure tls or bind device_ingress to loopback.",
        );
        issue(
            !self.management_http.ip().is_loopback()
                && self.tls.is_none()
                && self.management_tls.is_none(),
            "NBI-CFG-005",
            "management_http",
            "Non-loopback management requires TLS.",
            "Configure management_tls or tls, or use loopback.",
        );
        issue(
            self.management_tls
                .as_ref()
                .is_some_and(|t| t.require_client_certificate && t.client_ca.is_none()),
            "NBI-CFG-006",
            "management_tls.client_ca",
            "Required client certificates need a client CA.",
            "Configure a PEM CA file.",
        );
        issue(
            self.management_tls
                .as_ref()
                .is_some_and(|t| t.require_client_certificate)
                == self.management_auth.mtls_identities.is_empty(),
            "NBI-CFG-007",
            "management_auth.mtls_identities",
            "Management mTLS identity mappings and required client certificates must be configured together.",
            "Configure both management_tls and management_auth.mtls_identities.",
        );
        issue(
            self.validate_business().is_err(),
            "NBI-CFG-008",
            "business_rpc",
            "Business listener, roles, identities or protocol limits are inconsistent.",
            "Check the version, listener, mTLS identity roles and development token settings in docs/business-rpc.md.",
        );
        let auth = self
            .device_auth
            .unwrap_or(if self.auth_provider_url.is_some() {
                DeviceAuthSource::Http
            } else {
                DeviceAuthSource::Static
            });
        issue(
            auth == DeviceAuthSource::Static && self.credentials.is_empty(),
            "NBI-CFG-011",
            "credentials",
            "Static device authentication requires credentials.",
            "Configure valid static credentials or select an authentication provider.",
        );
        issue(
            auth == DeviceAuthSource::Http && self.auth_provider_url.is_none(),
            "NBI-CFG-011",
            "auth_provider_url",
            "HTTP device authentication requires a provider URL.",
            "Configure auth_provider_url.",
        );
        issue(
            auth == DeviceAuthSource::BusinessRpc && self.business_rpc.is_none(),
            "NBI-CFG-008",
            "device_auth",
            "Business RPC authentication requires a business RPC listener.",
            "Configure business_rpc and business_tcp.",
        );
        let delivery = self
            .event_delivery
            .unwrap_or(if self.business_tcp.is_some() {
                EventDeliverySource::BusinessRpc
            } else if self.delivery_url.is_some() {
                EventDeliverySource::Http
            } else {
                EventDeliverySource::DevelopmentAudit
            });
        issue(
            delivery == EventDeliverySource::BusinessRpc && self.business_tcp.is_none(),
            "NBI-CFG-008",
            "event_delivery",
            "Business RPC delivery requires a business listener.",
            "Configure business_tcp.",
        );
        issue(
            delivery == EventDeliverySource::Http && self.delivery_url.is_none(),
            "NBI-CFG-009",
            "delivery_url",
            "HTTP delivery requires a sink URL.",
            "Configure delivery_url.",
        );
        issue(
            !self.development && delivery == EventDeliverySource::DevelopmentAudit,
            "NBI-CFG-009",
            "event_delivery",
            "Production requires an explicit business sink.",
            "Configure a confirmed HTTP or business RPC sink.",
        );
        for (path, url) in [
            ("delivery_url", &self.delivery_url),
            ("auth_provider_url", &self.auth_provider_url),
        ] {
            issue(
                url.as_ref()
                    .is_some_and(|s| reqwest::Url::parse(s).map_or(true, |u| !safe_http_url(&u))),
                "NBI-CFG-009",
                path,
                "HTTP endpoints require HTTPS (loopback HTTP is allowed) and must not contain URL credentials.",
                "Use an HTTPS URL and supply service credentials through protected environment variables.",
            );
        }
        issue(
            self.spool_directory.as_os_str().is_empty(),
            "NBI-CFG-012",
            "spool_directory",
            "Recovery directory must not be empty.",
            "Use a dedicated local directory owned by one gateway instance.",
        );
        out
    }
}

/// Read bounded JSON and report the field path without displaying its value.
pub async fn load_config_diagnostic(path: &Path) -> std::result::Result<Config, ConfigReport> {
    let bytes = match crate::config::read_bounded_path(path, 16_777_216).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err(ConfigReport::from_diagnostics(vec![ConfigDiagnostic::new(
                "NBI-CFG-013",
                "config",
                "Cannot read configuration as a bounded regular file.",
                "Check the file path, permissions and 16 MiB size ceiling.",
            )]));
        }
    };
    let mut de = serde_json::Deserializer::from_slice(&bytes);
    let parsed: std::result::Result<Config, _> = serde_path_to_error::deserialize(&mut de);
    match parsed {
        Ok(config) if de.end().is_ok() => Ok(config),
        Ok(_) => Err(ConfigReport::from_diagnostics(vec![ConfigDiagnostic::new(
            "NBI-CFG-001",
            "config",
            "Configuration contains invalid or trailing JSON.",
            "Provide exactly one JSON configuration object.",
        )])),
        Err(error) => {
            let unknown = error.inner().to_string().starts_with("unknown field");
            let raw = error.path().to_string();
            // Paths are schema fields/array positions; never echo an unknown user field.
            let root = raw.split(['.', '[']).next().unwrap_or("config");
            let path = if [
                "device_ingress",
                "management_http",
                "business_tcp",
                "business_rpc",
                "device_auth",
                "event_delivery",
                "development",
                "limits",
                "credentials",
                "tls",
                "management_tls",
                "management_auth",
                "delivery_url",
                "auth_provider_url",
                "spool_directory",
            ]
            .contains(&root)
            {
                root
            } else {
                "config"
            };
            let (code, message) = if unknown {
                ("NBI-CFG-002", "Unknown configuration field.")
            } else if ["device_ingress", "management_http", "business_tcp"].contains(&path) {
                ("NBI-CFG-003", "Listener must be an IP address and port.")
            } else {
                (
                    "NBI-CFG-001",
                    "Malformed JSON or a value does not match the configuration schema.",
                )
            };
            Err(ConfigReport::from_diagnostics(vec![ConfigDiagnostic::new(
                code,
                path,
                message,
                "Check the JSON syntax and documented field types; values are omitted to protect secrets.",
            )]))
        }
    }
}

/// Local preflight only: no recovery mutation, listeners, delivery workers or remote calls.
pub async fn check_config(
    config: &Config,
    admin_secret: Option<&str>,
    stream_token: Option<&str>,
) -> ConfigReport {
    let mut out = config.diagnostics();
    if config.limits.validate().is_ok() {
        if config
            .device_auth
            .unwrap_or(if config.auth_provider_url.is_some() {
                DeviceAuthSource::Http
            } else {
                DeviceAuthSource::Static
            })
            == DeviceAuthSource::Static
            && !config.credentials.is_empty()
            && StaticAuthenticator::new(config.credentials.clone(), &config.limits).is_err()
        {
            out.push(ConfigDiagnostic::new("NBI-CFG-011", "credentials", "Device credentials are invalid, duplicated or exceed capacity.", "Check credential format, identity, version and count limits; secret values are not shown."));
        }
        let registry = crate::bootstrap::codec_registry(&config.limits);
        if config.credentials.iter().any(|c| {
            registry
                .as_ref()
                .map_or(true, |r| r.get(&c.identity).is_err())
        }) {
            out.push(ConfigDiagnostic::new(
                "NBI-CFG-011",
                "credentials",
                "A credential refers to an unavailable codec profile.",
                "Use an implemented server codec profile and version.",
            ));
        }
        if let Some(url) = config
            .auth_provider_url
            .as_deref()
            .filter(|url| reqwest::Url::parse(url).is_ok_and(|u| safe_http_url(&u)))
            && HttpAuthProvider::new(url, &config.limits).is_err()
        {
            out.push(ConfigDiagnostic::new(
                "NBI-CFG-011",
                "auth_provider_url",
                "HTTP authentication provider settings or secret source are invalid.",
                "Check NETBAIOT_AUTH_PROVIDER_TOKEN format; token values are never displayed.",
            ));
        }
        let legacy = admin_secret.and_then(|s| {
            AdminAccess::new(s, HashMap::new(), &config.limits)
                .ok()
                .map(Arc::new)
        });
        let invalid_secret = admin_secret.is_some() && legacy.is_none();
        let auth = ManagementAuthService::new(
            config.management_auth.clone(),
            legacy,
            Arc::new(config.limits.clone()),
        );
        if invalid_secret
            || auth.as_ref().map_or(true, |a| {
                !config.management_http.ip().is_loopback()
                    && !(if config
                        .management_tls
                        .as_ref()
                        .is_some_and(|t| t.require_client_certificate)
                    {
                        a.has_mtls_provider()
                    } else {
                        a.has_provider()
                    })
            })
        {
            out.push(ConfigDiagnostic::new("NBI-CFG-007", "management_auth", "Management authentication or NETBAIOT_ADMIN_SECRET is invalid or unavailable.", "Configure valid scoped providers and their secret sources; NETBAIOT_ADMIN_SECRET, when used, must be 64 hex characters."));
        }
    }
    if config
        .business_rpc
        .as_ref()
        .is_some_and(|rpc| rpc.development_token().is_err())
    {
        out.push(ConfigDiagnostic::new(
            "NBI-CFG-008",
            "business_rpc.development_token_env",
            "Business RPC development token source is unavailable or invalid.",
            "Set the configured environment variable to a bounded nonempty development token.",
        ));
    }
    if config.business_tcp.is_some()
        && config.business_rpc.as_ref().is_none_or(|rpc| rpc.allow_v1)
        && stream_token.is_none()
    {
        out.push(ConfigDiagnostic::new(
            "NBI-CFG-008",
            "business_tcp",
            "Legacy business streams require NETBAIOT_BUSINESS_STREAM_TOKEN.",
            "Set the stream token through a protected environment source.",
        ));
    }
    for (path, result) in [
        (
            "tls",
            match &config.tls {
                Some(t) => tls_acceptor(t).await.map(|_| ()),
                None => Ok(()),
            },
        ),
        (
            "management_tls",
            match &config.management_tls {
                Some(t) => management_tls_acceptor(t).await.map(|_| ()),
                None => Ok(()),
            },
        ),
        (
            "business_rpc.tls",
            match config.business_rpc.as_ref().and_then(|r| r.tls.as_ref()) {
                Some(t) => management_tls_acceptor(t).await.map(|_| ()),
                None => Ok(()),
            },
        ),
    ] {
        if result.is_err() {
            out.push(ConfigDiagnostic::new("NBI-CFG-006", path, "TLS files cannot be loaded, parsed or matched.", "Check bounded PEM certificate, private key and client CA files and their permissions."));
        }
    }
    match tokio::fs::symlink_metadata(&config.spool_directory).await {
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
            out.push(ConfigDiagnostic::new(
                "NBI-CFG-012",
                "spool_directory",
                "Recovery path is not a real directory.",
                "Use a dedicated local directory; do not use a file or symlink.",
            ))
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => out.push(ConfigDiagnostic::new(
            "NBI-CFG-012",
            "spool_directory",
            "Recovery directory metadata is inaccessible.",
            "Check parent directory permissions.",
        )),
        _ => (),
    }
    ConfigReport::from_diagnostics(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap()
    }
    #[tokio::test]
    async fn diagnostics_are_shared_specific_and_secret_safe_without_startup() {
        let mut config = config();
        let root = std::env::temp_dir().join(format!("netbaiot-check-{}", uuid::Uuid::new_v4()));
        config.spool_directory = root.join("not-created");
        assert!(check_config(&config, None, None).await.valid);
        assert!(!root.exists(), "config check created recovery state");
        config.device_ingress = "0.0.0.0:8080".parse().unwrap();
        config.limits.max_connections = 0;
        config.delivery_url = Some("http://user:secret-never-print@public.example/events".into());
        let report = check_config(&config, Some("admin-secret-never-print"), None).await;
        assert!(!report.valid);
        assert!(config.validate().is_err());
        for (code, path) in [
            ("NBI-CFG-003", "development"),
            ("NBI-CFG-004", "device_ingress"),
            ("NBI-CFG-009", "delivery_url"),
            ("NBI-CFG-010", "limits"),
        ] {
            assert!(
                report
                    .diagnostics
                    .iter()
                    .any(|d| d.code == code && d.path == path)
            );
        }
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("never-print")
        );
        let mut config = self::config();
        assert!(
            check_config(&config, Some("admin-secret-never-print"), None)
                .await
                .diagnostics
                .iter()
                .any(|d| d.code == "NBI-CFG-007")
        );
        config.tls = Some(TlsFiles {
            certificate: root.join("missing-cert").display().to_string(),
            private_key: root.join("missing-key").display().to_string(),
        });
        assert!(
            check_config(&config, None, None)
                .await
                .diagnostics
                .iter()
                .any(|d| d.code == "NBI-CFG-006" && d.path == "tls")
        );
        assert!(!root.exists());
    }
    #[tokio::test]
    async fn json_schema_failures_do_not_echo_secret_values_or_unknown_names() {
        let root =
            std::env::temp_dir().join(format!("netbaiot-json-check-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("config.json");
        for (value, code, path) in [
            ("{ broken secret-never-print", "NBI-CFG-001", "config"),
            (
                r#"{"device_ingress":"secret-never-print"}"#,
                "NBI-CFG-003",
                "device_ingress",
            ),
            (
                r#"{"secret-never-print": "secret-never-print"}"#,
                "NBI-CFG-002",
                "config",
            ),
        ] {
            std::fs::write(&file, value).unwrap();
            let report = load_config_diagnostic(&file).await.err().unwrap();
            assert_eq!(report.diagnostics[0].code, code);
            assert_eq!(report.diagnostics[0].path, path);
            assert!(!report.human().contains("secret-never-print"));
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
