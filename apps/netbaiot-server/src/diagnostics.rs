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

pub fn safe_http_url(url: &reqwest::Url) -> bool {
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
            "Check the version, listener, mTLS identity roles and development token settings in docs/business-rpc-v3.md.",
        );
        let auth = self
            .device_auth
            .unwrap_or(if self.auth_provider_url.is_some() {
                DeviceAuthSource::Http
            } else {
                DeviceAuthSource::Static
            });
        let registry = crate::bootstrap::configured_codec_registry_diagnostic(
            &self.limits,
            &self.payload_codecs,
            &self.native_codecs,
        );
        if let Err(error) = &registry {
            issue(
                true,
                "NBI-CFG-015",
                &error.path,
                error.message,
                "Use unique vendor ID/version pairs, bounded non-overlapping fields and valid checksum/length ranges; see docs/codecs.md.",
            );
        }
        issue(
            self.credentials
                .iter()
                .any(|c| registry.as_ref().is_ok_and(|r| r.get(&c.identity).is_err())),
            "NBI-CFG-011",
            "credentials",
            "A credential refers to an unavailable codec profile.",
            "Use a registered codec ID and version; version zero is invalid.",
        );
        issue(
            SinkId::new("preflight")
                .map_err(|_| Error::Configuration)
                .and_then(|id| crate::bootstrap::bootstrap_snapshot(self, id))
                .is_err(),
            "NBI-CFG-011",
            "credentials",
            "Static devices in one product have conflicting codec profiles.",
            "Use one codec ID/version per tenant and product, or provision distinct products.",
        );
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

// Preserve useful payload-codec array positions and known schema keys. Never echo
// arbitrary map keys from hostile JSON, even when serde includes one in a path.
fn payload_codec_path(path: &serde_path_to_error::Path) -> String {
    use serde_path_to_error::Segment;
    let mut out = String::new();
    for segment in path {
        match segment {
            Segment::Seq { index } => out.push_str(&format!("[{index}]")),
            Segment::Map { key }
                if [
                    "payload_codecs",
                    "id",
                    "version",
                    "header",
                    "message_type",
                    "length",
                    "field",
                    "adjustment",
                    "offset",
                    "type",
                    "endian",
                    "scale",
                    "bias",
                    "messages",
                    "commands",
                    "tag",
                    "size",
                    "source_message_id",
                    "occurred_at",
                    "payload",
                    "kind",
                    "fields",
                    "name",
                    "sequence",
                    "value",
                    "command_id_offset",
                    "execution",
                    "running",
                    "succeeded",
                    "failed",
                    "expires_at",
                    "checksum",
                    "algorithm",
                    "start",
                    "end",
                ]
                .contains(&key.as_str()) =>
            {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            _ => break,
        }
    }
    out
}

// Serde erases custom error types. Only the codec's stable identifiers cross that
// boundary; neither English prose nor input values determine the error category.
fn legacy_integer_member(error: &serde_json::Error) -> Option<&'static str> {
    use netbaiot_codecs::vendor::declarative::{
        LEGACY_INTEGER_BIAS_CODE, LEGACY_INTEGER_SCALE_CODE,
    };
    match error.to_string().split_once(':')?.0 {
        LEGACY_INTEGER_SCALE_CODE => Some("scale"),
        LEGACY_INTEGER_BIAS_CODE => Some("bias"),
        _ => None,
    }
}

// Borrow one JSON child and skip the others. This is used only after a recognized
// parse failure, never on successful startup. No full Value tree or field clones.
struct RawChild<'a>(&'a serde_path_to_error::Segment);
impl<'de> serde::de::DeserializeSeed<'de> for RawChild<'_> {
    type Value = Option<&'de serde_json::value::RawValue>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        de: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        de.deserialize_any(self)
    }
}
impl<'de> serde::de::Visitor<'de> for RawChild<'_> {
    type Value = Option<&'de serde_json::value::RawValue>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a configuration object or array")
    }
    fn visit_map<M: serde::de::MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut selected = None;
        while let Some(key) = map.next_key::<String>()? {
            if matches!(self.0, serde_path_to_error::Segment::Map { key: wanted } if wanted == &key)
            {
                selected = Some(map.next_value()?);
            } else {
                map.next_value::<serde::de::IgnoredAny>()?;
            }
        }
        Ok(selected)
    }
    fn visit_seq<S: serde::de::SeqAccess<'de>>(
        self,
        mut seq: S,
    ) -> std::result::Result<Self::Value, S::Error> {
        let mut selected = None;
        let mut index = 0;
        loop {
            if matches!(self.0, serde_path_to_error::Segment::Seq { index: wanted } if *wanted == index)
            {
                let Some(value) = seq.next_element()? else {
                    break;
                };
                selected = Some(value);
            } else if seq.next_element::<serde::de::IgnoredAny>()?.is_none() {
                break;
            }
            index += 1;
        }
        Ok(selected)
    }
}

// Internally tagged enums erase the path inside payload. Re-read just its fields
// with the same Field deserializer and a streaming sequence that drops each
// successful field immediately. Serde then supplies the actual nested index.
#[derive(Deserialize)]
struct DiagnosticPayload {
    #[serde(default, rename = "fields")]
    _fields: DiagnosticFields,
    #[serde(rename = "value")]
    _value: Option<netbaiot_codecs::vendor::declarative::Field>,
}
#[derive(Default)]
struct DiagnosticFields;
impl<'de> Deserialize<'de> for DiagnosticFields {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        struct Fields;
        impl<'de> serde::de::Visitor<'de> for Fields {
            type Value = DiagnosticFields;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an array of named fields")
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> std::result::Result<Self::Value, S::Error> {
                #[derive(Deserialize)]
                struct Named {
                    #[serde(rename = "field")]
                    _field: netbaiot_codecs::vendor::declarative::Field,
                }
                while seq.next_element::<Named>()?.is_some() {}
                Ok(DiagnosticFields)
            }
        }
        de.deserialize_seq(Fields)
    }
}

fn legacy_integer_path(
    path: &serde_path_to_error::Path,
    bytes: &[u8],
    member: &str,
) -> Option<String> {
    use serde::de::DeserializeSeed;
    // Validate all syntax/trailing data before promoting a schema error to a
    // migration diagnostic. RawValue borrows the already bounded (16 MiB) file.
    let mut raw: &serde_json::value::RawValue = serde_json::from_slice(bytes).ok()?;
    let mut out = payload_codec_path(path);
    if out.ends_with(".payload") {
        for segment in path {
            raw = RawChild(segment)
                .deserialize(&mut serde_json::Deserializer::from_str(raw.get()))
                .ok()??;
        }
        let mut de = serde_json::Deserializer::from_str(raw.get());
        let error = serde_path_to_error::deserialize::<_, DiagnosticPayload>(&mut de).err()?;
        if legacy_integer_member(error.inner()) != Some(member) {
            return None;
        }
        let suffix = payload_codec_path(error.path());
        if suffix.is_empty() {
            return None;
        }
        out.push('.');
        out.push_str(&suffix);
    }
    out.push('.');
    out.push_str(member);
    Some(out)
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
            if root == "payload_codecs"
                && let Some(member) = legacy_integer_member(error.inner())
                && let Some(path) = legacy_integer_path(error.path(), &bytes, member)
            {
                return Err(ConfigReport::from_diagnostics(vec![ConfigDiagnostic::new(
                    "NBI-CFG-016",
                    &path,
                    "Integer fields do not support legacy scale/bias settings.",
                    "Remove legacy scale/bias. For exact conversion, use conversion with mode decimal_exact and string scale/bias values. Omit conversion for an exact identity mapping.",
                )]));
            }
            let payload_path = payload_codec_path(error.path());
            let path = if root == "payload_codecs" {
                payload_path.as_str()
            } else if [
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
    business_rpc_token: Option<&str>,
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
        .is_some_and(|rpc| business_rpc_token.is_none() && rpc.development_token().is_err())
    {
        out.push(ConfigDiagnostic::new(
            "NBI-CFG-008",
            "business_rpc.development_token_env",
            "Business RPC development token source is unavailable or invalid.",
            "Set the configured environment variable to a bounded nonempty development token.",
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

/// Non-destructive local recovery decoder check, serialized with the gateway owner.
pub async fn inspect_recovery(directory: PathBuf, limits: Limits) -> Result<usize> {
    let path = directory.clone();
    let owner = Arc::new(
        tokio::task::spawn_blocking(move || {
            netbaiot_runtime::recovery_io::RecoveryDirectory::acquire_for_inspection(&path)
        })
        .await
        .map_err(|_| Error::Internal)??,
    );
    inspect_recovery_owned(directory, limits, owner).await
}
pub async fn inspect_recovery_owned(
    directory: PathBuf,
    limits: Limits,
    owner: Arc<netbaiot_runtime::recovery_io::RecoveryDirectory>,
) -> Result<usize> {
    limits.validate()?;
    let limits = Arc::new(limits);
    let broker = MqttBroker::new(limits.clone());
    broker.bind_recovery_owner(owner.clone())?;
    broker.recover_from(&directory).await?;
    let spool = RestartSpool::with_owner(directory, limits, owner);
    Ok(spool.recover().await?.records.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        serde_json::from_str(include_str!("../../../configs/development.json")).unwrap()
    }
    #[tokio::test]
    async fn payload_codec_parse_errors_keep_schema_paths_without_echoing_unknown_keys() {
        let original: serde_json::Value =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        let file = std::env::temp_dir().join(format!(
            "netbaiot-payload-diagnostic-{}.json",
            uuid::Uuid::new_v4()
        ));
        for (pointer, value, expected) in [
            (
                "/payload_codecs/0/commands/0/expires_at/type",
                serde_json::json!("secret-never-print"),
                "payload_codecs[0].commands[0].expires_at.type",
            ),
            (
                "/payload_codecs/0/messages/0/size",
                serde_json::json!("secret-never-print"),
                "payload_codecs[0].messages[0].size",
            ),
            (
                "/payload_codecs/0/commands/0/expires_at",
                serde_json::json!({"offset":23,"type":"u64","secret-never-print":"secret-never-print"}),
                "payload_codecs[0].commands[0].expires_at",
            ),
        ] {
            let mut value_config = original.clone();
            *value_config.pointer_mut(pointer).unwrap() = value;
            std::fs::write(&file, serde_json::to_vec(&value_config).unwrap()).unwrap();
            let report = load_config_diagnostic(&file).await.err().unwrap();
            assert_eq!(report.diagnostics[0].path, expected);
            assert!(!report.human().contains("secret-never-print"));
        }
        std::fs::remove_file(file).unwrap();
    }

    #[tokio::test]
    async fn legacy_integer_members_keep_complete_diagnostic_paths() {
        let original: serde_json::Value =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        let file = std::env::temp_dir().join(format!(
            "netbaiot-legacy-diagnostic-{}.json",
            uuid::Uuid::new_v4()
        ));
        for (pointer, path) in [
            (
                "/payload_codecs/0/messages/0/source_message_id",
                "payload_codecs[0].messages[0].source_message_id",
            ),
            (
                "/payload_codecs/0/messages/0/payload/fields/1/field",
                "payload_codecs[0].messages[0].payload.fields[1].field",
            ),
            (
                "/payload_codecs/0/commands/0/fields/0/field",
                "payload_codecs[0].commands[0].fields[0].field",
            ),
        ] {
            for (member, value) in [
                ("scale", serde_json::Value::Null),
                ("bias", serde_json::Value::Null),
                ("scale", serde_json::json!(1)),
                ("bias", serde_json::json!(0)),
                ("scale", serde_json::json!(-0.125)),
                ("bias", serde_json::json!(1e3)),
            ] {
                let mut config = original.clone();
                config.pointer_mut(pointer).unwrap()[member] = value;
                let field = config.pointer(pointer).unwrap();
                let encoded_field = serde_json::to_string(field).unwrap();
                let reversed = format!(
                    "{{{}}}",
                    field
                        .as_object()
                        .unwrap()
                        .iter()
                        .rev()
                        .map(|(key, value)| format!(
                            "{}:{}",
                            serde_json::to_string(key).unwrap(),
                            value
                        ))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let encoded = serde_json::to_string(&config).unwrap();
                for json in [encoded.clone(), encoded.replace(&encoded_field, &reversed)] {
                    std::fs::write(&file, json).unwrap();
                    let report = load_config_diagnostic(&file).await.err().unwrap();
                    assert_legacy_diagnostic(&report, &format!("{path}.{member}"));
                }
            }
        }
        for member in ["scale", "bias"] {
            let mut config = original.clone();
            config["payload_codecs"][0]["messages"][0]["payload"] = serde_json::json!({"kind":"event","name":"test","value":{"offset":9,"type":"i8",member:null}});
            std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
            let report = load_config_diagnostic(&file).await.err().unwrap();
            assert_legacy_diagnostic(
                &report,
                &format!("payload_codecs[0].messages[0].payload.value.{member}"),
            );
        }
        std::fs::remove_file(file).unwrap();
    }

    fn assert_legacy_diagnostic(report: &ConfigReport, path: &str) {
        let diagnostic = &report.diagnostics[0];
        assert_eq!(diagnostic.code, "NBI-CFG-016");
        assert_eq!(diagnostic.path, path);
        for output in [report.human(), serde_json::to_string(report).unwrap()] {
            for expected in [
                path,
                "Integer fields",
                "Remove legacy scale/bias",
                "decimal_exact",
                "string scale/bias",
                "identity mapping",
            ] {
                assert!(output.contains(expected), "{output}");
            }
            assert!(!output.contains("secret-never-print"));
        }
    }

    #[tokio::test]
    async fn legacy_diagnostics_preserve_nested_indices_and_do_not_classify_other_errors() {
        let original: serde_json::Value =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        let file = std::env::temp_dir().join(format!(
            "netbaiot-legacy-categories-{}.json",
            uuid::Uuid::new_v4()
        ));
        let mut config = original.clone();
        config["credentials"][0]["secret_hex"] = serde_json::json!("secret-never-print");
        config["payload_codecs"]
            .as_array_mut()
            .unwrap()
            .push(original["payload_codecs"][0].clone());
        let codec = &mut config["payload_codecs"][1];
        codec["messages"]
            .as_array_mut()
            .unwrap()
            .push(original["payload_codecs"][0]["messages"][0].clone());
        codec["messages"][4]["payload"]["fields"][1]["field"]["bias"] = serde_json::Value::Null;
        std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
        let report = load_config_diagnostic(&file).await.err().unwrap();
        assert_legacy_diagnostic(
            &report,
            "payload_codecs[1].messages[4].payload.fields[1].field.bias",
        );

        let pointer = "/payload_codecs/0/commands/0/fields/0/field";
        for (field, code) in [
            (
                r#"{"offset":3,"type":"i16","scale":"secret-never-print"}"#,
                "NBI-CFG-001",
            ),
            (r#"{"offset":3,"type":"i16","bias":true}"#, "NBI-CFG-001"),
            (r#"{"offset":3,"type":"i16","scale":1e999}"#, "NBI-CFG-001"),
            (
                r#"{"offset":3,"type":"f64","scale":"NBI-CODEC-001: secret-never-print"}"#,
                "NBI-CFG-001",
            ),
            (r#"{"offset":3,"type":"f32","bias":1e999}"#, "NBI-CFG-001"),
            (
                r#"{"offset":3,"type":"secret-never-print","scale":null}"#,
                "NBI-CFG-001",
            ),
            (
                r#"{"offset":3,"type":"i16","scale":null,"secret-never-print":0}"#,
                "NBI-CFG-002",
            ),
            (
                r#"{"offset":3,"type":"i16","scale":null,"bias":"secret-never-print"}"#,
                "NBI-CFG-001",
            ),
        ] {
            let old = serde_json::to_string(original.pointer(pointer).unwrap()).unwrap();
            // Preserve hostile numeric text without normalizing it through Value.
            let json = serde_json::to_string(&original)
                .unwrap()
                .replace(&old, field);
            std::fs::write(&file, json).unwrap();
            let report = load_config_diagnostic(&file).await.err().unwrap();
            assert_eq!(report.diagnostics[0].code, code, "{field}");
            for output in [report.human(), serde_json::to_string(&report).unwrap()] {
                assert!(!output.contains("secret-never-print"));
            }
        }
        let mut config = original.clone();
        config.pointer_mut(pointer).unwrap()["scale"] = serde_json::Value::Null;
        let encoded = serde_json::to_string(&config).unwrap();
        for json in [
            format!("{encoded} trailing-secret-never-print"),
            encoded[..encoded.len() - 1].to_owned(),
        ] {
            std::fs::write(&file, json).unwrap();
            let report = load_config_diagnostic(&file).await.err().unwrap();
            assert_eq!(report.diagnostics[0].code, "NBI-CFG-001");
            assert!(!report.human().contains("secret-never-print"));
        }
        std::fs::remove_file(file).unwrap();
    }

    #[tokio::test]
    async fn valid_integer_conversion_and_float_nulls_pass_config_check() {
        let mut config: serde_json::Value =
            serde_json::from_str(include_str!("../../../configs/vendor-codec.json")).unwrap();
        let file = std::env::temp_dir().join(format!(
            "netbaiot-valid-numerics-{}.json",
            uuid::Uuid::new_v4()
        ));
        for kind in ["f32", "f64"] {
            config["payload_codecs"][0]["messages"][0]["payload"]["fields"][0]["field"] =
                serde_json::json!({"offset":9,"type":kind,"scale":null,"bias":null});
            // Use a standalone event layout so either float width has a valid, nonoverlapping range.
            config["payload_codecs"][0]["messages"][0]["size"] = serde_json::json!(17);
            config["payload_codecs"][0]["messages"][0]["checksum"] = serde_json::Value::Null;
            config["payload_codecs"][0]["messages"][0]["payload"]["fields"]
                .as_array_mut()
                .unwrap()
                .truncate(1);
            std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
            let parsed = load_config_diagnostic(&file).await.unwrap();
            assert!(check_config(&parsed, None, None).await.valid);
        }
        config["payload_codecs"][0]["messages"][0]["payload"]["fields"][0]["field"] = serde_json::json!({"offset":9,"type":"i16","conversion":{"mode":"decimal_exact","scale":"0.001","bias":"1"}});
        std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
        let parsed = load_config_diagnostic(&file).await.unwrap();
        assert!(check_config(&parsed, None, None).await.valid);
        std::fs::remove_file(file).unwrap();
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
