use super::*;
use std::time::{SystemTime, UNIX_EPOCH};
pub(super) async fn inspect(config: &Config) -> Vec<Check> {
    let mut out = Vec::new();
    for (id, path) in [
        (
            "device_tls",
            config.tls.as_ref().map(|t| t.certificate.as_str()),
        ),
        (
            "management_tls",
            config
                .management_tls
                .as_ref()
                .map(|t| t.certificate.as_str())
                .or_else(|| config.tls.as_ref().map(|t| t.certificate.as_str())),
        ),
        (
            "management_client_ca",
            config
                .management_tls
                .as_ref()
                .and_then(|t| t.client_ca.as_deref()),
        ),
        (
            "business_client_ca",
            config
                .business_rpc
                .as_ref()
                .and_then(|r| r.tls.as_ref())
                .and_then(|t| t.client_ca.as_deref()),
        ),
        (
            "business_tls",
            config
                .business_rpc
                .as_ref()
                .and_then(|r| r.tls.as_ref())
                .map(|t| t.certificate.as_str()),
        ),
    ] {
        let Some(path) = path else {
            out.push(Check::new(
                id,
                Status::Skip,
                "NBI-DOC-005",
                "TLS not configured; configuration policy was checked separately",
            ));
            continue;
        };
        let data = crate::read_bounded(std::path::Path::new(path)).await;
        let result = data.ok().and_then(|bytes| {
            let cert = rustls_pemfile::certs(&mut bytes.as_slice()).next()?.ok()?;
            let (_, certificate) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
            Some((
                certificate.validity().not_before.timestamp(),
                certificate.validity().not_after.timestamp(),
            ))
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|t| t.as_secs() as i64)
            .unwrap_or(0);
        out.push(match result {
            Some((before,after)) if before>now||after<=now=>Check::new(id,Status::Fail,"NBI-DOC-005","Certificate is expired or not yet valid"),
            Some((_,after))=>{let days=(after-now)/86_400;Check::new(id,if days<30{Status::Warn}else{Status::Pass},"NBI-DOC-005",format!("Certificate expires in {days} days (UTC Unix expiry {after}); PEM/key/CA checks use config check"))},
            None=>Check::new(id,Status::Fail,"NBI-DOC-005","Certificate could not be read or parsed; private-key contents are never displayed"),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn expired_and_missing_certificates_fail_without_key_contents() {
        let mut config: Config =
            serde_json::from_str(include_str!("../../../../configs/development.json")).unwrap();
        let expired = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/management-expired.pem");
        config.tls = Some(netbaiot_server::TlsFiles {
            certificate: expired.display().to_string(),
            private_key: "private-key-never-print".into(),
        });
        let report = inspect(&config).await;
        assert!(
            report
                .iter()
                .any(|c| c.status == Status::Fail && c.message.contains("expired"))
        );
        config.tls.as_mut().unwrap().certificate = "missing-doctor-test-certificate.pem".into();
        let report = inspect(&config).await;
        assert!(report.iter().any(|c| c.status == Status::Fail));
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("private-key-never-print")
        );
    }
}
