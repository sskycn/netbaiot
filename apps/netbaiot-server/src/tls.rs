use super::*;
use crate::config::read_bounded;

pub async fn tls_acceptor(files: &TlsFiles) -> Result<TlsAcceptor> {
    let cert = read_bounded(&files.certificate, 1_048_576).await?;
    let key = read_bounded(&files.private_key, 65_536).await?;
    let certificates = rustls_pemfile::certs(&mut cert.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)?;
    let private = rustls_pemfile::private_key(&mut key.as_slice())
        .map_err(|_| Error::Configuration)?
        .ok_or(Error::Configuration)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::Configuration)?
    .with_no_client_auth()
    .with_single_cert(certificates, private)
    .map_err(|_| Error::Configuration)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn management_tls_acceptor(files: &ManagementTlsFiles) -> Result<TlsAcceptor> {
    let cert = read_bounded(&files.certificate, 1_048_576).await?;
    let key = read_bounded(&files.private_key, 65_536).await?;
    let certificates = rustls_pemfile::certs(&mut cert.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::Configuration)?;
    let private = rustls_pemfile::private_key(&mut key.as_slice())
        .map_err(|_| Error::Configuration)?
        .ok_or(Error::Configuration)?;
    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::Configuration)?;
    let builder = if files.require_client_certificate {
        let ca = read_bounded(
            files.client_ca.as_deref().ok_or(Error::Configuration)?,
            1_048_576,
        )
        .await?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots
                .add(cert.map_err(|_| Error::Configuration)?)
                .map_err(|_| Error::Configuration)?;
        }
        if roots.is_empty() {
            return Err(Error::Configuration);
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|_| Error::Configuration)?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let config = builder
        .with_single_cert(certificates, private)
        .map_err(|_| Error::Configuration)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
