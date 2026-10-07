//! Shared current Business RPC TLS, structured errors and auth method interface.
mod v3;
use async_trait::async_trait;
use netbaiot_protocol::{
    DeviceCommand,
    business_rpc::{
        AuthenticatedDeviceWire, BUSINESS_RPC_AUTH_MAX_BYTES, DeviceAuthenticateRequest,
        ResolveVerifierRequest, ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
};
use serde::Serialize;
use std::{
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, rustls};
pub use v3::{BusinessRpcV3Client, BusinessRpcV3ClientConfig, BusinessRpcV3Delivery};

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

struct BoundedCommandBody(Vec<u8>);
#[derive(Serialize)]
struct BorrowedCommandRequest<'a> {
    command: &'a DeviceCommand,
}
impl Write for BoundedCommandBody {
    fn write(&mut self, chunk: &[u8]) -> io::Result<usize> {
        if chunk.len() > BUSINESS_RPC_AUTH_MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("command body exceeds RPC limit"));
        }
        self.0.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct BusinessRpcTls {
    pub server_name: String,
    pub ca_pem: PathBuf,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
}
#[derive(Clone, Debug, Error)]
pub enum BusinessRpcClientError {
    #[error("invalid business RPC client configuration")]
    InvalidConfig,
    #[error("business RPC connection unavailable")]
    Unavailable,
    #[error("business RPC operation timed out")]
    Timeout,
    #[error("business RPC protocol error")]
    Protocol,
    #[error("business RPC authorization failed")]
    Unauthorized,
    #[error("business RPC overloaded")]
    Overloaded,
    #[error("command RPC outcome is unknown; retry only with the same command_id")]
    OutcomeUnknown,
    #[error("business RPC request rejected: {0:?}")]
    Remote(RpcErrorCode),
}

#[async_trait]
pub trait BusinessAuthHandler: Send + Sync + 'static {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> std::result::Result<AuthenticatedDeviceWire, RpcError>;
    async fn resolve_verifier(
        &self,
        request: ResolveVerifierRequest,
    ) -> std::result::Result<ResolveVerifierResponse, RpcError>;
}

async fn connect_io(
    config: &BusinessRpcV3ClientConfig,
) -> Result<Box<dyn Io>, BusinessRpcClientError> {
    let socket = tokio::time::timeout(config.connect_timeout, TcpStream::connect(config.address))
        .await
        .map_err(|_| BusinessRpcClientError::Timeout)?
        .map_err(|_| BusinessRpcClientError::Unavailable)?;
    if let Some(tls) = &config.tls {
        let ca = std::fs::read(&tls.ca_pem).map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let cert = std::fs::read(&tls.certificate_pem)
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let key = std::fs::read(&tls.private_key_pem)
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        if ca.len() > 1_048_576 || cert.len() > 1_048_576 || key.len() > 65_536 {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        let mut roots = rustls::RootCertStore::empty();
        for item in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots
                .add(item.map_err(|_| BusinessRpcClientError::InvalidConfig)?)
                .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        }
        if roots.is_empty() {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        let certs = rustls_pemfile::certs(&mut cert.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let key = rustls_pemfile::private_key(&mut key.as_slice())
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?
            .ok_or(BusinessRpcClientError::InvalidConfig)?;
        let rustls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| BusinessRpcClientError::InvalidConfig)?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let stream = tokio::time::timeout(
            config.connect_timeout,
            TlsConnector::from(Arc::new(rustls_config)).connect(name, socket),
        )
        .await
        .map_err(|_| BusinessRpcClientError::Timeout)?
        .map_err(|_| BusinessRpcClientError::Unauthorized)?;
        Ok(Box::new(stream))
    } else {
        Ok(Box::new(socket))
    }
}
