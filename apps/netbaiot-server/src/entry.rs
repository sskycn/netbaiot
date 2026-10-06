//! Shared executable boundary. Both binaries use these signals and the same runtime.
use super::*;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct BoundAddresses {
    pub device_ingress: SocketAddr,
    pub management_http: SocketAddr,
    pub business_tcp: Option<SocketAddr>,
}
#[derive(Debug)]
pub enum ServeError {
    Configuration(ConfigReport),
    Runtime(Error),
}
impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(r) => f.write_str(&r.human()),
            Self::Runtime(e) => write!(f, "Gateway runtime failed: {e}"),
        }
    }
}
impl std::error::Error for ServeError {}

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

pub async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| Error::Internal)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(|_| Error::Internal),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.map_err(|_| Error::Internal)
}

pub async fn run_until_signal(config: Config) -> Result<()> {
    let stop = CancellationToken::new();
    let server = run(config, stop.clone());
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        result = shutdown_signal() => {
            stop.cancel();
            let shutdown = server.await;
            result?;
            shutdown
        }
    }
}

pub async fn serve_path(path: &Path) -> std::result::Result<(), ServeError> {
    let config = load_config_diagnostic(path)
        .await
        .map_err(ServeError::Configuration)?;
    let report = check_config(
        &config,
        std::env::var("NETBAIOT_ADMIN_SECRET").ok().as_deref(),
        std::env::var("NETBAIOT_BUSINESS_STREAM_TOKEN")
            .ok()
            .as_deref(),
    )
    .await;
    if !report.valid {
        return Err(ServeError::Configuration(report));
    }
    run_until_signal(config).await.map_err(ServeError::Runtime)
}

/// One default-limit source for the CLI and compatibility binary.
pub fn default_limits_json() -> Result<String> {
    serde_json::to_string_pretty(&Limits::default()).map_err(|_| Error::Internal)
}
