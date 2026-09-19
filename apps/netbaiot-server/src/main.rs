use netbaiot_runtime::{Error, Result};
use tokio_util::sync::CancellationToken;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/development.json".into());
    if path == "--print-default-limits" {
        use std::io::Write;
        let bytes = serde_json::to_vec_pretty(&netbaiot_runtime::Limits::default())
            .map_err(|_| Error::Internal)?;
        std::io::stdout()
            .lock()
            .write_all(&bytes)
            .map_err(|_| Error::Internal)?;
        return Ok(());
    }
    let config = netbaiot_server::read_config(&path).await?;
    let stop = CancellationToken::new();
    let server = netbaiot_server::run(config, stop.clone());
    tokio::pin!(server);
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| Error::Internal)?;
    #[cfg(unix)]
    let shutdown = async {
        tokio::select! {result=tokio::signal::ctrl_c()=>result.map_err(|_|Error::Internal),_=terminate.recv()=>Ok(())}
    };
    #[cfg(not(unix))]
    let shutdown = async { tokio::signal::ctrl_c().await.map_err(|_| Error::Internal) };
    tokio::select! {result=&mut server=>result,result=shutdown=>{result?;stop.cancel();server.await}}
}
