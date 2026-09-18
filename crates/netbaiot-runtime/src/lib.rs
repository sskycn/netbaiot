pub mod auth;
pub mod limits;
pub mod metrics;
pub mod quota;
pub mod sessions;
pub mod store;
pub mod ingress;
pub mod commands;
pub mod worker;
pub use auth::*;
pub use commands::*;
pub use ingress::*;
pub use limits::*;
pub use metrics::*;
pub use quota::*;
pub use sessions::*;
pub use store::*;
use std::{sync::{Mutex, MutexGuard}, time::{Duration,SystemTime,UNIX_EPOCH}};
use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid configuration")] Configuration,
    #[error("authentication failed")] Authentication,
    #[error("not authorized")] Forbidden,
    #[error("invalid input")] Invalid,
    #[error("resource capacity exhausted")] Overloaded,
    #[error("conflicting application message identity or state")] Conflict,
    #[error("operation timed out")] Timeout,
    #[error("service is draining")] Draining,
    #[error("storage operation failed")] Storage,
    #[error("device unavailable")] Unavailable,
    #[error("internal synchronization failure")] Internal,
    #[error("codec failed")] Codec,
}
pub type Result<T> = std::result::Result<T, Error>;
pub fn now_ms() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)).unwrap_or(0) }
pub fn lock<T>(m: &Mutex<T>) -> Result<MutexGuard<'_,T>> { m.lock().map_err(|_| Error::Internal) }
pub async fn deadline<T>(ms:u64, f:impl std::future::Future<Output=Result<T>>) -> Result<T> { tokio::time::timeout(Duration::from_millis(ms),f).await.map_err(|_| Error::Timeout)? }
