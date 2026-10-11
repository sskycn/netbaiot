//! Existing authentication/key-provider boundary; no wire or HMAC replacement.
use crate::Result;
use crate::auth::{AuthenticationRequest, DeviceVerifier};
use async_trait::async_trait;
use netbaiot_core::AuthenticatedDevice;

#[async_trait]
pub trait DeviceAuthenticator: Send + Sync {
    async fn authenticate(&self, request: AuthenticationRequest<'_>)
    -> Result<AuthenticatedDevice>;

    /// Resolve stable verifier material once; UDP signatures are verified locally
    /// for every datagram after this bounded lookup.
    async fn resolve_verifier(&self, credential_id: &str) -> Result<DeviceVerifier>;
}
