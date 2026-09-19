use crate::*;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_core::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use subtle::ConstantTimeEq;

/// Provisioned high-entropy 256-bit key. Deliberately no Debug implementation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub credential_id: String,
    pub secret_hex: String,
    pub identity: AuthenticatedDevice,
}
pub enum AuthenticationRequest<'a> {
    Secret {
        credential_id: &'a str,
        secret: &'a [u8],
    },
    Signed {
        credential_id: &'a str,
        message: &'a [u8],
        tag: &'a [u8],
    },
}
#[async_trait]
pub trait DeviceAuthenticator: Send + Sync {
    async fn authenticate(&self, request: AuthenticationRequest<'_>)
    -> Result<AuthenticatedDevice>;
}
struct Entry {
    key: [u8; 32],
    secret_hash: [u8; 32],
    identity: AuthenticatedDevice,
}
pub struct StaticAuthenticator {
    credentials: HashMap<String, Entry>,
}
pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() > 256 || !s.len().is_multiple_of(2) {
        return Err(Error::Invalid);
    }
    s.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let a = (pair[0] as char).to_digit(16).ok_or(Error::Invalid)?;
            let b = (pair[1] as char).to_digit(16).ok_or(Error::Invalid)?;
            Ok((a * 16 + b) as u8)
        })
        .collect()
}
pub fn encode_hex(s: &[u8]) -> String {
    s.iter().map(|b| format!("{b:02x}")).collect()
}
impl StaticAuthenticator {
    pub fn new(credentials: Vec<Credential>, limits: &Limits) -> Result<Arc<Self>> {
        if credentials.is_empty() || credentials.len() > limits.max_devices {
            return Err(Error::Configuration);
        }
        let mut entries = HashMap::new();
        let mut devices = std::collections::HashSet::new();
        let mut tenants = HashMap::<TenantId, usize>::new();
        for credential in credentials {
            if DeviceId::new(&credential.credential_id).is_err()
                || credential.secret_hex.len() != 64
                || credential.identity.credential_version == 0
                || !devices.insert(credential.identity.device_key.clone())
            {
                return Err(Error::Configuration);
            }
            let count = tenants
                .entry(credential.identity.device_key.tenant_id.clone())
                .or_default();
            *count += 1;
            if *count > limits.max_devices_per_tenant {
                return Err(Error::Configuration);
            }
            let key: [u8; 32] = decode_hex(&credential.secret_hex)?
                .try_into()
                .map_err(|_| Error::Configuration)?;
            let secret_hash = Sha256::digest(credential.secret_hex.as_bytes()).into();
            if entries
                .insert(
                    credential.credential_id,
                    Entry {
                        key,
                        secret_hash,
                        identity: credential.identity,
                    },
                )
                .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        Ok(Arc::new(Self {
            credentials: entries,
        }))
    }
}
#[async_trait]
impl DeviceAuthenticator for StaticAuthenticator {
    async fn authenticate(
        &self,
        request: AuthenticationRequest<'_>,
    ) -> Result<AuthenticatedDevice> {
        let id = match request {
            AuthenticationRequest::Secret { credential_id, .. }
            | AuthenticationRequest::Signed { credential_id, .. } => credential_id,
        };
        let entry = self.credentials.get(id).ok_or(Error::Authentication)?;
        match request {
            AuthenticationRequest::Secret { secret, .. } => {
                if secret.len() != 64
                    || !bool::from(entry.secret_hash.ct_eq(&Sha256::digest(secret)))
                {
                    return Err(Error::Authentication);
                }
            }
            AuthenticationRequest::Signed { message, tag, .. } => {
                let mut mac = Hmac::<Sha256>::new_from_slice(&entry.key)
                    .map_err(|_| Error::Authentication)?;
                mac.update(message);
                mac.verify_slice(tag).map_err(|_| Error::Authentication)?;
            }
        }
        Ok(entry.identity.clone())
    }
}

/// Separate business API authorization; device secrets cannot enqueue commands.
pub struct AdminAccess {
    hash: [u8; 32],
    identities: HashMap<DeviceKey, AuthenticatedDevice>,
}
impl AdminAccess {
    pub fn new(
        secret: &str,
        identities: HashMap<DeviceKey, AuthenticatedDevice>,
        limits: &Limits,
    ) -> Result<Self> {
        if secret.len() != 64
            || decode_hex(secret)?.len() != 32
            || identities.len() > limits.max_devices
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            hash: Sha256::digest(secret.as_bytes()).into(),
            identities,
        })
    }
    pub fn verify(&self, secret: &[u8]) -> Result<()> {
        if secret.len() == 64 && bool::from(self.hash.ct_eq(&Sha256::digest(secret))) {
            Ok(())
        } else {
            Err(Error::Authentication)
        }
    }
    pub fn identity(&self, device: &DeviceKey) -> Option<&AuthenticatedDevice> {
        self.identities.get(device)
    }
}
