//! In-memory demo. The business application owns real credential and verifier storage.
//! Run with one of: multiplexed, dual, auth_webhook, invalidate.
use async_trait::async_trait;
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig, BusinessRpcTls,
};
use netbaiot_protocol::{
    AuthInvalidation, CodecId, DeviceId, DeviceKey, ProductId, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, ResolveVerifierRequest,
        ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
};
use std::{
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncBufReadExt, BufReader};

struct MemoryAuthority {
    credential_id: String,
    secret_wire_hex: String,
    verifier_key_hex: String,
    identity: DeviceKey,
    enabled: AtomicBool,
    revision: AtomicU64,
}
impl MemoryAuthority {
    fn wire_identity(&self) -> AuthenticatedDeviceWire {
        AuthenticatedDeviceWire {
            device_key: self.identity.clone(),
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("netbaiot-json").expect("fixed demo codec"),
            codec_version: 1,
            publish: true,
            commands: true,
            auth_revision: self.revision.load(Ordering::SeqCst),
        }
    }
    fn authorize(&self, credential_id: &str, minimum: u64) -> Result<(), RpcError> {
        if !self.enabled.load(Ordering::SeqCst) || self.credential_id != credential_id {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "credential rejected",
            ));
        }
        if self.revision.load(Ordering::SeqCst) < minimum {
            return Err(RpcError::new(
                RpcErrorCode::Unavailable,
                "authority revision not ready",
            ));
        }
        Ok(())
    }
}
#[async_trait]
impl BusinessAuthHandler for MemoryAuthority {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> Result<AuthenticatedDeviceWire, RpcError> {
        self.authorize(&request.credential_id, request.min_auth_revision)?;
        if self.secret_wire_hex != request.secret_hex {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "credential rejected",
            ));
        }
        Ok(self.wire_identity())
    }
    async fn resolve_verifier(
        &self,
        request: ResolveVerifierRequest,
    ) -> Result<ResolveVerifierResponse, RpcError> {
        self.authorize(&request.credential_id, request.min_auth_revision)?;
        Ok(ResolveVerifierResponse {
            identity: self.wire_identity(),
            verifier_key_hex: self.verifier_key_hex.clone(),
        })
    }
}
fn config(
    address: SocketAddr,
    role: BusinessRole,
) -> Result<BusinessRpcClientConfig, Box<dyn std::error::Error>> {
    let mut config = BusinessRpcClientConfig::development(
        address,
        env::var("NETBAIOT_BUSINESS_RPC_TOKEN")?,
        role,
    );
    if let Ok(ca) = env::var("NETBAIOT_BUSINESS_RPC_CA_PEM") {
        config.token = None;
        config.tls = Some(BusinessRpcTls {
            server_name: env::var("NETBAIOT_BUSINESS_RPC_SERVER_NAME")?,
            ca_pem: PathBuf::from(ca),
            certificate_pem: PathBuf::from(env::var("NETBAIOT_BUSINESS_RPC_CLIENT_CERT_PEM")?),
            private_key_pem: PathBuf::from(env::var("NETBAIOT_BUSINESS_RPC_CLIENT_KEY_PEM")?),
        });
    }
    Ok(config)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = env::args()
        .nth(1)
        .ok_or("mode required: multiplexed, dual, auth_webhook, invalidate")?;
    let address: SocketAddr = env::var("NETBAIOT_BUSINESS_RPC_ADDRESS")?.parse()?;
    let secret = env::var("DEMO_DEVICE_SECRET")?;
    let secret_wire_hex = secret
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let verifier_key_hex = env::var("DEMO_VERIFIER_KEY_HEX")?;
    if verifier_key_hex.len() != 64
        || !verifier_key_hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("DEMO_VERIFIER_KEY_HEX must contain exactly 32 hex bytes".into());
    }
    let authority = Arc::new(MemoryAuthority {
        credential_id: env::var("DEMO_CREDENTIAL_ID")?,
        secret_wire_hex,
        verifier_key_hex,
        identity: DeviceKey {
            tenant_id: TenantId::new(env::var("DEMO_TENANT_ID")?)?,
            product_id: ProductId::new(env::var("DEMO_PRODUCT_ID")?)?,
            device_id: DeviceId::new(env::var("DEMO_DEVICE_ID")?)?,
        },
        enabled: AtomicBool::new(true),
        revision: AtomicU64::new(1),
    });
    let role = match mode.as_str() {
        "multiplexed" => BusinessRole::Multiplexed,
        "dual" | "auth_webhook" | "invalidate" => BusinessRole::AuthControl,
        _ => return Err("unknown mode".into()),
    };
    let (auth, mut auth_events) =
        BusinessRpcClient::connect(config(address, role)?, Some(authority.clone()))?;
    auth.wait_ready().await?;
    let mut events_only = None;
    if mode == "dual" {
        let (events, receiver) =
            BusinessRpcClient::connect(config(address, BusinessRole::Events)?, None)?;
        events.wait_ready().await?;
        events_only = Some(events);
        auth_events = receiver;
    }
    println!("business RPC ready; commands: disable, enable, quit");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        tokio::select! {
            line = lines.next_line() => match line? {
                Some(line) if line == "quit" => break,
                Some(line) if line == "disable" || line == "enable" => {
                    let enabled = line == "enable";
                    authority.enabled.store(enabled, Ordering::SeqCst);
                    let revision = authority.revision.fetch_add(1, Ordering::SeqCst) + 1;
                    let applied = auth.invalidate(revision, AuthInvalidation::Device { device: authority.identity.clone() }).await?;
                    println!("invalidation applied at revision {}", applied.applied_revision);
                }
                Some(_) => println!("commands: disable, enable, quit"),
                None => break,
            },
            item = auth_events.recv(), if mode == "multiplexed" || mode == "dual" => {
                if let Some(delivery) = item {
                    // In a real application, commit the business transaction before this ACK.
                    println!("event {}", delivery.delivery.event.event_id);
                    delivery.ack().await?;
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    if let Some(events) = events_only {
        events.shutdown().await;
    }
    auth.shutdown().await;
    Ok(())
}
