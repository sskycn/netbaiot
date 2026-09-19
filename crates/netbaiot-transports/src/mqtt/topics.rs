use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicKind {
    Up,
    UpAck,
    Down,
    DownAck,
}
pub fn topic(device: &DeviceKey, kind: TopicKind) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/{}",
        device.tenant_id.as_str(),
        device.product_id.as_str(),
        device.device_id.as_str(),
        match kind {
            TopicKind::Up => "up",
            TopicKind::UpAck => "up_ack",
            TopicKind::Down => "down",
            TopicKind::DownAck => "down_ack",
        }
    )
}
pub fn publish_acl(auth: &AuthenticatedDevice, value: &str) -> Result<TopicKind> {
    if !auth.permissions.publish {
        return Err(Error::Forbidden);
    }
    if value == topic(&auth.device_key, TopicKind::Up) {
        Ok(TopicKind::Up)
    } else if auth.permissions.commands && value == topic(&auth.device_key, TopicKind::DownAck) {
        Ok(TopicKind::DownAck)
    } else {
        Err(Error::Forbidden)
    }
}
pub fn subscribe_acl(auth: &AuthenticatedDevice, value: &str) -> bool {
    (auth.permissions.commands && value == topic(&auth.device_key, TopicKind::Down))
        || value == topic(&auth.device_key, TopicKind::UpAck)
}
struct Subscription {
    device: DeviceKey,
    generation: u64,
    qos: u8,
}
/// Exact topic index: at most one authorized device owns a canonical topic.
pub struct Subscriptions {
    limits: Arc<Limits>,
    entries: Mutex<HashMap<String, Subscription>>,
}
impl Subscriptions {
    pub fn count(&self) -> Result<usize> {
        Ok(lock(&self.entries)?.len())
    }

    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            limits,
            entries: Mutex::new(HashMap::new()),
        })
    }
    pub fn subscribe(
        &self,
        auth: &AuthenticatedDevice,
        generation: u64,
        value: &str,
        qos: u8,
    ) -> Result<u8> {
        if qos > 1 || !subscribe_acl(auth, value) {
            return Err(Error::Forbidden);
        }
        let mut map = lock(&self.entries)?;
        if let Some(old) = map.get(value)
            && old.generation > generation
        {
            return Err(Error::Forbidden);
        }
        if !map.contains_key(value) {
            let device = map.values().filter(|s| s.device == auth.device_key).count();
            let tenant = map
                .values()
                .filter(|s| s.device.tenant_id == auth.device_key.tenant_id)
                .count();
            if map.len() >= self.limits.max_subscriptions
                || device
                    >= self
                        .limits
                        .max_subscriptions_per_device
                        .min(self.limits.max_subscriptions_per_connection)
                || tenant >= self.limits.max_subscriptions_per_tenant
            {
                return Err(Error::Overloaded);
            }
        }
        map.insert(
            value.to_owned(),
            Subscription {
                device: auth.device_key.clone(),
                generation,
                qos,
            },
        );
        Ok(qos)
    }
    pub fn lookup(&self, value: &str, generation: u64) -> Result<Option<u8>> {
        Ok(lock(&self.entries)?
            .get(value)
            .filter(|s| s.generation == generation)
            .map(|s| s.qos))
    }
    pub fn unsubscribe(&self, value: &str, generation: u64) -> Result<()> {
        let mut map = lock(&self.entries)?;
        if map.get(value).is_some_and(|s| s.generation == generation) {
            map.remove(value);
        }
        Ok(())
    }
    pub fn remove_session(&self, device: &DeviceKey, generation: u64) {
        if let Ok(mut map) = self.entries.lock() {
            map.retain(|_, s| s.device != *device || s.generation != generation);
        }
    }
}
pub struct SubscriptionLease {
    pub registry: Arc<Subscriptions>,
    pub device: DeviceKey,
    pub generation: u64,
}
impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        self.registry.remove_session(&self.device, self.generation);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_acl_and_stale_cleanup() {
        let l = Arc::new(Limits::default());
        let r = Subscriptions::new(l);
        let auth = AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new("a").unwrap(),
            },
            credential_version: 1,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        };
        let down = topic(&auth.device_key, TopicKind::Down);
        assert!(r.subscribe(&auth, 1, "v1/t/t/p/p/d/b/down", 1).is_err());
        assert!(r.subscribe(&auth, 1, "#", 1).is_err());
        r.subscribe(&auth, 1, &down, 1).unwrap();
        r.subscribe(&auth, 2, &down, 0).unwrap();
        r.remove_session(&auth.device_key, 1);
        assert_eq!(r.lookup(&down, 2).unwrap(), Some(0));
    }
}
