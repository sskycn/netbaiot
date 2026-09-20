use crate::{Error, Limits, Result, lock};
use async_trait::async_trait;
use netbaiot_core::{
    ControlSnapshot, DeviceConfigSnapshot, DeviceKey, ProductId, ProductRuntimeConfig,
    RouteDefinition, TenantId,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

#[async_trait]
pub trait ControlPlaneProvider: Send + Sync {
    async fn bootstrap(&self) -> Result<ControlSnapshot>;
}

pub struct StaticControlPlane {
    snapshot: ControlSnapshot,
}

impl StaticControlPlane {
    pub fn new(snapshot: ControlSnapshot) -> Self {
        Self { snapshot }
    }
}

#[async_trait]
impl ControlPlaneProvider for StaticControlPlane {
    async fn bootstrap(&self) -> Result<ControlSnapshot> {
        Ok(self.snapshot.clone())
    }
}

struct SnapshotIndex {
    revision: u64,
    products: HashMap<(TenantId, ProductId), Arc<ProductRuntimeConfig>>,
    devices: HashMap<DeviceKey, DeviceEntry>,
    routes: Arc<Vec<RouteDefinition>>,
    bytes: usize,
    product_bytes: usize,
    route_bytes: usize,
}

#[derive(Clone)]
struct DeviceEntry {
    value: Arc<DeviceConfigSnapshot>,
    bytes: usize,
}

/// A separately bounded, atomically replaced runtime configuration snapshot.
pub struct ConfigCache {
    limits: Arc<Limits>,
    snapshot: RwLock<Arc<SnapshotIndex>>,
    // Hit/miss/invalidations are local counters so the cache has no labels.
    stats: Mutex<(u64, u64, u64)>,
}

impl ConfigCache {
    pub fn empty(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            limits,
            snapshot: RwLock::new(Arc::new(SnapshotIndex {
                revision: 0,
                products: HashMap::new(),
                devices: HashMap::new(),
                routes: Arc::new(Vec::new()),
                bytes: 0,
                product_bytes: 0,
                route_bytes: 0,
            })),
            stats: Mutex::new((0, 0, 0)),
        })
    }

    pub fn apply(&self, snapshot: ControlSnapshot) -> Result<()> {
        let current = self.snapshot.read().map_err(|_| Error::Internal)?;
        if snapshot.revision <= current.revision {
            return Err(Error::Conflict);
        }
        drop(current);
        if snapshot
            .products
            .len()
            .saturating_add(snapshot.devices.len())
            > self.limits.config_cache_max_entries
            || snapshot.routes.len() > self.limits.max_routing_filters
        {
            return Err(Error::Overloaded);
        }
        let mut products = HashMap::new();
        for product in snapshot.products {
            if product.revision == 0
                || product.codec_version == 0
                || products
                    .insert(
                        (product.tenant_id.clone(), product.product_id.clone()),
                        Arc::new(product),
                    )
                    .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        let mut devices = HashMap::new();
        let mut bytes = 0usize;
        for device in snapshot.devices {
            if !products.contains_key(&(
                device.device.tenant_id.clone(),
                device.device.product_id.clone(),
            )) {
                return Err(Error::Configuration);
            }
            let device_bytes = serde_json::to_vec(&device)
                .map_err(|_| Error::Configuration)?
                .len();
            bytes = bytes.checked_add(device_bytes).ok_or(Error::Overloaded)?;
            if devices
                .insert(
                    device.device.clone(),
                    DeviceEntry {
                        value: Arc::new(device),
                        bytes: device_bytes,
                    },
                )
                .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        let product_bytes = products.values().try_fold(0usize, |total, product| {
            total
                .checked_add(
                    serde_json::to_vec(product.as_ref())
                        .map_err(|_| Error::Configuration)?
                        .len(),
                )
                .ok_or(Error::Overloaded)
        })?;
        let route_bytes = serde_json::to_vec(&snapshot.routes)
            .map_err(|_| Error::Configuration)?
            .len();
        bytes = bytes
            .checked_add(product_bytes)
            .and_then(|value| value.checked_add(route_bytes))
            .ok_or(Error::Overloaded)?;
        if bytes > self.limits.config_cache_max_bytes {
            return Err(Error::Overloaded);
        }
        let next = Arc::new(SnapshotIndex {
            revision: snapshot.revision,
            products,
            devices,
            routes: Arc::new(snapshot.routes),
            bytes,
            product_bytes,
            route_bytes,
        });
        *self.snapshot.write().map_err(|_| Error::Internal)? = next;
        Ok(())
    }

    pub fn device(&self, key: &DeviceKey) -> Result<Option<Arc<DeviceConfigSnapshot>>> {
        let value = self
            .snapshot
            .read()
            .map_err(|_| Error::Internal)?
            .devices
            .get(key)
            .map(|entry| entry.value.clone());
        let mut stats = lock(&self.stats)?;
        if value.is_some() {
            stats.0 += 1;
        } else {
            stats.1 += 1;
        }
        Ok(value)
    }

    /// Atomically replaces one public device configuration. The caller serializes
    /// this with other control-plane mutations.
    pub fn upsert_device(&self, config: DeviceConfigSnapshot) -> Result<()> {
        let current = self.snapshot.read().map_err(|_| Error::Internal)?.clone();
        if !current.products.contains_key(&(
            config.device.tenant_id.clone(),
            config.device.product_id.clone(),
        )) {
            return Err(Error::Configuration);
        }
        if current
            .devices
            .get(&config.device)
            .is_some_and(|old| old.value.revision >= config.revision)
        {
            return Err(Error::Conflict);
        }
        if !current.devices.contains_key(&config.device)
            && current.products.len().saturating_add(current.devices.len())
                >= self.limits.config_cache_max_entries
        {
            return Err(Error::Overloaded);
        }
        let entry_bytes = serde_json::to_vec(&config)
            .map_err(|_| Error::Configuration)?
            .len();
        let mut devices = current.devices.clone();
        let previous = devices.insert(
            config.device.clone(),
            DeviceEntry {
                value: Arc::new(config),
                bytes: entry_bytes,
            },
        );
        let bytes = current
            .bytes
            .checked_sub(previous.as_ref().map_or(0, |entry| entry.bytes))
            .and_then(|value| value.checked_add(entry_bytes))
            .ok_or(Error::Overloaded)?;
        if bytes > self.limits.config_cache_max_bytes {
            return Err(Error::Overloaded);
        }
        let next = Arc::new(SnapshotIndex {
            revision: current.revision.checked_add(1).ok_or(Error::Overloaded)?,
            products: current.products.clone(),
            devices,
            routes: current.routes.clone(),
            bytes,
            product_bytes: current.product_bytes,
            route_bytes: current.route_bytes,
        });
        *self.snapshot.write().map_err(|_| Error::Internal)? = next;
        Ok(())
    }

    pub fn product(
        &self,
        tenant: &TenantId,
        product: &ProductId,
    ) -> Result<Option<Arc<ProductRuntimeConfig>>> {
        Ok(self
            .snapshot
            .read()
            .map_err(|_| Error::Internal)?
            .products
            .get(&(tenant.clone(), product.clone()))
            .cloned())
    }

    pub fn invalidate_device(&self, key: &DeviceKey) -> Result<bool> {
        let current = self.snapshot.read().map_err(|_| Error::Internal)?.clone();
        if !current.devices.contains_key(key) {
            return Ok(false);
        }
        let mut devices = current.devices.clone();
        let removed = devices.remove(key).ok_or(Error::Internal)?;
        let next = Arc::new(SnapshotIndex {
            revision: current.revision,
            products: current.products.clone(),
            devices,
            routes: current.routes.clone(),
            bytes: current.bytes.saturating_sub(removed.bytes),
            product_bytes: current.product_bytes,
            route_bytes: current.route_bytes,
        });
        *self.snapshot.write().map_err(|_| Error::Internal)? = next;
        lock(&self.stats)?.2 += 1;
        Ok(true)
    }

    pub fn replace_routes(&self, revision: u64, routes: Vec<RouteDefinition>) -> Result<()> {
        if routes.len() > self.limits.max_routing_filters {
            return Err(Error::Overloaded);
        }
        let current = self.snapshot.read().map_err(|_| Error::Internal)?.clone();
        if revision <= current.revision {
            return Err(Error::Conflict);
        }
        let route_bytes = serde_json::to_vec(&routes)
            .map_err(|_| Error::Configuration)?
            .len();
        let bytes = current
            .bytes
            .checked_sub(current.route_bytes)
            .and_then(|value| value.checked_add(route_bytes))
            .ok_or(Error::Overloaded)?;
        if bytes > self.limits.config_cache_max_bytes {
            return Err(Error::Overloaded);
        }
        let next = Arc::new(SnapshotIndex {
            revision,
            products: current.products.clone(),
            devices: current.devices.clone(),
            routes: Arc::new(routes),
            bytes,
            product_bytes: current.product_bytes,
            route_bytes,
        });
        *self.snapshot.write().map_err(|_| Error::Internal)? = next;
        Ok(())
    }

    pub fn revision(&self) -> Result<u64> {
        Ok(self.snapshot.read().map_err(|_| Error::Internal)?.revision)
    }

    pub fn routes(&self) -> Result<Arc<Vec<RouteDefinition>>> {
        Ok(self
            .snapshot
            .read()
            .map_err(|_| Error::Internal)?
            .routes
            .clone())
    }

    pub fn usage(&self) -> Result<(usize, usize)> {
        let snapshot = self.snapshot.read().map_err(|_| Error::Internal)?;
        Ok((
            snapshot.products.len() + snapshot.devices.len(),
            snapshot.bytes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::{CodecId, ConfigRevision, DeviceId, SinkId};

    fn snapshot(revision: u64) -> ControlSnapshot {
        let tenant = TenantId::new("t").unwrap();
        let product = ProductId::new("p").unwrap();
        ControlSnapshot {
            revision,
            products: vec![ProductRuntimeConfig {
                tenant_id: tenant.clone(),
                product_id: product.clone(),
                codec_id: CodecId::new("json").unwrap(),
                codec_version: 1,
                revision,
            }],
            devices: vec![DeviceConfigSnapshot {
                device: DeviceKey {
                    tenant_id: tenant,
                    product_id: product,
                    device_id: DeviceId::new("d").unwrap(),
                },
                revision: ConfigRevision::new(revision).unwrap(),
                payload: Arc::new(serde_json::json!({"sample": 1})),
            }],
            routes: vec![RouteDefinition {
                tenant: None,
                sinks: vec![SinkId::new("sink").unwrap()],
            }],
        }
    }

    #[test]
    fn snapshot_replacement_is_validated_and_atomic() {
        let cache = ConfigCache::empty(Arc::new(Limits::default()));
        cache.apply(snapshot(1)).unwrap();
        assert!(matches!(cache.apply(snapshot(1)), Err(Error::Conflict)));
        assert_eq!(cache.revision().unwrap(), 1);
        let device = snapshot(2).devices.remove(0).device;
        let first = cache.device(&device).unwrap().unwrap();
        cache.apply(snapshot(2)).unwrap();
        let second = cache.device(&device).unwrap().unwrap();
        assert_eq!(first.revision, ConfigRevision::new(1).unwrap());
        assert_eq!(second.revision, ConfigRevision::new(2).unwrap());
    }

    #[test]
    fn invalidation_releases_exact_byte_charge_across_reinsert_cycles() {
        let cache = ConfigCache::empty(Arc::new(Limits::default()));
        cache.apply(snapshot(1)).unwrap();
        let key = snapshot(2).devices.remove(0).device;
        let (_, populated_bytes) = cache.usage().unwrap();
        assert!(cache.invalidate_device(&key).unwrap());
        let (_, invalidated_bytes) = cache.usage().unwrap();
        assert!(invalidated_bytes < populated_bytes);
        assert!(!cache.invalidate_device(&key).unwrap());
        assert_eq!(cache.usage().unwrap().1, invalidated_bytes);

        for revision in 2..=20 {
            let mut config = snapshot(revision).devices.remove(0);
            config.payload = Arc::new(serde_json::json!({"revision": revision}));
            cache.upsert_device(config).unwrap();
            assert!(cache.usage().unwrap().1 <= populated_bytes + 64);
            assert!(cache.invalidate_device(&key).unwrap());
            assert_eq!(cache.usage().unwrap().1, invalidated_bytes);
        }
    }
}
