use crate::{Error, Limits, Result, RouteDefinition, lock};
use async_trait::async_trait;
use netbaiot_core::{CodecId, DeviceKey, ProductId, TenantId};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductRuntimeConfig {
    pub tenant_id: TenantId,
    pub product_id: ProductId,
    pub codec_id: CodecId,
    pub codec_version: u16,
    pub revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfigSnapshot {
    pub device: DeviceKey,
    pub revision: u64,
    pub payload: Arc<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlSnapshot {
    pub revision: u64,
    pub products: Vec<ProductRuntimeConfig>,
    pub devices: Vec<DeviceConfigSnapshot>,
    pub routes: Vec<RouteDefinition>,
}

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
    devices: HashMap<DeviceKey, Arc<DeviceConfigSnapshot>>,
    routes: Arc<Vec<RouteDefinition>>,
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
            if device.revision == 0
                || !products.contains_key(&(
                    device.device.tenant_id.clone(),
                    device.device.product_id.clone(),
                ))
            {
                return Err(Error::Configuration);
            }
            bytes = bytes
                .checked_add(
                    serde_json::to_vec(&device)
                        .map_err(|_| Error::Configuration)?
                        .len(),
                )
                .ok_or(Error::Overloaded)?;
            if devices
                .insert(device.device.clone(), Arc::new(device))
                .is_some()
            {
                return Err(Error::Configuration);
            }
        }
        bytes = bytes
            .checked_add(
                products
                    .values()
                    .map(|product| serde_json::to_vec(product).map_or(usize::MAX, |v| v.len()))
                    .sum::<usize>(),
            )
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
            .cloned();
        let mut stats = lock(&self.stats)?;
        if value.is_some() {
            stats.0 += 1;
        } else {
            stats.1 += 1;
        }
        Ok(value)
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
        devices.remove(key);
        let next = Arc::new(SnapshotIndex {
            revision: current.revision,
            products: current.products.clone(),
            devices,
            routes: current.routes.clone(),
            bytes: current.bytes,
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
        let next = Arc::new(SnapshotIndex {
            revision,
            products: current.products.clone(),
            devices: current.devices.clone(),
            routes: Arc::new(routes),
            bytes: current.bytes,
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
    use netbaiot_core::{DeviceId, SinkId};

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
                revision,
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
        assert_eq!(first.revision, 1);
        assert_eq!(second.revision, 2);
    }
}
