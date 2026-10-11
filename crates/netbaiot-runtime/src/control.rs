use crate::{Error, Limits, Result};
use async_trait::async_trait;
use netbaiot_core::{ControlSnapshot, ProductId, ProductRuntimeConfig, RouteDefinition, TenantId};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
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
    routes: Arc<Vec<RouteDefinition>>,
    bytes: usize,
    route_bytes: usize,
}

/// Bounded gateway product profiles and routes, atomically replaced by revision.
/// Contains no per-device desired/reported state or business configuration.
pub struct GatewayControl {
    limits: Arc<Limits>,
    snapshot: RwLock<Arc<SnapshotIndex>>,
}

impl GatewayControl {
    pub fn empty(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            limits,
            snapshot: RwLock::new(Arc::new(SnapshotIndex {
                revision: 0,
                products: HashMap::new(),
                routes: Arc::new(Vec::new()),
                bytes: 0,
                route_bytes: 0,
            })),
        })
    }

    pub fn apply(&self, snapshot: ControlSnapshot, codecs: &crate::CodecRegistry) -> Result<()> {
        if snapshot.products.len() > self.limits.control_max_products
            || snapshot.routes.len() > self.limits.max_routing_filters
        {
            return Err(Error::Overloaded);
        }
        let mut products = HashMap::new();
        for product in snapshot.products {
            if product.revision == 0
                || !codecs.contains(&product.codec_id, product.codec_version)
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
        let bytes = product_bytes
            .checked_add(route_bytes)
            .ok_or(Error::Overloaded)?;
        if bytes > self.limits.control_max_bytes {
            return Err(Error::Overloaded);
        }
        let next = Arc::new(SnapshotIndex {
            revision: snapshot.revision,
            products,
            routes: Arc::new(snapshot.routes),
            bytes,
            route_bytes,
        });
        let mut current = self.snapshot.write().map_err(|_| Error::Internal)?;
        if next.revision <= current.revision {
            return Err(Error::Conflict);
        }
        // V1 does not hot-switch/remove established product codec profiles. A planned
        // restart with reprovisioning and authentication invalidation is required.
        for (key, old) in &current.products {
            let Some(new) = next.products.get(key) else {
                return Err(Error::Conflict);
            };
            if old.codec_id != new.codec_id || old.codec_version != new.codec_version {
                return Err(Error::Conflict);
            }
        }
        *current = next;
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

    pub fn replace_routes(&self, revision: u64, routes: Vec<RouteDefinition>) -> Result<()> {
        if routes.len() > self.limits.max_routing_filters {
            return Err(Error::Overloaded);
        }
        let mut current = self.snapshot.write().map_err(|_| Error::Internal)?;
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
        if bytes > self.limits.control_max_bytes {
            return Err(Error::Overloaded);
        }
        let next = Arc::new(SnapshotIndex {
            revision,
            products: current.products.clone(),
            routes: Arc::new(routes),
            bytes,
            route_bytes,
        });
        *current = next;
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
        Ok((snapshot.products.len(), snapshot.bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::{CodecId, SinkId};

    fn registry() -> crate::CodecRegistry {
        struct Codec;
        impl netbaiot_core::DeviceCodec for Codec {
            fn decode(
                &self,
                _: &netbaiot_core::DecodeContext<'_>,
                _: &[u8],
            ) -> std::result::Result<Vec<netbaiot_core::DeviceEvent>, netbaiot_core::CodecError>
            {
                Err(netbaiot_core::CodecError)
            }
            fn encode(
                &self,
                _: &netbaiot_core::EncodeContext<'_>,
                _: &netbaiot_core::DeviceCommand,
            ) -> std::result::Result<Vec<u8>, netbaiot_core::CodecError> {
                Err(netbaiot_core::CodecError)
            }
        }
        crate::CodecRegistry::new(vec![(CodecId::new("json").unwrap(), 1, Arc::new(Codec))])
            .unwrap()
    }

    fn snapshot(revision: u64) -> ControlSnapshot {
        ControlSnapshot {
            revision,
            products: vec![ProductRuntimeConfig {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                codec_id: CodecId::new("json").unwrap(),
                codec_version: 1,
                revision,
            }],
            routes: vec![RouteDefinition {
                tenant: None,
                sinks: vec![SinkId::new("sink").unwrap()],
            }],
        }
    }

    #[test]
    fn snapshot_replacement_is_validated_atomic_and_shared() {
        let control = GatewayControl::empty(Arc::new(Limits::default()));
        control.apply(snapshot(1), &registry()).unwrap();
        let product = &snapshot(1).products[0];
        let first = control
            .product(&product.tenant_id, &product.product_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            control.apply(snapshot(1), &registry()),
            Err(Error::Conflict)
        ));
        let mut invalid = snapshot(2);
        invalid.products[0].codec_version = 0;
        assert!(matches!(
            control.apply(invalid, &registry()),
            Err(Error::Configuration)
        ));
        let mut duplicate = snapshot(2);
        duplicate.products.push(duplicate.products[0].clone());
        assert!(matches!(
            control.apply(duplicate, &registry()),
            Err(Error::Configuration)
        ));
        assert_eq!(control.revision().unwrap(), 1);
        control.apply(snapshot(2), &registry()).unwrap();
        let second = control
            .product(&product.tenant_id, &product.product_id)
            .unwrap()
            .unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(second.revision, 2);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn product_route_and_byte_limits_roll_back_without_state_change() {
        let control = GatewayControl::empty(Arc::new(Limits {
            control_max_products: 1,
            control_max_bytes: 256,
            max_routing_filters: 1,
            ..Limits::default()
        }));
        control.apply(snapshot(1), &registry()).unwrap();
        let original = control.usage().unwrap();
        let mut too_many = snapshot(2);
        too_many.products.push(too_many.products[0].clone());
        assert!(matches!(
            control.apply(too_many, &registry()),
            Err(Error::Overloaded)
        ));
        let route = snapshot(2).routes.remove(0);
        assert!(matches!(
            control.replace_routes(2, vec![route.clone(), route]),
            Err(Error::Overloaded)
        ));
        let mut oversized = snapshot(2);
        oversized.routes[0].sinks = vec![SinkId::new("large-sink").unwrap(); 32];
        assert!(matches!(
            control.apply(oversized.clone(), &registry()),
            Err(Error::Overloaded)
        ));
        assert!(matches!(
            control.replace_routes(2, oversized.routes),
            Err(Error::Overloaded)
        ));
        assert_eq!(control.revision().unwrap(), 1);
        assert_eq!(control.usage().unwrap(), original);
        let held_routes = control.routes().unwrap();
        control.replace_routes(2, Vec::new()).unwrap();
        assert_eq!(held_routes.len(), 1);
        assert!(control.routes().unwrap().is_empty());
        assert!(control.usage().unwrap().1 < original.1);
        assert!(matches!(
            control.replace_routes(2, Vec::new()),
            Err(Error::Conflict)
        ));
    }
}
