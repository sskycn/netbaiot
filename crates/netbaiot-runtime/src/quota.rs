use crate::*;
use netbaiot_core::*;
use std::{collections::HashMap,net::IpAddr,sync::Arc,time::{Duration,Instant}};
use tokio::sync::{OwnedSemaphorePermit,Semaphore};

#[derive(Clone)]
pub struct ByteBudget(Arc<Semaphore>);
pub struct BytesPermit { _permit:OwnedSemaphorePermit }
impl ByteBudget {
    pub fn new(bytes:usize)->Self { Self(Arc::new(Semaphore::new(bytes))) }
    pub fn reserve(&self,bytes:usize)->Result<BytesPermit> { let n=u32::try_from(bytes).map_err(|_|Error::Overloaded)?; Ok(BytesPermit{_permit:self.0.clone().try_acquire_many_owned(n).map_err(|_|Error::Overloaded)?}) }
    pub fn available(&self)->usize { self.0.available_permits() }
}
#[derive(Default)]
struct ConnectionCounts { ips:HashMap<IpAddr,usize>,tenants:HashMap<TenantId,usize>,transports:[usize;4] }
pub struct Connections { limits:Arc<Limits>, counts:Mutex<ConnectionCounts>, slots:Arc<Semaphore>, memory:ByteBudget,metrics:Arc<Metrics> }
pub struct ConnectionLease { owner:Arc<Connections>,ip:IpAddr,tenant:Option<TenantId>,transport:Transport,_slot:OwnedSemaphorePermit,_bytes:BytesPermit }
impl Connections {
    pub fn new(limits:Arc<Limits>,metrics:Arc<Metrics>)->Arc<Self> { Arc::new(Self{counts:Mutex::new(ConnectionCounts::default()),slots:Arc::new(Semaphore::new(limits.max_connections)),memory:ByteBudget::new(limits.max_network_bytes),limits,metrics}) }
    pub fn acquire(self:&Arc<Self>,ip:IpAddr,transport:Transport)->Result<ConnectionLease> {
        let slot=self.slots.clone().try_acquire_owned().map_err(|_|Error::Overloaded)?; let bytes=self.memory.reserve(self.limits.connection_memory_reservation)?;
        let mut counts=lock(&self.counts)?; let count=counts.ips.entry(ip).or_default();
        if *count>=self.limits.max_connections_per_ip { return Err(Error::Overloaded); } *count+=1; counts.transports[transport as usize]+=1;
        self.metrics.inc(Metric::ConnectionsAccepted);
        Ok(ConnectionLease{owner:self.clone(),ip,tenant:None,transport,_slot:slot,_bytes:bytes})
    }
    pub fn active(&self)->Result<[usize;4]> { Ok(lock(&self.counts)?.transports) }
}
impl ConnectionLease {
    pub fn authenticate(&mut self,tenant:&TenantId)->Result<()> {
        if self.tenant.as_ref()==Some(tenant) { return Ok(()); }
        if self.tenant.is_some() { return Err(Error::Forbidden); }
        let mut counts=lock(&self.owner.counts)?; let count=counts.tenants.entry(tenant.clone()).or_default();
        if *count>=self.owner.limits.max_connections_per_tenant { return Err(Error::Overloaded); } *count+=1; self.tenant=Some(tenant.clone()); Ok(())
    }
}
fn decrement<K:Eq+std::hash::Hash>(map:&mut HashMap<K,usize>,key:&K) { if let Some(count)=map.get_mut(key) { *count=count.saturating_sub(1); if *count==0 { map.remove(key); } } }
impl Drop for ConnectionLease {
    fn drop(&mut self) { if let Ok(mut counts)=self.owner.counts.lock() { decrement(&mut counts.ips,&self.ip); if let Some(tenant)=&self.tenant { decrement(&mut counts.tenants,tenant); } counts.transports[self.transport as usize]=counts.transports[self.transport as usize].saturating_sub(1); } }
}
struct Window { start:Instant,count:usize }
impl Window { fn take(&mut self,limit:usize)->Result<()> { if self.start.elapsed()>=Duration::from_secs(1) {self.start=Instant::now();self.count=0;} if self.count>=limit {return Err(Error::Overloaded);} self.count+=1; Ok(()) } }
/// Rate table capacity and one-second expiry prevent unbounded source-IP state.
pub struct RateLimiter { limits:Arc<Limits>,inner:Mutex<(Window,HashMap<IpAddr,Window>)> }
impl RateLimiter {
    pub fn new(limits:Arc<Limits>)->Self { Self{limits,inner:Mutex::new((Window{start:Instant::now(),count:0},HashMap::new()))} }
    pub fn take(&self,ip:IpAddr)->Result<()> { let mut inner=lock(&self.inner)?; inner.0.take(self.limits.requests_per_second)?; inner.1.retain(|_,w|w.start.elapsed()<Duration::from_secs(1)); if !inner.1.contains_key(&ip)&&inner.1.len()>=self.limits.rate_entries {return Err(Error::Overloaded);} inner.1.entry(ip).or_insert_with(||Window{start:Instant::now(),count:0}).take(self.limits.requests_per_ip_second) }
}
struct AdmissionState { devices:HashMap<DeviceKey,(usize,Window)>,tenants:HashMap<TenantId,(usize,Window)> }
pub struct Admission { limits:Arc<Limits>,state:Mutex<AdmissionState>,slots:Arc<Semaphore>,bytes:ByteBudget }
pub struct AdmissionLease { owner:Arc<Admission>,device:DeviceKey,_slot:OwnedSemaphorePermit,_bytes:BytesPermit }
impl Admission {
    pub fn new(limits:Arc<Limits>)->Arc<Self> { Arc::new(Self{state:Mutex::new(AdmissionState{devices:HashMap::new(),tenants:HashMap::new()}),slots:Arc::new(Semaphore::new(limits.max_ingress)),bytes:ByteBudget::new(limits.max_ingress_bytes),limits}) }
    pub fn acquire(self:&Arc<Self>,device:&DeviceKey,bytes:usize)->Result<AdmissionLease> {
        let slot=self.slots.clone().try_acquire_owned().map_err(|_|Error::Overloaded)?;let bytes=self.bytes.reserve(bytes)?;
        let mut state=lock(&self.state)?;
        state.devices.retain(|_,(n,w)|*n>0||w.start.elapsed()<Duration::from_secs(1)); state.tenants.retain(|_,(n,w)|*n>0||w.start.elapsed()<Duration::from_secs(1));
        if (!state.devices.contains_key(device)&&state.devices.len()>=self.limits.max_devices)||(!state.tenants.contains_key(&device.tenant_id)&&state.tenants.len()>=self.limits.max_devices) {return Err(Error::Overloaded);}
        let d=state.devices.entry(device.clone()).or_insert_with(||(0,Window{start:Instant::now(),count:0}));
        if d.0>=self.limits.max_ingress_per_device {return Err(Error::Overloaded);} d.1.take(self.limits.messages_per_device_second)?;
        let t=state.tenants.entry(device.tenant_id.clone()).or_insert_with(||(0,Window{start:Instant::now(),count:0}));
        if t.0>=self.limits.max_ingress_per_tenant {return Err(Error::Overloaded);} t.1.take(self.limits.messages_per_tenant_second)?;t.0+=1;
        if let Some(d)=state.devices.get_mut(device) {d.0+=1;}
        Ok(AdmissionLease{owner:self.clone(),device:device.clone(),_slot:slot,_bytes:bytes})
    }
}
impl Drop for AdmissionLease { fn drop(&mut self) { if let Ok(mut s)=self.owner.state.lock() { if let Some(d)=s.devices.get_mut(&self.device) { d.0=d.0.saturating_sub(1); } if let Some(t)=s.tenants.get_mut(&self.device.tenant_id) {t.0=t.0.saturating_sub(1);} } } }
