use crate::*;
use netbaiot_core::*;
use std::{collections::HashMap,sync::Arc};
use tokio::sync::{mpsc,OwnedSemaphorePermit,Semaphore};
use tokio_util::sync::CancellationToken;

pub struct QueuedCommand { pub command:DeviceCommand,pub bytes:Vec<u8>,_bytes:Vec<BytesPermit>,_slot:OwnedSemaphorePermit }
#[derive(Clone)]
pub struct SessionEndpoint { pub generation:u64,pub transport:Transport,pub cancel:CancellationToken, sender:mpsc::Sender<QueuedCommand>,slots:Arc<Semaphore>,connection_bytes:ByteBudget,tenant_bytes:ByteBudget,global_bytes:ByteBudget }
impl SessionEndpoint {
    pub fn enqueue(&self,command:DeviceCommand,bytes:Vec<u8>)->Result<()> {
        if self.cancel.is_cancelled() {return Err(Error::Unavailable);}
        let slot=self.slots.clone().try_acquire_owned().map_err(|_|Error::Overloaded)?;
        let permits=vec![self.connection_bytes.reserve(bytes.len())?,self.tenant_bytes.reserve(bytes.len())?,self.global_bytes.reserve(bytes.len())?];
        self.sender.try_send(QueuedCommand{command,bytes,_bytes:permits,_slot:slot}).map_err(|_|Error::Overloaded)
    }
}
struct SessionState { generation:u64,sessions:HashMap<DeviceKey,SessionEndpoint>,tenants:HashMap<TenantId,(usize,ByteBudget)>,presence:HashMap<DeviceKey,Presence> }
pub struct Sessions { state:Mutex<SessionState>,limits:Arc<Limits>,global_bytes:ByteBudget }
pub struct SessionLease { owner:Arc<Sessions>,pub device:DeviceKey,pub generation:u64,pub cancel:CancellationToken }
impl Sessions {
    pub fn new(limits:Arc<Limits>)->Arc<Self> { Arc::new(Self{state:Mutex::new(SessionState{generation:0,sessions:HashMap::new(),tenants:HashMap::new(),presence:HashMap::new()}),global_bytes:ByteBudget::new(limits.max_outbound_bytes),limits}) }
    pub fn register(self:&Arc<Self>,device:&DeviceKey,transport:Transport)->Result<(SessionLease,mpsc::Receiver<QueuedCommand>)> {
        if !matches!(transport,Transport::Mqtt|Transport::Tcp) {return Err(Error::Invalid);}
        let mut s=lock(&self.state)?;
        let replacing=s.sessions.contains_key(device);
        if !replacing && s.sessions.len()>=self.limits.max_connections {return Err(Error::Overloaded);}
        if !s.presence.contains_key(device)&&s.presence.len()>=self.limits.max_devices {return Err(Error::Overloaded);}
        let generation=s.generation.checked_add(1).ok_or(Error::Overloaded)?;
        let tenant=s.tenants.entry(device.tenant_id.clone()).or_insert_with(||(0,ByteBudget::new(self.limits.max_outbound_bytes_per_tenant)));
        if !replacing&&tenant.0>=self.limits.max_connections_per_tenant {return Err(Error::Overloaded);}
        if !replacing {tenant.0+=1;} let tenant_bytes=tenant.1.clone();
        let (sender,receiver)=mpsc::channel(self.limits.max_outbound_messages_per_connection);
        let cancel=CancellationToken::new();
        let endpoint=SessionEndpoint{generation,transport,cancel:cancel.clone(),sender,slots:Arc::new(Semaphore::new(self.limits.max_outbound_messages_per_connection)),connection_bytes:ByteBudget::new(self.limits.max_outbound_bytes_per_connection),tenant_bytes,global_bytes:self.global_bytes.clone()};
        if let Some(old)=s.sessions.insert(device.clone(),endpoint) {old.cancel.cancel();}
        s.generation=generation;
        s.presence.insert(device.clone(),Presence{connected:true,last_seen:now_ms(),transport,session_generation:Some(generation)});
        Ok((SessionLease{owner:self.clone(),device:device.clone(),generation,cancel},receiver))
    }
    pub fn lookup(&self,device:&DeviceKey)->Result<Option<SessionEndpoint>> {Ok(lock(&self.state)?.sessions.get(device).cloned())}
    pub fn touch(&self,device:&DeviceKey,transport:Transport)->Result<()> {
        let mut s=lock(&self.state)?;
        if !s.presence.contains_key(device)&&s.presence.len()>=self.limits.max_devices {return Err(Error::Overloaded);}
        s.presence.entry(device.clone()).and_modify(|p|{p.last_seen=now_ms();if !p.connected {p.transport=transport;}}).or_insert(Presence{connected:false,last_seen:now_ms(),transport,session_generation:None}); Ok(())
    }
    pub fn presence(&self,device:&DeviceKey)->Result<Option<Presence>> {Ok(lock(&self.state)?.presence.get(device).cloned())}
    pub fn expire_presence(&self,now:i64)->Result<()> {let mut s=lock(&self.state)?;s.presence.retain(|_,p|p.connected||now.saturating_sub(p.last_seen)<self.limits.dedup_ttl_ms as i64);Ok(())}
    pub fn queued_bytes(&self)->usize {self.limits.max_outbound_bytes-self.global_bytes.available()}
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        if let Ok(mut s)=self.owner.state.lock() {
            if s.sessions.get(&self.device).is_some_and(|e|e.generation==self.generation) {
                s.sessions.remove(&self.device);self.cancel.cancel();
                if let Some(t)=s.tenants.get_mut(&self.device.tenant_id) {t.0=t.0.saturating_sub(1);if t.0==0{s.tenants.remove(&self.device.tenant_id);}}
                if let Some(p)=s.presence.get_mut(&self.device) {p.connected=false;p.last_seen=now_ms();p.session_generation=None;}
            }
        }
    }
}
#[cfg(test)] mod tests {
    use super::*;
    fn key()->DeviceKey {DeviceKey{tenant_id:TenantId::new("t").unwrap(),product_id:ProductId::new("p").unwrap(),device_id:DeviceId::new("d").unwrap()}}
    #[test] fn stale_disconnect_cannot_remove_new_generation() {
        let s=Sessions::new(Arc::new(Limits::default()));let d=key();let(old,_)=s.register(&d,Transport::Mqtt).unwrap();let(new,_)=s.register(&d,Transport::Mqtt).unwrap();assert!(old.cancel.is_cancelled());assert!(new.generation>old.generation);drop(old);assert_eq!(s.lookup(&d).unwrap().unwrap().generation,new.generation);drop(new);assert!(s.lookup(&d).unwrap().is_none());
    }
    #[test] fn queue_count_and_bytes_are_bounded() {
        let l=Limits{max_outbound_messages_per_connection:1,max_outbound_bytes_per_connection:16,..Limits::default()};let s=Sessions::new(Arc::new(l));let d=key();let(_lease,mut rx)=s.register(&d,Transport::Tcp).unwrap();let ep=s.lookup(&d).unwrap().unwrap();let c=DeviceCommand{command_id:CommandId::generate(),device:d,expires_at:100,payload:DeviceCommandPayload{name:"x".into(),arguments:Default::default()}};
        assert!(ep.enqueue(c.clone(),vec![0;17]).is_err());ep.enqueue(c.clone(),vec![0;16]).unwrap();assert!(ep.enqueue(c.clone(),vec![0]).is_err());let queued=rx.try_recv().unwrap();assert!(ep.enqueue(c.clone(),vec![0]).is_err());drop(queued);ep.enqueue(c,vec![0]).unwrap();
    }
}
