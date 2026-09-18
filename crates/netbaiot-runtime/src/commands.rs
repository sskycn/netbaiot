use crate::*;
use netbaiot_core::*;
use std::sync::Arc;
pub struct CommandRouter { pub ingress:Arc<Ingress> }
impl CommandRouter {
    pub async fn queue(&self,auth:&AuthenticatedDevice,command:DeviceCommand)->Result<CommandRecord> {
        if self.ingress.is_draining(){return Err(Error::Draining);}if command.device!=auth.device_key||!auth.permissions.commands {return Err(Error::Forbidden);}
        let now=now_ms();if command.expires_at<=now||command.expires_at.saturating_sub(now)>self.ingress.limits.command_ttl_ms as i64{return Err(Error::Invalid);}
        let encoded=self.ingress.codecs.get(auth)?.encode(&EncodeContext{device:&command.device},&command).map_err(|_|Error::Codec)?;
        if encoded.len()>self.ingress.limits.max_command_bytes {return Err(Error::Invalid);}
        let record=deadline(self.ingress.limits.external_timeout_ms,self.ingress.store.insert_command(command)).await?;self.ingress.metrics.inc(Metric::CommandQueued);Ok(record)
    }
    pub async fn dispatch(&self,command:DeviceCommand,auth:&AuthenticatedDevice)->Result<()> {
        if self.ingress.is_draining(){return Err(Error::Draining);}if command.expires_at<=now_ms(){return Err(Error::Invalid);}
        let endpoint=self.ingress.sessions.lookup(&command.device)?.ok_or(Error::Unavailable)?;
        let bytes=self.ingress.codecs.get(auth)?.encode(&EncodeContext{device:&command.device},&command).map_err(|_|Error::Codec)?;
        endpoint.enqueue(command,bytes).inspect_err(|_|self.ingress.metrics.inc(Metric::QueueRejects))
    }
    pub async fn state(&self,device:&DeviceKey,id:CommandId,state:DeliveryState)->Result<()> {
        deadline(self.ingress.limits.external_timeout_ms,self.ingress.store.command_state(device,id,state)).await?;
        match state {DeliveryState::Sent=>self.ingress.metrics.inc(Metric::CommandSent),DeliveryState::Received=>self.ingress.metrics.inc(Metric::CommandAcked),DeliveryState::Failed|DeliveryState::Expired=>self.ingress.metrics.inc(Metric::CommandFailed),_=>{}}Ok(())
    }
    pub async fn pull(&self,auth:&AuthenticatedDevice)->Result<Option<DeviceCommand>> {
        if self.ingress.is_draining(){return Err(Error::Draining);}if !auth.permissions.commands{return Err(Error::Forbidden);}
        let records=deadline(self.ingress.limits.external_timeout_ms,self.ingress.store.claim_commands(Some(&auth.device_key),now_ms(),1)).await?;
        Ok(records.into_iter().next().map(|r|r.command))
    }
}
