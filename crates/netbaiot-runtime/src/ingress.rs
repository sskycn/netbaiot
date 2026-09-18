use crate::*;
use netbaiot_core::*;
use std::{collections::HashMap,sync::{Arc,atomic::{AtomicBool,Ordering}},time::Instant};

pub struct CodecRegistry { codecs:HashMap<(CodecId,u16),Arc<dyn DeviceCodec>> }
impl CodecRegistry {
    pub fn new(entries:Vec<(CodecId,u16,Arc<dyn DeviceCodec>)>)->Result<Self> {
        if entries.is_empty()||entries.len()>64 {return Err(Error::Configuration);} let mut codecs=HashMap::new();
        for (id,v,c) in entries {if v==0||codecs.insert((id,v),c).is_some(){return Err(Error::Configuration);}}
        Ok(Self{codecs})
    }
    pub fn get(&self,auth:&AuthenticatedDevice)->Result<&Arc<dyn DeviceCodec>> {self.codecs.get(&(auth.codec_id.clone(),auth.codec_version)).ok_or(Error::Codec)}
}
pub struct IngressEnvelope<'a> { pub transport:Transport,pub payload:&'a[u8],pub require_command_ack:bool }
pub struct Ingress {
    pub limits:Arc<Limits>,pub authenticator:Arc<dyn DeviceAuthenticator>,pub codecs:CodecRegistry,pub store:Arc<dyn Store>,pub metrics:Arc<Metrics>,pub sessions:Arc<Sessions>,pub admission:Arc<Admission>,draining:AtomicBool,
}
impl Ingress {
    pub fn new(limits:Arc<Limits>,authenticator:Arc<dyn DeviceAuthenticator>,codecs:CodecRegistry,store:Arc<dyn Store>,metrics:Arc<Metrics>,sessions:Arc<Sessions>)->Self {Self{admission:Admission::new(limits.clone()),limits,authenticator,codecs,store,metrics,sessions,draining:AtomicBool::new(false)}}
    pub fn drain(&self){self.draining.store(true,Ordering::Release);}
    pub fn is_draining(&self)->bool{self.draining.load(Ordering::Acquire)}
    pub async fn authenticate(&self,request:AuthenticationRequest<'_>)->Result<AuthenticatedDevice> {
        if self.is_draining(){return Err(Error::Draining);}let result=deadline(self.limits.authentication_timeout_ms,self.authenticator.authenticate(request)).await;
        if result.is_err(){self.metrics.inc(Metric::AuthFailures);}result
    }
    pub async fn ingest(&self,auth:&AuthenticatedDevice,envelope:IngressEnvelope<'_>)->Result<IngressReceipt> {
        let result=self.ingest_inner(auth,envelope).await;
        if result.is_err(){self.metrics.inc(Metric::IngressRejected);}if matches!(result,Err(Error::Timeout)){self.metrics.inc(Metric::Timeouts);}result
    }
    async fn ingest_inner(&self,auth:&AuthenticatedDevice,envelope:IngressEnvelope<'_>)->Result<IngressReceipt> {
        if self.is_draining(){return Err(Error::Draining);}if !auth.permissions.publish {return Err(Error::Forbidden);}
        let _admission=self.admission.acquire(&auth.device_key,envelope.payload.len())?;
        let codec=self.codecs.get(auth)?;
        let messages=codec.decode(&DecodeContext{device:&auth.device_key,received_at:now_ms()},envelope.payload).map_err(|_|{self.metrics.inc(Metric::CodecFailures);Error::Codec})?;
        // Single-message acceptance avoids partial receipts across a non-atomic codec batch.
        if messages.len()!=1 {return Err(Error::Codec);}
        let message=messages.into_iter().next().ok_or(Error::Codec)?;
        if message.device!=auth.device_key {return Err(Error::Forbidden);}
        if envelope.require_command_ack && !matches!(message.payload,DevicePayload::CommandAck(_)){return Err(Error::Invalid);}
        if matches!(message.payload,DevicePayload::CommandAck(_))&&!auth.permissions.commands{return Err(Error::Forbidden);}
        let canonical=canonical(&message)?;
        if canonical.len()>self.limits.max_http_body_size.max(self.limits.max_mqtt_packet_size).saturating_mul(2){return Err(Error::Codec);}
        let start=Instant::now();
        let receipt=deadline(self.limits.external_timeout_ms,self.store.accept(StoredIngress{message,canonical})).await?;
        self.metrics.add(Metric::DatabaseLatencyMs,start.elapsed().as_millis() as u64);
        self.sessions.touch(&auth.device_key,envelope.transport)?;
        self.metrics.inc(Metric::IngressAccepted);if receipt.duplicate{self.metrics.inc(Metric::DedupHits);}Ok(receipt)
    }
}
