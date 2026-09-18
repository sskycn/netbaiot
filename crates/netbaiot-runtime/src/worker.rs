use crate::*;
use async_trait::async_trait;
use netbaiot_core::*;
use std::{collections::HashMap,sync::Arc,time::{Duration,Instant}};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
#[derive(Debug)]
pub enum DeliveryError { Retryable, Permanent }
#[async_trait]
pub trait DeliverySink:Send+Sync {async fn deliver(&self,message:&DeviceMessage)->std::result::Result<(),DeliveryError>;}
/// Full jitter is deterministic per message/attempt, reproducible in tests.
pub fn retry_delay(l:&Limits,id:MessageId,attempt:u32)->u64 {
    let cap=l.retry_base_ms.saturating_mul(1u64.checked_shl(attempt.min(30)).unwrap_or(u64::MAX)).min(l.retry_max_ms);
    let seed=(id.0.as_u128() as u64).wrapping_mul(6364136223846793005).wrapping_add(u64::from(attempt).wrapping_mul(1442695040888963407));
    1+seed%cap.max(1)
}
pub async fn delivery_worker(ingress:Arc<Ingress>,sink:Arc<dyn DeliverySink>,stop:CancellationToken)->Result<()> {
    let owner=Uuid::new_v4();let mut tick=tokio::time::interval(Duration::from_millis(200));tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select!{biased;_ = stop.cancelled()=>break,_=tick.tick()=>{}}
        let jobs=deadline(ingress.limits.external_timeout_ms,ingress.store.claim_jobs(owner,now_ms(),ingress.limits.delivery_batch)).await?;
        for job in jobs {
            if stop.is_cancelled(){break;}
            let start=Instant::now();let result=tokio::time::timeout(Duration::from_millis(ingress.limits.external_timeout_ms),sink.deliver(&job.message)).await;
            let (success,retryable)=match result {Ok(Ok(()))=>(true,false),Ok(Err(DeliveryError::Permanent))=>(false,false),_=>(false,true)};
            ingress.metrics.inc(if success{Metric::DeliverySuccess}else{Metric::DeliveryFailed});ingress.metrics.add(Metric::DeliveryLatencyMs,start.elapsed().as_millis() as u64);
            let now=now_ms();let next=now.saturating_add(retry_delay(&ingress.limits,job.message.message_id,job.attempts) as i64);
            deadline(ingress.limits.external_timeout_ms,ingress.store.finish_job(&job,success,retryable,now,next)).await?;
        }
        deadline(ingress.limits.external_timeout_ms,ingress.store.maintain(now_ms(),ingress.limits.delivery_batch)).await?;
        ingress.sessions.expire_presence(now_ms())?;
    }Ok(())
}
pub async fn command_worker(router:Arc<CommandRouter>,identities:HashMap<DeviceKey,AuthenticatedDevice>,stop:CancellationToken)->Result<()> {
    let ingress=&router.ingress;let mut tick=tokio::time::interval(Duration::from_millis(200));tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select!{biased;_=stop.cancelled()=>break,_=tick.tick()=>{}}
        // Only claim commands for active stream sessions; HTTP owns its pull leases.
        for (key,auth) in &identities {
            if stop.is_cancelled(){break;}
            if ingress.sessions.lookup(key)?.is_none(){continue;}
            let commands=deadline(ingress.limits.external_timeout_ms,ingress.store.claim_commands(Some(key),now_ms(),1)).await?;
            for record in commands {if let Err(e)=router.dispatch(record.command,auth).await {tracing::debug!(error=%e,"command remains leased for bounded retry");}}
        }
    }Ok(())
}
