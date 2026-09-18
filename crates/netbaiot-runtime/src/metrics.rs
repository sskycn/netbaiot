use std::sync::atomic::{AtomicU64, Ordering};
/// Closed label vocabulary. Never attach client or device identifiers.
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Metric { ConnectionsAccepted,ConnectionsRejected,MqttConnectSuccess,MqttConnectFailure,MqttPacketsReceived,MqttPacketsSent,MqttProtocolViolations,MqttPublishes,MqttSubscriptions,MqttPubacks,MqttKeepaliveDisconnects,HttpRequests,TcpFrames,UdpDatagrams,AuthFailures,CodecFailures,IngressAccepted,IngressRejected,DedupHits,QueueRejects,CommandQueued,CommandSent,CommandAcked,CommandFailed,DeliverySuccess,DeliveryFailed,DeliveryLatencyMs,DatabaseLatencyMs,Timeouts }
const NAMES:[&str;29]=["connections_accepted","connections_rejected","mqtt_connect_success","mqtt_connect_failure","mqtt_packets_received","mqtt_packets_sent","mqtt_protocol_violations","mqtt_publishes","mqtt_subscriptions","mqtt_pubacks","mqtt_keepalive_disconnects","http_requests","tcp_frames","udp_datagrams","auth_failures","codec_failures","ingress_accepted","ingress_rejected","dedup_hits","queue_rejects","command_queued","command_sent","command_acked","command_failed","delivery_success","delivery_failed","delivery_latency_ms","database_latency_ms","timeouts"];
pub struct Metrics { values:[AtomicU64;29] }
impl Default for Metrics { fn default()->Self { Self{values:std::array::from_fn(|_|AtomicU64::new(0))} } }
impl Metrics {
    pub fn inc(&self,m:Metric) { self.add(m,1); }
    pub fn add(&self,m:Metric,n:u64) { self.values[m as usize].fetch_add(n,Ordering::Relaxed); }
    pub fn get(&self,m:Metric)->u64 { self.values[m as usize].load(Ordering::Relaxed) }
    pub fn render(&self)->String { NAMES.iter().zip(&self.values).map(|(n,v)|format!("netbaiot_{n}_total {}\n",v.load(Ordering::Relaxed))).collect() }
}
