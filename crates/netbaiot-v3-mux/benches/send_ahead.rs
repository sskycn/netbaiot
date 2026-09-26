//! Scheduler-only microbenchmark. Peer credit is synthetic; results are not network throughput.
use bytes::Bytes;
use netbaiot_protocol::business_rpc_v3::V3Limits;
use netbaiot_v3_mux::{DataClass, MuxScheduler, SendAheadLimits};
use std::{hint::black_box, time::Instant};

fn main() {
    let cases = [
        (4096, 8192),
        (4096, 16384),
        (8192, 8192),
        (8192, 16384),
        (8192, 32768),
        (16384, 16384),
        (16384, 32768),
        (16384, 65536),
    ];
    println!(
        "frame_bytes,stream_send_ahead_bytes,runs,late_rpc_event_bytes_before_rpc,frames_per_second"
    );
    for (frame_bytes, stream_bytes) in cases {
        let limits = V3Limits {
            max_frame_payload_bytes: frame_bytes,
            initial_stream_window_bytes: 1024 * 1024,
            ..V3Limits::default()
        };
        let policy = Some(SendAheadLimits {
            stream_bytes,
            connection_bytes: 128 * 1024,
        });
        let event = Bytes::from(vec![0; 1024 * 1024]);
        let mut frames = 0u64;
        let mut before_rpc = 0;
        let started = Instant::now();
        for _ in 0..1000 {
            let mut scheduler = MuxScheduler::with_send_ahead(&limits, 2 * 1024 * 1024, policy)
                .expect("valid benchmark configuration");
            scheduler
                .queue_body(2, DataClass::Event, event.clone())
                .expect("bounded event");
            let mut submitted = 0;
            for _ in 0..4 {
                let Some(frame) = scheduler.next_frame() else {
                    break;
                };
                submitted += frame.payload.len();
                frames += 1;
            }
            scheduler
                .queue_body(1, DataClass::Rpc, Bytes::from_static(&[1; 100]))
                .expect("bounded RPC");
            let rpc = scheduler.next_frame().expect("RPC has connection credit");
            assert_eq!(rpc.header.stream_id, 1);
            frames += 1;
            before_rpc = submitted;
            black_box(rpc);
        }
        let elapsed = started.elapsed().as_secs_f64();
        println!(
            "{frame_bytes},{stream_bytes},1000,{before_rpc},{:.0}",
            frames as f64 / elapsed
        );
    }
}
