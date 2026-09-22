//! Bounded EventId generation/uniqueness probe. Release builds only for timing.
use netbaiot_protocol::EventId;
use serde_json::json;
use std::{
    hint::black_box,
    io::{self, Read, Write},
    sync::{Arc, Barrier},
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let mode = args
        .get(1)
        .ok_or("usage: eventid_probe bench|unique|emit|profile THREADS IDS_PER_THREAD")?;
    let threads: usize = args.get(2).ok_or("threads")?.parse()?;
    let count: usize = args.get(3).ok_or("count")?.parse()?;
    if !(1..=10).contains(&threads)
        || count == 0
        || count > 20_000_000
        || count.checked_mul(threads).is_none_or(|n| n > 100_000_000)
    {
        return Err("probe bounds exceeded".into());
    }
    let collect = mode == "unique" || mode == "emit";
    let profile = mode == "profile";
    if !collect && mode != "bench" && !profile {
        return Err("unknown probe mode".into());
    }
    // At most ten million stored 128-bit IDs (160 MB), not an unbounded hash set.
    if collect && count * threads > 10_000_000 {
        return Err("uniqueness memory bound exceeded".into());
    }
    if mode == "emit" {
        // Let the driver release two fully started processes together.
        eprintln!("ready");
        let mut release = [0; 3];
        io::stdin().read_exact(&mut release)?;
        if release != *b"go\n" {
            return Err("missing process overlap release".into());
        }
    }
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let cold = Instant::now();
            black_box(EventId::generate());
            let cold_ns = cold.elapsed().as_nanos();
            for _ in 0..10_000 {
                black_box(EventId::generate());
            }
            let mut ids = if collect {
                Vec::with_capacity(count)
            } else {
                Vec::new()
            };
            barrier.wait();
            let start = Instant::now();
            let mut generated = 0_u64;
            loop {
                for _ in 0..if profile { 10_000 } else { count } {
                    let id = black_box(EventId::generate());
                    if collect {
                        ids.push(id.0.as_u128());
                    }
                    generated += 1;
                }
                if !profile || start.elapsed().as_secs() >= 15 {
                    break;
                }
            }
            (start.elapsed().as_nanos(), cold_ns, ids, generated)
        }));
    }
    let mut elapsed = Vec::with_capacity(threads);
    let mut worker_ns_per_id = Vec::with_capacity(threads);
    let mut cold = Vec::with_capacity(threads);
    // Worker vectors are moved into one bounded vector; peak raw ID storage <=320 MB.
    let mut ids = if collect {
        Vec::with_capacity(count * threads)
    } else {
        Vec::new()
    };
    let mut total = 0_u64;
    for handle in handles {
        let (ns, first_ns, mut values, generated) =
            handle.join().map_err(|_| "generator thread panicked")?;
        elapsed.push(ns);
        worker_ns_per_id.push(ns as f64 / generated as f64);
        cold.push(first_ns);
        ids.append(&mut values);
        total += generated;
    }
    let ns = *elapsed.iter().max().ok_or("no workers")?;
    if mode == "emit" {
        let mut output = io::BufWriter::new(io::stdout().lock());
        for id in ids {
            output.write_all(&id.to_be_bytes())?;
        }
        output.flush()?;
    } else {
        let duplicates = if collect {
            ids.sort_unstable();
            ids.windows(2).filter(|pair| pair[0] == pair[1]).count()
        } else {
            0
        };
        println!(
            "{}",
            json!({"mode":mode,"threads":threads,"ids":total,"elapsed_ns":ns,
            "ids_per_second":total as f64*1e9/ns as f64,"wall_ns_per_id":ns as f64/total as f64,
            "worker_ns_per_id":worker_ns_per_id,
            "cold_first_id_ns":cold,"duplicates":if collect {Some(duplicates)} else {None}})
        );
        if duplicates != 0 {
            return Err("duplicate EventId".into());
        }
    }
    Ok(())
}
