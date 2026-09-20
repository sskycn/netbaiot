//! Fixed-memory diagnostic microbenchmarks, never reported as network capacity.
use hmac::{Hmac, Mac};
use netbaiot_core::*;
use netbaiot_runtime::*;
use serde_json::json;
use sha2::Sha256;
use std::{hint::black_box, sync::Arc, time::Instant};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn measure(name: &str, operation: impl FnMut() -> BenchResult<()>) -> BenchResult<()> {
    measure_n(name, 1000, 10000, operation)
}
fn measure_n(
    name: &str,
    warmup: usize,
    iterations: usize,
    mut operation: impl FnMut() -> BenchResult<()>,
) -> BenchResult<()> {
    for _ in 0..warmup {
        operation()?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let total = Instant::now();
    for _ in 0..iterations {
        let started = Instant::now();
        operation()?;
        samples.push(started.elapsed().as_nanos());
    }
    let elapsed = total.elapsed();
    samples.sort_unstable();
    println!(
        "{}",
        json!({"name":name,"iterations":iterations,"ops_per_sec":iterations as f64/elapsed.as_secs_f64(),"p50_ns":samples[iterations/2],"p95_ns":samples[iterations*95/100],"p99_ns":samples[iterations*99/100]})
    );
    Ok(())
}

fn key(i: usize, tenants: usize) -> BenchResult<DeviceKey> {
    Ok(DeviceKey {
        tenant_id: TenantId::new(format!("t{}", i % tenants))?,
        product_id: ProductId::new("p")?,
        device_id: DeviceId::new(format!("d{i}"))?,
    })
}

fn main() -> BenchResult<()> {
    for n in [1, 16, 64, 256, 1024, 2000] {
        for tenants in if n == 1 { vec![1] } else { vec![1, n] } {
            let limits = Arc::new(Limits {
                max_ingress: n + 2,
                max_ingress_per_tenant: n + 1,
                requests_per_second: 1_000_000,
                messages_per_tenant_second: 1_000_000,
                messages_per_device_second: 1_000_000,
                max_devices: 4096,
                max_replay_entries: 4096,
                ..Limits::default()
            });
            limits.validate()?;
            let admission = Admission::new(limits);
            let population_started = Instant::now();
            let mut held = Vec::with_capacity(n);
            for i in 0..n {
                held.push(admission.acquire(&key(i, tenants)?, 0)?);
            }
            let probe = key(n + 1, tenants)?;
            measure(&format!("admission_devices_{n}_tenants_{tenants}"), || {
                drop(black_box(admission.acquire(&probe, 0)?));
                Ok(())
            })?;
            drop(held);
            measure_n(
                &format!("admission_recent_devices_{n}_tenants_{tenants}"),
                100,
                1000,
                || {
                    drop(black_box(admission.acquire(&probe, 0)?));
                    Ok(())
                },
            )?;
            if population_started.elapsed().as_millis() >= 900 {
                return Err("recent-entry benchmark exceeded its one-second population window; rerun without competing work".into());
            }
        }
    }
    let limits = Limits::default();
    let credential = Credential {
        credential_id: "a0".into(),
        secret_hex: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f".into(),
        identity: AuthenticatedDevice {
            device_key: key(0, 1)?,
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("netbaiot-json")?,
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        },
    };
    let auth = StaticAuthenticator::new(vec![credential.clone()], &limits)?;
    let rt = tokio::runtime::Builder::new_current_thread().build()?;
    for (label, id, secret) in [
        ("auth_valid", "a0", credential.secret_hex.as_bytes()),
        ("auth_missing", "missing", credential.secret_hex.as_bytes()),
        ("auth_bad_key", "a0", &[b'f'; 64][..]),
    ] {
        measure(label, || {
            let result = rt.block_on(auth.authenticate(AuthenticationRequest::Secret {
                credential_id: id,
                secret,
            }));
            if result.is_ok() != (label == "auth_valid") {
                return Err("authentication benchmark oracle".into());
            }
            black_box(result.ok());
            Ok(())
        })?;
    }
    let message = [0u8; 1024];
    let mut mac = Hmac::<Sha256>::new_from_slice(&decode_hex(&credential.secret_hex)?)?;
    mac.update(&message);
    let tag = mac.finalize().into_bytes();
    let verifier = rt.block_on(auth.resolve_verifier("a0"))?;
    measure("auth_hmac_1024", || {
        black_box(verifier.verify(&message, &tag)?);
        Ok(())
    })?;
    Ok(())
}
