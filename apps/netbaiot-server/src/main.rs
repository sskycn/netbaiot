// Explicit measurement build only: requested allocator bytes are distinct from
// RSS, allocator metadata/rounding, mapped address space, and kernel sockets.
#[cfg(all(feature = "allocation-profile", not(test)))]
#[global_allocator]
static ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> = &stats_alloc::INSTRUMENTED_SYSTEM;

async fn serve(path: &std::path::Path) -> Result<(), netbaiot_server::ServeError> {
    #[cfg(all(feature = "allocation-profile", not(test)))]
    {
        if std::env::var_os("NETBAIOT_ALLOCATION_PROFILE").as_deref()
            == Some(std::ffi::OsStr::new("1"))
        {
            use tokio::io::AsyncWriteExt;
            let server = netbaiot_server::serve_path(path);
            tokio::pin!(server);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    result = &mut server => return result,
                    _ = tick.tick() => {
                        let stats = ALLOCATOR.stats();
                        let record = serde_json::json!({"event":"allocation_profile",
                            "requested_live_bytes":stats.bytes_allocated.saturating_sub(stats.bytes_deallocated),
                            "allocations":stats.allocations,"deallocations":stats.deallocations,
                            "bytes_allocated":stats.bytes_allocated,"bytes_deallocated":stats.bytes_deallocated,
                            "reallocations":stats.reallocations});
                        let mut bytes = record.to_string().into_bytes();
                        bytes.push(b'\n');
                        let _ = tokio::time::timeout(std::time::Duration::from_millis(100),
                            tokio::io::stderr().write_all(&bytes)).await;
                    }
                }
            }
        }
    }
    netbaiot_server::serve_path(path).await
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    netbaiot_server::init_logging();
    let path = std::env::args_os()
        .nth(1)
        .unwrap_or_else(|| "configs/development.json".into());
    if path == "--print-default-limits" {
        match netbaiot_server::default_limits_json() {
            Ok(json) => {
                use std::io::Write;
                if writeln!(std::io::stdout().lock(), "{json}").is_err() {
                    return std::process::ExitCode::FAILURE;
                }
            }
            Err(error) => {
                eprintln!("{error}");
                return std::process::ExitCode::FAILURE;
            }
        }
    } else if let Err(error) = serve(std::path::Path::new(&path)).await {
        eprintln!("{error}");
        return std::process::ExitCode::FAILURE;
    }
    std::process::ExitCode::SUCCESS
}
