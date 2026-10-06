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
    } else if let Err(error) = netbaiot_server::serve_path(std::path::Path::new(&path)).await {
        eprintln!("{error}");
        return std::process::ExitCode::FAILURE;
    }
    std::process::ExitCode::SUCCESS
}
