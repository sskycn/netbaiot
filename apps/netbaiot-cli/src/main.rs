mod args;
mod demo;
mod doctor;
mod init;
use args::*;
use clap::Parser;
use futures_util::StreamExt;
use netbaiot_client::{ClientError, NetbaIoTClient};
use netbaiot_protocol::*;
use std::{env, path::Path};
use tokio::io::{AsyncReadExt, AsyncWriteExt, stdout};

const MAX_INPUT_FILE_BYTES: u64 = 1_048_576;
#[derive(Debug)]
enum CliError {
    Usage(&'static str),
    Client(ClientError),
    Io(&'static str),
    Serve(netbaiot_server::ServeError),
    Demo(demo::DemoError),
    Configuration,
    Boundary(String),
}
impl From<ClientError> for CliError {
    fn from(e: ClientError) -> Self {
        Self::Client(e)
    }
}
#[tokio::main]
async fn main() -> std::process::ExitCode {
    // clap owns help/version and syntax errors; never connect before parsing.
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let code = match &error {
                CliError::Usage(m) => {
                    eprintln!("{m}");
                    2
                }
                CliError::Configuration => 2,
                CliError::Boundary(message) => {
                    eprintln!("{message}");
                    6
                }
                CliError::Serve(netbaiot_server::ServeError::Configuration(r)) => {
                    eprint!("{}", r.human());
                    2
                }
                CliError::Serve(e) => {
                    eprintln!("{e}");
                    6
                }
                CliError::Demo(e) => {
                    eprintln!("{e}");
                    6
                }
                CliError::Io(m) => {
                    eprintln!("{m}");
                    6
                }
                CliError::Client(e) => {
                    eprintln!("{e}");
                    match e {
                        ClientError::Unauthenticated { .. } => 3,
                        ClientError::Forbidden { .. } => 4,
                        ClientError::DeviceOffline { .. } => 5,
                        _ => 6,
                    }
                }
            };
            std::process::ExitCode::from(code)
        }
    }
}
async fn run(cli: Cli) -> Result<(), CliError> {
    let output = cli.output;
    match cli.command {
        RootCommand::Serve(path) => {
            netbaiot_server::init_logging();
            netbaiot_server::serve_path(&path.config)
                .await
                .map_err(CliError::Serve)
        }
        RootCommand::Demo { once } => {
            netbaiot_server::init_logging();
            demo::run(once).await.map_err(CliError::Demo)
        }
        RootCommand::Init {
            directory,
            production,
            force,
        } => {
            let result = init::create(directory, production, force)
                .await
                .map_err(CliError::Boundary)?;
            let message = format!(
                "Created {}: netbaiot.json, .env.example, README.md\n{}\nNext: netbaiot config check --config netbaiot.json",
                result.directory.display(),
                if production {
                    "Production skeleton is NOT ready to run until TLS/auth/sink values are supplied."
                } else {
                    "Development credentials only; do not reuse in production. Configure your example HTTP sink before serve."
                }
            );
            print_json_line(&result, output, &message).await
        }
        RootCommand::Doctor { config, network } => {
            let report = doctor::inspect(&config, network).await;
            print_json_line(&report, output, report.human().trim_end()).await?;
            if report.ok {
                Ok(())
            } else if !report.configuration.valid {
                Err(CliError::Configuration)
            } else {
                Err(CliError::Boundary(
                    "Doctor found blocking environment problems".into(),
                ))
            }
        }
        RootCommand::Config {
            command: ConfigCommand::Schema,
        } => {
            let mut out = stdout();
            out.write_all(include_bytes!(
                "../../../docs/schema/netbaiot-config.schema.json"
            ))
            .await
            .map_err(|_| CliError::Io("Cannot write schema"))?;
            out.flush()
                .await
                .map_err(|_| CliError::Io("Cannot flush schema"))
        }
        RootCommand::Version => {
            return print_json_line(
                &serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"rust_msrv":env!("CARGO_PKG_RUST_VERSION")}),
                output,
                &format!("NetbaIoT {}\nrust-msrv {}", env!("CARGO_PKG_VERSION"), env!("CARGO_PKG_RUST_VERSION")),
            )
            .await;
        }
        RootCommand::Config {
            command: ConfigCommand::Limits,
        } => {
            let json = netbaiot_server::default_limits_json()
                .map_err(|_| CliError::Io("Could not serialize default limits"))?;
            let mut out = stdout();
            out.write_all(format!("{json}\n").as_bytes())
                .await
                .map_err(|_| CliError::Io("Cannot write limits output"))?;
            out.flush()
                .await
                .map_err(|_| CliError::Io("Cannot flush limits output"))
        }
        RootCommand::Config {
            command: ConfigCommand::Check(path),
        } => {
            let report = match netbaiot_server::load_config_diagnostic(&path.config).await {
                Ok(config) => {
                    netbaiot_server::check_config(
                        &config,
                        env::var("NETBAIOT_ADMIN_SECRET").ok().as_deref(),
                        None,
                    )
                    .await
                }
                Err(report) => report,
            };
            print_json_line(&report, output, report.human().trim_end()).await?;
            if report.valid {
                Ok(())
            } else {
                Err(CliError::Configuration)
            }
        }
        command => {
            let endpoint = cli
                .endpoint
                .or_else(|| env::var("NETBAIOT_ENDPOINT").ok())
                .ok_or(CliError::Usage("Set --endpoint or NETBAIOT_ENDPOINT"))?;
            let (token, api_key) = if let Some(t) = cli.token {
                (t, false)
            } else if let Some(t) = cli.api_key {
                (t, true)
            } else if let Ok(t) = env::var("NETBAIOT_TOKEN") {
                (t, false)
            } else {
                (
                    env::var("NETBAIOT_API_KEY")
                        .map_err(|_| CliError::Usage("Set --token, --api-key or NETBAIOT_TOKEN"))?,
                    true,
                )
            };
            let mut builder = NetbaIoTClient::builder().endpoint(endpoint);
            builder = if api_key {
                builder.api_key(token)
            } else {
                builder.token(token)
            };
            if let Some(t) = cli
                .event_token
                .or_else(|| env::var("NETBAIOT_EVENT_TOKEN").ok())
            {
                builder = builder.event_token(t);
            }
            let address = cli.event_address.or(env::var("NETBAIOT_EVENT_ADDRESS")
                .ok()
                .map(|s| {
                    s.parse()
                        .map_err(|_| CliError::Usage("NETBAIOT_EVENT_ADDRESS must be IP:PORT"))
                })
                .transpose()?);
            if let Some(a) = address {
                builder = builder.event_address(a);
            }
            // Resolve local payload/identity inputs before management side effects.
            operate(command, builder.connect().await?, output).await
        }
    }
}
fn device_key(device: String, scope: Scope) -> Result<DeviceKey, CliError> {
    Ok(DeviceKey {
        tenant_id: TenantId::new(
            scope
                .tenant
                .or_else(|| env::var("NETBAIOT_TENANT").ok())
                .ok_or(CliError::Usage("Set --tenant or NETBAIOT_TENANT"))?,
        )
        .map_err(|_| CliError::Usage("Invalid tenant ID"))?,
        product_id: ProductId::new(
            scope
                .product
                .or_else(|| env::var("NETBAIOT_PRODUCT").ok())
                .ok_or(CliError::Usage("Set --product or NETBAIOT_PRODUCT"))?,
        )
        .map_err(|_| CliError::Usage("Invalid product ID"))?,
        device_id: DeviceId::new(device).map_err(|_| CliError::Usage("Invalid device ID"))?,
    })
}
async fn operate(
    command: RootCommand,
    client: NetbaIoTClient,
    output: Output,
) -> Result<(), CliError> {
    match command {
        RootCommand::Server {
            command: ServerCommand::Status,
        } => {
            let s = client.runtime().status().await?;
            print_value(&s, output, || {
                format!(
                    "lifecycle={:?} connections={} events={} pending_required={}",
                    s.lifecycle,
                    s.active_connections.total(),
                    s.event_count,
                    s.pending_required
                )
            })
            .await
        }
        RootCommand::Server {
            command: ServerCommand::Drain { .. },
        } => {
            client.runtime().drain().await?;
            print_json_line(
                &serde_json::json!({"draining":true}),
                output,
                "drain requested",
            )
            .await
        }
        RootCommand::Device {
            command: args::DeviceCommand::Status(t),
        } => {
            let s = client
                .devices()
                .connection(&device_key(t.device, t.scope)?)
                .await?;
            print_value(&s, output, || {
                format!(
                    "connected={} transport={} last_seen={}",
                    s.connected,
                    s.transport.map_or_else(|| "-".into(), |v| format!("{v:?}")),
                    s.last_seen.map_or_else(|| "-".into(), |v| v.to_string())
                )
            })
            .await
        }
        RootCommand::Command {
            command: CommandCommand::Send(a),
        } => {
            let device = device_key(a.target.device, a.target.scope)?;
            let bytes = match (a.json, a.payload_file) {
                (Some(json), None) => json.into_bytes(),
                (None, Some(path)) => read_bounded(&path).await?,
                _ => return Err(CliError::Usage("Select one command payload source")),
            };
            if bytes.len() as u64 > MAX_INPUT_FILE_BYTES {
                return Err(CliError::Usage("Command payload exceeds 1 MiB"));
            }
            let payload = serde_json::from_slice(&bytes).map_err(|_| {
                CliError::Usage(
                    "Invalid command payload; use the public DeviceCommandPayload JSON shape",
                )
            })?;
            let r = client
                .commands()
                .send(&netbaiot_protocol::DeviceCommand {
                    command_id: CommandId::generate(),
                    device,
                    expires_at: None,
                    payload,
                })
                .await?;
            print_value(&r, output, || {
                format!("command_id={} state={:?}", r.command_id, r.state)
            })
            .await
        }
        RootCommand::Auth {
            command: AuthCommand::Invalidate { device, scope },
        } => {
            let r = client
                .auth_cache()
                .invalidate_device(device_key(device, scope)?)
                .await?;
            print_value(&r, output, || format!("invalidated_cache_entries={} disconnected_connections={} invalidated_mqtt_sessions={}", r.invalidated_cache_entries, r.disconnected_connections, r.invalidated_mqtt_sessions)).await
        }
        RootCommand::Events {
            command: EventsCommand::Subscribe(a),
        } => {
            let filter = EventFilter {
                tenant: a
                    .tenant
                    .map(TenantId::new)
                    .transpose()
                    .map_err(|_| CliError::Usage("Invalid tenant"))?,
                product: a
                    .product
                    .map(ProductId::new)
                    .transpose()
                    .map_err(|_| CliError::Usage("Invalid product"))?,
                device: a
                    .device
                    .map(DeviceId::new)
                    .transpose()
                    .map_err(|_| CliError::Usage("Invalid device"))?,
                event_types: a
                    .types
                    .into_iter()
                    .map(|t| match t {
                        EventKind::Telemetry => EventType::Telemetry,
                        EventKind::DeviceEvent => EventType::DeviceEvent,
                        EventKind::Heartbeat => EventType::Heartbeat,
                        EventKind::CommandAck => EventType::CommandAck,
                    })
                    .collect(),
            };
            let mut stream = client.events().subscribe(filter).await?;
            let mut out = stdout();
            loop {
                let delivery = tokio::select! { d = stream.next() => match d { Some(d) => d?, None => break }, r = netbaiot_server::shutdown_signal() => { r.map_err(|_| CliError::Io("Cannot receive shutdown signal"))?; break; } };
                let mut line = if output == Output::Json {
                    serde_json::to_vec(delivery.event())
                        .map_err(|_| CliError::Io("Cannot serialize event"))?
                } else {
                    format!(
                        "event_id={} device={} type={:?}",
                        delivery.event_id(),
                        delivery.event().device.device_id,
                        delivery.event().kind.event_type()
                    )
                    .into_bytes()
                };
                line.push(b'\n');
                out.write_all(&line)
                    .await
                    .map_err(|_| CliError::Io("Cannot write event output"))?;
                out.flush()
                    .await
                    .map_err(|_| CliError::Io("Cannot flush event output"))?;
                delivery.ack().await?;
            }
            stream.close();
            Ok(())
        }
        _ => Err(CliError::Usage("Expected an operator command")),
    }
}
async fn read_bounded(path: &Path) -> Result<Vec<u8>, CliError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| CliError::Io("Cannot read input file"))?;
    if !metadata.is_file() {
        return Err(CliError::Usage("Input must be a regular file"));
    }
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await
        .map_err(|_| CliError::Io("Cannot open input file"))?
        .take(MAX_INPUT_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| CliError::Io("Cannot read input file"))?;
    if bytes.len() as u64 > MAX_INPUT_FILE_BYTES {
        return Err(CliError::Usage("Input file exceeds 1 MiB"));
    }
    Ok(bytes)
}
async fn print_value<T: serde::Serialize>(
    value: &T,
    output: Output,
    human: impl FnOnce() -> String,
) -> Result<(), CliError> {
    if output == Output::Json {
        print_json_line(value, output, "").await
    } else {
        print_json_line(&serde_json::Value::Null, output, &human()).await
    }
}

async fn print_json_line<T: serde::Serialize>(
    value: &T,
    output: Output,
    human: &str,
) -> Result<(), CliError> {
    let mut bytes = if output == Output::Json {
        serde_json::to_vec(value).map_err(|_| CliError::Io("Cannot write CLI output"))?
    } else {
        human.as_bytes().to_vec()
    };
    bytes.push(b'\n');
    let mut out = stdout();
    out.write_all(&bytes)
        .await
        .map_err(|_| CliError::Io("Cannot write CLI output"))?;
    out.flush()
        .await
        .map_err(|_| CliError::Io("Cannot write CLI output"))
}
