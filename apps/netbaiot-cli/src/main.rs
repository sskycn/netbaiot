use futures_util::StreamExt;
use netbaiot_client::{ClientError, NetbaIoTClient};
use netbaiot_protocol::*;
use std::{env, net::SocketAddr, path::Path};
use tokio::io::{AsyncWriteExt, stdout};

const EXIT_USAGE: u8 = 2;
const EXIT_AUTH: u8 = 3;
const EXIT_FORBIDDEN: u8 = 4;
const EXIT_OFFLINE: u8 = 5;
const EXIT_UNAVAILABLE: u8 = 6;
const MAX_INPUT_FILE_BYTES: u64 = 1_048_576;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    Human,
    Json,
}

struct Global {
    endpoint: String,
    token: String,
    event_token: Option<String>,
    event_address: Option<SocketAddr>,
    output: Output,
}

#[tokio::main]
async fn main() {
    let code = match run(env::args().skip(1).collect()).await {
        Ok(()) => 0,
        Err(CliError::Usage(message)) => {
            eprintln!("{message}\n\n{}", usage());
            EXIT_USAGE
        }
        Err(CliError::Client(error)) => {
            eprintln!("{error}");
            match error {
                ClientError::Unauthenticated { .. } => EXIT_AUTH,
                ClientError::Forbidden { .. } => EXIT_FORBIDDEN,
                ClientError::DeviceOffline { .. } => EXIT_OFFLINE,
                _ => EXIT_UNAVAILABLE,
            }
        }
        Err(CliError::Io(message)) => {
            eprintln!("{message}");
            EXIT_UNAVAILABLE
        }
    };
    std::process::exit(i32::from(code));
}

#[derive(Debug)]
enum CliError {
    Usage(String),
    Client(ClientError),
    Io(String),
}

impl From<ClientError> for CliError {
    fn from(value: ClientError) -> Self {
        Self::Client(value)
    }
}

async fn run(mut arguments: Vec<String>) -> Result<(), CliError> {
    let global = parse_global(&mut arguments)?;
    let mut builder = NetbaIoTClient::builder()
        .endpoint(global.endpoint)
        .token(global.token);
    if let Some(token) = global.event_token {
        builder = builder.event_token(token);
    }
    if let Some(address) = global.event_address {
        builder = builder.event_address(address);
    }
    let client = builder.connect().await?;
    let Some(group) = arguments.first().map(String::as_str) else {
        return Err(CliError::Usage("a command is required".into()));
    };
    match group {
        "server" => server(&client, &arguments[1..], global.output).await,
        "device" => device(&client, &arguments[1..], global.output).await,
        "command" => command(&client, &arguments[1..], global.output).await,
        "auth" => auth(&client, &arguments[1..], global.output).await,
        "events" => events(&client, &arguments[1..], global.output).await,
        _ => Err(CliError::Usage(format!("unknown command group: {group}"))),
    }
}

fn parse_global(arguments: &mut Vec<String>) -> Result<Global, CliError> {
    let mut endpoint = env::var("NETBAIOT_ENDPOINT").ok();
    let mut token = env::var("NETBAIOT_TOKEN").ok();
    let mut event_address = env::var("NETBAIOT_EVENT_ADDRESS")
        .ok()
        .map(|value| value.parse())
        .transpose()
        .map_err(|_| CliError::Usage("NETBAIOT_EVENT_ADDRESS is invalid".into()))?;
    let mut event_token = env::var("NETBAIOT_EVENT_TOKEN").ok();
    let mut output = Output::Human;
    let mut remaining = Vec::new();
    let mut at = 0usize;
    while at < arguments.len() {
        match arguments[at].as_str() {
            "--endpoint" | "--token" | "--event-token" | "--event-address" | "--output" => {
                let flag = arguments[at].clone();
                let value = arguments
                    .get(at + 1)
                    .ok_or_else(|| CliError::Usage(format!("{flag} requires a value")))?
                    .clone();
                match flag.as_str() {
                    "--endpoint" => endpoint = Some(value),
                    "--token" => token = Some(value),
                    "--event-token" => event_token = Some(value),
                    "--event-address" => {
                        event_address = Some(value.parse().map_err(|_| {
                            CliError::Usage("--event-address must be HOST:PORT".into())
                        })?)
                    }
                    "--output" => {
                        output = match value.as_str() {
                            "human" => Output::Human,
                            "json" => Output::Json,
                            _ => {
                                return Err(CliError::Usage(
                                    "--output must be human or json".into(),
                                ));
                            }
                        }
                    }
                    _ => unreachable!(),
                }
                at += 2;
            }
            _ => {
                remaining.push(arguments[at].clone());
                at += 1;
            }
        }
    }
    *arguments = remaining;
    Ok(Global {
        endpoint: endpoint
            .ok_or_else(|| CliError::Usage("set --endpoint or NETBAIOT_ENDPOINT".into()))?,
        token: token.ok_or_else(|| CliError::Usage("set --token or NETBAIOT_TOKEN".into()))?,
        event_token,
        event_address,
        output,
    })
}

async fn server(
    client: &NetbaIoTClient,
    arguments: &[String],
    output: Output,
) -> Result<(), CliError> {
    match arguments.first().map(String::as_str) {
        Some("status") if arguments.len() == 1 => {
            let status = client.runtime().status().await?;
            print_value(&status, output, || {
                format!(
                    "lifecycle={:?} connections={} events={} pending_required={}",
                    status.lifecycle,
                    status.active_connections.total(),
                    status.event_count,
                    status.pending_required
                )
            })
            .await
        }
        Some("drain") if arguments.iter().any(|value| value == "--yes") => {
            client.runtime().drain().await?;
            print_json_line(
                &serde_json::json!({"draining": true}),
                output,
                "drain requested",
            )
            .await
        }
        Some("drain") => Err(CliError::Usage(
            "server drain is destructive and requires --yes".into(),
        )),
        _ => Err(CliError::Usage(
            "expected: server status | server drain --yes".into(),
        )),
    }
}

async fn device(
    client: &NetbaIoTClient,
    arguments: &[String],
    output: Output,
) -> Result<(), CliError> {
    if arguments.first().map(String::as_str) != Some("status") {
        return Err(CliError::Usage("expected: device status DEVICE".into()));
    }
    let key = device_key(arguments.get(1).map(String::as_str), arguments)?;
    let status = client.devices().connection(&key).await?;
    print_value(&status, output, || {
        format!(
            "connected={} transport={} last_seen={}",
            status.connected,
            status
                .transport
                .map_or_else(|| "-".into(), |value| format!("{value:?}")),
            status
                .last_seen
                .map_or_else(|| "-".into(), |value| value.to_string())
        )
    })
    .await
}

async fn command(
    client: &NetbaIoTClient,
    arguments: &[String],
    output: Output,
) -> Result<(), CliError> {
    if arguments.first().map(String::as_str) != Some("send") {
        return Err(CliError::Usage("expected: command send DEVICE".into()));
    }
    let key = device_key(arguments.get(1).map(String::as_str), arguments)?;
    let json = option(arguments, "--json")
        .map(str::to_owned)
        .or_else(|| option(arguments, "--payload-file").map(str::to_owned))
        .ok_or_else(|| CliError::Usage("use --json or --payload-file".into()))?;
    let bytes = if option(arguments, "--json").is_some() {
        json.into_bytes()
    } else {
        read_bounded(Path::new(&json)).await?
    };
    let payload: DeviceCommandPayload = serde_json::from_slice(&bytes)
        .map_err(|error| CliError::Usage(format!("invalid command payload: {error}")))?;
    let result = client
        .commands()
        .send(&DeviceCommand {
            command_id: CommandId::generate(),
            device: key,
            expires_at: None,
            payload,
        })
        .await?;
    print_value(&result, output, || {
        format!("command_id={} state={:?}", result.command_id, result.state)
    })
    .await
}

async fn auth(
    client: &NetbaIoTClient,
    arguments: &[String],
    output: Output,
) -> Result<(), CliError> {
    if arguments.first().map(String::as_str) != Some("invalidate") {
        return Err(CliError::Usage(
            "expected: auth invalidate --device DEVICE".into(),
        ));
    }
    let id = option(arguments, "--device");
    let key = device_key(id, arguments)?;
    let result = client.auth_cache().invalidate_device(key).await?;
    print_value(&result, output, || {
        format!(
            "invalidated_cache_entries={} disconnected_connections={} invalidated_mqtt_sessions={}",
            result.invalidated_cache_entries,
            result.disconnected_connections,
            result.invalidated_mqtt_sessions
        )
    })
    .await
}

async fn events(
    client: &NetbaIoTClient,
    arguments: &[String],
    output: Output,
) -> Result<(), CliError> {
    if arguments.first().map(String::as_str) != Some("subscribe") {
        return Err(CliError::Usage("expected: events subscribe".into()));
    }
    let event_types = options(arguments, "--type")
        .map(parse_event_type)
        .collect::<Result<Vec<_>, _>>()?;
    let filter = EventFilter {
        tenant: option(arguments, "--tenant")
            .map(TenantId::new)
            .transpose()
            .map_err(|_| CliError::Usage("invalid --tenant".into()))?,
        product: option(arguments, "--product")
            .map(ProductId::new)
            .transpose()
            .map_err(|_| CliError::Usage("invalid --product".into()))?,
        device: option(arguments, "--device")
            .map(DeviceId::new)
            .transpose()
            .map_err(|_| CliError::Usage("invalid --device".into()))?,
        event_types,
    };
    let mut stream = client.events().subscribe(filter).await?;
    let mut out = stdout();
    while let Some(delivery) = stream.next().await {
        let delivery = delivery?;
        let line = if output == Output::Json {
            serde_json::to_vec(delivery.event()).map_err(|error| CliError::Io(error.to_string()))?
        } else {
            format!(
                "event_id={} device={} type={:?}\n",
                delivery.event_id(),
                delivery.event().device.device_id,
                delivery.event().kind.event_type()
            )
            .into_bytes()
        };
        out.write_all(&line)
            .await
            .map_err(|error| CliError::Io(error.to_string()))?;
        if output == Output::Json {
            out.write_all(b"\n")
                .await
                .map_err(|error| CliError::Io(error.to_string()))?;
        }
        out.flush()
            .await
            .map_err(|error| CliError::Io(error.to_string()))?;
        delivery.ack().await?;
    }
    Ok(())
}

fn device_key(id: Option<&str>, arguments: &[String]) -> Result<DeviceKey, CliError> {
    let id = id.ok_or_else(|| CliError::Usage("device ID is required".into()))?;
    let tenant = option(arguments, "--tenant")
        .map(str::to_owned)
        .or_else(|| env::var("NETBAIOT_TENANT").ok())
        .ok_or_else(|| CliError::Usage("set --tenant or NETBAIOT_TENANT".into()))?;
    let product = option(arguments, "--product")
        .map(str::to_owned)
        .or_else(|| env::var("NETBAIOT_PRODUCT").ok())
        .ok_or_else(|| CliError::Usage("set --product or NETBAIOT_PRODUCT".into()))?;
    Ok(DeviceKey {
        tenant_id: TenantId::new(tenant)
            .map_err(|_| CliError::Usage("invalid tenant ID".into()))?,
        product_id: ProductId::new(product)
            .map_err(|_| CliError::Usage("invalid product ID".into()))?,
        device_id: DeviceId::new(id).map_err(|_| CliError::Usage("invalid device ID".into()))?,
    })
}

fn option<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn options<'a>(arguments: &'a [String], name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
    arguments
        .windows(2)
        .filter(move |pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn parse_event_type(value: &str) -> Result<EventType, CliError> {
    match value {
        "telemetry" => Ok(EventType::Telemetry),
        "device_event" => Ok(EventType::DeviceEvent),
        "heartbeat" => Ok(EventType::Heartbeat),
        "connected" => Ok(EventType::Connected),
        "disconnected" => Ok(EventType::Disconnected),
        "command_ack" => Ok(EventType::CommandAck),
        _ => Err(CliError::Usage(format!("unknown event type: {value}"))),
    }
}

async fn read_bounded(path: &Path) -> Result<Vec<u8>, CliError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| CliError::Io(error.to_string()))?;
    if metadata.len() > MAX_INPUT_FILE_BYTES {
        return Err(CliError::Usage("input file exceeds 1 MiB".into()));
    }
    tokio::fs::read(path)
        .await
        .map_err(|error| CliError::Io(error.to_string()))
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
        serde_json::to_vec(value).map_err(|error| CliError::Io(error.to_string()))?
    } else {
        human.as_bytes().to_vec()
    };
    bytes.push(b'\n');
    let mut out = stdout();
    out.write_all(&bytes)
        .await
        .map_err(|error| CliError::Io(error.to_string()))?;
    out.flush()
        .await
        .map_err(|error| CliError::Io(error.to_string()))
}

fn usage() -> &'static str {
    "netbaiot [--endpoint URL] [--token TOKEN] [--output human|json] COMMAND\n\
     commands: server status | server drain --yes | device status DEVICE |\n\
     command send DEVICE --json JSON | auth invalidate | events subscribe\n\
     device-scoped commands require --tenant and --product (or matching environment variables)"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_parser_removes_secrets_and_preserves_command() {
        let mut arguments = vec![
            "--endpoint".into(),
            "http://localhost:9001".into(),
            "--token".into(),
            "secret".into(),
            "server".into(),
            "status".into(),
        ];
        let global = parse_global(&mut arguments).unwrap();
        assert_eq!(global.endpoint, "http://localhost:9001");
        assert_eq!(arguments, ["server", "status"]);
    }

    #[test]
    fn parses_multiple_event_types() {
        let arguments = vec![
            "subscribe".into(),
            "--type".into(),
            "telemetry".into(),
            "--type".into(),
            "heartbeat".into(),
        ];
        let values = options(&arguments, "--type").collect::<Vec<_>>();
        assert_eq!(values, ["telemetry", "heartbeat"]);
    }

    #[test]
    fn command_payload_shape_is_public_protocol() {
        let payload: DeviceCommandPayload =
            serde_json::from_str(r#"{"name":"relay","arguments":{"enabled":true}}"#).unwrap();
        assert_eq!(
            payload.arguments,
            std::collections::BTreeMap::from([("enabled".into(), Scalar::Boolean(true))])
        );
    }
}
