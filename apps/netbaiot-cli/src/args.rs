use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser)]
#[command(
    name = "netbaiot",
    version,
    about = "NetbaIoT IoT gateway and operator CLI",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[arg(long, global = true)]
    pub endpoint: Option<String>,
    #[arg(long, global = true, conflicts_with = "api_key")]
    pub token: Option<String>,
    #[arg(long, global = true, conflicts_with = "token")]
    pub api_key: Option<String>,
    #[arg(long, global = true)]
    pub event_token: Option<String>,
    #[arg(long, global = true)]
    pub event_address: Option<SocketAddr>,
    #[arg(long, global = true, value_enum, default_value = "human")]
    pub output: Output,
    #[command(subcommand)]
    pub command: RootCommand,
}
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Output {
    Human,
    Json,
}
#[derive(Subcommand)]
pub enum RootCommand {
    /// Run the gateway
    Serve(ConfigPath),
    /// Run a local end-to-end demo
    Demo {
        #[arg(long)]
        once: bool,
    },
    /// Validate or inspect configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Create a local development project or production skeleton
    Init {
        #[arg(default_value = ".")]
        directory: PathBuf,
        #[arg(long)]
        production: bool,
        #[arg(long)]
        force: bool,
    },
    /// Check local configuration and environment
    Doctor {
        #[arg(long, default_value = "netbaiot.json")]
        config: PathBuf,
        #[arg(long)]
        network: bool,
    },
    /// Print version information
    Version,
    /// Inspect or drain a running gateway
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
    /// Inspect device state
    Device {
        #[command(subcommand)]
        command: DeviceCommand,
    },
    /// Send a command to a live device
    Command {
        #[command(subcommand)]
        command: CommandCommand,
    },
    /// Manage authentication state
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Subscribe to business events
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
}
#[derive(Args)]
pub struct ConfigPath {
    #[arg(long, default_value = "configs/development.json")]
    pub config: PathBuf,
}
#[derive(Subcommand)]
pub enum ConfigCommand {
    Check(ConfigPath),
    Limits,
    Schema,
}
#[derive(Subcommand)]
pub enum ServerCommand {
    Status,
    Drain {
        #[arg(long, required = true)]
        yes: bool,
    },
}
#[derive(Args)]
pub struct Scope {
    #[arg(long, value_parser = identifier)]
    pub tenant: Option<String>,
    #[arg(long, value_parser = identifier)]
    pub product: Option<String>,
}
#[derive(Args)]
pub struct DeviceTarget {
    #[arg(value_parser = identifier)]
    pub device: String,
    #[command(flatten)]
    pub scope: Scope,
}
#[derive(Subcommand)]
pub enum DeviceCommand {
    Status(DeviceTarget),
}
#[derive(Subcommand)]
pub enum CommandCommand {
    Send(SendCommand),
}
#[derive(Args)]
#[group(id = "payload", required = true, multiple = false, args = ["json", "payload_file"])]
pub struct SendCommand {
    #[command(flatten)]
    pub target: DeviceTarget,
    #[arg(long)]
    pub json: Option<String>,
    #[arg(long)]
    pub payload_file: Option<PathBuf>,
}
#[derive(Subcommand)]
pub enum AuthCommand {
    Invalidate {
        #[arg(long, value_parser = identifier)]
        device: String,
        #[command(flatten)]
        scope: Scope,
    },
}
#[derive(Subcommand)]
pub enum EventsCommand {
    Subscribe(EventArgs),
}
#[derive(Args)]
pub struct EventArgs {
    #[arg(long, value_parser = identifier)]
    pub tenant: Option<String>,
    #[arg(long, value_parser = identifier)]
    pub product: Option<String>,
    #[arg(long, value_parser = identifier)]
    pub device: Option<String>,
    #[arg(long = "type", value_enum)]
    pub types: Vec<EventKind>,
}
#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum EventKind {
    Telemetry,
    DeviceEvent,
    Heartbeat,
    CommandAck,
}
fn identifier(value: &str) -> Result<String, &'static str> {
    netbaiot_protocol::DeviceId::new(value)
        .map(|_| value.to_owned())
        .map_err(|_| "invalid identifier")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn command_tree_preserves_existing_and_adds_local_commands() {
        let cases = [
            vec!["serve"],
            vec!["serve", "--config", "local.json"],
            vec!["demo", "--once"],
            vec!["config", "check"],
            vec!["config", "limits"],
            vec!["version"],
            vec!["server", "status"],
            vec!["server", "drain", "--yes"],
            vec![
                "device",
                "status",
                "device-1",
                "--tenant",
                "demo",
                "--product",
                "sensor",
            ],
            vec!["command", "send", "device-1", "--json", "{}"],
            vec!["auth", "invalidate", "--device", "device-1"],
            vec![
                "events",
                "subscribe",
                "--type",
                "heartbeat",
                "--type",
                "telemetry",
            ],
        ];
        for args in cases {
            assert!(Cli::try_parse_from(std::iter::once("netbaiot").chain(args)).is_ok());
        }
        let cli = Cli::try_parse_from(["netbaiot", "serve"]).unwrap();
        assert!(
            matches!(cli.command, RootCommand::Serve(ConfigPath { config }) if config == std::path::Path::new("configs/development.json"))
        );
    }
    #[test]
    fn rejects_ambiguous_and_invalid_operator_requests_before_network() {
        for args in [
            vec!["server", "drain"],
            vec!["server", "drain", "--yes", "--unknown"],
            vec!["version", "--token", "a", "--api-key", "b"],
            vec!["version", "--output", "xml"],
            vec!["serve", "--config"],
            vec!["serve", "--config", "a", "--config", "b"],
            vec!["version", "--event-address", "bad"],
            vec!["device", "status", "bad/id"],
            vec![
                "command",
                "send",
                "d",
                "--json",
                "{}",
                "--payload-file",
                "p",
            ],
            vec!["events", "subscribe", "--type", "connected"],
            vec!["unknown"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("netbaiot").chain(args)).is_err());
        }
    }
}
