use clap::{Parser, Subcommand, ValueEnum};
use std::{
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

#[derive(Parser)]
#[command(about = "NetbaIoT maintenance tasks", version)]
struct Cli {
    #[command(subcommand)]
    task: Task,
}
#[derive(Subcommand)]
enum Task {
    Check {
        #[arg(value_enum)]
        suite: Option<Suite>,
        #[arg(long, conflicts_with = "suite")]
        audit: bool,
        #[arg(long, value_enum, requires = "suite")]
        part: Option<Part>,
        #[arg(long, requires = "part")]
        archive: Option<PathBuf>,
    },
    Schema {
        #[arg(long)]
        check: bool,
    },
    ConfigReference {
        #[arg(long)]
        check: bool,
    },
    Package {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, requires = "target")]
        no_build: bool,
    },
}
#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Suite {
    Mqtt,
    Release,
}
#[derive(Clone, Copy, ValueEnum)]
enum Part {
    Rust,
    Audit,
    Mqtt,
    Preflight,
    Archive,
}
fn main() -> ExitCode {
    let cli = Cli::parse();
    match execute(cli.task) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("FAIL: {error}");
            ExitCode::FAILURE
        }
    }
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."))
}
fn run(program: &str, args: &[&str]) -> Result<(), String> {
    println!("RUN {program} {}", args.join(" "));
    let status = Command::new(program)
        .args(args)
        .current_dir(root())
        .status()
        .map_err(|_| {
            format!("BLOCKED: {program} could not be started; install the required tool")
        })?;
    if !status.success() {
        return Err(format!("{program} {} exited with {status}", args.join(" ")));
    }
    println!("PASS");
    Ok(())
}
fn rust_checks(toolchain: Option<&str>) -> Result<(), String> {
    let prefix = toolchain.map(|v| format!("+{v}"));
    for args in [
        vec!["fmt", "--all", "--", "--check"],
        vec![
            "clippy",
            "--locked",
            "--workspace",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ],
        vec!["test", "--locked", "--workspace", "--all-features"],
    ] {
        let mut command = Vec::new();
        if let Some(p) = &prefix {
            command.push(p.as_str());
        }
        command.extend(args);
        run("cargo", &command)?;
    }
    Ok(())
}
fn python(args: &[&str]) -> Result<(), String> {
    run(if cfg!(windows) { "python" } else { "python3" }, args)
}
fn mqtt() -> Result<(), String> {
    // Required dependencies fail explicitly. Do not silently skip interop.
    for tool in ["mosquitto", "mosquitto_pub", "mosquitto_sub", "openssl"] {
        Command::new(tool)
            .arg(if tool == "openssl" {
                "version"
            } else {
                "--help"
            })
            .output()
            .map_err(|_| format!("BLOCKED: {tool} not found"))?;
    }
    run("cargo", &["build", "--locked", "-p", "netbaiot-server"])?;
    python(&[
        "tests/mqtt_protocol_regressions.py",
        "--repo",
        ".",
        "--output",
        "target/mqtt-audit/ci.json",
    ])?;
    python(&[
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests/mqtt_conformance",
        "-p",
        "test_*.py",
        "-v",
    ])?;
    python(&["tests/mqtt_conformance/run.py", "--release-gate"])?;
    python(&["tests/mqtt_conformance/v5_smoke.py"])?;
    python(&["tests/mqtt_conformance/v5_mosquitto.py"])?;
    run(
        "cargo",
        &[
            "build",
            "--locked",
            "-p",
            "netbaiot-device-sdk",
            "--example",
            "device_mqtt",
        ],
    )?;
    python(&["tests/run_device_profile_mosquitto.py"])?;
    python(&["tests/measure_device_profile.py"])
}
fn schema_bytes() -> Result<Vec<u8>, String> {
    let output = Command::new("cargo")
        .args([
            "run",
            "--locked",
            "--quiet",
            "-p",
            "netbaiot-server",
            "--features",
            "schema",
            "--bin",
            "netbaiot-schema",
        ])
        .current_dir(root())
        .output()
        .map_err(|_| "BLOCKED: cargo could not start".to_owned())?;
    if !output.status.success() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        return Err("configuration schema generator failed".into());
    }
    Ok(output.stdout)
}
fn write_generated(path: &str, bytes: &[u8], check: bool) -> Result<(), String> {
    let path = root().join(path);
    if check {
        let old = std::fs::read(&path)
            .map_err(|_| format!("generated file missing: {}", path.display()))?;
        if old != bytes {
            return Err(format!("generated file drift: {}", path.display()));
        }
    } else {
        std::fs::create_dir_all(path.parent().ok_or("invalid generated path")?)
            .map_err(|_| "cannot create generated directory")?;
        std::fs::write(&path, bytes).map_err(|_| "cannot write generated file")?;
    }
    println!("PASS {}", path.display());
    Ok(())
}
fn reference(schema: &[u8]) -> Result<Vec<u8>, String> {
    let schema: serde_json::Value =
        serde_json::from_slice(schema).map_err(|_| "invalid generated schema")?;
    let mut text = String::from(
        "# Configuration fields (generated)\n\nGenerated from Rust Config/serde types. Run `cargo xtask config-reference`.\nSchema does not replace `netbaiot config check`; cross-field, TLS, secret-source and runtime checks remain authoritative.\n\n| Field | Schema type | Default | Description |\n| --- | --- | --- | --- |\n",
    );
    let mut groups = vec![("Config", &schema)];
    if let Some(definitions) = schema.get("$defs").and_then(serde_json::Value::as_object) {
        for (name, definition) in definitions {
            if definition.get("properties").is_some() {
                groups.push((name.as_str(), definition));
            }
        }
    }
    for (group, definition) in groups {
        text.push_str(&format!("\n## {group}\n\n| Field | Schema type | Default | Description |\n| --- | --- | --- | --- |\n"));
        for (name, value) in definition["properties"]
            .as_object()
            .ok_or("schema properties missing")?
        {
            let kind = value
                .get("type")
                .map(|v| v.to_string())
                .or_else(|| value.get("$ref").map(|v| v.to_string()))
                .unwrap_or_else(|| "union/object".into());
            let default = value
                .get("default")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "required/no default".into());
            let description = value
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .replace('\n', " ")
                .replace('|', "\\|");
            text.push_str(&format!(
                "| `{name}` | {kind} | `{default}` | {description} |\n"
            ));
        }
    }
    Ok(text.into_bytes())
}
fn package(target: Option<String>, no_build: bool) -> Result<(), String> {
    if let Ok(tag) = std::env::var("RELEASE_TAG")
        && !tag.is_empty()
        && tag != format!("v{}", env!("CARGO_PKG_VERSION"))
    {
        return Err("RELEASE_TAG must match the workspace package version".into());
    }
    let target = match target {
        Some(v) => v,
        None => {
            let out = Command::new("rustc")
                .arg("-vV")
                .output()
                .map_err(|_| "BLOCKED: rustc not found")?;
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .find_map(|l| l.strip_prefix("host: "))
                .ok_or("cannot determine host target")?
                .to_owned()
        }
    };
    if target.len() > 128
        || !target
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err("target must be a Rust target triple".into());
    }
    if !no_build {
        run(
            "cargo",
            &[
                "build",
                "--locked",
                "--release",
                "--target",
                &target,
                "-p",
                "netbaiot-server",
                "-p",
                "netbaiot-cli",
            ],
        )?;
    }
    let tag = format!("v{}", env!("CARGO_PKG_VERSION"));
    let dir = format!("target/{target}/release");
    python(&[
        "scripts/release_package.py",
        "package",
        "--tag",
        &tag,
        "--target",
        &target,
        "--build-dir",
        &dir,
        "--dist",
        "dist",
    ])
}
fn preflight() -> Result<(), String> {
    execute(Task::Schema { check: true })?;
    execute(Task::ConfigReference { check: true })?;
    let tag = std::env::var("RELEASE_TAG").unwrap_or_default();
    if tag.is_empty() {
        python(&["scripts/release_preflight.py"])?;
    } else {
        python(&["scripts/release_preflight.py", "--tag", &tag])?;
    }
    python(&[
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        "test_release_*.py",
        "-v",
    ])
}
fn archive(existing: Option<PathBuf>) -> Result<(), String> {
    if let Some(path) = existing {
        return python(&[
            "tests/release_archive_smoke.py",
            path.to_str().ok_or("archive path must be UTF-8")?,
        ]);
    }
    package(None, false)?;
    let out = Command::new("rustc")
        .arg("-vV")
        .output()
        .map_err(|_| "rustc unavailable")?;
    let text = String::from_utf8_lossy(&out.stdout);
    let host = text
        .lines()
        .find_map(|l| l.strip_prefix("host: "))
        .ok_or("host unavailable")?;
    if cfg!(windows) {
        return Err("BLOCKED: preserved Bash archive smoke requires Linux/macOS; native DX tests cover Windows".into());
    }
    python(&[
        "tests/release_archive_smoke.py",
        &format!("dist/netbaiot-v{}-{host}.tar.gz", env!("CARGO_PKG_VERSION")),
    ])
}
fn execute(task: Task) -> Result<(), String> {
    match task {
        Task::Schema { check } => write_generated(
            "docs/schema/netbaiot-config.schema.json",
            &schema_bytes()?,
            check,
        ),
        Task::ConfigReference { check } => write_generated(
            "docs/configuration-fields.md",
            &reference(&schema_bytes()?)?,
            check,
        ),
        Task::Package { target, no_build } => package(target, no_build),
        Task::Check {
            suite: None,
            audit,
            part: None,
            archive: None,
        } => {
            rust_checks(None)?;
            if audit {
                run("cargo", &["audit"])?;
            }
            Ok(())
        }
        Task::Check {
            suite: Some(Suite::Mqtt),
            part: None,
            archive: None,
            ..
        } => mqtt(),
        Task::Check {
            suite: Some(Suite::Release),
            part,
            archive,
            ..
        } => {
            if archive.is_some() && !matches!(part, Some(Part::Archive)) {
                return Err("--archive requires check release --part archive".into());
            }
            match part {
                Some(Part::Rust) => rust_checks(None),
                Some(Part::Audit) => run("cargo", &["audit"]),
                Some(Part::Mqtt) => mqtt(),
                Some(Part::Preflight) => preflight(),
                Some(Part::Archive) => self::archive(archive),
                None => {
                    let msrv = format!("{}.0", env!("CARGO_PKG_RUST_VERSION"));
                    rust_checks(Some(&msrv))?;
                    rust_checks(Some("stable"))?;
                    run("cargo", &["audit"])?;
                    mqtt()?;
                    preflight()?;
                    self::archive(archive)
                }
            }
        }
        _ => Err("--part is valid only with check release".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_tasks_and_failures_are_explicit() {
        for args in [
            vec!["check"],
            vec!["check", "mqtt"],
            vec!["check", "release"],
            vec!["schema", "--check"],
            vec!["config-reference"],
            vec!["package", "--target", "x86_64-unknown-linux-gnu"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("xtask").chain(args)).is_ok());
        }
        assert!(
            run("netbaiot-nonexistent-test-tool", &[])
                .unwrap_err()
                .contains("BLOCKED")
        );
        assert!(run("cargo", &["definitely-not-a-cargo-command"]).is_err());
        assert!(write_generated("target/xtask-test-missing.json", b"{}", true).is_err());
        let name = format!("target/xtask-drift-test-{}.json", std::process::id());
        write_generated(&name, b"old", false).unwrap();
        assert!(
            write_generated(&name, b"new", true)
                .unwrap_err()
                .contains("drift")
        );
        assert!(write_generated(&name, b"old", true).is_ok());
        std::fs::remove_file(root().join(name)).unwrap();
    }
}
