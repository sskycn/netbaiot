use clap::{Parser, Subcommand, ValueEnum};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, ExitCode},
};

const EVIDENCE_FILE_LIMIT: u64 = 1024 * 1024;
const EVIDENCE_TOTAL_LIMIT: u64 = 20 * 1024 * 1024;
const EVIDENCE_SUMMARY_LIMIT: u64 = 256 * 1024;
const EVIDENCE_EXCERPT_LIMIT: u64 = 64 * 1024;
const EVIDENCE_JSON_STRING_LIMIT: usize = 4 * 1024;

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
    Evidence,
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
    evidence_check()
}

fn evidence_check() -> Result<(), String> {
    let repo = root();
    let output = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "docs/performance",
        ])
        .current_dir(&repo)
        .output()
        .map_err(|_| "BLOCKED: git is required for the performance evidence check".to_owned())?;
    if !output.status.success() {
        return Err("could not enumerate tracked and unignored performance evidence files".into());
    }
    let mut paths = Vec::new();
    for path in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path = std::str::from_utf8(path)
            .map_err(|_| "git returned a non-UTF-8 performance evidence path".to_owned())?;
        paths.push(repo.join(path));
    }
    check_evidence_files(
        &repo.join("docs/performance"),
        paths,
        EVIDENCE_FILE_LIMIT,
        EVIDENCE_TOTAL_LIMIT,
        EVIDENCE_EXCERPT_LIMIT,
    )
}

fn check_evidence_files(
    performance_dir: &Path,
    paths: Vec<PathBuf>,
    file_limit: u64,
    total_limit: u64,
    excerpt_limit: u64,
) -> Result<(), String> {
    let allowlist_path = performance_dir.join("evidence-allowlist.json");
    let allowlist_file =
        read_evidence_file(&allowlist_path, "evidence-allowlist.json", file_limit)?;
    let allowlist: serde_json::Value =
        serde_json::from_slice(&allowlist_file.text.unwrap_or_default())
            .map_err(|error| format!("invalid evidence allowlist JSON: {error}"))?;
    if allowlist.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err("evidence allowlist must declare version 1".into());
    }
    let entries = allowlist
        .get("allowlist")
        .and_then(serde_json::Value::as_array)
        .ok_or("evidence allowlist must contain an allowlist array")?;
    let mut allowed = BTreeMap::new();
    for entry in entries {
        let relative = entry
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or("each evidence allowlist entry needs a path")?;
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
            || !(relative.ends_with(".excerpt.log")
                || (relative.ends_with(".json") && relative != "archive-manifest.json"))
        {
            return Err(format!(
                "allowlisted evidence path must be a safe relative *.excerpt.log or summary *.json path: {relative}"
            ));
        }
        let max_bytes = entry
            .get("max_bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("allowlist entry {relative} needs max_bytes"))?;
        let is_excerpt = relative.ends_with(".excerpt.log");
        let max_allowed = if is_excerpt {
            excerpt_limit
        } else {
            file_limit
        };
        if max_bytes == 0 || max_bytes > max_allowed {
            return Err(format!(
                "allowlist entry {relative} has max_bytes {max_bytes}; limit is {max_allowed}"
            ));
        }
        if !is_excerpt && max_bytes <= EVIDENCE_SUMMARY_LIMIT {
            return Err(format!(
                "summary allowlist entry {relative} must be above the ordinary summary limit {EVIDENCE_SUMMARY_LIMIT}"
            ));
        }
        if entry
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(format!(
                "allowlist entry {relative} needs a non-empty reason"
            ));
        }
        if allowed.insert(relative.to_owned(), max_bytes).is_some() {
            return Err(format!("duplicate evidence allowlist path: {relative}"));
        }
    }

    let mut total_bytes = 0_u64;
    let mut total_physical_bytes = 0_u64;
    let mut file_count = 0_usize;
    let mut seen_allowlist = BTreeSet::new();
    for path in paths {
        let Ok(relative) = path.strip_prefix(performance_dir) else {
            return Err(format!(
                "evidence path is outside docs/performance: {}",
                path.display()
            ));
        };
        let relative = relative.to_string_lossy().replace('\\', "/");
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!("cannot inspect evidence file {relative}: {error}"));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(format!(
                "unsupported evidence entry {relative}; only regular files are allowed"
            ));
        }
        file_count += 1;
        let evidence = read_evidence_file(&path, &relative, file_limit)?;
        let size = evidence.logical_size;
        let sizes = format!(
            "{size} canonical logical bytes ({} physical bytes)",
            evidence.physical_size
        );
        total_bytes = total_bytes
            .checked_add(size)
            .ok_or_else(|| format!("performance evidence byte count overflow at {relative}"))?;
        total_physical_bytes = total_physical_bytes
            .checked_add(evidence.physical_size)
            .ok_or_else(|| {
                format!("performance evidence physical byte count overflow at {relative}")
            })?;
        if size > file_limit {
            return Err(format!(
                "performance evidence file {relative} is {sizes}; limit is {file_limit}. Raw benchmark evidence belongs in GitHub Actions artifacts; commit only summary/manifest."
            ));
        }

        let is_excerpt = relative.ends_with(".excerpt.log");
        if is_excerpt {
            let Some(allowed_max) = allowed.get(&relative) else {
                return Err(format!(
                    "performance evidence excerpt {relative} is not in evidence-allowlist.json"
                ));
            };
            if size > *allowed_max {
                return Err(format!(
                    "performance evidence excerpt {relative} is {sizes}; allowlist limit is {allowed_max}"
                ));
            }
            seen_allowlist.insert(relative.clone());
        } else if allowed.contains_key(&relative) {
            if !relative.ends_with(".json") || relative == "archive-manifest.json" {
                return Err(format!(
                    "allowlisted evidence entry {relative} must be an excerpt log or summary JSON"
                ));
            }
            if size <= EVIDENCE_SUMMARY_LIMIT {
                return Err(format!(
                    "summary allowlist entry {relative} is unnecessary at {sizes}; limit is {EVIDENCE_SUMMARY_LIMIT}"
                ));
            }
            let allowed_max = allowed.get(&relative).copied().unwrap_or_default();
            if size > allowed_max {
                return Err(format!(
                    "allowlisted summary {relative} is {sizes}; allowlist limit is {allowed_max}"
                ));
            }
            seen_allowlist.insert(relative.clone());
        } else if relative.to_ascii_lowercase().ends_with(".json")
            && relative != "archive-manifest.json"
            && size > EVIDENCE_SUMMARY_LIMIT
        {
            return Err(format!(
                "summary JSON {relative} is {sizes}; ordinary summary limit is {EVIDENCE_SUMMARY_LIMIT}; add a justified evidence-allowlist.json entry or compact it"
            ));
        }

        let lower = relative.to_ascii_lowercase();
        let raw_extension = lower.ends_with(".raw.json")
            || lower.ends_with(".trace")
            || lower.ends_with(".jsonl")
            || lower.ends_with(".profraw")
            || lower.ends_with(".sample.txt")
            || lower.ends_with(".vmmap.txt")
            || lower.ends_with(".csv");
        if raw_extension || (lower.ends_with(".log") && !is_excerpt) {
            return Err(format!(
                "raw evidence file {relative} is {sizes}; file limit is {file_limit}. It is not permitted in docs/performance; write it under target/performance or target/evidence and upload it as a workflow artifact"
            ));
        }
        if lower.ends_with(".json") {
            let bytes = evidence.text.unwrap_or_default();
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|error| format!("invalid evidence JSON {relative}: {error}"))?;
            if let Some((field, count)) = raw_sample_array(&value, "$") {
                return Err(format!(
                    "raw sample array {field} ({count} entries) is in {relative} ({sizes}; file limit {file_limit}); summarize it and retain raw data as a workflow artifact"
                ));
            }
            if let Some((field, string_bytes)) =
                oversized_json_string(&value, "$", EVIDENCE_JSON_STRING_LIMIT)
            {
                return Err(format!(
                    "embedded JSON text field {field} is {string_bytes} bytes; limit is {EVIDENCE_JSON_STRING_LIMIT}; keep command output outside docs/performance"
                ));
            }
        }
    }
    if total_bytes > total_limit {
        return Err(format!(
            "docs/performance contains {total_bytes} canonical logical bytes ({total_physical_bytes} physical bytes); total limit is {total_limit}. Keep methods and summaries in Git; move raw evidence to workflow artifacts."
        ));
    }
    if let Some(unused) = allowed.keys().find(|path| !seen_allowlist.contains(*path)) {
        return Err(format!(
            "evidence allowlist entry {unused} does not match a tracked or unignored file"
        ));
    }
    println!(
        "PASS performance evidence budget: {file_count} files, {total_bytes} canonical logical bytes ({total_physical_bytes} physical bytes; file limit {file_limit}, total limit {total_limit})"
    );
    Ok(())
}

struct EvidenceFile {
    physical_size: u64,
    logical_size: u64,
    text: Option<Vec<u8>>,
}

fn read_evidence_file(
    path: &Path,
    relative: &str,
    file_limit: u64,
) -> Result<EvidenceFile, String> {
    let lower = relative.to_ascii_lowercase();
    let textual = [".json", ".md", ".excerpt.log", ".txt"]
        .iter()
        .any(|extension| lower.ends_with(extension));
    // CRLF can at most double LF text. This bounds reads independently of the
    // logical budget, without treating unknown/binary formats as text.
    let physical_limit = if textual {
        file_limit
            .checked_mul(2)
            .ok_or("evidence physical limit overflow")?
    } else {
        file_limit
    };
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect evidence file {relative}: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "unsupported evidence entry {relative}; only regular files are allowed"
        ));
    }
    let mut physical_size = metadata.len();
    if physical_size > physical_limit {
        return Err(format!(
            "performance evidence file {relative} is {physical_size} physical bytes; physical safety limit is {physical_limit} (canonical logical bytes not computed; file limit {file_limit})"
        ));
    }
    if !textual {
        return Ok(EvidenceFile {
            physical_size,
            logical_size: physical_size,
            text: None,
        });
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .and_then(|file| {
            file.take(physical_limit.saturating_add(1))
                .read_to_end(&mut bytes)
        })
        .map_err(|error| format!("cannot read evidence text {relative}: {error}"))?;
    physical_size = bytes.len() as u64;
    if physical_size > physical_limit {
        return Err(format!(
            "evidence text {relative} grew beyond physical safety limit {physical_limit}"
        ));
    }
    std::str::from_utf8(&bytes)
        .map_err(|error| format!("invalid UTF-8 evidence text {relative}: {error}"))?;
    let crlf_count = bytes.windows(2).filter(|pair| *pair == b"\r\n").count() as u64;
    Ok(EvidenceFile {
        physical_size,
        logical_size: physical_size - crlf_count,
        text: Some(bytes),
    })
}

fn raw_sample_array(value: &serde_json::Value, path: &str) -> Option<(String, usize)> {
    const RAW_KEYS: &[&str] = &[
        "samples",
        "client_samples",
        "command_traces",
        "all_samples",
        "latency_samples",
        "resource_samples",
        "trace_measurements",
    ];
    match value {
        serde_json::Value::Object(fields) => {
            for (key, child) in fields {
                let lower_key = key.to_ascii_lowercase();
                if let Some(values) = child
                    .as_array()
                    .filter(|values| RAW_KEYS.contains(&lower_key.as_str()) && values.len() > 32)
                {
                    return Some((format!("{path}.{key}"), values.len()));
                }
                if let Some(found) = raw_sample_array(child, &format!("{path}.{key}")) {
                    return Some(found);
                }
            }
            None
        }
        serde_json::Value::Array(values) => values
            .iter()
            .enumerate()
            .find_map(|(index, child)| raw_sample_array(child, &format!("{path}[{index}]"))),
        _ => None,
    }
}

fn oversized_json_string(
    value: &serde_json::Value,
    path: &str,
    limit: usize,
) -> Option<(String, usize)> {
    match value {
        serde_json::Value::String(text) if text.len() > limit => {
            Some((path.to_owned(), text.len()))
        }
        serde_json::Value::Object(fields) => fields
            .iter()
            .find_map(|(key, child)| oversized_json_string(child, &format!("{path}.{key}"), limit)),
        serde_json::Value::Array(values) => values.iter().enumerate().find_map(|(index, child)| {
            oversized_json_string(child, &format!("{path}[{index}]"), limit)
        }),
        _ => None,
    }
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
fn normalize_lines(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'\r' && bytes.get(at + 1) == Some(&b'\n') {
            at += 1;
        }
        out.push(bytes[at]);
        at += 1;
    }
    out
}
fn write_generated(path: &str, bytes: &[u8], check: bool) -> Result<(), String> {
    let path = root().join(path);
    if check {
        let old = std::fs::read(&path)
            .map_err(|_| format!("generated file missing: {}", path.display()))?;
        if normalize_lines(&old) != normalize_lines(bytes) {
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
            suite: Some(Suite::Evidence),
            part: None,
            archive: None,
            ..
        } => evidence_check(),
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
            vec!["check", "evidence"],
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
        assert_eq!(
            normalize_lines(b"{\r\n  \"value\": 1\r\n}\r\n"),
            normalize_lines(b"{\n  \"value\": 1\n}\n")
        );
        assert_ne!(
            normalize_lines(b"value=1\r\n"),
            normalize_lines(b"value=2\n")
        );
        std::fs::remove_file(root().join(name)).unwrap();
    }

    fn evidence_test_dir(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "netbaiot-xtask-evidence-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("evidence-allowlist.json"),
            "{\"version\":1,\"allowlist\":[]}",
        )
        .unwrap();
        directory
    }

    fn evidence_paths(directory: &Path, names: &[&str]) -> Vec<PathBuf> {
        std::iter::once(directory.join("evidence-allowlist.json"))
            .chain(names.iter().map(|name| directory.join(name)))
            .collect()
    }

    fn check_line_endings(
        directory: &Path,
        name: &str,
        contents: &str,
        file_limit: u64,
        total_limit: u64,
        expected_error: Option<&str>,
    ) {
        for text in [contents.to_owned(), contents.replace('\n', "\r\n")] {
            fs::write(directory.join(name), text).unwrap();
            let result = check_evidence_files(
                directory,
                evidence_paths(directory, &[name]),
                file_limit,
                total_limit,
                EVIDENCE_EXCERPT_LIMIT,
            );
            if let Some(expected) = expected_error {
                let error = result.unwrap_err();
                assert!(error.contains(expected), "{error}");
                assert!(error.contains("physical bytes"), "{error}");
            } else {
                assert!(result.is_ok(), "{result:?}");
            }
        }
    }

    #[test]
    fn evidence_summary_budget_is_independent_of_line_endings() {
        let directory = evidence_test_dir("summary-line-endings");
        check_line_endings(
            &directory,
            "summary.json",
            "{\n\"median\":12.5\n}\n",
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            None,
        );
        let near_limit = format!(
            "{{\n\"median\":1{}\n}}\n",
            "\n".repeat(EVIDENCE_SUMMARY_LIMIT as usize - 128)
        );
        assert!(near_limit.len() < EVIDENCE_SUMMARY_LIMIT as usize);
        assert!(near_limit.replace('\n', "\r\n").len() > EVIDENCE_SUMMARY_LIMIT as usize);
        check_line_endings(
            &directory,
            "summary.json",
            &near_limit,
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            None,
        );
        let too_large = format!(
            "{{\n\"median\":1{}\n}}\n",
            "\n".repeat(EVIDENCE_SUMMARY_LIMIT as usize)
        );
        check_line_endings(
            &directory,
            "summary.json",
            &too_large,
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            Some("ordinary summary limit"),
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_allowlisted_summary_and_excerpt_use_canonical_bytes() {
        for (name, contents) in [
            (
                "large.json",
                format!(
                    "{{\n\"median\":1{}\n}}\n",
                    "\n".repeat(EVIDENCE_SUMMARY_LIMIT as usize)
                ),
            ),
            ("failure.excerpt.log", "assertion failed\n".repeat(2048)),
        ] {
            let directory = evidence_test_dir("allowlist-line-endings");
            fs::write(directory.join("evidence-allowlist.json"), format!(
                "{{\n\"version\":1,\n\"allowlist\":[{{\"path\":\"{name}\",\"max_bytes\":{},\"reason\":\"bounded regression evidence\"}}]\n}}\n", contents.len()
            )).unwrap();
            check_line_endings(
                &directory,
                name,
                &contents,
                EVIDENCE_FILE_LIMIT,
                EVIDENCE_TOTAL_LIMIT,
                None,
            );
            check_line_endings(
                &directory,
                name,
                &format!("{contents}\n"),
                EVIDENCE_FILE_LIMIT,
                EVIDENCE_TOTAL_LIMIT,
                Some("allowlist limit"),
            );
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn evidence_file_and_total_limits_use_canonical_text_bytes() {
        let directory = evidence_test_dir("file-total-line-endings");
        let allowlist = "{\n\"version\":1,\n\"allowlist\":[]\n}\n";
        let contents = "method\n".repeat(18);
        let total = (allowlist.len() + contents.len()) as u64;
        for text in [allowlist.to_owned(), allowlist.replace('\n', "\r\n")] {
            fs::write(directory.join("evidence-allowlist.json"), text).unwrap();
            check_line_endings(&directory, "method.md", &contents, 128, total, None);
            check_line_endings(
                &directory,
                "method.md",
                &contents,
                128,
                total - 1,
                Some("total limit"),
            );
        }
        check_line_endings(
            &directory,
            "method.md",
            &format!("{}\n", "x".repeat(128)),
            128,
            EVIDENCE_TOTAL_LIMIT,
            Some("limit is 128"),
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_unknown_formats_are_raw_and_text_must_be_utf8() {
        let directory = evidence_test_dir("raw-and-utf8");
        let path = directory.join("fixture.bin");
        fs::write(&path, b"\r\n\xff\r\n").unwrap();
        let evidence = read_evidence_file(&path, "fixture.bin", 128).unwrap();
        assert_eq!(evidence.logical_size, 5);
        let allowlist_size = fs::metadata(directory.join("evidence-allowlist.json"))
            .unwrap()
            .len();
        let result = check_evidence_files(
            &directory,
            evidence_paths(&directory, &["fixture.bin"]),
            128,
            allowlist_size + 4,
            EVIDENCE_EXCERPT_LIMIT,
        );
        assert!(result.unwrap_err().contains("total limit"));
        check_line_endings(
            &directory,
            "raw.log",
            "raw output\n",
            128,
            EVIDENCE_TOTAL_LIMIT,
            Some("not permitted"),
        );
        for name in [
            "invalid.json",
            "invalid.md",
            "invalid.excerpt.log",
            "invalid.txt",
            "evidence-allowlist.json",
        ] {
            fs::write(directory.join(name), b"\xff\r\n").unwrap();
            let result = check_evidence_files(
                &directory,
                evidence_paths(&directory, &[name]),
                EVIDENCE_FILE_LIMIT,
                EVIDENCE_TOTAL_LIMIT,
                EVIDENCE_EXCERPT_LIMIT,
            );
            let error = result.unwrap_err();
            assert!(error.contains("invalid UTF-8"), "{error}");
            assert!(error.contains(name), "{error}");
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_canonical_size_preserves_lone_cr_and_bounds_physical_reads() {
        let directory = evidence_test_dir("physical-limit");
        let path = directory.join("method.md");
        fs::write(&path, b"a\rb\r\nc\n").unwrap();
        let evidence = read_evidence_file(&path, "method.md", 128).unwrap();
        assert_eq!(evidence.physical_size, 7);
        assert_eq!(evidence.logical_size, 6);
        fs::write(&path, "\n".repeat(257)).unwrap();
        let error = read_evidence_file(&path, "method.md", 128).err().unwrap();
        assert!(error.contains("physical safety limit is 256"), "{error}");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_budget_accepts_summaries_and_explicit_small_excerpts() {
        let directory = evidence_test_dir("allowlist");
        let excerpt = directory.join("failure.excerpt.log");
        std::fs::write(&excerpt, "assertion failed: old_bytes == 200\n").unwrap();
        std::fs::write(
            directory.join("evidence-allowlist.json"),
            "{\"version\":1,\"allowlist\":[{\"path\":\"failure.excerpt.log\",\"max_bytes\":128,\"reason\":\"minimal red regression proof\"}]}",
        )
        .unwrap();
        let summary = directory.join("summary.json");
        std::fs::write(&summary, "{\"median\":12.5,\"passed\":true}\n").unwrap();
        let paths = vec![directory.join("evidence-allowlist.json"), summary, excerpt];
        assert!(
            check_evidence_files(
                &directory,
                paths,
                EVIDENCE_FILE_LIMIT,
                EVIDENCE_TOTAL_LIMIT,
                EVIDENCE_EXCERPT_LIMIT,
            )
            .is_ok()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_budget_requires_a_reasoned_large_summary_exception() {
        let directory = evidence_test_dir("large-summary");
        let summary = directory.join("legacy-summary.json");
        let contents = format!(
            "{{\"aggregate_values\":[{}]}}\n",
            std::iter::repeat_n("12345", 60_000)
                .collect::<Vec<_>>()
                .join(",")
        );
        std::fs::write(&summary, contents).unwrap();
        let size = std::fs::metadata(&summary).unwrap().len();
        let paths = evidence_paths(&directory, &["legacy-summary.json"]);
        let error = check_evidence_files(
            &directory,
            paths.clone(),
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            EVIDENCE_EXCERPT_LIMIT,
        )
        .unwrap_err();
        assert!(error.contains("ordinary summary limit"));

        let allowlist = format!(
            "{{\"version\":1,\"allowlist\":[{{\"path\":\"legacy-summary.json\",\"max_bytes\":{size},\"reason\":\"legacy aggregate retained with its original values\"}}]}}"
        );
        std::fs::write(directory.join("evidence-allowlist.json"), allowlist).unwrap();
        assert!(
            check_evidence_files(
                &directory,
                paths,
                EVIDENCE_FILE_LIMIT,
                EVIDENCE_TOTAL_LIMIT,
                EVIDENCE_EXCERPT_LIMIT,
            )
            .is_ok()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_budget_rejects_raw_patterns_and_sample_arrays() {
        let directory = evidence_test_dir("raw");
        let trace = directory.join("capture.jsonl");
        std::fs::write(&trace, "{}\n").unwrap();
        let error = check_evidence_files(
            &directory,
            evidence_paths(&directory, &["capture.jsonl"]),
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            EVIDENCE_EXCERPT_LIMIT,
        )
        .unwrap_err();
        assert!(error.contains("raw evidence file capture.jsonl"));
        std::fs::remove_file(trace).unwrap();

        let samples = directory.join("summary.json");
        let json = format!(
            "{{\"records\":[{{}},{{}},{{\"samples\":[{}]}}]}}\n",
            std::iter::repeat_n("0", 33).collect::<Vec<_>>().join(",")
        );
        std::fs::write(&samples, json).unwrap();
        let error = check_evidence_files(
            &directory,
            evidence_paths(&directory, &["summary.json"]),
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            EVIDENCE_EXCERPT_LIMIT,
        )
        .unwrap_err();
        assert!(error.contains("raw sample array $.records[2].samples (33 entries)"));

        let embedded = directory.join("embedded-output.json");
        std::fs::write(
            &embedded,
            format!(
                "{{\"checks\":[{{\"tail\":[\"{}\"]}}]}}\n",
                "x".repeat(EVIDENCE_JSON_STRING_LIMIT + 1)
            ),
        )
        .unwrap();
        let error = check_evidence_files(
            &directory,
            evidence_paths(&directory, &["embedded-output.json"]),
            EVIDENCE_FILE_LIMIT,
            EVIDENCE_TOTAL_LIMIT,
            EVIDENCE_EXCERPT_LIMIT,
        )
        .unwrap_err();
        assert!(error.contains("embedded JSON text field $.checks[0].tail[0]"));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn evidence_budget_reports_total_directory_limit() {
        let directory = evidence_test_dir("total");
        let first = directory.join("first.md");
        let second = directory.join("second.md");
        std::fs::write(&first, "a".repeat(60)).unwrap();
        std::fs::write(&second, "b".repeat(60)).unwrap();
        let allowlist_size = std::fs::metadata(directory.join("evidence-allowlist.json"))
            .unwrap()
            .len();
        let error = check_evidence_files(
            &directory,
            evidence_paths(&directory, &["first.md", "second.md"]),
            128,
            allowlist_size + 100,
            EVIDENCE_EXCERPT_LIMIT,
        )
        .unwrap_err();
        assert!(error.contains("total limit"));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
