use netbaiot_server::{Config, TlsFiles};
use serde::Serialize;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};
const MANAGED: [&str; 3] = ["netbaiot.json", ".env.example", "README.md"];
#[derive(Serialize)]
pub struct InitResult {
    pub directory: PathBuf,
    pub files: Vec<&'static str>,
    pub production: bool,
    pub ready_to_run: bool,
}
/// Per-file atomic replacement, no directory deletion. Existing files are never
/// overwritten without force; create-new/hard-link protects the preflight race.
pub async fn create(
    directory: PathBuf,
    production: bool,
    force: bool,
) -> Result<InitResult, String> {
    tokio::task::spawn_blocking(move || create_sync(&directory, production, force))
        .await
        .map_err(|_| "init worker failed".to_owned())?
}
fn create_sync(directory: &Path, production: bool, force: bool) -> Result<InitResult, String> {
    match fs::symlink_metadata(directory) {
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
            return Err("init destination must be a real directory".into());
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err("cannot inspect init directory".into());
        }
        _ => (),
    }
    fs::create_dir_all(directory).map_err(|_| "cannot create init directory")?;
    let directory = directory
        .canonicalize()
        .map_err(|_| "cannot resolve init directory")?;
    for name in MANAGED {
        match fs::symlink_metadata(directory.join(name)) {
            Ok(meta) if !force || !meta.is_file() || meta.file_type().is_symlink() => {
                return Err(format!(
                    "Refusing to overwrite existing {name}; --force only replaces regular managed files."
                ));
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(format!("cannot inspect {name}"));
            }
            _ => (),
        }
    }
    match fs::symlink_metadata(directory.join("var")) {
        Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
            return Err(
                "var must be a real directory; refusing to follow a recovery symlink".into(),
            );
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err("cannot inspect local recovery path".into());
        }
        _ => (),
    }
    let mut config: Config =
        serde_json::from_str(include_str!("../../../configs/development.json"))
            .map_err(|_| "embedded development template is invalid")?;
    config.spool_directory = directory.join("var");
    config.delivery_url = Some("http://127.0.0.1:18080/events".into());
    if production {
        config.development = false;
        config.device_ingress = "0.0.0.0:8883"
            .parse()
            .map_err(|_| "invalid template address")?;
        config.credentials.clear();
        config.auth_provider_url = Some("https://auth.example.invalid/devices".into());
        config.delivery_url = Some("https://business.example.invalid/events".into());
        config.tls = Some(TlsFiles {
            certificate: "<set-certificate.pem>".into(),
            private_key: "<set-private-key.pem>".into(),
        });
    }
    let json = serde_json::to_vec_pretty(&config).map_err(|_| "cannot serialize init template")?;
    let environment = b"# Protected secret sources; fill securely. This file is not loaded automatically.\nNETBAIOT_ADMIN_SECRET=\nNETBAIOT_AUTH_PROVIDER_TOKEN=\nNETBAIOT_DELIVERY_TOKEN=\n";
    let readme = if production {
        "# NetbaIoT production skeleton\n\nConfiguration is NOT ready to run. Supply real TLS files, authentication provider and sink URLs, and protected secret sources before use. No production credentials were generated.\n\nRun `netbaiot config check --config netbaiot.json` then `netbaiot doctor --config netbaiot.json`.\nThe absolute recovery path points to this project's var directory; update it when relocating the project. .env.example is documentation only, not auto-loaded.\n"
    } else {
        "# Local NetbaIoT development\n\nDevelopment credentials only. Do not deploy this configuration or credential to production.\nThe HTTP sink at 127.0.0.1:18080/events is an example: start your own receiver before serve, or use `netbaiot demo` for a self-contained first experience.\n\nRun `netbaiot config check --config netbaiot.json`, `netbaiot doctor --config netbaiot.json`, then `netbaiot serve --config netbaiot.json`.\nProvide management credentials through protected environment sources. .env.example is not auto-loaded.\nThe recovery path is absolute to this project's var directory; update it when relocating.\n"
    };
    let data: [&[u8]; 3] = [&json, environment, readme.as_bytes()];
    let mut staged = Vec::new();
    let result = (|| {
        for bytes in data {
            let temporary = directory.join(format!(".netbaiot-init-{}", uuid::Uuid::new_v4()));
            let mut file = netbaiot_runtime::recovery_io::create_private(&temporary)
                .map_err(|_| "cannot stage init file")?;
            staged.push(temporary);
            file.write_all(bytes)
                .and_then(|_| file.sync_all())
                .map_err(|_| "cannot sync init file")?;
        }
        fs::create_dir_all(directory.join("var"))
            .map_err(|_| "cannot create local recovery directory")?;
        for (temporary, name) in staged.iter().zip(MANAGED) {
            let destination = directory.join(name);
            if force {
                netbaiot_runtime::recovery_io::replace_synced(temporary, &destination)
                    .map_err(|_| "cannot replace managed init file")?;
            } else {
                fs::hard_link(temporary, &destination)
                    .map_err(|_| "managed file appeared during init; refusing overwrite")?;
            }
        }
        Ok::<(), &str>(())
    })();
    for temporary in staged {
        let _ = fs::remove_file(temporary);
    }
    result.map_err(str::to_owned)?;
    Ok(InitResult {
        directory,
        files: MANAGED.to_vec(),
        production,
        ready_to_run: !production,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn templates_validate_and_force_preserves_unmanaged_files() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("project");
        create(directory.clone(), false, false).await.unwrap();
        let config = netbaiot_server::load_config_diagnostic(&directory.join("netbaiot.json"))
            .await
            .unwrap();
        assert!(
            netbaiot_server::check_config(&config, None, None)
                .await
                .valid
        );
        fs::write(directory.join("keep.txt"), b"user data").unwrap();
        let before = fs::read(directory.join("netbaiot.json")).unwrap();
        assert!(create(directory.clone(), true, false).await.is_err());
        assert_eq!(fs::read(directory.join("netbaiot.json")).unwrap(), before);
        create(directory.clone(), true, true).await.unwrap();
        let config = netbaiot_server::load_config_diagnostic(&directory.join("netbaiot.json"))
            .await
            .unwrap();
        assert!(config.credentials.is_empty());
        assert!(!config.development);
        assert!(config.tls.is_some());
        assert!(
            !netbaiot_server::check_config(&config, None, None)
                .await
                .valid
        );
        assert_eq!(fs::read(directory.join("keep.txt")).unwrap(), b"user data");
    }
}
