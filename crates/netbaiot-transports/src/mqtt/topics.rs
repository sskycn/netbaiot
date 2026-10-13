use netbaiot_core::*;
use netbaiot_runtime::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicKind {
    Up,
    UpAck,
    Down,
    DownAck,
}
pub fn topic(device: &DeviceKey, kind: TopicKind) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/{}",
        device.tenant_id.as_str(),
        device.product_id.as_str(),
        device.device_id.as_str(),
        match kind {
            TopicKind::Up => "up",
            TopicKind::UpAck => "up_ack",
            TopicKind::Down => "down",
            TopicKind::DownAck => "down_ack",
        }
    )
}
pub fn publish_acl(auth: &AuthenticatedDevice, value: &str) -> Result<TopicKind> {
    if !auth.permissions.publish {
        return Err(Error::Forbidden);
    }
    // Each trusted identifier must be followed by its complete namespace separator.
    // This keeps exact identity matching without constructing a topic for every packet.
    let suffix = value
        .strip_prefix("v1/t/")
        .and_then(|value| value.strip_prefix(auth.device_key.tenant_id.as_str()))
        .and_then(|value| value.strip_prefix("/p/"))
        .and_then(|value| value.strip_prefix(auth.device_key.product_id.as_str()))
        .and_then(|value| value.strip_prefix("/d/"))
        .and_then(|value| value.strip_prefix(auth.device_key.device_id.as_str()))
        .and_then(|value| value.strip_prefix('/'));
    match suffix {
        Some("up") => Ok(TopicKind::Up),
        Some("down_ack") if auth.permissions.commands => Ok(TopicKind::DownAck),
        _ => Err(Error::Forbidden),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn auth(tenant: &str, product: &str, device: &str) -> AuthenticatedDevice {
        AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new(tenant).unwrap(),
                product_id: ProductId::new(product).unwrap(),
                device_id: DeviceId::new(device).unwrap(),
            },
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        }
    }

    #[test]
    fn canonical_publish_acl_is_identity_scoped() {
        let auth = auth("t", "p", "a");
        assert!(publish_acl(&auth, &topic(&auth.device_key, TopicKind::Up)).is_ok());
        assert!(publish_acl(&auth, "v1/t/t/p/p/d/b/up").is_err());
    }

    #[test]
    fn publish_acl_preserves_permission_and_topic_kind_rules() {
        let mut auth = auth("tenant", "product", "device");
        for publish in [false, true] {
            for commands in [false, true] {
                auth.permissions = Permissions { publish, commands };
                for kind in [
                    TopicKind::Up,
                    TopicKind::UpAck,
                    TopicKind::Down,
                    TopicKind::DownAck,
                ] {
                    let result = publish_acl(&auth, &topic(&auth.device_key, kind));
                    let allowed = publish
                        && (kind == TopicKind::Up || (commands && kind == TopicKind::DownAck));
                    if allowed {
                        assert_eq!(result.unwrap(), kind);
                    } else {
                        assert!(matches!(result, Err(Error::Forbidden)));
                    }
                }
            }
        }
    }

    #[test]
    fn publish_acl_matches_exact_topic_reference_at_namespace_boundaries() {
        // Keep the previous authorization algorithm as an independent oracle.
        let reference = |auth: &AuthenticatedDevice, value: &str| {
            if !auth.permissions.publish {
                None
            } else if value == topic(&auth.device_key, TopicKind::Up) {
                Some(TopicKind::Up)
            } else if auth.permissions.commands
                && value == topic(&auth.device_key, TopicKind::DownAck)
            {
                Some(TopicKind::DownAck)
            } else {
                None
            }
        };
        for (tenant, product, device) in [
            ("t".to_owned(), "p".to_owned(), "d".to_owned()),
            (
                "tenant".to_owned(),
                "product".to_owned(),
                "device".to_owned(),
            ),
            ("t_-.:".to_owned(), "p_-.:".to_owned(), "d_-.:".to_owned()),
            ("t".repeat(64), "p".repeat(64), "d".repeat(64)),
        ] {
            let mut auth = auth(&tenant, &product, &device);
            let mut values = vec![String::new(), "#".into(), "+".into(), "up".into()];
            for kind in [TopicKind::Up, TopicKind::DownAck] {
                let canonical = topic(&auth.device_key, kind);
                // Canonical identities and separators are ASCII; every truncation is valid UTF-8.
                for end in 0..=canonical.len() {
                    values.push(canonical[..end].to_owned());
                }
                for suffix in ["/", "/extra", "_extra", "\0", "\n", "+", "#", "设备"] {
                    values.push(format!("{canonical}{suffix}"));
                }
                for prefix in ["/", "$SYS/", " ", "设备/"] {
                    values.push(format!("{prefix}{canonical}"));
                }
                for (at, byte) in canonical.bytes().enumerate() {
                    if byte == b'/' {
                        values.push(format!("{}/{}", &canonical[..at], &canonical[at..]));
                        values.push(format!("{}{}", &canonical[..at], &canonical[at + 1..]));
                    }
                }
                for segment in ["t", "p", "d"] {
                    let expected = match segment {
                        "t" => &tenant,
                        "p" => &product,
                        _ => &device,
                    };
                    for replacement in [
                        String::new(),
                        expected[..expected.len() - 1].to_owned(),
                        format!("{expected}x"),
                        format!("x{expected}"),
                        "+".into(),
                        "#".into(),
                        "设备".into(),
                    ] {
                        values.push(canonical.replacen(
                            &format!("/{segment}/{expected}/"),
                            &format!("/{segment}/{replacement}/"),
                            1,
                        ));
                    }
                }
            }
            for publish in [false, true] {
                for commands in [false, true] {
                    auth.permissions = Permissions { publish, commands };
                    for value in &values {
                        let result = publish_acl(&auth, value);
                        assert_eq!(
                            result.as_ref().ok().copied(),
                            reference(&auth, value),
                            "{value:?}"
                        );
                        if result.is_err() {
                            assert!(matches!(result, Err(Error::Forbidden)));
                        }
                    }
                }
            }
        }
    }
}
