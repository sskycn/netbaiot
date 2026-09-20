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
    if value == topic(&auth.device_key, TopicKind::Up) {
        Ok(TopicKind::Up)
    } else if auth.permissions.commands && value == topic(&auth.device_key, TopicKind::DownAck) {
        Ok(TopicKind::DownAck)
    } else {
        Err(Error::Forbidden)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_publish_acl_is_identity_scoped() {
        let auth = AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new("a").unwrap(),
            },
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        };
        assert!(publish_acl(&auth, &topic(&auth.device_key, TopicKind::Up)).is_ok());
        assert!(publish_acl(&auth, "v1/t/t/p/p/d/b/up").is_err());
    }
}
