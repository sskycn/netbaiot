use crate::{ProtocolError, SourceMessageId};

/// Authenticated NBI1 message identity, scoped to a DeviceKey by the consumer.
/// The source ID is rebuilt from signed envelope fields for monotonic providers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpMessageIdentity {
    pub credential_version: u32,
    pub boot_id: [u8; 16],
    pub sequence: u64,
}

impl UdpMessageIdentity {
    pub fn source_message_id(&self) -> Result<SourceMessageId, ProtocolError> {
        let boot: String = self
            .boot_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        SourceMessageId::new(format!(
            "udp:{:08x}:{boot}:{:016x}",
            self.credential_version, self.sequence
        ))
    }

    pub fn parse(source: &str) -> Option<Self> {
        if source.len() != 62 {
            return None;
        }
        let mut parts = source.split(':');
        if parts.next()? != "udp" {
            return None;
        }
        let version = parts.next()?;
        let boot = parts.next()?;
        let sequence = parts.next()?;
        if version.len() != 8 || boot.len() != 32 || sequence.len() != 16 || parts.next().is_some()
        {
            return None;
        }
        // Canonical lowercase ASCII is required before indexing/slicing.
        if ![version, boot, sequence].iter().all(|part| {
            part.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            return None;
        }
        let mut boot_id = [0; 16];
        for (i, byte) in boot_id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&boot[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(Self {
            credential_version: u32::from_str_radix(version, 16).ok()?,
            boot_id,
            sequence: u64::from_str_radix(sequence, 16).ok()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_canonical_identity_preserves_full_unsigned_sequence() {
        let identity = UdpMessageIdentity {
            credential_version: u32::MAX,
            boot_id: [0xab; 16],
            sequence: u64::MAX,
        };
        let source = identity.source_message_id().unwrap();
        assert_eq!(
            source.as_str(),
            "udp:ffffffff:abababababababababababababababab:ffffffffffffffff"
        );
        assert_eq!(UdpMessageIdentity::parse(source.as_str()), Some(identity));
        for invalid in [
            "udp:42",
            &source.as_str().to_uppercase(),
            "udp:00000001:éééééééééééééééé:0000000000000001",
        ] {
            assert_eq!(UdpMessageIdentity::parse(invalid), None);
        }
    }
}
