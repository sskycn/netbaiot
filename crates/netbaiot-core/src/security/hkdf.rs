use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

/// No Debug/serialization; owned secret bytes are erased when dropped.
pub struct SecretKey(Zeroizing<[u8; 32]>);

impl SecretKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("HKDF-SHA256 key derivation failed")]
pub struct KeyDerivationError;

/// RFC 5869 extract and expand, with a fixed 256-bit output.
pub fn derive_secret(
    input_key_material: &[u8],
    salt: &[u8],
    info: &[u8],
) -> Result<SecretKey, KeyDerivationError> {
    let mut output = Zeroizing::new([0; 32]);
    Hkdf::<Sha256>::new(Some(salt), input_key_material)
        .expand(info, output.as_mut())
        .map_err(|_| KeyDerivationError)?;
    Ok(SecretKey(output))
}

/// Provisioning profile v0.3, separate from the existing NBI1/NBA1 wire versions.
/// Devices and the authority use the same profile; neither HMAC nor its input
/// bytes change. Only the resulting 32-byte verifier key reaches the gateway.
pub fn derive_udp_hmac_key(root_secret: &[u8; 32]) -> Result<SecretKey, KeyDerivationError> {
    derive_secret(
        root_secret,
        b"netbaiot/device-key/v0.3",
        b"netbaiot/udp-hmac-sha256/v0.3",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc5869_sha256_case_one() {
        let salt: Vec<u8> = (0x00..=0x0c).collect();
        let info: Vec<u8> = (0xf0..=0xf9).collect();
        let output = derive_secret(&[0x0b; 22], &salt, &info).unwrap();
        let expected = [
            0x3c, 0xb2, 0x5f, 0x25, 0xfa, 0xac, 0xd5, 0x7a, 0x90, 0x43, 0x4f, 0x64, 0xd0, 0x36,
            0x2f, 0x2a, 0x2d, 0x2d, 0x0a, 0x90, 0xcf, 0x1a, 0x5a, 0x4c, 0x5d, 0xb0, 0x2d, 0x56,
            0xec, 0xc4, 0xc5, 0xbf,
        ];
        assert_eq!(output.expose(), &expected);
    }

    #[test]
    fn derivation_is_stable_and_separates_keys_and_purposes() {
        let first = derive_udp_hmac_key(&[7; 32]).unwrap();
        assert_eq!(
            first.expose(),
            derive_udp_hmac_key(&[7; 32]).unwrap().expose()
        );
        assert_ne!(
            first.expose(),
            derive_udp_hmac_key(&[8; 32]).unwrap().expose()
        );
        assert_ne!(
            first.expose(),
            derive_secret(&[7; 32], b"other", b"other")
                .unwrap()
                .expose()
        );
    }
}
