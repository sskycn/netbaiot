//! Key derivation only. Authentication, HMAC verification, and UDP replay retain
//! their existing owners; applications own master keys and device provisioning.
pub mod hkdf;
pub mod replay;
