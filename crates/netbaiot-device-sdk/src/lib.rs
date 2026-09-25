//! NetbaIoT MQTT Device Profile client. No broker, hidden runtime or disk queue.
mod mqtt;
mod profile;
pub use profile::*;
#[cfg(test)]
mod tests;
