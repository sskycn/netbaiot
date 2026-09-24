pub mod common;
pub mod v311;
pub mod v5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MqttVersion {
    #[default]
    V311,
    V5,
}
