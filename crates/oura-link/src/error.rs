//! Error and result types for the BLE link layer.
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("ble error: {0}")]
    Ble(String),
    #[error("no matching Oura ring found")]
    DeviceNotFound,
    #[error("characteristic not found: {0}")]
    CharacteristicNotFound(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Nothing came back within the quiet window: the request never reached
    /// the ring, or its reply never reached us. A link problem, not a verdict
    /// from the ring, so it says nothing about the key.
    #[error("no response from the ring to the {0}")]
    NoResponse(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(feature = "ble")]
impl From<btleplug::Error> for Error {
    fn from(e: btleplug::Error) -> Self {
        Error::Ble(e.to_string())
    }
}
