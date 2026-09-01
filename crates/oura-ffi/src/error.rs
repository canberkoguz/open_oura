//! The single error type that crosses the boundary.
//!
//! Every variant is one the Swift side can act on differently: `Disconnected`
//! means retry when the ring is back, `Auth` means the stored key is wrong and
//! retrying will never help, `Cancelled` means the user asked and nothing is
//! broken.

/// An error crossing the FFI boundary. Surfaces in Swift as a thrown error.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    /// The foreign writer refused a frame, or the link was never usable.
    #[error("bluetooth: {message}")]
    Bluetooth { message: String },
    /// The ring rejected the 16-byte key. Retrying with the same key will not help.
    #[error("authentication: {message}")]
    Auth { message: String },
    /// The ring answered, but not with something we could parse.
    #[error("protocol: {message}")]
    Protocol { message: String },
    /// The local database could not be opened, read or written.
    #[error("storage: {message}")]
    Storage { message: String },
    /// The link dropped mid-sync. Events already stored are kept and the
    /// cursor is already correct, so the next sync resumes rather than repeats.
    #[error("the ring disconnected")]
    Disconnected,
    /// `cancel()` was called. Same durability guarantee as `Disconnected`.
    #[error("cancelled")]
    Cancelled,
    /// A sync is already running on this session. Sequential by design: the
    /// ring is a single-conversation device and two drains would interleave
    /// their frames on one notify characteristic.
    #[error("a sync is already running on this session")]
    Busy,
    /// The auth key was not exactly 16 bytes.
    #[error("auth key must be 16 bytes, got {got}")]
    BadKeyLength { got: u32 },
}

impl From<oura_link::Error> for FfiError {
    fn from(e: oura_link::Error) -> Self {
        use oura_link::Error as L;
        match e {
            L::Auth(message) => FfiError::Auth { message },
            L::Protocol(message) => FfiError::Protocol { message },
            // Ble, DeviceNotFound, CharacteristicNotFound and Io all describe a
            // link that is not carrying frames, which is one situation to the
            // caller even though the crate distinguishes them.
            other => FfiError::Bluetooth {
                message: other.to_string(),
            },
        }
    }
}

impl From<oura_store::Error> for FfiError {
    fn from(e: oura_store::Error) -> Self {
        FfiError::Storage {
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for FfiError {
    fn from(e: serde_json::Error) -> Self {
        FfiError::Storage {
            message: format!("encoding events: {e}"),
        }
    }
}
