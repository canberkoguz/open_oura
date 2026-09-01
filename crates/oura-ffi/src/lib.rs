//! `oura-ffi` — the boundary between the Rust core and a foreign BLE stack.
//!
//! The line is drawn so that Rust keeps everything that is byte-format
//! knowledge or subtle state — framing, the AES app-auth challenge, every event
//! decoder, the drain loop's cursor rules, the SQLite schema the dashboard
//! reads — and the foreign side keeps everything the platform already owns:
//! scanning, connecting, discovery, the notify subscription, the auth key in
//! secure storage, and the upload.
//!
//! Three things cross. A [`RingWriter`] and a [`SyncProgress`] go
//! foreign→Rust as callback interfaces; a [`RingSession`] goes Rust→foreign as
//! an object. Nothing async crosses in either direction: see
//! [`transport`] for why the half of `Transport` that could not cross was
//! turned around instead of translated.
//!
//! ```text
//! CoreBluetooth delegate ── on_notification ──▶ broadcast ──▶ transact
//!                                                                │
//!                    write_frame ◀── RingWriter ◀── OuraClient ◀──┘
//! ```
//!
//! See `docs/rust-swift-ffi.md` in the dashboard repo for the design and the
//! alternatives considered.

uniffi::setup_scaffolding!();

mod error;
#[cfg(test)]
mod tests;
mod session;
mod transport;
mod upload;

pub use error::FfiError;
pub use session::{RingSession, SyncReport};
pub use transport::{RingWriter, SyncProgress};
