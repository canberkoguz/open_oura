//! Encoding stored events for upload.
//!
//! The shape is deliberately flat and self-describing: the dashboard has no
//! ingest endpoint yet, and whatever it grows should not have to reverse a
//! clever encoding to insert rows with the same `INSERT OR IGNORE` semantics
//! the store already uses.

use serde::Serialize;
use serde_json::value::RawValue;

use oura_store::EventRow;

use crate::error::FfiError;

#[derive(Serialize)]
struct UploadEvent<'a> {
    id: i64,
    serial: &'a str,
    tag: u8,
    name: &'a str,
    ring_timestamp: i64,
    /// Hex rather than base64: the raw body is what makes an unknown event
    /// recoverable later, and hex is what every other tool in this repo prints.
    body_hex: String,
    /// Nested as real JSON, not as a string holding JSON. `RawValue` splices
    /// the already-serialised text in without a parse-and-reserialise round trip.
    decoded: Option<&'a RawValue>,
    captured_unix: i64,
}

/// Encode rows as a JSON array, ready to be an HTTP body.
pub(crate) fn encode(rows: &[EventRow]) -> Result<Vec<u8>, FfiError> {
    // Borrowed for the lifetime of the payload below.
    let decoded: Vec<Option<Box<RawValue>>> = rows
        .iter()
        .map(|r| {
            r.decoded_json
                .as_deref()
                // A row whose `decoded_json` is not valid JSON would poison the
                // whole array, so drop the decode and keep the row: `body_hex`
                // is the lossless part and the receiver can re-decode from it.
                .and_then(|s| RawValue::from_string(s.to_string()).ok())
        })
        .collect();

    let payload: Vec<UploadEvent> = rows
        .iter()
        .zip(&decoded)
        .map(|(r, d)| UploadEvent {
            id: r.id,
            serial: &r.serial,
            tag: r.tag,
            name: &r.name,
            ring_timestamp: r.ring_timestamp,
            body_hex: hex::encode(&r.body),
            decoded: d.as_deref(),
            captured_unix: r.captured_unix,
        })
        .collect();

    Ok(serde_json::to_vec(&payload)?)
}
