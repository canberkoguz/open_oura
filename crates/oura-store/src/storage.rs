//! Optional SQLite persistence (feature `storage`).
//!
//! Events are stored with their raw body retained, so unknown event types are
//! never lost and can be decoded later. A per-device sync cursor enables
//! incremental syncs. Re-syncing is idempotent: identical events are de-duped.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use oura_protocol::device::{Battery, DeviceInfo};
use crate::error::Result;
use oura_protocol::events::RingEvent;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS device (
    serial        TEXT PRIMARY KEY,
    hardware_id   TEXT,
    firmware      TEXT,
    api_version   TEXT,
    mac           TEXT,
    updated_unix  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    serial        TEXT PRIMARY KEY,
    next_cursor   INTEGER NOT NULL,
    last_sync_unix INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    serial         TEXT NOT NULL,
    tag            INTEGER NOT NULL,
    name           TEXT NOT NULL,
    ring_timestamp INTEGER NOT NULL,
    body           BLOB NOT NULL,
    decoded_json   TEXT,
    captured_unix  INTEGER NOT NULL,
    UNIQUE(serial, tag, ring_timestamp, body)
);
CREATE INDEX IF NOT EXISTS idx_events_serial_tag ON events(serial, tag);
-- Serves the read side. A consumer charts a metric by selecting one device's
-- events of a given name across a ring_timestamp range, and the index above
-- cannot answer that: with a single device in the database its serial prefix
-- matches every row, so the query degenerates to a full pass with a rowid
-- lookup each, then a temp b-tree to sort. Measured on a 91,183-event
-- database, nine metrics cost 492 ms of SQL without this index and 116 ms
-- with it -- and, more to the point, the cost stops scaling with the table
-- and starts scaling with the result: one metric returning 69 rows went from
-- 44.9 ms to 0.1 ms.
--
-- It costs about 20% of the file (3.9 MB there) and roughly doubles the
-- insert time per event, which is invisible against a sync that is BLE-bound.
-- Because this schema is replayed with execute_batch on every store open and
-- every statement is IF NOT EXISTS, adding the line is also what upgrades
-- databases that already exist: they gain the index on the next open. Note
-- that this reaches only readers that go through this crate -- a database
-- written by something else (oura-dash's /ingest endpoint, which is Python
-- and does not replay this schema) needs the index created out of band.
CREATE INDEX IF NOT EXISTS idx_events_serial_name_ts
    ON events(serial, name, ring_timestamp);

CREATE TABLE IF NOT EXISTS readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    serial        TEXT NOT NULL,
    kind          TEXT NOT NULL,
    value         REAL NOT NULL,
    unit          TEXT,
    captured_unix INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_serial_kind ON readings(serial, kind);
"#;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One stored event, as read back out for incremental upload.
///
/// `body` is the raw event payload exactly as the ring sent it; `decoded_json`
/// is present only for tags the decoders understand at the time the row was
/// written (or last re-decoded).
#[derive(Clone, Debug)]
pub struct EventRow {
    pub id: i64,
    pub serial: String,
    pub tag: u8,
    pub name: String,
    pub ring_timestamp: i64,
    pub body: Vec<u8>,
    pub decoded_json: Option<String>,
    pub captured_unix: i64,
}

/// A SQLite-backed store for ring data.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open (creating if needed) a database at `path` and ensure the schema.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let conn = Connection::open(path)?;
        // Health data + device identifiers are sensitive; keep the DB owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| crate::error::Error::Storage(e.to_string()))?;
        }
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    /// Record/refresh device metadata.
    pub fn upsert_device(
        &self,
        serial: &str,
        hardware_id: Option<&str>,
        info: Option<&DeviceInfo>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO device (serial, hardware_id, firmware, api_version, mac, updated_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(serial) DO UPDATE SET
               hardware_id=COALESCE(excluded.hardware_id, device.hardware_id),
               firmware=COALESCE(excluded.firmware, device.firmware),
               api_version=COALESCE(excluded.api_version, device.api_version),
               mac=COALESCE(excluded.mac, device.mac),
               updated_unix=excluded.updated_unix",
            params![
                serial,
                hardware_id,
                info.map(|i| i.firmware_version.clone()),
                info.map(|i| i.api_version.clone()),
                info.map(|i| i.mac.clone()),
                now_unix(),
            ],
        )?;
        Ok(())
    }

    /// The persisted incremental-sync cursor (deciseconds), or 0 if none.
    pub fn cursor(&self, serial: &str) -> Result<u32> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT next_cursor FROM sync_state WHERE serial = ?1",
                params![serial],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.unwrap_or(0) as u32)
    }

    /// Persist the next sync cursor.
    pub fn set_cursor(&self, serial: &str, cursor: u32) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_state (serial, next_cursor, last_sync_unix)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(serial) DO UPDATE SET
               next_cursor=excluded.next_cursor,
               last_sync_unix=excluded.last_sync_unix",
            params![serial, cursor as i64, now_unix()],
        )?;
        Ok(())
    }

    /// Insert an event, ignoring exact duplicates. Returns true if a row was added.
    pub fn insert_event(&self, serial: &str, ev: &RingEvent) -> Result<bool> {
        let decoded = ev
            .decoded
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default());
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO events
               (serial, tag, name, ring_timestamp, body, decoded_json, captured_unix)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                serial,
                ev.tag as i64,
                ev.name,
                ev.timestamp as i64,
                ev.body,
                decoded,
                now_unix(),
            ],
        )?;
        Ok(changed > 0)
    }

    /// Record a scalar reading (e.g. live HR bpm, SpO2 %, battery %).
    pub fn insert_reading(&self, serial: &str, kind: &str, value: f64, unit: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO readings (serial, kind, value, unit, captured_unix)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![serial, kind, value, unit, now_unix()],
        )?;
        Ok(())
    }

    /// Convenience: store a battery reading.
    pub fn insert_battery(&self, serial: &str, battery: &Battery) -> Result<()> {
        self.insert_reading(serial, "battery_percent", battery.percent as f64, "%")
    }

    /// Re-decode every stored event body with the current decoders, updating
    /// `decoded_json`. Returns `(rows_with_decode, total_rows)`. Lets new decoders
    /// be applied to events captured before they existed — no re-sync needed.
    pub fn redecode(&self) -> Result<(usize, usize)> {
        let rows: Vec<(i64, i64, Vec<u8>)> = {
            let mut stmt = self.conn.prepare("SELECT id, tag, body FROM events")?;
            let collected = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            collected
        };
        let total = rows.len();
        let mut decoded_count = 0;
        for (id, tag, body) in rows {
            let decoded = oura_protocol::events::decode_event_body(tag as u8, &body)
                .map(|v| serde_json::to_string(&v).unwrap_or_default());
            if decoded.is_some() {
                decoded_count += 1;
            }
            let name = oura_protocol::events::event_name(tag as u8);
            self.conn.execute(
                "UPDATE events SET decoded_json = ?1, name = ?2 WHERE id = ?3",
                params![decoded, name, id],
            )?;
        }
        Ok((decoded_count, total))
    }

    /// All decoded events as `(ring_timestamp_deciseconds, tag, decoded_json,
    /// captured_unix)`, ordered by ring time. For analysis/reporting commands that
    /// reconstruct time series from stored events.
    pub fn decoded_events(&self) -> Result<Vec<(i64, u8, String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ring_timestamp, tag, decoded_json, captured_unix FROM events \
             WHERE decoded_json IS NOT NULL ORDER BY ring_timestamp",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)? as u8,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Events with `id` greater than `after_id`, oldest first, at most `limit`
    /// rows. `limit` of 0 returns nothing rather than everything.
    ///
    /// The monotonic `id` is what makes incremental upload possible without a
    /// second writer: a consumer keeps the highest `id` it has accepted and
    /// asks for what came after. No `uploaded` column, no schema change, and
    /// the dashboard's read-only view of this file stays read-only.
    pub fn events_since(&self, after_id: i64, limit: u32) -> Result<Vec<EventRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, serial, tag, name, ring_timestamp, body, decoded_json, captured_unix \
             FROM events WHERE id > ?1 ORDER BY id LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![after_id, limit as i64], |r| {
                Ok(EventRow {
                    id: r.get(0)?,
                    serial: r.get(1)?,
                    tag: r.get::<_, i64>(2)? as u8,
                    name: r.get(3)?,
                    ring_timestamp: r.get(4)?,
                    body: r.get(5)?,
                    decoded_json: r.get(6)?,
                    captured_unix: r.get(7)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The highest event `id` in the store, or 0 when there are none. Lets a
    /// caller report how far an upload would have to reach without pulling the
    /// rows themselves.
    pub fn max_event_id(&self) -> Result<i64> {
        // COALESCE, not `.optional()`: MAX over an empty table returns one row
        // holding NULL rather than no rows at all.
        Ok(self
            .conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |r| r.get(0))?)
    }

    /// Distinct device serials that have stored events.
    pub fn device_serials(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT serial FROM events ORDER BY serial")?;
        let rows = stmt
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Count stored events grouped by event name (descending).
    pub fn event_counts(&self, serial: &str) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, COUNT(*) FROM events WHERE serial = ?1 GROUP BY name ORDER BY 2 DESC",
        )?;
        let rows = stmt
            .query_map(params![serial], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_dedup_and_cursor_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let ev = RingEvent {
            tag: 0x43,
            name: "debug_event",
            timestamp: 42,
            body: vec![1, 2, 3],
            decoded: None,
        };
        assert!(store.insert_event("S1", &ev).unwrap());
        assert!(!store.insert_event("S1", &ev).unwrap()); // duplicate ignored

        store.set_cursor("S1", 1234).unwrap();
        assert_eq!(store.cursor("S1").unwrap(), 1234);

        let counts = store.event_counts("S1").unwrap();
        assert_eq!(counts, vec![("debug_event".to_string(), 1)]);
    }

    #[test]
    fn events_since_pages_forward_without_gaps_or_repeats() {
        let store = Store::open_in_memory().unwrap();
        for ts in 0..10u32 {
            store
                .insert_event(
                    "S1",
                    &RingEvent {
                        tag: 0x43,
                        name: "debug_event",
                        timestamp: ts,
                        body: vec![ts as u8],
                        decoded: None,
                    },
                )
                .unwrap();
        }

        // Walk the whole table the way an uploader does: keep the last id seen
        // and ask for what came after. Every row exactly once, in order.
        let mut seen = Vec::new();
        let mut after = 0i64;
        loop {
            let page = store.events_since(after, 3).unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().id;
            seen.extend(page.iter().map(|r| r.ring_timestamp));
        }
        assert_eq!(seen, (0..10).collect::<Vec<i64>>());
        assert_eq!(store.max_event_id().unwrap(), after);

        // A limit of 0 must mean nothing, not everything -- SQLite's LIMIT 0
        // returns no rows, and an uploader that passed 0 by accident should
        // stall rather than pull the entire history.
        assert!(store.events_since(0, 0).unwrap().is_empty());
        // Past the end is empty, not an error.
        assert!(store.events_since(after, 10).unwrap().is_empty());
    }

    #[test]
    fn max_event_id_is_zero_on_an_empty_store() {
        // The uploader's high-water mark starts at 0, so an empty store must
        // not report something that would skip the first real row.
        let store = Store::open_in_memory().unwrap();
        assert_eq!(store.max_event_id().unwrap(), 0);
    }
}
