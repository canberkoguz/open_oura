//! The boundary exercised end to end against a scripted ring.
//!
//! These drive the *real* callback interface: a fake ring implements
//! [`RingWriter`], and its answers come back through
//! [`RingSession::on_notification`] exactly as CoreBluetooth's delegate would
//! deliver them. Nothing here reaches into the transport directly, so what is
//! tested is the arrangement the Swift side will use.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use oura_protocol::protocol::{self, Packet};
use tokio::sync::broadcast;

use crate::error::FfiError;
use crate::session::{RingSession, SyncReport};
use crate::transport::{RingWriter, SyncProgress};

/// Short enough that a whole scripted sync runs in well under a second.
const TEST_QUIET: Duration = Duration::from_millis(20);

const KEY: [u8; 16] = [
    0x44, 0x31, 0x96, 0x7d, 0x8b, 0xac, 0xc2, 0x65, 0x97, 0x43, 0x14, 0x2b, 0x68, 0x39, 0x1d, 0x9a,
];

/// A ring made of canned answers.
struct FakeRing {
    /// Set after construction: the writer and the session each need the other.
    session: Mutex<Weak<RingSession>>,
    /// `(ring_timestamp, tag, body)`, ascending.
    events: Vec<(u32, u8, Vec<u8>)>,
    /// Events returned per `GetEvent`, so the drain loop runs more than once.
    batch: usize,
    requests: Mutex<Vec<Vec<u8>>>,
    /// Stop answering once this many requests have been seen, standing in for a
    /// ring that goes away mid-sync.
    go_silent_after: Option<usize>,
    /// Invoked before answering, so a test can cancel from "another thread".
    on_request: Mutex<Option<Box<dyn Fn(&RingSession, usize) + Send>>>,
    auth_state: u8,
}

impl FakeRing {
    fn new(events: Vec<(u32, u8, Vec<u8>)>, batch: usize) -> Arc<Self> {
        Arc::new(Self {
            session: Mutex::new(Weak::new()),
            events,
            batch,
            requests: Mutex::new(Vec::new()),
            go_silent_after: None,
            on_request: Mutex::new(None),
            auth_state: 0x00, // success
        })
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    /// Every answer this ring would give to `frame`.
    fn answers(&self, frame: &[u8]) -> Vec<Vec<u8>> {
        let tag = frame[0];
        let payload = &frame[2..];
        match tag {
            // --- app auth ---
            0x2f if payload.first() == Some(&0x2b) => {
                let mut p = vec![0x2c];
                p.extend_from_slice(&[0x2d, 0x6a, 0x0a, 0x08, 0xc9, 0x9b, 0x43, 0x65, 0xf4, 0x58,
                                      0xe6, 0xe9, 0x73, 0x82, 0x11]);
                vec![Packet::new(0x2f, p).encode()]
            }
            0x2f if payload.first() == Some(&0x2d) => {
                vec![Packet::new(0x2f, vec![0x2e, self.auth_state]).encode()]
            }
            // Capabilities: this ring declines to answer, which must not be
            // fatal -- `run_sync` reads them for handshake order, not content.
            0x2f if payload.first() == Some(&0x01) => vec![],

            // --- metadata ---
            0x08 => vec![hex::decode("091202000003040301000105000cffeeddccbbaa").unwrap()],
            0x0c => vec![Packet::new(0x0d, vec![77, 0, 0]).encode()],
            0x18 if frame == protocol::product::SERIAL => {
                let mut p = vec![0x00];
                p.extend_from_slice(b"2016082448090214");
                vec![Packet::new(0x19, p).encode()]
            }
            0x18 if frame == protocol::product::HARDWARE => {
                let mut p = vec![0x00];
                p.extend_from_slice(b"BLB_03");
                vec![Packet::new(0x19, p).encode()]
            }

            // --- the drain ---
            0x10 => {
                let start = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let pending: Vec<_> =
                    self.events.iter().filter(|(ts, _, _)| *ts >= start).collect();
                let batch: Vec<_> = pending.iter().take(self.batch).collect();
                let mut out: Vec<Vec<u8>> = batch
                    .iter()
                    .map(|(ts, tag, body)| {
                        let mut p = ts.to_le_bytes().to_vec();
                        p.extend_from_slice(body);
                        Packet::new(*tag, p).encode()
                    })
                    .collect();
                let left = pending.len().saturating_sub(batch.len());
                out.push(
                    Packet::new(
                        0x11,
                        {
                            let mut p = vec![batch.len() as u8, 0];
                            p.extend_from_slice(&((left as u32) * 10).to_le_bytes());
                            p
                        },
                    )
                    .encode(),
                );
                out
            }
            _ => vec![],
        }
    }
}

impl RingWriter for FakeRing {
    fn write_frame(&self, frame: Vec<u8>) -> Result<(), FfiError> {
        let n = {
            let mut reqs = self.requests.lock().unwrap();
            reqs.push(frame.clone());
            reqs.len()
        };
        let session = self.session.lock().unwrap().upgrade().expect("session gone");

        if let Some(hook) = self.on_request.lock().unwrap().as_ref() {
            hook(&session, n);
        }
        if self.go_silent_after.is_some_and(|limit| n > limit) {
            return Ok(()); // accepted the write, but nothing ever comes back
        }
        for answer in self.answers(&frame) {
            session.on_notification(answer);
        }
        Ok(())
    }
}

/// Records what the progress callback saw.
#[derive(Default)]
struct RecordingProgress {
    calls: Mutex<Vec<(u32, u32)>>,
}

impl SyncProgress for RecordingProgress {
    fn batch_done(&self, events_so_far: u32, cursor: u32) {
        self.calls.lock().unwrap().push((events_so_far, cursor));
    }
}

/// Wire a fake ring to a session over a fresh database.
fn session_with(ring: Arc<FakeRing>) -> (Arc<RingSession>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("ring.db");
    let session =
        RingSession::open(ring.clone(), db.to_str().unwrap(), TEST_QUIET).unwrap();
    *ring.session.lock().unwrap() = Arc::downgrade(&session);
    (session, dir)
}

fn sample_events(n: u32) -> Vec<(u32, u8, Vec<u8>)> {
    // Tag 0x43 is `debug_event`; body bytes vary so dedupe is exercised by the
    // UNIQUE(serial, tag, ring_timestamp, body) constraint rather than by luck.
    (0..n).map(|i| (i * 10, 0x43u8, vec![i as u8, 0xaa])).collect()
}

// --- the property the whole bridge rests on -----------------------------

#[test]
fn current_thread_runtime_wakes_on_a_cross_thread_send() {
    // `run_sync` blocks a background thread inside `block_on`, and frames are
    // pushed in from the platform's delegate queue. If a current-thread runtime
    // with only the time driver did not wake from its park on that send, every
    // request would stall for its full quiet window and the design would be
    // unworkable. This is the check that says it does.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (tx, mut rx) = broadcast::channel::<Vec<u8>>(64);

    std::thread::spawn(move || {
        for i in 0..100u8 {
            std::thread::sleep(Duration::from_millis(1));
            tx.send(vec![i]).unwrap();
        }
    });

    let started = Instant::now();
    let n = rt.block_on(async {
        let mut n = 0;
        while n < 100 {
            match tokio::time::timeout(Duration::from_millis(1500), rx.recv()).await {
                Ok(Ok(_)) => n += 1,
                other => panic!("stalled after {n} frames: {other:?}"),
            }
        }
        n
    });
    assert_eq!(n, 100);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "100 frames took {:?} -- the runtime parked until its timer instead of \
         waking on the send",
        started.elapsed()
    );
}

// --- the happy path -----------------------------------------------------

fn run(ring: &Arc<FakeRing>, session: &Arc<RingSession>) -> Result<SyncReport, FfiError> {
    let _ = ring;
    session.run_sync(KEY.to_vec(), false, None)
}

#[test]
fn sync_authenticates_drains_and_reports() {
    let ring = FakeRing::new(sample_events(25), 10);
    let (session, _dir) = session_with(ring.clone());

    let report = run(&ring, &session).unwrap();

    assert_eq!(report.serial, "2016082448090214");
    assert_eq!(report.firmware.as_deref(), Some("3.4.3"));
    assert_eq!(report.battery_percent, Some(77));
    assert_eq!(report.events_received, 25);
    assert_eq!(report.rows_inserted, 25);
    // Cursor lands one past the newest event's timestamp (24 * 10).
    assert_eq!(report.next_cursor, 241);
    assert_eq!(report.max_event_id, 25);
}

#[test]
fn the_handshake_runs_in_the_documented_order() {
    // The ordering is load-bearing per docs/sync-orchestration.md, and it is
    // the reason `run_sync` is one call rather than nine. If it silently
    // reorders, this is the only place that would notice.
    let ring = FakeRing::new(sample_events(3), 10);
    let (session, _dir) = session_with(ring.clone());
    run(&ring, &session).unwrap();

    let reqs = ring.requests.lock().unwrap();
    let tags: Vec<(u8, Option<u8>)> = reqs
        .iter()
        .map(|f| (f[0], f.get(2).copied().filter(|_| f[0] == 0x2f)))
        .collect();

    assert_eq!(tags[0], (0x2f, Some(0x2b)), "nonce first");
    assert_eq!(tags[1], (0x2f, Some(0x2d)), "then authenticate");
    assert_eq!(tags[2], (0x2f, Some(0x01)), "then capabilities page 0");
    assert_eq!(tags[3], (0x2f, Some(0x01)), "then capabilities page 1");
    // Metadata, then the drain -- never before auth.
    assert_eq!(tags[4].0, 0x18, "serial");
    assert_eq!(tags[5].0, 0x08, "firmware");
    assert_eq!(tags[6].0, 0x18, "hardware id");
    assert_eq!(tags[7].0, 0x0c, "battery");
    assert!(tags[8..].iter().all(|t| t.0 == 0x10), "then only GetEvent");
}

#[test]
fn sync_time_is_the_callers_choice_not_a_hidden_write() {
    // Writing the host clock into the ring is a state change. It must happen
    // when asked and never when not.
    for (want, expected) in [(false, 0usize), (true, 1)] {
        let ring = FakeRing::new(sample_events(1), 10);
        let (session, _dir) = session_with(ring.clone());
        session.run_sync(KEY.to_vec(), want, None).unwrap();
        let n = ring
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f[0] == 0x12)
            .count();
        assert_eq!(n, expected, "sync_time={want}");
    }
}

#[test]
fn resyncing_is_idempotent_and_resumes_from_the_cursor() {
    let ring = FakeRing::new(sample_events(12), 5);
    let (session, dir) = session_with(ring.clone());
    let first = run(&ring, &session).unwrap();
    assert_eq!(first.rows_inserted, 12);
    drop(session);

    // A second session over the same database, same ring: the cursor is past
    // everything, so the ring offers nothing and no rows are added.
    let ring2 = FakeRing::new(sample_events(12), 5);
    let db = dir.path().join("ring.db");
    let session2 = RingSession::open(ring2.clone(), db.to_str().unwrap(), TEST_QUIET).unwrap();
    *ring2.session.lock().unwrap() = Arc::downgrade(&session2);
    let second = session2.run_sync(KEY.to_vec(), false, None).unwrap();

    assert_eq!(second.events_received, 0);
    assert_eq!(second.rows_inserted, 0);
    assert_eq!(second.next_cursor, first.next_cursor);
    assert_eq!(second.max_event_id, first.max_event_id);
}

#[test]
fn progress_reports_events_pulled_after_every_batch() {
    let ring = FakeRing::new(sample_events(25), 10);
    let (session, _dir) = session_with(ring.clone());
    let progress = Arc::new(RecordingProgress::default());

    session
        .run_sync(KEY.to_vec(), false, Some(progress.clone()))
        .unwrap();

    let calls = progress.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3, "one per drained batch: {calls:?}");
    // Monotonic in both axes, and the last one matches the final report.
    assert_eq!(calls, vec![(10, 91), (20, 191), (25, 241)]);
}

// --- interruption -------------------------------------------------------

#[test]
fn cancel_stops_the_drain_and_keeps_what_was_stored() {
    let ring = FakeRing::new(sample_events(50), 10);
    let (session, _dir) = session_with(ring.clone());
    // Cancel from inside the writer, standing in for a UI tap arriving on
    // another thread partway through the drain.
    *ring.on_request.lock().unwrap() = Some(Box::new(|session, n| {
        if n == 10 {
            session.cancel();
        }
    }));

    let err = session.run_sync(KEY.to_vec(), false, None).unwrap_err();
    assert!(matches!(err, FfiError::Cancelled), "got {err:?}");

    // Cancelling is not a rollback: the batches that completed are durable and
    // the cursor moved with them, so the next sync resumes rather than repeats.
    let json = session.events_since(0, 1000).unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let stored = rows.as_array().unwrap().len();
    assert!(stored > 0 && stored < 50, "stored {stored} of 50");
}

#[test]
fn a_ring_that_goes_silent_reports_disconnected_not_a_generic_failure() {
    let mut ring = FakeRing::new(sample_events(50), 10);
    Arc::get_mut(&mut ring).unwrap().go_silent_after = Some(9);
    let (session, _dir) = session_with(ring.clone());
    *ring.on_request.lock().unwrap() = Some(Box::new(|session, n| {
        // The platform notices the drop and tells us; the in-flight sync then
        // ends at its next write.
        if n == 10 {
            session.on_disconnected();
        }
    }));

    let err = session.run_sync(KEY.to_vec(), false, None).unwrap_err();
    assert!(matches!(err, FfiError::Disconnected), "got {err:?}");
}

#[test]
fn a_rejected_key_is_an_auth_error_not_a_bluetooth_one() {
    // The distinction matters to the caller: retrying an auth failure with the
    // same key never helps, where retrying a link failure usually does.
    let mut ring = FakeRing::new(sample_events(1), 10);
    Arc::get_mut(&mut ring).unwrap().auth_state = 0x03; // NotOriginalOnboardedDevice
    let (session, _dir) = session_with(ring.clone());

    let err = session.run_sync(KEY.to_vec(), false, None).unwrap_err();
    assert!(matches!(err, FfiError::Auth { .. }), "got {err:?}");
}

#[test]
fn a_short_key_is_rejected_before_anything_is_written_to_the_ring() {
    let ring = FakeRing::new(sample_events(1), 10);
    let (session, _dir) = session_with(ring.clone());

    let err = session.run_sync(vec![0u8; 8], false, None).unwrap_err();
    assert!(matches!(err, FfiError::BadKeyLength { got: 8 }), "got {err:?}");
    assert_eq!(ring.request_count(), 0, "must not touch the ring");
}

#[test]
fn a_second_concurrent_sync_is_refused_rather_than_interleaved() {
    // Two drains would interleave their frames on one notify characteristic,
    // and `transact` has no way to tell whose response is whose.
    let ring = FakeRing::new(sample_events(40), 10);
    let (session, _dir) = session_with(ring.clone());
    let second_result = Arc::new(Mutex::new(None));
    let saw_busy = Arc::new(AtomicBool::new(false));

    let s2 = session.clone();
    let out = second_result.clone();
    let flag = saw_busy.clone();
    *ring.on_request.lock().unwrap() = Some(Box::new(move |_, n| {
        if n == 6 && !flag.swap(true, Ordering::SeqCst) {
            // Re-entrant from another thread while the first sync is mid-drain.
            let s2 = s2.clone();
            let out = out.clone();
            std::thread::spawn(move || {
                *out.lock().unwrap() = Some(s2.run_sync(KEY.to_vec(), false, None));
            })
            .join()
            .unwrap();
        }
    }));

    session.run_sync(KEY.to_vec(), false, None).unwrap();
    let second = second_result.lock().unwrap().take().expect("second sync ran");
    assert!(matches!(second, Err(FfiError::Busy)), "got {second:?}");
}

// --- the upload shape ---------------------------------------------------

#[test]
fn events_since_nests_decoded_json_rather_than_stringifying_it() {
    // A consumer that has to `JSON.parse` a field inside a JSON document is a
    // consumer that will eventually get it wrong.
    let ring = FakeRing::new(
        // Tag 0x0b is a real decoded event type, so `decoded` is populated.
        vec![(10, 0x43, vec![1, 2]), (20, 0x43, vec![3, 4])],
        10,
    );
    let (session, _dir) = session_with(ring.clone());
    run(&ring, &session).unwrap();

    let json = session.events_since(0, 100).unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&json).unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first["serial"], "2016082448090214");
    assert_eq!(first["ring_timestamp"], 10);
    assert_eq!(first["body_hex"], "0102");
    assert_eq!(first["tag"], 0x43);
    // Present as a key, and never a string holding JSON.
    assert!(first.get("decoded").is_some());
    assert!(
        !first["decoded"].is_string(),
        "decoded came back stringified: {}",
        first["decoded"]
    );

    // Paging by the id the rows carry walks forward without repeats.
    let after = first["id"].as_i64().unwrap();
    let page2: serde_json::Value =
        serde_json::from_slice(&session.events_since(after, 100).unwrap()).unwrap();
    assert_eq!(page2.as_array().unwrap().len(), 1);
    assert_eq!(page2[0]["ring_timestamp"], 20);
}

#[test]
fn max_event_id_tracks_the_store_for_an_uploaders_high_water_mark() {
    let ring = FakeRing::new(sample_events(7), 10);
    let (session, _dir) = session_with(ring.clone());
    assert_eq!(session.max_event_id().unwrap(), 0);
    let report = run(&ring, &session).unwrap();
    assert_eq!(session.max_event_id().unwrap(), report.max_event_id);
    assert_eq!(report.max_event_id, 7);
}
