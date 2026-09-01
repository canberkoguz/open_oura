//! [`RingSession`] — the one object the foreign side holds for a whole sync.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use oura_link::OuraClient;
use oura_store::Store;

use crate::error::FfiError;
use crate::transport::{ForeignTransport, SyncProgress, RingWriter, Stop};
use crate::upload;

/// What one sync accomplished.
#[derive(Debug, uniffi::Record)]
pub struct SyncReport {
    /// The ring's serial, or `"unknown"` if it would not tell us.
    pub serial: String,
    pub firmware: Option<String>,
    pub battery_percent: Option<u8>,
    /// Events the ring sent, including ones already stored.
    pub events_received: u32,
    /// Of those, how many were new. The difference is normal: re-syncing
    /// overlapping ranges is idempotent by design.
    pub rows_inserted: u32,
    /// The cursor now persisted for this serial.
    pub next_cursor: u32,
    /// The highest event id in the store, for the uploader's high-water mark.
    pub max_event_id: i64,
}

/// A ring conversation: one database, one foreign writer, one sync at a time.
///
/// ## Threading contract
///
/// This is the part most easily got wrong, so it is stated rather than implied:
///
/// - [`run_sync`](RingSession::run_sync) **blocks for the whole sync** — minutes
///   against a backlog. It must run on a background thread. Calling it on the
///   thread that delivers notifications guarantees a deadlock: no frame could
///   ever arrive, so every request would burn its full quiet window and the
///   drain would never see an event.
/// - [`on_notification`](RingSession::on_notification),
///   [`on_connected`](RingSession::on_connected),
///   [`on_disconnected`](RingSession::on_disconnected) and
///   [`cancel`](RingSession::cancel) never block and are safe on a delegate queue.
/// - [`RingWriter::write_frame`] is called from inside `run_sync`, on that
///   background thread — not from any foreign queue.
#[derive(uniffi::Object)]
pub struct RingSession {
    /// A current-thread runtime with only the time driver. `transact` needs a
    /// timer for its quiet window; nothing here needs IO, because all the IO
    /// happens on the foreign side. It is verified in this crate's tests that
    /// such a runtime wakes from its park on a cross-thread `broadcast::send`,
    /// which is the property the whole bridge rests on.
    rt: tokio::runtime::Runtime,
    client: OuraClient<ForeignTransport>,
    transport: ForeignTransport,
    /// `rusqlite::Connection` is `Send` but not `Sync`, and a UniFFI object
    /// must be both.
    store: Mutex<Store>,
    syncing: AtomicBool,
}

/// Clears the in-progress flag on every exit path, including a panic.
struct SyncGuard<'a>(&'a AtomicBool);

impl<'a> SyncGuard<'a> {
    fn acquire(flag: &'a AtomicBool) -> Result<Self, FfiError> {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| SyncGuard(flag))
            .map_err(|_| FfiError::Busy)
    }
}

impl Drop for SyncGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[uniffi::export]
impl RingSession {
    /// Open the database and bind this session to a writer.
    ///
    /// `db_path` should be inside the app's container. Set the file's data
    /// protection class from the foreign side if a sync may run while the
    /// device is locked — with the default class the file cannot even be
    /// opened then, and Rust has no way to set it.
    #[uniffi::constructor]
    pub fn new(writer: Arc<dyn RingWriter>, db_path: String) -> Result<Arc<Self>, FfiError> {
        Self::open(writer, &db_path, oura_link::client::DEFAULT_QUIET)
    }
}

impl RingSession {
    /// The real constructor. `quiet` is the per-request window `transact` waits
    /// for the link to fall silent; tests shorten it so a scripted ring does not
    /// cost 1500 ms per request.
    pub(crate) fn open(
        writer: Arc<dyn RingWriter>,
        db_path: &str,
        quiet: std::time::Duration,
    ) -> Result<Arc<Self>, FfiError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| FfiError::Storage {
                message: format!("starting the runtime: {e}"),
            })?;
        let transport = ForeignTransport::new(writer);
        let store = Store::open(db_path)?;
        Ok(Arc::new(Self {
            rt,
            client: OuraClient::new(transport.clone()).with_quiet(quiet),
            transport,
            store: Mutex::new(store),
            syncing: AtomicBool::new(false),
        }))
    }

}

#[uniffi::export]
impl RingSession {
    /// Feed one inbound notification frame from the ring's notify
    /// characteristic. Non-blocking; safe on a delegate queue.
    pub fn on_notification(&self, frame: Vec<u8>) {
        self.transport.on_notification(frame);
    }

    /// Report that the link is up. Not required before the first `run_sync`
    /// (which resets the state itself), but harmless and clearer.
    pub fn on_connected(&self) {
        self.transport.set_stop(Stop::Running);
    }

    /// Report that the link dropped. The in-flight sync ends at its next write
    /// with [`FfiError::Disconnected`]; stored events and the cursor survive.
    pub fn on_disconnected(&self) {
        self.transport.set_stop(Stop::Disconnected);
    }

    /// Ask the running sync to stop. It ends at its next write — within one
    /// quiet window — with [`FfiError::Cancelled`], keeping everything drained
    /// so far.
    pub fn cancel(&self) {
        self.transport.set_stop(Stop::Cancelled);
    }

    /// Authenticate and drain history events into the store. **Blocking.**
    ///
    /// `auth_key` is the ring's 16-byte key; this crate never stores it and
    /// never sees it outside this call. `sync_time` writes the host clock to
    /// the ring — the phone's clock is network-synced and the Android app does
    /// this on every connect, but it is a state change, so it is the caller's
    /// choice rather than a hidden effect.
    ///
    /// The ordering here is the one the Android app uses
    /// (`docs/sync-orchestration.md`) and is load-bearing, which is why this is
    /// one call rather than nine: the ordering belongs to the crate that knows
    /// the protocol, not to whoever is holding the Xcode project.
    pub fn run_sync(
        &self,
        auth_key: Vec<u8>,
        sync_time: bool,
        progress: Option<Arc<dyn SyncProgress>>,
    ) -> Result<SyncReport, FfiError> {
        let got = auth_key.len();
        let key: [u8; 16] = auth_key
            .try_into()
            .map_err(|_| FfiError::BadKeyLength { got: got as u32 })?;

        let _guard = SyncGuard::acquire(&self.syncing)?;
        // The caller only reaches here with a live, discovered link, so a stale
        // stop flag from a previous connection must not veto this sync.
        self.transport.set_stop(Stop::Running);

        let store = self.store.lock().map_err(|_| FfiError::Storage {
            message: "the store lock was poisoned by an earlier panic".into(),
        })?;

        let outcome = self.rt.block_on(self.drain(&store, &key, sync_time, progress));

        // A stop reason outranks the link error it produced: `write` can only
        // report "ble error", and the caller wants to know whether to retry.
        outcome.map_err(|e| match self.transport.stop() {
            Stop::Cancelled => FfiError::Cancelled,
            Stop::Disconnected => FfiError::Disconnected,
            Stop::Running => e,
        })
    }

    /// Events with `id` greater than `after_id`, oldest first, at most `limit`,
    /// as a JSON array ready to be an HTTP body.
    ///
    /// JSON rather than a record array on purpose: marshalling ten thousand
    /// rows through generated per-field converters only to re-encode them as
    /// JSON is work for nothing. Blocks until any running sync finishes.
    pub fn events_since(&self, after_id: i64, limit: u32) -> Result<Vec<u8>, FfiError> {
        let store = self.store.lock().map_err(|_| FfiError::Storage {
            message: "the store lock was poisoned by an earlier panic".into(),
        })?;
        upload::encode(&store.events_since(after_id, limit)?)
    }

    /// The highest event id in the store — what an uploader compares its
    /// high-water mark against to know whether anything is pending.
    pub fn max_event_id(&self) -> Result<i64, FfiError> {
        let store = self.store.lock().map_err(|_| FfiError::Storage {
            message: "the store lock was poisoned by an earlier panic".into(),
        })?;
        Ok(store.max_event_id()?)
    }
}

impl RingSession {
    /// The handshake and drain, in the app's documented order.
    async fn drain(
        &self,
        store: &Store,
        key: &[u8; 16],
        sync_time: bool,
        progress: Option<Arc<dyn SyncProgress>>,
    ) -> Result<SyncReport, FfiError> {
        // 1-2. AUTHENTICATE. `oura-link` exposes one auth primitive -- the
        //      nonce/AES challenge -- which covers both the app's AUTHENTICATE
        //      and APP_LEVEL_AUTHENTICATE states.
        self.client.authenticate(key).await?;

        // 3. GET_CAPABILITIES. We always take the legacy event path, so the
        //    answer is not branched on; the read stays because the documented
        //    order puts it inside the handshake and it is cheap. A ring that
        //    declines to answer is not a reason to abandon the sync.
        let _ = self.client.capabilities().await;

        // 5. SYNC_TIMESTAMPS.
        if sync_time {
            self.client.sync_time().await?;
        }

        // 6. ENABLE_NOTIFICATION is deliberately skipped. `SetNotification`'s
        //    flag byte has no recorded semantics anywhere in this repo, and the
        //    CLI's working sync omits the step entirely. Writing a guessed byte
        //    to the ring is a state change with unknown effect; leaving it out
        //    matches the only path we have ever seen return events.

        // 7. Metadata, while the link is authenticated.
        let serial = self
            .client
            .serial()
            .await
            .unwrap_or_else(|_| "unknown".into());
        let info = self.client.firmware().await.ok();
        let hardware_id = self.client.hardware_id().await.ok();
        store.upsert_device(&serial, hardware_id.as_deref(), info.as_ref())?;

        let battery = self.client.battery().await.ok();
        if let Some(b) = &battery {
            let _ = store.insert_battery(&serial, b);
        }

        // 9. SYNC_EVENTS.
        let cursor = store.cursor(&serial)?;
        let inserted = Cell::new(0u32);
        let seen = Cell::new(0u32);
        // A failed insert must stop the cursor advancing past events we did not
        // store -- otherwise they are dropped permanently on the next sync.
        let db_err: RefCell<Option<oura_store::Error>> = RefCell::new(None);

        let outcome = self
            .client
            .drain_events(
                cursor,
                |ev| {
                    if db_err.borrow().is_some() {
                        return;
                    }
                    seen.set(seen.get() + 1);
                    match store.insert_event(&serial, ev) {
                        Ok(true) => inserted.set(inserted.get() + 1),
                        Ok(false) => {}
                        Err(e) => *db_err.borrow_mut() = Some(e),
                    }
                },
                |c| {
                    if db_err.borrow().is_some() {
                        return;
                    }
                    if let Err(e) = store.set_cursor(&serial, c) {
                        *db_err.borrow_mut() = Some(e);
                        return;
                    }
                    if let Some(p) = &progress {
                        // Events pulled, not rows added: a resumed sync
                        // re-reads an overlapping range and would otherwise
                        // look stalled while it worked through it.
                        p.batch_done(seen.get(), c);
                    }
                },
            )
            .await?;

        if let Some(e) = db_err.into_inner() {
            return Err(e.into());
        }
        store.set_cursor(&serial, outcome.next_cursor)?;

        // A drop or a cancel does not always surface as a write error. The stop
        // flag is only checked on the *next* write, and `drain_events` ends its
        // loop as soon as a batch comes back empty -- which is exactly what a
        // silent ring produces. Without this check that reads as a clean finish,
        // and the caller would report a drained backlog that is still there.
        match self.transport.stop() {
            Stop::Cancelled => return Err(FfiError::Cancelled),
            Stop::Disconnected => return Err(FfiError::Disconnected),
            Stop::Running => {}
        }

        Ok(SyncReport {
            serial,
            firmware: info.map(|i| i.firmware_version),
            battery_percent: battery.map(|b| b.percent),
            events_received: outcome.events_synced,
            rows_inserted: inserted.get(),
            next_cursor: outcome.next_cursor,
            max_event_id: store.max_event_id()?,
        })
    }
}
