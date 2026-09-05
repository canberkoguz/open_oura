//! The foreign-driven transport.
//!
//! `oura-link`'s [`Transport`] has two halves and only one of them can cross a
//! language boundary. `write` can: it is one call with a byte slice. `subscribe`
//! cannot: it hands back a `tokio::sync::broadcast::Receiver`, which has no
//! representation in any foreign language.
//!
//! So the flow is inverted. Rust keeps the broadcast channel and the foreign
//! side *pushes* frames in through [`ForeignTransport::on_notification`]:
//!
//! ```text
//! btleplug:  notification stream -> tx.send -> Receiver -> transact
//! foreign:   didUpdateValueFor   -> on_notification -> tx.send -> Receiver -> transact
//! ```
//!
//! What remains crossing is a single synchronous `write_frame`, and no async
//! construct crosses in either direction.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use oura_link::transport::Transport;
use oura_link::Error as LinkError;
use tokio::sync::broadcast;

use crate::error::FfiError;

/// Inbound frames buffered between the delegate thread and the drain loop. A
/// batch of history events arrives as a burst of notifications while the drain
/// task is briefly not at the receiver, so this needs headroom over the 255
/// events a single `GetEvent` can return.
const CHANNEL_CAPACITY: usize = 512;

const RUNNING: u8 = 0;
const DISCONNECTED: u8 = 1;
const CANCELLED: u8 = 2;

/// Writes one request frame to the ring's write characteristic.
///
/// Implemented on the foreign side. **Fire-and-forget**: the implementation
/// must not wait for a GATT write acknowledgement. Nothing in the protocol
/// correlates a response to a write ack — `transact` writes and then collects
/// notification frames until the link has been quiet for 1500 ms — so waiting
/// buys nothing and risks deadlocking against the very queue the notifications
/// arrive on.
///
/// Called from the thread inside `run_sync`, never from a foreign queue. An
/// implementation that must hop to a specific queue should dispatch
/// *asynchronously*; a synchronous hop into a queue that is itself waiting on
/// this sync will deadlock.
#[uniffi::export(with_foreign)]
pub trait RingWriter: Send + Sync {
    fn write_frame(&self, frame: Vec<u8>) -> Result<(), FfiError>;
}

/// Called after each fully-drained batch, so a UI can show progress.
///
/// Runs on the sync thread. It must not block and must not call back into the
/// session — `run_sync` holds the store lock for its whole duration, so a
/// re-entrant call would deadlock rather than return an error.
///
/// `bytes_left` is the ring's own count of the backlog still queued behind this
/// batch, and reaches zero on the batch that drains it. It is what a determinate
/// progress bar needs: `events_so_far` only ever climbs, and `cursor` is a
/// position on the ring's clock whose end is not known in advance. Note that the
/// first call already reports the backlog *after* one batch — there is no reading
/// to be had before the ring has answered once — so a bar scaled to it starts
/// just above zero rather than at it.
#[uniffi::export(with_foreign)]
pub trait SyncProgress: Send + Sync {
    fn batch_done(&self, events_so_far: u32, cursor: u32, bytes_left: u32);
}

struct Inner {
    writer: Arc<dyn RingWriter>,
    tx: broadcast::Sender<Vec<u8>>,
    stop: AtomicU8,
}

/// A [`Transport`] whose writes go out through a foreign implementation and
/// whose reads are fed in from one.
///
/// Cheap to clone: `OuraClient` takes ownership of its transport, and the
/// session needs a handle of its own to push frames into.
#[derive(Clone)]
pub(crate) struct ForeignTransport {
    inner: Arc<Inner>,
}

/// Why a sync stopped, when it stopped for a reason the link layer's own error
/// type cannot express.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    Running,
    Disconnected,
    Cancelled,
}

impl ForeignTransport {
    pub(crate) fn new(writer: Arc<dyn RingWriter>) -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(Inner {
                writer,
                tx,
                stop: AtomicU8::new(RUNNING),
            }),
        }
    }

    /// Feed one inbound notification frame. Never blocks: `broadcast::send` is
    /// synchronous and non-blocking, and drops the oldest frame rather than
    /// waiting when the buffer is full. Safe to call from a delegate queue.
    pub(crate) fn on_notification(&self, frame: Vec<u8>) {
        // An error here means nothing is subscribed -- no sync is running -- so
        // the frame is genuinely uninteresting rather than lost.
        let _ = self.inner.tx.send(frame);
    }

    pub(crate) fn set_stop(&self, stop: Stop) {
        let v = match stop {
            Stop::Running => RUNNING,
            Stop::Disconnected => DISCONNECTED,
            Stop::Cancelled => CANCELLED,
        };
        self.inner.stop.store(v, Ordering::SeqCst);
    }

    pub(crate) fn stop(&self) -> Stop {
        match self.inner.stop.load(Ordering::SeqCst) {
            DISCONNECTED => Stop::Disconnected,
            CANCELLED => Stop::Cancelled,
            _ => Stop::Running,
        }
    }
}

#[async_trait]
impl Transport for ForeignTransport {
    async fn write(&self, data: &[u8]) -> oura_link::Result<()> {
        // Stopping is observed here rather than through a cancellation hook in
        // `drain_events`. The next request after a cancel or a drop fails, `?`
        // propagates it out of `transact`, and the drain ends -- with every
        // event already stored and the cursor already advanced past the last
        // fully-drained batch. Latency is therefore at most one quiet window.
        match self.stop() {
            Stop::Cancelled => return Err(LinkError::Ble("cancelled".into())),
            Stop::Disconnected => return Err(LinkError::Ble("ring disconnected".into())),
            Stop::Running => {}
        }
        self.inner
            .writer
            .write_frame(data.to_vec())
            .map_err(|e| LinkError::Ble(e.to_string()))
    }

    fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.inner.tx.subscribe()
    }
}
