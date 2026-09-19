//! Transport abstraction over the ring's BLE link.
//!
//! The protocol is request/response with asynchronous notifications. [`Transport`]
//! captures just what the client needs — write a request, and subscribe to the
//! stream of inbound frames — so the higher layers can be exercised with a mock
//! in tests while [`crate::ble`] provides the real `btleplug` implementation.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::error::Result;

/// A bidirectional link to a ring.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Write a raw request frame to the ring's write characteristic.
    async fn write(&self, data: &[u8]) -> Result<()>;

    /// Subscribe to inbound notification frames (raw bytes, one per notification).
    fn subscribe(&self) -> broadcast::Receiver<Vec<u8>>;
}

/// Write `request` and collect notification frames until the link is quiet for
/// `quiet` (i.e. no new frame arrives within that window). This matches the
/// ring's behaviour of emitting one or more notifications per request with no
/// explicit terminator on most commands.
///
/// Prefer [`transact_until`] wherever the last frame of the reply can be
/// recognised: waiting out the window is the single largest cost in a sync
/// against a backlog, and this function always pays it in full.
pub async fn transact<T>(transport: &T, request: &[u8], quiet: Duration) -> Result<Vec<Vec<u8>>>
where
    T: Transport + ?Sized,
{
    transact_until(transport, request, quiet, |_| false).await
}

/// Write `request` and collect notification frames until `done` accepts one, or
/// until the link falls quiet for `quiet`.
///
/// The quiet window is a *fallback*, not the normal exit. Most requests have a
/// recognisable last frame — a known response tag, or for `GetEvent` the `0x11`
/// summary that closes the batch — and returning on it is worth a great deal.
/// Measured against a real ring, a 255-event batch transfers in about 0.6 s and
/// then costs another 1.5 s proving the ring has stopped talking; a night's
/// backlog is a hundred such batches, so two thirds of the first sync of the
/// morning was spent in this timeout.
///
/// `done` sees each frame as it arrives, and the frame it accepts is included in
/// the result. It is called on every inbound frame of a 255-event batch, so
/// callers keep it to reading a tag byte rather than parsing a packet.
///
/// A `done` that never fires, or one that is wrong about which frame comes last,
/// degrades to exactly the behaviour of [`transact`] — which is why a caller may
/// reasonably supply a terminator it has only observed rather than proven.
pub async fn transact_until<T, F>(
    transport: &T,
    request: &[u8],
    quiet: Duration,
    mut done: F,
) -> Result<Vec<Vec<u8>>>
where
    T: Transport + ?Sized,
    F: FnMut(&[u8]) -> bool,
{
    let mut rx = transport.subscribe();
    // Drop any backlog so we only observe responses to *this* request.
    while rx.try_recv().is_ok() {}

    transport.write(request).await?;

    let mut frames = Vec::new();
    loop {
        match tokio::time::timeout(quiet, rx.recv()).await {
            Ok(Ok(frame)) => {
                let last = done(&frame);
                frames.push(frame);
                if last {
                    break;
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
            // Channel closed or quiet window elapsed: we're done collecting.
            _ => break,
        }
    }
    Ok(frames)
}

#[cfg(test)]
pub(crate) mod mock {
    //! A scripted transport for unit tests: maps request hex prefixes to canned
    //! response frames.
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    pub struct MockTransport {
        tx: broadcast::Sender<Vec<u8>>,
        responses: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    }

    impl MockTransport {
        pub fn new() -> Self {
            let (tx, _) = broadcast::channel(64);
            Self {
                tx,
                responses: Mutex::new(HashMap::new()),
            }
        }

        /// Register canned responses keyed by the request's full hex.
        pub fn on(&self, request_hex: &str, responses: &[&str]) {
            self.responses.lock().unwrap().insert(
                request_hex.to_string(),
                responses.iter().map(|h| hex::decode(h).unwrap()).collect(),
            );
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn write(&self, data: &[u8]) -> Result<()> {
            let key = hex::encode(data);
            if let Some(frames) = self.responses.lock().unwrap().get(&key) {
                for f in frames {
                    let _ = self.tx.send(f.clone());
                }
            }
            Ok(())
        }

        fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
            self.tx.subscribe()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock::MockTransport;

    /// Virtual time, so these assert on the window being *skipped* rather than on
    /// how fast the test machine is. Under `start_paused` the clock only advances
    /// when the runtime has nothing to run, which is precisely when `transact` is
    /// sitting in its timeout.
    const QUIET: Duration = Duration::from_millis(1500);

    #[tokio::test(start_paused = true)]
    async fn stopping_on_the_last_frame_skips_the_quiet_window() {
        let mock = MockTransport::new();
        mock.on("10", &["4100", "4101", "1100"]);

        let started = tokio::time::Instant::now();
        let frames = transact_until(&mock, &[0x10], QUIET, |f| f.first() == Some(&0x11))
            .await
            .unwrap();

        assert_eq!(frames.len(), 3, "the terminator is part of the reply");
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_terminator_that_never_arrives_falls_back_to_the_window() {
        // The guarantee that lets a caller name a terminator it has observed on a
        // real ring but cannot prove: being wrong costs the old behaviour, not
        // the reply.
        let mock = MockTransport::new();
        mock.on("10", &["4100", "4101"]);

        let started = tokio::time::Instant::now();
        let frames = transact_until(&mock, &[0x10], QUIET, |f| f.first() == Some(&0x11))
            .await
            .unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(started.elapsed(), QUIET);
    }

    #[tokio::test(start_paused = true)]
    async fn transact_still_waits_out_the_window() {
        let mock = MockTransport::new();
        mock.on("10", &["1100"]);

        let started = tokio::time::Instant::now();
        let frames = transact(&mock, &[0x10], QUIET).await.unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(started.elapsed(), QUIET);
    }
}
