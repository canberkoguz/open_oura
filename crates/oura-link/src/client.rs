//! [`OuraClient`] — the high-level, transport-generic API.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oura_protocol::auth::{encrypt_nonce, AuthResult};
use oura_protocol::device::{self, Battery, Capability, DeviceInfo};
use crate::error::{Error, Result};
use oura_protocol::events::{EventBatchSummary, RingEvent};
use oura_protocol::protocol::{self, feature, feature_mode, Packet};
use crate::transport::{transact, transact_until, Transport};

/// Default quiet window for collecting responses to a request.
pub const DEFAULT_QUIET: Duration = Duration::from_millis(1500);

/// One live heart-rate sample derived from an IBI subscription notification.
#[derive(Clone, Copy, Debug)]
pub struct HeartRateSample {
    pub bpm: u16,
    pub ibi_ms: u16,
}

/// One live accelerometer sample (signed raw counts) from the ACM stream.
#[derive(Clone, Copy, Debug)]
pub struct AcmSample {
    pub x: i16,
    pub y: i16,
    pub z: i16,
}

impl AcmSample {
    /// Vector magnitude, useful for motion/wave detection.
    pub fn magnitude(&self) -> f64 {
        ((self.x as f64).powi(2) + (self.y as f64).powi(2) + (self.z as f64).powi(2)).sqrt()
    }

    /// Parse an ACM measurement-indication frame (tag `0x33`) into its samples.
    pub fn parse_frame(frame: &[u8]) -> Vec<AcmSample> {
        parse_acm_frame(frame)
    }
}

/// Latest cached feature values read on demand (not a live stream).
#[derive(Clone, Copy, Debug, Default)]
pub struct LatestValues {
    /// Heart rate in bpm, if the feature reported one.
    pub bpm: Option<u16>,
    /// Blood-oxygen saturation in percent (SpO2 feature only).
    pub spo2_percent: Option<u8>,
}

/// Outcome of an event-drain sync.
#[derive(Clone, Copy, Debug)]
pub struct SyncOutcome {
    pub events_synced: u32,
    pub next_cursor: u32,
}

/// A feature's reported status (`0x2f` ext `0x21`): mode/status/state/subscription.
#[derive(Clone, Copy, Debug)]
pub struct FeatureStatus {
    pub feature: u8,
    pub mode: u8,
    pub status: u8,
    pub state: u8,
    pub subscription: u8,
}

impl FeatureStatus {
    fn parse(p: &Packet) -> Option<FeatureStatus> {
        if p.ext_tag() != Some(0x21) || p.payload.len() < 6 {
            return None;
        }
        Some(FeatureStatus {
            feature: p.payload[1],
            mode: p.payload[2],
            status: p.payload[3],
            state: p.payload[4],
            subscription: p.payload[5],
        })
    }
}

/// High-level client over any [`Transport`].
pub struct OuraClient<T: Transport> {
    transport: T,
    quiet: Duration,
}

impl<T: Transport> OuraClient<T> {
    /// Wrap a transport with the default response window.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            quiet: DEFAULT_QUIET,
        }
    }

    /// Override the per-request quiet window.
    pub fn with_quiet(mut self, quiet: Duration) -> Self {
        self.quiet = quiet;
        self
    }

    /// Borrow the underlying transport (e.g. to disconnect a BLE link).
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Write a request and collect its whole reply, ending only when the link
    /// falls quiet.
    ///
    /// For requests whose last frame is recognisable, use [`Self::request_until`]
    /// instead: this one always pays the quiet window in full.
    async fn request(&self, bytes: &[u8]) -> Result<Vec<Packet>> {
        let frames = transact(&self.transport, bytes, self.quiet).await?;
        Ok(frames.iter().filter_map(|f| Packet::parse(f)).collect())
    }

    /// Write a request and stop collecting at the frame `done` accepts.
    ///
    /// Every caller below reads exactly one field out of one frame of the reply,
    /// which is what makes stopping at that frame lossless rather than a
    /// trade: there is nothing later in the reply that the caller would have
    /// looked at. The drain is the one exception and says why at its call site.
    ///
    /// `done` takes a raw frame rather than a [`Packet`]. It runs once per
    /// inbound frame — 255 of them in an event batch — and reading two bytes off
    /// the wire is cheaper than parsing a packet that is then parsed again here.
    async fn request_until<F>(&self, bytes: &[u8], done: F) -> Result<Vec<Packet>>
    where
        F: FnMut(&[u8]) -> bool,
    {
        let frames = transact_until(&self.transport, bytes, self.quiet, done).await?;
        Ok(frames.iter().filter_map(|f| Packet::parse(f)).collect())
    }

    fn find(packets: &[Packet], tag: u8) -> Option<&Packet> {
        packets.iter().find(|p| p.tag == tag)
    }

    /// The error for a reply that lacks the frame we wanted: silence is a link
    /// failure, anything else is the ring saying something we did not expect.
    fn missing(packets: &[Packet], request: &str) -> Error {
        if packets.is_empty() {
            Error::NoResponse(format!("{request} request"))
        } else {
            Error::Protocol(format!("unexpected reply to the {request} request"))
        }
    }

    // --- device info -------------------------------------------------------

    /// Read firmware/version metadata (no auth required).
    pub async fn firmware(&self) -> Result<DeviceInfo> {
        let packets = self
            .request_until(&protocol::req_firmware(), |f| tag_is(f, 0x09))
            .await?;
        Self::find(&packets, 0x09)
            .and_then(DeviceInfo::parse)
            .ok_or_else(|| Error::Protocol("no firmware response".into()))
    }

    /// Read battery state (requires app-auth on rings with a key installed).
    pub async fn battery(&self) -> Result<Battery> {
        let packets = self
            .request_until(&protocol::req_battery(), |f| tag_is(f, 0x0d))
            .await?;
        Self::find(&packets, 0x0d)
            .and_then(Battery::parse)
            .ok_or_else(|| Error::Protocol("no battery response (auth required?)".into()))
    }

    /// Read the ring serial number.
    pub async fn serial(&self) -> Result<String> {
        let packets = self
            .request_until(&protocol::product::SERIAL, |f| tag_is(f, 0x19))
            .await?;
        Self::find(&packets, 0x19)
            .and_then(device::parse_product_ascii)
            .ok_or_else(|| Error::Protocol("no serial response".into()))
    }

    /// Read the hardware id (e.g. `BLB_03`).
    pub async fn hardware_id(&self) -> Result<String> {
        let packets = self
            .request_until(&protocol::product::HARDWARE, |f| tag_is(f, 0x19))
            .await?;
        Self::find(&packets, 0x19)
            .and_then(device::parse_product_ascii)
            .ok_or_else(|| Error::Protocol("no hardware response".into()))
    }

    /// Read both capability pages.
    pub async fn capabilities(&self) -> Result<Vec<Capability>> {
        let mut caps = Vec::new();
        for page in 0u8..2 {
            let packets = self
                .request_until(&protocol::req_capabilities(page), |f| ext_tag_is(f, 0x02))
                .await?;
            if let Some(p) = packets.iter().find(|p| p.ext_tag() == Some(0x02)) {
                caps.extend(device::parse_capabilities(p));
            }
        }
        Ok(caps)
    }

    // --- auth & session ----------------------------------------------------

    /// Run the app-auth challenge with a 16-byte key. Must be repeated per
    /// connection on rings that have a key installed.
    ///
    /// Only a received, non-success `0x2e` state is [`Error::Auth`]. A silent
    /// ring is [`Error::NoResponse`] and an unexpected reply is
    /// [`Error::Protocol`]: neither says anything about the key, and callers
    /// treat `Auth` as "retrying will never help".
    pub async fn authenticate(&self, key: &[u8; 16]) -> Result<AuthResult> {
        let packets = self
            .request_until(&protocol::req_auth_nonce(), |f| ext_tag_is(f, 0x2c))
            .await?;
        let nonce = packets
            .iter()
            .find(|p| p.ext_tag() == Some(0x2c))
            .map(|p| p.payload[1..].to_vec())
            .ok_or_else(|| Self::missing(&packets, "nonce"))?;

        let encrypted = encrypt_nonce(key, &nonce);
        let packets = self
            .request_until(&protocol::req_authenticate(&encrypted), |f| {
                ext_tag_is(f, 0x2e)
            })
            .await?;
        let state = packets
            .iter()
            .find(|p| p.ext_tag() == Some(0x2e))
            .and_then(|p| p.payload.get(1).copied())
            .ok_or_else(|| Self::missing(&packets, "authenticate"))?;

        let result = AuthResult::from(state);
        if result.is_success() {
            Ok(result)
        } else {
            Err(Error::Auth(format!("{result:?}")))
        }
    }

    /// Install a new 16-byte auth key. Only valid on a factory-reset ring.
    pub async fn set_auth_key(&self, key: &[u8; 16]) -> Result<()> {
        let packets = self.request(&protocol::req_set_auth_key(key)).await?;
        match Self::find(&packets, 0x25).and_then(|p| p.payload.first().copied()) {
            Some(0x00) => Ok(()),
            Some(other) => Err(Error::Auth(format!("set_auth_key status {other:#04x}"))),
            None => Err(Error::Protocol("no set_auth_key response".into())),
        }
    }

    /// Align the ring clock to host UTC.
    ///
    /// The terminator here is inferred rather than captured. The protocol
    /// cheatsheet in `docs/` records only that `0x12` gets a "success-shaped
    /// response", without its tag; every other request/response pair we *have*
    /// captured is consecutive (`0x08`/`0x09`, `0x0c`/`0x0d`, `0x10`/`0x11`,
    /// `0x18`/`0x19`, `0x24`/`0x25`, `0x28`/`0x29`), which puts this one at
    /// `0x13`. Guessing is free: the reply is discarded either way, and a wrong
    /// tag just means waiting out the quiet window as before.
    pub async fn sync_time(&self) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.request_until(&protocol::req_sync_time(now, 0), |f| tag_is(f, 0x13))
            .await?;
        Ok(())
    }

    /// Enable the async notification flags so the ring pushes events.
    pub async fn set_notification(&self, flags: u8) -> Result<()> {
        self.request(&protocol::req_set_notification(flags)).await?;
        Ok(())
    }

    // --- history events ----------------------------------------------------

    /// Drain history events starting from `cursor` (deciseconds), invoking
    /// `on_event` for each. Loops until the ring reports no bytes left. Returns
    /// the count synced and the next cursor to persist for incremental sync.
    ///
    /// `on_batch` is called with the advanced cursor and the ring's own
    /// `bytes_left` after every fully-processed batch, so callers can persist the
    /// cursor incrementally — otherwise an interrupted sync (timeout / BLE drop)
    /// inserts events but loses the cursor advance, forcing the next sync to
    /// re-pull from the old cursor (and never reach new events if a batch cap is
    /// hit first).
    ///
    /// `bytes_left` is the backlog the ring says it still holds. It is the only
    /// number in the whole drain that is a *total* rather than a running count,
    /// which makes it the one thing a determinate progress bar can be built on;
    /// the cursor cannot, being a position on a clock with no known end.
    pub async fn drain_events<F, G>(
        &self,
        cursor: u32,
        mut on_event: F,
        mut on_batch: G,
    ) -> Result<SyncOutcome>
    where
        F: FnMut(&RingEvent),
        G: FnMut(u32, u32),
    {
        let mut start = cursor;
        let mut total = 0u32;
        // Safety bound against a misbehaving ring that never reports drained.
        for _ in 0..100_000 {
            // The batch's events arrive and are then closed by the `0x11`
            // summary that counts them (the protocol cheatsheet in `docs/`,
            // "Captured Events"). Stopping there instead of on silence is
            // the single biggest thing in a sync: measured against a real ring a
            // 255-event batch transfers in ~0.6 s and then spent ~1.5 s waiting
            // out the window, so a night's hundred batches lost ~150 s to it.
            //
            // The `seen_event` guard is what makes an *observed* terminator safe
            // to act on. Were the ordering ever the other way round, stopping at
            // the summary would leave the batch's events unread — and because
            // `progressed` would then be false while `bytes_left` was not, the
            // drain would stop with the backlog still there. Requiring an event
            // first means a ring that summarises up front simply never triggers
            // the early exit and falls back to the quiet window. The cost is one
            // window on a batch that is genuinely empty, which is the sync that
            // had nothing to do anyway.
            //
            // Only an event at or after `start` arms it, because only such an
            // event can be an answer to *this* request. Returning early is what
            // makes that distinction matter: the reply now ends before the link
            // is known to be quiet, so a frame the ring emits after its summary
            // lands in the next batch's window instead, and nothing below the
            // transport correlates a frame to the request it answers. Such a
            // stray is always older than the cursor it arrives behind — that is
            // what makes it recognisable — and arming on one would let the next
            // summary cut the batch short before its real events.
            let mut seen_event = false;
            let packets = self
                .request_until(&protocol::req_get_event(start, 255, -1), |f| {
                    match f.first() {
                        Some(&0x11) => seen_event,
                        Some(&t) if t >= protocol::HISTORY_EVENT_PREFIX => {
                            if in_range(f, start) {
                                seen_event = true;
                            }
                            false
                        }
                        _ => false,
                    }
                })
                .await?;

            let mut summary: Option<EventBatchSummary> = None;
            let mut max_ts = start;
            let mut batch_events = 0u32;
            for p in &packets {
                if p.tag == 0x11 {
                    summary = EventBatchSummary::parse(p);
                } else if p.tag >= protocol::HISTORY_EVENT_PREFIX {
                    let ev = RingEvent::from_packet(p);
                    // Same range test as the terminator above, and for the same
                    // reason: counting a stray would inflate `events_synced` and
                    // hand the caller an event it has already stored.
                    if ev.timestamp < start {
                        continue;
                    }
                    max_ts = max_ts.max(ev.timestamp);
                    batch_events += 1;
                    total += 1;
                    on_event(&ev);
                }
            }

            let bytes_left = summary.map(|s| s.bytes_left).unwrap_or(0);
            // Advance the cursor past the newest event seen.
            let next = max_ts.saturating_add(1);
            let progressed = batch_events > 0 && next > start;
            if progressed {
                start = next;
                // Persist incrementally (this batch is fully drained), and hand
                // over what the ring says is still queued behind it.
                on_batch(start, bytes_left);
            }
            // Stop when drained, or when we can make no further progress.
            if bytes_left == 0 || !progressed {
                break;
            }
        }
        Ok(SyncOutcome {
            events_synced: total,
            next_cursor: start,
        })
    }

    // --- live / latest -----------------------------------------------------

    /// Read a feature's latest cached values (HR / SpO2). Reflects the last
    /// automatic measurement; meaningful only when the ring is worn.
    pub async fn feature_latest(&self, feature_id: u8) -> Result<LatestValues> {
        let packets = self.request(&protocol::req_feature_latest(feature_id)).await?;
        let p = packets
            .iter()
            .find(|p| p.ext_tag() == Some(0x25))
            .ok_or_else(|| Error::Protocol("no feature-latest response".into()))?;
        // payload: [0]=0x25,[1]=feature,[2]=result,[3]=status,[4]=state,
        //          [5..7]=counter, [7..]=feature-specific data.
        let data = p.payload.get(7..).unwrap_or(&[]);
        let mut out = LatestValues::default();
        match feature_id {
            feature::DAYTIME_HR => {
                // data[0..2] = rr-corrected IBI (ms); bpm = 60000 / ibi.
                if data.len() >= 2 {
                    let ibi = u16::from_le_bytes([data[0], data[1]]);
                    out.bpm = bpm_from_ibi(ibi);
                }
            }
            feature::EXERCISE_HR => {
                // data[4] = last HR value (bpm).
                if let Some(&bpm) = data.get(4) {
                    if bpm > 0 {
                        out.bpm = Some(bpm as u16);
                    }
                }
            }
            feature::SPO2 => {
                // data[3] = SpO2 %, data[4] = HR bpm.
                if let Some(&spo2) = data.get(3) {
                    if spo2 > 0 {
                        out.spo2_percent = Some(spo2);
                    }
                }
                if let Some(&bpm) = data.get(4) {
                    if bpm > 0 {
                        out.bpm = Some(bpm as u16);
                    }
                }
            }
            _ => {}
        }
        Ok(out)
    }

    /// Trigger the ring's sleep analysis. Returns the `0x29` status byte.
    pub async fn check_sleep_analysis(&self, force: bool) -> Result<u8> {
        let packets = self
            .request_until(&protocol::req_check_sleep_analysis(force), |f| {
                tag_is(f, 0x29)
            })
            .await?;
        Self::find(&packets, 0x29)
            .and_then(|p| p.payload.first().copied())
            .ok_or_else(|| Error::Protocol("no sleep-analysis response".into()))
    }

    /// Read a feature's status (mode/state/subscription).
    pub async fn feature_status(&self, feature_id: u8) -> Result<FeatureStatus> {
        let packets = self.request(&protocol::req_feature_status(feature_id)).await?;
        packets
            .iter()
            .find_map(FeatureStatus::parse)
            .ok_or_else(|| Error::Protocol("no feature-status response".into()))
    }

    /// Set a feature's mode (e.g. `feature_mode::AUTOMATIC` to enable measurement).
    pub async fn set_feature_mode(&self, feature_id: u8, mode: u8) -> Result<()> {
        let packets = self
            .request(&protocol::req_set_feature_mode(feature_id, mode))
            .await?;
        match packets
            .iter()
            .find(|p| p.ext_tag() == Some(0x23))
            .and_then(|p| p.payload.get(2).copied())
        {
            Some(0x00) => Ok(()),
            Some(other) => Err(Error::Protocol(format!("set_feature_mode result {other:#04x}"))),
            None => Err(Error::Protocol("no set_feature_mode response".into())),
        }
    }

    /// Subscribe/unsubscribe a feature capability (e.g. real steps, Atlas/bioZ) via
    /// `SetFeatureSubscription`. Returns the ring's raw result byte (0 = success;
    /// non-zero = rejected, e.g. feature disabled in firmware) so the caller can see
    /// exactly how the ring responds.
    pub async fn set_feature_subscription(&self, capability: u8, mode: u8) -> Result<u8> {
        let packets = self
            .request(&protocol::req_set_feature_subscription(capability, mode))
            .await?;
        packets
            .iter()
            .find(|p| p.ext_tag() == Some(0x27))
            .and_then(|p| p.payload.get(2).copied())
            .ok_or_else(|| Error::Protocol("no set_feature_subscription response".into()))
    }

    /// Query the RData collection state (read-only). Returns `(subtag, status)`.
    pub async fn rdata_state(&self) -> Result<(u8, u8)> {
        let packets = self.request(&protocol::req_rdata_state()).await?;
        Self::find(&packets, 0x03)
            .and_then(|p| Some((*p.payload.first()?, *p.payload.get(1)?)))
            .ok_or_else(|| Error::Protocol("no RData state response (auth required?)".into()))
    }

    /// Stop an active RData collection session (part of mandatory teardown).
    /// Returns the response status byte (255 if absent).
    pub async fn rdata_stop(&self) -> Result<u8> {
        let packets = self.request(&protocol::req_rdata_stop()).await?;
        Ok(Self::find(&packets, 0x03).and_then(|p| p.payload.get(1).copied()).unwrap_or(255))
    }

    /// Clear the RData session/data from the ring's flash (part of teardown).
    /// Returns the response status byte (255 if absent).
    pub async fn rdata_clear(&self) -> Result<u8> {
        let packets = self.request(&protocol::req_rdata_clear()).await?;
        Ok(Self::find(&packets, 0x03).and_then(|p| p.payload.get(1).copied()).unwrap_or(255))
    }

    /// Configure/arm an RData session for one or more signal types. **This starts
    /// persistent flash sampling that does NOT self-stop** — the caller is
    /// responsible for the `stop`+`clear` teardown. Returns `(subtag, status)`.
    pub async fn rdata_configure(
        &self,
        types: &[protocol::rdata::DataType],
        start_unix: u32,
        current_unix: u32,
    ) -> Result<(u8, u8)> {
        let packets = self
            .request(&protocol::req_rdata_configure(types, start_unix, current_unix))
            .await?;
        Self::find(&packets, 0x03)
            .and_then(|p| Some((*p.payload.first()?, *p.payload.get(1)?)))
            .ok_or_else(|| Error::Protocol("no RData configure response".into()))
    }

    /// Fetch one RData page by index. Returns `(status, page_bytes)` where
    /// `status` is the subtag-status byte (`6` = NO_DATA / past the end) and
    /// `page_bytes` is the payload after the `[subtag, status]` header.
    pub async fn rdata_get_page(&self, page: u16) -> Result<(u8, Vec<u8>)> {
        let packets = self.request(&protocol::req_rdata_get_page(page)).await?;
        Self::find(&packets, 0x03)
            .map(|p| {
                let status = p.payload.get(1).copied().unwrap_or(0);
                let bytes = p.payload.get(2..).unwrap_or(&[]).to_vec();
                (status, bytes)
            })
            .ok_or_else(|| Error::Protocol("no RData page response".into()))
    }

    /// Enable live heart rate (daytime HR, `CONNECTED_LIVE`) and invoke `on_sample`
    /// for each valid beat for up to `duration`. Restores `AUTOMATIC` mode on exit.
    /// The ring must be worn for samples to appear.
    pub async fn live_heart_rate<F>(
        &self,
        duration: Duration,
        debug: bool,
        mut on_sample: F,
    ) -> Result<()>
    where
        F: FnMut(HeartRateSample),
    {
        let mut rx = self.transport.subscribe();
        // Drain backlog.
        while rx.try_recv().is_ok() {}

        self.transport
            .write(&protocol::req_set_feature_mode(
                feature::DAYTIME_HR,
                feature_mode::CONNECTED_LIVE,
            ))
            .await?;

        let deadline = tokio::time::Instant::now() + duration;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(frame)) => {
                    if debug {
                        eprintln!("raw notify: {}", hex::encode(&frame));
                    }
                    if let Some(sample) = parse_live_hr_frame(&frame) {
                        on_sample(sample);
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                _ => break,
            }
        }

        // Best-effort restore to automatic mode.
        let _ = self
            .transport
            .write(&protocol::req_set_feature_mode(
                feature::DAYTIME_HR,
                feature_mode::AUTOMATIC,
            ))
            .await;
        Ok(())
    }

    /// Stream live accelerometer samples (the "wave to test motion" path): enable
    /// the ACM real-time measurement for `duration` and invoke `on_sample` for each
    /// x/y/z reading. The request is time-boxed (minutes) so the ring auto-stops,
    /// and we also send an explicit OFF on exit. The ring must be worn/moving.
    pub async fn stream_accelerometer<F>(&self, duration: Duration, mut on_sample: F) -> Result<()>
    where
        F: FnMut(AcmSample),
    {
        let mut rx = self.transport.subscribe();
        while rx.try_recv().is_ok() {}

        let minutes = (duration.as_secs().div_ceil(60)).max(1) as u16;
        self.transport
            .write(&protocol::req_set_realtime(
                protocol::realtime::ACM,
                minutes,
                0,
            ))
            .await?;

        let deadline = tokio::time::Instant::now() + duration;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(frame)) => {
                    for sample in parse_acm_frame(&frame) {
                        on_sample(sample);
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                _ => break,
            }
        }

        // Mandatory teardown: real-time measurements do not self-stop reliably.
        let _ = self.transport.write(&protocol::req_realtime_off()).await;
        Ok(())
    }
}

/// Does this frame carry response tag `tag`?
///
/// Reads the wire bytes directly — see [`OuraClient::request_until`] for why
/// these are not expressed over [`Packet`]. The framing is `[tag, len,
/// payload..]`, and for extended ops the first payload byte is the op tag.
fn tag_is(frame: &[u8], tag: u8) -> bool {
    frame.first() == Some(&tag)
}

/// Does this frame carry extended (`0x2f`) response op `ext`?
fn ext_tag_is(frame: &[u8], ext: u8) -> bool {
    frame.first() == Some(&0x2f) && frame.get(2) == Some(&ext)
}

/// Is this history-event frame an answer to a `GetEvent` from `start`?
///
/// An event frame is `[tag, len, timestamp(4, LE), body..]`, so this is four
/// bytes off the wire — cheap enough to run on all 255 frames of a batch, which
/// is why the drain's terminator is expressed over frames and not [`Packet`]s.
/// A frame too short to hold a timestamp is not an event we can place, so it is
/// not treated as one.
fn in_range(frame: &[u8], start: u32) -> bool {
    match frame.get(2..6) {
        Some(ts) => u32::from_le_bytes([ts[0], ts[1], ts[2], ts[3]]) >= start,
        None => false,
    }
}

/// Parse an ACM measurement indication (response tag `0x33`) into up to 2 samples.
///
/// Frame: `[0]=0x33 [1]=len [2]=sampleRate [3]=seq [4..10]=x,y,z [10..16]=x,y,z?`,
/// each axis a signed `i16` little-endian.
fn parse_acm_frame(frame: &[u8]) -> Vec<AcmSample> {
    let mut out = Vec::new();
    if frame.len() < 10 || frame[0] != protocol::realtime::ACM_RESPONSE_TAG {
        return out;
    }
    let s = |o: usize| i16::from_le_bytes([frame[o], frame[o + 1]]);
    out.push(AcmSample {
        x: s(4),
        y: s(6),
        z: s(8),
    });
    if frame.len() >= 16 {
        out.push(AcmSample {
            x: s(10),
            y: s(12),
            z: s(14),
        });
    }
    out
}

/// Compute bpm from an inter-beat interval, ignoring implausible values.
fn bpm_from_ibi(ibi_ms: u16) -> Option<u16> {
    if (300..=2000).contains(&ibi_ms) {
        Some((60_000u32 / ibi_ms as u32) as u16)
    } else {
        None
    }
}

/// Parse a daytime-HR live subscription notification (tag `0x2f`, sub-tag `0x28`).
///
/// Frame layout: `[0]=0x2f [1]=len [2]=0x28(IND1) [3]=cap [4]=status [5]=state
/// [6..8]=timeSince [8..10]=IBI`. The IBI word packs a 12-bit interval (ms) and a
/// 4-bit validity nibble (1 = VALID), per the app's `IBI` decoder.
fn parse_live_hr_frame(frame: &[u8]) -> Option<HeartRateSample> {
    if frame.len() < 10 || frame[0] != 0x2f || frame[2] != 0x28 {
        return None;
    }
    if frame[3] != feature::DAYTIME_HR {
        return None;
    }
    let lo = frame[8];
    let hi = frame[9];
    let ibi_ms = (((hi & 0x0f) as u16) << 8) | lo as u16;
    let validity = (hi >> 4) & 0x0f;
    if validity != 1 {
        return None;
    }
    bpm_from_ibi(ibi_ms).map(|bpm| HeartRateSample { bpm, ibi_ms })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::mock::MockTransport;

    #[tokio::test]
    async fn reads_firmware_over_mock() {
        let mock = MockTransport::new();
        mock.on(
            "0803000000",
            &["091202000003040301000105000cffeeddccbbaa"],
        );
        let client = OuraClient::new(mock).with_quiet(Duration::from_millis(20));
        let info = client.firmware().await.unwrap();
        assert_eq!(info.firmware_version, "3.4.3");
    }

    #[tokio::test]
    async fn authenticates_over_mock() {
        let mock = MockTransport::new();
        mock.on("2f012b", &["2f102c0e2d6a0a08c99b4365f458e6e97382"]);
        // The encrypted authenticate request for this key+nonce, then success.
        mock.on(
            "2f112da38a8772d3acb6db5c2b516dd56987c8",
            &["2f022e00"],
        );
        let client = OuraClient::new(mock).with_quiet(Duration::from_millis(20));
        let key: [u8; 16] = hex::decode("4431967d8bacc2659743142b68391d9a")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(client.authenticate(&key).await.unwrap(), AuthResult::Success);
    }

    const AUTH_KEY: &str = "4431967d8bacc2659743142b68391d9a";
    const NONCE_REQUEST: &str = "2f012b";
    const NONCE_REPLY: &str = "2f102c0e2d6a0a08c99b4365f458e6e97382";
    const AUTHENTICATE_REQUEST: &str = "2f112da38a8772d3acb6db5c2b516dd56987c8";

    async fn authenticate_against(mock: MockTransport) -> Result<AuthResult> {
        let client = OuraClient::new(mock).with_quiet(Duration::from_millis(20));
        let key: [u8; 16] = hex::decode(AUTH_KEY).unwrap().try_into().unwrap();
        client.authenticate(&key).await
    }

    #[tokio::test]
    async fn silent_nonce_request_is_no_response_not_auth() {
        // A write dropped before the link was up looks exactly like this, and
        // it must not read as a rejected key.
        let err = authenticate_against(MockTransport::new()).await.unwrap_err();
        assert!(matches!(err, Error::NoResponse(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn silent_authenticate_request_is_no_response_not_auth() {
        let mock = MockTransport::new();
        mock.on(NONCE_REQUEST, &[NONCE_REPLY]);
        let err = authenticate_against(mock).await.unwrap_err();
        assert!(matches!(err, Error::NoResponse(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unexpected_nonce_reply_is_protocol_not_auth() {
        let mock = MockTransport::new();
        mock.on(NONCE_REQUEST, &["2f022e00"]);
        let err = authenticate_against(mock).await.unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn explicit_auth_failure_is_auth() {
        let mock = MockTransport::new();
        mock.on(NONCE_REQUEST, &[NONCE_REPLY]);
        // 0x03 = NotOriginalOnboardedDevice.
        mock.on(AUTHENTICATE_REQUEST, &["2f022e03"]);
        let err = authenticate_against(mock).await.unwrap_err();
        assert!(matches!(err, Error::Auth(_)), "got {err:?}");
    }

    // --- stopping at the last frame instead of on silence -------------------

    /// The real window, against a paused clock: these assert that it was never
    /// entered, not that the machine is fast.
    const REAL_QUIET: Duration = DEFAULT_QUIET;

    const GET_EVENTS_FROM_0: &str = "100900000000ffffffffff";
    const GET_EVENTS_FROM_11: &str = "10090b000000ffffffffff";
    /// `[count=1, sleep_progress=0, bytes_left]`.
    const SUMMARY_MORE_LEFT: &str = "110601000a000000";
    const SUMMARY_DRAINED: &str = "1106010000000000";
    /// `debug_event` (0x43) at deciseconds 10 and 20.
    const EVENT_AT_10: &str = "43050a000000aa";
    const EVENT_AT_20: &str = "430514000000bb";

    #[tokio::test(start_paused = true)]
    async fn a_metadata_read_returns_on_its_response_tag() {
        let mock = MockTransport::new();
        mock.on(
            "0803000000",
            &["091202000003040301000105000cffeeddccbbaa"],
        );
        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);

        let started = tokio::time::Instant::now();
        assert_eq!(client.firmware().await.unwrap().firmware_version, "3.4.3");
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn authenticating_costs_no_quiet_windows() {
        let mock = MockTransport::new();
        mock.on(NONCE_REQUEST, &[NONCE_REPLY]);
        mock.on(AUTHENTICATE_REQUEST, &["2f022e00"]);

        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);
        let key: [u8; 16] = hex::decode(AUTH_KEY).unwrap().try_into().unwrap();

        let started = tokio::time::Instant::now();
        assert_eq!(client.authenticate(&key).await.unwrap(), AuthResult::Success);
        assert_eq!(started.elapsed(), Duration::ZERO, "two requests, no windows");
    }

    #[tokio::test(start_paused = true)]
    async fn the_drain_returns_on_each_batch_summary() {
        // Two batches, both closed by a `0x11`. Before the summary was treated as
        // the terminator this cost one quiet window per batch, which against a
        // night's hundred batches was two thirds of the whole sync.
        let mock = MockTransport::new();
        mock.on(GET_EVENTS_FROM_0, &[EVENT_AT_10, SUMMARY_MORE_LEFT]);
        mock.on(GET_EVENTS_FROM_11, &[EVENT_AT_20, SUMMARY_DRAINED]);
        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);

        let started = tokio::time::Instant::now();
        let mut seen = Vec::new();
        let outcome = client
            .drain_events(0, |ev| seen.push(ev.timestamp), |_, _| {})
            .await
            .unwrap();

        assert_eq!(seen, vec![10, 20]);
        assert_eq!(outcome.events_synced, 2);
        assert_eq!(outcome.next_cursor, 21);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_summary_before_the_events_still_drains_the_batch() {
        // The ordering is observed, not guaranteed, and getting it wrong must not
        // cost events: a ring that summarised up front would otherwise have its
        // batch cut off before the events, and the drain would stop with the
        // backlog still on the ring. Instead the early exit simply never fires.
        let mock = MockTransport::new();
        mock.on(GET_EVENTS_FROM_0, &[SUMMARY_MORE_LEFT, EVENT_AT_10]);
        mock.on(GET_EVENTS_FROM_11, &[SUMMARY_DRAINED, EVENT_AT_20]);
        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);

        let started = tokio::time::Instant::now();
        let mut seen = Vec::new();
        let outcome = client
            .drain_events(0, |ev| seen.push(ev.timestamp), |_, _| {})
            .await
            .unwrap();

        assert_eq!(seen, vec![10, 20], "no event may be lost to a wrong guess");
        assert_eq!(outcome.next_cursor, 21);
        assert_eq!(started.elapsed(), 2 * REAL_QUIET, "one window per batch");
    }

    #[tokio::test(start_paused = true)]
    async fn a_frame_trailing_one_batch_is_not_counted_against_the_next() {
        // Returning at the summary ends the reply before the link is known to be
        // quiet, so a frame the ring emits after it arrives inside the *next*
        // batch's window. It is recognisable by being older than the cursor it
        // turned up behind, and counting it would hand the caller an event it
        // has already stored and overstate `events_synced`.
        let mock = MockTransport::new();
        mock.on(GET_EVENTS_FROM_0, &[EVENT_AT_10, SUMMARY_MORE_LEFT]);
        mock.on(
            GET_EVENTS_FROM_11,
            &[EVENT_AT_10, EVENT_AT_20, SUMMARY_DRAINED],
        );
        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);

        let mut seen = Vec::new();
        let outcome = client
            .drain_events(0, |ev| seen.push(ev.timestamp), |_, _| {})
            .await
            .unwrap();

        assert_eq!(seen, vec![10, 20], "the trailing frame is not an event here");
        assert_eq!(outcome.events_synced, 2);
        assert_eq!(outcome.next_cursor, 21);
    }

    #[tokio::test(start_paused = true)]
    async fn a_trailing_frame_cannot_arm_the_early_exit() {
        // The same stray, but ahead of a summary that the batch's real events
        // follow. Arming on it would stop the read at that summary and lose
        // `EVENT_AT_20` for good: the cursor advances on the stray, so the drain
        // moves past 20 and no later sync goes back for it.
        let mock = MockTransport::new();
        mock.on(GET_EVENTS_FROM_0, &[EVENT_AT_10, SUMMARY_MORE_LEFT]);
        mock.on(
            GET_EVENTS_FROM_11,
            &[EVENT_AT_10, SUMMARY_DRAINED, EVENT_AT_20],
        );
        let client = OuraClient::new(mock).with_quiet(REAL_QUIET);

        let mut seen = Vec::new();
        let outcome = client
            .drain_events(0, |ev| seen.push(ev.timestamp), |_, _| {})
            .await
            .unwrap();

        assert_eq!(seen, vec![10, 20], "the batch's own event must survive");
        assert_eq!(outcome.next_cursor, 21);
    }

    #[test]
    fn acm_frame_decodes_two_samples() {
        // 33 0c 32 01 | 0100 0200 0300 | 0400 0500 0600
        let frame = [
            0x33, 0x0c, 0x32, 0x01, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0,
        ];
        let s = parse_acm_frame(&frame);
        assert_eq!(s.len(), 2);
        assert_eq!((s[0].x, s[0].y, s[0].z), (1, 2, 3));
        assert_eq!((s[1].x, s[1].y, s[1].z), (4, 5, 6));
    }

    #[test]
    fn live_hr_frame_decodes() {
        // ibi=857ms (0x359), validity=1 -> hi=0x13, lo=0x59; bpm=60000/857=70
        let frame = [0x2f, 0x08, 0x28, 0x02, 0x00, 0x02, 0x00, 0x00, 0x59, 0x13];
        let s = parse_live_hr_frame(&frame).unwrap();
        assert_eq!(s.ibi_ms, 857);
        assert_eq!(s.bpm, 70);
    }
}
