use std::time::{Duration, Instant};

// Time constant of the exponentially decaying average.
const AVERAGING_WINDOW: Duration = Duration::from_secs(2);

// If the peer sent nothing for this long, its current rate says nothing about it.
const QUIET_PEER: Duration = Duration::from_secs(30);

/// Payload download rate from one peer.
#[derive(Debug, Default)]
pub(crate) struct DownloadRate {
    bytes_per_second: f64,
    peak: f64,
    last_payload: Option<Instant>,
}

impl DownloadRate {
    pub fn on_payload(&mut self, bytes: u32, now: Instant) {
        self.bytes_per_second = self.current(now) + bytes as f64 / AVERAGING_WINDOW.as_secs_f64();
        self.peak = self.peak.max(self.bytes_per_second);
        self.last_payload = Some(now);
    }

    fn current(&self, now: Instant) -> f64 {
        let Some(last) = self.last_payload else {
            return 0.;
        };
        let decay =
            now.saturating_duration_since(last).as_secs_f64() / AVERAGING_WINDOW.as_secs_f64();
        self.bytes_per_second * (-decay).exp()
    }

    /// The rate to expect for new requests, None if the peer never sent any payload.
    pub fn estimate(&self, now: Instant) -> Option<f64> {
        let last = self.last_payload?;
        if now.saturating_duration_since(last) > QUIET_PEER {
            return Some(self.peak);
        }
        Some(self.current(now))
    }
}
