//! Observation timing: the value object recorded each time an endpoint delivers
//! a transaction, plus the wall/monotonic clock reads used to build it.
//!
//! The domain works in plain Unix-millisecond `f64`s. Converting a provider's
//! protobuf `created_at` into milliseconds is an infrastructure concern (see
//! [`crate::infrastructure::geyser`]).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampSource {
    ServerCreatedAt,
    ClientWallclock,
}

#[derive(Debug, Clone)]
pub struct TransactionData {
    /// Best available timestamp in unix milliseconds (server or client)
    pub timestamp_ms: f64,
    /// Where the timestamp came from
    pub timestamp_source: TimestampSource,
    /// Client wallclock always captured as fallback (unix ms)
    pub client_wallclock_ms: f64,
    /// Monotonic elapsed since benchmark start (for relative comparisons)
    pub elapsed_since_start: Duration,
    /// Wallclock at benchmark start (unix ms), used to detect backfill
    pub start_wallclock_ms: f64,
}

pub fn get_current_timestamp_ms() -> f64 {
    let now = SystemTime::now();
    match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64() * 1000.0,
        Err(e) => {
            warn!("SystemTime error (clock skew): {}", e);
            0.0
        }
    }
}

/// Build an observation. `server_ms` is the server `created_at` in Unix
/// milliseconds when present; otherwise the client wallclock is used as the
/// timestamp (and flagged as such).
pub fn observe(
    server_ms: Option<f64>,
    start_instant: Instant,
    start_wallclock_ms: f64,
) -> TransactionData {
    let client_wallclock_ms = get_current_timestamp_ms();
    let elapsed = start_instant.elapsed();

    match server_ms {
        Some(server_ms) => TransactionData {
            timestamp_ms: server_ms,
            timestamp_source: TimestampSource::ServerCreatedAt,
            client_wallclock_ms,
            elapsed_since_start: elapsed,
            start_wallclock_ms,
        },
        None => TransactionData {
            timestamp_ms: client_wallclock_ms,
            timestamp_source: TimestampSource::ClientWallclock,
            client_wallclock_ms,
            elapsed_since_start: elapsed,
            start_wallclock_ms,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_uses_server_timestamp_when_present() {
        let obs = observe(Some(10_000.0), Instant::now(), 1_000.0);
        assert_eq!(obs.timestamp_source, TimestampSource::ServerCreatedAt);
        assert_eq!(obs.timestamp_ms, 10_000.0);
        assert_eq!(obs.start_wallclock_ms, 1_000.0);
    }

    #[test]
    fn observe_falls_back_to_client_wallclock() {
        let obs = observe(None, Instant::now(), 1_000.0);
        assert_eq!(obs.timestamp_source, TimestampSource::ClientWallclock);
        // With no server stamp, timestamp_ms mirrors the client wallclock.
        assert_eq!(obs.timestamp_ms, obs.client_wallclock_ms);
    }
}
