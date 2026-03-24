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

pub fn extract_server_timestamp(created_at: Option<&prost_types::Timestamp>) -> Option<f64> {
    created_at.map(|ts| (ts.seconds as f64) * 1000.0 + (ts.nanos as f64) / 1_000_000.0)
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

pub fn make_observation(
    created_at: Option<&prost_types::Timestamp>,
    start_instant: Instant,
    start_wallclock_ms: f64,
) -> TransactionData {
    let client_wallclock_ms = get_current_timestamp_ms();
    let elapsed = start_instant.elapsed();

    match extract_server_timestamp(created_at) {
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
