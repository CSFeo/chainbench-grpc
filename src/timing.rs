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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_server_timestamp_none_when_absent() {
        assert_eq!(extract_server_timestamp(None), None);
    }

    #[test]
    fn extract_server_timestamp_combines_seconds_and_nanos() {
        let ts = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000, // 0.5s -> 500ms
        };
        let ms = extract_server_timestamp(Some(&ts)).unwrap();
        assert_eq!(ms, 1_700_000_000_000.0 + 500.0);
    }

    #[test]
    fn extract_server_timestamp_sub_millisecond_precision() {
        // 1ns -> 1e-6 ms; verifies we don't lose sub-ms precision (unlike
        // blocktime-based tools that only have second granularity).
        let ts = prost_types::Timestamp {
            seconds: 0,
            nanos: 1,
        };
        assert_eq!(extract_server_timestamp(Some(&ts)).unwrap(), 1e-6);
    }

    #[test]
    fn make_observation_uses_server_timestamp_when_present() {
        let ts = prost_types::Timestamp {
            seconds: 10,
            nanos: 0,
        };
        let obs = make_observation(Some(&ts), Instant::now(), 1_000.0);
        assert_eq!(obs.timestamp_source, TimestampSource::ServerCreatedAt);
        assert_eq!(obs.timestamp_ms, 10_000.0);
        assert_eq!(obs.start_wallclock_ms, 1_000.0);
    }

    #[test]
    fn make_observation_falls_back_to_client_wallclock() {
        let obs = make_observation(None, Instant::now(), 1_000.0);
        assert_eq!(obs.timestamp_source, TimestampSource::ClientWallclock);
        // With no server stamp, timestamp_ms mirrors the client wallclock.
        assert_eq!(obs.timestamp_ms, obs.client_wallclock_ms);
    }
}
