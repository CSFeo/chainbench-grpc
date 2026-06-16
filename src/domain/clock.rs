//! Clock-offset domain logic.
//!
//! Absolute one-way latency (`client_wallclock − server_created_at`) is only
//! valid when the two clocks are synchronized. This module owns the offset
//! *value object* and the *formula*; the NTP wire protocol and socket I/O that
//! feed it live in [`crate::infrastructure::sntp`].
//!
//! Sign convention: **positive offset means the local clock is behind UTC**, so
//! the correction applied to a client timestamp is `client + offset`.

#[derive(Debug, Clone)]
pub struct ClockOffset {
    /// Estimated offset in milliseconds; positive = local clock is behind UTC.
    pub offset_ms: f64,
    /// Round-trip time to the reference server, milliseconds.
    pub rtt_ms: f64,
    /// Identifier of the server that produced this estimate.
    pub server: String,
}

/// NTP offset/delay from the four timestamps (all in Unix seconds):
/// T1 = client send, T2 = server recv, T3 = server send, T4 = client recv.
/// Returns `(offset_ms, rtt_ms)`; positive offset = local clock behind UTC.
pub fn compute_offset_ms(t1: f64, t2: f64, t3: f64, t4: f64) -> (f64, f64) {
    let offset = ((t2 - t1) + (t3 - t4)) / 2.0;
    let rtt = (t4 - t1) - (t3 - t2);
    (offset * 1000.0, rtt * 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_zero_when_clocks_agree_and_symmetric() {
        // Client and server agree; symmetric 20ms each way.
        let (offset, rtt) = compute_offset_ms(0.0, 0.020, 0.020, 0.040);
        assert!(offset.abs() < 1e-6, "offset={offset}");
        assert!((rtt - 40.0).abs() < 1e-6, "rtt={rtt}");
    }

    #[test]
    fn positive_offset_when_local_behind() {
        // Server clock 84ms ahead of local; symmetric 25ms path.
        let t1 = 0.0;
        let t2 = 0.025 + 0.084;
        let t3 = 0.025 + 0.084;
        let t4 = 0.050;
        let (offset, rtt) = compute_offset_ms(t1, t2, t3, t4);
        assert!((offset - 84.0).abs() < 1.0, "offset={offset}");
        assert!((rtt - 50.0).abs() < 1.0, "rtt={rtt}");
    }
}
