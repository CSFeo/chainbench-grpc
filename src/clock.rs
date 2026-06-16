//! Minimal SNTP client for clock-offset calibration.
//!
//! Absolute one-way latency (`client_wallclock − server_created_at`) is only
//! valid when the measurement host's clock is synchronized. This module probes
//! NTP servers at startup to estimate the host's offset vs UTC so that offset
//! can be corrected out of the absolute-latency measurement.
//!
//! Pure `std` (UDP) — no extra dependencies. Offset sign convention: **positive
//! means the local clock is behind UTC** (so the correction is `local + offset`).

use std::net::{ToSocketAddrs, UdpSocket};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default public NTP servers probed when no manual offset is given.
pub const DEFAULT_NTP_SERVERS: &[&str] =
    &["time.cloudflare.com", "time.google.com", "pool.ntp.org"];

/// Seconds between the NTP epoch (1900-01-01) and the Unix epoch (1970-01-01).
const NTP_UNIX_DELTA: f64 = 2_208_988_800.0;

#[derive(Debug, Clone)]
pub struct ClockOffset {
    /// Estimated offset in milliseconds; positive = local clock is behind UTC.
    pub offset_ms: f64,
    /// Round-trip time to the NTP server, milliseconds.
    pub rtt_ms: f64,
    /// Server that produced this estimate.
    pub server: String,
}

fn now_unix_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Convert an 8-byte NTP timestamp (big-endian: 32-bit seconds + 32-bit fraction)
/// to Unix seconds as f64.
fn ntp_to_unix(bytes: &[u8]) -> f64 {
    let secs = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64;
    let frac =
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as f64 / 4_294_967_296.0;
    (secs + frac) - NTP_UNIX_DELTA
}

/// NTP offset/delay from the four timestamps (all in Unix seconds):
/// T1 = client send, T2 = server recv, T3 = server send, T4 = client recv.
/// Returns (offset_ms, rtt_ms). Positive offset = local clock behind UTC.
pub fn compute_offset_ms(t1: f64, t2: f64, t3: f64, t4: f64) -> (f64, f64) {
    let offset = ((t2 - t1) + (t3 - t4)) / 2.0;
    let rtt = (t4 - t1) - (t3 - t2);
    (offset * 1000.0, rtt * 1000.0)
}

/// Query a single NTP server. `timeout` bounds both send and receive.
pub fn query_ntp_offset(server: &str, timeout: Duration) -> std::io::Result<ClockOffset> {
    let addr = format!("{server}:123")
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no NTP address"))?;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_read_timeout(Some(timeout))?;
    socket.set_write_timeout(Some(timeout))?;

    // 48-byte request: LI=0, VN=4, Mode=3 (client) -> 0x23.
    let mut packet = [0u8; 48];
    packet[0] = 0x23;

    let t1 = now_unix_secs();
    socket.send_to(&packet, addr)?;

    let mut buf = [0u8; 48];
    let (n, _) = socket.recv_from(&mut buf)?;
    let t4 = now_unix_secs();
    if n < 48 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "short NTP response",
        ));
    }

    // T2 = server receive timestamp (bytes 32..40), T3 = transmit (bytes 40..48).
    let t2 = ntp_to_unix(&buf[32..40]);
    let t3 = ntp_to_unix(&buf[40..48]);
    if t2 <= 0.0 || t3 <= 0.0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid NTP timestamps",
        ));
    }

    let (offset_ms, rtt_ms) = compute_offset_ms(t1, t2, t3, t4);
    Ok(ClockOffset {
        offset_ms,
        rtt_ms,
        server: server.to_string(),
    })
}

/// Probe several NTP servers and return the most reliable estimate (lowest RTT,
/// per NTP's own selection heuristic). Returns `None` if all probes fail (e.g.
/// UDP/123 firewalled).
pub fn measure_clock_offset(servers: &[&str]) -> Option<ClockOffset> {
    let timeout = Duration::from_secs(2);
    servers
        .iter()
        .filter_map(|s| query_ntp_offset(s, timeout).ok())
        .min_by(|a, b| {
            a.rtt_ms
                .partial_cmp(&b.rtt_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_zero_when_clocks_agree_and_symmetric() {
        // Client and server agree; symmetric 20ms each way.
        // T1=0, T2=0.020, T3=0.020, T4=0.040
        let (offset, rtt) = compute_offset_ms(0.0, 0.020, 0.020, 0.040);
        assert!((offset - 0.0).abs() < 1e-6, "offset={offset}");
        assert!((rtt - 40.0).abs() < 1e-6, "rtt={rtt}");
    }

    #[test]
    fn positive_offset_when_local_behind() {
        // Server clock is 84ms ahead of local; symmetric 25ms path.
        // local sends at 0 -> arrives at server's 0.025+0.084; server replies
        // instantly at 0.109; client receives at 0.050 (local).
        let t1 = 0.0;
        let t2 = 0.025 + 0.084; // server receive (server clock)
        let t3 = 0.025 + 0.084; // server transmit (server clock, no processing)
        let t4 = 0.050; // client receive (local clock)
        let (offset, rtt) = compute_offset_ms(t1, t2, t3, t4);
        assert!((offset - 84.0).abs() < 1.0, "offset={offset}");
        assert!((rtt - 50.0).abs() < 1.0, "rtt={rtt}");
    }

    #[test]
    fn ntp_to_unix_known_value() {
        // NTP seconds for 1970-01-01 is exactly NTP_UNIX_DELTA -> unix 0.
        let secs = (NTP_UNIX_DELTA as u32).to_be_bytes();
        let bytes = [secs[0], secs[1], secs[2], secs[3], 0, 0, 0, 0];
        assert!(ntp_to_unix(&bytes).abs() < 1e-3);
    }

    #[test]
    fn ntp_to_unix_half_second_fraction() {
        // fraction = 0x80000000 -> 0.5s
        let secs = ((NTP_UNIX_DELTA as u32) + 10).to_be_bytes();
        let bytes = [secs[0], secs[1], secs[2], secs[3], 0x80, 0, 0, 0];
        assert!((ntp_to_unix(&bytes) - 10.5).abs() < 1e-6);
    }
}
