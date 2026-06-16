//! SNTP client — probes NTP servers over UDP and produces a domain
//! [`ClockOffset`]. The offset *formula* lives in [`crate::domain::clock`];
//! this module owns the NTP wire format and socket I/O.
//!
//! Pure `std` (UDP) — no extra dependencies.

use std::net::{ToSocketAddrs, UdpSocket};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::domain::clock::{ClockOffset, compute_offset_ms};

/// Default public NTP servers probed when no manual offset is given.
pub const DEFAULT_NTP_SERVERS: &[&str] =
    &["time.cloudflare.com", "time.google.com", "pool.ntp.org"];

/// Seconds between the NTP epoch (1900-01-01) and the Unix epoch (1970-01-01).
const NTP_UNIX_DELTA: f64 = 2_208_988_800.0;

fn now_unix_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Convert an 8-byte NTP timestamp (big-endian: 32-bit seconds + 32-bit
/// fraction) to Unix seconds as f64.
fn ntp_to_unix(bytes: &[u8]) -> f64 {
    let secs = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64;
    let frac =
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as f64 / 4_294_967_296.0;
    (secs + frac) - NTP_UNIX_DELTA
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
