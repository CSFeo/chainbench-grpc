use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

use serde::Serialize;

use crate::collector::Comparator;
use crate::timing::TimestampSource;

/// Latency distribution buckets (from Shyft pattern)
#[derive(Debug, Default, Clone, Serialize)]
pub struct LatencyBuckets {
    pub less_than_400: usize,
    pub from_400_to_799: usize,
    pub from_800_to_999: usize,
    pub from_1000_to_1199: usize,
    pub from_1200_to_1499: usize,
    pub from_1500_to_1999: usize,
    pub at_2000_or_more: usize,
}

impl LatencyBuckets {
    pub fn record(&mut self, latency_ms: f64) {
        match latency_ms as u64 {
            0..400 => self.less_than_400 += 1,
            400..800 => self.from_400_to_799 += 1,
            800..1000 => self.from_800_to_999 += 1,
            1000..1200 => self.from_1000_to_1199 += 1,
            1200..1500 => self.from_1200_to_1499 += 1,
            1500..2000 => self.from_1500_to_1999 += 1,
            _ => self.at_2000_or_more += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.less_than_400
            + self.from_400_to_799
            + self.from_800_to_999
            + self.from_1000_to_1199
            + self.from_1200_to_1499
            + self.from_1500_to_1999
            + self.at_2000_or_more
    }
}

#[derive(Default)]
struct EndpointStats {
    total_observations: usize,
    first_detections: usize,
    /// Count of non-backfill signatures this endpoint delivered, regardless of
    /// whether all peers also delivered them. Basis for delivery success rate.
    observed_signatures: usize,
    delays_ms: Vec<f64>,
    absolute_latencies_ms: Vec<f64>,
    buckets: LatencyBuckets,
    backfill_transactions: usize,
    server_timestamp_count: usize,
    client_timestamp_count: usize,
    /// Server-stamped samples with negative absolute latency (client clock
    /// behind server clock). Excluded from the distribution, reported for
    /// transparency — a high count signals NTP skew on the benchmark host.
    skewed_latency_count: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct EndpointSummary {
    pub name: String,
    // Relative metrics (win rate)
    pub first_share: f64,
    pub rel_p50_ms: Option<f64>,
    pub rel_p90_ms: Option<f64>,
    pub rel_p95_ms: Option<f64>,
    pub rel_p99_ms: Option<f64>,
    // Absolute latency metrics
    pub abs_p50_ms: Option<f64>,
    pub abs_p90_ms: Option<f64>,
    pub abs_p95_ms: Option<f64>,
    pub abs_p99_ms: Option<f64>,
    pub buckets: LatencyBuckets,
    // Reliability
    pub server_timestamp_count: usize,
    pub client_timestamp_count: usize,
    pub skewed_latency_count: usize,
    pub timestamp_coverage_pct: f64,
    pub reconnect_count: u32,
    /// Fraction of non-backfill signatures (seen by any endpoint) that this
    /// endpoint delivered, as a percentage. 100% means it missed nothing.
    pub success_rate_pct: f64,
    // Counts
    pub valid_transactions: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
    pub tx_per_sec: f64,
    // Composite score (0-100)
    pub score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub endpoints: Vec<EndpointSummary>,
    pub fastest_endpoint: Option<String>,
    pub has_data: bool,
    pub total_signatures: usize,
    pub backfill_signatures: usize,
    // Test metadata
    pub test_duration_secs: f64,
    pub throughput_tx_per_sec: f64,
    pub warmup_skipped: usize,
    pub total_errors: usize,
    /// Clock-offset (ms, local-behind-UTC) added to client wallclock before
    /// computing absolute latency. 0.0 = no correction applied.
    pub clock_offset_ms: f64,
}

/// Per-endpoint runtime stats reported back by each provider task.
#[derive(Debug, Clone, Default)]
pub struct EndpointRuntime {
    pub reconnect_count: u32,
    pub warmup_skipped: usize,
}

pub struct RunMetadata {
    pub duration_secs: f64,
    pub total_errors: usize,
    /// Keyed by endpoint name.
    pub endpoint_runtime: HashMap<String, EndpointRuntime>,
    /// Clock offset (ms, local-behind-UTC) to add to client wallclock before
    /// computing absolute latency. 0.0 disables correction.
    pub clock_offset_ms: f64,
}

pub fn compute_run_summary(
    comparator: &Comparator,
    endpoint_names: &[String],
    metadata: RunMetadata,
) -> RunSummary {
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();
    let expected_producers = endpoint_names.len();
    let clock_offset_ms = metadata.clock_offset_ms;
    let mut total_signatures = 0usize;
    let mut backfill_signatures = 0usize;
    // Union of non-backfill signatures seen by *any* endpoint — the denominator
    // for per-endpoint delivery success rate.
    let mut union_signatures = 0usize;

    for name in endpoint_names {
        endpoint_stats.insert(name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();

        if sig_data.is_empty() {
            continue;
        }

        // Backfill = a transaction the server created *before* this benchmark
        // started but delivered to us on subscribe. The only reliable signal is
        // the server `created_at` timestamp predating our start wallclock; the
        // client receive time is always after start, so it can never detect this.
        let is_historical = sig_data.values().any(|tx| {
            tx.timestamp_source == TimestampSource::ServerCreatedAt
                && tx.timestamp_ms < tx.start_wallclock_ms
        });

        if is_historical {
            backfill_signatures += 1;
            for endpoint in sig_data.keys() {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.backfill_transactions += 1;
                }
            }
            continue;
        }

        // Delivery tracking — counted for every non-backfill signature regardless
        // of how many endpoints reported it, so success_rate captures misses.
        union_signatures += 1;
        for endpoint in sig_data.keys() {
            if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                stats.observed_signatures += 1;
            }
        }

        // LIMITATION: win-rate and relative-latency require *all* N producers to
        // have reported this signature. A signature missed by any endpoint is
        // excluded from these comparison metrics (but still counted in the
        // delivery tracking above and reflected in success_rate).
        if expected_producers > 1 && sig_data.len() != expected_producers {
            continue;
        }

        // Find the fastest endpoint by elapsed_since_start
        let Some((first_endpoint, first_tx)) =
            sig_data.iter().min_by_key(|(_, tx)| tx.elapsed_since_start)
        else {
            continue;
        };

        total_signatures += 1;
        let first_endpoint_name = first_endpoint.clone();

        for (endpoint, tx) in sig_data.iter() {
            if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                stats.total_observations += 1;

                // Relative delay (win rate computation)
                if endpoint == &first_endpoint_name {
                    stats.first_detections += 1;
                    stats.delays_ms.push(0.0);
                } else {
                    let delay: Duration = tx
                        .elapsed_since_start
                        .saturating_sub(first_tx.elapsed_since_start);
                    stats.delays_ms.push(delay.as_secs_f64() * 1000.0);
                }

                // Track timestamp source
                if tx.timestamp_source == TimestampSource::ServerCreatedAt {
                    stats.server_timestamp_count += 1;
                    // Correct the client wallclock by the measured host offset
                    // before differencing against the server `created_at`.
                    let abs_latency = (tx.client_wallclock_ms + clock_offset_ms) - tx.timestamp_ms;
                    if abs_latency >= 0.0 {
                        stats.absolute_latencies_ms.push(abs_latency);
                        stats.buckets.record(abs_latency);
                    } else {
                        // Negative latency is physically meaningless (clock skew);
                        // count it rather than silently discarding the sample.
                        stats.skewed_latency_count += 1;
                    }
                } else {
                    stats.client_timestamp_count += 1;
                }
            }
        }
    }

    let num_endpoints = endpoint_names.len();

    let mut endpoints: Vec<EndpointSummary> = endpoint_stats
        .into_iter()
        .map(|(name, stats)| {
            let runtime = metadata
                .endpoint_runtime
                .get(&name)
                .cloned()
                .unwrap_or_default();
            build_summary(
                name,
                stats,
                total_signatures,
                union_signatures,
                metadata.duration_secs,
                runtime,
            )
        })
        .collect();

    // Compute composite scores
    compute_scores(&mut endpoints, num_endpoints);

    let has_data = total_signatures > 0;

    let fastest_endpoint = endpoints
        .iter()
        .filter(|s| s.valid_transactions > 0)
        .min_by(|a, b| compare_latency(a, b))
        .map(|s| s.name.clone());

    let throughput = if metadata.duration_secs > 0.0 {
        total_signatures as f64 / metadata.duration_secs
    } else {
        0.0
    };

    let warmup_skipped = metadata
        .endpoint_runtime
        .values()
        .map(|r| r.warmup_skipped)
        .sum();

    RunSummary {
        endpoints,
        fastest_endpoint,
        has_data,
        total_signatures,
        backfill_signatures,
        test_duration_secs: metadata.duration_secs,
        throughput_tx_per_sec: throughput,
        warmup_skipped,
        total_errors: metadata.total_errors,
        clock_offset_ms: metadata.clock_offset_ms,
    }
}

fn build_summary(
    name: String,
    stats: EndpointStats,
    total_signatures: usize,
    union_signatures: usize,
    duration_secs: f64,
    runtime: EndpointRuntime,
) -> EndpointSummary {
    let total_ts = stats.server_timestamp_count + stats.client_timestamp_count;
    let coverage = if total_ts > 0 {
        stats.server_timestamp_count as f64 / total_ts as f64 * 100.0
    } else {
        0.0
    };

    let success_rate_pct = if union_signatures > 0 {
        stats.observed_signatures as f64 / union_signatures as f64 * 100.0
    } else {
        0.0
    };

    let tx_per_sec = if duration_secs > 0.0 {
        stats.observed_signatures as f64 / duration_secs
    } else {
        0.0
    };

    let mut summary = EndpointSummary {
        name,
        valid_transactions: stats.total_observations,
        first_detections: stats.first_detections,
        backfill_transactions: stats.backfill_transactions,
        server_timestamp_count: stats.server_timestamp_count,
        client_timestamp_count: stats.client_timestamp_count,
        skewed_latency_count: stats.skewed_latency_count,
        timestamp_coverage_pct: coverage,
        reconnect_count: runtime.reconnect_count,
        success_rate_pct,
        tx_per_sec,
        buckets: stats.buckets,
        ..Default::default()
    };

    if total_signatures > 0 {
        summary.first_share = stats.first_detections as f64 / total_signatures as f64;
    }

    if !stats.delays_ms.is_empty() {
        let mut sorted = stats.delays_ms;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        summary.rel_p50_ms = Some(percentile(&sorted, 0.50));
        summary.rel_p90_ms = Some(percentile(&sorted, 0.90));
        summary.rel_p95_ms = Some(percentile(&sorted, 0.95));
        summary.rel_p99_ms = Some(percentile(&sorted, 0.99));
    }

    if !stats.absolute_latencies_ms.is_empty() {
        let mut sorted = stats.absolute_latencies_ms;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        summary.abs_p50_ms = Some(percentile(&sorted, 0.50));
        summary.abs_p90_ms = Some(percentile(&sorted, 0.90));
        summary.abs_p95_ms = Some(percentile(&sorted, 0.95));
        summary.abs_p99_ms = Some(percentile(&sorted, 0.99));
    }

    summary
}

pub fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

/// Composite score (0-100) combining win rate, latency, and reliability.
///
/// Uses **fixed absolute thresholds**, not normalization relative to the best
/// peer in the run — so a given endpoint's score is stable regardless of which
/// competitors it is compared against (a relative-to-best score would shift as
/// the competitor set changes). The throughput component is the one exception:
/// it is relative to the busiest endpoint, measuring "did this endpoint keep up".
///
/// Formula:
///   win_rate_component    = first_share * 30                      (30% weight)
///   latency_component     = clamp((1000 - p50)/950) * 25          (25% weight, lower P50 = better)
///   reliability_component = coverage_pct / 100 * 25               (25% weight)
///   stability_component   = clamp((1000 - (p99-p50))/950) * 10    (10% weight, low jitter)
///   throughput_component  = min(observations / max_observations, 1.0) * 10 (10% weight)
fn compute_scores(endpoints: &mut [EndpointSummary], num_endpoints: usize) {
    let max_observations = endpoints
        .iter()
        .map(|e| e.valid_transactions)
        .max()
        .unwrap_or(1)
        .max(1);

    for ep in endpoints.iter_mut() {
        let mut score = 0.0;

        // Win rate component (30 points) — only meaningful with 2+ endpoints
        if num_endpoints > 1 {
            score += ep.first_share * 30.0;
        } else {
            score += 30.0; // single endpoint gets full win rate score
        }

        // Latency component (25 points) — based on absolute P50
        if let Some(p50) = ep.abs_p50_ms {
            // Score: 25 if P50 <= 50ms, linearly to 0 at P50 >= 1000ms
            let latency_score = ((1000.0 - p50) / 950.0).clamp(0.0, 1.0);
            score += latency_score * 25.0;
        }

        // Reliability component (25 points) — server timestamp coverage
        score += (ep.timestamp_coverage_pct / 100.0) * 25.0;

        // Stability component (10 points) — low jitter (P99 - P50)
        if let (Some(p50), Some(p99)) = (ep.abs_p50_ms, ep.abs_p99_ms) {
            let jitter = p99 - p50;
            // Score: 10 if jitter <= 50ms, linearly to 0 at jitter >= 1000ms
            let stability = ((1000.0 - jitter) / 950.0).clamp(0.0, 1.0);
            score += stability * 10.0;
        }

        // Throughput component (10 points) — did this endpoint keep up?
        let throughput_ratio = ep.valid_transactions as f64 / max_observations as f64;
        score += throughput_ratio.min(1.0) * 10.0;

        ep.score = (score * 100.0).round() / 100.0; // round to 2 decimals
    }
}

fn compare_latency(lhs: &EndpointSummary, rhs: &EndpointSummary) -> Ordering {
    match (lhs.rel_p50_ms, rhs.rel_p50_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timing::{TimestampSource, TransactionData};
    use std::collections::HashMap;

    // --- percentile (nearest-rank with round) ---

    #[test]
    fn percentile_empty_is_zero() {
        assert_eq!(percentile(&[], 0.5), 0.0);
    }

    #[test]
    fn percentile_single_element() {
        assert_eq!(percentile(&[42.0], 0.5), 42.0);
        assert_eq!(percentile(&[42.0], 0.99), 42.0);
    }

    #[test]
    fn percentile_known_distribution() {
        // 1..=10, indices 0..=9. p50 -> round(0.5*9)=round(4.5)=5 -> 6.0
        let data: Vec<f64> = (1..=10).map(|n| n as f64).collect();
        assert_eq!(percentile(&data, 0.50), 6.0);
        assert_eq!(percentile(&data, 0.95), 10.0); // round(8.55)=9 -> 10
        assert_eq!(percentile(&data, 0.99), 10.0);
        assert_eq!(percentile(&data, 0.0), 1.0);
    }

    // --- latency buckets boundaries ---

    #[test]
    fn buckets_boundaries() {
        let mut b = LatencyBuckets::default();
        for v in [
            0.0, 399.9, 400.0, 799.0, 800.0, 999.0, 1000.0, 1500.0, 2000.0, 5000.0,
        ] {
            b.record(v);
        }
        assert_eq!(b.less_than_400, 2); // 0, 399.9
        assert_eq!(b.from_400_to_799, 2); // 400, 799
        assert_eq!(b.from_800_to_999, 2); // 800, 999
        assert_eq!(b.from_1000_to_1199, 1); // 1000
        assert_eq!(b.from_1500_to_1999, 1); // 1500
        assert_eq!(b.at_2000_or_more, 2); // 2000, 5000
        assert_eq!(b.total(), 10);
    }

    // --- compute_run_summary: backfill + skew + happy path ---

    fn server_tx(
        timestamp_ms: f64,
        client_ms: f64,
        start_ms: f64,
        elapsed_ms: u64,
    ) -> TransactionData {
        TransactionData {
            timestamp_ms,
            timestamp_source: TimestampSource::ServerCreatedAt,
            client_wallclock_ms: client_ms,
            elapsed_since_start: Duration::from_millis(elapsed_ms),
            start_wallclock_ms: start_ms,
        }
    }

    fn single_endpoint_summary(sigs: Vec<(&str, TransactionData)>) -> RunSummary {
        let comparator = Comparator::new();
        let mut batch = HashMap::new();
        for (sig, data) in sigs {
            batch.insert(sig.to_string(), data);
        }
        comparator.add_batch("ep1", batch);
        compute_run_summary(
            &comparator,
            &["ep1".to_string()],
            RunMetadata {
                duration_secs: 1.0,
                total_errors: 0,
                endpoint_runtime: HashMap::new(),
                clock_offset_ms: 0.0,
            },
        )
    }

    fn single_endpoint_summary_with_offset(
        sigs: Vec<(&str, TransactionData)>,
        clock_offset_ms: f64,
    ) -> RunSummary {
        let comparator = Comparator::new();
        let mut batch = HashMap::new();
        for (sig, data) in sigs {
            batch.insert(sig.to_string(), data);
        }
        comparator.add_batch("ep1", batch);
        compute_run_summary(
            &comparator,
            &["ep1".to_string()],
            RunMetadata {
                duration_secs: 1.0,
                total_errors: 0,
                endpoint_runtime: HashMap::new(),
                clock_offset_ms,
            },
        )
    }

    #[test]
    fn backfill_detected_via_server_timestamp() {
        let start = 1_000_000.0;
        // created 5s before start, received after start -> historical
        let summary = single_endpoint_summary(vec![(
            "sigA",
            server_tx(start - 5000.0, start + 50.0, start, 50),
        )]);
        assert_eq!(summary.backfill_signatures, 1);
        assert_eq!(summary.total_signatures, 0);
        assert_eq!(summary.endpoints[0].backfill_transactions, 1);
    }

    #[test]
    fn realtime_not_flagged_as_backfill() {
        let start = 1_000_000.0;
        // created after start -> real-time, 46ms latency
        let summary = single_endpoint_summary(vec![(
            "sigA",
            server_tx(start + 100.0, start + 146.0, start, 146),
        )]);
        assert_eq!(summary.backfill_signatures, 0);
        assert_eq!(summary.total_signatures, 1);
        assert_eq!(summary.endpoints[0].abs_p50_ms, Some(46.0));
        assert_eq!(summary.endpoints[0].abs_p90_ms, Some(46.0));
        assert_eq!(summary.endpoints[0].skewed_latency_count, 0);
        // single endpoint delivered the only signature -> 100% success
        assert_eq!(summary.endpoints[0].success_rate_pct, 100.0);
        assert_eq!(summary.endpoints[0].tx_per_sec, 1.0); // 1 sig / 1.0s
    }

    #[test]
    fn success_rate_reflects_missed_signatures() {
        // Two endpoints; sigA seen by both, sigB only by fast. Multi-endpoint
        // win-rate excludes sigB (partial), but success_rate must still show the
        // slow endpoint missed it.
        let start = 1_000_000.0;
        let comparator = Comparator::new();
        let mut fast = HashMap::new();
        fast.insert(
            "sigA".to_string(),
            server_tx(start + 100.0, start + 140.0, start, 100),
        );
        fast.insert(
            "sigB".to_string(),
            server_tx(start + 200.0, start + 240.0, start, 200),
        );
        comparator.add_batch("fast", fast);
        let mut slow = HashMap::new();
        slow.insert(
            "sigA".to_string(),
            server_tx(start + 100.0, start + 150.0, start, 110),
        );
        comparator.add_batch("slow", slow);

        let summary = compute_run_summary(
            &comparator,
            &["fast".to_string(), "slow".to_string()],
            RunMetadata {
                duration_secs: 1.0,
                total_errors: 0,
                endpoint_runtime: HashMap::new(),
                clock_offset_ms: 0.0,
            },
        );

        // union = 2 (sigA, sigB); only sigA is complete -> total_signatures = 1
        assert_eq!(summary.total_signatures, 1);
        let fast = summary.endpoints.iter().find(|e| e.name == "fast").unwrap();
        let slow = summary.endpoints.iter().find(|e| e.name == "slow").unwrap();
        assert_eq!(fast.success_rate_pct, 100.0); // saw both
        assert_eq!(slow.success_rate_pct, 50.0); // missed sigB
    }

    #[test]
    fn negative_latency_counted_not_dropped() {
        let start = 1_000_000.0;
        // server clock ahead of client -> negative abs latency, but real-time
        let summary = single_endpoint_summary(vec![(
            "sigA",
            server_tx(start + 200.0, start + 100.0, start, 100),
        )]);
        assert_eq!(summary.total_signatures, 1);
        assert_eq!(summary.endpoints[0].skewed_latency_count, 1);
        assert_eq!(summary.endpoints[0].abs_p50_ms, None); // excluded from distribution
        assert_eq!(summary.endpoints[0].buckets.total(), 0);
    }

    #[test]
    fn clock_offset_correction_recovers_true_latency() {
        // Reproduces the live finding: host clock 84ms behind, true one-way 25ms.
        // Raw abs latency = client - server = -59ms (negative, skewed).
        let start = 1_000_000.0;
        let server_created = start + 100.0;
        let client_recv = server_created + 25.0 - 84.0; // host clock 84ms behind
        let tx = server_tx(server_created, client_recv, start, 25);

        // Without correction: negative, excluded.
        let raw = single_endpoint_summary_with_offset(vec![("sigA", tx.clone())], 0.0);
        assert_eq!(raw.endpoints[0].skewed_latency_count, 1);
        assert_eq!(raw.endpoints[0].abs_p50_ms, None);

        // With +84ms offset correction: recovers ~25ms, no longer skewed.
        let corrected = single_endpoint_summary_with_offset(vec![("sigA", tx)], 84.0);
        assert_eq!(corrected.endpoints[0].skewed_latency_count, 0);
        assert_eq!(corrected.endpoints[0].abs_p50_ms, Some(25.0));
        assert_eq!(corrected.clock_offset_ms, 84.0);
    }

    // --- compute_scores ---

    #[test]
    fn single_endpoint_gets_full_win_component() {
        let mut eps = vec![EndpointSummary {
            name: "solo".into(),
            abs_p50_ms: Some(50.0),
            abs_p99_ms: Some(50.0),
            timestamp_coverage_pct: 100.0,
            valid_transactions: 100,
            ..Default::default()
        }];
        compute_scores(&mut eps, 1);
        // win 30 + latency ~25 + reliability 25 + stability ~10 + throughput 10 ~= 100
        assert!(eps[0].score > 99.0, "score was {}", eps[0].score);
    }

    #[test]
    fn score_rewards_lower_latency() {
        let base = |p50: f64| EndpointSummary {
            name: "e".into(),
            first_share: 0.5,
            abs_p50_ms: Some(p50),
            abs_p99_ms: Some(p50),
            timestamp_coverage_pct: 100.0,
            valid_transactions: 100,
            ..Default::default()
        };
        let mut fast = vec![base(50.0)];
        let mut slow = vec![base(900.0)];
        compute_scores(&mut fast, 2);
        compute_scores(&mut slow, 2);
        assert!(fast[0].score > slow[0].score);
    }
}
