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
    delays_ms: Vec<f64>,
    absolute_latencies_ms: Vec<f64>,
    buckets: LatencyBuckets,
    backfill_transactions: usize,
    server_timestamp_count: usize,
    client_timestamp_count: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct EndpointSummary {
    pub name: String,
    // Relative metrics (win rate)
    pub first_share: f64,
    pub rel_p50_ms: Option<f64>,
    pub rel_p95_ms: Option<f64>,
    pub rel_p99_ms: Option<f64>,
    // Absolute latency metrics
    pub abs_p50_ms: Option<f64>,
    pub abs_p95_ms: Option<f64>,
    pub abs_p99_ms: Option<f64>,
    pub buckets: LatencyBuckets,
    // Reliability
    pub server_timestamp_count: usize,
    pub client_timestamp_count: usize,
    pub timestamp_coverage_pct: f64,
    // Counts
    pub valid_transactions: usize,
    pub first_detections: usize,
    pub backfill_transactions: usize,
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
}

pub struct RunMetadata {
    pub duration_secs: f64,
    pub warmup_skipped: usize,
    pub total_errors: usize,
}

pub fn compute_run_summary(
    comparator: &Comparator,
    endpoint_names: &[String],
    metadata: RunMetadata,
) -> RunSummary {
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();
    let expected_producers = endpoint_names.len();
    let mut total_signatures = 0usize;
    let mut backfill_signatures = 0usize;

    for name in endpoint_names {
        endpoint_stats.insert(name.clone(), EndpointStats::default());
    }

    for sig_entry in comparator.iter() {
        let sig_data = sig_entry.value();

        // For multi-endpoint: skip partial observations
        if expected_producers > 1 && sig_data.len() != expected_producers {
            continue;
        }

        // For single-endpoint: accept all observations
        if expected_producers == 1 && sig_data.is_empty() {
            continue;
        }

        let is_historical = sig_data
            .values()
            .any(|tx| tx.client_wallclock_ms < tx.start_wallclock_ms);

        if is_historical {
            backfill_signatures += 1;
            for endpoint in sig_data.keys() {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.backfill_transactions += 1;
                }
            }
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
                    let abs_latency = tx.client_wallclock_ms - tx.timestamp_ms;
                    if abs_latency >= 0.0 {
                        stats.absolute_latencies_ms.push(abs_latency);
                        stats.buckets.record(abs_latency);
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
        .map(|(name, stats)| build_summary(name, stats, total_signatures))
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

    RunSummary {
        endpoints,
        fastest_endpoint,
        has_data,
        total_signatures,
        backfill_signatures,
        test_duration_secs: metadata.duration_secs,
        throughput_tx_per_sec: throughput,
        warmup_skipped: metadata.warmup_skipped,
        total_errors: metadata.total_errors,
    }
}

fn build_summary(name: String, stats: EndpointStats, total_signatures: usize) -> EndpointSummary {
    let total_ts = stats.server_timestamp_count + stats.client_timestamp_count;
    let coverage = if total_ts > 0 {
        stats.server_timestamp_count as f64 / total_ts as f64 * 100.0
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
        timestamp_coverage_pct: coverage,
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
        summary.rel_p95_ms = Some(percentile(&sorted, 0.95));
        summary.rel_p99_ms = Some(percentile(&sorted, 0.99));
    }

    if !stats.absolute_latencies_ms.is_empty() {
        let mut sorted = stats.absolute_latencies_ms;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        summary.abs_p50_ms = Some(percentile(&sorted, 0.50));
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
/// Formula:
///   win_rate_component  = first_share * 30          (30% weight)
///   latency_component   = latency_score * 25        (25% weight, lower P50 = better)
///   reliability_component = coverage_pct / 100 * 25 (25% weight)
///   stability_component = stability * 10            (10% weight, low P99-P50 jitter)
///   throughput_component = min(observations / expected, 1.0) * 10 (10% weight)
fn compute_scores(endpoints: &mut [EndpointSummary], num_endpoints: usize) {
    // Find the best absolute P50 across all endpoints for normalization
    let best_abs_p50 = endpoints
        .iter()
        .filter_map(|e| e.abs_p50_ms)
        .fold(f64::MAX, f64::min);

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
