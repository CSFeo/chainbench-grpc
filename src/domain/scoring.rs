//! Composite scoring — collapses an endpoint's metrics into a single 0–100
//! number for ranking.

use crate::domain::analysis::EndpointSummary;

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
pub fn compute_scores(endpoints: &mut [EndpointSummary], num_endpoints: usize) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
