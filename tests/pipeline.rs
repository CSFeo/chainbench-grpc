//! End-to-end test of the public library API: drive the `Comparator` the way a
//! provider would, then run the full `compute_run_summary` pipeline and assert
//! the race/latency/success-rate/score outputs. Exercises the crate as an
//! external consumer would (validating the lib surface), without a live gRPC
//! endpoint.

use std::collections::HashMap;
use std::time::Duration;

use chainbench_grpc::analysis::{EndpointRuntime, RunMetadata, compute_run_summary};
use chainbench_grpc::collector::Comparator;
use chainbench_grpc::timing::{TimestampSource, TransactionData};

const START: f64 = 1_000_000.0;

fn server_tx(latency_ms: f64, elapsed_ms: u64) -> TransactionData {
    // Real-time tx: created after START, received `latency_ms` later.
    TransactionData {
        timestamp_ms: START + 100.0,
        timestamp_source: TimestampSource::ServerCreatedAt,
        client_wallclock_ms: START + 100.0 + latency_ms,
        elapsed_since_start: Duration::from_millis(elapsed_ms),
        start_wallclock_ms: START,
    }
}

#[test]
fn two_endpoint_race_full_pipeline() {
    let comparator = Comparator::new();
    let names = vec!["fast".to_string(), "slow".to_string()];

    // 100 signatures, both endpoints see all of them. "fast" always arrives
    // ~10ms earlier (smaller elapsed) and with lower absolute latency.
    for i in 0..100u64 {
        let sig = format!("sig{i}");
        // fast: elapsed = i, latency 40ms
        comparator.record_observation("fast", &sig, server_tx(40.0, i), 2);
        // slow: elapsed = i + 10, latency 60ms
        comparator.record_observation("slow", &sig, server_tx(60.0, i + 10), 2);
    }

    let summary = compute_run_summary(
        &comparator,
        &names,
        RunMetadata {
            duration_secs: 10.0,
            total_errors: 0,
            endpoint_runtime: HashMap::from([
                (
                    "fast".to_string(),
                    EndpointRuntime {
                        reconnect_count: 1,
                        warmup_skipped: 5,
                    },
                ),
                (
                    "slow".to_string(),
                    EndpointRuntime {
                        reconnect_count: 0,
                        warmup_skipped: 3,
                    },
                ),
            ]),
        },
    );

    assert!(summary.has_data);
    assert_eq!(summary.total_signatures, 100);
    assert_eq!(summary.backfill_signatures, 0);
    assert_eq!(summary.warmup_skipped, 8); // 5 + 3 aggregated

    let fast = summary.endpoints.iter().find(|e| e.name == "fast").unwrap();
    let slow = summary.endpoints.iter().find(|e| e.name == "slow").unwrap();

    // fast wins every signature
    assert_eq!(fast.first_detections, 100);
    assert_eq!(slow.first_detections, 0);
    assert_eq!(fast.first_share, 1.0);

    // both delivered everything -> 100% success and 10 tx/s (100 / 10s)
    assert_eq!(fast.success_rate_pct, 100.0);
    assert_eq!(slow.success_rate_pct, 100.0);
    assert_eq!(fast.tx_per_sec, 10.0);

    // p90 present on both rel + abs ladders
    assert!(fast.rel_p90_ms.is_some());
    assert!(fast.abs_p90_ms.is_some());

    // fast has lower relative latency (0) and lower absolute latency (40 vs 60)
    assert_eq!(fast.rel_p50_ms, Some(0.0));
    assert!(slow.rel_p50_ms.unwrap() > 0.0);
    assert_eq!(fast.abs_p50_ms, Some(40.0));
    assert_eq!(slow.abs_p50_ms, Some(60.0));

    // runtime stats surfaced
    assert_eq!(fast.reconnect_count, 1);
    assert_eq!(slow.reconnect_count, 0);

    // fastest endpoint + higher composite score
    assert_eq!(summary.fastest_endpoint.as_deref(), Some("fast"));
    assert!(fast.score > slow.score);
}

#[test]
fn backfill_is_excluded_from_realtime_metrics() {
    let comparator = Comparator::new();
    let names = vec!["solo".to_string()];

    // One real-time signature + one historical (server-created before START).
    comparator.record_observation("solo", "live", server_tx(40.0, 100), 1);
    let mut historical = server_tx(40.0, 5);
    historical.timestamp_ms = START - 5_000.0; // created 5s before start
    comparator.record_observation("solo", "old", historical, 1);

    let summary = compute_run_summary(
        &comparator,
        &names,
        RunMetadata {
            duration_secs: 1.0,
            total_errors: 0,
            endpoint_runtime: HashMap::new(),
        },
    );

    assert_eq!(summary.total_signatures, 1);
    assert_eq!(summary.backfill_signatures, 1);
    let solo = &summary.endpoints[0];
    assert_eq!(solo.backfill_transactions, 1);
}
