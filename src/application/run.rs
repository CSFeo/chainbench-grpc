//! Race / latency / full pipeline: subscribe to all endpoints concurrently,
//! collect observations into the comparator until the transaction target is hit
//! (or Ctrl+C / max-duration), then compute the run summary.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Instant;

use tokio::sync::broadcast;
use tracing::{error, info};

use crate::domain::analysis::{self, EndpointRuntime, RunMetadata, RunSummary};
use crate::domain::collector::{Comparator, ProgressTracker};
use crate::domain::config::{BenchConfig, Endpoint};
use crate::domain::timing;
use crate::domain::warmup::WarmupGuard;
use crate::infrastructure::geyser::{ProviderContext, create_provider};

/// Inputs for a comparison run.
pub struct ComparisonRun {
    pub endpoints: Vec<Endpoint>,
    pub config: BenchConfig,
    pub max_duration_secs: u64,
    /// Clock offset (ms, local-behind-UTC) to apply to absolute latency.
    pub clock_offset_ms: f64,
}

pub async fn run_comparison(run: ComparisonRun) -> RunSummary {
    let ComparisonRun {
        endpoints,
        config,
        max_duration_secs,
        clock_offset_ms,
    } = run;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let comparator = Arc::new(Comparator::new());
    let warmup_guard = Arc::new(WarmupGuard::new(config.warmup_secs));
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));

    let target = if config.transactions > 0 {
        Some(config.transactions as usize)
    } else {
        None
    };
    let progress = target.map(|t| Arc::new(ProgressTracker::new(t)));
    let total_producers = endpoints.len();

    let start_wallclock_ms = timing::get_current_timestamp_ms();
    let start_instant = Instant::now();

    // Spawn provider tasks
    let mut handles = Vec::new();
    let endpoint_names: Vec<String> = endpoints.iter().map(|e| e.name.clone()).collect();

    for ep in endpoints {
        let provider = create_provider(&ep.kind);
        let ctx = ProviderContext {
            shutdown_tx: shutdown_tx.clone(),
            shutdown_rx: shutdown_tx.subscribe(),
            start_wallclock_ms,
            start_instant,
            comparator: Arc::clone(&comparator),
            warmup: Arc::clone(&warmup_guard),
            shared_counter: Arc::clone(&shared_counter),
            shared_shutdown: Arc::clone(&shared_shutdown),
            target_transactions: target,
            total_producers,
            progress: progress.clone(),
        };
        handles.push(provider.process(ep, config.clone(), ctx));
    }

    // Ctrl+C handler
    let ctrl_shutdown_tx = shutdown_tx.clone();
    let ctrl_shared_shutdown = Arc::clone(&shared_shutdown);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Ctrl+C received, shutting down...");
            ctrl_shared_shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = ctrl_shutdown_tx.send(());
        }
    });

    // Safety timeout
    let timeout_shutdown_tx = shutdown_tx.clone();
    let timeout_shared_shutdown = Arc::clone(&shared_shutdown);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(max_duration_secs)).await;
        if !timeout_shared_shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            info!(
                "Max duration {}s reached, forcing shutdown",
                max_duration_secs
            );
            eprintln!(
                "\n  Warning: max duration {}s reached. Use --max-duration to increase.",
                max_duration_secs
            );
            timeout_shared_shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = timeout_shutdown_tx.send(());
        }
    });

    // Wait for all providers, collect per-endpoint runtime stats, count errors
    let mut total_errors = 0usize;
    let mut endpoint_runtime: HashMap<String, EndpointRuntime> = HashMap::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(stats)) => {
                endpoint_runtime.insert(
                    stats.endpoint_name.clone(),
                    EndpointRuntime {
                        reconnect_count: stats.reconnect_count,
                        warmup_skipped: stats.warmup_skipped,
                    },
                );
            }
            Ok(Err(e)) => {
                error!("Provider error: {}", e);
                total_errors += 1;
            }
            Err(e) => {
                error!("Provider task panicked: {}", e);
                total_errors += 1;
            }
        }
    }

    let test_duration = start_instant.elapsed();
    let collection_duration = test_duration.as_secs_f64() - config.warmup_secs as f64;

    let metadata = RunMetadata {
        duration_secs: collection_duration.max(0.0),
        total_errors,
        endpoint_runtime,
        clock_offset_ms,
    };

    analysis::compute_run_summary(&comparator, &endpoint_names, metadata)
}
