use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Instant,
};

use clap::{Parser, Subcommand, ValueEnum};
use tokio::sync::broadcast;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod analysis;
mod collector;
mod config;
mod html;
mod output;
mod proto;
mod providers;
mod slots;
mod throughput;
mod timing;
mod warmup;

use collector::{Comparator, ProgressTracker};
use config::ConfigToml;
use providers::{ProviderContext, create_provider};
use warmup::WarmupGuard;

const DEFAULT_CONFIG_PATH: &str = "config.toml";

#[derive(Parser)]
#[command(
    name = "chainbench-grpc",
    about = "Comprehensive Solana gRPC benchmarking tool",
    version
)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,

    /// Path to TOML configuration file
    #[arg(long, default_value = DEFAULT_CONFIG_PATH, global = true)]
    config: String,

    /// Warmup duration in seconds (data discarded during warmup)
    #[arg(long, global = true)]
    warmup: Option<u64>,

    /// Override transaction count from config
    #[arg(long, global = true)]
    transactions: Option<i32>,

    /// Maximum test duration in seconds (safety timeout, default 300s)
    #[arg(long, default_value_t = 300, global = true)]
    max_duration: u64,

    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Console, global = true)]
    output: OutputFormat,
}

#[derive(Subcommand)]
enum Mode {
    /// Compare endpoints by transaction delivery speed (win rate + relative latency)
    Race,
    /// Measure absolute latency per endpoint (server -> client)
    Latency,
    /// Measure gRPC throughput (messages/s, bytes/s) per endpoint
    Throughput {
        /// Duration in seconds for throughput measurement
        #[arg(long, default_value_t = 60)]
        duration: u64,
    },
    /// Track slot lifecycle stages (download, replay, confirm, finalize)
    Slots {
        /// Number of finalized slots to collect
        #[arg(long, default_value_t = 100)]
        target_slots: usize,
    },
    /// Full benchmark: race + absolute latency + distribution buckets
    Full,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Console,
    Json,
    Csv,
    Html,
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let config_toml = match ConfigToml::load_or_create(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Config error: {}", e);
            eprintln!("Failed to load config: {}", e);
            if !std::path::Path::new(&cli.config).exists() {
                eprintln!("A default config.toml has been created. Edit it and re-run.");
            }
            std::process::exit(1);
        }
    };

    let mut bench_config = config_toml.config;
    let endpoints = config_toml.endpoint;

    // CLI overrides
    if let Some(warmup) = cli.warmup {
        bench_config.warmup_secs = warmup;
    }
    if let Some(txs) = cli.transactions {
        bench_config.transactions = txs;
    }

    if endpoints.is_empty() {
        eprintln!("No endpoints configured. Add at least one [[endpoint]] to config.toml.");
        std::process::exit(1);
    }

    // Handle slots mode separately
    if let Mode::Slots { target_slots } = &cli.mode {
        println!("\n  chainbench-grpc v{}", env!("CARGO_PKG_VERSION"));
        println!("  Mode: slots (lifecycle)");
        println!("  Target finalized slots: {}", target_slots);
        println!("  Max duration: {}s", cli.max_duration);
        println!("  Endpoints: {}", endpoints.len());
        for ep in &endpoints {
            println!("    - {} ({}) @ {}", ep.name, ep.kind.as_str(), ep.url);
        }
        println!();

        let result = slots::run_slot_benchmark(
            endpoints,
            bench_config,
            *target_slots,
            cli.max_duration,
        )
        .await;

        match cli.output {
            OutputFormat::Console => slots::display_slot_console(&result),
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
            }
            OutputFormat::Csv | OutputFormat::Html => {
                slots::display_slot_console(&result);
                eprintln!("  (CSV/HTML not yet implemented for slots mode)");
            }
        }
        return;
    }

    // Handle throughput mode separately (different pipeline)
    if let Mode::Throughput { duration } = &cli.mode {
        println!("\n  chainbench-grpc v{}", env!("CARGO_PKG_VERSION"));
        println!("  Mode: throughput");
        println!("  Duration: {}s", duration);
        println!("  Account: {}", bench_config.account);
        println!("  Endpoints: {}", endpoints.len());
        for ep in &endpoints {
            println!("    - {} ({}) @ {}", ep.name, ep.kind.as_str(), ep.url);
        }
        println!();

        let result = throughput::run_throughput(endpoints, bench_config, *duration).await;

        match cli.output {
            OutputFormat::Console => throughput::display_throughput_console(&result),
            OutputFormat::Json => println!("{}", throughput::output_throughput_json(&result)),
            OutputFormat::Csv => println!("{}", output::throughput_to_csv(&result)),
            OutputFormat::Html => {
                let path = "report.html";
                std::fs::write(path, html::render_throughput(&result)).expect("Failed to write HTML");
                eprintln!("  Report saved to {}", path);
            }
        }
        return;
    }

    let (show_race, show_latency) = match &cli.mode {
        Mode::Race => (true, false),
        Mode::Latency => (false, true),
        Mode::Full => (true, true),
        Mode::Throughput { .. } | Mode::Slots { .. } => unreachable!(),
    };

    let mode_name = match &cli.mode {
        Mode::Race => "race",
        Mode::Latency => "latency",
        Mode::Full => "full",
        Mode::Throughput { .. } | Mode::Slots { .. } => unreachable!(),
    };

    println!("\n  chainbench-grpc v{}", env!("CARGO_PKG_VERSION"));
    println!("  Mode: {}", mode_name);
    println!("  Account: {}", bench_config.account);
    println!("  Commitment: {}", bench_config.commitment.as_str());
    println!("  Transactions: {}", bench_config.transactions);
    println!("  Warmup: {}s", bench_config.warmup_secs);
    println!("  Max duration: {}s", cli.max_duration);
    println!("  Endpoints: {}", endpoints.len());
    for ep in &endpoints {
        println!("    - {} ({}) @ {}", ep.name, ep.kind.as_str(), ep.url);
    }
    println!();

    // Setup shared state
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let comparator = Arc::new(Comparator::new());
    let warmup_guard = Arc::new(WarmupGuard::new(bench_config.warmup_secs));
    let shared_counter = Arc::new(AtomicUsize::new(0));
    let shared_shutdown = Arc::new(AtomicBool::new(false));

    let target = if bench_config.transactions > 0 {
        Some(bench_config.transactions as usize)
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
        handles.push(provider.process(ep, bench_config.clone(), ctx));
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
    let max_duration = cli.max_duration;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(max_duration)).await;
        if !timeout_shared_shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            info!("Max duration {}s reached, forcing shutdown", max_duration);
            eprintln!(
                "\n  Warning: max duration {}s reached. Use --max-duration to increase.",
                max_duration
            );
            timeout_shared_shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = timeout_shutdown_tx.send(());
        }
    });

    // Wait for all providers and count errors
    let mut total_errors = 0usize;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
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
    let warmup_duration = bench_config.warmup_secs as f64;
    let collection_duration = test_duration.as_secs_f64() - warmup_duration;

    // Compute and display results
    let metadata = analysis::RunMetadata {
        duration_secs: collection_duration.max(0.0),
        warmup_skipped: 0, // TODO: aggregate from providers
        total_errors,
    };

    let summary = analysis::compute_run_summary(&comparator, &endpoint_names, metadata);

    match cli.output {
        OutputFormat::Console => {
            output::display_console(&summary, show_race, show_latency);
        }
        OutputFormat::Json => {
            println!("{}", output::output_json(&summary));
        }
        OutputFormat::Csv => {
            println!("{}", output::output_csv(&summary));
        }
        OutputFormat::Html => {
            let path = "report.html";
            std::fs::write(path, html::render_run_summary(&summary)).expect("Failed to write HTML");
            eprintln!("  Report saved to {}", path);
        }
    }
}
