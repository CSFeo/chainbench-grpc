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
mod output;
mod proto;
mod providers;
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
    /// Full benchmark: race + absolute latency + distribution buckets
    Full,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Console,
    Json,
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

    let (show_race, show_latency) = match &cli.mode {
        Mode::Race => (true, false),
        Mode::Latency => (false, true),
        Mode::Full => (true, true),
    };

    let mode_name = match &cli.mode {
        Mode::Race => "race",
        Mode::Latency => "latency",
        Mode::Full => "full",
    };

    println!("\n  chainbench-grpc v{}", env!("CARGO_PKG_VERSION"));
    println!("  Mode: {}", mode_name);
    println!("  Account: {}", bench_config.account);
    println!("  Commitment: {}", bench_config.commitment.as_str());
    println!("  Transactions: {}", bench_config.transactions);
    println!("  Warmup: {}s", bench_config.warmup_secs);
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

    // Wait for all providers
    for handle in handles {
        let _ = handle.await;
    }

    // Compute and display results
    let summary = analysis::compute_run_summary(&comparator, &endpoint_names);

    match cli.output {
        OutputFormat::Console => {
            output::display_console(&summary, show_race, show_latency);
        }
        OutputFormat::Json => {
            let json = output::output_json(&summary);
            println!("{}", json);
        }
    }
}
