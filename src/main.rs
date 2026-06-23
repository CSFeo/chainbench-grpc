use clap::{Parser, Subcommand, ValueEnum};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use chainbench_grpc::application::run::{ComparisonRun, run_comparison};
use chainbench_grpc::application::{slots, throughput};
use chainbench_grpc::domain::config;
use chainbench_grpc::infrastructure::config_file::ConfigToml;
use chainbench_grpc::infrastructure::sntp;
use chainbench_grpc::presentation::{html, output};

const DEFAULT_CONFIG_PATH: &str = "config.toml";

#[derive(Parser)]
#[command(
    name = "chainbench-grpc",
    about = "Solana gRPC benchmarking tool",
    version,
    after_help = "EXAMPLES:\n  \
      chainbench-grpc latency --url https://grpc.example.com --token abc123\n  \
      chainbench-grpc race --url https://ep1.com --token t1 --url https://ep2.com --token t2\n  \
      chainbench-grpc full --config endpoints.toml --transactions 5000\n  \
      chainbench-grpc throughput --url https://grpc.example.com --duration 60\n  \
      chainbench-grpc slots --url https://grpc.example.com --target-slots 100"
)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,

    /// gRPC endpoint URL (can be repeated for multi-endpoint comparison)
    #[arg(short = 'u', long = "url", global = true)]
    urls: Vec<String>,

    /// x-token for authentication (pairs with --url in order).
    /// WARNING: visible in `ps aux`; prefer --token-from-env / --token-from-file.
    #[arg(short = 't', long = "token", global = true)]
    tokens: Vec<String>,

    /// Read x-token from an environment variable (pairs with --url in order)
    #[arg(long = "token-from-env", global = true)]
    tokens_from_env: Vec<String>,

    /// Read x-token from a file, trimmed (pairs with --url in order)
    #[arg(long = "token-from-file", global = true)]
    tokens_from_file: Vec<String>,

    /// Endpoint name (pairs with --url in order; auto-generated if omitted)
    #[arg(short = 'n', long = "name", global = true)]
    names: Vec<String>,

    /// Solana account to monitor (default: pAMMBay/pump.fun)
    #[arg(
        long,
        global = true,
        default_value = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
    )]
    account: String,

    /// Number of transactions to collect
    #[arg(long, global = true, default_value_t = 1000)]
    transactions: i32,

    /// Warmup duration in seconds (data discarded during warmup)
    #[arg(long, global = true, default_value_t = 10)]
    warmup: u64,

    /// Maximum test duration in seconds (safety timeout)
    #[arg(long, global = true, default_value_t = 300)]
    max_duration: u64,

    /// Commitment level: processed, confirmed, finalized
    #[arg(long, global = true, default_value = "processed")]
    commitment: String,

    /// Manually set the client clock offset in ms (positive = local clock behind
    /// UTC). Added to client wallclock before computing absolute latency. Skips
    /// the automatic NTP probe.
    #[arg(long, global = true)]
    clock_offset_ms: Option<f64>,

    /// Disable clock-offset correction entirely (report raw absolute latency).
    #[arg(long, global = true, default_value_t = false)]
    no_clock_correction: bool,

    /// Path to TOML config file (alternative to --url flags)
    #[arg(long, global = true)]
    config: Option<String>,

    /// Output format
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Console, global = true)]
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

/// Resolve the x-token for endpoint `i` by priority: explicit `--token`, then
/// `--token-from-env` (read the named env var), then `--token-from-file` (read
/// and trim the file). Exits with a clear error if a referenced source is
/// missing, so tokens never silently fall back to empty.
fn resolve_token(cli: &Cli, i: usize) -> Option<String> {
    if let Some(t) = cli.tokens.get(i) {
        return Some(t.clone());
    }
    if let Some(var) = cli.tokens_from_env.get(i) {
        match std::env::var(var) {
            Ok(v) => return Some(v),
            Err(_) => {
                eprintln!(
                    "Error: --token-from-env '{}' is not set in the environment",
                    var
                );
                std::process::exit(1);
            }
        }
    }
    if let Some(path) = cli.tokens_from_file.get(i) {
        match std::fs::read_to_string(path) {
            Ok(v) => return Some(v.trim().to_string()),
            Err(e) => {
                eprintln!(
                    "Error: --token-from-file '{}' could not be read: {}",
                    path, e
                );
                std::process::exit(1);
            }
        }
    }
    None
}

/// Resolve the clock offset (ms, local-behind-UTC) used to correct absolute
/// latency: explicit `--no-clock-correction` wins, then `--clock-offset-ms`,
/// otherwise an automatic NTP probe (graceful fallback to 0 if it fails).
/// Returns (offset_ms, source).
async fn resolve_clock_offset(cli: &Cli) -> (f64, &'static str) {
    if cli.no_clock_correction {
        return (0.0, "disabled");
    }
    if let Some(off) = cli.clock_offset_ms {
        return (off, "manual");
    }
    match tokio::task::spawn_blocking(|| sntp::measure_clock_offset(sntp::DEFAULT_NTP_SERVERS))
        .await
    {
        Ok(Some(c)) => {
            info!(
                offset_ms = c.offset_ms,
                rtt_ms = c.rtt_ms,
                server = %c.server,
                "Measured clock offset via NTP"
            );
            (c.offset_ms, "ntp")
        }
        _ => {
            error!("NTP clock probe failed; absolute latency will be uncorrected");
            (0.0, "unavailable")
        }
    }
}

/// Write an HTML report to `report.html`, printing a clean error and exiting
/// non-zero on failure instead of panicking.
fn write_html_report(contents: String) {
    let path = "report.html";
    match std::fs::write(path, contents) {
        Ok(()) => eprintln!("  Report saved to {}", path),
        Err(e) => {
            eprintln!("  Error: failed to write {}: {}", path, e);
            std::process::exit(1);
        }
    }
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

    // Build config: either from --url flags or from config file
    let (bench_config, endpoints) = if !cli.urls.is_empty() {
        // CLI mode: build endpoints from --url/--token/--name flags
        let endpoints: Vec<config::Endpoint> = cli
            .urls
            .iter()
            .enumerate()
            .map(|(i, url)| {
                let token = resolve_token(&cli, i);
                let name = cli
                    .names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("endpoint-{}", i + 1));
                config::Endpoint {
                    name,
                    url: url.clone(),
                    x_token: token,
                    kind: config::EndpointKind::Yellowstone,
                }
            })
            .collect();

        let commitment = match cli.commitment.as_str() {
            "confirmed" => config::ArgsCommitment::Confirmed,
            "finalized" => config::ArgsCommitment::Finalized,
            _ => config::ArgsCommitment::Processed,
        };

        let bench = config::BenchConfig {
            transactions: cli.transactions,
            account: cli.account.clone(),
            commitment,
            warmup_secs: cli.warmup,
        };
        (bench, endpoints)
    } else if let Some(config_path) = &cli.config {
        // Config file mode
        match ConfigToml::load(config_path) {
            Ok(c) => (c.config, c.endpoint),
            Err(e) => {
                eprintln!("Failed to load config {}: {}", config_path, e);
                std::process::exit(1);
            }
        }
    } else if std::path::Path::new(DEFAULT_CONFIG_PATH).exists() {
        // Auto-detect config.toml in current directory
        match ConfigToml::load(DEFAULT_CONFIG_PATH) {
            Ok(c) => (c.config, c.endpoint),
            Err(e) => {
                eprintln!("Failed to load {}: {}", DEFAULT_CONFIG_PATH, e);
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("No endpoints specified. Use --url or --config.\n");
        eprintln!("Examples:");
        eprintln!("  chainbench-grpc latency --url https://grpc.example.com --token YOUR_TOKEN");
        eprintln!(
            "  chainbench-grpc race --url https://ep1.com --token t1 --url https://ep2.com --token t2"
        );
        eprintln!("  chainbench-grpc full --config endpoints.toml");
        std::process::exit(1);
    };

    if endpoints.is_empty() {
        eprintln!("No endpoints configured.");
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

        let result =
            slots::run_slot_benchmark(endpoints, bench_config, *target_slots, cli.max_duration)
                .await;

        match cli.output {
            OutputFormat::Console => output::display_slot_console(&result),
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&result).unwrap_or_default()
                );
            }
            OutputFormat::Csv => {
                output::display_slot_console(&result);
            }
            OutputFormat::Html => {
                write_html_report(html::render_slots(&result));
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
            OutputFormat::Console => output::display_throughput_console(&result),
            OutputFormat::Json => println!("{}", output::output_throughput_json(&result)),
            OutputFormat::Csv => println!("{}", output::throughput_to_csv(&result)),
            OutputFormat::Html => {
                write_html_report(html::render_throughput(&result));
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
    println!("  Timeout: {}s", cli.max_duration);
    println!("  Endpoints: {}", endpoints.len());
    for ep in &endpoints {
        println!("    - {} ({}) @ {}", ep.name, ep.kind.as_str(), ep.url);
    }

    // Resolve the clock offset used to correct absolute latency.
    let (clock_offset_ms, offset_source) = resolve_clock_offset(&cli).await;
    match offset_source {
        "disabled" => println!("  Clock correction: disabled (raw absolute latency)"),
        "manual" => println!("  Clock offset: {:+.1}ms (manual)", clock_offset_ms),
        "ntp" => {
            println!("  Clock offset: {:+.1}ms (NTP)", clock_offset_ms);
            if clock_offset_ms.abs() > 5.0 {
                println!(
                    "    note: host clock is {:+.1}ms off UTC — absolute latency corrected",
                    clock_offset_ms
                );
            }
        }
        _ => println!(
            "  Clock offset: unavailable (NTP probe failed) — absolute latency uncorrected"
        ),
    }
    println!();

    let summary = run_comparison(ComparisonRun {
        endpoints,
        config: bench_config,
        max_duration_secs: cli.max_duration,
        clock_offset_ms,
    })
    .await;

    match cli.output {
        OutputFormat::Console => output::display_console(&summary, show_race, show_latency),
        OutputFormat::Json => println!("{}", output::output_json(&summary)),
        OutputFormat::Csv => println!("{}", output::output_csv(&summary)),
        OutputFormat::Html => write_html_report(html::render_run_summary(&summary)),
    }
}
