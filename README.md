# chainbench-grpc

Comprehensive Solana gRPC (Yellowstone Geyser) benchmarking tool. Compare endpoint performance, measure latency, track slot lifecycle stages, and generate shareable reports.

Built from the ground up combining best practices from [GeyserBench](https://github.com/solstackapp/geyserbench), [Yellowstone Thorofare](https://github.com/rpcpool/yellowstone-thorofare), [Dysnix solana-test](https://github.com/dysnix/solana-test), and [Shyft grpc-latency-checker](https://github.com/Shyft-to/solana-defi).

## Features

- **5 benchmark modes**: race, latency, throughput, slots, full
- **N-endpoint comparison**: compare any number of Yellowstone gRPC providers simultaneously
- **Server-side timestamps**: uses gRPC `created_at` for precise latency measurement
- **Win rate**: which provider detects transactions first (% of first detections)
- **Absolute & relative latency**: P50/**P90**/P95/P99 from server to client
- **Latency distribution**: bucketed histogram (<400ms, 400-799ms, ... 2000ms+)
- **Per-endpoint TPS & delivery success rate**: throughput and % of signatures delivered
- **Slot lifecycle**: track 6 Solana slot stages (FirstShred, Completed, CreatedBank, Processed, Confirmed, Finalized)
- **Throughput**: messages/s, bytes/s, KB/s per endpoint
- **Composite score**: 0-100 combining win rate, latency, reliability, stability, throughput
- **Reliability metrics**: server timestamp coverage %, reconnect count, clock-skew samples
- **Warmup phase**: configurable warmup period (data discarded)
- **Backfill detection**: separates historical transactions from real-time
- **Auto-reconnection**: exponential backoff with up to 3 retries (all modes)
- **Safety timeout**: `--max-duration` prevents infinite hangs
- **4 output formats**: console tables, JSON, CSV, HTML
- **CLI-first UX**: pass endpoints directly via `--url`/`--token`, no config file required
- **Secret-safe auth**: `--token-from-env` / `--token-from-file` keep tokens out of `ps aux`

## Quick Start

### Prebuilt binaries

Tagged releases attach binaries for linux (x64/arm64) and macOS (x64/arm64) to the
[GitHub Releases](https://github.com/CSFeo/chainbench-grpc/releases) page. Download
the archive for your platform, extract, and run `./chainbench-grpc`.

### Install from source

```bash
git clone https://github.com/CSFeo/chainbench-grpc.git
cd chainbench-grpc
cargo build --release
```

Binary will be at `target/release/chainbench-grpc`.

### Requirements

- Rust 1.90+ (pinned in `rust-toolchain.toml`)
- A Solana Yellowstone gRPC endpoint with authentication token

### Run your first benchmark

```bash
# Measure absolute latency of a single endpoint
chainbench-grpc latency \
  --url https://your-grpc-endpoint.com \
  --token YOUR_X_TOKEN

# Compare two endpoints (race mode)
chainbench-grpc race \
  -u https://endpoint-a.com -t TOKEN_A \
  -u https://endpoint-b.com -t TOKEN_B

# Full benchmark with all metrics
chainbench-grpc full \
  -u https://endpoint.com -t TOKEN \
  --transactions 5000 --warmup 30
```

## Benchmark Modes

### `race` — Transaction Delivery Comparison

Compares which endpoint delivers each transaction first. Requires 2+ endpoints.

```bash
chainbench-grpc race \
  -u https://ep1.com -t token1 -n "Provider A" \
  -u https://ep2.com -t token2 -n "Provider B" \
  --transactions 5000 --warmup 30
```

**Metrics**: Win rate %, relative latency P50/P90/P95/P99, per-endpoint TPS, delivery success rate %, first detections count.

> **Limitation**: win rate and relative latency are computed only over signatures
> that *all* endpoints reported. A signature missed by any endpoint is excluded
> from those comparison metrics — but it is still counted in each endpoint's
> **delivery success rate %**, so misses remain visible. (For a long-running live
> mode this all-N gate needs to be relaxed; tracked separately.)

### `latency` — Absolute Latency Measurement

Measures end-to-end latency from server `created_at` timestamp to client receive time. Works with 1+ endpoints.

```bash
chainbench-grpc latency \
  -u https://endpoint.com -t TOKEN \
  --transactions 1000
```

**Metrics**: Absolute P50/P90/P95/P99, latency distribution buckets, timestamp coverage %, clock-skew sample count.

### `full` — Complete Benchmark

Combines race + latency. Shows all metrics including composite score.

```bash
chainbench-grpc full \
  -u https://ep1.com -t token1 \
  -u https://ep2.com -t token2 \
  --transactions 5000 --warmup 30 -o html
```

### `throughput` — Stream Throughput

Measures raw gRPC throughput: messages/s, bytes/s.

```bash
chainbench-grpc throughput \
  -u https://endpoint.com -t TOKEN \
  --duration 60
```

**Metrics**: Total messages, total bytes, msgs/s, KB/s, transaction/slot/ping breakdown.

### `slots` — Slot Lifecycle Stages

Tracks the 6 stages of Solana slot processing (inspired by [Yellowstone Thorofare](https://github.com/rpcpool/yellowstone-thorofare)):

| Stage | Transition | What it measures |
|-------|-----------|-----------------|
| Download | FirstShredReceived → Completed | Shred download time |
| Replay | CreatedBank → Processed | Block replay/execution time |
| Confirm | Processed → Confirmed | Confirmation propagation |
| Finalize | Confirmed → Finalized | Finalization time (~32 confirmations) |

```bash
chainbench-grpc slots \
  -u https://endpoint.com -t TOKEN \
  --target-slots 100
```

## CLI Reference

```
chainbench-grpc [OPTIONS] <COMMAND>

Commands:
  race        Compare endpoints by transaction delivery speed
  latency     Measure absolute latency per endpoint
  throughput  Measure gRPC throughput (messages/s, bytes/s)
  slots       Track slot lifecycle stages
  full        Full benchmark: race + latency + distribution

Options:
  -u, --url <URL>               gRPC endpoint URL (repeatable)
  -t, --token <TOKEN>           x-token authentication (pairs with --url; visible in ps aux)
      --token-from-env <VAR>    Read x-token from an env var (pairs with --url)
      --token-from-file <PATH>  Read x-token from a file, trimmed (pairs with --url)
  -n, --name <NAME>             Endpoint display name (pairs with --url)
      --account <ACCOUNT>       Solana account to monitor [default: pAMMBay...]
      --transactions <N>        Number of transactions to collect [default: 1000]
      --warmup <SECS>           Warmup duration in seconds [default: 10]
      --max-duration <SECS>     Safety timeout [default: 300]
      --commitment <LEVEL>      processed|confirmed|finalized [default: processed]
      --config <PATH>           TOML config file (alternative to --url)
  -o, --output <FORMAT>         console|json|csv|html [default: console]
  -h, --help                    Print help
  -V, --version                 Print version
```

### Endpoint configuration

**CLI flags (recommended for quick tests):**

```bash
chainbench-grpc latency -u https://grpc.example.com -t abc123
```

Multiple endpoints — repeat `-u` and `-t`:

```bash
chainbench-grpc race \
  -u https://ep1.com -t token1 -n "Provider A" \
  -u https://ep2.com -t token2 -n "Provider B"
```

**Keeping tokens out of `ps aux`** — `--token` is visible to any user who can list
processes. For shared or production hosts, read the token from the environment or
a file instead (resolved per endpoint, in `--url` order):

```bash
# from an environment variable
export EP1_TOKEN=abc123
chainbench-grpc latency -u https://grpc.example.com --token-from-env EP1_TOKEN

# from a file
chainbench-grpc latency -u https://grpc.example.com --token-from-file ./ep1.token
```

> **Scheme & TLS**: `https://` endpoints connect over TLS (system trust roots);
> `http://` endpoints connect in plaintext, for local or in-cluster endpoints.

**TOML config file (for saved/complex setups):**

```bash
chainbench-grpc full --config endpoints.toml
```

Config file format:

```toml
[config]
transactions = 5000
account = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
commitment = "processed"
warmup_secs = 30

[[endpoint]]
name = "Provider A"
url = "https://grpc-endpoint-a.com"
x_token = "your-token-a"
kind = "yellowstone"

[[endpoint]]
name = "Provider B"
url = "https://grpc-endpoint-b.com"
x_token = "your-token-b"
kind = "yellowstone"
```

## Output Formats

### Console (default)

Human-readable tables with all metrics.

### JSON (`-o json`)

Machine-readable output for automation and CI pipelines.

```bash
chainbench-grpc full -u https://ep.com -t TOKEN -o json > results.json
```

Per-endpoint shape (abridged):

```json
{
  "win_rate": 0.51,
  "relative_latency": { "p50_ms": 0.0, "p90_ms": 4.2, "p95_ms": 6.8, "p99_ms": 11.3 },
  "absolute_latency": { "p50_ms": 46.0, "p90_ms": 58.0, "p95_ms": 63.0, "p99_ms": 80.0, "source": "server_created_at" },
  "tx_per_sec": 512.4,
  "success_rate_pct": 100.0,
  "reconnect_count": 0,
  "reliability": { "server_timestamps": 1000, "client_timestamps": 0, "skewed_latency_samples": 0, "coverage_pct": 100.0 },
  "score": 92.4,
  "valid_transactions": 1000,
  "first_detections": 510,
  "backfill_transactions": 0
}
```

### CSV (`-o csv`)

Spreadsheet-friendly output. Columns:

```
endpoint,score,win_pct,rel_p50_ms,rel_p90_ms,rel_p95_ms,rel_p99_ms,
abs_p50_ms,abs_p90_ms,abs_p95_ms,abs_p99_ms,coverage_pct,tx_per_sec,
success_rate_pct,reconnects,valid_tx,first_detections,backfill
```

```bash
chainbench-grpc full -u https://ep.com -t TOKEN -o csv > results.csv
```

### HTML (`-o html`)

Standalone dark-themed HTML report saved to `report.html`. Suitable for embedding in blog posts or sharing.

```bash
chainbench-grpc full -u https://ep.com -t TOKEN -o html
# Opens report.html
```

## Composite Score

Each endpoint receives a score from 0 to 100:

| Component | Weight | What it measures |
|-----------|--------|-----------------|
| Win Rate | 30% | % of transactions detected first |
| Latency | 25% | Absolute P50 (lower = better) |
| Reliability | 25% | Server timestamp coverage % |
| Stability | 10% | P99-P50 jitter (lower = better) |
| Throughput | 10% | Transaction observation capacity |

With a single endpoint, win rate gets full 30 points (no comparison possible).

Latency, reliability, and stability use **fixed absolute thresholds** (e.g. latency
scores 25 at ≤50ms, scaling to 0 at ≥1000ms), so an endpoint's score is stable
regardless of which competitors it is run against. Only the throughput component is
relative — it measures whether an endpoint kept up with the busiest one in the run.

## How It Works

### Transaction-level comparison (race/latency/full)

1. Connects to N Yellowstone gRPC endpoints concurrently
2. Subscribes to transactions for a target Solana account (default: pump.fun / pAMMBay)
3. Configurable warmup phase — data discarded during warmup
4. Records when each endpoint delivers each transaction signature
5. Uses `created_at` server-side timestamp when available (nanosecond precision)
6. Falls back to client wallclock if server timestamp unavailable
7. Computes win rate (who was first) and latency (absolute + relative)

### Slot lifecycle (slots)

1. Subscribes to slot status updates with `interslot_updates: true`
2. Records monotonic `Instant` for each of the 6 slot stages
3. Computes stage durations and P50/P90/P99 percentiles

### Timestamp precision

The tool uses the `created_at` field from the Yellowstone gRPC `SubscribeUpdate` message, which is set server-side with nanosecond precision. This is more accurate than Solana block timestamps (second-level granularity only).

Absolute latency = `client_wallclock - server_created_at`. This includes network propagation time and any server-side queuing.

## Testing Guidelines

For statistically meaningful results:

| Use case | Transactions | Warmup | Notes |
|----------|-------------|--------|-------|
| Quick check | 200-500 | 10s | Good for smoke tests |
| Blog / report | 5,000+ | 30s | Reliable P95/P99 |
| Serious comparison | 10,000+ | 30s | Run 3-5 times, average |
| Slot lifecycle | 100+ slots | — | ~40 seconds minimum |

**Best practices:**
- Run from the same region as your endpoints for fair comparison
- Test at different times of day (network load varies)
- Use `--warmup 30` for production tests (lets connections stabilize)
- For multi-endpoint race, ensure both endpoints serve the same account data

## Library usage

The crate is split into a library (`chainbench_grpc`) and a thin CLI binary, so
the benchmarking engine can be embedded without shelling out to the binary.

```toml
# Cargo.toml
[dependencies]
chainbench-grpc = { git = "https://github.com/CSFeo/chainbench-grpc" }
```

```rust
use std::collections::HashMap;
use chainbench_grpc::analysis::{compute_run_summary, RunMetadata};
use chainbench_grpc::collector::Comparator;

let comparator = Comparator::new();
// ... drive comparator.record_observation(...) from your own collection loop ...
let summary = compute_run_summary(
    &comparator,
    &["endpoint-1".to_string()],
    RunMetadata { duration_secs: 10.0, total_errors: 0, endpoint_runtime: HashMap::new() },
);
println!("{}", chainbench_grpc::output::output_json(&summary));
```

Key entry points: [`analysis::compute_run_summary`], [`providers::create_provider`],
[`slots::run_slot_benchmark`], [`throughput::run_throughput`], and the
`output` / `html` renderers.

## Development

```bash
cargo test                              # 23 tests (unit + integration)
cargo fmt --all -- --check              # formatting
cargo clippy --all-targets -- -D warnings   # lints (zero warnings)
cargo build --release
```

Tests include unit coverage for the analysis/collector/timing logic plus two
integration tests: a public-API pipeline test and a full **mock Yellowstone gRPC
server** test (`tests/mock_server.rs`) that drives the real provider end to end.

CI (`.github/workflows/ci.yml`) runs fmt + clippy + test + release build on every
push and PR. Tagging `vX.Y.Z` triggers `release.yml`, which builds binaries for
linux x64/arm64 and macOS x64/arm64 and attaches them to a GitHub Release.

## Architecture

```
src/
├── lib.rs            # Library root — re-exports the modules below
├── main.rs           # Thin CLI (clap), mode dispatch, orchestration
├── config.rs         # TOML config + CLI flag parsing
├── timing.rs         # Server-side timestamp extraction
├── warmup.rs         # Warmup phase guard
├── collector.rs      # DashMap comparator + transaction accumulator
├── analysis.rs       # Win rate, latency, percentiles, composite score
├── output.rs         # Console tables, JSON, CSV formatters
├── html.rs           # Standalone HTML report generator
├── throughput.rs     # Throughput measurement mode
├── slots.rs          # Slot lifecycle tracking (6 stages, with reconnection)
├── proto.rs          # Generated protobuf modules
└── providers/
    ├── mod.rs                # GeyserProvider trait + factory + ProviderStats
    ├── yellowstone.rs        # Yellowstone provider (reconnection + backoff)
    └── yellowstone_client.rs # gRPC client wrapper (TLS + x-token auth)
tests/
├── pipeline.rs       # End-to-end test via the public library API
└── mock_server.rs    # Real provider driven against an in-process mock server
```

## License

Apache-2.0

## Contributing

Issues and pull requests welcome at [github.com/CSFeo/chainbench-grpc](https://github.com/CSFeo/chainbench-grpc).
