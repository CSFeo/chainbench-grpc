# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-06-15

Layered (DDD-style) architecture refactor. **No behavioral change** — all 28
tests pass unchanged — but the internal structure and the public module paths
moved, so this is a breaking change for library consumers.

### Changed
- Reorganized `src/` into four layers with dependencies pointing inward:
  `domain` (model + pure logic), `application` (use-case pipelines),
  `infrastructure` (gRPC/NTP/TOML adapters), `presentation` (renderers). See
  the new [ARCHITECTURE.md](ARCHITECTURE.md).
- **Public API paths moved** for library users, e.g. `chainbench_grpc::analysis`
  → `chainbench_grpc::domain::analysis`, `chainbench_grpc::providers` →
  `chainbench_grpc::infrastructure::geyser`, `output`/`html` →
  `chainbench_grpc::presentation::*`.
- Split mixed modules onto layer boundaries: clock math (`domain::clock`) vs NTP
  probe (`infrastructure::sntp`); config value objects (`domain::config`) vs TOML
  loading (`infrastructure::config_file`); the protobuf `created_at` extraction
  and commitment conversion moved into `infrastructure::geyser` so `domain` no
  longer depends on generated protobuf; race/latency/full orchestration extracted
  from `main` into `application::run`; slot/throughput console renderers moved to
  `presentation::output`.

### Removed (dead code)
- `BenchConfig.duration_secs` (written, never read).
- Unused dependencies `chrono` and a duplicate `tokio-stream` (kept as a
  dev-dependency for tests).
- Dead `GeyserGrpcClient::subscribe_with_request` branch + the orphaned
  `SubscribeSendError` error variant.

## [0.3.0] - 2026-06-15

Clock-offset correction for absolute latency (TS1 clock-skew design, Tier 1).

### Added
- **NTP clock-offset correction.** Absolute latency now corrects the client
  wallclock by the host's measured offset vs UTC: `corrected = (client + offset) − server_created_at`.
  By default race/latency/full probe NTP at startup (`time.cloudflare.com`,
  `time.google.com`, `pool.ntp.org`; lowest-RTT estimate); graceful fallback to
  uncorrected if UDP/123 is blocked. New `src/clock.rs` — pure-`std` SNTP client.
- Flags: `--clock-offset-ms <f64>` (manual offset, skips the probe) and
  `--no-clock-correction` (report raw absolute latency).
- Applied offset is shown in the run header, console summary, and JSON
  (`clock_offset_ms`).
- 5 new tests (SNTP offset/delay math, NTP timestamp parsing, and an end-to-end
  correction test reproducing the live clock-skew finding). Suite now 28 tests.

### Why
A live run surfaced all-negative absolute latencies: the measuring host's clock
was ~84 ms behind UTC while one-way network latency was ~25 ms. The TS1 guard
correctly flagged and excluded the skewed samples; this release lets the tool
*correct* the offset and recover a usable absolute number when the host clock is
not perfectly synced. Relative/race, throughput, and slots were never affected.
For published competitor numbers, still run from an NTP-disciplined, colocated host.

## [0.2.0] - 2026-06-04

First production-hardening pass (TS1). Builds clean, `clippy -D warnings` clean,
`cargo fmt --check` clean, with the first unit-test suite in place.

### Added
- **Library + thin binary**: logic now lives in `lib.rs` (crate `chainbench_grpc`),
  with `main.rs` reduced to a CLI wrapper, so the engine can be embedded without
  shelling out to the binary.
- **Plaintext (`http://`) endpoints**: TLS is now applied only for `https://` URLs,
  enabling local / in-cluster endpoints and integration tests.
- **Test suite expanded to 23**: unit tests for `collector` (emit-once, earliest-wins)
  and `timing` (server-timestamp precision, source selection), plus integration tests —
  a public-API pipeline test and a full **mock Yellowstone gRPC server** test that
  drives the real provider end to end.
- **P90 percentiles** for both relative and absolute latency, alongside P50/P95/P99
  (RPC Fast weights P90 4× over the average in its scoring).
- **Per-endpoint TPS** in `race` / `full` modes (previously only in `throughput`).
- **Per-endpoint delivery success rate %** — fraction of non-backfill signatures
  (seen by any endpoint) that this endpoint delivered. Surfaces silent misses
  that the all-N comparison gate would otherwise hide.
- **Reconnect count** is now reported per endpoint (console, JSON, CSV, HTML).
- **Clock-skew reporting**: server-stamped samples with negative absolute latency
  are now counted (`skewed_latency_count`) instead of being silently dropped.
- `--token-from-env VAR` and `--token-from-file PATH` so tokens no longer have to
  be passed via `--token` (which is visible in `ps aux`).
- Unit tests for percentile math, latency buckets, backfill detection, clock-skew
  handling, success-rate, and composite scoring.
- GitHub Actions: `ci.yml` (fmt + clippy + test + release build) and `release.yml`
  (multi-platform binaries on tag: linux x64/arm64, macOS x64/arm64).

### Fixed
- **Backfill detection** now compares the server `created_at` timestamp to the
  benchmark start. The previous check compared client receive time to start,
  which is structurally always false — so backfill was never detected.
- **Composite score** no longer carries dead normalization code; the doc-comment
  now accurately describes the fixed-threshold model (stable across competitor
  sets, unlike a relative-to-best score).
- HTML report writes return a clean error and non-zero exit instead of panicking
  (`.expect` removed from the three write sites).
- `warmup_skipped` is now aggregated from providers instead of hardcoded to 0.
- Reconnect paths for subscribe/send failures now back off exponentially instead
  of busy-looping; `slots` mode now reconnects on stream drops like the tx pipeline.

### Known limitations
- Multi-endpoint win-rate and relative latency require all N producers to report a
  signature; partials are excluded from those metrics (but counted in success rate).
- `Comparator` storage is unbounded — fine for one-shot runs, needs TTL eviction
  before a long-running `serve` mode.
