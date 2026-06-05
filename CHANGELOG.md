# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
