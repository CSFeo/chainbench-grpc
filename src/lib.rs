//! chainbench-grpc — Solana Yellowstone gRPC benchmarking library.
//!
//! The codebase is organized into layers with dependencies pointing **inward**:
//!
//! - [`domain`] — the benchmarking model and pure logic: observations, the
//!   comparator, latency/percentile/scoring analysis, the clock-offset formula,
//!   and configuration value objects. Depends on nothing else in the crate.
//! - [`application`] — use-case pipelines that orchestrate a run (race/latency/
//!   full, slots, throughput). Depends on `domain` and `infrastructure`.
//! - [`infrastructure`] — I/O adapters: the Yellowstone gRPC provider/client,
//!   the generated protobuf, the SNTP clock probe, and TOML config loading.
//!   Depends on `domain`.
//! - [`presentation`] — renderers (console/JSON/CSV/HTML). Depends on `domain`
//!   and `application` result types.
//!
//! The binary (`main.rs`) is the composition root: it parses the CLI, wires the
//! layers together, and selects a renderer.
//!
//! ## Ubiquitous language
//! - **Endpoint** — a gRPC provider under test.
//! - **Observation** — one endpoint receiving one transaction signature, with
//!   server (`created_at`) and client timestamps ([`domain::timing`]).
//! - **Comparator** — aggregates observations per signature across endpoints
//!   ([`domain::collector`]).
//! - **Run / Summary** — a completed benchmark and its computed metrics
//!   ([`domain::analysis`]).
//! - **Provider** — an adapter that streams observations from an endpoint
//!   ([`infrastructure::geyser`]).
//! - **Clock offset** — host-vs-UTC skew used to correct absolute latency
//!   ([`domain::clock`], probed by [`infrastructure::sntp`]).

pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod presentation;
