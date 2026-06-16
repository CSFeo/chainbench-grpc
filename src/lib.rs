//! chainbench-grpc — Solana Yellowstone gRPC benchmarking library.
//!
//! The CLI binary is a thin wrapper over these modules. They are exposed as a
//! library so the benchmarking engine can be embedded (e.g. inside ChainBench)
//! without shelling out to the binary.
//!
//! Key entry points:
//! - [`analysis::compute_run_summary`] — turn collected observations into a [`analysis::RunSummary`]
//! - [`providers::create_provider`] — build a [`providers::GeyserProvider`] for an endpoint
//! - [`slots::run_slot_benchmark`] / [`throughput::run_throughput`] — the standalone mode pipelines
//! - [`output`] / [`html`] — render a summary to console/JSON/CSV/HTML

pub mod analysis;
pub mod clock;
pub mod collector;
pub mod config;
pub mod html;
pub mod output;
pub mod proto;
pub mod providers;
pub mod slots;
pub mod throughput;
pub mod timing;
pub mod warmup;
