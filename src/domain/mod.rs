//! Domain layer — the benchmarking model and pure logic. Depends on nothing
//! else in the crate (no gRPC, no sockets, no rendering).

pub mod analysis;
pub mod clock;
pub mod collector;
pub mod config;
pub mod scoring;
pub mod timing;
pub mod warmup;
