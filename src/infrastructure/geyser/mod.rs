use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
    time::Instant,
};
use tokio::sync::broadcast;

use crate::domain::collector::{Comparator, ProgressTracker};
use crate::domain::config::{ArgsCommitment, BenchConfig, Endpoint, EndpointKind};
use crate::domain::warmup::WarmupGuard;
use crate::infrastructure::proto::geyser::CommitmentLevel;

pub mod client;
pub mod yellowstone;

/// Adapt the domain commitment value object to the gRPC enum. Lives in the
/// infrastructure layer because `CommitmentLevel` is generated protobuf.
impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

/// Per-endpoint runtime stats returned by a provider task when it finishes.
#[derive(Debug, Clone, Default)]
pub struct ProviderStats {
    pub endpoint_name: String,
    pub warmup_skipped: usize,
    pub reconnect_count: u32,
}

pub trait GeyserProvider: Send + Sync {
    fn process(
        &self,
        endpoint: Endpoint,
        config: BenchConfig,
        context: ProviderContext,
    ) -> tokio::task::JoinHandle<Result<ProviderStats, Box<dyn Error + Send + Sync>>>;
}

pub fn create_provider(kind: &EndpointKind) -> Box<dyn GeyserProvider> {
    match kind {
        EndpointKind::Yellowstone => Box::new(yellowstone::YellowstoneProvider),
        other => {
            eprintln!(
                "Provider '{}' is not yet implemented. Only 'yellowstone' is supported in this version.",
                other.as_str()
            );
            std::process::exit(1);
        }
    }
}

pub struct ProviderContext {
    pub shutdown_tx: broadcast::Sender<()>,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub start_wallclock_ms: f64,
    pub start_instant: Instant,
    pub comparator: Arc<Comparator>,
    pub warmup: Arc<WarmupGuard>,
    pub shared_counter: Arc<AtomicUsize>,
    pub shared_shutdown: Arc<AtomicBool>,
    pub target_transactions: Option<usize>,
    pub total_producers: usize,
    pub progress: Option<Arc<ProgressTracker>>,
}
