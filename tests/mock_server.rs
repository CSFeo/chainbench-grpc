//! Integration test against an in-process mock Yellowstone gRPC server.
//!
//! Spins up a real tonic server implementing the Geyser service over plaintext
//! HTTP/2 (exercising the `http://` no-TLS connect path), points the real
//! `YellowstoneProvider` at it, and asserts the end-to-end pipeline: stream
//! decode → account filter → signature dedup → comparator → `RunSummary`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, transport::Server};

use chainbench_grpc::domain::analysis::{RunMetadata, compute_run_summary};
use chainbench_grpc::domain::collector::Comparator;
use chainbench_grpc::domain::config::{ArgsCommitment, BenchConfig, Endpoint, EndpointKind};
use chainbench_grpc::domain::timing;
use chainbench_grpc::domain::warmup::WarmupGuard;
use chainbench_grpc::infrastructure::geyser::{ProviderContext, create_provider};
use chainbench_grpc::infrastructure::proto::geyser::{
    PingRequest, PongResponse, SubscribeUpdate, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo,
    geyser_server::{Geyser, GeyserServer},
    subscribe_update::UpdateOneof,
};
use chainbench_grpc::infrastructure::proto::solana::storage::confirmed_block::{
    Message, Transaction,
};

/// "11111111111111111111111111111111" (System Program) decodes to 32 zero bytes,
/// so the mock can emit that account without pulling in a base58 dependency.
const ACCOUNT: &str = "11111111111111111111111111111111";
const N_UPDATES: usize = 5;

struct MockGeyser;

fn unix_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn make_update(i: usize) -> SubscribeUpdate {
    // Distinct 64-byte signature per update so they dedup as separate txs.
    let mut sig = vec![0u8; 64];
    sig[0] = i as u8 + 1;
    let now = unix_millis();
    SubscribeUpdate {
        filters: vec!["account".to_string()],
        created_at: Some(prost_types::Timestamp {
            seconds: (now / 1000.0) as i64,
            nanos: ((now % 1000.0) * 1_000_000.0) as i32,
        }),
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot: 100 + i as u64,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: sig.clone(),
                is_vote: false,
                index: i as u64,
                transaction: Some(Transaction {
                    signatures: vec![sig],
                    message: Some(Message {
                        // The target account, as raw 32 bytes.
                        account_keys: vec![vec![0u8; 32]],
                        ..Default::default()
                    }),
                }),
                meta: None,
            }),
        })),
    }
}

#[tonic::async_trait]
impl Geyser for MockGeyser {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        _request: Request<
            tonic::Streaming<chainbench_grpc::infrastructure::proto::geyser::SubscribeRequest>,
        >,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let updates: Vec<Result<SubscribeUpdate, Status>> =
            (0..N_UPDATES).map(|i| Ok(make_update(i))).collect();
        // Emit the fixed batch, then stay open (pending) so the provider drives
        // its own shutdown once the transaction target is reached.
        let stream = tokio_stream::iter(updates).chain(tokio_stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse { count: 1 }))
    }

    async fn subscribe_replay_info(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::SubscribeReplayInfoRequest>,
    ) -> Result<
        Response<chainbench_grpc::infrastructure::proto::geyser::SubscribeReplayInfoResponse>,
        Status,
    > {
        unimplemented!()
    }
    async fn get_latest_blockhash(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::GetLatestBlockhashRequest>,
    ) -> Result<
        Response<chainbench_grpc::infrastructure::proto::geyser::GetLatestBlockhashResponse>,
        Status,
    > {
        unimplemented!()
    }
    async fn get_block_height(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::GetBlockHeightRequest>,
    ) -> Result<
        Response<chainbench_grpc::infrastructure::proto::geyser::GetBlockHeightResponse>,
        Status,
    > {
        unimplemented!()
    }
    async fn get_slot(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::GetSlotRequest>,
    ) -> Result<Response<chainbench_grpc::infrastructure::proto::geyser::GetSlotResponse>, Status>
    {
        unimplemented!()
    }
    async fn is_blockhash_valid(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::IsBlockhashValidRequest>,
    ) -> Result<
        Response<chainbench_grpc::infrastructure::proto::geyser::IsBlockhashValidResponse>,
        Status,
    > {
        unimplemented!()
    }
    async fn get_version(
        &self,
        _r: Request<chainbench_grpc::infrastructure::proto::geyser::GetVersionRequest>,
    ) -> Result<Response<chainbench_grpc::infrastructure::proto::geyser::GetVersionResponse>, Status>
    {
        unimplemented!()
    }
}

#[tokio::test]
async fn provider_collects_from_mock_server() {
    // Bind first so the address is live before the provider connects (no race).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(GeyserServer::new(MockGeyser))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let endpoint = Endpoint {
        name: "mock".to_string(),
        url: format!("http://{addr}"),
        x_token: None,
        kind: EndpointKind::Yellowstone,
    };
    let config = BenchConfig {
        transactions: N_UPDATES as i32,
        account: ACCOUNT.to_string(),
        commitment: ArgsCommitment::Processed,
        warmup_secs: 0,
    };

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let comparator = Arc::new(Comparator::new());
    let ctx = ProviderContext {
        shutdown_tx: shutdown_tx.clone(),
        shutdown_rx,
        start_wallclock_ms: timing::get_current_timestamp_ms(),
        start_instant: Instant::now(),
        comparator: Arc::clone(&comparator),
        warmup: Arc::new(WarmupGuard::new(0)),
        shared_counter: Arc::new(AtomicUsize::new(0)),
        shared_shutdown: Arc::new(AtomicBool::new(false)),
        target_transactions: Some(N_UPDATES),
        total_producers: 1,
        progress: None,
    };

    let provider = create_provider(&EndpointKind::Yellowstone);
    let handle = provider.process(endpoint, config, ctx);

    // Guard against a hang if the pipeline breaks.
    let stats = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
        .await
        .expect("provider timed out")
        .expect("join error")
        .expect("provider error");
    assert_eq!(stats.endpoint_name, "mock");

    let summary = compute_run_summary(
        &comparator,
        &["mock".to_string()],
        RunMetadata {
            duration_secs: 1.0,
            total_errors: 0,
            endpoint_runtime: HashMap::new(),
            clock_offset_ms: 0.0,
        },
    );

    assert!(summary.has_data, "expected collected data from mock");
    assert_eq!(summary.total_signatures, N_UPDATES);
    assert_eq!(summary.backfill_signatures, 0);

    let mock = &summary.endpoints[0];
    assert_eq!(mock.valid_transactions, N_UPDATES);
    assert_eq!(mock.first_detections, N_UPDATES); // single endpoint wins all
    // Server `created_at` was present on every update -> full coverage, abs latency set.
    assert_eq!(mock.timestamp_coverage_pct, 100.0);
    assert!(mock.abs_p50_ms.is_some());
    assert!(mock.abs_p90_ms.is_some());

    server.abort();
}
