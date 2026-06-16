use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::Serialize;
use tokio::sync::broadcast;
use tonic::transport::ClientTlsConfig;
use tracing::{error, info};

use crate::domain::config::{BenchConfig, Endpoint};
use crate::infrastructure::geyser::client::GeyserGrpcClient;
use crate::infrastructure::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

#[derive(Debug, Clone, Serialize)]
pub struct ThroughputResult {
    pub endpoint: String,
    pub duration_secs: f64,
    pub total_messages: usize,
    pub total_bytes: usize,
    pub messages_per_sec: f64,
    pub bytes_per_sec: f64,
    pub transactions: usize,
    pub slots: usize,
    pub pings: usize,
    pub other: usize,
    pub errors: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThroughputSummary {
    pub results: Vec<ThroughputResult>,
}

pub async fn run_throughput(
    endpoints: Vec<Endpoint>,
    config: BenchConfig,
    duration_secs: u64,
) -> ThroughputSummary {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let running = Arc::new(AtomicBool::new(true));

    // Ctrl+C handler
    let ctrl_running = Arc::clone(&running);
    let ctrl_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Ctrl+C received, stopping throughput test...");
            ctrl_running.store(false, Ordering::SeqCst);
            let _ = ctrl_tx.send(());
        }
    });

    // Timer
    let timer_running = Arc::clone(&running);
    let timer_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(duration_secs)).await;
        timer_running.store(false, Ordering::SeqCst);
        let _ = timer_tx.send(());
    });

    let mut handles = Vec::new();

    for ep in endpoints {
        let config = config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let running = Arc::clone(&running);

        handles.push(tokio::spawn(async move {
            measure_endpoint(ep, config, &mut shutdown_rx, running).await
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => error!("Throughput task error: {}", e),
            Err(e) => error!("Throughput task panicked: {}", e),
        }
    }

    ThroughputSummary { results }
}

async fn measure_endpoint(
    endpoint: Endpoint,
    config: BenchConfig,
    shutdown_rx: &mut broadcast::Receiver<()>,
    running: Arc<AtomicBool>,
) -> Result<ThroughputResult, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint_name = endpoint.name.clone();
    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint.x_token.clone().filter(|t| !t.trim().is_empty());

    info!(endpoint = %endpoint_name, "Throughput: connecting");

    let mut builder = GeyserGrpcClient::build_from_shared(endpoint_url.clone())?;
    if let Some(token) = endpoint_token {
        builder = builder.x_token(Some(token))?;
    }
    if endpoint_url.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    let mut client = builder.connect().await?;

    info!(endpoint = %endpoint_name, "Throughput: connected, subscribing to all");

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: CommitmentLevel = config.commitment.into();

    // Subscribe to transactions for the target account
    let mut transactions = HashMap::new();
    transactions.insert(
        "account".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: vec![config.account.clone()],
            account_exclude: vec![],
            account_required: vec![],
            ..Default::default()
        },
    );

    subscribe_tx
        .send(SubscribeRequest {
            slots: HashMap::default(),
            accounts: HashMap::default(),
            transactions,
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        })
        .await?;

    let start = Instant::now();
    let total_messages = AtomicUsize::new(0);
    let total_bytes = AtomicUsize::new(0);
    let mut tx_count = 0usize;
    let mut slot_count = 0usize;
    let mut ping_count = 0usize;
    let mut other_count = 0usize;
    let mut error_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Throughput: stop signal received");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        let msg_size = prost::Message::encoded_len(&msg);
                        total_messages.fetch_add(1, Ordering::Relaxed);
                        total_bytes.fetch_add(msg_size, Ordering::Relaxed);

                        match &msg.update_oneof {
                            Some(UpdateOneof::Transaction(_)) => tx_count += 1,
                            Some(UpdateOneof::Slot(_)) => slot_count += 1,
                            Some(UpdateOneof::Ping(_)) => {
                                ping_count += 1;
                                let _ = subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await;
                            }
                            _ => other_count += 1,
                        }
                    }
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Throughput: stream error");
                        error_count += 1;
                        break;
                    }
                    None => {
                        info!(endpoint = %endpoint_name, "Throughput: stream closed");
                        break;
                    }
                }
            }
        }

        if !running.load(Ordering::Relaxed) {
            break;
        }
    }

    let duration = start.elapsed();
    let duration_secs = duration.as_secs_f64();
    let msgs = total_messages.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);

    let result = ThroughputResult {
        endpoint: endpoint_name.clone(),
        duration_secs,
        total_messages: msgs,
        total_bytes: bytes,
        messages_per_sec: if duration_secs > 0.0 {
            msgs as f64 / duration_secs
        } else {
            0.0
        },
        bytes_per_sec: if duration_secs > 0.0 {
            bytes as f64 / duration_secs
        } else {
            0.0
        },
        transactions: tx_count,
        slots: slot_count,
        pings: ping_count,
        other: other_count,
        errors: error_count,
    };

    info!(
        endpoint = %endpoint_name,
        duration_secs = format!("{:.1}", duration_secs),
        total_messages = msgs,
        total_bytes = bytes,
        msgs_per_sec = format!("{:.1}", result.messages_per_sec),
        "Throughput: finished"
    );

    Ok(result)
}
