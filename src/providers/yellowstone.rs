use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use solana_pubkey::Pubkey;
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{error, info, warn};

use crate::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

use crate::{
    collector::TransactionAccumulator,
    config::{BenchConfig, Endpoint},
    timing,
};

use super::{GeyserProvider, ProviderContext, yellowstone_client::GeyserGrpcClient};

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: BenchConfig,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_yellowstone_endpoint(endpoint, config, context).await })
    }
}

fn fatal_connection_error(endpoint: &str, err: impl std::fmt::Display) -> ! {
    error!(endpoint = endpoint, error = %err, "Failed to connect to endpoint");
    eprintln!("Failed to connect to endpoint {}: {}", endpoint, err);
    std::process::exit(1);
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: BenchConfig,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_ms,
        start_instant,
        comparator,
        warmup,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();
    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint
        .x_token
        .clone()
        .filter(|token| !token.trim().is_empty());

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let builder = GeyserGrpcClient::build_from_shared(endpoint_url.clone())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let builder = if let Some(token) = endpoint_token {
        builder
            .x_token(Some(token))
            .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err))
    } else {
        builder
    };
    let builder = builder
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let mut client = builder
        .connect()
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));

    info!(endpoint = %endpoint_name, "Connected");

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: CommitmentLevel = config.commitment.into();

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

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut warmup_skipped = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction.as_ref()
                                    && let Some(inner_msg) = tx.transaction.as_ref().and_then(|t| t.message.as_ref()) {
                                        let has_account = inner_msg
                                            .account_keys
                                            .iter()
                                            .any(|key| key.as_slice() == account_pubkey.as_ref());

                                        if has_account {
                                            // Skip observations during warmup
                                            if warmup.is_warming_up() {
                                                warmup_skipped += 1;
                                                continue;
                                            }

                                            let signature = match tx.transaction.as_ref()
                                                .and_then(|t| t.signatures.first()) {
                                                Some(sig) => bs58::encode(sig).into_string(),
                                                None => {
                                                    warn!(endpoint = %endpoint_name, "Missing signature");
                                                    continue;
                                                }
                                            };

                                            // Build observation with server-side timestamp if available
                                            let tx_data = timing::make_observation(
                                                msg.created_at.as_ref(),
                                                start_instant,
                                                start_wallclock_ms,
                                            );

                                            // Log first few observations for diagnostics
                                            if transaction_count < 3 {
                                                let abs_lat = tx_data.client_wallclock_ms - tx_data.timestamp_ms;
                                                info!(
                                                    endpoint = %endpoint_name,
                                                    sig = %&signature[..16],
                                                    server_ts_ms = tx_data.timestamp_ms,
                                                    client_ts_ms = tx_data.client_wallclock_ms,
                                                    abs_latency_ms = abs_lat,
                                                    source = ?tx_data.timestamp_source,
                                                    "Diagnostic: first observations"
                                                );
                                            }

                                            let updated = accumulator.record(
                                                signature.clone(),
                                                tx_data.clone(),
                                            );

                                            if updated {
                                                if let Some(_snapshot) = comparator.record_observation(
                                                    &endpoint_name,
                                                    &signature,
                                                    tx_data,
                                                    total_producers,
                                                ) {
                                                    if let Some(target) = target_transactions {
                                                        let shared = shared_counter
                                                            .fetch_add(1, Ordering::AcqRel)
                                                            + 1;
                                                        if let Some(tracker) = progress.as_ref() {
                                                            tracker.record(shared);
                                                        }
                                                        if shared >= target
                                                            && !shared_shutdown.swap(true, Ordering::AcqRel)
                                                        {
                                                            info!(endpoint = %endpoint_name, target, "Reached target; broadcasting shutdown");
                                                            let _ = shutdown_tx.send(());
                                                        }
                                                    }
                                                }
                                            }

                                            transaction_count += 1;
                                        }
                                    }
                            },
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            },
                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Stream error");
                        break;
                    },
                    None => {
                        info!(endpoint = %endpoint_name, "Stream closed");
                        break;
                    }
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        warmup_skipped,
        "Provider finished"
    );
    Ok(())
}
