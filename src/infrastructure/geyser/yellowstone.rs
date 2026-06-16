use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use solana_pubkey::Pubkey;
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{error, info, warn};

use crate::domain::collector::TransactionAccumulator;
use crate::domain::config::{BenchConfig, Endpoint};
use crate::domain::timing;
use crate::infrastructure::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

use super::{GeyserProvider, ProviderContext, ProviderStats, client::GeyserGrpcClient};

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: BenchConfig,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<ProviderStats, Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_yellowstone_endpoint(endpoint, config, context).await })
    }
}

/// Exponential backoff capped at 2^4 = 16s.
fn backoff_delay(reconnect_count: u32) -> std::time::Duration {
    std::time::Duration::from_secs(2u64.pow(reconnect_count.min(4)))
}

/// Convert a protobuf `created_at` to Unix milliseconds (nanosecond precision).
/// This adapts the gRPC wire type to the plain `f64` the domain works in.
fn server_timestamp_ms(created_at: Option<&prost_types::Timestamp>) -> Option<f64> {
    created_at.map(|ts| (ts.seconds as f64) * 1000.0 + (ts.nanos as f64) / 1_000_000.0)
}

async fn connect_client(
    endpoint_url: &str,
    endpoint_token: &Option<String>,
) -> Result<GeyserGrpcClient, Box<dyn Error + Send + Sync>> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint_url.to_string())?;
    if let Some(token) = endpoint_token {
        builder = builder.x_token(Some(token.clone()))?;
    }
    // Apply TLS only for https:// endpoints; http:// connects in plaintext
    // (used for local/in-cluster endpoints and integration tests).
    if endpoint_url.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: BenchConfig,
    context: ProviderContext,
) -> Result<ProviderStats, Box<dyn Error + Send + Sync>> {
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
    let commitment: CommitmentLevel = config.commitment.into();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;
    let mut warmup_skipped = 0usize;
    let mut reconnect_count = 0u32;
    let max_reconnects = 3;

    'outer: loop {
        info!(endpoint = %endpoint_name, url = %endpoint_url, reconnects = reconnect_count, "Connecting");

        let mut client = match connect_client(&endpoint_url, &endpoint_token).await {
            Ok(c) => c,
            Err(e) => {
                if reconnect_count >= max_reconnects {
                    error!(endpoint = %endpoint_name, error = %e, "Max reconnects reached, giving up");
                    break;
                }
                reconnect_count += 1;
                let delay = backoff_delay(reconnect_count);
                warn!(endpoint = %endpoint_name, error = %e, delay_secs = delay.as_secs(), "Connection failed, retrying");
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        info!(endpoint = %endpoint_name, "Connected");

        let (mut subscribe_tx, mut stream) = match client.subscribe().await {
            Ok(pair) => pair,
            Err(e) => {
                if reconnect_count >= max_reconnects {
                    error!(endpoint = %endpoint_name, error = %e, "Subscribe failed, max reconnects reached");
                    break;
                }
                reconnect_count += 1;
                let delay = backoff_delay(reconnect_count);
                warn!(endpoint = %endpoint_name, error = %e, delay_secs = delay.as_secs(), "Subscribe failed, retrying");
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        // Send subscription request
        let mut transactions_filter = HashMap::new();
        transactions_filter.insert(
            "account".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![config.account.clone()],
                account_exclude: vec![],
                account_required: vec![],
                ..Default::default()
            },
        );

        if let Err(e) = subscribe_tx
            .send(SubscribeRequest {
                slots: HashMap::default(),
                accounts: HashMap::default(),
                transactions: transactions_filter,
                transactions_status: HashMap::default(),
                entry: HashMap::default(),
                blocks: HashMap::default(),
                blocks_meta: HashMap::default(),
                commitment: Some(commitment as i32),
                accounts_data_slice: Vec::default(),
                ping: None,
                from_slot: None,
            })
            .await
        {
            if reconnect_count >= max_reconnects {
                error!(endpoint = %endpoint_name, error = %e, "Send subscribe request failed, max reconnects reached");
                break;
            }
            reconnect_count += 1;
            let delay = backoff_delay(reconnect_count);
            warn!(endpoint = %endpoint_name, error = %e, delay_secs = delay.as_secs(), "Send subscribe request failed, retrying");
            tokio::time::sleep(delay).await;
            continue;
        }
        let mut last_log_count = transaction_count;
        let mut last_log_time = std::time::Instant::now();

        loop {
            // Periodic per-endpoint activity log
            if last_log_time.elapsed() >= std::time::Duration::from_secs(10) {
                let new_txs = transaction_count - last_log_count;
                info!(
                    endpoint = %endpoint_name,
                    total = transaction_count,
                    last_10s = new_txs,
                    reconnects = reconnect_count,
                    "{:.0} tx/s",
                    new_txs as f64 / last_log_time.elapsed().as_secs_f64()
                );
                last_log_count = transaction_count;
                last_log_time = std::time::Instant::now();
            }

            tokio::select! { biased;
                _ = shutdown_rx.recv() => {
                    info!(endpoint = %endpoint_name, "Received stop signal");
                    break 'outer;
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
                                                if warmup.is_warming_up() {
                                                    warmup_skipped += 1;
                                                    continue;
                                                }

                                                let signature = match tx.transaction.as_ref()
                                                    .and_then(|t| t.signatures.first()) {
                                                    Some(sig) => bs58::encode(sig).into_string(),
                                                    None => continue,
                                                };

                                                let tx_data = timing::observe(
                                                    server_timestamp_ms(msg.created_at.as_ref()),
                                                    start_instant,
                                                    start_wallclock_ms,
                                                );

                                                if transaction_count < 3 {
                                                    let abs_lat = tx_data.client_wallclock_ms - tx_data.timestamp_ms;
                                                    info!(
                                                        endpoint = %endpoint_name,
                                                        sig = %&signature[..16],
                                                        abs_latency_ms = abs_lat,
                                                        source = ?tx_data.timestamp_source,
                                                        "Diagnostic"
                                                    );
                                                }

                                                let updated = accumulator.record(signature.clone(), tx_data.clone());

                                                if updated
                                                    && comparator
                                                        .record_observation(
                                                            &endpoint_name,
                                                            &signature,
                                                            tx_data,
                                                            total_producers,
                                                        )
                                                        .is_some()
                                                    && let Some(target) = target_transactions
                                                {
                                                    let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                                                    if let Some(tracker) = progress.as_ref() {
                                                        tracker.record(shared);
                                                    }
                                                    if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                                                        info!(endpoint = %endpoint_name, target, "Reached target; broadcasting shutdown");
                                                        let _ = shutdown_tx.send(());
                                                    }
                                                }

                                                transaction_count += 1;
                                            }
                                        }
                                },
                                Some(UpdateOneof::Ping(_)) => {
                                    let _ = subscribe_tx
                                        .send(SubscribeRequest {
                                            ping: Some(SubscribeRequestPing { id: 1 }),
                                            ..Default::default()
                                        })
                                        .await;
                                },
                                _ => {}
                            }
                        },
                        Some(Err(e)) => {
                            warn!(endpoint = %endpoint_name, error = ?e, "Stream error");
                            break; // break inner loop, retry outer
                        },
                        None => {
                            warn!(endpoint = %endpoint_name, "Stream closed by server");
                            break;
                        }
                    }
                }
            }
        }

        // Stream broke — try reconnecting
        if shared_shutdown.load(Ordering::Acquire) {
            break;
        }
        if reconnect_count >= max_reconnects {
            error!(endpoint = %endpoint_name, "Max reconnects ({}) reached", max_reconnects);
            break;
        }
        reconnect_count += 1;
        let delay = backoff_delay(reconnect_count);
        warn!(endpoint = %endpoint_name, reconnects = reconnect_count, delay_secs = delay.as_secs(), "Reconnecting");
        tokio::time::sleep(delay).await;
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        warmup_skipped,
        reconnects = reconnect_count,
        "Provider finished"
    );
    Ok(ProviderStats {
        endpoint_name: endpoint_name.clone(),
        warmup_skipped,
        reconnect_count,
    })
}

#[cfg(test)]
mod tests {
    use super::server_timestamp_ms;

    #[test]
    fn server_timestamp_none_when_absent() {
        assert_eq!(server_timestamp_ms(None), None);
    }

    #[test]
    fn server_timestamp_combines_seconds_and_nanos() {
        let ts = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000, // 0.5s -> 500ms
        };
        assert_eq!(
            server_timestamp_ms(Some(&ts)).unwrap(),
            1_700_000_000_000.0 + 500.0
        );
    }

    #[test]
    fn server_timestamp_sub_millisecond_precision() {
        // 1ns -> 1e-6 ms; verifies sub-ms precision is preserved.
        let ts = prost_types::Timestamp {
            seconds: 0,
            nanos: 1,
        };
        assert_eq!(server_timestamp_ms(Some(&ts)).unwrap(), 1e-6);
    }
}
