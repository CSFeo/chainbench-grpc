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
use tracing::{info, error, warn};

use crate::{
    config::{BenchConfig, Endpoint},
    proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeRequestPing,
        subscribe_update::UpdateOneof,
    },
    providers::yellowstone_client::GeyserGrpcClient,
};

/// Slot status stages we track (from Thorofare)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub enum SlotStage {
    FirstShredReceived,  // status 3
    Completed,           // status 4
    CreatedBank,         // status 5
    Processed,           // status 0
    Confirmed,           // status 1
    Finalized,           // status 2
}

impl SlotStage {
    fn from_i32(status: i32) -> Option<Self> {
        match status {
            3 => Some(SlotStage::FirstShredReceived),
            4 => Some(SlotStage::Completed),
            5 => Some(SlotStage::CreatedBank),
            0 => Some(SlotStage::Processed),
            1 => Some(SlotStage::Confirmed),
            2 => Some(SlotStage::Finalized),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SlotStage::FirstShredReceived => "FirstShred",
            SlotStage::Completed => "Completed",
            SlotStage::CreatedBank => "CreatedBank",
            SlotStage::Processed => "Processed",
            SlotStage::Confirmed => "Confirmed",
            SlotStage::Finalized => "Finalized",
        }
    }
}

#[derive(Debug, Clone)]
struct SlotObservation {
    stage: SlotStage,
    instant: Instant,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotStageTiming {
    pub download_ms: Option<f64>,    // FirstShred -> Completed
    pub replay_ms: Option<f64>,      // CreatedBank -> Processed
    pub confirm_ms: Option<f64>,     // Processed -> Confirmed
    pub finalize_ms: Option<f64>,    // Confirmed -> Finalized
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotEndpointSummary {
    pub endpoint: String,
    pub slots_collected: usize,
    pub slots_complete: usize,
    pub download: PercentileSummary,
    pub replay: PercentileSummary,
    pub confirm: PercentileSummary,
    pub finalize: PercentileSummary,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PercentileSummary {
    pub p50_ms: Option<f64>,
    pub p90_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlotBenchResult {
    pub endpoints: Vec<SlotEndpointSummary>,
    pub common_slots: usize,
    pub duration_secs: f64,
}

pub async fn run_slot_benchmark(
    endpoints: Vec<Endpoint>,
    config: BenchConfig,
    target_slots: usize,
    max_duration_secs: u64,
) -> SlotBenchResult {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let running = Arc::new(AtomicBool::new(true));
    let slot_counter = Arc::new(AtomicUsize::new(0));

    // Ctrl+C
    let ctrl_running = Arc::clone(&running);
    let ctrl_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrl_running.store(false, Ordering::SeqCst);
            let _ = ctrl_tx.send(());
        }
    });

    // Timer
    let timer_running = Arc::clone(&running);
    let timer_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(max_duration_secs)).await;
        timer_running.store(false, Ordering::SeqCst);
        let _ = timer_tx.send(());
    });

    let start = Instant::now();
    let mut handles = Vec::new();

    for ep in &endpoints {
        let ep = ep.clone();
        let config = config.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let running = Arc::clone(&running);
        let counter = Arc::clone(&slot_counter);

        handles.push(tokio::spawn(async move {
            collect_slots(ep, config, &mut shutdown_rx, running, counter, target_slots).await
        }));
    }

    // Collect per-endpoint data
    let mut all_data: Vec<(String, HashMap<u64, HashMap<SlotStage, Instant>>)> = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(Ok((name, data))) => all_data.push((name, data)),
            Ok(Err(e)) => error!("Slot collector error: {}", e),
            Err(e) => error!("Slot collector panicked: {}", e),
        }
    }

    let duration = start.elapsed().as_secs_f64();

    // Compute per-endpoint summaries
    let mut summaries = Vec::new();
    for (name, data) in &all_data {
        summaries.push(compute_endpoint_slot_summary(name, data));
    }

    // Count common slots (seen by all endpoints)
    let common = if all_data.len() > 1 {
        let sets: Vec<std::collections::HashSet<u64>> = all_data
            .iter()
            .map(|(_, data)| data.keys().copied().collect())
            .collect();
        sets.iter()
            .skip(1)
            .fold(sets[0].clone(), |acc, s| acc.intersection(s).copied().collect())
            .len()
    } else {
        all_data.first().map(|(_, d)| d.len()).unwrap_or(0)
    };

    SlotBenchResult {
        endpoints: summaries,
        common_slots: common,
        duration_secs: duration,
    }
}

async fn collect_slots(
    endpoint: Endpoint,
    config: BenchConfig,
    shutdown_rx: &mut broadcast::Receiver<()>,
    running: Arc<AtomicBool>,
    counter: Arc<AtomicUsize>,
    target_slots: usize,
) -> Result<(String, HashMap<u64, HashMap<SlotStage, Instant>>), Box<dyn std::error::Error + Send + Sync>> {
    let name = endpoint.name.clone();
    let url = endpoint.url.clone();
    let token = endpoint.x_token.clone().filter(|t| !t.trim().is_empty());

    info!(endpoint = %name, "Slots: connecting");

    let builder = GeyserGrpcClient::build_from_shared(url)?;
    let builder = if let Some(t) = token { builder.x_token(Some(t))? } else { builder };
    let builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    let mut client = builder.connect().await?;

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;

    // Subscribe to slot updates only
    let mut slots_filter = HashMap::new();
    slots_filter.insert(
        "slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: Some(true),
        },
    );

    subscribe_tx
        .send(SubscribeRequest {
            slots: slots_filter,
            accounts: HashMap::default(),
            transactions: HashMap::default(),
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: None,
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        })
        .await?;

    info!(endpoint = %name, "Slots: subscribed");

    let mut slot_data: HashMap<u64, HashMap<SlotStage, Instant>> = HashMap::new();
    let mut finalized_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => break,
            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Slot(slot_msg)) => {
                                if let Some(stage) = SlotStage::from_i32(slot_msg.status) {
                                    let slot = slot_msg.slot;
                                    let now = Instant::now();
                                    slot_data.entry(slot).or_default().insert(stage, now);

                                    if stage == SlotStage::Finalized {
                                        finalized_count += 1;
                                        let total = counter.fetch_add(1, Ordering::AcqRel) + 1;
                                        if total % 50 == 0 || total <= 5 {
                                            info!(endpoint = %name, finalized = finalized_count, total_slots = slot_data.len());
                                        }
                                        if finalized_count >= target_slots {
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                let _ = subscribe_tx.send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                }).await;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => { error!(endpoint = %name, error = ?e, "Slot stream error"); break; }
                    None => break,
                }
            }
        }
        if !running.load(Ordering::Relaxed) { break; }
    }

    info!(endpoint = %name, total_slots = slot_data.len(), finalized = finalized_count, "Slots: finished");
    Ok((name, slot_data))
}

fn compute_endpoint_slot_summary(
    name: &str,
    data: &HashMap<u64, HashMap<SlotStage, Instant>>,
) -> SlotEndpointSummary {
    let mut downloads = Vec::new();
    let mut replays = Vec::new();
    let mut confirms = Vec::new();
    let mut finalizes = Vec::new();
    let mut complete = 0usize;

    for stages in data.values() {
        let get = |s: SlotStage| stages.get(&s);

        let has_all = get(SlotStage::FirstShredReceived).is_some()
            && get(SlotStage::Completed).is_some()
            && get(SlotStage::CreatedBank).is_some()
            && get(SlotStage::Processed).is_some()
            && get(SlotStage::Confirmed).is_some()
            && get(SlotStage::Finalized).is_some();

        if !has_all {
            continue;
        }
        complete += 1;

        if let (Some(a), Some(b)) = (get(SlotStage::FirstShredReceived), get(SlotStage::Completed)) {
            downloads.push(b.duration_since(*a).as_secs_f64() * 1000.0);
        }
        if let (Some(a), Some(b)) = (get(SlotStage::CreatedBank), get(SlotStage::Processed)) {
            replays.push(b.duration_since(*a).as_secs_f64() * 1000.0);
        }
        if let (Some(a), Some(b)) = (get(SlotStage::Processed), get(SlotStage::Confirmed)) {
            confirms.push(b.duration_since(*a).as_secs_f64() * 1000.0);
        }
        if let (Some(a), Some(b)) = (get(SlotStage::Confirmed), get(SlotStage::Finalized)) {
            finalizes.push(b.duration_since(*a).as_secs_f64() * 1000.0);
        }
    }

    SlotEndpointSummary {
        endpoint: name.to_string(),
        slots_collected: data.len(),
        slots_complete: complete,
        download: make_percentiles(&mut downloads),
        replay: make_percentiles(&mut replays),
        confirm: make_percentiles(&mut confirms),
        finalize: make_percentiles(&mut finalizes),
    }
}

fn make_percentiles(data: &mut Vec<f64>) -> PercentileSummary {
    if data.is_empty() {
        return PercentileSummary::default();
    }
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    PercentileSummary {
        p50_ms: Some(percentile(data, 0.50)),
        p90_ms: Some(percentile(data, 0.90)),
        p99_ms: Some(percentile(data, 0.99)),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

pub fn display_slot_console(result: &SlotBenchResult) {
    use comfy_table::{ContentArrangement, Table};

    println!("\n  Slot Lifecycle Results");
    println!("  ============================================");
    println!("  Common slots: {} | Duration: {:.1}s", result.common_slots, result.duration_secs);

    let mut table = Table::new();
    #[cfg(not(target_os = "windows"))]
    table.load_preset(comfy_table::presets::UTF8_FULL);
    #[cfg(target_os = "windows")]
    table.load_preset(comfy_table::presets::ASCII_FULL);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        "Endpoint", "Slots", "Complete",
        "Download P50", "Download P90",
        "Replay P50", "Replay P90",
        "Confirm P50", "Confirm P90",
        "Finalize P50", "Finalize P90",
    ]);

    for ep in &result.endpoints {
        table.add_row(vec![
            ep.endpoint.clone(),
            ep.slots_collected.to_string(),
            ep.slots_complete.to_string(),
            f(ep.download.p50_ms), f(ep.download.p90_ms),
            f(ep.replay.p50_ms), f(ep.replay.p90_ms),
            f(ep.confirm.p50_ms), f(ep.confirm.p90_ms),
            f(ep.finalize.p50_ms), f(ep.finalize.p90_ms),
        ]);
    }

    println!("{}", table);
}

fn f(v: Option<f64>) -> String {
    v.map(|x| format!("{:.0}ms", x)).unwrap_or_else(|| "-".into())
}
