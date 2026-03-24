use comfy_table::{ContentArrangement, Table};
use serde_json::{Map, json};
use std::cmp::Ordering;

use crate::analysis::{EndpointSummary, RunSummary};

#[cfg(target_os = "windows")]
fn table_preset() -> &'static str {
    comfy_table::presets::ASCII_FULL
}

#[cfg(not(target_os = "windows"))]
fn table_preset() -> &'static str {
    comfy_table::presets::UTF8_FULL
}

pub fn display_console(summary: &RunSummary, show_race: bool, show_latency: bool) {
    println!("\n  chainbench-grpc results");
    println!("  ============================================");

    if !summary.has_data {
        println!("  Not enough data collected.");
        return;
    }

    let mut rows: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
    rows.sort_by(|a, b| compare_by_p50(a, b));

    // Quick summary
    let fastest = summary.fastest_endpoint.as_deref();
    for ep in &rows {
        if ep.valid_transactions == 0 {
            println!("  {}: Not enough data", ep.name);
            continue;
        }
        let win = format_percent(ep.first_share);
        let is_fastest = fastest == Some(ep.name.as_str());
        if is_fastest {
            println!("  {}: Win rate {}%, p50 0.00ms (fastest)", ep.name, win);
        } else {
            let p50 = ep.rel_p50_ms.map(|v| format!("{:.2}ms", v)).unwrap_or_else(|| "-".into());
            println!("  {}: Win rate {}%, p50 {}", ep.name, win, p50);
        }
    }

    // Race table (relative latency / win rate)
    if show_race && summary.endpoints.len() > 1 {
        println!("\n  Race Results (relative latency)");
        println!("  --------------------------------------------");

        let mut table = Table::new();
        table.load_preset(table_preset());
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            "Endpoint", "Win %", "Rel P50 ms", "Rel P95 ms", "Rel P99 ms", "Valid Tx", "Firsts", "Backfill",
        ]);

        for ep in &rows {
            table.add_row(vec![
                ep.name.clone(),
                format_percent(ep.first_share),
                fmt_opt(ep.rel_p50_ms),
                fmt_opt(ep.rel_p95_ms),
                fmt_opt(ep.rel_p99_ms),
                ep.valid_transactions.to_string(),
                ep.first_detections.to_string(),
                ep.backfill_transactions.to_string(),
            ]);
        }

        println!("{}", table);
    }

    // Absolute latency table
    if show_latency {
        let has_abs = rows.iter().any(|ep| ep.abs_p50_ms.is_some());
        if has_abs {
            println!("\n  Absolute Latency (server -> client)");
            println!("  --------------------------------------------");

            let mut table = Table::new();
            table.load_preset(table_preset());
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                "Endpoint", "Abs P50 ms", "Abs P95 ms", "Abs P99 ms", "Source", "Samples",
            ]);

            for ep in &rows {
                let source = if ep.has_server_timestamps {
                    "server created_at"
                } else {
                    "client wallclock"
                };
                table.add_row(vec![
                    ep.name.clone(),
                    fmt_opt(ep.abs_p50_ms),
                    fmt_opt(ep.abs_p95_ms),
                    fmt_opt(ep.abs_p99_ms),
                    source.to_string(),
                    ep.buckets.total().to_string(),
                ]);
            }

            println!("{}", table);
        }

        // Latency buckets
        let has_buckets = rows.iter().any(|ep| ep.buckets.total() > 0);
        if has_buckets {
            println!("\n  Latency Distribution");
            println!("  --------------------------------------------");

            let mut table = Table::new();
            table.load_preset(table_preset());
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                "Endpoint", "<400ms", "400-799", "800-999", "1000-1199", "1200-1499", "1500-1999", "2000+",
            ]);

            for ep in &rows {
                let b = &ep.buckets;
                table.add_row(vec![
                    ep.name.clone(),
                    b.less_than_400.to_string(),
                    b.from_400_to_799.to_string(),
                    b.from_800_to_999.to_string(),
                    b.from_1000_to_1199.to_string(),
                    b.from_1200_to_1499.to_string(),
                    b.from_1500_to_1999.to_string(),
                    b.at_2000_or_more.to_string(),
                ]);
            }

            println!("{}", table);
        }
    }

    println!(
        "\n  Total signatures: {} | Backfill: {}",
        summary.total_signatures, summary.backfill_signatures
    );
}

pub fn output_json(summary: &RunSummary) -> String {
    let mut per_endpoint = Map::new();
    for ep in &summary.endpoints {
        let payload = json!({
            "win_rate": ep.first_share,
            "relative_latency": {
                "p50_ms": ep.rel_p50_ms,
                "p95_ms": ep.rel_p95_ms,
                "p99_ms": ep.rel_p99_ms,
            },
            "absolute_latency": {
                "p50_ms": ep.abs_p50_ms,
                "p95_ms": ep.abs_p95_ms,
                "p99_ms": ep.abs_p99_ms,
                "source": if ep.has_server_timestamps { "server_created_at" } else { "client_wallclock" },
            },
            "buckets": {
                "<400ms": ep.buckets.less_than_400,
                "400-799ms": ep.buckets.from_400_to_799,
                "800-999ms": ep.buckets.from_800_to_999,
                "1000-1199ms": ep.buckets.from_1000_to_1199,
                "1200-1499ms": ep.buckets.from_1200_to_1499,
                "1500-1999ms": ep.buckets.from_1500_to_1999,
                "2000ms+": ep.buckets.at_2000_or_more,
            },
            "valid_transactions": ep.valid_transactions,
            "first_detections": ep.first_detections,
            "backfill_transactions": ep.backfill_transactions,
        });
        per_endpoint.insert(ep.name.clone(), payload);
    }

    let report = json!({
        "tool": "chainbench-grpc",
        "version": env!("CARGO_PKG_VERSION"),
        "total_signatures": summary.total_signatures,
        "backfill_signatures": summary.backfill_signatures,
        "fastest_endpoint": summary.fastest_endpoint,
        "per_endpoint": per_endpoint,
    });

    serde_json::to_string_pretty(&report).unwrap_or_else(|_| "{}".to_string())
}

fn fmt_opt(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "-".to_string())
}

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{:.2}", value * 100.0)
    } else {
        "-".to_string()
    }
}

fn compare_by_p50(lhs: &EndpointSummary, rhs: &EndpointSummary) -> Ordering {
    match (lhs.rel_p50_ms, rhs.rel_p50_ms) {
        (Some(l), Some(r)) => l
            .partial_cmp(&r)
            .unwrap_or(Ordering::Equal)
            .then_with(|| lhs.name.cmp(&rhs.name)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => lhs.name.cmp(&rhs.name),
    }
}
