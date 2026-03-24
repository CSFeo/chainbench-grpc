use crate::analysis::{EndpointSummary, RunSummary};
use crate::throughput::ThroughputSummary;

pub fn render_run_summary(summary: &RunSummary) -> String {
    let mut endpoints_sorted: Vec<&EndpointSummary> = summary.endpoints.iter().collect();
    endpoints_sorted.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let race_rows = endpoints_sorted
        .iter()
        .map(|ep| {
            let is_fastest = summary.fastest_endpoint.as_deref() == Some(ep.name.as_str());
            let badge = if is_fastest {
                r#" <span class="badge">FASTEST</span>"#
            } else {
                ""
            };
            format!(
                r#"<tr>
  <td>{}{}</td>
  <td class="num">{:.1}</td>
  <td class="num">{:.2}%</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
</tr>"#,
                ep.name,
                badge,
                ep.score,
                ep.first_share * 100.0,
                fmt(ep.rel_p50_ms),
                fmt(ep.rel_p95_ms),
                fmt(ep.rel_p99_ms),
                fmt(ep.abs_p50_ms),
                fmt(ep.abs_p95_ms),
                fmt(ep.abs_p99_ms),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let bucket_rows = endpoints_sorted
        .iter()
        .map(|ep| {
            let b = &ep.buckets;
            let total = b.total().max(1) as f64;
            format!(
                r#"<tr>
  <td>{}</td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
  <td class="num">{} <small>({:.0}%)</small></td>
</tr>"#,
                ep.name,
                b.less_than_400, b.less_than_400 as f64 / total * 100.0,
                b.from_400_to_799, b.from_400_to_799 as f64 / total * 100.0,
                b.from_800_to_999, b.from_800_to_999 as f64 / total * 100.0,
                b.from_1000_to_1199, b.from_1000_to_1199 as f64 / total * 100.0,
                b.from_1200_to_1499, b.from_1200_to_1499 as f64 / total * 100.0,
                b.from_1500_to_1999, b.from_1500_to_1999 as f64 / total * 100.0,
                b.at_2000_or_more, b.at_2000_or_more as f64 / total * 100.0,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let reliability_rows = endpoints_sorted
        .iter()
        .map(|ep| {
            format!(
                r#"<tr>
  <td>{}</td>
  <td class="num">{:.1}%</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
  <td class="num">{}</td>
</tr>"#,
                ep.name,
                ep.timestamp_coverage_pct,
                ep.server_timestamp_count,
                ep.client_timestamp_count,
                ep.valid_transactions,
                ep.backfill_transactions,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>chainbench-grpc Report</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e1e4e8; padding: 2rem; }}
  .container {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 1.8rem; margin-bottom: 0.3rem; color: #58a6ff; }}
  h2 {{ font-size: 1.2rem; margin: 2rem 0 0.8rem; color: #8b949e; border-bottom: 1px solid #30363d; padding-bottom: 0.4rem; }}
  .subtitle {{ color: #8b949e; font-size: 0.9rem; margin-bottom: 2rem; }}
  .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
  .meta-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1rem; }}
  .meta-card .label {{ font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }}
  .meta-card .value {{ font-size: 1.4rem; font-weight: 600; margin-top: 0.3rem; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; margin-bottom: 1rem; }}
  th {{ background: #1c2128; color: #8b949e; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; padding: 0.7rem 1rem; text-align: left; }}
  td {{ padding: 0.6rem 1rem; border-top: 1px solid #21262d; font-size: 0.9rem; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tr:hover td {{ background: #1c2128; }}
  .badge {{ background: #238636; color: #fff; font-size: 0.65rem; padding: 2px 6px; border-radius: 4px; margin-left: 6px; font-weight: 600; }}
  small {{ color: #8b949e; }}
  .footer {{ margin-top: 3rem; padding-top: 1rem; border-top: 1px solid #30363d; color: #484f58; font-size: 0.8rem; }}
</style>
</head>
<body>
<div class="container">

<h1>chainbench-grpc</h1>
<p class="subtitle">Solana gRPC Benchmark Report &mdash; v{version}</p>

<div class="meta">
  <div class="meta-card">
    <div class="label">Total Signatures</div>
    <div class="value">{total_sigs}</div>
  </div>
  <div class="meta-card">
    <div class="label">Collection Time</div>
    <div class="value">{duration:.1}s</div>
  </div>
  <div class="meta-card">
    <div class="label">Throughput</div>
    <div class="value">{throughput:.1} tx/s</div>
  </div>
  <div class="meta-card">
    <div class="label">Backfill</div>
    <div class="value">{backfill}</div>
  </div>
  <div class="meta-card">
    <div class="label">Errors</div>
    <div class="value">{errors}</div>
  </div>
</div>

<h2>Endpoint Comparison</h2>
<table>
<tr>
  <th>Endpoint</th><th>Score</th><th>Win %</th>
  <th>Rel P50</th><th>Rel P95</th><th>Rel P99</th>
  <th>Abs P50</th><th>Abs P95</th><th>Abs P99</th>
</tr>
{race_rows}
</table>

<h2>Latency Distribution (ms)</h2>
<table>
<tr>
  <th>Endpoint</th><th>&lt;400</th><th>400-799</th><th>800-999</th>
  <th>1000-1199</th><th>1200-1499</th><th>1500-1999</th><th>2000+</th>
</tr>
{bucket_rows}
</table>

<h2>Reliability</h2>
<table>
<tr>
  <th>Endpoint</th><th>TS Coverage</th><th>Server TS</th><th>Client TS</th>
  <th>Valid Tx</th><th>Backfill</th>
</tr>
{reliability_rows}
</table>

<div class="footer">
  Generated by chainbench-grpc v{version}
</div>

</div>
</body>
</html>"##,
        version = env!("CARGO_PKG_VERSION"),
        total_sigs = summary.total_signatures,
        duration = summary.test_duration_secs,
        throughput = summary.throughput_tx_per_sec,
        backfill = summary.backfill_signatures,
        errors = summary.total_errors,
        race_rows = race_rows,
        bucket_rows = bucket_rows,
        reliability_rows = reliability_rows,
    )
}

pub fn render_throughput(summary: &ThroughputSummary) -> String {
    let rows = summary
        .results
        .iter()
        .map(|r| {
            format!(
                r#"<tr>
  <td>{}</td><td class="num">{:.1}s</td><td class="num">{}</td>
  <td class="num">{}</td><td class="num">{:.1}</td><td class="num">{:.1}</td>
  <td class="num">{}</td><td class="num">{}</td>
</tr>"#,
                r.endpoint,
                r.duration_secs,
                r.total_messages,
                humanize(r.total_bytes),
                r.messages_per_sec,
                r.bytes_per_sec / 1024.0,
                r.transactions,
                r.errors,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r##"<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>chainbench-grpc Throughput</title>
<style>
  body {{ font-family: sans-serif; background: #0f1117; color: #e1e4e8; padding: 2rem; }}
  .container {{ max-width: 900px; margin: 0 auto; }}
  h1 {{ color: #58a6ff; }} h2 {{ color: #8b949e; margin: 1.5rem 0 0.5rem; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }}
  th {{ background: #1c2128; color: #8b949e; font-size: 0.75rem; text-transform: uppercase; padding: 0.7rem 1rem; text-align: left; }}
  td {{ padding: 0.6rem 1rem; border-top: 1px solid #21262d; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
</style></head><body><div class="container">
<h1>chainbench-grpc</h1>
<h2>Throughput Results</h2>
<table>
<tr><th>Endpoint</th><th>Duration</th><th>Messages</th><th>Data</th><th>Msgs/s</th><th>KB/s</th><th>Txs</th><th>Errors</th></tr>
{rows}
</table>
<p style="color:#484f58;margin-top:2rem;font-size:0.8rem">Generated by chainbench-grpc v{version}</p>
</div></body></html>"##,
        rows = rows,
        version = env!("CARGO_PKG_VERSION"),
    )
}

pub fn render_slots(result: &crate::slots::SlotBenchResult) -> String {
    let rows = result
        .endpoints
        .iter()
        .map(|ep| {
            format!(
                r#"<tr>
  <td>{}</td><td class="num">{}</td><td class="num">{}</td>
  <td class="num">{}</td><td class="num">{}</td>
  <td class="num">{}</td><td class="num">{}</td>
  <td class="num">{}</td><td class="num">{}</td>
  <td class="num">{}</td><td class="num">{}</td>
</tr>"#,
                ep.endpoint, ep.slots_collected, ep.slots_complete,
                fms(ep.download.p50_ms), fms(ep.download.p90_ms),
                fms(ep.replay.p50_ms), fms(ep.replay.p90_ms),
                fms(ep.confirm.p50_ms), fms(ep.confirm.p90_ms),
                fms(ep.finalize.p50_ms), fms(ep.finalize.p90_ms),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r##"<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>chainbench-grpc Slot Lifecycle</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e1e4e8; padding: 2rem; }}
  .container {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 1.8rem; color: #58a6ff; margin-bottom: 0.3rem; }}
  h2 {{ font-size: 1.2rem; margin: 2rem 0 0.8rem; color: #8b949e; border-bottom: 1px solid #30363d; padding-bottom: 0.4rem; }}
  .subtitle {{ color: #8b949e; font-size: 0.9rem; margin-bottom: 2rem; }}
  .meta {{ display: flex; gap: 1.5rem; margin-bottom: 2rem; flex-wrap: wrap; }}
  .meta-card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1rem 1.4rem; }}
  .meta-card .label {{ font-size: 0.75rem; color: #8b949e; text-transform: uppercase; }}
  .meta-card .value {{ font-size: 1.4rem; font-weight: 600; margin-top: 0.2rem; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }}
  th {{ background: #1c2128; color: #8b949e; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; padding: 0.7rem 0.8rem; text-align: left; }}
  td {{ padding: 0.6rem 0.8rem; border-top: 1px solid #21262d; font-size: 0.85rem; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  tr:hover td {{ background: #1c2128; }}
  .footer {{ margin-top: 3rem; color: #484f58; font-size: 0.8rem; }}
  .stage {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-right: 4px; }}
  .s-download {{ background: #1f6feb33; color: #58a6ff; }}
  .s-replay {{ background: #23863633; color: #3fb950; }}
  .s-confirm {{ background: #d2992233; color: #d29922; }}
  .s-finalize {{ background: #f8514933; color: #f85149; }}
</style></head><body><div class="container">
<h1>chainbench-grpc</h1>
<p class="subtitle">Slot Lifecycle Report &mdash; v{version}</p>
<div class="meta">
  <div class="meta-card"><div class="label">Common Slots</div><div class="value">{common}</div></div>
  <div class="meta-card"><div class="label">Duration</div><div class="value">{duration:.1}s</div></div>
</div>
<h2>Pipeline Stages</h2>
<p style="margin-bottom:1rem;color:#8b949e;font-size:0.85rem">
  <span class="stage s-download">Download</span> FirstShred &rarr; Completed &nbsp;
  <span class="stage s-replay">Replay</span> CreatedBank &rarr; Processed &nbsp;
  <span class="stage s-confirm">Confirm</span> Processed &rarr; Confirmed &nbsp;
  <span class="stage s-finalize">Finalize</span> Confirmed &rarr; Finalized
</p>
<table>
<tr>
  <th>Endpoint</th><th>Slots</th><th>Complete</th>
  <th>DL P50</th><th>DL P90</th>
  <th>Replay P50</th><th>Replay P90</th>
  <th>Confirm P50</th><th>Confirm P90</th>
  <th>Final P50</th><th>Final P90</th>
</tr>
{rows}
</table>
<div class="footer">Generated by chainbench-grpc v{version}</div>
</div></body></html>"##,
        version = env!("CARGO_PKG_VERSION"),
        common = result.common_slots,
        duration = result.duration_secs,
        rows = rows,
    )
}

fn fms(v: Option<f64>) -> String {
    v.map(|x| format!("{:.0}ms", x)).unwrap_or_else(|| "-".into())
}

fn fmt(v: Option<f64>) -> String {
    v.map(|x| format!("{:.2}", x))
        .unwrap_or_else(|| "-".into())
}

fn humanize(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
