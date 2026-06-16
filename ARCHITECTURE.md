# Architecture

`chainbench-grpc` is organized into four layers with dependencies pointing
**inward**. The domain is the stable core; everything else depends on it, and it
depends on nothing in the crate.

```
            ┌─────────────────────────────────────────────┐
            │                 presentation                 │  console · json · csv · html
            └───────────────┬───────────────┬─────────────┘
                            │               │
            ┌───────────────▼─────┐         │
            │     application     │         │   use-case pipelines
            │  run · slots ·      │         │   (orchestration)
            │  throughput         │         │
            └───────┬─────────────┘         │
                    │                       │
        ┌───────────▼───────────┐           │
        │     infrastructure    │           │   I/O adapters
        │  geyser · proto ·     │           │   (gRPC, NTP, TOML)
        │  sntp · config_file   │           │
        └───────────┬───────────┘           │
                    │                       │
            ┌───────▼───────────────────────▼─────┐
            │                domain                │  pure model + logic
            │  analysis · scoring · collector ·    │
            │  timing · clock · config · warmup    │
            └──────────────────────────────────────┘
```

**Dependency rule.** `domain` imports nothing from the other layers.
`infrastructure` imports only `domain` (it adapts protobuf/NTP/TOML *to* domain
types). `application` imports `domain` + `infrastructure`. `presentation`
imports `domain` + `application` result types. `main.rs` is the composition
root that wires them together and selects a renderer.

## Module map

| Layer | Module | Responsibility |
|---|---|---|
| domain | `analysis` | `RunSummary`/`EndpointSummary`, percentiles, win-rate, backfill + clock-skew handling |
| domain | `scoring` | composite 0–100 score (fixed-threshold weights) |
| domain | `collector` | `Comparator` + `TransactionAccumulator` (dedup, earliest-wins, emit-once) |
| domain | `timing` | `TransactionData`/`TimestampSource` value objects; `observe()` |
| domain | `clock` | `ClockOffset` value object + offset formula |
| domain | `config` | `BenchConfig`, `Endpoint`, `EndpointKind`, `ArgsCommitment` |
| domain | `warmup` | `WarmupGuard` |
| application | `run` | race/latency/full pipeline (spawn providers, shutdown, summarize) |
| application | `slots` | slot-lifecycle pipeline + computation |
| application | `throughput` | throughput pipeline |
| infrastructure | `geyser` | `GeyserProvider` trait, factory, `ProviderContext`, Yellowstone provider + gRPC client; protobuf↔domain conversions |
| infrastructure | `proto` | generated protobuf (build-time) |
| infrastructure | `sntp` | NTP probe over UDP → `ClockOffset` |
| infrastructure | `config_file` | TOML config loading |
| presentation | `output` | console tables, JSON, CSV |
| presentation | `html` | standalone HTML report |

## Ubiquitous language

- **Endpoint** — a gRPC provider under test.
- **Observation** — one endpoint receiving one transaction signature, carrying
  server (`created_at`) and client timestamps (`domain::timing`).
- **Comparator** — aggregates observations per signature across endpoints
  (`domain::collector`).
- **Run / Summary** — a completed benchmark and its computed metrics
  (`domain::analysis`).
- **Provider** — an adapter that streams observations from an endpoint
  (`infrastructure::geyser`).
- **Clock offset** — host-vs-UTC skew used to correct absolute latency
  (`domain::clock`, probed by `infrastructure::sntp`).

## How a `race`/`latency`/`full` run flows

1. **`main`** parses the CLI, builds a `BenchConfig` + `Endpoint`s
   (`domain::config`), and resolves the clock offset (probing
   `infrastructure::sntp`).
2. **`application::run::run_comparison`** spawns one `infrastructure::geyser`
   provider per endpoint. Each provider converts protobuf updates into
   `domain::timing` observations and feeds the shared `domain::collector::Comparator`.
3. When the transaction target is hit (or Ctrl+C / max-duration), the pipeline
   calls `domain::analysis::compute_run_summary`, which uses `domain::scoring`.
4. **`main`** hands the `RunSummary` to a `presentation` renderer.

`slots` and `throughput` follow the same shape with their own pipelines.

## Testing

- **Unit tests** live beside the domain/infra logic they cover (percentiles,
  scoring, comparator semantics, clock formula, NTP parsing, timestamp extraction).
- **`tests/pipeline.rs`** drives the public domain API end to end.
- **`tests/mock_server.rs`** runs the real Yellowstone provider against an
  in-process mock gRPC server over plaintext HTTP/2.
