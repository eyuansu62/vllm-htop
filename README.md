# vllm-htop

[![PyPI version](https://img.shields.io/pypi/v/vllm-htop.svg)](https://pypi.org/project/vllm-htop/)
[![Python](https://img.shields.io/pypi/pyversions/vllm-htop.svg)](https://pypi.org/project/vllm-htop/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**`htop` for [vLLM](https://github.com/vllm-project/vllm) inference servers.** Point it at one or more `/metrics` endpoints, get the right numbers, the right way, right now.

Zero dependencies. Single file. Python 3.8+.

```
vLLM DP Monitor  │  8/8 up  │  2026-05-19 14:23:01  (interval=2.0s)
──────────────────────────────────────────────────────────────────────────────────────────────────────────
 DP                          Status   Run  Wait  Swap   KV%   Cache%   in tok/s  out tok/s   TTFT-P95  TPOT-P95
──────────────────────────────────────────────────────────────────────────────────────────────────────────
 Llama-3.1-8B-Instruct.e0   OK         12    0     —   55.0%    78%       49793      16597       410ms     37.0ms
 Llama-3.1-8B-Instruct.e1   OK         11    0     —   58.0%    82%       47841      15947       415ms     38.0ms
 Llama-3.1-8B-Instruct.e2   OK         18    6     —   91.0%    65%       69738      23246       820ms     52.0ms
 Llama-3.1-8B-Instruct.e3   OK         12    0     —   57.0%    79%       48100      16100       420ms     38.0ms
 bge-large-zh-v1.5.e0       OK          3    0     —    8.0%     —           0       1234         —ms      —ms
 bge-large-zh-v1.5.e1       OK          3    0     —    8.0%     —           0       1198         —ms      —ms
──────────────────────────────────────────────────────────────────────────────────────────────────────────
 ALL                                    59    6     0   max91%   76%      215472      71890       512ms     41.0ms

▸ Imbalance check  Llama-3.1-8B-Instruct  (× 4 replicas)  ⚠ 1/4 failed
  ✓ Running req          range 11–18, median 12
  ✓ KV cache             range 55.0%–91.0%
  ⚠ slow-replica (TTFT)  Llama-3.1-8B-Instruct.e2: 820ms is 2.0× median (410ms)
  ✓ slow-decode (TPOT)   median 38.0ms, max 52.0ms (1.4×)

▸ Imbalance check  bge-large-zh-v1.5  (× 2 replicas)  ✓ all 3 checks pass

▸ Cost  (estimated)
  Token-based  ($0.5/M in, $1.5/M out)
    Lifetime         :     $165.17  ($75.08 in + $90.09 out)
    This session     :       $0.13  (since vllm-htop attached)
    Current rate     :       $3.86/min  ($231.55/hour at current throughput)
  Compute-based  (NVIDIA H100 80GB × 8 @ $3.39/h — RunPod Secure reference)
    Burn rate        :      $27.12/hour  (paid whether busy or idle)
    Lifetime         :      $54.24      (over 2h00m of vLLM uptime)
    This session     :       $0.92      (over 2m11s)
  Margin (token revenue ÷ compute cost)
    At current load  :       8.54×  ($231.55/h revenue vs $27.12/h compute)
```

## At a glance

- **Auto-discovers** vLLM endpoints on the host — no `--url` needed for typical local setups
- **Auto-splits internal DP** — `vllm serve --data-parallel-size N` becomes N rows automatically
- **Model-aware row names** — `Llama-3.1-8B.e0` instead of `0.e0`, so mixed deployments (LLM + embedding) are readable
- **Windowed + long-window percentiles** — P50/P95/P99 over ~2s, plus stabilized `P95@1m` for SLO reads
- **Prefix cache hit rate** column when vLLM exposes it
- **Imbalance check** that points to the bad replica by name, median-based and grouped by model
- **Cost estimation** — token-based, compute-based (auto-detected from `nvidia-smi`), and a margin row
- **htop-style alt-screen** rendering — fixed window refresh, scrollback stays clean
- **JSON output** mode for piping into scripts, logs, or alerting
- **Trend sparklines** in detail view — 60-sample rolling history per metric
- **Cross-DP percentile aggregation** done correctly (merged buckets, not averaged P95s)
- **Fault-tolerant** — DOWN / STALE replicas surface without breaking the table

## Install

```bash
# Recommended: uvx (zero install, always fresh)
uvx vllm-htop@latest

# pip
pip install vllm-htop
vllm-htop

# Or grab the single file and run it
curl -O https://raw.githubusercontent.com/eyuansu62/vllm-htop/main/vllm_htop.py
python vllm_htop.py
```

## Quick start

```bash
vllm-htop
```

With no flags, `vllm-htop`:

1. Scans `localhost:8000-8015` for vLLM endpoints (parallel TCP + `/metrics` probe, <100ms)
2. Detects internal DP via the `engine="N"` label and expands each URL into per-engine rows
3. Picks table view if it found ≥2 replicas, detail view otherwise
4. Runs `nvidia-smi` to detect local GPUs and shows compute cost when the model matches the built-in price table
5. Refreshes every 2s in alt-screen mode (no scrollback pollution)
6. Ctrl-C exits; original terminal contents return

## Features

### Multi-replica DP — comparison table

```bash
vllm-htop --url http://h1:8000 http://h2:8000 http://h3:8000 http://h4:8000

# Comma-separated
vllm-htop --url http://h1:8000,http://h2:8000,http://h3:8000,http://h4:8000

# Shell brace expansion
vllm-htop --url http://localhost:{8000,8001,8002,8003}
```

Compact per-replica rows + aggregate `ALL` row + cross-replica imbalance check.

### Auto-discovery

If you don't pass `--url`, `vllm-htop` scans the configured port range for vLLM-shaped `/metrics` endpoints. Open ports get an HTTP probe checking for the `vllm:` metric prefix; non-vLLM services on the same ports are filtered out.

```bash
vllm-htop                                  # implicit, falls back to localhost:8000 if nothing's found
vllm-htop --auto                           # forced; fails loudly if nothing found
vllm-htop --auto --host 10.0.0.7           # remote host
vllm-htop --auto --port-range 9000-9031    # wider range
```

### Internal DP — auto-split

`vllm serve --data-parallel-size N` exposes one `/metrics` endpoint with `engine="0".."N-1"` labels. `vllm-htop` detects this on first contact and expands the URL into one virtual replica per engine — the comparison table, imbalance check, and aggregate percentiles work just like for separate-process DP.

Row naming chooses the most informative form available:

| Setup | Names |
|---|---|
| External DP (N URLs, no engine) | `0`, `1`, … |
| Internal DP (1 URL, N engines) | `e0`, `e1`, … |
| Mixed (M URLs × N engines each) | `0.e0`, `0.e1`, `1.e0`, … |
| Model name extractable from labels | `Llama-3.1-8B-Instruct.e0`, `bge-large-zh.e1`, … |

When `model_name` labels are present and distinct across URLs, the names use the model — so multi-model deployments (e.g. LLM + embedding on the same host) are readable at a glance. On name collisions (two URLs serving the same model) the tool falls back to URL indices to keep rows unique.

### Imbalance check

When ≥2 replicas serve the same model, `vllm-htop` runs four checks for cross-replica anomalies. Healthy state collapses to one line; warnings name the bad replica explicitly.

```
▸ Imbalance check  (× 4 replicas)  ⚠ 1/4 failed
  ✓ Running req          range 5–8, median 6
  ✓ KV cache             range 40.0%–46.0%
  ⚠ slow-replica (TTFT)  Llama-3.1-8B.e3: 979ms is 5.2× median (188ms)
  ✓ slow-decode (TPOT)   median 38.0ms, max 52.0ms (1.4×)
```

| Check | Threshold | Means |
|---|---|---|
| Running req | Δ > 3 **and** max > 1.5× median | ⚠ load-balancer skew / sticky session |
| KV cache | Δ > 15 percentage points | ⚠ uneven KV pressure (prefix-cache asymmetry?) |
| TTFT P95 | max / median > 1.5× | ⚠ slow replica (GPU thermal, NCCL, contention) |
| TPOT P95 | max / median > 1.5× | ⚠ slow decode |

Two design choices worth noting:

- **median, not min**, as the baseline ratio denominator. Min would be dragged to zero by any idle replica and produce misleading 75× ratios.
- **grouped by model**, so a mixed LLM+embedding deployment doesn't cross-compare workloads that are fundamentally different.

### Cost estimation

Two independent pricing models, either or both can be on:

```bash
# Token-based: explicit prices in $/M tokens (OpenAI-style)
vllm-htop --cost-in 0.50 --cost-out 1.50

# Compute-based: auto-detected from nvidia-smi
vllm-htop                                      # auto
vllm-htop --gpu-cost-hour 2.99 --num-gpus 8    # explicit override
vllm-htop --no-gpu-detect                      # disable auto-detect

# Both — also surfaces the Margin row
vllm-htop --cost-in 0.50 --cost-out 1.50 --gpu-cost-hour 2.99 --num-gpus 8
```

Each model reports **Lifetime** (since vLLM started), **This session** (since vllm-htop attached), and a **Current rate** / **Burn rate** for the live read. Margin is `token-revenue ÷ compute-cost` — colored green ≥2× / yellow ≥1× / red <1×.

**Built-in GPU price hints** cover:

- **Blackwell** datacenter: B200, B100, GB200
- **Blackwell** workstation/consumer: RTX PRO 6000, RTX 5090, RTX 5080
- **Hopper**: H100, H100 NVL, H200
- **Hopper China-market**: H20-3e, H20
- **Ampere**: A100 (40/80GB), A40, A30, A10, A10G, RTX A6000/A5000/A4000, RTX 3090
- **Ada Lovelace**: L40S, L40, **L20** (China), L4, RTX 6000 Ada, RTX 4090, RTX 4080
- **Older datacenter**: V100, T4

Prices are anchored to **RunPod Secure tier** published rates (2026-05) — what OpenRouter-class token-API providers (Lambda, Hyperbolic, DeepInfra, …) typically pay for their compute. Cross-provider variance:

| Reference | Vs. our hints |
|---|---|
| AWS / GCP on-demand | 3-5× higher |
| Lambda Labs | within ±10% |
| RunPod Community | 20-40% lower |
| vast.ai community | 30-50% lower |

Treat the numbers as ±30% ballpark; override `--gpu-cost-hour` for anything serious.

### Long-window P95

In the detail view's Latency section, the standard `P95` column reflects only the latest poll-to-poll delta — noisy, often `—` when no requests completed in those 2s. The `P95@1m` column shows the same percentile over the last ~60 seconds of accumulated samples — much more stable, what you'd actually use for an SLO read.

```
▸ Latency  (windowed percentiles)
  metric             P50       P95       P99    P95@1m
  TTFT  (ms)       100.0     916.7    3758.6     520.3
  TPOT  (ms)         8.3      77.8     100.0      65.1
```

### Prefix cache hit rate

When vLLM exposes `vllm:prefix_cache_queries_total` / `vllm:prefix_cache_hits_total`, the table view picks up a `Cache%` column (green ≥60% / yellow ≥30% / red <30%) and the `ALL` row shows a query-weighted aggregate. The detail view's Saturation block reports both window and lifetime rates.

```
  Prefix cache hit  :  78.4% window   76.1% life
```

### Trend sparklines (detail view)

Rolling 60-sample history for the metrics that change most:

```
▸ Trend  (last 60 samples, newest on right)
  Running        :              ▆▆▇▇██▅▆▆▇▇█  min 10 max 16 now 15
  KV cache %     :              ▄▄▅▅▆▆▆▄▄▄▅▆  min 40.0% max 75.0% now 60.0%
  in tok/s       :               █▃▆▂▁▄▄█▃█▆  min 12244 max 12411 now 12411
  out tok/s      :               █▃▆▂▁▄▄█▃█▆  min  4898 max  4964 now  4964
  TTFT P95 ms    :              ████████████  min   917 max   917 now   917
  TPOT P95 ms    :              ████████████  min  77.8 max  77.8 now  77.8
```

Counters and KV% pin to 0 baseline so the bar height reflects absolute level; rates and latencies auto-scale so motion stays visible.

### JSON output for scripting

```bash
# Pipe one snapshot to jq
vllm-htop --output json --once | jq '.aggregate.kv_pct_max'
vllm-htop --output json --once | jq '.cost.compute_based.burn_rate_per_hour'

# Stream JSONL to a log file
vllm-htop --output json --interval 5 >> /var/log/vllm-htop.jsonl
```

Each poll emits one JSON object on stdout. The schema covers per-replica gauges, throughput, latency (windowed + long-window), lifetime counters, session peaks, the aggregate row, and the cost section.

### htop-style alt-screen rendering

In interactive mode (TTY + continuous polling), `vllm-htop` uses the terminal's alternate screen buffer — the same mechanism as `htop`, `vim`, `less`. Successive refreshes overwrite a fixed window; on exit, the original terminal contents return (the vllm-htop output is not left in scrollback).

Falls back to plain printing automatically when:

- `--once` is set (one-shot snapshot, you might want to capture it)
- `--output json` (structured output for pipelines)
- stdout is captured (`> out.log`, `| tee` — `isatty()` returns False)

## CLI reference

| Flag | Default | What it does |
|---|---|---|
| `--url URL [URL ...]` | _(auto-discovery)_ | Explicit base URLs. Space- or comma-separated, shell brace expansion supported |
| `--auto` | _(implicit)_ | Force discovery, fail loudly if nothing found |
| `--host HOST` | `localhost` | Hostname for `--auto` discovery and the fallback URL |
| `--port-range LO-HI` | `8000-8015` | Port range for `--auto` |
| `--interval N` | `2.0` | Refresh interval in seconds |
| `--timeout N` | `4.0` | Per-endpoint fetch timeout |
| `--once` | off | Print one snapshot and exit |
| `--output MODE` | `auto` | `auto`/`table`/`detail`/`json`. `json` emits JSONL |
| `--table` | off | Force table view (legacy; use `--output table`) |
| `--detail` | off | Force detail view (legacy; use `--output detail`) |
| `--cost-in PRICE` | off | USD per 1M input (prompt) tokens — enables token cost |
| `--cost-out PRICE` | off | USD per 1M output (generation) tokens |
| `--gpu-cost-hour PRICE` | _auto_ | USD per GPU-hour. Defaults to nvidia-smi + built-in price hint |
| `--num-gpus N` | _auto_ | GPU count. Defaults to nvidia-smi count |
| `--no-gpu-detect` | off | Skip nvidia-smi auto-detect entirely |
| `--currency SYM` | `$` | Currency symbol shown in the Cost section |
| `-V`, `--version` | — | Print version and exit |
| `-h`, `--help` | — | Help |

## Concepts

### Time-scale of every metric

`vllm-htop` mixes several time scales — each answers a different question:

| Scale | Examples | Source | Best for |
|---|---|---|---|
| **Instantaneous** | Run, Wait, Swap, KV% | gauge at this poll | "What's the state right now?" |
| **Windowed** (~2s) | in/out tok/s, TTFT-P95, Cache% | counter / histogram-bucket deltas | "What's been happening this second?" |
| **Long-window** (~60s) | `P95@1m` column | bucket delta over snapshots ≤60s old | "What's the SLO state?" |
| **Trend** (~2 min) | sparklines in detail view | rolling 60-sample buffer | "Is something trending up or down?" |
| **Lifetime** | life-Prompt/Output/Reqs, lifetime cost | vLLM `*_total` counters | "How much total work since vLLM started?" |
| **Session** | peak-Run/KV, this-session cost | tracked since `vllm-htop` attached | "How much during my monitoring window?" |

### Aggregating percentiles across DP

The `ALL` row's P95 is computed by **merging raw histogram buckets** across replicas and then taking the percentile of the merged distribution. Averaging per-replica P95s is mathematically wrong — `mean(P95)` isn't `P95(union)`. This matters most when one replica is hot and others are idle: averaging would understate the tail.

### Time-based vs token-based cost

These answer different questions, and both are useful:

- **Compute-based** (`$/h × N × uptime`) — what's actually leaving your account
- **Token-based** (`tokens × $/M`) — what the inference would cost (or is worth) at API prices
- **Margin** (`token revenue ÷ compute cost`) — whether the GPU is paying for itself

Self-host LLM as an API: watch margin. Internal-only tool: compute is what matters. Researcher/benchmarker: tokens-burned is a hardware-independent yardstick.

### Fault tolerance

- **DOWN** replicas (fetch failed and no prior snapshot) appear as a row with the error, but don't break the aggregate or imbalance check.
- **STALE** when the latest fetch failed but we have an older snapshot — useful through transient network blips.
- **Parallel polling** via `ThreadPoolExecutor` — total refresh ≈ slowest single fetch, regardless of replica count.
- **Substring metric-name matching** so version drift between `vllm:gpu_cache_usage_perc` and `vllm:kv_cache_usage_perc` doesn't break anything.

## Why?

vLLM exports a rich Prometheus `/metrics` endpoint, but:

- Production Prometheus + Grafana is overkill when you just SSH'd in and want to know if a server is healthy *right now*.
- `curl /metrics | grep` can't compute windowed percentiles, rates, or cross-replica aggregates.
- The default vLLM Grafana dashboard doesn't surface cross-replica imbalance — which is the most common operational failure mode for DP setups.

`vllm-htop` sits between Grafana (always-on, persistent) and `curl` (one-off, raw). Single binary, ssh-friendly, zero ops setup.

## Limitations

- **No alerting** — this is a viewer, not a notifier. For real alerting see [Andrey Krisanov's vLLM Prometheus rules](https://akrisanov.com/vllm-metrics/).
- **Peaks are in-memory only** — when the script exits, session peaks are lost. For long-term persistence, use Prometheus.
- **GPU price hints are ballpark** — RunPod-anchored medians, ±30% across providers. Pass `--gpu-cost-hour` for accuracy.
- **`nvidia-smi` auto-detection only works on the host running vLLM** — if you SSH'd in from your laptop and ran `vllm-htop` against `localhost`, the GPU detection sees the local box (correct). If you point `--url` at a remote vLLM, the local GPU info isn't relevant; pass explicit `--gpu-cost-hour`.

## Acknowledgments

The vLLM project for [exposing rich metrics by default](https://docs.vllm.ai/en/stable/design/metrics/), and the [reference Grafana dashboard](https://github.com/vllm-project/vllm/tree/main/examples/online_serving/prometheus_grafana) that informed the choice of which metrics matter most.

## License

MIT
