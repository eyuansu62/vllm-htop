# vllm-htop

`htop` for [vLLM](https://github.com/vllm-project/vllm) inference servers — point it at one or more `/metrics` endpoints and get the right numbers, the right way, right now.

Zero dependencies. Single file. Python 3.8+.

```
vLLM DP Monitor  │  4/4 up  │  2026-05-18 14:23:01  (interval=2.0s)
──────────────────────────────────────────────────────────────────────────────────
 DP  Status   Run  Wait  Swap   KV%      in tok/s  out tok/s   TTFT-P95  TPOT-P95
──────────────────────────────────────────────────────────────────────────────────
 0   OK       12     0     0    55.0%       49793      16597       410ms     37.0ms
 1   OK       11     0     0    58.0%       47841      15947       415ms     38.0ms
 2   OK       18     6     0    91.0%       69738      23246       820ms     52.0ms
 3   OK       12     0     0    57.0%       48100      16100       420ms     38.0ms
──────────────────────────────────────────────────────────────────────────────────
 ALL          53     6     0   max91.0%      215472      71890       512ms     41.0ms

▸ Imbalance check  (across 4 replicas)
  Running req     :    11  →  18    (Δ=7)             ⚠ load-balancer skew?
  KV cache        :  55.0% → 91.0%  (Δ=36.0pp)        ⚠ uneven KV pressure
  TTFT P95        :   410ms → 820ms (2.00×)           ⚠ slow replica
  TPOT P95        :  37.0ms → 52.0ms (1.41×)

▸ Cumulative  (life = vLLM counters · sess = peaks observed since monitor uptime 12m34s)
──────────────────────────────────────────────────────────────────────────────────
 DP   life-Prompt  life-Output  life-Reqs   peak-Run  peak-Wait  peak-KV%   peak in/out tok/s
──────────────────────────────────────────────────────────────────────────────────
 0        12.3M         3.4M        10.2K       19         4     71.3%   52.1K/17.4K
 1        11.9M         3.3M         9.9K       17         2     67.8%   50.3K/16.8K
 2        13.1M         3.7M        11.2K       28        12     91.0%   72.4K/24.1K
 3        12.1M         3.4M        10.1K       18         3     68.5%   51.2K/17.1K
──────────────────────────────────────────────────────────────────────────────────
 ALL      49.4M        13.8M       41.4K
```

## Why?

vLLM exports a rich Prometheus `/metrics` endpoint with everything you need to understand serving performance — TTFT/TPOT/E2E histograms, KV cache usage, queue depth, swap counts. But...

- ...running production Prometheus + Grafana is overkill when you just SSH'd in and want to know if a server is healthy *right now*
- ...`curl /metrics | grep` can't compute windowed percentiles or rates
- ...when you run **Data Parallel** replicas, you really want side-by-side comparison and imbalance detection, which the default vLLM Grafana dashboard doesn't surface at all

`vllm-htop` is the thing you reach for between Grafana (always-on, persistent) and `curl` (one-off, raw). It complements both — not a replacement.

## Install

The fastest way — no install needed (recommended):

```bash
uvx vllm-htop --url http://localhost:8000
```

With pip:

```bash
pip install vllm-htop
vllm-htop --url http://localhost:8000
```

Or just grab the single file and run it (no dependencies needed beyond Python 3.8+):

```bash
curl -O https://raw.githubusercontent.com/eyuansu62/vllm-htop/main/vllm_htop.py
python vllm_htop.py --url http://localhost:8000
```

## Usage

### Single instance — detail view

```bash
vllm-htop --url http://localhost:8000
```

Shows P50/P95/P99 across TTFT/TPOT/E2E/Queue, current saturation gauges, and lifetime cumulative.

### DP / multiple replicas — comparison table

```bash
# Space-separated
vllm-htop --url http://h1:8000 http://h2:8000 http://h3:8000 http://h4:8000

# Comma-separated
vllm-htop --url http://h1:8000,http://h2:8000,http://h3:8000,http://h4:8000

# Shell brace expansion (most concise)
vllm-htop --url http://localhost:{8000,8001,8002,8003}
```

Automatically switches to compact per-replica rows + aggregate + imbalance check.

### Auto-discovery — one machine, many DP replicas

If you don't pass `--url`, `vllm-htop` scans `localhost:8000-8015` for vLLM-shaped `/metrics` endpoints and attaches to whatever it finds. So when you have multiple `vllm serve` processes on the same host (one per port), monitoring all of them is just:

```bash
vllm-htop
```

It narrates the discovery only when interesting (≥2 endpoints found, or `--auto` was explicit); the single-instance case stays quiet.

```bash
# Force discovery (fails loudly if nothing's found — useful in scripts)
vllm-htop --auto

# Wider range, different host
vllm-htop --auto --host 10.0.0.7 --port-range 9000-9031
```

Discovery does a parallel TCP probe over the range, then HTTP-probes only the open ports for the `vllm:` metric-name prefix, so it's fast (typically <100ms on a localhost scan) even on wide ranges. Non-vLLM services on the same ports are filtered out, not confused for replicas.

If discovery turns up nothing and you didn't pass `--auto`, the tool falls back to `http://<host>:8000` and surfaces the real fetch error there — more useful than a generic "no endpoints found".

### Cost estimation (optional)

Two independent pricing models, either or both can be on:

**Token-based** — explicit prices in $/1M tokens (OpenAI-style convention):

```bash
vllm-htop --cost-in 0.50 --cost-out 1.50
```

**Compute-based** — auto-detected from `nvidia-smi`, with a built-in GPU price-hint table:

```bash
# Just run it. If `nvidia-smi` is on PATH, vllm-htop reads the GPU model and
# count, looks up a community-market reference rate, and shows compute burn.
vllm-htop

# Or override the rate / count explicitly:
vllm-htop --gpu-cost-hour 2.99 --num-gpus 8

# Skip the auto-detect entirely:
vllm-htop --no-gpu-detect
```

The built-in hints cover:
- **Blackwell** datacenter: B200, B100, GB200
- **Blackwell** workstation/consumer: RTX PRO 6000, RTX 5090, RTX 5080
- **Hopper**: H100, H100 NVL, H200
- **Ampere**: A100 (40/80GB), A40, A30, A10, A10G, RTX A6000/A5000/A4000, RTX 3090
- **Ada Lovelace**: L40S, L40, L4, RTX 6000 Ada, RTX 4090, RTX 4080
- **Older datacenter**: V100, T4

Prices are anchored to **RunPod Secure tier** published rates as of 2026-05 — this is what OpenRouter-class token-API providers (Lambda, Hyperbolic, DeepInfra, …) typically pay for their compute, so it's the most representative "GPU rental cost" for someone running their own vLLM stack. Cross-provider variance:

- AWS / GCP on-demand: 3-5× higher
- Lambda Labs: within ±10%
- RunPod Community: 20-40% lower
- vast.ai community: 30-50% lower (high variance)

Treat the numbers as a ballpark (±30%) and override via `--gpu-cost-hour` for anything serious.

**Both at once** — also surfaces a `Margin` row (token revenue ÷ compute cost):

```bash
vllm-htop --cost-in 0.50 --cost-out 1.50 --gpu-cost-hour 2.99 --num-gpus 8
```

Example output:

```
▸ Cost  (estimated · sum across 3 replicas)
  Token-based  ($0.5/M in, $1.5/M out)
    Lifetime     :      $165.17  ($75.08 in + $90.09 out)
    This session :        $0.13  (over 2m11s)
    Current rate :        $3.86/min  ($231.55/hour at current throughput)
  Compute-based  (NVIDIA H100 80GB HBM3 × 8 @ $2.99/h — auto-detected, estimate)
    Burn rate    :       $23.92/hour  (paid whether busy or idle)
    This session :         $0.87  (over 2m11s)
  Margin (token revenue ÷ compute cost)
    At current load :       9.68×  ($231.55/h revenue vs $23.92/h compute)
```

The Cost section is hidden when no pricing is configured (no `--cost-*` flags **and** GPU auto-detect found nothing).

### Flags

| Flag | Default | What it does |
|---|---|---|
| `--url URL [URL ...]` | _(auto-discovery)_ | Explicit vLLM base URLs. Overrides auto-discovery |
| `--auto` | _(implicit default)_ | Force discovery, fail loudly if nothing found. Without `--url`, discovery already runs implicitly |
| `--host HOST` | `localhost` | Hostname for discovery and the fallback URL |
| `--port-range LO-HI` | `8000-8015` | Port range for discovery (e.g. `8000-8015`, `8000:8015`) |
| `--interval N` | `2.0` | Refresh interval in seconds |
| `--timeout N` | `4.0` | Per-endpoint fetch timeout |
| `--once` | off | Print one snapshot and exit (good for cron / CI smoke tests) |
| `--table` | auto | Force compact table view |
| `--cost-in PRICE` | off | USD per 1M input (prompt) tokens — enables token-based Cost section |
| `--cost-out PRICE` | off | USD per 1M output (generation) tokens |
| `--gpu-cost-hour PRICE` | _auto_ | USD per GPU-hour. Defaults to a built-in hint based on `nvidia-smi` detection |
| `--num-gpus N` | _auto_ | GPU count. Defaults to `nvidia-smi` count |
| `--no-gpu-detect` | off | Skip `nvidia-smi` auto-detection entirely |
| `--currency SYM` | `$` | Currency symbol shown in the Cost section |
| `--detail` | auto | Force per-instance detail view |

## What it shows

### Throughput (windowed)
Token and request rates computed from the delta between the last two polls — reflects *recent* behavior, not lifetime average.

### Latency (windowed percentiles)
P50/P95/P99 for TTFT, TPOT, E2E, queue time. Percentiles come from histogram bucket *deltas* between polls — equivalent to Prometheus' `histogram_quantile(0.95, rate(..._bucket[Δ]))`.

### Saturation (current gauges)
Running / waiting / swapped requests, plus KV cache usage with a colored bar (green < 65%, yellow < 85%, red ≥ 85%).

### Imbalance check (DP only, ≥2 replicas)

| Check | Threshold | Means |
|---|---|---|
| Running req | ratio > 1.5× **and** Δ > 3 | ⚠ load-balancer skew / sticky session |
| KV cache | Δ > 15 percentage points | ⚠ uneven KV pressure (prefix-cache asymmetry?) |
| TTFT P95 | max/min > 1.5× | ⚠ slow replica (GPU thermal, NCCL, contention) |
| TPOT P95 | max/min > 1.5× | ⚠ slow decode |

### Cumulative

Two clearly-labelled sources:
- **`life`** — read directly from vLLM `*_total` counters: prompt tokens, output tokens, successful requests **since vLLM started**
- **`sess`** — peaks observed by the monitor since it started watching: peak running / waiting / KV% / tokens/s

`swap-seen` is **sticky** within a session: if swapping fires once, it stays red as a warning even after it recovers.

## Design notes

- **Aggregate percentiles across DP** are computed by merging histogram buckets — that's the only mathematically correct way to combine percentiles. Averaging per-replica P95s is wrong.
- **DOWN replicas are isolated** — they don't break the table, aggregate, or imbalance check. The header shows `3/4 up` and the offending row stays visible with its error.
- **STALE** status: last fetch failed but we have an older snapshot, useful for transient network blips.
- **Parallel polling** via `ThreadPoolExecutor` — refresh time stays ≈ slowest single fetch regardless of replica count.
- Metric-name matching is **substring-based** (`time_to_first_token`, `cache_usage_perc`) so the tool tolerates vLLM version drift between `vllm:gpu_cache_usage_perc` and `vllm:kv_cache_usage_perc`.

## Internal DP (engine labels) is auto-split

When you run `vllm serve --data-parallel-size N`, vLLM exposes one `/metrics` endpoint whose samples are tagged with `engine="0".."N-1"`. `vllm-htop` detects this on first contact and **expands the single URL into one virtual replica per engine** — so the comparison table, imbalance check, and aggregate percentiles all work just like they do for separate-process external DP.

Naming convention in the table:

| Setup | Replica names |
|---|---|
| Pure external (N URLs, no engine label) | `0`, `1`, `2`, … |
| Pure internal (1 URL, N engines) | `e0`, `e1`, `e2`, … |
| Mixed (M URLs × N engines each) | `0.e0`, `0.e1`, `1.e0`, … |

No new flag — detection runs automatically on startup.

## Limitations

- **Peaks are in-memory only** — when the script exits, session peaks are lost. For long-term persistence, use Prometheus.
- **No alerting** — this is a viewer, not a notifier. For real alerting see [Andrey Krisanov's vLLM Prometheus rules](https://akrisanov.com/vllm-metrics/) as a starting point.

## Acknowledgments

The vLLM project for [exposing rich metrics by default](https://docs.vllm.ai/en/stable/design/metrics/), and for shipping a [reference Grafana dashboard](https://github.com/vllm-project/vllm/tree/main/examples/online_serving/prometheus_grafana) that informed the choice of which metrics matter most.

## License

MIT
