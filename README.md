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

## Limitations

- **vLLM internal DP** (`vllm serve --data-parallel-size N` where multiple ranks share one `/metrics` endpoint and label samples with `engine="0"`, `engine="1"`, etc.): currently all engines' samples are summed into one. For per-rank visibility, run a separate `vllm-htop` per endpoint, or open an issue for `--split-by-label` support.
- **Peaks are in-memory only** — when the script exits, session peaks are lost. For long-term persistence, use Prometheus.
- **No alerting** — this is a viewer, not a notifier. For real alerting see [Andrey Krisanov's vLLM Prometheus rules](https://akrisanov.com/vllm-metrics/) as a starting point.

## Acknowledgments

The vLLM project for [exposing rich metrics by default](https://docs.vllm.ai/en/stable/design/metrics/), and for shipping a [reference Grafana dashboard](https://github.com/vllm-project/vllm/tree/main/examples/online_serving/prometheus_grafana) that informed the choice of which metrics matter most.

## License

MIT
