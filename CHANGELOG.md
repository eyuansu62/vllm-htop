# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] — 2026-05-19

### Added
- **Lifetime compute cost** in the Cost section's Compute-based subsection — computed from `process_start_time_seconds` (the standard prometheus_client metric vLLM auto-exports) × `$/h × N`. Symmetric with the token-based Lifetime row, both reflect "since the vLLM process started." JSON output exposes it as `cost.compute_based.lifetime_total` plus `vllm_uptime_seconds`. Falls back gracefully when the metric isn't available.

### Changed
- **Imbalance check redesigned** for actionability. Three concrete improvements:
  - **Healthy case collapses to one line** (`✓ all N checks pass`) so the section disappears visually when nothing is wrong.
  - **Outlier replica is named** in the warning (`Llama-3.1-8B.e3: 979ms is 5.2× median`) — no more cross-referencing the table above to identify which row is slow.
  - **Median-based ratio** (`max / median`) instead of `max / min`. Robust to idle replicas that previously dragged `min` to zero and produced misleading 75× ratios. Threshold remains 1.5× since median is a stronger baseline.
  - **Grouped by model**: each served model gets its own imbalance section, so mixed deployments (e.g. LLM + embedding) don't cross-compare workloads that are inherently different.
- **htop-style alt-screen rendering** in interactive mode. The monitor now claims the terminal's alternate screen buffer for its lifetime (same mechanism as `htop`, `vim`, `less`) — successive refreshes overwrite the same fixed window rather than scrolling new frames into history, and the original terminal contents are restored on Ctrl-C. Falls back to plain printing when output is captured (`> out.log`, `| tee`), one-shot (`--once`), or in JSON mode, so pipelines and scripting are unaffected.

### Added
- **Long-window P95** in the detail view's Latency section: a new `P95@1m` column showing the percentile over the last ~60 seconds of accumulated histogram samples, alongside the existing noisy 2s `P95`. Bridges the gap between "what just happened" (twitchy, often `—`) and "lifetime average" (too smoothed). Backed by a 10-minute rolling snapshot buffer per replica. Configurable via `LONG_WINDOW_SECS` (planned: CLI flag).
- **Prefix cache hit rate** — surfaced from `vllm:prefix_cache_queries_total` / `vllm:prefix_cache_hits_total` whenever vLLM exposes them. Shows up as a new `Cache%` column in the table view (with green ≥60% / yellow ≥30% / red threshold coloring), a windowed-and-lifetime line in the detail view's Saturation section, and a weighted-aggregate in the table's ALL row.
- **JSON output mode** (`--output json`): emits one JSON object per poll on stdout, suitable for piping into scripts, log files, or alerting pipelines. Schema covers per-replica gauges, throughput, latency (incl. long-window P95), lifetime counters, session peaks, aggregate, and the Cost section.
  ```bash
  vllm-htop --output json --interval 5 >> /var/log/vllm-htop.jsonl
  vllm-htop --output json --once | jq '.aggregate.kv_pct_max'
  ```

## [0.2.2] — 2026-05-19

### Added
- **Row names now use the served model name** when it can be extracted from `/metrics` labels (`model_name`, `served_model_name`, or `model`). E.g. an `LLM + embedding` two-process deployment shows up as `Llama-3.1-8B-Instruct.e0..e5` / `bge-large-zh-v1.5.e0..e1` instead of the previous `0.e0..0.e5` / `1.e0..1.e1`. Falls back to URL indices when (a) no model name is exposed, or (b) two URLs serve the same model (would create ambiguous duplicates).
- Legend at the bottom of the table view is now model-centric: `Llama-3.1-8B-Instruct ×6 engines @ http://localhost:8000`, much more compact than listing every engine name.
- GPU price table now covers NVIDIA's China-market Hopper variants (`H20-3e`, `H20`) and Ada variant (`L20`) — anchored to mainland-China rental rates (AutoDL / GpuMall / Aliyun mid-tier).

### Fixed
- `lookup_gpu_price()` now normalizes hyphens on the hint side too, so hints like `"H20-3E"` and `"A100 80GB"` work uniformly regardless of whether `nvidia-smi` emits hyphens (`NVIDIA H20-3e`, `A100-SXM4-80GB`) or spaces (`A100 80GB PCIe`).

## [0.2.1] — 2026-05-18

> Skipped 0.2.0 on PyPI — everything from that working-version batch ships here.



### Added
- **vLLM internal DP is now auto-split** in both detail and table views. When a single `/metrics` endpoint exposes multiple `engine="N"` labels (typical of `vllm serve --data-parallel-size N`), `vllm-htop` detects them on first contact and expands the URL into one virtual replica per engine — so the table shows per-engine rows, the imbalance check runs across engines, and aggregate percentiles are correctly merged. External DP (multiple URLs) and internal DP (engine labels) can be combined; replicas are named `0`/`1` for pure external, `e0`/`e1` for pure internal, and `0.e0`/`1.e1` for mixed setups. No new flag — detection is automatic.
- Table view auto-sizes the DP column width for longer engine-split names, and dedupes the URL legend when multiple engines share an endpoint.
- **▸ Cost section** in both detail and table views, supporting two independent pricing models that can be enabled together:
  - **Token-based** (opt-in via `--cost-in $/M` + `--cost-out $/M`, USD per 1 million tokens, OpenAI-style convention). Shows lifetime cost (from `*_total` counters), this-session cost (counter delta since attach), and current rate (windowed throughput × price) in $/min and $/hour.
  - **Compute-based** (auto-detected via `nvidia-smi --query-gpu=name`, with a built-in GPU price-hint table). Coverage: Blackwell (B200/B100/GB200, RTX PRO 6000, RTX 5090/5080), Hopper (H100/H100 NVL/H200), Ampere (A100 40/80GB, A40/A30/A10/A10G, RTX A6000/A5000/A4000, RTX 3090), Ada Lovelace (L40S/L40/L4, RTX 6000 Ada, RTX 4090/4080), and older datacenter chips (V100/T4). Prices anchored to **RunPod Secure tier** published rates as of 2026-05, which represents what OpenRouter-class token-API providers typically pay for compute. Cross-provider variance ≈ ±30% (AWS on-demand 3-5× higher; vast.ai community 20-40% lower). Shows hourly burn rate (paid whether busy or idle) and this-session cost. Overridable with `--gpu-cost-hour` and `--num-gpus`; opt out via `--no-gpu-detect`. GPU-name matching uses word-boundary token matching to correctly disambiguate variants (e.g. `A100-SXM4-80GB` vs `A100-SXM4-40GB`, `L4` vs `L40` vs `L40S`).
  - **Margin row** when both pricings are enabled: `token-revenue ÷ compute-cost` ratio, colored green ≥2× / yellow ≥1× / red <1×. Lets you immediately see whether throughput justifies the GPU bill.
  - Currency symbol configurable via `--currency`.
  - For multi-replica DP, all values are aggregated across replicas.

## [0.1.1] — 2026-05-18

### Added
- **Trend section** in single-instance detail view: rolling sparklines for Running / KV% / in tok/s / out tok/s / TTFT P95 / TPOT P95 (last 60 samples), each with a `min/max/now` readout. Counters and KV% use fixed-floor scaling; rates and latencies auto-scale so motion stays visible.
- **Auto-discovery as the default**: when `--url` is omitted, `vllm-htop` scans `localhost:8000-8015` for vLLM-shaped `/metrics` endpoints and attaches to whatever it finds. Single-instance happy path stays silent; multi-instance discovery is narrated on stderr. `--auto` (explicit) still forces discovery and fails loudly if nothing is found — useful in scripts. New flags `--host` and `--port-range`. Falls back to `http://<host>:8000` if implicit discovery turns up nothing, so the real fetch error surfaces.
- GitHub Actions workflow (`.github/workflows/publish.yml`) for tag-driven PyPI publishing via Trusted Publishing, with a tag-vs-`__version__` consistency check.

## [0.1.0] — 2026-05-18

Initial release.

### Added
- Real-time TUI for vLLM `/metrics` endpoints
- Single-instance detail view: P50/P95/P99 across TTFT/TPOT/E2E/Queue, current saturation gauges, cumulative section
- Multi-instance comparison table with auto-detection (≥2 URLs → table mode)
- Parallel polling via `ThreadPoolExecutor` — refresh stays constant regardless of replica count
- Cross-replica imbalance check: load-balancer skew, KV pressure, slow-replica TTFT, slow-decode TPOT
- Cumulative section: vLLM-lifetime counters (`life`) + monitor-session peaks (`sess`)
- Mathematically correct aggregate percentiles via histogram bucket merging
- Substring-based metric-name matching for version tolerance (e.g. `gpu_cache_usage_perc` vs `kv_cache_usage_perc`)
- DOWN/STALE handling for fault tolerance
- `__version__` attribute and `-V/--version` CLI flag
- Zero runtime dependencies (Python 3.8+, stdlib only)
