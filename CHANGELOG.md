# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-05-18

### Added
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
