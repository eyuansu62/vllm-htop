# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
