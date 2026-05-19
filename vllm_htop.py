#!/usr/bin/env python3
"""
vllm-htop — htop-style terminal monitor for vLLM inference servers.

Polls one or more vLLM /metrics endpoints and surfaces only the numbers that
matter:
  • Throughput  : prompt tokens/s, output tokens/s, successful req/s (windowed)
  • Latency    : TTFT, TPOT, E2E, queue time — P50/P95/P99 (windowed)
  • Saturation : running / waiting / swapped requests, KV cache usage
  • Cumulative : vLLM-lifetime totals + session-observed peaks

With multiple URLs (DP deployment), it switches to a per-replica comparison
table plus an imbalance check (the load-balancer-is-broken detector). All
endpoints are polled in parallel so refresh stays constant regardless of
replica count.

Aggregate latency percentiles across replicas are computed by *merging*
histogram buckets across replicas — averaging per-replica P95s would be
mathematically wrong.

Usage:
    # Single instance
    vllm-htop --url http://localhost:8000

    # DP deployment — multiple URLs (space- or comma-separated)
    vllm-htop --url http://h1:8000 http://h2:8000 http://h3:8000
    vllm-htop --url http://h1:8000,http://h2:8000,http://h3:8000

    # Same host, multiple ports (shell brace expansion)
    vllm-htop --url http://localhost:{8000,8001,8002,8003}

    vllm-htop --url ... --interval 5
    vllm-htop --url ... --once

Project: https://github.com/eyuansu62/vllm-htop
No third-party dependencies. Python 3.8+.
"""

import argparse
import re
import socket
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

__version__ = "0.2.1"


# ───────────────────────────── ANSI styling ──────────────────────────────
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, GRAY = (
    "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[90m",
)
CLEAR = "\033[2J\033[H"

# ───────────────────────── trend / sparkline config ──────────────────────
HISTORY_LEN = 60          # rolling samples kept per metric (≈2 min @ 2s interval)
SPARK_WIDTH = 30          # characters per sparkline
# 8 levels of vertical block. Space (idx 0) is reserved for "no data" padding.
SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"


# ─────────────────────── which metrics we surface ────────────────────────
HIST_METRICS: List[Tuple[str, str, float]] = [
    # (display label, name fragment, value scale → display unit)
    ("TTFT  (ms)",  "time_to_first_token",   1000.0),
    ("TPOT  (ms)",  "time_per_output_token", 1000.0),
    ("E2E    (s)",  "e2e_request_latency",      1.0),
    ("Queue  (s)",  "request_queue_time",       1.0),
]

GAUGE_FRAGMENTS = {
    "running":  "num_requests_running",
    "waiting":  "num_requests_waiting",
    "swapped":  "num_requests_swapped",
    "kv_cache": "cache_usage_perc",   # matches gpu_cache_usage_perc / kv_cache_usage_perc
}


# ────────────────────── Prometheus text-format parser ────────────────────
_SAMPLE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)')
_LABEL_RE  = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prom_text(text: str) -> Tuple[Dict[str, str], Dict[str, List[Tuple[Dict[str, str], float]]]]:
    types: Dict[str, str] = {}
    samples: Dict[str, List[Tuple[Dict[str, str], float]]] = {}
    for line in text.splitlines():
        line = line.rstrip()
        if not line:
            continue
        if line.startswith("# TYPE "):
            parts = line[7:].split(" ", 1)
            if len(parts) == 2:
                types[parts[0]] = parts[1]
            continue
        if line.startswith("#"):
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name, labels_str, value_str = m.groups()
        try:
            value = float(value_str)
        except ValueError:
            continue
        labels: Dict[str, str] = {}
        if labels_str:
            for lm in _LABEL_RE.finditer(labels_str):
                labels[lm.group(1)] = lm.group(2)
        samples.setdefault(name, []).append((labels, value))
    return types, samples


# ─────────────────────────── snapshot model ──────────────────────────────
@dataclass
class Snapshot:
    timestamp: float
    counters: Dict[str, float] = field(default_factory=dict)
    gauges: Dict[str, float] = field(default_factory=dict)
    histograms: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # histogram: name → {"buckets": {le: cum_count}, "count": x, "sum": x}


def parse_snapshot(raw: str, engine_filter: Optional[str] = None) -> Snapshot:
    """Parse raw /metrics text into a Snapshot.

    When `engine_filter` is None (default), all samples are aggregated — the
    legacy behavior, correct for endpoints that don't use vLLM's internal DP.

    When `engine_filter` is set (e.g. "0"), only samples carrying
    `engine="<value>"` are included. This is how we surface per-engine views
    of a single `/metrics` endpoint produced by
    `vllm serve --data-parallel-size N`.
    """
    types, samples = parse_prom_text(raw)
    snap = Snapshot(timestamp=time.time())

    def keep(labels: Dict[str, str]) -> bool:
        return engine_filter is None or labels.get("engine") == engine_filter

    for name, kind in types.items():
        if not name.startswith("vllm"):
            continue
        if kind == "counter":
            total = 0.0
            for sample_name in (name, name + "_total"):
                for labels, v in samples.get(sample_name, []):
                    if keep(labels):
                        total += v
            snap.counters[name] = total
        elif kind == "gauge":
            total = 0.0
            for labels, v in samples.get(name, []):
                if keep(labels):
                    total += v
            snap.gauges[name] = total
        elif kind == "histogram":
            buckets: Dict[str, float] = {}
            for labels, v in samples.get(name + "_bucket", []):
                if keep(labels):
                    le = labels.get("le", "+Inf")
                    buckets[le] = buckets.get(le, 0.0) + v
            count = sum(v for labels, v in samples.get(name + "_count", []) if keep(labels))
            sm    = sum(v for labels, v in samples.get(name + "_sum",   []) if keep(labels))
            snap.histograms[name] = {"buckets": buckets, "count": count, "sum": sm}
    return snap


def detect_engines(raw: str) -> List[str]:
    """Return sorted distinct values of the `engine` label, or [] if absent.

    vLLM's internal data-parallel mode labels samples with `engine="0".."N-1"`.
    A non-empty return value means this endpoint hosts multiple engines in one
    process; we use that to expand a single URL into multiple virtual Instances.
    """
    _types, samples = parse_prom_text(raw)
    engines: set = set()
    for sample_list in samples.values():
        for labels, _ in sample_list:
            eng = labels.get("engine")
            if eng is not None:
                engines.add(eng)
    return sorted(engines, key=lambda e: int(e) if e.isdigit() else e)


# ───────────────────────────── analytics ─────────────────────────────────
def find_metric(d: Dict[str, Any], fragment: str) -> Optional[str]:
    for name in d:
        if fragment in name:
            return name
    return None


def _le_to_float(le: str) -> float:
    if le == "+Inf":
        return float("inf")
    try:
        return float(le)
    except ValueError:
        return float("inf")


def histogram_percentile(buckets: Dict[str, float], total: float, q: float) -> Optional[float]:
    if total <= 0 or not buckets:
        return None
    sorted_buckets = sorted(buckets.items(), key=lambda kv: _le_to_float(kv[0]))
    target = q * total
    prev_le, prev_count = 0.0, 0.0
    for le_str, cum in sorted_buckets:
        if cum >= target:
            le_val = _le_to_float(le_str)
            if le_val == float("inf"):
                return prev_le if prev_le > 0 else None
            if cum > prev_count:
                frac = (target - prev_count) / (cum - prev_count)
                return prev_le + frac * (le_val - prev_le)
            return le_val
        if le_str != "+Inf":
            prev_le = _le_to_float(le_str)
        prev_count = cum
    return None


def window_buckets(curr: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> Tuple[Dict[str, float], float]:
    curr_b: Dict[str, float] = curr["buckets"]
    curr_c: float = curr["count"]
    if prev is None:
        return curr_b, curr_c
    prev_b: Dict[str, float] = prev["buckets"]
    delta = {le: max(0.0, c - prev_b.get(le, 0.0)) for le, c in curr_b.items()}
    delta_count = max(0.0, curr_c - prev["count"])
    return delta, delta_count


def merge_window_buckets(snaps: List[Tuple[Snapshot, Optional[Snapshot]]], fragment: str
                         ) -> Tuple[Dict[str, float], float]:
    """Merge windowed histograms across instances — the correct way to compute aggregate percentiles."""
    merged_b: Dict[str, float] = {}
    merged_c = 0.0
    for s, p in snaps:
        name = find_metric(s.histograms, fragment)
        if not name:
            continue
        prev_h = p.histograms.get(name) if p else None
        wb, wc = window_buckets(s.histograms[name], prev_h)
        for le, c in wb.items():
            merged_b[le] = merged_b.get(le, 0.0) + c
        merged_c += wc
    return merged_b, merged_c


# ─────────────────────────── cost model ─────────────────────────────────
#
# vllm-htop's Cost section supports two independent pricing models:
#
#   1. Token-based — opt-in via `--cost-in $/M` and `--cost-out $/M`.
#      Estimates "what this inference would cost at API prices."
#
#   2. Compute-based — auto-detected when `nvidia-smi` is on PATH. Looks up
#      the detected GPU model in a built-in price-hint table (community-market
#      median rates) and multiplies uptime × GPU count × $/h. Always
#      overridable via `--gpu-cost-hour` and `--num-gpus`.
#
# Both can be on simultaneously; the second is a useful cross-check on the
# first ("am I charging enough to cover the GPUs?").
#
# Price hints below are anchored to **RunPod Secure tier** published rates
# (2026-05 snapshot). Rationale: RunPod Secure is what OpenRouter-class
# token-API providers (Lambda, Hyperbolic, DeepInfra, …) typically pay for
# their compute, so it's the most representative "GPU rental cost" for
# someone running their own vLLM serving stack.
#
# Cross-provider sanity check:
#   - AWS / GCP on-demand: typically 3-5× higher than this table
#   - Lambda Labs:         within ±10% of this table
#   - RunPod Community:    typically 20-40% lower
#   - vast.ai community:   often 30-50% lower (high variance)
#
# Substring match — longer/more-specific hints first (so "H100 NVL" hits
# before plain "H100", "A100 80GB" before "A100", "RTX PRO 6000" before
# "RTX A6000", etc.). Override with --gpu-cost-hour for anything serious.
#
# Format: (substring, $/h per GPU). Source notes in trailing comment.
GPU_PRICE_HINTS: List[Tuple[str, float]] = [
    # Datacenter — Blackwell (B-series, 2024-2025+)
    ("GB200",         8.99),   # GB200 NVL72 per-GPU est; rarely sold standalone
    ("B200",          5.99),   # RunPod Secure: B200 SXM5 192GB HBM3e
    ("B100",          4.99),   # B100 PCIe variant
    # Datacenter — Hopper (H-series)
    ("H200",          3.99),   # RunPod Secure: H200 SXM5 141GB
    ("H100 NVL",      3.69),   # RunPod Secure: H100 NVL 94GB
    ("H100",          3.39),   # RunPod Secure: H100 80GB SXM5 / PCIe
    # Datacenter — Ampere (A-series)
    ("A100 80GB",     1.89),   # RunPod Secure
    ("A100",          1.59),   # RunPod Secure: A100 40GB
    ("A40",           0.79),   # RunPod Secure
    ("A30",           0.49),
    ("A10G",          0.79),   # AWS-only variant of A10
    ("A10",           0.69),
    # Datacenter — Ada Lovelace (L-series)
    ("L40S",          1.19),   # RunPod Secure
    ("L40",           0.99),
    ("L4",            0.49),
    # Datacenter — older (still common for hobby vLLM)
    ("V100 32GB",     0.59),
    ("V100",          0.49),
    ("T4",            0.29),
    # Workstation — Blackwell (RTX PRO + RTX 50-series)
    ("RTX PRO 6000",  2.29),   # Blackwell workstation, 96GB
    ("RTX 5090",      0.89),   # consumer Blackwell, 32GB GDDR7
    ("RTX 5080",      0.55),
    # Workstation — Ada (RTX 6000 ADA + RTX 40-series)
    ("RTX 6000 ADA",  1.49),   # Ada workstation, 48GB
    ("RTX 4090",      0.69),   # consumer Ada, 24GB
    ("RTX 4080",      0.39),
    # Workstation — Ampere (RTX A-series)
    ("RTX A6000",     0.79),   # 48GB, Ampere workstation
    ("RTX A5000",     0.59),
    ("RTX A4000",     0.39),
    # Older consumer (still seen in homelab vLLM)
    ("RTX 3090",      0.34),   # 24GB Ampere consumer
]


def lookup_gpu_price(name: str) -> Optional[float]:
    """Match a GPU name against the built-in hint table. First match wins.

    Uses *token-set* matching, not raw substring: a hint like "A100 80GB"
    splits into tokens {"A100", "80GB"} and matches if every token appears
    anywhere in the GPU name. This handles both nvidia-smi conventions —
    space-separated (`A100 80GB PCIe`) and hyphen-separated (`A100-SXM4-80GB`).
    Hint ordering still matters: more specific hints must come first
    (e.g. "A100 80GB" before plain "A100").
    """
    # Normalize: uppercase, swap separators for spaces, collapse whitespace.
    haystack = " ".join(name.upper().replace("-", " ").split())
    for hint, price in GPU_PRICE_HINTS:
        # Each token must appear as a whole word — `(?<![A-Z0-9])TOKEN(?![A-Z0-9])`
        # — so "A100" doesn't match "RTX A1000" and "L4" doesn't match "L40".
        if all(re.search(rf"(?<![A-Z0-9]){re.escape(t)}(?![A-Z0-9])", haystack)
               for t in hint.upper().split()):
            return price
    return None


def detect_gpus(timeout: float = 2.0) -> Optional[Tuple[str, int]]:
    """Best-effort GPU detection via nvidia-smi. Returns (model, count) or None.

    Returns None when nvidia-smi isn't on PATH (e.g. CPU box, container without
    GPU passthrough, AMD/Intel/Apple Silicon host). Caller handles fallback.
    """
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=timeout, check=True,
        )
    except (FileNotFoundError, subprocess.SubprocessError, OSError):
        return None
    names = [ln.strip() for ln in result.stdout.splitlines() if ln.strip()]
    if not names:
        return None
    # Most multi-GPU setups are homogeneous; return the first model as
    # representative. Heterogeneous boxes are rare for vLLM serving anyway.
    return names[0], len(names)


@dataclass
class CostConfig:
    """Configures the ▸ Cost section. Either pricing model may be enabled."""
    # Token pricing (opt-in via CLI)
    input_per_m:  float = 0.0   # $ per 1M prompt tokens
    output_per_m: float = 0.0   # $ per 1M generation tokens
    currency:     str   = "$"
    # Compute pricing (auto-detected or opt-in)
    gpu_cost_hour: float = 0.0
    num_gpus:      int   = 0
    gpu_model:     Optional[str] = None     # informational, for display
    gpu_price_source: str = ""              # "user" / "auto" / ""

    @property
    def token_enabled(self) -> bool:
        return self.input_per_m > 0 or self.output_per_m > 0

    @property
    def compute_enabled(self) -> bool:
        return self.gpu_cost_hour > 0 and self.num_gpus > 0

    @property
    def enabled(self) -> bool:
        return self.token_enabled or self.compute_enabled

    @property
    def compute_per_hour(self) -> float:
        return self.gpu_cost_hour * self.num_gpus

    def for_tokens(self, prompt: float, gen: float) -> Tuple[float, float, float]:
        """Return (total, input_cost, output_cost)."""
        ic = (prompt or 0) * self.input_per_m  / 1_000_000.0
        oc = (gen    or 0) * self.output_per_m / 1_000_000.0
        return ic + oc, ic, oc

    def for_seconds(self, secs: float) -> float:
        """Compute cost for an interval of wallclock seconds (uses both GPUs and rate)."""
        return self.compute_per_hour * (secs / 3600.0)


def fmt_money(v: Optional[float], symbol: str = "$") -> str:
    """Format a monetary value with thousands separators and adaptive precision."""
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1000:   return f"{symbol}{v:,.2f}"
    if av >= 1:      return f"{symbol}{v:.2f}"
    if av >= 0.01:   return f"{symbol}{v:.4f}"
    if av == 0:      return f"{symbol}0.00"
    return f"{symbol}{v:.6f}"


# ─────────────────────────── instance model ──────────────────────────────
@dataclass
class Instance:
    name: str
    url: str
    # When set, this Instance is one engine of a multi-engine /metrics endpoint
    # (vLLM internal DP). Fetches go to `url`, parsing filters by this label.
    engine: Optional[str] = None
    snapshot: Optional[Snapshot] = None
    prev: Optional[Snapshot] = None
    error: Optional[str] = None
    # Session tracking — populated on first successful fetch & kept updated
    first_seen: Optional[float] = None
    peak_running: float = 0.0
    peak_waiting: float = 0.0
    peak_swapped: float = 0.0
    peak_kv: float = 0.0          # percent (0-100)
    peak_prompt_rps: float = 0.0
    peak_gen_rps: float = 0.0
    # Rolling history (most recent HISTORY_LEN samples) for trend sparklines
    hist_running:    List[float] = field(default_factory=list)
    hist_kv:         List[float] = field(default_factory=list)  # percent
    hist_prompt_rps: List[float] = field(default_factory=list)
    hist_gen_rps:    List[float] = field(default_factory=list)
    hist_ttft_p95:   List[float] = field(default_factory=list)  # ms
    hist_tpot_p95:   List[float] = field(default_factory=list)  # ms
    # Cost-tracking baselines: counter values when the monitor first saw this
    # replica, so "session cost" can subtract them from current totals.
    baseline_prompt_tokens: Optional[float] = None
    baseline_gen_tokens:    Optional[float] = None


def fetch_metrics(url: str, timeout: float = 5.0) -> str:
    req = urllib.request.Request(url, headers={"Accept": "text/plain"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _update_session(inst: Instance) -> None:
    """Update running peaks observed since the script started watching this replica."""
    s = inst.snapshot
    if s is None:
        return
    if inst.first_seen is None:
        inst.first_seen = s.timestamp
        # Snapshot baseline counters so "this session" cost can be derived later.
        pn = find_metric(s.counters, "prompt_tokens")
        gn = find_metric(s.counters, "generation_tokens")
        if pn: inst.baseline_prompt_tokens = s.counters[pn]
        if gn: inst.baseline_gen_tokens    = s.counters[gn]

    # Peak gauges (current absolute values)
    def gauge(frag: str) -> Optional[float]:
        n = find_metric(s.gauges, frag)
        return s.gauges[n] if n else None

    r = gauge(GAUGE_FRAGMENTS["running"])
    if r is not None: inst.peak_running = max(inst.peak_running, r)
    w = gauge(GAUGE_FRAGMENTS["waiting"])
    if w is not None: inst.peak_waiting = max(inst.peak_waiting, w)
    sw = gauge(GAUGE_FRAGMENTS["swapped"])
    if sw is not None: inst.peak_swapped = max(inst.peak_swapped, sw)
    kv = gauge(GAUGE_FRAGMENTS["kv_cache"])
    if kv is not None:
        kv_pct = kv * 100 if kv <= 1.0 else kv
        inst.peak_kv = max(inst.peak_kv, kv_pct)

    # Peak rates (need both snapshots to compute window rate)
    if inst.prev is not None:
        dt = s.timestamp - inst.prev.timestamp
        if dt > 0:
            for frag, attr in (("prompt_tokens",     "peak_prompt_rps"),
                               ("generation_tokens", "peak_gen_rps")):
                n = find_metric(s.counters, frag)
                if n and n in inst.prev.counters:
                    rate = (s.counters[n] - inst.prev.counters[n]) / dt
                    if rate > getattr(inst, attr):
                        setattr(inst, attr, rate)


def _update_history(inst: Instance) -> None:
    """Append the latest derived metrics to rolling per-metric histories.

    Skips a metric for this tick if the value is unavailable (so the sparkline
    doesn't pretend we saw a zero when really we had nothing).
    """
    smry = summarize(inst)
    if smry is None:
        return

    def push(buf: List[float], v: Optional[float]) -> None:
        if v is None:
            return
        buf.append(v)
        if len(buf) > HISTORY_LEN:
            del buf[0]

    push(inst.hist_running, smry["running"])
    push(inst.hist_kv,      smry["kv_pct"])
    # Rates only exist on the 2nd+ poll
    push(inst.hist_prompt_rps, smry["prompt_rps"])
    push(inst.hist_gen_rps,    smry["gen_rps"])
    if smry["ttft_p95"] is not None:
        push(inst.hist_ttft_p95, smry["ttft_p95"] * 1000.0)
    if smry["tpot_p95"] is not None:
        push(inst.hist_tpot_p95, smry["tpot_p95"] * 1000.0)


def _apply_raw(inst: Instance, raw: str) -> None:
    """Update an Instance from raw /metrics text (applying its engine filter)."""
    try:
        inst.prev = inst.snapshot
        inst.snapshot = parse_snapshot(raw, engine_filter=inst.engine)
        inst.error = None
        _update_session(inst)
        _update_history(inst)
    except Exception as e:
        inst.error = f"{type(e).__name__}: {e}"


def fetch_all(instances: List[Instance], timeout: float = 5.0) -> None:
    """Parallel fetch — keeps total fetch time ≈ slowest single fetch.

    Multiple Instances may share a single URL (vLLM internal DP: one endpoint,
    N engines → N virtual Instances). We fetch each URL exactly once and
    demultiplex the response across its sub-instances via their engine filter.
    """
    if not instances:
        return

    # Group instances by URL so each URL is fetched once.
    by_url: Dict[str, List[Instance]] = {}
    for inst in instances:
        by_url.setdefault(inst.url, []).append(inst)

    def fetch_url(url: str) -> Tuple[str, Optional[str], Optional[Exception]]:
        try:
            return url, fetch_metrics(url, timeout=timeout), None
        except Exception as e:
            return url, None, e

    with ThreadPoolExecutor(max_workers=min(32, len(by_url))) as ex:
        results = list(ex.map(fetch_url, by_url.keys()))

    for url, raw, err in results:
        for inst in by_url[url]:
            if err is not None:
                inst.error = f"{type(err).__name__}: {err}"
            else:
                _apply_raw(inst, raw)


def expand_instances_by_engine(instances: List[Instance],
                               timeout: float = 5.0) -> List[Instance]:
    """Probe each URL once; for endpoints that expose multiple `engine` labels,
    expand the single Instance into one Instance per engine.

    Called once at startup. The returned list is what the monitor loop polls.
    If a URL is unreachable on this initial probe we keep the original Instance
    — it'll just render as DOWN until the network recovers.
    """
    def probe(inst: Instance) -> Tuple[Instance, Optional[str]]:
        try:
            return inst, fetch_metrics(inst.url, timeout=timeout)
        except Exception:
            return inst, None

    with ThreadPoolExecutor(max_workers=min(32, len(instances))) as ex:
        probed = list(ex.map(probe, instances))

    multi_url = len(instances) > 1
    expanded: List[Instance] = []
    for inst, raw in probed:
        if raw is None:
            # Probe failed — keep the Instance as-is; the next fetch attempt
            # will surface the real fetch error.
            expanded.append(inst)
            continue
        engines = detect_engines(raw)
        if len(engines) <= 1:
            expanded.append(inst)
        else:
            # Multi-engine — fan out into per-engine sub-instances. Naming:
            #   * external-only:   "0", "1", ...           (unchanged)
            #   * internal-only:   "e0", "e1", ...
            #   * mixed (N×M):     "0.e0", "0.e1", "1.e0", ...
            for eng in engines:
                sub_name = (f"{inst.name}.e{eng}" if multi_url else f"e{eng}")
                expanded.append(Instance(name=sub_name, url=inst.url, engine=eng))
    # Note: we deliberately don't seed any snapshots here — the monitor loop's
    # first fetch_all() does that. Seeding would set prev to a snapshot taken
    # microseconds earlier, giving meaningless rates on the first render.
    return expanded


def summarize(inst: Instance) -> Optional[Dict[str, Any]]:
    """Per-instance derived metrics, or None if there's no snapshot yet."""
    if inst.snapshot is None:
        return None
    s, p = inst.snapshot, inst.prev
    dt = (s.timestamp - p.timestamp) if p else 0.0

    def gauge(frag: str) -> Optional[float]:
        n = find_metric(s.gauges, frag)
        return s.gauges[n] if n else None

    def rate(frag: str) -> Optional[float]:
        if not p or dt <= 0:
            return None
        n = find_metric(s.counters, frag)
        if n and n in p.counters:
            return (s.counters[n] - p.counters[n]) / dt
        return None

    def win_pct(frag: str, q: float) -> Optional[float]:
        n = find_metric(s.histograms, frag)
        if not n:
            return None
        prev_h = p.histograms.get(n) if p else None
        wb, wc = window_buckets(s.histograms[n], prev_h)
        return histogram_percentile(wb, wc, q)

    kv = gauge(GAUGE_FRAGMENTS["kv_cache"])
    kv_pct: Optional[float] = (kv * 100) if (kv is not None and kv <= 1.0) else kv

    return {
        "running":    gauge(GAUGE_FRAGMENTS["running"]),
        "waiting":    gauge(GAUGE_FRAGMENTS["waiting"]),
        "swapped":    gauge(GAUGE_FRAGMENTS["swapped"]),
        "kv_pct":     kv_pct,
        "prompt_rps": rate("prompt_tokens"),
        "gen_rps":    rate("generation_tokens"),
        "req_rps":    rate("request_success"),
        "ttft_p50":   win_pct("time_to_first_token",   0.50),
        "ttft_p95":   win_pct("time_to_first_token",   0.95),
        "ttft_p99":   win_pct("time_to_first_token",   0.99),
        "tpot_p50":   win_pct("time_per_output_token", 0.50),
        "tpot_p95":   win_pct("time_per_output_token", 0.95),
        "tpot_p99":   win_pct("time_per_output_token", 0.99),
        "e2e_p50":    win_pct("e2e_request_latency",   0.50),
        "e2e_p95":    win_pct("e2e_request_latency",   0.95),
        "e2e_p99":    win_pct("e2e_request_latency",   0.99),
        "queue_p95":  win_pct("request_queue_time",    0.95),
        "_snap": s, "_prev": p, "_dt": dt,
    }


# ───────────────────────────── rendering ─────────────────────────────────
def fmt(v: Optional[float], spec: str = "{:.1f}", na: str = "—") -> str:
    return na if v is None else spec.format(v)


def humanize(v: Optional[float]) -> str:
    """Short SI-suffixed integer (12345678 → '12.3M')."""
    if v is None:
        return "—"
    abs_v = abs(v)
    if abs_v >= 1e12: return f"{v/1e12:.2f}T"
    if abs_v >= 1e9:  return f"{v/1e9:.2f}G"
    if abs_v >= 1e6:  return f"{v/1e6:.2f}M"
    if abs_v >= 1e3:  return f"{v/1e3:.1f}K"
    return f"{v:.0f}"


def fmt_duration(secs: Optional[float]) -> str:
    if secs is None or secs < 0:
        return "—"
    s = int(secs)
    if s < 60:        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    if h < 24:        return f"{h}h{m:02d}m"
    d, h = divmod(h, 24)
    return f"{d}d{h:02d}h"


def sparkline(values: List[float], width: int = SPARK_WIDTH,
              fixed_min: Optional[float] = None,
              fixed_max: Optional[float] = None) -> str:
    """Render a sequence of values as a Unicode block-character sparkline.

    Most recent value is on the right. If there are fewer samples than `width`,
    the sparkline is left-padded with spaces so the right edge always shows the
    latest reading.
    """
    if not values:
        return " " * width
    vals = values[-width:]
    pad = max(0, width - len(vals))
    lo = fixed_min if fixed_min is not None else min(vals)
    hi = fixed_max if fixed_max is not None else max(vals)
    span = hi - lo
    if span < 1e-9:
        # All samples equal — draw a flat mid-row so it's still visible.
        return " " * pad + "▄" * len(vals)
    out = []
    # Indices 1..8 of SPARK_BLOCKS (skip 0 = space, reserved for padding).
    for v in vals:
        idx = 1 + int((v - lo) / span * 7.999)
        idx = max(1, min(8, idx))
        out.append(SPARK_BLOCKS[idx])
    return " " * pad + "".join(out)


def bar(pct: Optional[float], width: int = 22) -> str:
    if pct is None:
        return " " * width
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100.0 * width))
    color = RED if pct > 85 else YELLOW if pct > 65 else GREEN
    return color + "█" * filled + GRAY + "░" * (width - filled) + RESET


def _kv_color(p: Optional[float]) -> str:
    if p is None: return ""
    return RED if p > 85 else YELLOW if p > 65 else ""


def _wait_color(w: Optional[float]) -> str:
    if not w: return ""
    return RED if w > 5 else YELLOW


def render_detail(inst: Instance, cost: Optional[CostConfig] = None) -> None:
    """Full detail view for a single instance (P50/P95/P99 × 4 latency metrics)."""
    s = inst.snapshot
    p = inst.prev
    dt = (s.timestamp - p.timestamp) if (s and p) else 0.0
    smry = summarize(inst)
    lines: List[str] = [CLEAR]

    ts = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"{BOLD}{CYAN}vLLM Monitor{RESET}  {DIM}│{RESET}  {inst.url}  "
                 f"{DIM}│{RESET}  {ts}  {DIM}(Δ={dt:.1f}s){RESET}")
    lines.append(GRAY + "─" * 64 + RESET)

    if inst.error and not s:
        lines.append(f"{RED}fetch error:{RESET} {inst.error}")
        sys.stdout.write("\n".join(lines) + "\n"); sys.stdout.flush(); return
    if inst.error:
        lines.append(f"{YELLOW}STALE — last fetch failed:{RESET} {inst.error}")

    lines.append(f"{BOLD}▸ Throughput{RESET}  {DIM}(windowed){RESET}")
    if smry and p:
        lines.append(f"  Prompt tokens/s   : {BOLD}{fmt(smry['prompt_rps'], '{:.1f}'):>10}{RESET}")
        lines.append(f"  Output tokens/s   : {BOLD}{fmt(smry['gen_rps'],    '{:.1f}'):>10}{RESET}")
        lines.append(f"  Successful req/s  : {BOLD}{fmt(smry['req_rps'],    '{:.2f}'):>10}{RESET}")
    else:
        lines.append(f"  {DIM}(collecting first sample…){RESET}")
    lines.append("")

    lines.append(f"{BOLD}▸ Latency{RESET}  {DIM}(windowed percentiles){RESET}")
    lines.append(f"  {DIM}{'metric':<12}{'P50':>10}{'P95':>10}{'P99':>10}{RESET}")
    for label, fragment, scale in HIST_METRICS:
        if not s or not find_metric(s.histograms, fragment):
            continue
        def scaled(v: Optional[float]) -> Optional[float]:
            return v * scale if v is not None else None
        key = (
            "ttft" if "first_token"  in fragment else
            "tpot" if "output_token" in fragment else
            "e2e"  if "e2e"          in fragment else
            "queue"
        )
        p50 = smry and smry.get(f"{key}_p50")
        p95 = smry and smry.get(f"{key}_p95")
        p99 = smry and smry.get(f"{key}_p99")
        lines.append(f"  {label:<12}"
                     f"{fmt(scaled(p50)):>10}{fmt(scaled(p95)):>10}{fmt(scaled(p99)):>10}")
    lines.append("")

    lines.append(f"{BOLD}▸ Saturation{RESET}  {DIM}(current){RESET}")
    if smry:
        wc = _wait_color(smry["waiting"])
        sc = RED if (smry["swapped"] or 0) > 0 else ""
        kc = _kv_color(smry["kv_pct"])
        lines.append(f"  Running           : {BOLD}{fmt(smry['running'], '{:.0f}'):>6}{RESET}")
        lines.append(f"  Waiting (queue)   : {wc}{BOLD}{fmt(smry['waiting'], '{:.0f}'):>6}{RESET}")
        lines.append(f"  Swapped           : {sc}{fmt(smry['swapped'], '{:.0f}'):>6}{RESET}")
        if smry["kv_pct"] is not None:
            lines.append(f"  KV cache usage    :  {kc}{BOLD}{smry['kv_pct']:>5.1f}%{RESET}  {bar(smry['kv_pct'])}")
        else:
            lines.append(f"  KV cache usage    :     —")
    lines.append("")

    # ── Trend (sparklines over recent samples) ──
    if inst.hist_running or inst.hist_kv:
        n_samples = max(
            len(inst.hist_running), len(inst.hist_kv),
            len(inst.hist_prompt_rps), len(inst.hist_gen_rps),
            len(inst.hist_ttft_p95), len(inst.hist_tpot_p95),
        )
        lines.append(f"{BOLD}▸ Trend{RESET}  "
                     f"{DIM}(last {min(HISTORY_LEN, n_samples)} samples, newest on right){RESET}")

        def trend_row(label: str, hist: List[float], val_fmt: str,
                      fixed_min: Optional[float] = 0.0,
                      fixed_max: Optional[float] = None,
                      color_fn=None) -> str:
            if not hist:
                return f"  {label:<14}: {DIM}(no data yet){RESET}"
            spark = sparkline(hist, fixed_min=fixed_min, fixed_max=fixed_max)
            lo, hi, cur = min(hist), max(hist), hist[-1]
            color = color_fn(cur) if color_fn else ""
            stats = (f"{DIM}min{RESET}{val_fmt.format(lo)} "
                     f"{DIM}max{RESET}{val_fmt.format(hi)} "
                     f"{DIM}now{RESET}{color}{val_fmt.format(cur)}{RESET}")
            return f"  {label:<14}: {spark}  {stats}"

        # For counts & KV%, the natural floor is 0 — keep it pinned so the
        # sparkline communicates absolute level. For rates & latencies, let
        # the scale auto-fit so trend motion is visible (the min/max/now
        # readout next to the bar already conveys absolute magnitude).
        lines.append(trend_row("Running",      inst.hist_running,    "{:>4.0f}",
                               fixed_min=0.0))
        lines.append(trend_row("KV cache %",   inst.hist_kv,         "{:>5.1f}%",
                               fixed_min=0.0, fixed_max=100.0, color_fn=_kv_color))
        lines.append(trend_row("in tok/s",     inst.hist_prompt_rps, "{:>6.0f}",
                               fixed_min=None))
        lines.append(trend_row("out tok/s",    inst.hist_gen_rps,    "{:>6.0f}",
                               fixed_min=None))
        lines.append(trend_row("TTFT P95 ms",  inst.hist_ttft_p95,   "{:>6.0f}",
                               fixed_min=None))
        lines.append(trend_row("TPOT P95 ms",  inst.hist_tpot_p95,   "{:>6.1f}",
                               fixed_min=None))
        lines.append("")

    # ── Cumulative ──
    if s is not None:
        uptime = (time.time() - inst.first_seen) if inst.first_seen else 0.0
        lines.append(f"{BOLD}▸ Cumulative{RESET}  "
                     f"{DIM}(vLLM-process counters · monitor uptime {fmt_duration(uptime)}){RESET}")

        def ctr(frag: str) -> Optional[float]:
            n = find_metric(s.counters, frag)
            return s.counters[n] if n else None

        lines.append(f"  Prompt tokens (life): {BOLD}{humanize(ctr('prompt_tokens')):>10}{RESET}")
        lines.append(f"  Output tokens (life): {BOLD}{humanize(ctr('generation_tokens')):>10}{RESET}")
        lines.append(f"  Successful reqs(life): {BOLD}{humanize(ctr('request_success')):>9}{RESET}")
        lines.append(f"  Peak running  (sess): {BOLD}{inst.peak_running:>10.0f}{RESET}")
        lines.append(f"  Peak waiting  (sess): {BOLD}{inst.peak_waiting:>10.0f}{RESET}")
        if inst.peak_swapped > 0:
            lines.append(f"  Peak swapped  (sess): {RED}{BOLD}{inst.peak_swapped:>10.0f}{RESET}")
        lines.append(f"  Peak KV cache (sess): {BOLD}{inst.peak_kv:>9.1f}%{RESET}")
        if inst.peak_prompt_rps > 0:
            lines.append(f"  Peak in  tok/s(sess): {BOLD}{inst.peak_prompt_rps:>10.0f}{RESET}")
        if inst.peak_gen_rps > 0:
            lines.append(f"  Peak out tok/s(sess): {BOLD}{inst.peak_gen_rps:>10.0f}{RESET}")
        lines.append("")

    # ── Cost (when any pricing model is enabled) ──
    if cost is not None and cost.enabled and s is not None:
        def ctr(frag: str) -> Optional[float]:
            n = find_metric(s.counters, frag)
            return s.counters[n] if n else None

        uptime = (time.time() - inst.first_seen) if inst.first_seen else 0.0
        lines.append(f"{BOLD}▸ Cost{RESET}  {DIM}(estimated){RESET}")

        # Token-based pricing
        if cost.token_enabled:
            p_life = ctr("prompt_tokens")     or 0.0
            g_life = ctr("generation_tokens") or 0.0
            life_total, life_in, life_out = cost.for_tokens(p_life, g_life)
            lines.append(f"  {DIM}Token-based  ({cost.currency}{cost.input_per_m:g}/M in, "
                         f"{cost.currency}{cost.output_per_m:g}/M out){RESET}")
            lines.append(f"    Lifetime         : {BOLD}{fmt_money(life_total, cost.currency):>12}{RESET}  "
                         f"{DIM}({fmt_money(life_in, cost.currency)} in + "
                         f"{fmt_money(life_out, cost.currency)} out){RESET}")
            if inst.baseline_prompt_tokens is not None or inst.baseline_gen_tokens is not None:
                sess_p = max(0.0, p_life - (inst.baseline_prompt_tokens or 0.0))
                sess_g = max(0.0, g_life - (inst.baseline_gen_tokens    or 0.0))
                sess_total, _, _ = cost.for_tokens(sess_p, sess_g)
                lines.append(f"    This session     : {BOLD}{fmt_money(sess_total, cost.currency):>12}{RESET}  "
                             f"{DIM}(over {fmt_duration(uptime)}){RESET}")
            smry_now = summarize(inst)
            if smry_now and smry_now["prompt_rps"] is not None and smry_now["gen_rps"] is not None:
                per_sec, _, _ = cost.for_tokens(smry_now["prompt_rps"], smry_now["gen_rps"])
                if per_sec > 0:
                    lines.append(f"    Current rate     : {BOLD}{fmt_money(per_sec*60, cost.currency):>12}/min{RESET}  "
                                 f"{DIM}({fmt_money(per_sec*3600, cost.currency)}/hour at current throughput){RESET}")

        # Compute-based pricing
        if cost.compute_enabled:
            src = (" — RunPod Secure reference, ±30% across providers"
                   if cost.gpu_price_source == "auto" else "")
            model = cost.gpu_model or "GPU"
            lines.append(f"  {DIM}Compute-based  ({model} × {cost.num_gpus} @ "
                         f"{cost.currency}{cost.gpu_cost_hour:g}/h{src}){RESET}")
            lines.append(f"    Burn rate        : {BOLD}{fmt_money(cost.compute_per_hour, cost.currency):>12}/hour{RESET}  "
                         f"{DIM}(paid whether busy or idle){RESET}")
            lines.append(f"    This session     : {BOLD}{fmt_money(cost.for_seconds(uptime), cost.currency):>12}{RESET}  "
                         f"{DIM}(over {fmt_duration(uptime)}){RESET}")

        # Margin: only sensible when BOTH are enabled
        if cost.token_enabled and cost.compute_enabled:
            smry_now = summarize(inst)
            if smry_now and smry_now["prompt_rps"] is not None and smry_now["gen_rps"] is not None:
                revenue_per_sec, _, _ = cost.for_tokens(smry_now["prompt_rps"], smry_now["gen_rps"])
                cost_per_sec = cost.compute_per_hour / 3600.0
                if cost_per_sec > 0:
                    ratio = revenue_per_sec / cost_per_sec
                    color = GREEN if ratio >= 2.0 else YELLOW if ratio >= 1.0 else RED
                    lines.append(f"  {DIM}Margin (token revenue ÷ compute cost){RESET}")
                    lines.append(f"    At current load  : {color}{BOLD}{ratio:>10.2f}×{RESET}  "
                                 f"{DIM}({fmt_money(revenue_per_sec*3600, cost.currency)}/h revenue vs "
                                 f"{fmt_money(cost.compute_per_hour, cost.currency)}/h compute){RESET}")
        lines.append("")

    lines.append(GRAY + "Ctrl-C to exit" + RESET)
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


def render_table(instances: List[Instance], interval: float,
                 cost: Optional[CostConfig] = None) -> None:
    """Per-DP comparison table + aggregate row + imbalance check."""
    summaries = [(inst, summarize(inst)) for inst in instances]
    n = len(instances)
    up = sum(1 for inst, smry in summaries if smry is not None and not inst.error)

    # Dynamic width for the DP/engine name column — short ("0","1") for plain
    # external DP, longer ("0.e0","1.e15") when internal DP is also in play.
    name_w = max(2, max((len(i.name) for i in instances), default=2))
    # Pad the header label so the column lines still align at width 86.
    pad_extra = max(0, name_w - 3)
    rule = GRAY + "─" * (86 + pad_extra) + RESET

    lines: List[str] = [CLEAR]
    ts = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
    health = f"{GREEN}{up}/{n} up{RESET}" if up == n else f"{RED}{up}/{n} up{RESET}"
    lines.append(f"{BOLD}{CYAN}vLLM DP Monitor{RESET}  {DIM}│{RESET}  "
                 f"{health}  {DIM}│{RESET}  {ts}  {DIM}(interval={interval}s){RESET}")
    lines.append(rule)
    lines.append(f"{DIM} {'DP':<{name_w}}  Status   Run  Wait  Swap   KV%      in tok/s  out tok/s   TTFT-P95  TPOT-P95{RESET}")
    lines.append(rule)

    have_first_sample = False
    for inst, smry in summaries:
        if smry is None:
            err = (inst.error or "no data yet")[:34]
            lines.append(f" {inst.name:<{name_w}} {RED}DOWN  {RESET} "
                         f"{GRAY}  —     —     —      —          —          —          —         —{RESET}  "
                         f"{DIM}{err}{RESET}")
            continue
        if smry.get("_prev") is not None:
            have_first_sample = True

        wc = _wait_color(smry["waiting"])
        sc = RED if (smry["swapped"] or 0) > 0 else ""
        kc = _kv_color(smry["kv_pct"])
        status = f"{YELLOW}STALE {RESET}" if inst.error else f"{GREEN}OK    {RESET}"
        ttft_ms = (smry["ttft_p95"] * 1000) if smry["ttft_p95"] is not None else None
        tpot_ms = (smry["tpot_p95"] * 1000) if smry["tpot_p95"] is not None else None

        lines.append(
            f" {inst.name:<{name_w}} {status} "
            f"{fmt(smry['running'], '{:.0f}'):>4}  "
            f"{wc}{fmt(smry['waiting'], '{:.0f}'):>4}{RESET}  "
            f"{sc}{fmt(smry['swapped'], '{:.0f}'):>4}{RESET}   "
            f"{kc}{fmt(smry['kv_pct'], '{:.1f}'):>5}%{RESET}    "
            f"{fmt(smry['prompt_rps'], '{:.0f}'):>8}   "
            f"{fmt(smry['gen_rps'],    '{:.0f}'):>8}    "
            f"{fmt(ttft_ms, '{:.0f}'):>6}ms   "
            f"{fmt(tpot_ms, '{:.1f}'):>6}ms"
        )

    ok_smries = [s for _, s in summaries if s is not None]
    if ok_smries:
        lines.append(rule)
        sum_run  = sum((s["running"] or 0) for s in ok_smries)
        sum_wait = sum((s["waiting"] or 0) for s in ok_smries)
        sum_swap = sum((s["swapped"] or 0) for s in ok_smries)
        kvs = [s["kv_pct"] for s in ok_smries if s["kv_pct"] is not None]
        max_kv = max(kvs) if kvs else None
        sum_pin  = sum((s["prompt_rps"] or 0) for s in ok_smries if s["prompt_rps"] is not None)
        sum_pout = sum((s["gen_rps"]    or 0) for s in ok_smries if s["gen_rps"]    is not None)

        snaps_pairs = [(s["_snap"], s["_prev"]) for s in ok_smries]
        ttft_b, ttft_c = merge_window_buckets(snaps_pairs, "time_to_first_token")
        tpot_b, tpot_c = merge_window_buckets(snaps_pairs, "time_per_output_token")
        ttft_all = histogram_percentile(ttft_b, ttft_c, 0.95)
        tpot_all = histogram_percentile(tpot_b, tpot_c, 0.95)
        ttft_ms_all = (ttft_all * 1000) if ttft_all else None
        tpot_ms_all = (tpot_all * 1000) if tpot_all else None

        lines.append(
            f" {BOLD}ALL{RESET}{' ' * (name_w - 3)}        "
            f" {sum_run:>4.0f}  "
            f"{sum_wait:>4.0f}  "
            f"{sum_swap:>4.0f}    "
            f"{DIM}max{RESET}{fmt(max_kv, '{:.1f}'):>4}%    "
            f"{sum_pin:>8.0f}   "
            f"{sum_pout:>8.0f}    "
            f"{fmt(ttft_ms_all, '{:.0f}'):>6}ms   "
            f"{fmt(tpot_ms_all, '{:.1f}'):>6}ms"
        )

        # Imbalance check
        if len(ok_smries) >= 2:
            lines.append("")
            lines.append(f"{BOLD}▸ Imbalance check{RESET}  {DIM}(across {len(ok_smries)} replicas){RESET}")

            runs = [s["running"] for s in ok_smries if s["running"] is not None]
            if len(runs) >= 2:
                r_min, r_max = min(runs), max(runs)
                bad = (r_max - r_min) > 3 and r_max > 1.5 * max(1, r_min)
                tag = f"  {YELLOW}⚠ load-balancer skew?{RESET}" if bad else ""
                lines.append(f"  Running req     : {r_min:>5.0f}  →  {r_max:<5.0f} (Δ={r_max-r_min:.0f}){tag}")

            if len(kvs) >= 2:
                k_min, k_max = min(kvs), max(kvs)
                bad = (k_max - k_min) > 15
                tag = f"  {YELLOW}⚠ uneven KV pressure{RESET}" if bad else ""
                lines.append(f"  KV cache        : {k_min:>5.1f}% → {k_max:<5.1f}% (Δ={k_max-k_min:.1f}pp){tag}")

            ttfts = [s["ttft_p95"] for s in ok_smries if s["ttft_p95"] is not None]
            if len(ttfts) >= 2:
                t_min, t_max = min(ttfts) * 1000, max(ttfts) * 1000
                ratio = t_max / max(0.001, t_min)
                bad = ratio > 1.5
                tag = f"  {YELLOW}⚠ slow replica{RESET}" if bad else ""
                lines.append(f"  TTFT P95        : {t_min:>5.0f}ms → {t_max:<5.0f}ms ({ratio:.2f}×){tag}")

            tpots = [s["tpot_p95"] for s in ok_smries if s["tpot_p95"] is not None]
            if len(tpots) >= 2:
                t_min, t_max = min(tpots) * 1000, max(tpots) * 1000
                ratio = t_max / max(0.001, t_min)
                bad = ratio > 1.5
                tag = f"  {YELLOW}⚠ slow decode{RESET}" if bad else ""
                lines.append(f"  TPOT P95        : {t_min:>5.1f}ms → {t_max:<5.1f}ms ({ratio:.2f}×){tag}")

    if not have_first_sample:
        lines.append("")
        lines.append(f"  {DIM}(throughput & percentiles populate after the 2nd poll…){RESET}")

    # ── Cumulative section ──
    if any(inst.first_seen is not None for inst in instances):
        lines.append("")
        # Use earliest first_seen as monitor uptime
        first_seens = [inst.first_seen for inst in instances if inst.first_seen is not None]
        uptime = (time.time() - min(first_seens)) if first_seens else 0.0
        lines.append(f"{BOLD}▸ Cumulative{RESET}  "
                     f"{DIM}(life = vLLM counters · sess = peaks observed since monitor uptime {fmt_duration(uptime)}){RESET}")
        lines.append(rule)
        lines.append(f"{DIM} {'DP':<{name_w}}   life-Prompt  life-Output  life-Reqs   peak-Run  peak-Wait  peak-KV%   peak in/out tok/s{RESET}")
        lines.append(rule)

        sum_pin_life = sum_pout_life = sum_req_life = 0.0
        for inst in instances:
            s = inst.snapshot
            if s is None:
                lines.append(f" {inst.name:<{name_w}} {GRAY}     —            —            —          —          —         —          — / —{RESET}")
                continue
            def ctr(frag: str) -> Optional[float]:
                n = find_metric(s.counters, frag)
                return s.counters[n] if n else None
            pin  = ctr("prompt_tokens")
            pout = ctr("generation_tokens")
            req  = ctr("request_success")
            if pin  is not None: sum_pin_life  += pin
            if pout is not None: sum_pout_life += pout
            if req  is not None: sum_req_life  += req

            swap_warn = RED if inst.peak_swapped > 0 else ""
            kv_warn   = RED if inst.peak_kv > 90 else YELLOW if inst.peak_kv > 75 else ""
            lines.append(
                f" {inst.name:<{name_w}}  "
                f"{humanize(pin):>10}  "
                f"{humanize(pout):>10}  "
                f"{humanize(req):>9}    "
                f"{inst.peak_running:>6.0f}     "
                f"{inst.peak_waiting:>4.0f}    "
                f"{kv_warn}{inst.peak_kv:>5.1f}%{RESET}   "
                f"{humanize(inst.peak_prompt_rps):>5}/{humanize(inst.peak_gen_rps):<5}"
                + (f"  {swap_warn}swap-seen{RESET}" if inst.peak_swapped > 0 else "")
            )
        lines.append(rule)
        lines.append(
            f" {BOLD}ALL{RESET}{' ' * (name_w - 3)}  "
            f"{BOLD}{humanize(sum_pin_life):>10}{RESET}  "
            f"{BOLD}{humanize(sum_pout_life):>10}{RESET}  "
            f"{BOLD}{humanize(sum_req_life):>9}{RESET}"
        )

        # ── Cost (when any pricing model is enabled) ──
        if cost is not None and cost.enabled:
            def _counter(snap: Snapshot, frag: str) -> float:
                n = find_metric(snap.counters, frag)
                return snap.counters[n] if n else 0.0

            ok_smries = [s for s in (summarize(i) for i in instances) if s is not None]

            lines.append("")
            lines.append(f"{BOLD}▸ Cost{RESET}  "
                         f"{DIM}(estimated · sum across {len(instances)} replicas){RESET}")

            # Token-based
            if cost.token_enabled:
                life_total, life_in, life_out = cost.for_tokens(sum_pin_life, sum_pout_life)
                sess_p = sess_g = 0.0
                for i in instances:
                    if i.snapshot is None:
                        continue
                    cur_p = _counter(i.snapshot, "prompt_tokens")
                    cur_g = _counter(i.snapshot, "generation_tokens")
                    sess_p += max(0.0, cur_p - (i.baseline_prompt_tokens or cur_p))
                    sess_g += max(0.0, cur_g - (i.baseline_gen_tokens    or cur_g))
                sess_total, _, _ = cost.for_tokens(sess_p, sess_g)
                sum_pin_rate  = sum((s["prompt_rps"] or 0) for s in ok_smries)
                sum_pout_rate = sum((s["gen_rps"]    or 0) for s in ok_smries)
                per_sec, _, _ = cost.for_tokens(sum_pin_rate, sum_pout_rate)

                lines.append(f"  {DIM}Token-based  ({cost.currency}{cost.input_per_m:g}/M in, "
                             f"{cost.currency}{cost.output_per_m:g}/M out){RESET}")
                lines.append(f"    Lifetime     : {BOLD}{fmt_money(life_total, cost.currency):>12}{RESET}  "
                             f"{DIM}({fmt_money(life_in, cost.currency)} in + "
                             f"{fmt_money(life_out, cost.currency)} out){RESET}")
                lines.append(f"    This session : {BOLD}{fmt_money(sess_total, cost.currency):>12}{RESET}  "
                             f"{DIM}(over {fmt_duration(uptime)}){RESET}")
                if per_sec > 0:
                    lines.append(f"    Current rate : {BOLD}{fmt_money(per_sec*60, cost.currency):>12}/min{RESET}  "
                                 f"{DIM}({fmt_money(per_sec*3600, cost.currency)}/hour at current throughput){RESET}")

            # Compute-based
            if cost.compute_enabled:
                src = " — auto-detected, estimate" if cost.gpu_price_source == "auto" else ""
                model = cost.gpu_model or "GPU"
                lines.append(f"  {DIM}Compute-based  ({model} × {cost.num_gpus} @ "
                             f"{cost.currency}{cost.gpu_cost_hour:g}/h{src}){RESET}")
                lines.append(f"    Burn rate    : {BOLD}{fmt_money(cost.compute_per_hour, cost.currency):>12}/hour{RESET}  "
                             f"{DIM}(paid whether busy or idle){RESET}")
                lines.append(f"    This session : {BOLD}{fmt_money(cost.for_seconds(uptime), cost.currency):>12}{RESET}  "
                             f"{DIM}(over {fmt_duration(uptime)}){RESET}")

            # Margin: token revenue vs compute cost
            if cost.token_enabled and cost.compute_enabled and ok_smries:
                sum_pin_rate  = sum((s["prompt_rps"] or 0) for s in ok_smries)
                sum_pout_rate = sum((s["gen_rps"]    or 0) for s in ok_smries)
                revenue_per_sec, _, _ = cost.for_tokens(sum_pin_rate, sum_pout_rate)
                cost_per_sec = cost.compute_per_hour / 3600.0
                if cost_per_sec > 0:
                    ratio = revenue_per_sec / cost_per_sec
                    color = GREEN if ratio >= 2.0 else YELLOW if ratio >= 1.0 else RED
                    lines.append(f"  {DIM}Margin (token revenue ÷ compute cost){RESET}")
                    lines.append(f"    At current load : {color}{BOLD}{ratio:>9.2f}×{RESET}  "
                                 f"{DIM}({fmt_money(revenue_per_sec*3600, cost.currency)}/h revenue vs "
                                 f"{fmt_money(cost.compute_per_hour, cost.currency)}/h compute){RESET}")

    lines.append("")
    # Dedupe URLs (internal-DP engines all share one URL) and group their
    # display names against it.
    by_url_legend: Dict[str, List[str]] = {}
    for i in instances:
        by_url_legend.setdefault(i.url.replace("/metrics", ""), []).append(i.name)
    legend = "  ".join(
        f"DP{','.join(names)}={url}" if len(names) == 1
        else f"DP{{{','.join(names)}}}={url}"
        for url, names in by_url_legend.items()
    )
    lines.append(GRAY + "Legend: " + legend + RESET)
    lines.append(GRAY + "Ctrl-C to exit" + RESET)
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


# ──────────────────────── auto-discovery (--auto) ────────────────────────
DEFAULT_DISCOVER_RANGE = (8000, 8015)   # inclusive on both ends


def parse_port_range(spec: str) -> Tuple[int, int]:
    """Parse '8000-8015' or '8000:8015' or '8000,8015' into (lo, hi)."""
    for sep in ("-", ":", ","):
        if sep in spec:
            a, b = spec.split(sep, 1)
            return int(a.strip()), int(b.strip())
    # Single port: treat as a one-port range
    p = int(spec.strip())
    return p, p


def _probe_tcp(host: str, port: int, timeout: float) -> bool:
    """Quick TCP connect check — true if the port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def _probe_vllm_metrics(host: str, port: int, timeout: float) -> bool:
    """True if http://host:port/metrics returns text that looks like vLLM."""
    url = f"http://{host}:{port}/metrics"
    try:
        body = fetch_metrics(url, timeout=timeout)
    except Exception:
        return False
    # Heuristic: vLLM exports metric names with the `vllm:` prefix.
    return "vllm:" in body


def discover_vllm_endpoints(host: str = "localhost",
                            port_range: Tuple[int, int] = DEFAULT_DISCOVER_RANGE,
                            connect_timeout: float = 0.3,
                            fetch_timeout: float = 1.5) -> List[str]:
    """Find all vLLM-shaped `/metrics` endpoints on `host` within the port range.

    Two parallel passes:
      1. TCP-connect probe to filter to actually-open ports (cheap)
      2. HTTP probe of `/metrics` on those ports, checking for the `vllm:`
         metric-name prefix (more expensive, but only runs on open ports)
    """
    lo, hi = port_range
    ports = list(range(lo, hi + 1))
    if not ports:
        return []

    # Pass 1: which ports accept TCP connections?
    with ThreadPoolExecutor(max_workers=min(32, len(ports))) as ex:
        open_ports = [p for p, ok in zip(
            ports,
            ex.map(lambda p: _probe_tcp(host, p, connect_timeout), ports)
        ) if ok]

    if not open_ports:
        return []

    # Pass 2: which open ports speak vLLM's /metrics dialect?
    with ThreadPoolExecutor(max_workers=min(32, len(open_ports))) as ex:
        is_vllm = list(ex.map(
            lambda p: _probe_vllm_metrics(host, p, fetch_timeout), open_ports
        ))

    return [f"http://{host}:{p}" for p, ok in zip(open_ports, is_vllm) if ok]


# ──────────────────────────────── main ───────────────────────────────────
def _expand_urls(raw: List[str]) -> List[str]:
    """Allow comma-separated values inside any --url arg."""
    out: List[str] = []
    for r in raw:
        for piece in r.split(","):
            piece = piece.strip()
            if piece:
                out.append(piece)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Real-time summary of vLLM /metrics. Supports DP (multiple URLs).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-V", "--version", action="version",
                    version=f"vllm-htop {__version__}")
    ap.add_argument("--url", nargs="+", default=None,
                    help="One or more vLLM server base URLs (space- or comma-separated). "
                         "Default: http://localhost:8000 (unless --auto is set).")
    ap.add_argument("--auto", action="store_true",
                    help="auto-discover vLLM endpoints on localhost by scanning a port range "
                         "(see --port-range). Mutually exclusive with --url.")
    ap.add_argument("--port-range", default=f"{DEFAULT_DISCOVER_RANGE[0]}-{DEFAULT_DISCOVER_RANGE[1]}",
                    help=f"port range for --auto, e.g. '8000-8015' "
                         f"(default: {DEFAULT_DISCOVER_RANGE[0]}-{DEFAULT_DISCOVER_RANGE[1]})")
    ap.add_argument("--host", default="localhost",
                    help="hostname for --auto discovery (default: localhost)")
    ap.add_argument("--interval", type=float, default=2.0,
                    help="polling interval in seconds (default: 2.0)")
    ap.add_argument("--timeout",  type=float, default=4.0,
                    help="per-endpoint fetch timeout in seconds (default: 4.0)")
    ap.add_argument("--once",     action="store_true",
                    help="print one snapshot and exit")
    ap.add_argument("--table",    action="store_true",
                    help="force compact table view (default: auto — table when ≥2 URLs)")
    ap.add_argument("--detail",   action="store_true",
                    help="force per-replica detail view (only sensible for a single URL)")
    ap.add_argument("--cost-in",  type=float, default=0.0, metavar="PRICE",
                    help="USD per 1M input (prompt) tokens — enables token-based "
                         "cost (e.g. 0.50 to model OpenAI-style pricing)")
    ap.add_argument("--cost-out", type=float, default=0.0, metavar="PRICE",
                    help="USD per 1M output (generation) tokens")
    ap.add_argument("--gpu-cost-hour", type=float, default=0.0, metavar="PRICE",
                    help="USD per GPU per hour for compute-based cost. "
                         "If omitted, vllm-htop tries `nvidia-smi` and looks up "
                         "a built-in price hint; use this flag to override.")
    ap.add_argument("--num-gpus", type=int, default=0, metavar="N",
                    help="GPU count for compute-based cost. Default: count from "
                         "nvidia-smi, falling back to the number of monitored replicas.")
    ap.add_argument("--no-gpu-detect", action="store_true",
                    help="skip nvidia-smi auto-detection (use only explicit "
                         "--gpu-cost-hour / --num-gpus)")
    ap.add_argument("--currency", default="$",
                    help="currency symbol shown in the Cost section (default: $)")
    args = ap.parse_args()

    # Token pricing — purely from flags
    token_in  = max(0.0, args.cost_in)
    token_out = max(0.0, args.cost_out)

    # Compute pricing — explicit flag > nvidia-smi auto-detect
    gpu_cost   = max(0.0, args.gpu_cost_hour)
    num_gpus   = max(0, args.num_gpus)
    gpu_model: Optional[str] = None
    price_src  = "user" if gpu_cost > 0 else ""

    if not args.no_gpu_detect and (gpu_cost == 0 or num_gpus == 0 or gpu_model is None):
        detected = detect_gpus()
        if detected is not None:
            det_model, det_count = detected
            gpu_model = det_model
            if num_gpus == 0:
                num_gpus = det_count
            if gpu_cost == 0:
                hint = lookup_gpu_price(det_model)
                if hint is not None:
                    gpu_cost = hint
                    price_src = "auto"

    cost = CostConfig(
        input_per_m=token_in,
        output_per_m=token_out,
        currency=args.currency,
        gpu_cost_hour=gpu_cost,
        num_gpus=num_gpus,
        gpu_model=gpu_model,
        gpu_price_source=price_src,
    )

    if args.auto and args.url:
        sys.exit("--auto and --url are mutually exclusive")

    if args.url:
        # Explicit URLs — skip discovery entirely
        urls = _expand_urls(args.url)
        if not urls:
            sys.exit("no URLs provided")
    else:
        # No explicit --url: auto-discover by default. When the user passes
        # --auto explicitly, we're chatty and fail loudly; when discovery is
        # implicit, we stay quiet on the single-instance happy path and fall
        # back gracefully if nothing turns up.
        try:
            lo, hi = parse_port_range(args.port_range)
        except ValueError as e:
            sys.exit(f"invalid --port-range {args.port_range!r}: {e}")

        if args.auto:
            print(f"vllm-htop: scanning {args.host}:{lo}-{hi} for vLLM endpoints...",
                  file=sys.stderr, flush=True)

        urls = discover_vllm_endpoints(host=args.host, port_range=(lo, hi))

        if urls:
            # Narrate when there's something interesting to say. A single
            # found endpoint on the default range is the boring case — stay quiet.
            if args.auto or len(urls) > 1:
                print(f"vllm-htop: discovered {len(urls)} endpoint(s): "
                      + ", ".join(urls), file=sys.stderr, flush=True)
        else:
            if args.auto:
                sys.exit(f"no vLLM endpoints found on {args.host}:{lo}-{hi}. "
                         f"Try a wider --port-range or pass --url explicitly.")
            # Implicit discovery turned up nothing — fall back to the host's
            # default port. The subsequent fetch error (if any) will tell the
            # user what went wrong, more informatively than a generic "no
            # endpoints found".
            urls = [f"http://{args.host}:8000"]

    instances = [
        Instance(name=str(i), url=u.rstrip("/") + "/metrics")
        for i, u in enumerate(urls)
    ]

    # One-shot probe: expand single URLs that expose vLLM internal DP (multiple
    # `engine="*"` labels in one /metrics) into per-engine sub-instances.
    instances = expand_instances_by_engine(instances, timeout=args.timeout)
    if any(inst.engine is not None for inst in instances):
        n_engines = sum(1 for inst in instances if inst.engine is not None)
        print(f"vllm-htop: detected internal DP — expanded to {len(instances)} "
              f"replica(s) (of which {n_engines} are engine splits)",
              file=sys.stderr, flush=True)

    use_table = args.table or (len(instances) >= 2 and not args.detail)

    try:
        while True:
            fetch_all(instances, timeout=args.timeout)
            if use_table:
                render_table(instances, interval=args.interval, cost=cost)
            else:
                render_detail(instances[0], cost=cost)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
