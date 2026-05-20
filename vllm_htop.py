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
import atexit
import json
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

__version__ = "0.3.3"


# ───────────────────────────── ANSI styling ──────────────────────────────
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, GRAY = (
    "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[90m",
)
CLEAR = "\033[2J\033[H"

# Alternate-screen-buffer control — the same trick htop/vim/less use to claim
# the whole terminal while running and restore it on exit. Activated only for
# the interactive monitoring loop; piping, --once, and JSON modes skip it.
ALT_SCREEN_ENTER = "\033[?1049h\033[?25l"   # enter alt screen + hide cursor
ALT_SCREEN_LEAVE = "\033[?25h\033[?1049l"   # show cursor + leave alt screen
_alt_screen_active = False


def enter_alt_screen() -> None:
    """Switch the terminal into its alternate-screen buffer (htop-style)."""
    global _alt_screen_active
    if _alt_screen_active:
        return
    sys.stdout.write(ALT_SCREEN_ENTER)
    sys.stdout.flush()
    _alt_screen_active = True
    # Belt-and-braces: even if we exit via an unhandled exception or a signal
    # we don't catch, atexit still runs and the user gets their terminal back.
    atexit.register(leave_alt_screen)


def leave_alt_screen() -> None:
    """Restore the terminal to the normal screen buffer."""
    global _alt_screen_active
    if not _alt_screen_active:
        return
    sys.stdout.write(ALT_SCREEN_LEAVE)
    sys.stdout.flush()
    _alt_screen_active = False

# ───────────────────────── trend / sparkline config ──────────────────────
HISTORY_LEN = 60          # rolling samples kept per metric (≈2 min @ 2s interval)
SPARK_WIDTH = 30          # characters per sparkline
# 8 levels of vertical block. Space (idx 0) is reserved for "no data" padding.
SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"

# How long we keep raw histogram snapshots around, in seconds. Bounds memory
# for the long-window percentile (e.g. P95-over-the-last-1-minute). At the
# default 2s polling interval this is 300 snapshots per replica.
HISTORY_RETENTION_SECS = 600.0    # 10 minutes
# Default long window length surfaced as "P95@1m" — picked to be much more
# stable than the noisy 2s delta but still feel "live."
LONG_WINDOW_SECS = 60.0


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

    # Surface a few standard Prometheus process_* metrics (auto-exported by
    # the python prometheus_client) in addition to vLLM's own. We need
    # `process_start_time_seconds` to compute vLLM's true uptime, which the
    # Cost section uses for lifetime compute cost.
    PROCESS_METRICS = ("process_start_time_seconds",)

    def keep_for(name_: str, labels: Dict[str, str]) -> bool:
        # process_* metrics are process-wide (no engine label) — every engine
        # filter should still see them.
        if name_ in PROCESS_METRICS:
            return True
        return keep(labels)

    for name, kind in types.items():
        if not (name.startswith("vllm") or name in PROCESS_METRICS):
            continue
        if kind == "counter":
            total = 0.0
            for sample_name in (name, name + "_total"):
                for labels, v in samples.get(sample_name, []):
                    if keep_for(name, labels):
                        total += v
            snap.counters[name] = total
        elif kind == "gauge":
            total = 0.0
            for labels, v in samples.get(name, []):
                if keep_for(name, labels):
                    total += v
            snap.gauges[name] = total
        elif kind == "histogram":
            buckets: Dict[str, float] = {}
            for labels, v in samples.get(name + "_bucket", []):
                if keep_for(name, labels):
                    le = labels.get("le", "+Inf")
                    buckets[le] = buckets.get(le, 0.0) + v
            count = sum(v for labels, v in samples.get(name + "_count", []) if keep_for(name, labels))
            sm    = sum(v for labels, v in samples.get(name + "_sum",   []) if keep_for(name, labels))
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


def detect_model_name(raw: str) -> Optional[str]:
    """Extract the served model name from /metrics labels, if any.

    vLLM exposes the model identifier as a label on most metrics. The label
    key varies slightly across versions — we try the common spellings in
    order of preference.
    """
    _types, samples = parse_prom_text(raw)
    for key in ("model_name", "served_model_name", "model"):
        for sample_list in samples.values():
            for labels, _ in sample_list:
                v = labels.get(key)
                if v:
                    return v
    return None


def short_model_name(full: str, max_len: int = 24) -> str:
    """Squeeze a HuggingFace-style model id into a name friendly for table rows.

    Strategy: take the part after the last `/` (drops the org prefix), and
    if it's still longer than `max_len` characters truncate with `…`.

    Examples:
        meta-llama/Meta-Llama-3-8B-Instruct  →  Meta-Llama-3-8B-Instruct
        BAAI/bge-large-zh-v1.5                →  bge-large-zh-v1.5
        nvidia/Llama-3_1-Nemotron-253B-NF4    →  Llama-3_1-Nemotron-253B…
    """
    short = full.rsplit("/", 1)[-1]
    return short if len(short) <= max_len else short[: max_len - 1] + "…"


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
    # Datacenter — Hopper, China-market export-compliant variants
    # (anchored to mainland-China rental rates: AutoDL/GpuMall/Aliyun mid-tier)
    ("H20-3E",        3.50),   # H20 with HBM3e, 96/141GB; ~¥25/h on AutoDL
    ("H20",           2.80),   # original H20 96GB HBM3; ~¥20/h on AutoDL
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
    ("L20",           0.99),   # China-market Ada variant (~L40 perf, 48GB)
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
    anywhere in the GPU name. Both `-` and spaces count as token separators
    on both sides, so this handles all nvidia-smi conventions:
    `A100 80GB PCIe`, `A100-SXM4-80GB`, and `H20-3e` all tokenize cleanly.
    Hint ordering still matters: more specific hints must come first
    (e.g. "A100 80GB" before plain "A100", "H20-3E" before plain "H20").
    """
    # Normalize: uppercase, swap separators for spaces, collapse whitespace.
    haystack = " ".join(name.upper().replace("-", " ").split())
    for hint, price in GPU_PRICE_HINTS:
        # Tokenize hint the same way as haystack so "H20-3E" → ["H20", "3E"].
        # Each token must appear as a whole word — `(?<![A-Z0-9])TOKEN(?![A-Z0-9])`
        # — so "A100" doesn't match "RTX A1000" and "L4" doesn't match "L40".
        tokens = hint.upper().replace("-", " ").split()
        if all(re.search(rf"(?<![A-Z0-9]){re.escape(t)}(?![A-Z0-9])", haystack)
               for t in tokens):
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


def vllm_uptime_seconds(instances: List["Instance"]) -> Optional[float]:
    """Estimate vLLM's true uptime from `process_start_time_seconds` exported
    by python's prometheus_client. Returns None if no replica exposes it.

    For multi-URL deployments we take the *earliest* start time across
    replicas — i.e. the time the first replica came up. That's a slightly
    conservative estimate for lifetime compute cost (which is itself a
    rough ±30% number).
    """
    starts: List[float] = []
    for inst in instances:
        if inst.snapshot is None:
            continue
        n = find_metric(inst.snapshot.gauges, "process_start_time_seconds")
        if n:
            starts.append(inst.snapshot.gauges[n])
    if not starts:
        return None
    return max(0.0, time.time() - min(starts))


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
    # Served model name, extracted from /metrics labels on first probe.
    # Used to make `name` human-readable (e.g. "Llama-3.1-8B-Instruct.e0").
    model: Optional[str] = None
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
    # Rolling buffer of recent snapshots — used to compute percentiles over
    # longer windows than the single poll-to-poll delta. Trimmed by timestamp
    # in `_apply_raw` (default retention: HISTORY_RETENTION_SECS = 10 min).
    snapshot_history: List[Snapshot] = field(default_factory=list)


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
        # Rolling snapshot buffer for long-window percentile.
        inst.snapshot_history.append(inst.snapshot)
        cutoff = inst.snapshot.timestamp - HISTORY_RETENTION_SECS
        while inst.snapshot_history and inst.snapshot_history[0].timestamp < cutoff:
            inst.snapshot_history.pop(0)
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
    """Probe each URL once; pick a human-readable display name (model when we
    can, URL index otherwise); expand multi-engine endpoints into one Instance
    per engine.

    Called once at startup. The returned list is what the monitor loop polls.
    Names chosen here:
      * Pure external DP (single engine per URL), distinct models → model names
      * Multi-engine URL → `<base>.eN` where `<base>` is the model or URL index
      * Model collisions across URLs → fall back to the URL index, since a
        duplicated name would be ambiguous in the table

    If a URL is unreachable on this initial probe we keep the original
    Instance — it'll render as DOWN until the network recovers.
    """
    def probe(inst: Instance) -> Tuple[Instance, Optional[str]]:
        try:
            return inst, fetch_metrics(inst.url, timeout=timeout)
        except Exception:
            return inst, None

    with ThreadPoolExecutor(max_workers=min(32, len(instances))) as ex:
        probed = list(ex.map(probe, instances))

    # First pass — record (model, engines) per URL.
    info: Dict[str, Tuple[Optional[str], List[str]]] = {}
    for inst, raw in probed:
        if raw is None:
            info[inst.url] = (None, [])
        else:
            info[inst.url] = (detect_model_name(raw), detect_engines(raw))

    # Are the model names unique across URLs? If two URLs serve the same model,
    # we can't use the model name as a row identifier — fall back to URL index.
    models = [m for m, _ in info.values() if m]
    models_unique = len(models) == len(set(models)) and len(models) == len(info)

    multi_url = len(instances) > 1
    expanded: List[Instance] = []
    for inst, raw in probed:
        if raw is None:
            expanded.append(inst)
            continue
        model, engines = info[inst.url]

        # Decide the row label base.
        #   * If model names are unique → use the model (most informative)
        #   * Else if there are several URLs → keep the URL index (disambiguates)
        #   * Single URL with no engine label → display empty / engine-only
        if models_unique and model:
            base = short_model_name(model)
        elif multi_url:
            base = inst.name  # URL index, e.g. "0", "1"
        else:
            base = ""

        if len(engines) <= 1:
            # No internal DP — one row for the whole URL.
            display_name = base or inst.name
            expanded.append(Instance(name=display_name, url=inst.url, model=model))
        else:
            # Internal DP — one row per engine. Build `base.eN` or just `eN`.
            for eng in engines:
                sub_name = f"{base}.e{eng}" if base else f"e{eng}"
                expanded.append(Instance(name=sub_name, url=inst.url,
                                         engine=eng, model=model))
    # Note: we deliberately don't seed any snapshots here — the monitor loop's
    # first fetch_all() does that. Seeding would set prev to a snapshot taken
    # microseconds earlier, giving meaningless rates on the first render.
    return expanded


def long_window_percentile(inst: Instance, fragment: str,
                           target_secs: float, q: float) -> Optional[float]:
    """Percentile over the last `target_secs` of accumulated samples.

    Looks back through `inst.snapshot_history` to find the oldest snapshot at
    or before `now - target_secs` and computes the bucket delta from there to
    the current snapshot. If we don't yet have that much history, falls back
    to the oldest snapshot we do have (so the value is non-None as soon as
    we've taken at least two polls).
    """
    s = inst.snapshot
    if s is None or not inst.snapshot_history:
        return None
    target_ts = s.timestamp - target_secs
    # Walk back to the latest snapshot at or before target_ts.
    base: Optional[Snapshot] = None
    for h in inst.snapshot_history:
        if h is s:
            break
        if h.timestamp <= target_ts:
            base = h
    if base is None:
        # Not enough history — fall back to the oldest snapshot we have.
        base = inst.snapshot_history[0]
    if base is s:
        return None
    name = find_metric(s.histograms, fragment)
    if not name:
        return None
    base_h = base.histograms.get(name)
    if not base_h:
        return None
    wb, wc = window_buckets(s.histograms[name], base_h)
    return histogram_percentile(wb, wc, q)


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

    def long_pct(frag: str, q: float) -> Optional[float]:
        return long_window_percentile(inst, frag, LONG_WINDOW_SECS, q)

    kv = gauge(GAUGE_FRAGMENTS["kv_cache"])
    kv_pct: Optional[float] = (kv * 100) if (kv is not None and kv <= 1.0) else kv

    # Prefix cache hit rate — windowed and lifetime.
    # vLLM exposes per-version: `vllm:prefix_cache_queries_total` and
    # `vllm:prefix_cache_hits_total`. Both names contain "prefix_cache".
    def cache_lifetime_hit_pct() -> Optional[float]:
        qn = find_metric(s.counters, "prefix_cache_queries")
        hn = find_metric(s.counters, "prefix_cache_hits")
        if not qn or not hn:
            return None
        q = s.counters.get(qn, 0.0)
        h = s.counters.get(hn, 0.0)
        return (h / q * 100) if q > 0 else None

    def cache_window_hit_pct() -> Optional[float]:
        q_rps = rate("prefix_cache_queries")
        h_rps = rate("prefix_cache_hits")
        if q_rps is None or h_rps is None or q_rps <= 0:
            return None
        return h_rps / q_rps * 100

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
        # Long-window P95 (rolling ~1min) — more stable than the noisy
        # poll-to-poll number, useful as a "current SLO state" reference.
        "ttft_p95_long":  long_pct("time_to_first_token",   0.95),
        "tpot_p95_long":  long_pct("time_per_output_token", 0.95),
        "e2e_p95_long":   long_pct("e2e_request_latency",   0.95),
        "queue_p95_long": long_pct("request_queue_time",    0.95),
        "cache_hit_pct":      cache_window_hit_pct(),
        "cache_hit_pct_life": cache_lifetime_hit_pct(),
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


def _cache_color(p: Optional[float]) -> str:
    """Higher is better for prefix-cache hit rate."""
    if p is None: return ""
    return GREEN if p >= 60 else YELLOW if p >= 30 else RED


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

    # Latency table: short window (P50/P95/P99 from the latest poll-to-poll
    # delta) plus a longer-window P95 column for SLO-style stability.
    long_label = f"P95@{int(LONG_WINDOW_SECS)}s" if LONG_WINDOW_SECS < 60 else f"P95@{int(LONG_WINDOW_SECS/60)}m"
    lines.append(f"{BOLD}▸ Latency{RESET}  {DIM}(windowed percentiles){RESET}")
    lines.append(f"  {DIM}{'metric':<12}{'P50':>10}{'P95':>10}{'P99':>10}{long_label:>10}{RESET}")
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
        p50      = smry and smry.get(f"{key}_p50")
        p95      = smry and smry.get(f"{key}_p95")
        p99      = smry and smry.get(f"{key}_p99")
        p95_long = smry and smry.get(f"{key}_p95_long")
        lines.append(f"  {label:<12}"
                     f"{fmt(scaled(p50)):>10}{fmt(scaled(p95)):>10}{fmt(scaled(p99)):>10}"
                     f"{fmt(scaled(p95_long)):>10}")
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
        # Prefix cache hit rate (only if vLLM exposes the counter at all)
        if smry["cache_hit_pct_life"] is not None or smry["cache_hit_pct"] is not None:
            win = smry["cache_hit_pct"]
            life = smry["cache_hit_pct_life"]
            cc_w = _cache_color(win)
            cc_l = _cache_color(life)
            lines.append(
                f"  Prefix cache hit  :  "
                f"{cc_w}{BOLD}{fmt(win, '{:>5.1f}', '   —')}%{RESET} {DIM}window{RESET}"
                f"   {cc_l}{BOLD}{fmt(life, '{:>5.1f}', '   —')}%{RESET} {DIM}life{RESET}"
            )
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
            # Lifetime — uses vLLM's reported process start time when available
            vllm_up = vllm_uptime_seconds([inst])
            if vllm_up is not None and vllm_up > 0:
                lines.append(f"    Lifetime         : {BOLD}{fmt_money(cost.for_seconds(vllm_up), cost.currency):>12}{RESET}  "
                             f"{DIM}(over {fmt_duration(vllm_up)} of vLLM uptime){RESET}")
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


def _median(values: List[float]) -> float:
    """Plain median (no statistics-module dep). Caller must pass a non-empty list."""
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def long_window_counter_delta(inst: Instance, fragment: str,
                              target_secs: float) -> Optional[Tuple[float, float]]:
    """Counter delta over the last `target_secs` for a metric on `inst`.

    Returns (delta, elapsed_seconds) or None when we lack history. Uses the
    same snapshot-history walk as `long_window_percentile` so a 60-second
    request-share view stays consistent with the 60-second latency P95.
    """
    s = inst.snapshot
    if s is None or not inst.snapshot_history:
        return None
    target_ts = s.timestamp - target_secs
    base: Optional[Snapshot] = None
    for h in inst.snapshot_history:
        if h is s:
            break
        if h.timestamp <= target_ts:
            base = h
    if base is None:
        base = inst.snapshot_history[0]
    if base is s:
        return None
    name = find_metric(s.counters, fragment)
    if not name:
        return None
    base_v = base.counters.get(name)
    if base_v is None:
        return None
    return max(0.0, s.counters[name] - base_v), max(0.0, s.timestamp - base.timestamp)


def _imbalance_check_for_group(
    group: List[Tuple[Instance, Dict[str, Any]]],
) -> Tuple[List[Tuple[str, str, bool]], int]:
    """Run the imbalance checks over a group of replicas (already filtered
    to one model, all with valid summaries). This block now focuses on
    *performance* asymmetry (slow GPU, KV pressure); request-distribution
    asymmetry is in `_load_balance_check_for_group` and rendered separately.

    Returns (results, n_checks) where each result is (label, detail, is_bad).
    Median-based outlier detection: ratio = max / median.
    """
    results: List[Tuple[str, str, bool]] = []

    # KV cache — Δ in percentage points, threshold 15pp
    kvs = [(i, s["kv_pct"]) for i, s in group if s["kv_pct"] is not None]
    if len(kvs) >= 2:
        ks = [k[1] for k in kvs]
        k_min, k_max = min(ks), max(ks)
        outlier = max(kvs, key=lambda x: x[1])[0]
        bad = (k_max - k_min) > 15
        if bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {k_max:.1f}% "
                      f"(others as low as {k_min:.1f}%, Δ={k_max - k_min:.1f}pp)")
        else:
            detail = f"range {k_min:.1f}%–{k_max:.1f}%"
        results.append(("KV cache", detail, bad))

    # TTFT P95 — outlier vs median, threshold 2×
    ttfts = [(i, s["ttft_p95"] * 1000) for i, s in group if s["ttft_p95"] is not None]
    if len(ttfts) >= 2:
        ts = [t[1] for t in ttfts]
        t_med = _median(ts)
        t_max = max(ts)
        outlier = max(ttfts, key=lambda x: x[1])[0]
        ratio = t_max / max(0.001, t_med)
        bad = ratio > 1.5
        if bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {t_max:.0f}ms is "
                      f"{ratio:.1f}× median ({t_med:.0f}ms)")
        else:
            detail = f"median {t_med:.0f}ms, max {t_max:.0f}ms ({ratio:.1f}×)"
        results.append(("slow-replica (TTFT)", detail, bad))

    # TPOT P95 — outlier vs median, threshold 2×
    tpots = [(i, s["tpot_p95"] * 1000) for i, s in group if s["tpot_p95"] is not None]
    if len(tpots) >= 2:
        ts = [t[1] for t in tpots]
        t_med = _median(ts)
        t_max = max(ts)
        outlier = max(tpots, key=lambda x: x[1])[0]
        ratio = t_max / max(0.001, t_med)
        bad = ratio > 1.5
        if bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {t_max:.1f}ms is "
                      f"{ratio:.1f}× median ({t_med:.1f}ms)")
        else:
            detail = f"median {t_med:.1f}ms, max {t_max:.1f}ms ({ratio:.1f}×)"
        results.append(("slow-decode (TPOT)", detail, bad))

    return results, len(results)


def _render_imbalance_sections(
    summaries: List[Tuple[Instance, Optional[Dict[str, Any]]]],
) -> List[List[str]]:
    """Yield one block of lines per model-group that has ≥2 ok replicas.

    Healthy groups collapse to a single line; groups with at least one
    failed check expand to detail. When EVERY group across the deployment
    is healthy and there's more than one of them, the whole section
    collapses further into a single "✓ N model groups OK" summary —
    keeps short terminals readable.
    """
    by_model: Dict[Optional[str], List[Tuple[Instance, Dict[str, Any]]]] = {}
    for inst, s in summaries:
        if s is None:
            continue
        by_model.setdefault(inst.model, []).append((inst, s))

    multi_model = len([k for k in by_model if k is not None]) > 1
    blocks: List[List[str]] = []
    all_healthy = True
    total_replicas = 0
    for model, group in by_model.items():
        if len(group) < 2:
            continue
        total_replicas += len(group)
        results, n_checks = _imbalance_check_for_group(group)
        if not results:
            continue

        n_bad = sum(1 for _, _, is_bad in results if is_bad)
        if n_bad > 0:
            all_healthy = False
        suffix = f"  {DIM}(× {len(group)} replicas){RESET}"
        if multi_model and model:
            head = f"{BOLD}▸ Imbalance check  {short_model_name(model)}{RESET}{suffix}"
        else:
            head = f"{BOLD}▸ Imbalance check{RESET}{suffix}"

        if n_bad == 0:
            blocks.append([f"{head}  {GREEN}✓ all {n_checks} checks pass{RESET}"])
        else:
            block = [f"{head}  {YELLOW}⚠ {n_bad}/{n_checks} failed{RESET}"]
            for label, detail, bad in results:
                icon = f"{RED}⚠{RESET}" if bad else f"{GREEN}✓{RESET}"
                block.append(f"  {icon} {label:<20} {detail}")
            blocks.append(block)

    # All-healthy multi-group → one-liner summary
    if all_healthy and len(blocks) > 1:
        return [[f"{BOLD}▸ Imbalance check{RESET}  "
                 f"{GREEN}✓ all {len(blocks)} model groups OK{RESET}  "
                 f"{DIM}(× {total_replicas} replicas total){RESET}"]]
    return blocks


# How long a skew has to persist before we call it "sticky" rather than noise.
LB_STICKY_WINDOW_SECS = 60.0
# Minimum total req/s in the window before we trust the share computation
# at all (very low traffic = high variance = false positives).
LB_MIN_TOTAL_REQS = 5.0


def _load_balance_check_for_group(
    group: List[Tuple[Instance, Dict[str, Any]]],
) -> List[Tuple[str, str, bool]]:
    """Three checks for request-distribution asymmetry within one model group.

    Each returns (label, detail, is_bad). Median-based ratios so an idle
    replica can't fake an alert. Two flavors of "bad" are reported:
      * **instant skew** — current poll's share max/median > 1.5
      * **sticky skew**  — same condition holds in the long-window cumulative
        delta (default ~60s). Sticky is the alert that actually matters for
        production load balancers; instant alone is too noisy.
    """
    results: List[Tuple[str, str, bool]] = []

    # --- Request share (instant + sticky) ---
    rps_list = [(i, s["req_rps"]) for i, s in group if s["req_rps"] is not None]
    total_rps = sum(r for _, r in rps_list)
    if len(rps_list) >= 2 and total_rps >= 0.5:
        shares = [(inst, r / total_rps * 100) for inst, r in rps_list]
        share_vals = [s for _, s in shares]
        med = _median(share_vals)
        max_share = max(share_vals)
        outlier = max(shares, key=lambda x: x[1])[0]
        instant_bad = max_share > 1.5 * med and max_share > 1.5 * (100.0 / len(rps_list))

        # Sticky check: look at the cumulative request_success delta over the
        # long window (~60s) and recompute share. If the skew is still there,
        # it's not just one bursty poll.
        sticky_bad = False
        sticky_detail = ""
        deltas = []
        elapsed_min = None
        for inst, _ in rps_list:
            d = long_window_counter_delta(inst, "request_success",
                                          LB_STICKY_WINDOW_SECS)
            if d is None:
                deltas.append((inst, None))
                continue
            d_val, d_elapsed = d
            deltas.append((inst, d_val))
            elapsed_min = d_elapsed if elapsed_min is None else min(elapsed_min, d_elapsed)
        long_total = sum(d for _, d in deltas if d is not None)
        if long_total >= LB_MIN_TOTAL_REQS and elapsed_min and elapsed_min >= 10:
            long_shares = [(i, (d / long_total * 100) if d is not None else 0.0)
                           for i, d in deltas]
            lmed = _median([s for _, s in long_shares])
            lmax = max(s for _, s in long_shares)
            sticky_outlier = max(long_shares, key=lambda x: x[1])[0]
            if lmax > 1.5 * lmed and lmax > 1.5 * (100.0 / len(long_shares)):
                sticky_bad = True
                sticky_detail = (f"{BOLD}{sticky_outlier.name}{RESET} handled "
                                 f"{lmax:.0f}% of requests for {int(elapsed_min)}s "
                                 f"(median {lmed:.0f}%)")

        bad = sticky_bad
        if sticky_bad:
            detail = sticky_detail
            label = "sticky-looking skew"
        elif instant_bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {max_share:.0f}% of req/s right now "
                      f"(median {med:.0f}%) — may be a one-poll blip")
            label = "instant skew"
            # Don't flag instant-only as bad; just informational
            bad = False
        else:
            shares_fmt = ", ".join(f"{s:.0f}%" for s in sorted(share_vals, reverse=True))
            detail = f"shares {shares_fmt}, median {med:.0f}%"
            label = "request share"
        results.append((label, detail, bad))

    # --- Running req (instantaneous; complements request share) ---
    runs = [(i, s["running"]) for i, s in group if s["running"] is not None]
    if len(runs) >= 2:
        rs = [r for _, r in runs]
        r_min, r_max, r_med = min(rs), max(rs), _median(rs)
        outlier = max(runs, key=lambda x: x[1])[0]
        bad = (r_max - r_min) > 3 and r_max > 1.5 * max(1.0, r_med)
        if bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {r_max:.0f} running "
                      f"(median {r_med:.0f}, others as low as {r_min:.0f})")
        else:
            detail = f"range {r_min:.0f}–{r_max:.0f}, median {r_med:.0f}"
        results.append(("running req", detail, bad))

    # --- Token share (in+out tok/s combined) ---
    tok_list = []
    for inst, s in group:
        pr, gr = s["prompt_rps"], s["gen_rps"]
        if pr is None and gr is None:
            continue
        tok_list.append((inst, (pr or 0) + (gr or 0)))
    total_tok = sum(t for _, t in tok_list)
    if len(tok_list) >= 2 and total_tok >= 1.0:
        share_vals = [t / total_tok * 100 for _, t in tok_list]
        med = _median(share_vals)
        max_share = max(share_vals)
        outlier = max(zip([i for i, _ in tok_list], share_vals), key=lambda x: x[1])[0]
        bad = max_share > 1.5 * med and max_share > 1.5 * (100.0 / len(tok_list))
        if bad:
            detail = (f"{BOLD}{outlier.name}{RESET}: {max_share:.0f}% of tok/s "
                      f"(median {med:.0f}%)")
        else:
            shares_fmt = ", ".join(f"{s:.0f}%" for s in sorted(share_vals, reverse=True))
            detail = f"shares {shares_fmt}, median {med:.0f}%"
        results.append(("token share", detail, bad))

    return results


def _render_load_balance_sections(
    summaries: List[Tuple[Instance, Optional[Dict[str, Any]]]],
) -> List[List[str]]:
    """One ▸ Load balance block per model group (≥2 replicas).

    Same compaction rules as Imbalance check: healthy groups → one line,
    all-healthy multi-group → single section summary.
    """
    by_model: Dict[Optional[str], List[Tuple[Instance, Dict[str, Any]]]] = {}
    for inst, s in summaries:
        if s is None:
            continue
        by_model.setdefault(inst.model, []).append((inst, s))

    multi_model = len([k for k in by_model if k is not None]) > 1
    blocks: List[List[str]] = []
    all_healthy = True
    total_replicas = 0
    for model, group in by_model.items():
        if len(group) < 2:
            continue
        total_replicas += len(group)
        results = _load_balance_check_for_group(group)
        if not results:
            continue

        n_checks = len(results)
        n_bad = sum(1 for _, _, is_bad in results if is_bad)
        if n_bad > 0:
            all_healthy = False
        suffix = f"  {DIM}(× {len(group)} replicas){RESET}"
        if multi_model and model:
            head = f"{BOLD}▸ Load balance  {short_model_name(model)}{RESET}{suffix}"
        else:
            head = f"{BOLD}▸ Load balance{RESET}{suffix}"

        if n_bad == 0:
            blocks.append([f"{head}  {GREEN}✓ {n_checks} checks pass{RESET}"])
        else:
            block = [f"{head}  {YELLOW}⚠ {n_bad}/{n_checks} failed{RESET}"]
            for label, detail, bad in results:
                icon = f"{RED}⚠{RESET}" if bad else f"{GREEN}✓{RESET}"
                block.append(f"  {icon} {label:<20} {detail}")
            blocks.append(block)

    if all_healthy and len(blocks) > 1:
        return [[f"{BOLD}▸ Load balance{RESET}  "
                 f"{GREEN}✓ all {len(blocks)} model groups OK{RESET}  "
                 f"{DIM}(× {total_replicas} replicas total){RESET}"]]
    return blocks


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
    # Only show the Cache% column when at least one replica exposes prefix-cache
    # metrics — saves table width on older vLLM versions that don't have them.
    show_cache = any(
        smry and (smry.get("cache_hit_pct") is not None
                  or smry.get("cache_hit_pct_life") is not None)
        for _, smry in summaries
    )
    cache_hdr = f"  Cache%" if show_cache else ""
    cache_pad = 8 if show_cache else 0
    # +13 for the two new columns (Req/s + Req%) inserted between Wait and Swap
    rule = GRAY + "─" * (86 + 13 + pad_extra + cache_pad) + RESET
    # Pre-compute totals for Req/s and Req% so each row knows its share.
    total_req_rps = sum((s["req_rps"] or 0) for _, s in summaries if s is not None)

    lines[-1] = rule  # replace the rule we appended earlier
    lines.append(f"{DIM} {'DP':<{name_w}}  Status   Run  Wait  Req/s  Req%  Swap   KV%{cache_hdr}      in tok/s  out tok/s   TTFT-P95  TPOT-P95{RESET}")
    lines.append(rule)

    have_first_sample = False
    for inst, smry in summaries:
        if smry is None:
            err = (inst.error or "no data yet")[:34]
            cache_dash = f"{GRAY}     —{RESET}" if show_cache else ""
            lines.append(f" {inst.name:<{name_w}} {RED}DOWN  {RESET} "
                         f"{GRAY}  —     —    —    —     —      —{RESET}{cache_dash}"
                         f"{GRAY}          —          —          —         —{RESET}  "
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

        # Req% — share of the deployment's total request rate. Useful for
        # spotting load-balancer skew at a glance.
        rps = smry["req_rps"]
        req_pct = (rps / total_req_rps * 100) if (rps and total_req_rps > 0) else None

        # Cache% cell (only when column is shown). Prefer window value;
        # fall back to lifetime if window hasn't accumulated samples yet.
        cache_cell = ""
        if show_cache:
            cache_v = smry["cache_hit_pct"]
            if cache_v is None:
                cache_v = smry["cache_hit_pct_life"]
            cc = _cache_color(cache_v)
            cache_cell = f"  {cc}{fmt(cache_v, '{:.0f}'):>4}%{RESET}"

        lines.append(
            f" {inst.name:<{name_w}} {status} "
            f"{fmt(smry['running'], '{:.0f}'):>4}  "
            f"{wc}{fmt(smry['waiting'], '{:.0f}'):>4}{RESET}  "
            f"{fmt(rps,     '{:.1f}'):>4}  "
            f"{fmt(req_pct, '{:.0f}'):>3}%  "
            f"{sc}{fmt(smry['swapped'], '{:.0f}'):>4}{RESET}   "
            f"{kc}{fmt(smry['kv_pct'], '{:.1f}'):>5}%{RESET}"
            f"{cache_cell}    "
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

        # Aggregate Cache%: weighted by queries rate (so big replicas dominate).
        agg_cache_cell = ""
        if show_cache:
            tot_q = tot_h = 0.0
            for inst, sm in summaries:
                if sm is None or sm.get("_prev") is None:
                    continue
                dt = sm.get("_dt") or 0.0
                if dt <= 0:
                    continue
                s, p = sm["_snap"], sm["_prev"]
                qn = find_metric(s.counters, "prefix_cache_queries")
                hn = find_metric(s.counters, "prefix_cache_hits")
                if qn and qn in p.counters:
                    tot_q += max(0.0, s.counters[qn] - p.counters[qn])
                if hn and hn in p.counters:
                    tot_h += max(0.0, s.counters[hn] - p.counters[hn])
            agg = (tot_h / tot_q * 100) if tot_q > 0 else None
            cc = _cache_color(agg)
            agg_cache_cell = f"  {cc}{fmt(agg, '{:.0f}'):>4}%{RESET}"

        sum_req_rps = sum((s["req_rps"] or 0) for s in ok_smries)
        lines.append(
            f" {BOLD}ALL{RESET}{' ' * (name_w - 3)}        "
            f" {sum_run:>4.0f}  "
            f"{sum_wait:>4.0f}  "
            f"{sum_req_rps:>4.1f}  "
            f"{DIM}100%{RESET}  "
            f"{sum_swap:>4.0f}    "
            f"{DIM}max{RESET}{fmt(max_kv, '{:.1f}'):>4}%"
            f"{agg_cache_cell}    "
            f"{sum_pin:>8.0f}   "
            f"{sum_pout:>8.0f}    "
            f"{fmt(ttft_ms_all, '{:.0f}'):>6}ms   "
            f"{fmt(tpot_ms_all, '{:.1f}'):>6}ms"
        )

        # Load balance + Imbalance check — emit with smart spacing: blank
        # separator before EXPANDED blocks (something failed and we want
        # visual breathing room), but consecutive single-line summaries pack
        # together without blanks to save vertical space on short terminals.
        all_blocks = (_render_load_balance_sections(summaries)
                      + _render_imbalance_sections(summaries))
        prev_one_liner = False
        for block in all_blocks:
            is_one_liner = len(block) == 1
            if not (prev_one_liner and is_one_liner):
                lines.append("")
            lines.extend(block)
            prev_one_liner = is_one_liner

    if not have_first_sample:
        lines.append("")
        lines.append(f"  {DIM}(throughput & percentiles populate after the 2nd poll…){RESET}")

    # ── Compute once for both Cost and Cumulative ──
    first_seens = [inst.first_seen for inst in instances if inst.first_seen is not None]
    have_data = bool(first_seens)
    uptime = (time.time() - min(first_seens)) if first_seens else 0.0

    sum_pin_life = sum_pout_life = sum_req_life = 0.0
    for inst in instances:
        if inst.snapshot is None:
            continue
        sc = inst.snapshot.counters
        def _ctr(frag: str) -> Optional[float]:
            n = find_metric(sc, frag)
            return sc[n] if n else None
        pin = _ctr("prompt_tokens")
        pout = _ctr("generation_tokens")
        req = _ctr("request_success")
        if pin is not None:  sum_pin_life  += pin
        if pout is not None: sum_pout_life += pout
        if req is not None:  sum_req_life  += req

    # ── Cost section ── (always shown when enabled; doesn't depend on
    # Cumulative being rendered, so a short terminal won't hide it)
    if have_data and cost is not None and cost.enabled:
        ok_smries = [s for s in (summarize(i) for i in instances) if s is not None]

        def _counter(snap: Snapshot, frag: str) -> float:
            nm = find_metric(snap.counters, frag)
            return snap.counters[nm] if nm else 0.0

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
            vllm_up = vllm_uptime_seconds(instances)
            if vllm_up is not None and vllm_up > 0:
                lines.append(f"    Lifetime     : {BOLD}{fmt_money(cost.for_seconds(vllm_up), cost.currency):>12}{RESET}  "
                             f"{DIM}(over {fmt_duration(vllm_up)} of vLLM uptime){RESET}")
            lines.append(f"    This session : {BOLD}{fmt_money(cost.for_seconds(uptime), cost.currency):>12}{RESET}  "
                         f"{DIM}(over {fmt_duration(uptime)}){RESET}")

        # Margin (only meaningful when both pricings are on)
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

        # Hint when only one pricing model is on — explains the "missing"
        # subsection the user might expect.
        if cost.compute_enabled and not cost.token_enabled:
            lines.append(f"  {DIM}✦ pass --cost-in PRICE --cost-out PRICE to also see token cost and Margin{RESET}")
        elif cost.token_enabled and not cost.compute_enabled:
            lines.append(f"  {DIM}✦ pass --gpu-cost-hour PRICE to also see compute cost and Margin{RESET}")

    # ── Cumulative section ── (auto-hidden when terminal is too short)
    if have_data:
        try:
            term_h = shutil.get_terminal_size((100, 24)).lines
        except (AttributeError, OSError):
            term_h = 24
        # Approx height of Cumulative + Legend so we can decide ahead of time.
        cumulative_h = 5 + len(instances) + 1   # header + rule + rule + rows + rule + ALL
        legend_h = 3                            # blank + Legend + Ctrl-C
        projected = len(lines) + cumulative_h + legend_h

        if projected <= term_h:
            lines.append("")
            lines.append(f"{BOLD}▸ Cumulative{RESET}  "
                         f"{DIM}(life = vLLM counters · sess = peaks observed since monitor uptime {fmt_duration(uptime)}){RESET}")
            lines.append(rule)
            lines.append(f"{DIM} {'DP':<{name_w}}   life-Prompt  life-Output  life-Reqs   peak-Run  peak-Wait  peak-KV%   peak in/out tok/s{RESET}")
            lines.append(rule)
            for inst in instances:
                s = inst.snapshot
                if s is None:
                    lines.append(f" {inst.name:<{name_w}} {GRAY}     —            —            —          —          —         —          — / —{RESET}")
                    continue
                def _c(frag: str) -> Optional[float]:
                    nm = find_metric(s.counters, frag)
                    return s.counters[nm] if nm else None
                pin, pout, req = _c("prompt_tokens"), _c("generation_tokens"), _c("request_success")
                swap_warn = RED if inst.peak_swapped > 0 else ""
                kv_warn   = RED if inst.peak_kv > 90 else YELLOW if inst.peak_kv > 75 else ""
                lines.append(
                    f" {inst.name:<{name_w}}  "
                    f"{humanize(pin):>10}  {humanize(pout):>10}  {humanize(req):>9}    "
                    f"{inst.peak_running:>6.0f}     {inst.peak_waiting:>4.0f}    "
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
        else:
            lines.append("")
            shortfall = projected - term_h
            lines.append(f"{DIM}(▸ Cumulative hidden — needs {shortfall} more terminal rows; "
                         f"resize, or run `vllm-htop --output json` for full data){RESET}")

    lines.append("")
    # Legend: one entry per URL. When the model is known, show it as the
    # headline (it's already the row prefix) plus the engine count; otherwise
    # fall back to listing the row names verbatim.
    by_url_legend: Dict[str, List[Instance]] = {}
    for i in instances:
        by_url_legend.setdefault(i.url.replace("/metrics", ""), []).append(i)
    legend_parts = []
    for url, insts in by_url_legend.items():
        model = insts[0].model
        n_eng = sum(1 for i in insts if i.engine is not None)
        if model:
            head = short_model_name(model)
            suffix = f" ×{n_eng} engines" if n_eng > 1 else ""
            legend_parts.append(f"{head}{suffix} @ {url}")
        else:
            names = [i.name for i in insts]
            head = f"{{{','.join(names)}}}" if len(names) > 1 else names[0]
            legend_parts.append(f"DP{head}={url}")
    legend = "  ".join(legend_parts)
    lines.append(GRAY + "Legend: " + legend + RESET)
    lines.append(GRAY + "Ctrl-C to exit" + RESET)
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


# ─────────────────────────── JSON output ────────────────────────────────
def _replica_to_dict(inst: "Instance", smry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    r: Dict[str, Any] = {
        "name":   inst.name,
        "url":    inst.url.replace("/metrics", ""),
        "model":  inst.model,
        "engine": inst.engine,
        "status": "DOWN" if smry is None else ("STALE" if inst.error else "OK"),
    }
    if inst.error:
        r["error"] = inst.error
    if smry is None:
        return r

    r.update({
        "running": smry["running"],
        "waiting": smry["waiting"],
        "swapped": smry["swapped"],
        "kv_pct":  smry["kv_pct"],
        "prefix_cache_hit_pct_window":   smry["cache_hit_pct"],
        "prefix_cache_hit_pct_lifetime": smry["cache_hit_pct_life"],
        "throughput": {
            "prompt_tokens_per_s": smry["prompt_rps"],
            "gen_tokens_per_s":    smry["gen_rps"],
            "requests_per_s":      smry["req_rps"],
        },
        "latency_seconds": {
            "ttft":  {"p50": smry["ttft_p50"], "p95": smry["ttft_p95"], "p99": smry["ttft_p99"],
                      f"p95_{int(LONG_WINDOW_SECS)}s": smry["ttft_p95_long"]},
            "tpot":  {"p50": smry["tpot_p50"], "p95": smry["tpot_p95"], "p99": smry["tpot_p99"],
                      f"p95_{int(LONG_WINDOW_SECS)}s": smry["tpot_p95_long"]},
            "e2e":   {"p50": smry["e2e_p50"],  "p95": smry["e2e_p95"],  "p99": smry["e2e_p99"],
                      f"p95_{int(LONG_WINDOW_SECS)}s": smry["e2e_p95_long"]},
            "queue": {"p95": smry["queue_p95"], f"p95_{int(LONG_WINDOW_SECS)}s": smry["queue_p95_long"]},
        },
    })

    s = smry["_snap"]
    def ctr(frag: str) -> Optional[float]:
        n = find_metric(s.counters, frag)
        return s.counters[n] if n else None
    r["lifetime"] = {
        "prompt_tokens_total":   ctr("prompt_tokens"),
        "gen_tokens_total":      ctr("generation_tokens"),
        "requests_total":        ctr("request_success"),
        "prefix_cache_queries_total": ctr("prefix_cache_queries"),
        "prefix_cache_hits_total":    ctr("prefix_cache_hits"),
    }

    if inst.first_seen is not None:
        r["session"] = {
            "first_seen_timestamp":     inst.first_seen,
            "uptime_seconds":           time.time() - inst.first_seen,
            "peak_running":             inst.peak_running,
            "peak_waiting":             inst.peak_waiting,
            "peak_swapped":             inst.peak_swapped,
            "peak_kv_pct":              inst.peak_kv,
            "peak_prompt_tokens_per_s": inst.peak_prompt_rps,
            "peak_gen_tokens_per_s":    inst.peak_gen_rps,
        }
    return r


def _cost_to_dict(instances: List["Instance"],
                  summaries: List[Tuple["Instance", Optional[Dict[str, Any]]]],
                  cost: "CostConfig") -> Dict[str, Any]:
    out: Dict[str, Any] = {"currency": cost.currency}

    def _counter(snap, frag):
        n = find_metric(snap.counters, frag)
        return snap.counters[n] if n else 0.0

    if cost.token_enabled:
        out["token_pricing"] = {
            "input_usd_per_million":  cost.input_per_m,
            "output_usd_per_million": cost.output_per_m,
        }
        sum_pin_life = sum_pout_life = 0.0
        sess_p = sess_g = 0.0
        for i in instances:
            if i.snapshot is None:
                continue
            cp = _counter(i.snapshot, "prompt_tokens")
            cg = _counter(i.snapshot, "generation_tokens")
            sum_pin_life  += cp
            sum_pout_life += cg
            sess_p += max(0.0, cp - (i.baseline_prompt_tokens or cp))
            sess_g += max(0.0, cg - (i.baseline_gen_tokens    or cg))
        life_total, life_in, life_out = cost.for_tokens(sum_pin_life, sum_pout_life)
        sess_total, _, _ = cost.for_tokens(sess_p, sess_g)
        ok = [s for _, s in summaries if s is not None]
        sum_pin_rate  = sum((s["prompt_rps"] or 0) for s in ok)
        sum_pout_rate = sum((s["gen_rps"]    or 0) for s in ok)
        per_sec, _, _ = cost.for_tokens(sum_pin_rate, sum_pout_rate)
        out["token_based"] = {
            "lifetime_total":     life_total,
            "lifetime_input":     life_in,
            "lifetime_output":    life_out,
            "session_total":      sess_total,
            "current_per_hour":   per_sec * 3600.0,
        }

    if cost.compute_enabled:
        out["compute_pricing"] = {
            "gpu_model":          cost.gpu_model,
            "num_gpus":           cost.num_gpus,
            "usd_per_gpu_hour":   cost.gpu_cost_hour,
            "source":             cost.gpu_price_source,
        }
        first_seens = [i.first_seen for i in instances if i.first_seen is not None]
        uptime = (time.time() - min(first_seens)) if first_seens else 0.0
        vllm_up = vllm_uptime_seconds(instances)
        out["compute_based"] = {
            "burn_rate_per_hour": cost.compute_per_hour,
            "session_total":      cost.for_seconds(uptime),
            "lifetime_total":     cost.for_seconds(vllm_up) if vllm_up else None,
            "vllm_uptime_seconds": vllm_up,
        }

    return out


def build_json_payload(instances: List["Instance"], interval: float,
                       cost: Optional["CostConfig"]) -> Dict[str, Any]:
    """Build the per-poll JSON object emitted by `--output json`."""
    summaries = [(i, summarize(i)) for i in instances]
    n = len(instances)
    up = sum(1 for i, s in summaries if s is not None and not i.error)

    replicas = [_replica_to_dict(i, s) for i, s in summaries]

    agg: Dict[str, Any] = {"up": up, "total": n}
    ok_smries = [s for _, s in summaries if s is not None]
    if ok_smries:
        kvs = [s["kv_pct"] for s in ok_smries if s["kv_pct"] is not None]
        agg.update({
            "running_total": sum((s["running"] or 0) for s in ok_smries),
            "waiting_total": sum((s["waiting"] or 0) for s in ok_smries),
            "swapped_total": sum((s["swapped"] or 0) for s in ok_smries),
            "kv_pct_max":    max(kvs) if kvs else None,
            "prompt_tokens_per_s_total": sum((s["prompt_rps"] or 0) for s in ok_smries),
            "gen_tokens_per_s_total":    sum((s["gen_rps"]    or 0) for s in ok_smries),
        })
        snaps_pairs = [(s["_snap"], s["_prev"]) for s in ok_smries]
        for label, frag in [("ttft", "time_to_first_token"),
                            ("tpot", "time_per_output_token"),
                            ("e2e",  "e2e_request_latency")]:
            b, c = merge_window_buckets(snaps_pairs, frag)
            agg[f"latency_{label}_p95_seconds"] = histogram_percentile(b, c, 0.95)

    payload: Dict[str, Any] = {
        "timestamp":        time.time(),
        "interval_seconds": interval,
        "replicas":         replicas,
        "aggregate":        agg,
    }
    if cost is not None and cost.enabled:
        payload["cost"] = _cost_to_dict(instances, summaries, cost)
    return payload


def render_json(instances: List["Instance"], interval: float,
                cost: Optional["CostConfig"] = None) -> None:
    """Emit a single JSON object on stdout (newline-terminated → JSONL-friendly).

    Designed to be piped to scripts, log files, or alerting pipelines:
        vllm-htop --output json --interval 5 >> /var/log/vllm-htop.jsonl
        vllm-htop --output json --once | jq '.aggregate.kv_pct_max'
    """
    print(json.dumps(build_json_payload(instances, interval, cost)), flush=True)


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
    ap.add_argument("--output",   choices=("auto", "table", "detail", "json"),
                    default="auto",
                    help="output mode. 'auto' picks table for ≥2 replicas else "
                         "detail; 'json' emits a JSONL stream (one object per "
                         "poll), suitable for piping to scripts.")
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

    # Resolve output mode. `--output` is the modern way; the legacy
    # --table / --detail flags still win when set (backward compat).
    if args.output == "json":
        mode = "json"
    elif args.table:
        mode = "table"
    elif args.detail:
        mode = "detail"
    elif args.output == "table":
        mode = "table"
    elif args.output == "detail":
        mode = "detail"
    else:
        mode = "table" if len(instances) >= 2 else "detail"

    # htop-style: claim the alternate screen buffer for the interactive loop
    # so the monitor's frames don't pile up in the scrollback. Skip when the
    # output is structured (JSON), one-shot (--once), or being captured
    # (stdout isn't a tty — e.g. `vllm-htop > out.log` or `| tee`).
    use_alt = (mode in ("table", "detail")
               and not args.once
               and sys.stdout.isatty())
    if use_alt:
        enter_alt_screen()

    try:
        while True:
            fetch_all(instances, timeout=args.timeout)
            if mode == "json":
                render_json(instances, interval=args.interval, cost=cost)
            elif mode == "table":
                render_table(instances, interval=args.interval, cost=cost)
            else:
                render_detail(instances[0], cost=cost)
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        leave_alt_screen()


if __name__ == "__main__":
    main()
