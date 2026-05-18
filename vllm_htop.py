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
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

__version__ = "0.1.0"


# ───────────────────────────── ANSI styling ──────────────────────────────
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, GRAY = (
    "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[90m",
)
CLEAR = "\033[2J\033[H"


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


def parse_snapshot(raw: str) -> Snapshot:
    types, samples = parse_prom_text(raw)
    snap = Snapshot(timestamp=time.time())
    for name, kind in types.items():
        if not name.startswith("vllm"):
            continue
        if kind == "counter":
            total = 0.0
            for sample_name in (name, name + "_total"):
                for _l, v in samples.get(sample_name, []):
                    total += v
            snap.counters[name] = total
        elif kind == "gauge":
            total = 0.0
            for _l, v in samples.get(name, []):
                total += v
            snap.gauges[name] = total
        elif kind == "histogram":
            buckets: Dict[str, float] = {}
            for labels, v in samples.get(name + "_bucket", []):
                le = labels.get("le", "+Inf")
                buckets[le] = buckets.get(le, 0.0) + v
            count = sum(v for _l, v in samples.get(name + "_count", []))
            sm    = sum(v for _l, v in samples.get(name + "_sum",   []))
            snap.histograms[name] = {"buckets": buckets, "count": count, "sum": sm}
    return snap


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


# ─────────────────────────── instance model ──────────────────────────────
@dataclass
class Instance:
    name: str
    url: str
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


def fetch_one(inst: Instance, timeout: float) -> None:
    try:
        raw = fetch_metrics(inst.url, timeout=timeout)
        inst.prev = inst.snapshot
        inst.snapshot = parse_snapshot(raw)
        inst.error = None
        _update_session(inst)
    except Exception as e:
        inst.error = f"{type(e).__name__}: {e}"


def fetch_all(instances: List[Instance], timeout: float = 5.0) -> None:
    """Parallel fetch — keeps total fetch time ≈ slowest single fetch."""
    if not instances:
        return
    with ThreadPoolExecutor(max_workers=min(32, len(instances))) as ex:
        list(ex.map(lambda i: fetch_one(i, timeout), instances))


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


def render_detail(inst: Instance) -> None:
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

    lines.append(GRAY + "Ctrl-C to exit" + RESET)
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


def render_table(instances: List[Instance], interval: float) -> None:
    """Per-DP comparison table + aggregate row + imbalance check."""
    summaries = [(inst, summarize(inst)) for inst in instances]
    n = len(instances)
    up = sum(1 for inst, smry in summaries if smry is not None and not inst.error)

    lines: List[str] = [CLEAR]
    ts = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
    health = f"{GREEN}{up}/{n} up{RESET}" if up == n else f"{RED}{up}/{n} up{RESET}"
    lines.append(f"{BOLD}{CYAN}vLLM DP Monitor{RESET}  {DIM}│{RESET}  "
                 f"{health}  {DIM}│{RESET}  {ts}  {DIM}(interval={interval}s){RESET}")
    lines.append(GRAY + "─" * 86 + RESET)
    lines.append(f"{DIM} DP  Status   Run  Wait  Swap   KV%      in tok/s  out tok/s   TTFT-P95  TPOT-P95{RESET}")
    lines.append(GRAY + "─" * 86 + RESET)

    have_first_sample = False
    for inst, smry in summaries:
        if smry is None:
            err = (inst.error or "no data yet")[:34]
            lines.append(f" {inst.name:<3} {RED}DOWN  {RESET} "
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
            f" {inst.name:<3} {status} "
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
        lines.append(GRAY + "─" * 86 + RESET)
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
            f" {BOLD}ALL{RESET}        "
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
        lines.append(GRAY + "─" * 86 + RESET)
        lines.append(f"{DIM} DP   life-Prompt  life-Output  life-Reqs   peak-Run  peak-Wait  peak-KV%   peak in/out tok/s{RESET}")
        lines.append(GRAY + "─" * 86 + RESET)

        sum_pin_life = sum_pout_life = sum_req_life = 0.0
        for inst in instances:
            s = inst.snapshot
            if s is None:
                lines.append(f" {inst.name:<3} {GRAY}     —            —            —          —          —         —          — / —{RESET}")
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
                f" {inst.name:<3}  "
                f"{humanize(pin):>10}  "
                f"{humanize(pout):>10}  "
                f"{humanize(req):>9}    "
                f"{inst.peak_running:>6.0f}     "
                f"{inst.peak_waiting:>4.0f}    "
                f"{kv_warn}{inst.peak_kv:>5.1f}%{RESET}   "
                f"{humanize(inst.peak_prompt_rps):>5}/{humanize(inst.peak_gen_rps):<5}"
                + (f"  {swap_warn}swap-seen{RESET}" if inst.peak_swapped > 0 else "")
            )
        lines.append(GRAY + "─" * 86 + RESET)
        lines.append(
            f" {BOLD}ALL{RESET}  "
            f"{BOLD}{humanize(sum_pin_life):>10}{RESET}  "
            f"{BOLD}{humanize(sum_pout_life):>10}{RESET}  "
            f"{BOLD}{humanize(sum_req_life):>9}{RESET}"
        )

    lines.append("")
    legend = "  ".join(f"DP{i.name}={i.url.replace('/metrics','')}" for i in instances)
    lines.append(GRAY + "Legend: " + legend + RESET)
    lines.append(GRAY + "Ctrl-C to exit" + RESET)
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


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
    ap.add_argument("--url", nargs="+", default=["http://localhost:8000"],
                    help="One or more vLLM server base URLs (space- or comma-separated).")
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
    args = ap.parse_args()

    urls = _expand_urls(args.url)
    if not urls:
        sys.exit("no URLs provided")

    instances = [
        Instance(name=str(i), url=u.rstrip("/") + "/metrics")
        for i, u in enumerate(urls)
    ]

    use_table = args.table or (len(instances) >= 2 and not args.detail)

    try:
        while True:
            fetch_all(instances, timeout=args.timeout)
            if use_table:
                render_table(instances, interval=args.interval)
            else:
                render_detail(instances[0])
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
