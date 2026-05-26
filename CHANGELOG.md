# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.10] — 2026-05-26

### Added
- **Cluster verdict line** — the new visual focal point of the page. Renders directly under the title with a bold, color-coded severity badge and a one-sentence description of the worst thing currently happening:
  - `● CRITICAL` (red) — any replica DOWN, or any HOT replica. Detail names the affected replica(s).
  - `◆ DEGRADED` (yellow) — recent alert event (last 60s), or LB/IMB check failing. Detail cites the actual event (`STICKY: groupA.e0 — 33% req share for 27s`) so the reader learns *what* is wrong, not just that something is.
  - `✓ HEALTHY` (green) — no warnings, no recent alerts.
  Lets you answer "is anything wrong, and if so what?" in the first 2 seconds, instead of scanning every section.
- **Section-header glyph encodes pass/fail state** on Load balance and Imbalance check headers. Green `✓` when the section passes, red `●` when any check failed. Replaces the neutral `▸` so section status is readable in the reader's peripheral vision without parsing the right-side `⚠ N/M failed` badge. Cost / Cumulative / Recent events keep the neutral `▸` since they have no pass/fail semantics.
- **Problem-row left-bar gutter** in the main table — a red `▌` on HOT/DOWN rows and yellow `▌` on STALE rows. The column header carries a matching 2-space left margin so alignment stays clean for healthy rows. The bar is visible in peripheral vision and makes the "bad actor" replica trivially scannable in a long table.

### Changed
- Rule line under the table is 1 character wider to accommodate the new left-bar gutter. ALL-row prefix is 2 spaces instead of 1.

## [0.4.9] — 2026-05-26

### Changed
- **Cumulative section is now a single cluster-wide line.** The old per-engine table needed `7 + N` rows (blank + header + rule + col header + rule + N engine rows + rule + ALL = ~14 rows for 8 engines), which made it nearly impossible to fit on a default 13" laptop terminal — the height-projection guard hid it almost every session and showed `(▸ Cumulative hidden — needs N more terminal rows)` instead. The new one-liner consolidates the genuinely Cumulative-only signal:
  ```
  ▸ Cumulative  life: 1.2M reqs · 366M in + 136M out  ·  peaks: Run 24 / Wait 6 / KV 91% / 70K/s in + 23K/s out
  ```
  Lifetime requests + tokens (in/out) on the left; cluster peak observations (Run, Wait, KV, in/out tok/s) on the right; `swap-seen` chip appended in red if any replica ever swapped during the session. The per-engine lifetime/peak breakdown is still in `--output json` for audit/log use cases — what the old in-terminal table showed per replica was largely already visible in the live table anyway. Net saving: ~12 rows on the user's 8-engine deployment, enough to make Cumulative visible by default on a 13" MacBook terminal for the first time.

### Removed
- Height-projection logic that decided whether to render the full Cumulative table vs. the "Cumulative hidden — needs N more rows" message. No longer needed — the one-liner always fits.

## [0.4.8] — 2026-05-26

### Fixed
- **Eliminated remaining cross-frame layout drift.** 0.4.5 stabilized LB/IMB *while* warnings were active, but the transitions in/out of "all healthy" still made the layout jump:
  - `▸ Load balance ✓ all N model groups OK` (one combined line) ↔ `▸ Load balance Group1 …` + `▸ Load balance Group2 …` (per-group lines) — 1 row of drift per section every time a warning appeared or cleared.
  - `▸ Recent events` was hidden when the log was empty; the moment the first event fired, the section materialized and pushed everything below it down by 2–3 rows.
  - `KV-Hit` in the summary header bar tagged the lifetime fallback with a 5-char ` life` suffix, shifting every chunk to its right whenever traffic ebbed for one poll.

  0.4.8 makes the layout **deterministic** — same number of rows in every state. Concrete changes:
  - LB/IMB unconditionally render one line per (section × model group). The multi-group all-healthy collapse is gone. Cost: 1 extra row per section when the deployment is fully healthy with N>1 model groups. Benefit: no row shift when warnings come and go.
  - Recent events always renders. When the log is empty, the header shows `(no events yet)` as a 1-line placeholder. The section never appears or disappears mid-session.
  - Cluster-aggregate `KV-Hit` in the summary bar drops the ` life` suffix. The lifetime fallback is signaled by losing the bold weight instead — same width, same column, no horizontal shift downstream.

### Changed
- Removed the within-section all-healthy multi-group collapse from `_render_load_balance_sections` / `_render_imbalance_sections`. The `combine_healthy` parameter still exists but the in-tree caller always passes `False`. (Internal — the parameter is not part of any documented API.)

## [0.4.7] — 2026-05-21

### Docs
- **Replaced the README hero image** with a polished dashboard mockup at `docs/screenshot.png` (replica table, imbalance check with severity badges, 5-minute trend sparklines, hourly/daily/30-day cost estimate, top-talker endpoints, system uptime + memory). The old 0.3.0-era `docs/screenshot.svg` is removed. The new hero is illustrative of the tool's scope rather than a 1:1 capture of the current terminal output; the README sections below give the literal CLI experience.

### Changed
- **Renamed the prefix-cache hit-rate field from `Cache` to `KV-Hit`** in both the summary header bar and the table column (`Cache%` → `KV-Hit%`). The previous label sat right next to `KV` (KV cache fill %) and both used "cache" + "%", so the reader had to stop and think: one means "how full" (lower is better, the swap warning), the other means "how often was it reused" (higher is better, the throughput win). `KV-Hit` pairs explicitly with `KV` as the "fill vs hit" dual, and the direction-of-good is immediately readable. JSON keys are unchanged (`prefix_cache_hit_pct_window` / `prefix_cache_hit_pct_lifetime`) — the rename is purely visual.

## [0.4.6] — 2026-05-21

### Added
- **Cluster-wide prefix-cache hit % in the summary header bar.** Previously the cache hit rate was only visible per replica (Cache% column) and as the table ALL-row aggregate. The headline summary bar (QPS / in / out / Run / Wait / KV / Burn / Lifetime) now also carries a `Cache xx%` chunk between KV and Burn, computed as the weighted aggregate across replicas (weights = per-replica query delta over the live window, so a busy replica's high hit rate dominates an idle one stuck at 0%). Color thresholds match the column (`_cache_color`): ≥60% green, ≥30% yellow, else red — higher is better, opposite of KV. When the live window hasn't accumulated samples yet, falls back to the lifetime weighted aggregate and tags it `life` so the dim suffix tells you it's not the live rate. The chunk is omitted entirely on deployments whose vLLM build doesn't expose `prefix_cache_queries_total` / `prefix_cache_hits_total`.

## [0.4.5] — 2026-05-21

### Fixed
- **LB/IMB layout no longer drifts between polls.** 0.4.4 made the section heights threshold-dependent — once the projection sat near the terminal-height boundary, a single check's `bad` flag flipping (e.g. token-share crossing back under 1.5× median for one poll) would expand/compact the block and shift every row below it (Cost, Cumulative-hidden, Recent events, footer). Likewise, the within-section *all-healthy multi-group collapse* could fire on the Imbalance section while Load balance still had warnings, then unfire one poll later when Imbalance also degraded — another silent 1-row shift. **New rule**: as long as ANY check fails anywhere in the deployment, the section pair locks to a fixed layout — one one-liner per `(section × model group)`, with the multi-group collapse disabled in both sections. The detail you'd have seen expanded in-place is preserved in the Recent events log just below. When the whole deployment is healthy again, the compact 1-line-per-section collapse returns. Net effect: row positions for Cost / Cumulative-hidden / Recent events stay pinned across frames, no more visual jitter.

### Removed
- The 0.4.4 height-projection switch for LB/IMB expand vs. collapse. The new warning-aware stability rule subsumes it — expanded blocks are unconditionally compacted whenever any warning is active, which already takes the worst case down to one row per (section × group).

## [0.4.4] — 2026-05-20

### Fixed
- **Expanded Load-balance / Imbalance-check blocks now collapse on short terminals.** 0.4.3 fixed the Cumulative-section projection, but on a 13" terminal with multiple active warnings (STICKY skew on one replica, SLOW TTFT on another) the expanded LB/IMB sections themselves (~4 lines each × 2 model groups) still pushed the title bar off the top of the viewport. When the projection shows the full form won't fit even with Cumulative hidden, expanded blocks now collapse back to their one-line header form (`▸ Load balance  ⚠ 2/3 failed  ↓ details in Recent events`). The detail isn't lost — every check that triggers expansion (`STICKY`, `SLOW TTFT`, `SLOW TPOT`) also records into the Recent events log, which stays rendered below. Net effect on a tight terminal: title bar + summary + the full table stay visible; LB/IMB shows the alert badge; Recent events provides the per-replica detail.

## [0.4.3] — 2026-05-20

### Fixed
- **Cumulative-section height projection was under-counting**, letting it render even when the total content would overflow the viewport — pushing the title bar and summary header off the top of the screen on short terminals (notably 13" MacBook defaults). Projection now correctly includes the Recent events section (when it has entries) and the full 4-line basis footer + shortcuts, so on a tight terminal Cumulative is suppressed earlier and the most important top-of-screen info stays visible. Trade-off: when events fire on a short terminal, Cumulative now hides more aggressively — use `--output json` to see full cumulative data or resize/full-screen the terminal.

## [0.4.2] — 2026-05-20

### Added
- **Reactor-core KV cache grid** in the single-instance detail view — replaces the linear `█████░░░` bar with a 4-row × 24-column grid that fills bottom-up like a fuel tank. Same color thresholds (green < 65% / yellow < 85% / red ≥ 85%). The 2D grid grabs the eye before the user even reads the percentage. Detail view only — the table view still uses the linear column.
- **▸ Recent events** scrolling log section — captures transient signals (HOT entry, sticky-skew detection, slow-replica TTFT/TPOT, STALE / DOWN fetches) so a problem that flashed for one poll isn't lost. Capped at 50 entries internally, shows the 5 most recent; same-`(category, replica)` events are deduped within a 60-second window to avoid spam. The section renders only when there's something to show, so the healthy steady state is unaffected. Event categories:
  - `HOT` (alert / red) — a replica trips ≥2 of: KV > 85%, Wait > 5, Swapped > 0, slow TTFT, slow TPOT
  - `STICKY` (alert / red) — request share max/median > 1.5× for ≥10s
  - `SLOW` (warn / yellow) — replica's TTFT or TPOT P95 > 1.5× cluster median
  - `STALE` (warn / yellow) — fetch failed but a previous snapshot exists
  - `DOWN` (alert / red) — fetch failed with no prior snapshot

## [0.4.1] — 2026-05-20

### Added
- **Summary header bar** below the title — single line with the highest-density signal: total QPS, in/out tok/s, current Run/Wait, max KV% with a mini block-bar, compute burn rate, and lifetime token total (with in/out breakdown). Lets you read the cluster's pulse without scanning every replica row.
- **HOT status badge** in the DP column — replaces `OK` with `HOT` (red) when a replica trips ≥2 of these signals at once: KV > 85%, Wait > 5, Swapped > 0, TTFT P95 > 2× cluster median, TPOT P95 > 2× cluster median. Single signals stay as `OK` (transient spikes aren't problems); two or more is the line between "noise" and "this is the bad actor."
- **`Runtime basis` and `Cost basis` footer** — two short lines making implicit assumptions explicit. When vLLM exposes `process_start_time_seconds`, runtime basis cites it; otherwise it cites "observed since vllm-htop attached — vLLM uptime unavailable in DP/multiproc mode" so users immediately understand why Lifetime compute cost is missing. Cost basis cites which pricing modes are configured + their source (auto / user-provided / OpenRouter / none).

## [0.4.0] — 2026-05-20

### Added
- **Interactive keyboard shortcuts** in the live monitoring loop (htop-style; zero new config required):

  | Key | Action |
  |---|---|
  | `q` / `Q` / Ctrl-C / Ctrl-D | Graceful quit |
  | `Space` | Pause / resume refresh (last frame stays on screen) |
  | `+` / `=` | Faster refresh (cycles through 0.5s / 1s / 2s / 5s / 10s / 30s) |
  | `-` / `_` | Slower refresh (same cycle, reverse) |
  | `d` | Toggle between table view and detail view at runtime |
  | `r` | Force a refresh now (bypasses pause) |

  Implementation uses stdlib `termios` + `tty` + `select` in cbreak mode — no curses, no external library, no permanent config files. On Windows or non-TTY stdin the loop degrades silently to passive refresh (original behaviour).

  A transient status line appears below the legend when a key triggers an action (e.g. `interval → 5.0s (slower)`) and clears on the next refresh, so the user gets visible feedback.

### Changed
- **Compact Cost section when only compute pricing is configured** (no `--cost-in`/`--cost-out` passed). Collapses a 5-line block:
  ```
  ▸ Cost  (estimated · sum across 8 replicas)
    Compute-based  (NVIDIA H20-3e × 8 @ $3.5/h — auto-detected, estimate)
      Burn rate    :       $28.00/hour  (paid whether busy or idle)
      This session :        $1.61  (over 3m27s)
    ✦ pass --cost-in PRICE ...
  ```
  into 2 lines:
  ```
  ▸ Cost  ≈ $28.00/h burn (NVIDIA H20-3e × 8 @ $3.5/h auto-detected)  ·  $1.61 this session (3m27s)  [·  $54.24 lifetime (2h00m)]
    ✦ pass --cost-in PRICE --cost-out PRICE to add token cost and Margin
  ```
  Saves 3 vertical rows. Verbose 5-line format is still used when token pricing is also on (the Margin row needs structured layout). Helps fit the full output including the Cumulative section on shorter terminals.

## [0.3.4] — 2026-05-20

### Fixed
- **0.3.3's wheel was built before the all-healthy collapse change landed in the working tree** — the published artifact had the `prev_one_liner` packing helper but was missing the `if all_healthy and len(blocks) > 1` branch in `_render_imbalance_sections` and `_render_load_balance_sections`. Verified via `uvx --from "vllm-htop==0.3.3" python -c "..."`. Re-published with the same intended functionality.

## [0.3.3] — 2026-05-20

### Changed
- **All-healthy multi-group checks collapse to one line per section.** When every model group passes every check, the Load balance and Imbalance check sections each collapse to `▸ <section>  ✓ all N model groups OK (× M replicas total)` instead of one line per group. Adjacent single-line summaries no longer get a blank separator. Together this saves ~5 vertical rows for a 2-model 8-engine setup — the difference between fitting the full output on a 13" MacBook terminal and having the Cumulative section get auto-hidden. When any check fails the relevant block expands automatically, so no diagnostic info is lost. (Was originally intended for 0.3.2 but missed that release — included here.)

## [0.3.2] — 2026-05-19

### Added
- **`Req/s` and `Req%` columns** in the table view, between Wait and Swap. Shows each replica's request rate and its share of the deployment's total — load-balancer skew is now visible at a glance ("e2 is taking 60% of req/s when there are 4 replicas").
- **▸ Load balance section** — distinct from Imbalance check. Three checks, grouped by model:
  - **request share** — windowed `req/s` distribution; flagged when one replica's long-window share is >1.5× median for ≥10s ("sticky-looking skew"). Single-poll skew is reported but not alerted (avoids one-blip false positives).
  - **running req** — moved from Imbalance check; same median-based outlier detection.
  - **token share** — combined in+out tokens/s distribution; same median-based check.
- **Sticky skew detection** uses the snapshot-history rolling buffer to compute the request-share distribution over the last ~60s. Only fires when both the long-window share is skewed and the window covers ≥10s of data, so brief bursts don't trigger.

### Changed
- **Imbalance check now focuses on performance asymmetry only** (KV cache, slow-replica TTFT, slow-decode TPOT). The Running req check moved to the new Load balance section, since "is one replica getting more requests" is a load-distribution question, not a performance question.

## [0.3.1] — 2026-05-19

### Fixed
- **`process_start_time_seconds` no longer filtered out under internal DP**. The gauge and histogram branches of `parse_snapshot()` were using the raw `keep()` predicate instead of `keep_for(name, labels)`, so process-wide Prometheus metrics (which carry no `engine` label) were being dropped whenever an engine filter was active. This made `vllm_uptime_seconds()` see a value of 0 and report nonsensical lifetime compute cost (~$13M / 56 years). All three sample categories (counter / gauge / histogram) now use the name-aware keep.
- **Cost section no longer hidden along with Cumulative** when the terminal is short. Cost was previously nested inside the Cumulative `if` block; it's been hoisted out so it always renders when pricing is configured, regardless of whether Cumulative fits.

### Added
- **Terminal-height aware Cumulative section**. When the rendered output would exceed the terminal's `LINES`, the Cumulative table is auto-hidden with a one-line hint pointing to `--output json` for full data. Keeps the more important sections (table, imbalance, cost) visible on short terminals.
- **Missing-pricing hint** in the Cost section: when only one of `--cost-in/--cost-out` or `--gpu-cost-hour` is set, a dim line tells you what flag to add to also see the other pricing model and the Margin row.

## [0.3.0] — 2026-05-19

### Added
- **Lifetime compute cost** in the Cost section's Compute-based subsection — computed from `process_start_time_seconds` (the standard prometheus_client metric vLLM auto-exports) × `$/h × N`. Symmetric with the token-based Lifetime row, both reflect "since the vLLM process started." JSON output exposes it as `cost.compute_based.lifetime_total` plus `vllm_uptime_seconds`. Falls back gracefully when the metric isn't available.

### Changed
- **Imbalance check redesigned** for actionability. Three concrete improvements:
  - **Healthy case collapses to one line** (`✓ all N checks pass`) so the section disappears visually when nothing is wrong.
  - **Outlier replica is named** in the warning (`<model>.e3: 979ms is 5.2× median`) — no more cross-referencing the table above to identify which row is slow.
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
- **Row names now use the served model name** when it can be extracted from `/metrics` labels (`model_name`, `served_model_name`, or `model`). E.g. an `LLM + embedding` two-process deployment shows up as `<llm-model>.e0..e5` / `<embed-model>.e0..e1` instead of the previous `0.e0..0.e5` / `1.e0..1.e1`. Falls back to URL indices when (a) no model name is exposed, or (b) two URLs serve the same model (would create ambiguous duplicates).
- Legend at the bottom of the table view is now model-centric: `<model> ×6 engines @ http://localhost:8000`, much more compact than listing every engine name.
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
