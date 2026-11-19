#!/usr/bin/env python3
"""Phase 3 GPU Rerun Analysis & Gate Verdict Script (gblock1: Direct vs Ray HAProxy).

Parses the raw multi-trial records exported by the Task 3 hardened GPU matrix
runner (results grouped per (target_key, concurrency)) and regenerates:

1. A header computed from the trial records (total trials, levels, per-arm
   counts, model, batching config, co-location AZ).
2. Per (arm x concurrency-level) latency and goodput statistics: median [IQR]
   and mean +/- Student's t 95% CI (df = n - 1 per group) for p50/p95/p99,
   goodput mean, failure counts, and per-row threshold-breach flags.
3. A delta table (ray_ha - direct per level) for p50/p95/p99 with seeded
   bootstrap 95% CIs of the median difference.
4. The four Phase 3 gate verdicts:
   - Low-load overhead gate (C=1 and C=4, absolute <= 3.0 ms OR relative <= 10%).
   - Goodput retention gate (ray_ha >= 95% of direct mean throughput per level).
   - Batch scaling gate (ray arm: C=32 throughput >= 3x C=1 AND p95(C=32) <= 20 ms).
   - C=64 saturation gate (ray_ha p99(C=64) <= direct p99(C=64) + 3.0 ms).

Algorithm note: the bootstrap (10k resamples, seed 42, percentile CIs), the
t-CI helpers and the signed-delta formatting are re-implemented here from
results/analyze.py (the Phase 2 CPU analyzer). The duplication is deliberate —
the two analyzers intentionally stay self-contained so they can evolve
independently — but the algorithms themselves must remain equivalent.

Robustness: an arm missing a concurrency level renders `missing` at that cell
and the affected gates are SKIPPED; a missing arm or level never raises.
"""

import argparse
import json
import math
import os
import random
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# The two gblock1 support arms, in canonical report order.
SUPPORT_ARMS = ["direct", "ray_ha"]

# Fallback architecture display names when a trial record is missing one.
ARCH_NAMES = {
    "direct": "Direct Serving (GPU)",
    "ray_ha": "Ray Serve (HAProxy :8000)",
}

# Low-load overhead gate is evaluated at these concurrency levels.
GATE_LOW_LOAD_LEVELS = (1, 4)

# Gate thresholds (spec: Phase 3 GPU rerun gates).
LOW_LOAD_ABS_MS = 3.0
LOW_LOAD_REL_PCT = 10.0
GOODPUT_RETENTION_PCT = 95.0
BATCH_SCALING_FACTOR = 3.0
BATCH_SCALING_P95_MAX_MS = 20.0
SATURATION_P99_TOLERANCE_MS = 3.0

# Critical two-sided Student's t-value for df = n - 1 (alpha = 0.05).
T_DIST_95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
}


def fmt_signed(value: float, decimals: int = 3) -> str:
    """Format a signed delta with exactly one sign character (``+0.100`` / ``-0.033``)."""
    rounded = round(value, decimals) + 0.0  # normalise -0.0 to 0.0
    return f"{rounded:+.{decimals}f}"


def _ordered_unique(values: List[Any]) -> List[Any]:
    out: List[Any] = []
    for v in values:
        if v not in out:
            out.append(v)
    return out


def bootstrap_median_ci(vals: List[float], n_bootstraps: int = 10000, alpha: float = 0.05, seed: int = 42) -> Tuple[float, float]:
    """Compute empirical percentile bootstrap confidence interval for the sample median."""
    if not vals:
        return 0.0, 0.0
    rng = random.Random(seed)
    n = len(vals)
    boot_medians = []
    for _ in range(n_bootstraps):
        sample = [vals[rng.randrange(n)] for _ in range(n)]
        sample.sort()
        med = sample[n // 2] if n % 2 == 1 else (sample[n // 2 - 1] + sample[n // 2]) / 2.0
        boot_medians.append(med)
    boot_medians.sort()
    low_idx = int((alpha / 2.0) * n_bootstraps)
    high_idx = int((1.0 - alpha / 2.0) * n_bootstraps)
    return round(boot_medians[low_idx], 3), round(boot_medians[high_idx], 3)


def bootstrap_median_diff_ci(vals_a: List[float], vals_b: List[float], n_bootstraps: int = 10000, alpha: float = 0.05, seed: int = 42) -> Tuple[float, float]:
    """Compute empirical percentile bootstrap CI for the difference of medians (b - a)."""
    if not vals_a or not vals_b:
        return 0.0, 0.0
    rng = random.Random(seed)
    n_a = len(vals_a)
    n_b = len(vals_b)
    boot_diffs = []
    for _ in range(n_bootstraps):
        sample_a = sorted([vals_a[rng.randrange(n_a)] for _ in range(n_a)])
        sample_b = sorted([vals_b[rng.randrange(n_b)] for _ in range(n_b)])
        med_a = sample_a[n_a // 2] if n_a % 2 == 1 else (sample_a[n_a // 2 - 1] + sample_a[n_a // 2]) / 2.0
        med_b = sample_b[n_b // 2] if n_b % 2 == 1 else (sample_b[n_b // 2 - 1] + sample_b[n_b // 2]) / 2.0
        boot_diffs.append(med_b - med_a)
    boot_diffs.sort()
    low_idx = int((alpha / 2.0) * n_bootstraps)
    high_idx = int((1.0 - alpha / 2.0) * n_bootstraps)
    return round(boot_diffs[low_idx], 3), round(boot_diffs[high_idx], 3)


def compute_sample_stats(values: List[float]) -> Dict[str, Any]:
    """Compute parametric (mean t-CI) and non-parametric (median, IQR) statistics."""
    if not values:
        return {
            "median": 0.0, "iqr": 0.0, "min": 0.0, "max": 0.0,
            "mean": 0.0, "std": 0.0, "mean_ci95": 0.0,
        }

    n = len(values)
    vals = sorted(values)

    if n % 2 == 1:
        median = vals[n // 2]
    else:
        median = (vals[n // 2 - 1] + vals[n // 2]) / 2.0

    q1 = vals[n // 4]
    q3 = vals[(3 * n) // 4]
    iqr = q3 - q1

    mean = sum(vals) / n
    if n > 1:
        variance = sum((x - mean) ** 2 for x in vals) / (n - 1)
        std_dev = math.sqrt(variance)
        df = n - 1
        t_val = T_DIST_95.get(df, 1.96)
        mean_ci95 = t_val * (std_dev / math.sqrt(n))
    else:
        std_dev = 0.0
        mean_ci95 = 0.0

    return {
        "median": round(median, 3),
        "iqr": round(iqr, 3),
        "min": round(vals[0], 3),
        "max": round(vals[-1], 3),
        "mean": round(mean, 3),
        "std": round(std_dev, 3),
        "mean_ci95": round(mean_ci95, 3),
    }


def load_raw_data(data_path: str) -> Tuple[Dict[str, Any], Dict[Tuple[str, int], List[Dict[str, Any]]]]:
    """Load the raw trials JSON and group records per (target_key, concurrency)."""
    resolved_path = data_path
    if not os.path.exists(resolved_path):
        candidate = os.path.join(SCRIPT_DIR, data_path)
        if os.path.exists(candidate):
            resolved_path = candidate
        else:
            raise FileNotFoundError(f"Raw data file not found: {data_path} (checked {resolved_path} and {candidate})")

    with open(resolved_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    meta = payload.get("benchmark_metadata", {})
    trials = payload.get("trials", [])

    grouped: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for t in trials:
        key = (t.get("target_key", ""), t.get("concurrency"))
        grouped.setdefault(key, []).append(t)

    return meta, grouped


def _arm_arch(runs: List[Dict[str, Any]], target_key: str) -> str:
    for t in runs:
        if t.get("target_key") == target_key and t.get("architecture"):
            return str(t["architecture"])
    return ARCH_NAMES.get(target_key, target_key)


def _levels(grouped: Dict[Tuple[str, int], List[Dict[str, Any]]]) -> List[int]:
    levels = set()
    for _key, _arm, level, _runs in _iter_groups(grouped):
        levels.add(level)
    return sorted(levels)


def _iter_groups(grouped: Dict[Tuple[str, int], List[Dict[str, Any]]]):
    for key, runs in grouped.items():
        target_key = key[0] if isinstance(key, tuple) and len(key) >= 2 else ""
        try:
            level = int(key[1])
        except (TypeError, ValueError, IndexError):
            continue
        yield key, target_key, level, runs


def _runs_for(grouped: Dict[Any, List[Dict[str, Any]]], target_key: str, level: int) -> List[Dict[str, Any]]:
    for key, arm, lvl, runs in _iter_groups(grouped):
        if arm == target_key and lvl == level:
            return runs
    return []


def _metric_stats(runs: List[Dict[str, Any]], metric_key: str) -> Dict[str, Any]:
    return compute_sample_stats([float(t.get(metric_key, 0.0)) for t in runs])


def gate_low_load(rows: Dict[Any, List[Dict[str, Any]]]) -> str:
    """Low-load overhead gate: at C=1 and C=4, PASS if median Δp95 <= 3.0 ms (absolute)
    OR Δp95 <= 10% of the direct p95 (relative).

    Returns ``"PASS (absolute)"`` / ``"PASS (relative)"`` / ``"FAIL"``. If the two
    evaluated levels disagree, the worse (more severe) verdict wins, with the
    failing level stated. Levels missing an arm are skipped. If no level can be
    evaluated, the gate is reported as SKIPPED (never an exception).
    """
    severity = {"PASS (absolute)": 0, "PASS (relative)": 1, "FAIL": 2}
    evaluated: List[Dict[str, Any]] = []
    for level in GATE_LOW_LOAD_LEVELS:
        d_runs = _runs_for(rows, "direct", level)
        r_runs = _runs_for(rows, "ray_ha", level)
        if not d_runs or not r_runs:
            continue
        d_med = _metric_stats(d_runs, "p95_ms")["median"]
        r_med = _metric_stats(r_runs, "p95_ms")["median"]
        delta = r_med - d_med
        rel_pct = (delta / d_med * 100.0) if d_med != 0 else (0.0 if delta == 0 else float("inf"))
        if delta <= LOW_LOAD_ABS_MS:
            verdict = "PASS (absolute)"
        elif rel_pct <= LOW_LOAD_REL_PCT:
            verdict = "PASS (relative)"
        else:
            verdict = "FAIL"
        evaluated.append({"level": level, "verdict": verdict, "d_med": d_med,
                          "r_med": r_med, "delta": delta, "rel_pct": rel_pct})

    if not evaluated:
        return "SKIPPED (no C=1/C=4 level with complete direct + ray_ha data)"

    evaluated.sort(key=lambda e: (severity[e["verdict"]], e["level"]))
    worst = evaluated[-1]
    best = evaluated[0]
    if worst["verdict"] == best["verdict"]:
        return worst["verdict"]
    if worst["verdict"] != "FAIL":
        # Disagreement between two PASSING labels (absolute vs relative): every
        # level passed per spec, so return the monotone-worst passing verdict.
        # Both levels' detail rows still render in the Gate 1 table.
        return worst["verdict"]
    # A level genuinely failed: report the worse verdict with the failing level's numbers.
    return (f"FAIL (C={worst['level']}: Δp95={worst['delta']:.3f} ms = "
            f"{fmt_signed(worst['rel_pct'], 1)}% exceeds both gates)")


def _render_low_load_gate(grouped: Dict[Any, List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    verdict = gate_low_load(grouped)
    lines.append(f"**Verdict: {verdict}**")
    lines.append("")
    lines.append("| Level | Direct p95 (median, ms) | ray_ha p95 (median, ms) | Δp95 (ms) | Δp95 (relative) | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |")
    lines.append("|---|---|---|---|---|---|---|")
    for level in GATE_LOW_LOAD_LEVELS:
        d_runs = _runs_for(grouped, "direct", level)
        r_runs = _runs_for(grouped, "ray_ha", level)
        if not d_runs or not r_runs:
            lines.append(f"| C={level} | {_missing_cell(d_runs)} | {_missing_cell(r_runs)} | — | — | SKIPPED | SKIPPED |")
            continue
        d_med = _metric_stats(d_runs, "p95_ms")["median"]
        r_med = _metric_stats(r_runs, "p95_ms")["median"]
        delta = r_med - d_med
        rel_pct = (delta / d_med * 100.0) if d_med != 0 else (0.0 if delta == 0 else float("inf"))
        abs_pass = "PASS" if delta <= LOW_LOAD_ABS_MS else "FAIL"
        rel_pass = "PASS" if rel_pct <= LOW_LOAD_REL_PCT else "FAIL"
        lines.append(
            f"| C={level} | {d_med:.3f} | {r_med:.3f} | {fmt_signed(delta)} | "
            f"{fmt_signed(rel_pct, 1)}% | {abs_pass} | {rel_pass} |"
        )
    return lines


def _missing_cell(runs: Optional[List[Dict[str, Any]]]) -> str:
    return f"missing ({len(runs)} trials)" if runs else "missing"


def _render_goodput_gate(grouped: Dict[Any, List[Dict[str, Any]]], levels: List[int]) -> List[str]:
    lines: List[str] = []
    per_level: List[Tuple[int, str, Optional[float]]] = []
    any_fail = False
    any_evaluated = False
    for level in levels:
        d_runs = _runs_for(grouped, "direct", level)
        r_runs = _runs_for(grouped, "ray_ha", level)
        if not d_runs or not r_runs:
            per_level.append((level, "SKIPPED (missing arm data)", None))
            continue
        d_mean = _metric_stats(d_runs, "throughput_rps")["mean"]
        r_mean = _metric_stats(r_runs, "throughput_rps")["mean"]
        pct = (r_mean / d_mean * 100.0) if d_mean != 0 else (100.0 if r_mean == 0 else 0.0)
        verdict = "PASS" if pct >= GOODPUT_RETENTION_PCT else "FAIL"
        any_fail = any_fail or verdict == "FAIL"
        any_evaluated = True
        per_level.append((level, f"{verdict} (ray_ha {r_mean:.2f} req/s = {pct:.2f}% of direct {d_mean:.2f} req/s; gate >= 95%)", pct))
    lines.append("| Level | Verdict |")
    lines.append("|---|---|")
    for level, note, _pct in per_level:
        lines.append(f"| C={level} | {note} |")
    if not any_evaluated:
        overall = "SKIPPED (no level with complete direct + ray_ha data)"
    elif any_fail:
        overall = "FAIL (at least one level below 95% goodput retention)"
    else:
        overall = "PASS (ray_ha retains >= 95% of direct goodput at every level)"
    lines.append("")
    lines.append(f"**Overall Verdict: {overall}**")
    return lines


def _render_batch_scaling_gate(grouped: Dict[Any, List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    base_runs = _runs_for(grouped, "ray_ha", 1)
    scaled_runs = _runs_for(grouped, "ray_ha", 32)
    if not base_runs or not scaled_runs:
        lines.append("_SKIPPED (missing ray_ha data at C=1 and/or C=32)._")
        return lines
    base_tp = _metric_stats(base_runs, "throughput_rps")["mean"]
    scaled_tp = _metric_stats(scaled_runs, "throughput_rps")["mean"]
    scaled_p95 = _metric_stats(scaled_runs, "p95_ms")["median"]
    tp_ok = scaled_tp >= BATCH_SCALING_FACTOR * base_tp
    p95_ok = scaled_p95 <= BATCH_SCALING_P95_MAX_MS
    verdict = "PASS" if (tp_ok and p95_ok) else "FAIL"
    lines.append(
        f"- ray_ha mean throughput C=1: **{base_tp:.2f} req/s**, C=32: **{scaled_tp:.2f} req/s** "
        f"(x{(scaled_tp / base_tp if base_tp else 0.0):.2f}; gate: >= x{BATCH_SCALING_FACTOR:.0f}) → {'PASS' if tp_ok else 'FAIL'}"
    )
    lines.append(
        f"- ray_ha p95 median at C=32: **{scaled_p95:.3f} ms** (gate: <= {BATCH_SCALING_P95_MAX_MS:.1f} ms) → {'PASS' if p95_ok else 'FAIL'}"
    )
    lines.append(f"**Verdict: {verdict}**")
    return lines


def _render_saturation_gate(grouped: Dict[Any, List[Dict[str, Any]]]) -> List[str]:
    lines: List[str] = []
    d_runs = _runs_for(grouped, "direct", 64)
    r_runs = _runs_for(grouped, "ray_ha", 64)
    if not d_runs or not r_runs:
        lines.append("_SKIPPED (missing direct and/or ray_ha data at C=64); both p99s printed below when available._")
    d_p99 = _metric_stats(d_runs, "p99_ms")["median"] if d_runs else None
    r_p99 = _metric_stats(r_runs, "p99_ms")["median"] if r_runs else None
    lines.append(f"- direct p99 median at C=64: **{d_p99:.3f} ms**" if d_p99 is not None else "- direct p99 median at C=64: _missing_")
    lines.append(f"- ray_ha p99 median at C=64: **{r_p99:.3f} ms**" if r_p99 is not None else "- ray_ha p99 median at C=64: _missing_")
    if d_p99 is not None and r_p99 is not None:
        delta = r_p99 - d_p99
        verdict = "PASS" if delta <= SATURATION_P99_TOLERANCE_MS else "FAIL"
        lines.append(
            f"- Δp99 (ray_ha − direct) at C=64: **{fmt_signed(delta)} ms** "
            f"(gate: <= +{SATURATION_P99_TOLERANCE_MS:.1f} ms)"
        )
        lines.append(f"**Verdict: {verdict}**")
    return lines


def generate_gpu_tables(meta: Dict[str, Any], grouped: Dict[Tuple[str, int], List[Dict[str, Any]]]) -> str:
    """Render the full Phase 3 GPU markdown summary (header, per-arm tables,
    delta table, gate verdicts) from trials grouped per (target_key, concurrency)."""
    lines: List[str] = []
    all_trials = [t for runs in grouped.values() for t in runs]
    levels = _levels(grouped)

    arch_by_arm: Dict[str, str] = {}
    arm_counts: Dict[str, int] = {arm: 0 for arm in SUPPORT_ARMS}
    for key, arm, level, runs in _iter_groups(grouped):
        if runs and arm in arm_counts:
            arm_counts[arm] += len(runs)
            arch_by_arm[arm] = runs[0].get("architecture") or arch_by_arm.get(arm) or ARCH_NAMES.get(arm, arm)

    # 0. Header (counts always computed from the trial records).
    lines.append("# Phase 3 GPU Rerun — Statistical Summary (gblock1)\n")
    lines.append(f"- **Model:** {meta.get('model', 'sentence-transformers/all-MiniLM-L6-v2')}")
    lines.append(
        f"- **Batching:** max_batch_size={meta.get('max_batch_size', 32)}, "
        f"batch_wait_timeout_s={meta.get('batch_wait_timeout_s', 0.005)}, "
        f"max_concurrent_batches={meta.get('max_concurrent_batches', 1)}"
    )
    lines.append(f"- **Co-Location:** {meta.get('co_location', 'Single Availability Zone (us-west-2a), zero cross-AZ transit')}")
    lines.append(f"- **Total Trials (reconciled):** {len(all_trials)}")
    lines.append(f"- **Concurrency Levels:** {', '.join(f'C={l}' for l in levels) if levels else '_none_'}")
    counts_note = ", ".join(f"{arch_by_arm.get(arm, ARCH_NAMES.get(arm, arm))}: {arm_counts[arm]}" for arm in SUPPORT_ARMS)
    lines.append(f"- **Trials per Arm:** {counts_note}")
    per_level_counts = []
    for arm in SUPPORT_ARMS:
        n_values = {len(_runs_for(grouped, arm, level)) for level in levels}
        label = ", ".join(str(n) for n in sorted(n_values))
        per_level_counts.append(f"{arch_by_arm.get(arm, ARCH_NAMES.get(arm, arm))}: {label or '0'}")
    lines.append(f"- **Trials per (Arm x Level):** {'; '.join(per_level_counts)}")
    lines.append("")

    # 1. Per-arm x level latency & goodput table.
    lines.append("## 1. Per-Arm × Level Latency & Goodput (Median [IQR] / Mean ± 95% CI)\n")
    lines.append("Median [IQR] is non-parametric; mean ± 95% CI uses Student's t with df = n − 1 per (arm, level) group.\n")
    lines.append("| Arm | Level | n | p50 (ms) | p95 (ms) | p99 (ms) | Goodput mean (req/s) | Failed | Flags |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for arm in SUPPORT_ARMS:
        arch = arch_by_arm.get(arm, ARCH_NAMES.get(arm, arm))
        for level in levels:
            runs = _runs_for(grouped, arm, level)
            if not runs:
                lines.append(f"| **{arch}** | C={level} | 0 | missing | missing | missing | missing | — | — |")
                continue
            n = len(runs)
            cells = []
            for metric in ("p50_ms", "p95_ms", "p99_ms"):
                st = _metric_stats(runs, metric)
                cells.append(f"{st['median']:.3f} [{st['iqr']:.3f}] / {st['mean']:.3f} ± {st['mean_ci95']:.3f}")
            tp = _metric_stats(runs, "throughput_rps")
            failed = sum(int(t.get("failed_requests", 0)) for t in runs)
            breached = sum(1 for t in runs if t.get("threshold_breached"))
            flags = []
            if failed:
                flags.append(f"{failed} failed requests")
            if breached:
                flags.append(f"**THRESHOLD BREACHED on {breached} trial(s)**")
            lines.append(
                f"| **{arch}** | C={level} | {n} | {cells[0]} | {cells[1]} | {cells[2]} | "
                f"{tp['mean']:.2f} ± {tp['mean_ci95']:.2f} | {failed} | {'; '.join(flags) if flags else '—'} |"
            )
    lines.append("")

    # 2. Delta table: ray_ha - direct per level, bootstrap 95% CI of the median difference.
    lines.append("## 2. Delta Table (ray_ha − Direct per Level, Bootstrap 95% CI of Median Difference)\n")
    lines.append("Deltas are median differences; CIs are seeded percentile bootstrap (10,000 resamples, seed 42), same algorithm as the Phase 2 analyzer.\n")
    lines.append("| Level | Δp50 (ms) | Δp95 (ms) | Δp99 (ms) |")
    lines.append("|---|---|---|---|")
    for level in levels:
        d_runs = _runs_for(grouped, "direct", level)
        r_runs = _runs_for(grouped, "ray_ha", level)
        if not d_runs or not r_runs:
            lines.append(f"| C={level} | missing | missing | missing |")
            continue
        cells = []
        for metric in ("p50_ms", "p95_ms", "p99_ms"):
            d_vals = [float(t.get(metric, 0.0)) for t in d_runs]
            r_vals = [float(t.get(metric, 0.0)) for t in r_runs]
            d_med = compute_sample_stats(d_vals)["median"]
            r_med = compute_sample_stats(r_vals)["median"]
            b_low, b_high = bootstrap_median_diff_ci(d_vals, r_vals)
            cells.append(f"{fmt_signed(r_med - d_med)} ms [{fmt_signed(b_low)}, {fmt_signed(b_high)}]")
        lines.append(f"| C={level} | {cells[0]} | {cells[1]} | {cells[2]} |")
    lines.append("")

    # 3. Gate verdicts.
    lines.append("## 3. Gate Verdicts\n")
    lines.append("### Gate 1: Low-Load Overhead (C=1 and C=4, Δp95 <= 3.0 ms OR <= 10%)\n")
    lines.extend(_render_low_load_gate(grouped))

    lines.append("\n### Gate 2: Goodput Retention (ray_ha >= 95% of direct mean throughput per level)\n")
    lines.extend(_render_goodput_gate(grouped, levels))

    lines.append("\n### Gate 3: Batch Scaling (ray arm, C=32 vs C=1)\n")
    lines.append(
        f"Throughput(C=32) >= {BATCH_SCALING_FACTOR:.0f} × throughput(C=1) **and** "
        f"p95(C=32) <= {BATCH_SCALING_P95_MAX_MS:.1f} ms.\n"
    )
    lines.extend(_render_batch_scaling_gate(grouped))

    lines.append("\n### Gate 4: C=64 Saturation (ray_ha p99 <= direct p99 + 3.0 ms)\n")
    lines.extend(_render_saturation_gate(grouped))

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze the Phase 3 GPU rerun trials and render the gate-verdict summary."
    )
    # Both paths are required so a bare invocation can never fall back into
    # (and overwrite) a historical dataset's summary.
    parser.add_argument("--data", required=True, help="Path to the raw_trials.json to analyze")
    parser.add_argument("--output", required=True, help="Path of the markdown summary to write")
    args = parser.parse_args()

    meta, grouped = load_raw_data(args.data)
    report_md = generate_gpu_tables(meta, grouped)

    out_path = args.output
    out_dir = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md + "\n")

    print(report_md)
    print(f"\n[OK] Summary report successfully written to {out_path}")


if __name__ == "__main__":
    main()
