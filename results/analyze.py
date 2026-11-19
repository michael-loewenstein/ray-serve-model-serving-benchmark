#!/usr/bin/env python3
"""Benchmark Results Analysis & Statistical Reconstruction Script.

This script parses the raw multi-trial benchmark measurements and regenerates:
1. Aggregate goodput and latency metrics across trials.
2. Parametric Student's t 95% Confidence Intervals for the trial-level MEAN (df = n - 1 per arm).
3. Non-parametric statistics: Medians, Interquartile Range (IQR), and Bootstrap 95% Confidence Intervals for the MEDIAN.
4. Server compute vs. network transit decomposition.
5. Hypothesis H1 evaluation (Absolute <= 3.0 ms vs. Relative <= 10% gates).
"""

import argparse
import json
import math
import os
import random
from typing import Any, Dict, List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Critical two-sided Student's t-value for df = n - 1 (alpha = 0.05)
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
    """Compute comprehensive parametric (mean CI) and non-parametric (median, IQR, bootstrap CI) statistics."""
    if not values:
        return {
            "median": 0.0, "iqr": 0.0, "min": 0.0, "max": 0.0,
            "boot_ci95": (0.0, 0.0),
            "mean": 0.0, "std": 0.0, "mean_ci95": 0.0,
        }

    n = len(values)
    vals = sorted(values)

    # Median
    if n % 2 == 1:
        median = vals[n // 2]
    else:
        median = (vals[n // 2 - 1] + vals[n // 2]) / 2.0

    # Quartiles & IQR
    q1 = vals[n // 4]
    q3 = vals[(3 * n) // 4]
    iqr = q3 - q1

    # Bootstrap 95% CI for the median
    boot_low, boot_high = bootstrap_median_ci(vals)

    # Mean & Sample Variance
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
        "boot_ci95": (boot_low, boot_high),
        "mean": round(mean, 3),
        "std": round(std_dev, 3),
        "mean_ci95": round(mean_ci95, 3),
    }


def load_raw_data(data_path: str) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]:
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

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for t in trials:
        arch = t["architecture"]
        grouped.setdefault(arch, []).append(t)

    return meta, grouped


def baseline_for(arch: str, runs: List[Dict[str, Any]], grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Return the Direct-baseline trials belonging to the same block as ``runs``.

    Trials without an explicit ``block`` field default to ``"legacy"`` (the
    October dataset predates block-aware runs), so legacy Ray arms are still
    compared against the legacy Direct trials.
    """
    if not runs:
        return []
    block_counts: Dict[str, int] = {}
    for t in runs:
        block = t.get("block", "legacy")
        block_counts[block] = block_counts.get(block, 0) + 1
    # Modal block; ties resolve to the first block encountered (dicts preserve order).
    block = max(block_counts.items(), key=lambda kv: kv[1])[0]

    baseline: List[Dict[str, Any]] = []
    for other_runs in grouped.values():
        for t in other_runs:
            if t.get("target_key") == "direct" and t.get("block", "legacy") == block:
                baseline.append(t)
    return baseline


def _arm_runs(runs: List[Dict[str, Any]], target_key: str) -> List[Dict[str, Any]]:
    return [t for t in runs if t.get("target_key", "") == target_key]


def generate_tables(meta: Dict[str, Any], grouped: Dict[str, List[Dict[str, Any]]]) -> str:
    lines = []
    lines.append("# Statistical Benchmark Summary Report\n")
    lines.append(f"- **Workload:** {meta.get('workload', 'XGBoost CPU Tabular')}")
    lines.append(f"- **Hardware:** {meta.get('hardware', 'c6i.2xlarge')}")
    lines.append(f"- **Co-Location:** {meta.get('co_location', 'Single Availability Zone')}")
    # Counts are always computed from the trial records themselves; benchmark
    # metadata may carry stale or template values.
    all_trials = [t for runs in grouped.values() for t in runs]
    total_requests = sum(t.get("total_requests", 0) for t in all_trials)
    has_blocks = any("block" in t for t in all_trials)
    blocks = _ordered_unique([t.get("block", "legacy") for t in all_trials])
    block_note = f" in {len(blocks)} blocks" if has_blocks else ""
    lines.append(
        f"- **Total Requests Reconciled:** {total_requests} across {len(all_trials)} trials{block_note}"
    )
    arch_summary = ", ".join(f"{arch}: {len(runs)}" for arch, runs in grouped.items())
    lines.append(f"- **Trials per Architecture:** {arch_summary}")
    if has_blocks:
        block_totals = ", ".join(
            f"{b}: {sum(1 for t in all_trials if t.get('block', 'legacy') == b)}" for b in blocks
        )
        lines.append(f"- **Trials per Block:** {block_totals}")
        runs_per_arm = []
        for b in blocks:
            counts: Dict[str, int] = {}
            for t in all_trials:
                if t.get("block", "legacy") == b:
                    key = t.get("target_key", t.get("architecture", ""))
                    counts[key] = counts.get(key, 0) + 1
            distinct = sorted(set(counts.values()))
            label = str(distinct[0]) if len(distinct) == 1 else f"{distinct[0]}-{distinct[-1]}"
            runs_per_arm.append(f"{b}: {label}")
        lines.append(f"- **Runs per Arm per Block:** {', '.join(runs_per_arm)}")
    lines.append("")

    # 1. Hypothesis H1 Overhead Gate Table (block-aware baselines)
    lines.append("## 1. Formal Evaluation of Hypothesis H1 (Serving Overhead Gate)\n")
    lines.append(
        "Hypothesis H1 evaluates whether Ray Serve satisfies the absolute gate (overhead <= 3.0 ms) "
        "and/or the relative gate (overhead <= 10.0% over Direct Serving baseline).\n"
    )
    lines.append(
        "Each Ray arm is compared against the Direct Serving trials of the **same block**. "
        "Local-control arms whose block has no Direct baseline are skipped here and "
        "reported in the HAProxy-only control table below.\n"
    )
    lines.append("| Arm | Block | Metric | Direct-serving baseline (median) | Arm (median) | Absolute delta [Bootstrap 95% CI] | Relative delta | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |")
    lines.append("|---|---|---|---|---|---|---|---|---|")

    metrics_eval = ["p50", "p90", "p95", "p99"]
    arms_emitted = 0
    for arch, runs in grouped.items():
        seen_keys = []
        for t in runs:
            key = t.get("target_key", "")
            if key not in seen_keys:
                seen_keys.append(key)
        for key in seen_keys:
            if key == "direct":
                continue
            key_runs = _arm_runs(runs, key)
            if not key_runs:
                continue
            baseline_runs = baseline_for(arch, key_runs, grouped)
            if not baseline_runs:
                # No Direct baseline in this arm's block (local control).
                continue
            arm_block = key_runs[0].get("block", "legacy")
            for label in metrics_eval:
                metric_key = f"{label}_ms"
                d_val = compute_sample_stats([t[metric_key] for t in baseline_runs])["median"]
                h_val = compute_sample_stats([t[metric_key] for t in key_runs])["median"]
                abs_diff = h_val - d_val
                rel_pct = (abs_diff / d_val) * 100.0 if d_val != 0 else 0.0
                abs_pass = "PASS" if abs_diff <= 3.0 else "FAIL"
                rel_pass = "PASS" if rel_pct <= 10.0 else "FAIL"
                d_list = [t[metric_key] for t in baseline_runs]
                h_list = [t[metric_key] for t in key_runs]
                b_low, b_high = bootstrap_median_diff_ci(d_list, h_list)
                lines.append(
                    f"| **{arch}** | {arm_block} | **{label}** | **{d_val:.3f} ms** | **{h_val:.3f} ms** | "
                    f"**{fmt_signed(abs_diff)} ms** [{fmt_signed(b_low)}, {fmt_signed(b_high)}] | "
                    f"**{fmt_signed(rel_pct, 1)}%** | "
                    f"**{abs_pass}** | **{rel_pass}** |"
                )
            arms_emitted += 1

    if arms_emitted == 0:
        lines.append("| _No Ray arm with a same-block Direct baseline found._ | | | | | | | | |")

    # 2. HAProxy-only control table (local control, no Direct baseline in block)
    lines.append("\n## 2. HAProxy-only control (HAProxy local vs. Direct-Ingress local)\n")
    lines.append(
        "Isolates the HAProxy routing overhead within the local-control block: "
        "`ray_haproxy_local` minus `ray_direct_local`, both served from the same worker node.\n"
    )
    haproxy_local_runs: List[Dict[str, Any]] = []
    direct_local_runs: List[Dict[str, Any]] = []
    for runs in grouped.values():
        for t in runs:
            key = t.get("target_key", "")
            if key == "ray_haproxy_local":
                haproxy_local_runs.append(t)
            elif key == "ray_direct_local":
                direct_local_runs.append(t)

    if haproxy_local_runs and direct_local_runs:
        lines.append("| Metric | Direct Ingress local control (median) | HAProxy local control (median) | Absolute delta [Bootstrap 95% CI] |")
        lines.append("|---|---|---|---|")
        for label in metrics_eval:
            metric_key = f"{label}_ms"
            dl_val = compute_sample_stats([t[metric_key] for t in direct_local_runs])["median"]
            hl_val = compute_sample_stats([t[metric_key] for t in haproxy_local_runs])["median"]
            abs_diff = hl_val - dl_val
            dl_list = [t[metric_key] for t in direct_local_runs]
            hl_list = [t[metric_key] for t in haproxy_local_runs]
            b_low, b_high = bootstrap_median_diff_ci(dl_list, hl_list)
            lines.append(
                f"| **{label}** | **{dl_val:.3f} ms** | **{hl_val:.3f} ms** | "
                f"**{fmt_signed(abs_diff)} ms** [{fmt_signed(b_low)}, {fmt_signed(b_high)}] |"
            )
    else:
        lines.append("_No complete local-control pair (`ray_haproxy_local` vs `ray_direct_local`) found in this dataset._")

    # 3. Main Aggregate Summary Table with Separate Median and Mean CI
    lines.append("\n## 3. Aggregate Latency & Goodput Across Independent Trials\n")
    n_summary = []
    for arch, runs in grouped.items():
        n = len(runs)
        t_val = T_DIST_95.get(n - 1, 1.96) if n > 1 else None
        t_label = f"t = {t_val:.3f}" if t_val is not None else "no CI"
        n_summary.append(f"{arch} n={n} ({t_label})")
    lines.append(
        "Statistical Note: n is the number of independent trials per arm; we report both:\n"
        "- Non-parametric: **Median [IQR]** and [Min, Max]\n"
        "- Parametric: **Mean ± 95% CI** using Student's t distribution with df = n − 1 per arm\n"
        f"- Per-arm n: {'; '.join(n_summary)}\n"
        "- Each row pools an architecture's trials across every block it ran in; "
        "the H1 deltas in Section 1 use same-block Direct baselines only.\n"
    )
    lines.append("| Architecture | Goodput (req/s) | p50 Median [IQR] (ms) | p50 Mean ± 95% CI (ms) | p90 Median (ms) | p95 Median [IQR] (ms) | p95 Mean ± 95% CI (ms) | p99 Median [IQR] (ms) | p99 Mean ± 95% CI (ms) | Failure Rate |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")

    for arch, runs in grouped.items():
        goodput = compute_sample_stats([t["goodput_rps"] for t in runs])
        p50 = compute_sample_stats([t["p50_ms"] for t in runs])
        p90 = compute_sample_stats([t["p90_ms"] for t in runs])
        p95 = compute_sample_stats([t["p95_ms"] for t in runs])
        p99 = compute_sample_stats([t["p99_ms"] for t in runs])
        total_reqs = sum(t["total_requests"] for t in runs)
        failed_reqs = sum(t["failed_requests"] for t in runs)
        fail_rate = (failed_reqs / total_reqs * 100.0) if total_reqs > 0 else 0.0

        lines.append(
            f"| **{arch}** | {goodput['median']:.2f} (Mean: {goodput['mean']:.2f} ± {goodput['mean_ci95']:.3f}) | "
            f"**{p50['median']:.3f}** [{p50['iqr']:.3f}] | {p50['mean']:.3f} ± {p50['mean_ci95']:.3f} | "
            f"{p90['median']:.3f} | "
            f"**{p95['median']:.3f}** [{p95['iqr']:.3f}] | {p95['mean']:.3f} ± {p95['mean_ci95']:.3f} | "
            f"**{p99['median']:.3f}** [{p99['iqr']:.3f}] | {p99['mean']:.3f} ± {p99['mean_ci95']:.3f} | "
            f"{fail_rate:.2f}% ({failed_reqs}/{total_reqs}) |"
        )

    # 4. Server Compute vs. Network Transit Breakdown
    lines.append("\n## 4. Server Compute vs. Network Transit Breakdown (Median ms)\n")
    lines.append("| Architecture | Pure Booster Predict (p50) | Server Total incl. JSON (p50) | Network & Proxy Transit Overhead (p50) |")
    lines.append("|---|---|---|---|")

    for arch, runs in grouped.items():
        pure_p50 = compute_sample_stats([t["pure_booster_predict_p50_ms"] for t in runs])["median"]
        srv_p50 = compute_sample_stats([t["server_total_p50_ms"] for t in runs])["median"]
        net_p50 = compute_sample_stats([t["network_transit_p50_ms"] for t in runs])["median"]
        lines.append(f"| **{arch}** | {pure_p50:.3f} ms | {srv_p50:.3f} ms | {net_p50:.3f} ms |")

    # 5. Individual Trial Measurements Table
    lines.append(f"\n## 5. Complete Individual Trial Measurements ({len(all_trials)} Trials)\n")
    lines.append("| Architecture | Block | Trial # | Reqs | Goodput (req/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")

    for arch, runs in grouped.items():
        for r in runs:
            lines.append(
                f"| {arch} | {r.get('block', 'legacy')} | {r['trial_number']} | {r['total_requests']} | {r['goodput_rps']:.2f} | "
                f"{r['p50_ms']:.3f} | {r['p90_ms']:.3f} | {r['p95_ms']:.3f} | {r['p99_ms']:.3f} | {r['max_ms']:.2f} |"
            )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyze benchmark results and regenerate statistical tables.")
    # Both paths are required so a bare invocation can never overwrite a
    # historical dataset's summary (e.g. the October results/summary_report.md).
    parser.add_argument("--data", required=True, help="Path to the raw_trials.json to analyze")
    parser.add_argument("--output", required=True, help="Path of the markdown summary to write")
    args = parser.parse_args()

    meta, grouped = load_raw_data(args.data)
    report_md = generate_tables(meta, grouped)

    out_path = args.output
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md + "\n")

    print(report_md)
    print(f"\n[OK] Summary report successfully written to {out_path}")


if __name__ == "__main__":
    main()
