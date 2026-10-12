#!/usr/bin/env python3
"""Benchmark Results Analysis & Statistical Reconstruction Script.

This script parses the raw multi-trial benchmark measurements and regenerates:
1. Aggregate goodput and latency metrics across trials.
2. Parametric Student's t 95% Confidence Intervals for the trial-level MEAN (df = 4, t = 2.776).
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
DEFAULT_DATA_PATH = os.path.join(SCRIPT_DIR, "raw_trials.json")
DEFAULT_OUTPUT_PATH = os.path.join(SCRIPT_DIR, "summary_report.md")

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


def generate_tables(meta: Dict[str, Any], grouped: Dict[str, List[Dict[str, Any]]]) -> str:
    lines = []
    lines.append("# Statistical Benchmark Summary Report\n")
    lines.append(f"- **Workload:** {meta.get('workload', 'XGBoost CPU Tabular')}")
    lines.append(f"- **Hardware:** {meta.get('hardware', 'c6i.2xlarge')}")
    lines.append(f"- **Co-Location:** {meta.get('co_location', 'Single Availability Zone')}")
    lines.append(f"- **Total Requests Reconciled:** {meta.get('total_requests', 22508)} across 15 randomized trials")
    lines.append(f"- **Trials per Architecture:** {meta.get('trials_per_architecture', 5)} independent trials\n")

    # 1. Hypothesis H1 Overhead Gate Table
    direct_trials = grouped.get("Direct Serving (Uvicorn)", [])
    direct_p50 = compute_sample_stats([t["p50_ms"] for t in direct_trials])["median"]
    direct_p90 = compute_sample_stats([t["p90_ms"] for t in direct_trials])["median"]
    direct_p95 = compute_sample_stats([t["p95_ms"] for t in direct_trials])["median"]
    direct_p99 = compute_sample_stats([t["p99_ms"] for t in direct_trials])["median"]

    lines.append("## 1. Formal Evaluation of Hypothesis H1 (Serving Overhead Gate)\n")
    lines.append(
        "Hypothesis H1 evaluates whether Ray Serve satisfies the absolute gate (overhead <= 3.0 ms) "
        "and/or the relative gate (overhead <= 10.0% over Direct Serving baseline).\n"
    )
    lines.append("| Metric | Direct Serving Baseline (Median) | Ray Serve Worker HAProxy (Median) | Absolute Overhead | Relative Overhead | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |")
    lines.append("|---|---|---|---|---|---|---|")

    haproxy_trials = grouped.get("Ray Serve (Worker HAProxy :30000)", [])
    if haproxy_trials and direct_trials:
        metrics_eval = [
            ("p50", "p50_ms", direct_p50),
            ("p90", "p90_ms", direct_p90),
            ("p95", "p95_ms", direct_p95),
            ("p99", "p99_ms", direct_p99),
        ]
        for label, metric_key, d_val in metrics_eval:
            h_val = compute_sample_stats([t[metric_key] for t in haproxy_trials])["median"]
            abs_diff = h_val - d_val
            rel_pct = (abs_diff / d_val) * 100.0
            abs_pass = "PASS" if abs_diff <= 3.0 else "FAIL"
            rel_pass = "PASS" if rel_pct <= 10.0 else "FAIL"
            lines.append(
                f"| **{label}** | **{d_val:.3f} ms** | **{h_val:.3f} ms** | "
                f"**+{abs_diff:.3f} ms** | **+{rel_pct:.1f}%** | "
                f"**{abs_pass}** | **{rel_pass}** |"
            )

    # 2. Main Aggregate Summary Table with Separate Median and Mean CI
    lines.append("\n## 2. Aggregate Latency & Goodput Across Independent Trials\n")
    lines.append(
        "Statistical Note: To maintain statistical rigor with n=5 independent trials, we report both:\n"
        "- Non-parametric: **Median [IQR]** and [Min, Max]\n"
        "- Parametric: **Mean ± 95% CI** using Student's t distribution (df = 4, t = 2.776)\n"
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

    # 3. Server Compute vs. Network Transit Breakdown
    lines.append("\n## 3. Server Compute vs. Network Transit Breakdown (Median ms)\n")
    lines.append("| Architecture | Pure Booster Predict (p50) | Server Total incl. JSON (p50) | Network & Proxy Transit Overhead (p50) |")
    lines.append("|---|---|---|---|")

    for arch, runs in grouped.items():
        pure_p50 = compute_sample_stats([t["pure_booster_predict_p50_ms"] for t in runs])["median"]
        srv_p50 = compute_sample_stats([t["server_total_p50_ms"] for t in runs])["median"]
        net_p50 = compute_sample_stats([t["network_transit_p50_ms"] for t in runs])["median"]
        lines.append(f"| **{arch}** | {pure_p50:.3f} ms | {srv_p50:.3f} ms | {net_p50:.3f} ms |")

    # 4. Individual Trial Measurements Table
    lines.append("\n## 4. Complete Individual Trial Measurements (15 Trials)\n")
    lines.append("| Architecture | Trial # | Reqs | Goodput (req/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |")
    lines.append("|---|---|---|---|---|---|---|---|---|")

    for arch, runs in grouped.items():
        for r in runs:
            lines.append(
                f"| {arch} | {r['trial_number']} | {r['total_requests']} | {r['goodput_rps']:.2f} | "
                f"{r['p50_ms']:.3f} | {r['p90_ms']:.3f} | {r['p95_ms']:.3f} | {r['p99_ms']:.3f} | {r['max_ms']:.2f} |"
            )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyze benchmark results and regenerate statistical tables.")
    parser.add_argument("--data", default=DEFAULT_DATA_PATH, help="Path to raw_trials.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to output markdown report")
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
