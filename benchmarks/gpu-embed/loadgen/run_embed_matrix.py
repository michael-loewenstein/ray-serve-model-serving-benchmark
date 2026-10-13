#!/usr/bin/env python3
"""Automated, statistically rigorous GPU embedding benchmark matrix runner.

Features:
- Closed-loop concurrency scaling (constant-vus) and optional arrival-rate load profiles.
- Warmup stage per target to eliminate cold starts, JIT compilation, and CUDA context overhead.
- Multi-trial execution (default 5 independent trials per concurrency level).
- Randomized / alternating execution order to eliminate thermal and temporal bias.
- Statistical aggregation: Median, Mean, Standard Deviation, and 95% Confidence Intervals.
- Saves individual trial raw k6 JSON exports for 100% data provenance.
"""

import argparse
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

TARGETS = {
    "direct": {
        "name": "Direct Serving (Async Queue Batcher)",
        "url": os.environ.get("DIRECT_URL", "http://direct-gpu-embed-svc:8000"),
    },
    "ray_haproxy": {
        "name": "Ray Serve (@serve.batch + HAProxy :30000)",
        "url": os.environ.get("RAY_URL", "http://rayservice-gpu-embed-haproxy-svc:8000"),
    },
}


def run_k6_trial(
    target_key: str,
    target_url: str,
    concurrency: int,
    duration: str,
    output_json: str,
    k6_script: str,
    profile: str = "concurrency",
    is_warmup: bool = False,
) -> Optional[Dict[str, Any]]:
    cmd = [
        "k6", "run",
        "-e", f"TARGET_HOST={target_url}",
        "-e", "EMBED_PATH=/embed",
        "-e", f"CONCURRENCY={concurrency}",
        "-e", f"STAGE_DURATION={duration}",
        "-e", f"TEST_PROFILE={profile}",
        "--summary-trend-stats", "min,avg,med,max,p(90),p(95),p(99)",
        "--summary-export", output_json,
        k6_script,
    ]
    label = f"[WARMUP]" if is_warmup else f"[TRIAL]"
    print(f"\n{label} Running {target_key} (C={concurrency}, duration={duration}, profile={profile})...")

    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        print(f"[ERROR] k6 exited with non-zero exit code {res.returncode}: " + res.stdout[-500:])
        return None
    if not os.path.exists(output_json):
        print(f"[ERROR] Output not found at {output_json}\nOutput was:\n{res.stdout[-500:]}")
        return None

    try:
        with open(output_json, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception as exc:
        print(f"[ERROR] Failed to parse JSON export {output_json}: {exc}")
        return None

    metrics = d.get("metrics", {})

    def get_count(metric_name: str) -> int:
        m = metrics.get(metric_name, {})
        if "count" in m:
            return int(m["count"])
        if "values" in m and "count" in m["values"]:
            return int(m["values"]["count"])
        return 0

    def get_stat(metric_name: str, stat_name: str) -> float:
        m = metrics.get(metric_name, {})
        if stat_name in m:
            return float(m[stat_name])
        if "values" in m and stat_name in m["values"]:
            return float(m["values"][stat_name])
        return 0.0

    reqs = get_count("http_reqs")
    success_cnt = get_count("successful_embeddings")
    failed_cnt = get_count("failed_embeddings")
    if reqs > 0 and (success_cnt + failed_cnt == 0):
        failed_cnt = int(metrics.get("http_req_failed", {}).get("passes", 0))
        success_cnt = reqs - failed_cnt

    rate = metrics.get("http_reqs", {}).get("rate", 0.0)

    p50 = get_stat("http_req_duration", "med") or get_stat("http_req_duration", "p(50)")
    p90 = get_stat("http_req_duration", "p(90)")
    p95 = get_stat("http_req_duration", "p(95)")
    p99 = get_stat("http_req_duration", "p(99)")
    avg_dur = get_stat("http_req_duration", "avg")

    model_ms = get_stat("server_model_latency_ms", "med") or get_stat("server_model_latency_ms", "p(50)")
    tot_ms = get_stat("server_total_latency_ms", "med") or get_stat("server_total_latency_ms", "p(50)")

    return {
        "target_key": target_key,
        "concurrency": concurrency,
        "total_requests": reqs,
        "successful_requests": success_cnt,
        "failed_requests": failed_cnt,
        "throughput_rps": round(rate, 2),
        "p50_ms": round(p50, 3),
        "p90_ms": round(p90, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
        "avg_ms": round(avg_dur, 3),
        "server_model_p50_ms": round(model_ms, 3),
        "server_total_p50_ms": round(tot_ms, 3),
    }


def compute_statistics(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"median": 0.0, "mean": 0.0, "min": 0.0, "max": 0.0, "std": 0.0, "ci95": 0.0}
    n = len(values)
    med = float(statistics.median(values))
    mean = float(statistics.mean(values))
    min_v = float(min(values))
    max_v = float(max(values))
    std = float(statistics.stdev(values)) if n > 1 else 0.0
    t_crit_table = {1: 12.71, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306}
    t_crit = t_crit_table.get(n - 1, 1.96)
    ci95 = (t_crit * std / math.sqrt(n)) if n > 1 else 0.0
    return {
        "median": round(med, 3),
        "mean": round(mean, 3),
        "min": round(min_v, 3),
        "max": round(max_v, 3),
        "std": round(std, 3),
        "ci95": round(ci95, 3),
    }


def main():
    parser = argparse.ArgumentParser(description="Multi-trial rigorous GPU dynamic batching benchmark.")
    parser.add_argument("--concurrencies", default="1,4,8,16,32,64", help="Comma-separated concurrency levels")
    parser.add_argument("--trials", type=int, default=5, help="Number of independent trials per concurrency level")
    parser.add_argument("--duration", default="30s", help="Measurement duration per trial stage (default: 30s)")
    parser.add_argument("--warmup-duration", default="15s", help="Duration of pre-benchmark warmup run")
    parser.add_argument("--profile", default="concurrency", choices=["concurrency", "steady", "sweep"], help="k6 load profile")
    parser.add_argument("--output-dir", default="results/gpu_embed", help="Directory to save raw k6 JSONs and summaries")
    parser.add_argument("--k6-script", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "k6_embed.js"), help="Path to k6 script")
    parser.add_argument("--skip-warmup", action="store_true", help="Skip the initial warmup phase")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    raw_dir = os.path.join(args.output_dir, "raw_trials")
    os.makedirs(raw_dir, exist_ok=True)

    levels = [int(x.strip()) for x in args.concurrencies.split(",")]

    # 1. Warmup Stage
    if not args.skip_warmup and args.warmup_duration:
        print("=" * 60)
        print("STAGE 0: Warmup Phase (Warming up CUDA context, PyTorch kernels & Ray Actor)")
        print("=" * 60)
        for target_key, target_info in TARGETS.items():
            warmup_file = os.path.join(raw_dir, f"warmup_{target_key}.json")
            run_k6_trial(
                target_key=target_key,
                target_url=target_info["url"],
                concurrency=4,
                duration=args.warmup_duration,
                output_json=warmup_file,
                k6_script=args.k6_script,
                profile=args.profile,
                is_warmup=True,
            )
            time.sleep(2)

    # 2. Benchmark Execution Matrix
    results_by_conc = {conc: {k: [] for k in TARGETS.keys()} for conc in levels}
    all_trials_log = []

    print("\n" + "=" * 60)
    print(f"STAGE 1: Executing Matrix ({len(levels)} levels x {len(TARGETS)} targets x {args.trials} trials)")
    print("=" * 60)

    for conc in levels:
        print(f"\n>>> Running Concurrency Level C={conc} <<<")
        for trial_idx in range(1, args.trials + 1):
            target_keys = list(TARGETS.keys())
            if trial_idx % 2 == 0:
                target_keys.reverse()
            else:
                random.shuffle(target_keys)

            for target_key in target_keys:
                target_info = TARGETS[target_key]
                trial_file = os.path.join(raw_dir, f"{target_key}_c{conc}_trial{trial_idx}.json")
                res = run_k6_trial(
                    target_key=target_key,
                    target_url=target_info["url"],
                    concurrency=conc,
                    duration=args.duration,
                    output_json=trial_file,
                    k6_script=args.k6_script,
                    profile=args.profile,
                    is_warmup=False,
                )
                if res:
                    res["trial"] = trial_idx
                    results_by_conc[conc][target_key].append(res)
                    all_trials_log.append(res)
                    print(f"  [C={conc} Trial {trial_idx}/{args.trials}] {target_key}: "
                          f"{res['throughput_rps']} req/s | p50={res['p50_ms']}ms | "
                          f"p95={res['p95_ms']}ms | p99={res['p99_ms']}ms")
                time.sleep(2)

    # 3. Generate Rigorous Statistical Summary
    summary_md = os.path.join(args.output_dir, "statistical_summary_concurrency.md")
    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Rigorous Statistical GPU Embedding Benchmark: Concurrency Scaling Matrix\n\n")
        f.write(f"- **Methodology:** Closed-Loop Concurrency Scaling (`constant-vus`) across C in [{', '.join(str(c) for c in levels)}]\n")
        f.write(f"- **Independent Trials:** {args.trials} per level per architecture (Alternating Execution Order)\n")
        f.write(f"- **Duration per Trial:** {args.duration} (+ {args.warmup_duration} warmup)\n")
        f.write(f"- **Workload:** `all-MiniLM-L6-v2` FP16 on NVIDIA L4 (Max batch: 32, Wait: 5 ms)\n\n")

        f.write("## 1. Aggregate Latency & Throughput Matrix across Trials\n\n")
        f.write("| Architecture | Concurrency | Goodput (req/s) [Mean ± 95% CI] | p50 Median [IQR] (ms) | p90 Median (ms) | p95 Median [IQR] (ms) | p99 Median [IQR] (ms) | Pure Model GPU p50 (ms) |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")

        for conc in levels:
            for target_key, target_info in TARGETS.items():
                trial_list = results_by_conc[conc][target_key]
                if not trial_list:
                    continue

                tps = [t["throughput_rps"] for t in trial_list]
                p50s = [t["p50_ms"] for t in trial_list]
                p90s = [t["p90_ms"] for t in trial_list]
                p95s = [t["p95_ms"] for t in trial_list]
                p99s = [t["p99_ms"] for t in trial_list]
                model_times = [t["server_model_p50_ms"] for t in trial_list]

                stat_tp = compute_statistics(tps)
                stat_p50 = compute_statistics(p50s)
                stat_p90 = compute_statistics(p90s)
                stat_p95 = compute_statistics(p95s)
                stat_p99 = compute_statistics(p99s)
                stat_model = compute_statistics(model_times)

                f.write(
                    f"| **{target_info['name']}** | {conc} | "
                    f"**{stat_tp['mean']}** ± {stat_tp['ci95']} | "
                    f"**{stat_p50['median']}** (±{stat_p50['ci95']}) | "
                    f"{stat_p90['median']} | "
                    f"**{stat_p95['median']}** (±{stat_p95['ci95']}) | "
                    f"**{stat_p99['median']}** (±{stat_p99['ci95']}) | "
                    f"{stat_model['median']} ms |\n"
                )

        f.write("\n## 2. Individual Trial Measurements\n\n")
        f.write("| Concurrency | Trial # | Architecture | Total Reqs | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Pure Model (ms) |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for row in all_trials_log:
            arch_name = TARGETS.get(row["target_key"], {}).get("name", row["target_key"])
            f.write(f"| {row['concurrency']} | {row.get('trial', 1)} | {arch_name} | {row['total_requests']} | {row['throughput_rps']} | {row['p50_ms']} | {row['p95_ms']} | {row['p99_ms']} | {row['server_model_p50_ms']} |\n")

    print("\n" + "=" * 60)
    with open(summary_md, "r", encoding="utf-8") as f:
        print(f.read())
    print("=" * 60)


if __name__ == "__main__":
    main()
