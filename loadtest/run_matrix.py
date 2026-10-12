#!/usr/bin/env python3
"""Automated Matrix Benchmark Runner for 3-Way Serving Architecture Comparison.

Features:
- Randomizes execution order across targets to eliminate temporal bias.
- Runs N independent trials per architecture (default 5).
- Captures offered, completed, successful, and failed requests (Goodput).
- Decomposes server execution time from network transit latency.
- Calculates Median, Min, Max, IQR, and 95% Confidence Intervals across trials.
"""

import argparse
import glob
import json
import math
import os
import random
import subprocess
import sys
import time
from typing import Any, Dict, List


TARGETS = {
    "direct": {
        "name": "Direct Serving (Uvicorn)",
        "url": os.environ.get("DIRECT_URL", "http://direct-serving-svc:8000"),
    },
    "ray_python": {
        "name": "Ray Serve (Python Proxy)",
        "url": os.environ.get("RAY_PYTHON_URL", "http://ray-serve-python-proxy-serve-svc:8000"),
    },
    "ray_haproxy": {
        "name": "Ray Serve (Worker HAProxy :30000)",
        "url": os.environ.get("RAY_HAPROXY_URL", "http://ray-serve-worker-haproxy-svc:8000"),
    },
}


def parse_k6_json(json_path: str, target_key: str, profile: str) -> Dict[str, Any]:
    if not os.path.exists(json_path):
        return {}
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    metrics = data.get("metrics", {})

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

    http_reqs = get_count("http_reqs")
    success_cnt = get_count("successful_requests")
    failed_cnt = get_count("failed_requests")

    if success_cnt == 0 and http_reqs > 0:
        failed_cnt = int(metrics.get("http_req_failed", {}).get("passes", 0))
        success_cnt = http_reqs - failed_cnt

    dur_vals = metrics.get("http_req_duration", {})
    p50 = get_stat("http_req_duration", "med")
    if p50 == 0.0:
        p50 = get_stat("http_req_duration", "p(50)")
    p90 = get_stat("http_req_duration", "p(90)")
    p95 = get_stat("http_req_duration", "p(95)")
    p99 = get_stat("http_req_duration", "p(99)")
    avg_dur = get_stat("http_req_duration", "avg")

    model_ms = get_stat("server_model_latency_ms", "med")
    if model_ms == 0.0:
        model_ms = get_stat("server_model_latency_ms", "p(50)")

    server_tot_ms = get_stat("server_total_latency_ms", "med")
    if server_tot_ms == 0.0:
        server_tot_ms = get_stat("server_total_latency_ms", "p(50)")

    net_ms = get_stat("network_roundtrip_latency_ms", "med")
    if net_ms == 0.0:
        net_ms = get_stat("network_roundtrip_latency_ms", "p(50)")

    error_rate = (failed_cnt / http_reqs * 100.0) if http_reqs > 0 else 0.0
    goodput = metrics.get("http_reqs", {}).get("rate", 0.0)
    if error_rate > 0:
        goodput = goodput * (1.0 - error_rate / 100.0)

    return {
        "target_key": target_key,
        "profile": profile,
        "total_requests": http_reqs,
        "successful_requests": success_cnt,
        "failed_requests": failed_cnt,
        "error_rate_pct": round(error_rate, 3),
        "goodput_rps": round(goodput, 2),
        "p50_ms": round(p50, 3),
        "p90_ms": round(p90, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
        "avg_ms": round(avg_dur, 3),
        "server_model_p50_ms": round(model_ms, 3),
        "server_total_p50_ms": round(server_tot_ms, 3),
        "network_roundtrip_p50_ms": round(net_ms, 3),
    }


def compute_statistics(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"median": 0.0, "min": 0.0, "max": 0.0, "iqr": 0.0, "mean": 0.0, "ci95": 0.0}
    vals = sorted(values)
    n = len(vals)
    median = vals[n // 2] if n % 2 == 1 else (vals[n // 2 - 1] + vals[n // 2]) / 2.0
    q1 = vals[n // 4]
    q3 = vals[(3 * n) // 4]
    iqr = q3 - q1
    mean = sum(vals) / n
    variance = sum((x - mean) ** 2 for x in vals) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)
    t_val = 2.776 if n == 5 else 1.96
    ci95 = t_val * (std_dev / math.sqrt(n)) if n > 1 else 0.0

    return {
        "median": round(median, 3),
        "min": round(vals[0], 3),
        "max": round(vals[-1], 3),
        "iqr": round(iqr, 3),
        "mean": round(mean, 3),
        "ci95": round(ci95, 3),
    }


def run_k6_trial(
    target_key: str,
    target_url: str,
    rate_rps: int,
    duration: str,
    output_json: str,
    k6_script_path: str = "k6_benchmark.js",
) -> Dict[str, Any]:
    cmd = [
        "k6", "run",
        "-e", f"TARGET_HOST={target_url}",
        "-e", "INFER_PATH=/infer",
        "-e", f"RATE_RPS={rate_rps}",
        "-e", f"STAGE_DURATION={duration}",
        "--summary-trend-stats", "min,avg,med,max,p(90),p(95),p(99)",
        "--summary-export", output_json,
        k6_script_path,
    ]
    print(f"\n[EXEC] Running {target_key} ({rate_rps} RPS, {duration})...")
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return parse_k6_json(output_json, target_key, f"{rate_rps}rps")


def main():
    parser = argparse.ArgumentParser(description="Run randomized matrix benchmark across serving architectures.")
    parser.add_argument("--trials", type=int, default=5, help="Number of independent trials per architecture")
    parser.add_argument("--rate-rps", type=int, default=50, help="Target constant arrival rate in req/s")
    parser.add_argument("--duration", default="30s", help="Duration per test trial")
    parser.add_argument("--output-dir", default="../results/matrix_runs", help="Directory for trial JSON files")
    parser.add_argument("--k6-script", default="k6_benchmark.js", help="Path to k6 script")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for trial execution order")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    random.seed(args.seed)

    trial_queue = []
    for target_key in TARGETS.keys():
        for i in range(args.trials):
            trial_queue.append((target_key, i + 1))
    random.shuffle(trial_queue)

    print(f"================================================================")
    print(f" Matrix Benchmark: {len(trial_queue)} Total Randomized Trials")
    print(f" Rate: {args.rate_rps} req/s | Duration: {args.duration} per trial")
    print(f" Random Order: {[t[0] + '#' + str(t[1]) for t in trial_queue]}")
    print(f"================================================================")

    results_by_target: Dict[str, List[Dict[str, Any]]] = {k: [] for k in TARGETS.keys()}

    for step, (target_key, trial_num) in enumerate(trial_queue, 1):
        target_info = TARGETS[target_key]
        json_file = os.path.join(args.output_dir, f"{target_key}_trial_{trial_num}.json")
        print(f"\n[{step}/{len(trial_queue)}] Starting Trial #{trial_num} for {target_info['name']}...")
        trial_res = run_k6_trial(
            target_key,
            target_info["url"],
            args.rate_rps,
            args.duration,
            json_file,
            args.k6_script,
        )
        if trial_res:
            results_by_target[target_key].append(trial_res)
            print(f"--> Result: p50={trial_res.get('p50_ms')}ms, p95={trial_res.get('p95_ms')}ms, Goodput={trial_res.get('goodput_rps')} req/s")
        time.sleep(3)

    print("\nBenchmark matrix run complete. Raw JSON results saved to:", args.output_dir)


if __name__ == "__main__":
    main()
