#!/usr/bin/env python3
"""Automated Matrix Benchmark Runner for 3-Way Serving Architecture Comparison.

Features:
- Randomizes execution order across targets to eliminate temporal bias.
- Runs N independent trials per architecture (default 5).
- Captures offered, completed, successful, and failed requests (Goodput).
- Decomposes server execution time from network transit latency.
- Emits raw trial data in the exact JSON schema consumed by results/analyze.py.
"""

import argparse
import csv
import json
import math
import os
import random
import subprocess
import sys
import time
from typing import Any, Dict, List, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_K6_SCRIPT = os.path.join(SCRIPT_DIR, "k6_benchmark.js")
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "results", "matrix_runs")
DEFAULT_RAW_JSON = os.path.join(SCRIPT_DIR, "..", "results", "raw_trials.json")
DEFAULT_RAW_CSV = os.path.join(SCRIPT_DIR, "..", "results", "raw_trials.csv")

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


def parse_k6_json(json_path: str, target_key: str, profile: str, trial_num: int) -> Dict[str, Any]:
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

    p50 = get_stat("http_req_duration", "med")
    if p50 == 0.0:
        p50 = get_stat("http_req_duration", "p(50)")
    p90 = get_stat("http_req_duration", "p(90)")
    p95 = get_stat("http_req_duration", "p(95)")
    p99 = get_stat("http_req_duration", "p(99)")
    max_ms = get_stat("http_req_duration", "max")

    model_ms = get_stat("server_model_latency_ms", "med")
    if model_ms == 0.0:
        model_ms = get_stat("server_model_latency_ms", "p(50)")

    server_tot_ms = get_stat("server_total_latency_ms", "med")
    if server_tot_ms == 0.0:
        server_tot_ms = get_stat("server_total_latency_ms", "p(50)")

    net_ms = get_stat("network_roundtrip_latency_ms", "med")
    if net_ms == 0.0:
        net_ms = get_stat("network_roundtrip_latency_ms", "p(50)")

    goodput = metrics.get("http_reqs", {}).get("rate", 0.0)

    arch_name = TARGETS[target_key]["name"]

    return {
        "architecture": arch_name,
        "target_key": target_key,
        "trial_number": trial_num,
        "total_requests": http_reqs,
        "successful_requests": success_cnt,
        "failed_requests": failed_cnt,
        "goodput_rps": round(goodput, 2),
        "p50_ms": round(p50, 3),
        "p90_ms": round(p90, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
        "max_ms": round(max_ms, 2),
        "pure_booster_predict_p50_ms": round(model_ms, 3),
        "server_total_p50_ms": round(server_tot_ms, 3),
        "network_transit_p50_ms": round(net_ms, 3),
    }


def run_k6_trial(
    target_key: str,
    target_url: str,
    rate_rps: int,
    duration: str,
    output_json: str,
    trial_num: int,
    k6_script_path: str = DEFAULT_K6_SCRIPT,
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
    return parse_k6_json(output_json, target_key, f"{rate_rps}rps", trial_num)


def export_raw_trials_schema(
    all_trials: List[Dict[str, Any]],
    output_json: str,
    output_csv: str,
    rate_rps: int,
    duration: str,
    trials_per_arch: int,
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(output_csv)), exist_ok=True)

    metadata = {
        "workload": "Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, depth 6)",
        "hardware": "AWS c6i.2xlarge On-Demand (Intel Xeon 8375C @ 2.90GHz)",
        "co_location": "Single Availability Zone (us-east-1d), zero cross-AZ transit",
        "replicas_per_architecture": 2,
        "cpu_per_replica": 1.0,
        "memory_per_replica": "2 GiB",
        "omp_num_threads": 1,
        "target_arrival_rate_rps": float(rate_rps),
        "measured_stage_duration_seconds": 30.0,
        "total_trials": len(all_trials),
        "trials_per_architecture": trials_per_arch,
    }

    # Sort deterministically by architecture and trial number
    sorted_trials = sorted(all_trials, key=lambda t: (t["architecture"], t["trial_number"]))

    payload = {
        "benchmark_metadata": metadata,
        "trials": sorted_trials,
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"[OK] Exported raw JSON results in analyze.py schema to: {output_json}")

    fieldnames = [
        "architecture", "target_key", "trial_number", "total_requests",
        "successful_requests", "failed_requests", "goodput_rps",
        "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms",
        "pure_booster_predict_p50_ms", "server_total_p50_ms", "network_transit_p50_ms",
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in sorted_trials:
            writer.writerow({k: t.get(k, "") for k in fieldnames})
    print(f"[OK] Exported raw CSV results to: {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Run randomized matrix benchmark across serving architectures.")
    parser.add_argument("--trials", type=int, default=5, help="Number of independent trials per architecture")
    parser.add_argument("--rate-rps", type=int, default=50, help="Target constant arrival rate in req/s")
    parser.add_argument("--duration", default="30s", help="Duration per test trial")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for individual trial JSON files")
    parser.add_argument("--output-raw-json", default=DEFAULT_RAW_JSON, help="Path for aggregated raw_trials.json")
    parser.add_argument("--output-raw-csv", default=DEFAULT_RAW_CSV, help="Path for aggregated raw_trials.csv")
    parser.add_argument("--k6-script", default=DEFAULT_K6_SCRIPT, help="Path to k6 script")
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

    all_trials: List[Dict[str, Any]] = []

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
            trial_num,
            args.k6_script,
        )
        if trial_res:
            all_trials.append(trial_res)
            print(f"--> Result: p50={trial_res.get('p50_ms')}ms, p95={trial_res.get('p95_ms')}ms, Goodput={trial_res.get('goodput_rps')} req/s")
        time.sleep(3)

    if all_trials:
        export_raw_trials_schema(
            all_trials,
            args.output_raw_json,
            args.output_raw_csv,
            args.rate_rps,
            args.duration,
            args.trials,
        )
    print("\nBenchmark matrix run complete.")


if __name__ == "__main__":
    main()
