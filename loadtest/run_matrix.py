#!/usr/bin/env python3
"""Automated Matrix Benchmark Runner for 3-Way Serving Architecture Comparison.

Features:
- Randomizes execution order across targets to eliminate temporal bias.
- Runs N independent trials per architecture (default 5).
- Captures offered, completed, successful, and failed requests (Goodput).
- Decomposes server execution time from network transit latency.
- Emits raw trial data in the exact JSON schema consumed by results/analyze.py.
- Records each trial's seed, UTC start time, and whether k6 thresholds were breached.
- Re-exports the raw files after every recorded trial so an abort keeps completed trials.
"""

import argparse
import csv
import datetime
import json
import math
import os
import random
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_K6_SCRIPT = os.path.join(SCRIPT_DIR, "k6_benchmark.js")
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "results", "matrix_runs")

# k6 exits with this code when a threshold in k6_benchmark.js is crossed. The
# run itself completed and its summary is valid, so the trial is recorded and
# flagged rather than discarded.
K6_THRESHOLD_EXIT_CODE = 99

TARGETS = {
    "direct": {
        "name": "Direct Serving (Uvicorn)",
        "url": "http://direct-serving-svc:8000",
    },
    "ray_python": {
        "name": "Ray Serve (Default Python Proxy :8000)",
        "url": "http://ray-python-proxy-svc:8000",
    },
    "ray_haproxy": {
        "name": "Ray Serve (HAProxy :8000)",
        "url": "http://ray-haproxy-svc:8000",
    },
    "ray_direct": {
        "name": "Ray Serve (Direct Ingress :30000)",
        "url": "http://ray-direct-ingress-svc:8000",
    },
    "ray_haproxy_local": {
        "name": "Ray Serve (HAProxy local control)",
        "url": "http://ray-haproxy-w1-svc:8000",
    },
    "ray_direct_local": {
        "name": "Ray Serve (Direct Ingress local control)",
        "url": "http://ray-direct-w1-svc:8000",
    },
}

BLOCKS = {
    "block1": ["direct", "ray_haproxy", "ray_direct"],
    "block2": ["direct", "ray_python"],
    "block3": ["ray_haproxy_local", "ray_direct_local"],
}


def resolve_targets(block: str, override: Optional[str] = None) -> List[str]:
    """Return the target keys for a block, optionally narrowed to a comma-separated override."""
    if block not in BLOCKS:
        raise ValueError(f"Unknown block: {block!r}. Expected one of {sorted(BLOCKS)}")
    allowed = BLOCKS[block]
    if override is None:
        return list(allowed)

    selected = [name.strip() for name in override.split(",")]
    for name in selected:
        if name not in allowed:
            raise ValueError(f"Target {name!r} is not part of {block}; choose from {allowed}")
    return selected


def parse_k6_json(
    json_path: str,
    target_key: str,
    profile: str,
    trial_num: int,
    block: str = "legacy",
) -> Dict[str, Any]:
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
        "block": block,
    }


def run_k6_trial(
    target_key: str,
    target_url: str,
    rate_rps: int,
    duration: str,
    output_json: str,
    trial_num: int,
    k6_script_path: str = DEFAULT_K6_SCRIPT,
    block: str = "legacy",
    seed: Optional[int] = None,
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
    started_at_utc = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    threshold_breached = completed.returncode == K6_THRESHOLD_EXIT_CODE
    if completed.returncode not in (0, K6_THRESHOLD_EXIT_CODE):
        print(completed.stdout)
        raise RuntimeError(
            f"k6 exited with code {completed.returncode} for {target_key} ({duration})"
        )
    if threshold_breached:
        print(f"[WARN] k6 thresholds crossed for {target_key} ({duration}); recording trial with threshold_breached=true")
    result = parse_k6_json(output_json, target_key, f"{rate_rps}rps", trial_num, block)
    if result:
        result["threshold_breached"] = threshold_breached
        result["seed"] = seed
        result["started_at_utc"] = started_at_utc
    return result


def export_raw_trials_schema(
    all_trials: List[Dict[str, Any]],
    output_json: str,
    output_csv: str,
    rate_rps: int,
    duration: str,
    trials_per_arch: int,
    append: bool = False,
    az: str = "us-west-2a",
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(output_csv)), exist_ok=True)

    merged_trials: List[Dict[str, Any]] = list(all_trials)
    if append and os.path.exists(output_json):
        with open(output_json, "r", encoding="utf-8") as f:
            existing_payload = json.load(f)
        merged_trials = list(existing_payload.get("trials", [])) + merged_trials

    # Deduplicate by (block, target_key, trial_number); a newer run replaces an
    # earlier result for the same slot so reruns are idempotent.
    trials_by_key: Dict[Tuple[str, str, Any], Dict[str, Any]] = {}
    for trial in merged_trials:
        key = (
            trial.get("block", "legacy"),
            trial.get("target_key", ""),
            trial.get("trial_number"),
        )
        trials_by_key[key] = trial
    sorted_trials = [trials_by_key[key] for key in sorted(trials_by_key)]

    # Note: this counts distinct trial numbers per block, i.e. runs per arm per
    # block, not total trial records in the block.
    trial_numbers_by_block: Dict[str, set] = {}
    for trial in sorted_trials:
        trial_numbers_by_block.setdefault(trial.get("block", "legacy"), set()).add(
            trial.get("trial_number")
        )
    trials_per_block = {
        block: len(numbers) for block, numbers in trial_numbers_by_block.items()
    }

    metadata = {
        "workload": "Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, depth 6)",
        "hardware": "AWS c6i.2xlarge On-Demand (Intel Xeon 8375C @ 2.90GHz)",
        "co_location": f"Single Availability Zone ({az}), zero cross-AZ transit",
        "replicas_per_architecture": 2,
        "cpu_per_replica": 1.0,
        "memory_per_replica": "2 GiB",
        "omp_num_threads": 1,
        "target_arrival_rate_rps": float(rate_rps),
        "measured_stage_duration_seconds": 30.0,
        "total_trials": len(sorted_trials),
        "trials_per_architecture": trials_per_arch,
        "trials_per_block": trials_per_block,
    }

    payload = {
        "benchmark_metadata": metadata,
        "trials": sorted_trials,
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"[OK] Exported raw JSON results in analyze.py schema to: {output_json}")

    fieldnames = [
        "architecture", "target_key", "block", "trial_number", "total_requests",
        "successful_requests", "failed_requests", "goodput_rps",
        "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms",
        "pure_booster_predict_p50_ms", "server_total_p50_ms", "network_transit_p50_ms",
        "threshold_breached", "seed", "started_at_utc",
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in sorted_trials:
            writer.writerow({k: t.get(k, "") for k in fieldnames})
    print(f"[OK] Exported raw CSV results to: {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Run randomized matrix benchmark across serving architectures.")
    parser.add_argument("--block", required=True, choices=list(BLOCKS.keys()), help="Benchmark block to run")
    parser.add_argument("--targets", default=None, help="Comma-separated subset of the block's target keys")
    parser.add_argument("--trials", type=int, default=5, help="Number of independent trials per architecture")
    parser.add_argument("--rate-rps", type=int, default=50, help="Target constant arrival rate in req/s")
    parser.add_argument("--duration", default="30s", help="Duration per test trial")
    parser.add_argument("--az", default="us-west-2a", help="Availability zone the cluster runs in (recorded in benchmark metadata)")
    parser.add_argument("--warmup-duration", default="15s", help="Unrecorded warmup duration per target before trials")
    parser.add_argument("--append", action="store_true", help="Merge these trials into an existing raw results file")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for individual trial JSON files")
    # Required: there is no default so a run can never overwrite a historical
    # dataset (e.g. the October results/raw_trials.{json,csv}).
    parser.add_argument("--output-raw-json", required=True, help="Path for aggregated raw_trials.json")
    parser.add_argument("--output-raw-csv", required=True, help="Path for aggregated raw_trials.csv")
    parser.add_argument("--k6-script", default=DEFAULT_K6_SCRIPT, help="Path to k6 script")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for trial execution order")
    args = parser.parse_args()

    try:
        target_keys = resolve_targets(args.block, args.targets)
    except ValueError as exc:
        parser.error(str(exc))

    os.makedirs(args.output_dir, exist_ok=True)
    random.seed(args.seed)

    # Warmup pass: run once per target; results are not recorded as trials.
    try:
        for target_key in target_keys:
            target_info = TARGETS[target_key]
            warmup_file = os.path.join(args.output_dir, f"warmup_{target_key}.json")
            run_k6_trial(
                target_key,
                target_info["url"],
                args.rate_rps,
                args.warmup_duration,
                warmup_file,
                0,
                args.k6_script,
                block=args.block,
                seed=args.seed,
            )
            print(f"--> Warmup complete for {target_info['name']}")
    except RuntimeError as exc:
        print(f"[ERROR] Warmup failed: {exc}", file=sys.stderr)
        sys.exit(1)

    trial_queue = []
    for target_key in target_keys:
        for i in range(args.trials):
            trial_queue.append((target_key, i + 1))
    random.shuffle(trial_queue)

    print(f"================================================================")
    print(f" Matrix Benchmark ({args.block}): {len(trial_queue)} Total Randomized Trials")
    print(f" Rate: {args.rate_rps} req/s | Duration: {args.duration} per trial")
    print(f" Targets: {[TARGETS[t[0]]['name'] for t in trial_queue[:len(target_keys)]]}")
    print(f" Random Order: {[t[0] + '#' + str(t[1]) for t in trial_queue]}")
    print(f"================================================================")

    all_trials: List[Dict[str, Any]] = []

    try:
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
                block=args.block,
                seed=args.seed,
            )
            if trial_res:
                all_trials.append(trial_res)
                print(f"--> Result: p50={trial_res.get('p50_ms')}ms, p95={trial_res.get('p95_ms')}ms, Goodput={trial_res.get('goodput_rps')} req/s")
                # Export after every recorded trial (same merge/dedupe semantics as a
                # single end-of-block export) so an abort never discards completed trials.
                export_raw_trials_schema(
                    all_trials,
                    args.output_raw_json,
                    args.output_raw_csv,
                    args.rate_rps,
                    args.duration,
                    args.trials,
                    append=args.append,
                    az=args.az,
                )
            time.sleep(3)
    except RuntimeError as exc:
        print(f"[ERROR] Trial failed: {exc}", file=sys.stderr)
        if all_trials:
            print(f"[INFO] {len(all_trials)} completed trial(s) already exported to {args.output_raw_json}", file=sys.stderr)
        sys.exit(1)

    print("\nBenchmark matrix run complete.")


if __name__ == "__main__":
    main()
