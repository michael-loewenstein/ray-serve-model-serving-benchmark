#!/usr/bin/env python3
"""Automated GPU embedding benchmark runner comparing Direct vs Ray Serve dynamic batching."""

import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List

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


def run_k6_concurrency_trial(target_key: str, target_url: str, concurrency: int, duration: str, output_json: str, k6_script: str) -> Dict[str, Any]:
    cmd = [
        "k6", "run",
        "-e", f"TARGET_HOST={target_url}",
        "-e", "EMBED_PATH=/embed",
        "-e", f"CONCURRENCY={concurrency}",
        "-e", f"STAGE_DURATION={duration}",
        "-e", "TEST_PROFILE=concurrency",
        "--summary-trend-stats", "min,avg,med,max,p(90),p(95),p(99)",
        "--summary-export", output_json,
        k6_script,
    ]
    print(f"\n[EXEC] Running {target_key} at Concurrency={concurrency} ({duration})...")
    t0 = time.time()
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0

    if not os.path.exists(output_json):
        print(f"[ERROR] Output not found at {output_json}")
        return {}

    with open(output_json) as f:
        d = json.load(f)

    metrics = d.get("metrics", {})
    reqs = metrics.get("http_reqs", {}).get("count", 0)
    rate = metrics.get("http_reqs", {}).get("rate", 0.0)
    failed = metrics.get("failed_embeddings", {}).get("count", 0)
    dur = metrics.get("http_req_duration", {})
    p50 = dur.get("med", 0.0)
    p90 = dur.get("p(90)", 0.0)
    p95 = dur.get("p(95)", 0.0)
    p99 = dur.get("p(99)", 0.0)

    model_ms = metrics.get("server_model_latency_ms", {}).get("med", 0.0)
    tot_ms = metrics.get("server_total_latency_ms", {}).get("med", 0.0)

    return {
        "target_key": target_key,
        "concurrency": concurrency,
        "total_requests": reqs,
        "throughput_rps": round(rate, 2),
        "failed_requests": failed,
        "p50_ms": round(p50, 3),
        "p90_ms": round(p90, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
        "server_model_p50_ms": round(model_ms, 3),
        "server_total_p50_ms": round(tot_ms, 3),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrencies", default="1,4,8,16,32,64", help="Comma-separated concurrency levels")
    parser.add_argument("--duration", default="45s", help="Duration per step")
    parser.add_argument("--output-dir", default="results/gpu_embed", help="Output directory")
    parser.add_argument("--k6-script", default="k6_embed.js", help="Path to k6 script")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    levels = [int(x.strip()) for x in args.concurrencies.split(",")]

    results = {k: [] for k in TARGETS.keys()}

    for conc in levels:
        for target_key, target_info in TARGETS.items():
            json_file = os.path.join(args.output_dir, f"{target_key}_c{conc}.json")
            res = run_k6_concurrency_trial(target_key, target_info["url"], conc, args.duration, json_file, args.k6_script)
            if res:
                results[target_key].append(res)
                print(f"--> Done {target_key} (C={conc}): Throughput={res['throughput_rps']} req/s, p50={res['p50_ms']}ms, p95={res['p95_ms']}ms")
            time.sleep(3)

    summary_md = os.path.join(args.output_dir, "summary_concurrency.md")
    with open(summary_md, "w") as f:
        f.write("# GPU Embedding Concurrency Sweep: Direct vs. Ray Serve\n\n")
        f.write("| Architecture | Concurrency | Throughput (embeddings/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Server Batch Compute (ms) |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")
        for conc in levels:
            for target_key, target_info in TARGETS.items():
                match = [r for r in results[target_key] if r["concurrency"] == conc]
                if match:
                    r = match[0]
                    f.write(f"| **{target_info['name']}** | {r['concurrency']} | **{r['throughput_rps']}** | {r['p50_ms']} | {r['p90_ms']} | **{r['p95_ms']}** | {r['p99_ms']} | {r['server_model_p50_ms']} ms |\n")

    print("\n" + "="*60)
    with open(summary_md) as f:
        print(f.read())
    print("="*60)


if __name__ == "__main__":
    main()
