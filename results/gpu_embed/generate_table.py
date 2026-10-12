#!/usr/bin/env python3
"""Parses raw k6 sweep JSON results and regenerates the Markdown benchmark table."""

import glob
import json
import os
import sys

CONCURRENCIES = [1, 4, 8, 16, 32, 64]
ARCHITECTURES = [
    ("direct", "Direct Serving (Async Queue)"),
    ("ray_haproxy", "Ray Serve (@serve.batch + HAProxy :30000)"),
]

def parse_json(fpath):
    if not os.path.exists(fpath):
        return None
    with open(fpath) as f:
        d = json.load(f)
    m = d.get("metrics", {})
    rate = m.get("http_reqs", {}).get("rate", 0.0)
    failed = m.get("failed_embeddings", {}).get("count", 0)
    dur = m.get("http_req_duration", {})
    return {
        "throughput": round(rate, 2),
        "p50": round(dur.get("med", 0.0), 2),
        "p90": round(dur.get("p(90)", 0.0), 2),
        "p95": round(dur.get("p(95)", 0.0), 2),
        "p99": round(dur.get("p(99)", 0.0), 2),
        "failure_rate": "0.00%" if failed == 0 else f"{(failed / m.get('http_reqs', {}).get('count', 1))*100:.2f}%"
    }

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print("# Phase 3 GPU Embedding Concurrency Sweep Results\n")
    print("| Architecture | Concurrency | Throughput (embeddings/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Failure Rate |")
    print("|---|---|---|---|---|---|---|---|")
    
    for c in CONCURRENCIES:
        for key, name in ARCHITECTURES:
            fpath = os.path.join(script_dir, f"{key}_c{c}.json")
            stats = parse_json(fpath)
            if stats:
                print(f"| **{name}** | {c} | **{stats['throughput']}** | {stats['p50']} | {stats['p90']} | **{stats['p95']}** | {stats['p99']} | {stats['failure_rate']} |")

if __name__ == "__main__":
    main()
