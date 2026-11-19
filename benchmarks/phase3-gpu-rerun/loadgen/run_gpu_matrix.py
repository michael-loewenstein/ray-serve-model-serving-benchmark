#!/usr/bin/env python3
"""Hardened Phase 3 GPU embedding matrix runner (gblock1: Direct vs Ray HAProxy).

Drives the closed-loop concurrency-scaling k6 matrix against the two Phase 3
serving Services with the hardening the Phase 2 rerun established:

- Requires explicit raw-output paths, so a bare invocation can never fall back
  into (and overwrite) the October artifacts under results/gpu_embed/.
- Every recorded trial is re-exported after it completes (append + dedupe on
  (block, target_key, concurrency, trial_number); a newer run wins a slot), so
  an abort mid-block never discards already-completed trials.
- A k6 threshold breach (exit 99) keeps the run going: the trial is recorded
  with threshold_breached=true. Any other non-zero k6 exit aborts the run by
  raising, after completed trials have already been exported.
- Seeded execution order: (arm, level) pairs are shuffled, each pair runs an
  unrecorded warmup before its trials, and trial numbers within a pair are
  shuffled too, eliminating temporal/thermal ordering bias while guaranteeing
  a pair's warmup always precedes its trials (a failed warmup skips only that
  pair so its cold-start data can never contaminate the recorded trials).
- Records each trial's run seed, UTC start time, and threshold status.

Scheduling note: warmups are interleaved per (arm, level) pair rather than
batched; each pair still gets exactly one warmup run (12 warmups + 60 recorded
trials in the default 2x6x5 matrix) that always precedes that pair's trials.
"""

import argparse
import csv
import datetime
import json
import os
import random
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

# Test-suite compat shim: the pinned tests/test_gpu_runner.py fakes return
# `subprocess.SimpleNamespace(...)`. That re-export does not exist on the
# Python 3.9 stdlib `subprocess` module, so alias it here once (this module
# is imported before any fake runs). Additive and idempotent.
if not hasattr(subprocess, "SimpleNamespace"):
    subprocess.SimpleNamespace = SimpleNamespace  # type: ignore[attr-defined]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_K6_SCRIPT = os.path.join(SCRIPT_DIR, "k6_embed.js")
DEFAULT_OUTPUT_DIR = os.path.join("results", "gpu_matrix")

# k6 exits with this code when a threshold in k6_embed.js is crossed. The run
# itself completed and its summary export is valid, so the trial is recorded
# and flagged threshold_breached rather than discarded.
K6_THRESHOLD_EXIT_CODE = 99

TARGETS = {
    "direct": {
        "name": "Direct Serving (GPU)",
        "url": "http://direct-gpu-embed-svc:8000",
    },
    "ray_ha": {
        "name": "Ray Serve (HAProxy :8000)",
        "url": "http://ray-ha-svc:8000",
    },
}

BLOCKS = {
    "gblock1": ["direct", "ray_ha"],
}

# Exact trial-record schema consumed by results/analyze_gpu.py (Task 4).
RECORD_KEYS = [
    "block", "target_key", "architecture", "concurrency", "trial_number",
    "seed", "started_at_utc", "total_requests", "successful_requests",
    "failed_requests", "threshold_breached", "throughput_rps",
    "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms",
    "server_model_p50_ms", "server_total_p50_ms", "network_transit_p50_ms",
]

# Run parameters mirrored into the exported benchmark_metadata block.
# export_raw_trials() reads these; main_with_args refreshes them from the
# CLI so the metadata always reflects the run that produced the file.
RUN_META: Dict[str, Any] = {
    "az": "us-west-2a",
    "seed": 42,
    "warmup_duration": "15s",
    "trials_per_level": 5,
    "concurrency_levels": [1, 4, 8, 16, 32, 64],
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


def parse_concurrencies(spec: str) -> List[int]:
    levels = [int(level.strip()) for level in spec.split(",") if level.strip()]
    if not levels:
        raise ValueError("At least one concurrency level is required")
    return levels


def parse_k6_metrics(summary_path: str) -> Dict[str, Any]:
    """Extract the trial metrics from a k6 --summary-export JSON file.

    Metric names mirror k6_embed.js: http_reqs (count + rate),
    successful_embeddings/failed_embeddings counters, http_req_duration trend,
    and the server/network latency trends the serving app reports in-band.
    Missing file or metric yields zeros; the summary export path is the
    single source of truth for a trial's numbers.
    """
    metrics: Dict[str, Any] = {}
    if summary_path and os.path.exists(summary_path):
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                metrics = json.load(f).get("metrics", {})
        except (OSError, ValueError) as exc:
            print(f"[WARN] Could not read k6 summary {summary_path}: {exc}")

    def count(name: str) -> int:
        m = metrics.get(name, {})
        if "count" in m:
            return int(m["count"])
        if "values" in m and "count" in m["values"]:
            return int(m["values"]["count"])
        return 0

    def stat(name: str, stat_name: str) -> float:
        m = metrics.get(name, {})
        if stat_name in m:
            return float(m[stat_name])
        if "values" in m and stat_name in m["values"]:
            return float(m["values"][stat_name])
        return 0.0

    http_reqs = count("http_reqs")
    success_cnt = count("successful_embeddings")
    failed_cnt = count("failed_embeddings")
    if http_reqs > 0 and (success_cnt + failed_cnt) == 0:
        failed_cnt = int(metrics.get("http_req_failed", {}).get("passes", 0))
        success_cnt = http_reqs - failed_cnt

    def med(name: str) -> float:
        val = stat(name, "med")
        if val == 0.0:
            val = stat(name, "p(50)")
        return val

    rate_raw = metrics.get("http_reqs", {}).get("rate")
    if rate_raw is None:  # counter rate lives under "values" in k6 0.5x exports
        rate_raw = metrics.get("http_reqs", {}).get("values", {}).get("rate", 0.0)
    rate = float(rate_raw)

    return {
        "total_requests": http_reqs,
        "successful_requests": success_cnt,
        "failed_requests": failed_cnt,
        "throughput_rps": round(float(rate), 2),
        "p50_ms": round(stat("http_req_duration", "med") or stat("http_req_duration", "p(50)"), 3),
        "p90_ms": round(stat("http_req_duration", "p(90)"), 3),
        "p95_ms": round(stat("http_req_duration", "p(95)"), 3),
        "p99_ms": round(stat("http_req_duration", "p(99)"), 3),
        "max_ms": round(stat("http_req_duration", "max"), 2),
        "server_model_p50_ms": round(med("server_model_latency_ms"), 3),
        "server_total_p50_ms": round(med("server_total_latency_ms"), 3),
        "network_transit_p50_ms": round(med("network_roundtrip_latency_ms"), 3),
    }


def run_k6_trial(
    target_key: str,
    target_url: str,
    concurrency: int,
    duration: str,
    output_json: str,
    trial_num: int,
    k6_script_path: str = DEFAULT_K6_SCRIPT,
    block: str = "gblock1",
    seed: Optional[int] = None,
    is_warmup: bool = False,
    cooldown_s: int = 0,
) -> Dict[str, Any]:
    """Run one k6 invocation and return its trial record.

    Exit 99 (k6 threshold breach) records the trial with
    threshold_breached=True; any other non-zero exit raises RuntimeError so
    the run aborts with completed trials already on disk.

    When cooldown_s > 0, sleep that many seconds after every completed k6
    invocation (warmup and trial alike) so consecutive 30 s closed-loop GPU
    stages drain thermally instead of running back-to-back — which inflates
    within-level variance (early trial cool, late trial hot) and hammers a
    struggling server right after a threshold breach.
    """
    cmd = [
        "k6", "run",
        "-e", f"TARGET_HOST={target_url}",
        "-e", "EMBED_PATH=/embed",
        "-e", f"CONCURRENCY={concurrency}",
        "-e", f"STAGE_DURATION={duration}",
        "-e", "TEST_PROFILE=concurrency",
        "--summary-trend-stats", "min,avg,med,max,p(90),p(95),p(99)",
        "--summary-export", output_json,
        k6_script_path,
    ]
    label = "[WARMUP]" if is_warmup else f"[TRIAL #{trial_num}]"
    print(f"\n{label} Running {target_key} (C={concurrency}, duration={duration})...")
    started_at_utc = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    threshold_breached = completed.returncode == K6_THRESHOLD_EXIT_CODE
    if completed.returncode not in (0, K6_THRESHOLD_EXIT_CODE):
        print(completed.stdout)
        raise RuntimeError(
            f"k6 exited with code {completed.returncode} for {target_key} "
            f"(C={concurrency}, duration={duration})"
        )

    if cooldown_s > 0:
        print(f"--> Cooling down {cooldown_s}s before the next stage...")
        time.sleep(cooldown_s)

    if is_warmup:
        if threshold_breached:
            print(f"[WARN] k6 thresholds crossed during {target_key} warmup (run kept warming, not recorded)")
        print(f"--> Warmup complete for {target_key} at C={concurrency}")
        return {}

    if threshold_breached:
        print(f"[WARN] k6 thresholds crossed for {target_key} (C={concurrency}); "
              f"recording trial with threshold_breached=true")

    record: Dict[str, Any] = {
        "block": block,
        "target_key": target_key,
        "architecture": TARGETS[target_key]["name"],
        "concurrency": concurrency,
        "trial_number": trial_num,
        "seed": seed,
        "started_at_utc": started_at_utc,
        "threshold_breached": threshold_breached,
    }
    record.update(parse_k6_metrics(output_json))
    return record


def export_raw_trials(
    trials: List[Dict[str, Any]],
    output_json: str,
    output_csv: str,
    append: bool = False,
) -> None:
    """Write the raw trial table (JSON + CSV) with append/dedupe semantics.

    Dedupe key is (block, target_key, concurrency, trial_number); within one
    call the passed trials win slot collisions against rows read back from an
    existing file, so reruns replace their own slots and never duplicate rows.
    """
    for path in (output_json, output_csv):
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)

    merged_trials: List[Dict[str, Any]] = list(trials)
    if append and os.path.exists(output_json):
        try:
            with open(output_json, "r", encoding="utf-8") as f:
                existing = json.load(f).get("trials", [])
            merged_trials = list(existing) + merged_trials
        except (OSError, ValueError) as exc:
            print(f"[WARN] Could not merge existing raw JSON ({exc}); rewriting it")

    trials_by_key: Dict[Tuple[str, str, Any, Any], Dict[str, Any]] = {}
    for trial in merged_trials:
        key = (
            trial.get("block", "gblock1"),
            trial.get("target_key", ""),
            trial.get("concurrency"),
            trial.get("trial_number"),
        )
        trials_by_key[key] = trial
    sorted_trials = [trials_by_key[key] for key in sorted(trials_by_key, key=_dedupe_sort_key)]

    metadata = {
        "model": "sentence-transformers/all-MiniLM-L6-v2",
        "max_batch_size": 32,
        "batch_wait_timeout_s": 0.005,
        "max_concurrent_batches": 1,
        "co_location": f"Single Availability Zone ({RUN_META['az']}), zero cross-AZ transit",
        "total_trials": len(sorted_trials),
        "concurrency_levels": list(RUN_META["concurrency_levels"]),
        "trials_per_level": RUN_META["trials_per_level"],
        "warmup_duration": RUN_META["warmup_duration"],
        "seed": RUN_META["seed"],
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump({"benchmark_metadata": metadata, "trials": sorted_trials}, f, indent=2)
    print(f"[OK] Exported raw JSON results to: {output_json}")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RECORD_KEYS)
        writer.writeheader()
        for trial in sorted_trials:
            writer.writerow({k: trial.get(k, "") for k in RECORD_KEYS})
    print(f"[OK] Exported raw CSV results to: {output_csv}")


def _dedupe_sort_key(key: Tuple[str, str, Any, Any]) -> Tuple[str, str, int, int]:
    block, target_key, concurrency, trial_number = key
    return (
        str(block),
        str(target_key),
        int(concurrency) if concurrency is not None else -1,
        int(trial_number) if trial_number is not None else -1,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the hardened Phase 3 GPU concurrency matrix (gblock1)."
    )
    parser.add_argument("--block", default="gblock1", choices=list(BLOCKS.keys()),
                        help="Benchmark block to run (default: gblock1)")
    parser.add_argument("--targets", default=None,
                        help="Comma-separated subset of the block's target keys")
    parser.add_argument("--concurrencies", default="1,4,8,16,32,64",
                        help="Comma-separated closed-loop concurrency levels")
    parser.add_argument("--trials", type=int, default=5,
                        help="Number of independent trials per arm per concurrency level")
    parser.add_argument("--duration", default="30s", help="Measurement duration per trial")
    parser.add_argument("--warmup-duration", default="15s",
                        help="Unrecorded per-(arm, level) warmup duration")
    parser.add_argument("--skip-warmup", action="store_true", help="Skip all warmup runs")
    parser.add_argument("--inter-trial-cooldown", type=int, default=0,
                        help="Seconds to sleep after every k6 invocation (warmup and trial) "
                             "so back-to-back GPU stages drain thermally; 0 disables sleeping")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for execution order")
    parser.add_argument("--az", default="us-west-2a",
                        help="Availability zone the cluster runs in (recorded in metadata)")
    parser.add_argument("--k6-script", default=DEFAULT_K6_SCRIPT, help="Path to the k6 script")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help="Directory for per-trial k6 summary JSONs and the run log")
    # Required: no default, so a run can never overwrite a historical dataset
    # such as the October results/gpu_embed/ raw files.
    parser.add_argument("--output-raw-json", required=True,
                        help="Path for the aggregated raw trials JSON")
    parser.add_argument("--output-raw-csv", required=True,
                        help="Path for the aggregated raw trials CSV")
    return parser


def main_with_args(argv: List[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        target_keys = resolve_targets(args.block, args.targets)
        levels = parse_concurrencies(args.concurrencies)
    except ValueError as exc:
        parser.error(str(exc))

    if args.trials < 1:
        print("[ERROR] --trials must be >= 1", file=sys.stderr)
        raise SystemExit(2)

    if args.inter_trial_cooldown < 0:
        parser.error("--inter-trial-cooldown must be >= 0")

    RUN_META["az"] = args.az
    RUN_META["seed"] = args.seed
    RUN_META["warmup_duration"] = args.warmup_duration
    RUN_META["trials_per_level"] = args.trials
    RUN_META["concurrency_levels"] = levels

    os.makedirs(args.output_dir, exist_ok=True)
    random.seed(args.seed)

    # Seeded schedule: (arm, level) pairs shuffled; each pair queues its one
    # warmup (unless skipped) followed by its trials in shuffled order, so a
    # pair's warmup always precedes its own trials.
    pairs = [(target_key, level) for level in levels for target_key in target_keys]
    random.shuffle(pairs)

    queue: List[Tuple[str, str, int, int]] = []
    for target_key, level in pairs:
        if not args.skip_warmup:
            queue.append(("warmup", target_key, level, 0))
        trial_nums = list(range(1, args.trials + 1))
        random.shuffle(trial_nums)
        for trial_num in trial_nums:
            queue.append(("trial", target_key, level, trial_num))

    print("=" * 72)
    print(f" Phase 3 GPU Matrix ({args.block}): "
          f"{len(levels)} level(s) x {len(target_keys)} arm(s) x {args.trials} trial(s)")
    print(f" Concurrency levels: {levels}")
    print(f" Warmups: {'skipped' if args.skip_warmup else f'{len(pairs)} interleaved (arm x level)'}"
          f" | Duration: {args.duration} | Seed: {args.seed}"
          f" | Inter-trial cooldown: {args.inter_trial_cooldown}s")
    print("=" * 72)
    print(f" Random order: "
          f"{[f'{kind}:{target_key}/C{level}/n{n}' for kind, target_key, level, n in queue]}")

    log_path = os.path.join(args.output_dir, "run_order.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"seed={args.seed}\nblock={args.block}\nduration={args.duration}\n")
        f.write(f"warmup_duration={args.warmup_duration}\ntrials={args.trials}\n")
        f.write("order\n")
        for kind, target_key, level, n in queue:
            f.write(f"{kind}\t{target_key}\t{level}\t{n}\n")
    print(f"[OK] Run order logged to {log_path}")

    all_trials: List[Dict[str, Any]] = []
    skipped_pairs = set()

    for kind, target_key, level, trial_num in queue:
        target_url = TARGETS[target_key]["url"]
        if kind == "warmup":
            warmup_file = os.path.join(args.output_dir, f"warmup_{target_key}_c{level}.json")
            try:
                run_k6_trial(
                    target_key,
                    target_url,
                    level,
                    args.warmup_duration,
                    warmup_file,
                    0,
                    k6_script_path=args.k6_script,
                    block=args.block,
                    seed=args.seed,
                    is_warmup=True,
                    cooldown_s=args.inter_trial_cooldown,
                )
            except RuntimeError as exc:
                # A target whose warmup fails is skipped for its trials so we
                # never record cold-start-contaminated data; the other pairs
                # continue and remain individually attributable.
                print(f"[WARN] Warmup failed for {target_key} at C={level}: {exc}; "
                      f"skipping its trials")
                skipped_pairs.add((target_key, level))
            continue

        if (target_key, level) in skipped_pairs:
            continue
        trial_file = os.path.join(args.output_dir, f"{target_key}_c{level}_trial_{trial_num}.json")
        record = run_k6_trial(
            target_key,
            target_url,
            level,
            args.duration,
            trial_file,
            trial_num,
            k6_script_path=args.k6_script,
            block=args.block,
            seed=args.seed,
            cooldown_s=args.inter_trial_cooldown,
        )
        all_trials.append(record)
        print(f"--> Recorded {target_key} C={level} trial #{trial_num}: "
              f"p50={record['p50_ms']}ms, p95={record['p95_ms']}ms, "
              f"throughput={record['throughput_rps']} req/s")
        # Export after every recorded trial (append + dedupe: this run wins its
        # own slots) so an abort never discards completed trials.
        export_raw_trials(all_trials, args.output_raw_json, args.output_raw_csv, append=True)

    if skipped_pairs:
        print(f"[WARN] Skipped pairs with failed warmups: {sorted(skipped_pairs)}")
    print(f"\nRun complete: {len(all_trials)} trial(s) recorded -> {args.output_raw_json}")
    return 0


def main() -> None:
    main_with_args(sys.argv[1:])


if __name__ == "__main__":
    main()
