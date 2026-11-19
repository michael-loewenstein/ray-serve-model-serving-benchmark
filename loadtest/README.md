# Load Testing Suite with k6

This directory provides the open-loop benchmark harness using [k6](https://k6.io/) to measure latency and throughput with high statistical rigor.

## Workload Model

- **Executor:** `constant-arrival-rate`
- **Rate:** 50 req/s (default)
- **Duration:** 30s per trial (nominal 1,500 requests per trial)
- **Pre-Allocated VUs:** 25
- **Max VUs:** 100
- **Traffic Nature:** Open-loop arrival (independent of server response time, avoiding closed-loop coordinated omission bias).

## IMPORTANT: Network Topology Requirement

> **Do NOT run load tests via `kubectl port-forward`.**  
> Port-forwarding routes traffic through the Kubernetes API server control plane and workstation network interfaces, injecting multi-millisecond connection queuing and latency jitter. Tests must be run **in-cluster** (via `k8s/04-loadgen-job.yaml`) or from a host co-located in the same AWS Availability Zone as the serving pods (the 2026-11-18 rerun used `us-west-2a`).

## Prerequisites

Install `k6` (pinned: `v0.54.0`):
```bash
# macOS
brew install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

## Running the Automated Randomized Matrix

`run_matrix.py` is block-aware: it runs one benchmark block at a time, chosen by the required `--block` flag:

- `--block block1` — `direct`, `ray_haproxy`, `ray_direct`
- `--block block2` — `direct`, `ray_python`
- `--block block3` — `ray_haproxy_local`, `ray_direct_local`

`--targets` optionally narrows a run to a comma-separated subset of the block (e.g. `--targets direct,ray_haproxy`); values outside the block are rejected.

To eliminate temporal bias (such as cloud noisy neighbor shifts or thermal throttling), the runner first performs one unrecorded warmup pass per target (`--warmup-duration`, default `15s`), then shuffles the execution order of the measured trials and runs `--trials` independent trials per target:

```bash
python3 loadtest/run_matrix.py --block block1 --trials 5 --rate-rps 50 --duration 30s --warmup-duration 15s --output-dir results/phase2-rerun-2026-11-18/block1 --output-raw-json results/phase2-rerun-2026-11-18/raw_trials.json --output-raw-csv results/phase2-rerun-2026-11-18/raw_trials.csv --append
```

`--append` merges the block's trials into an existing raw results file (deduplicating by `block`/target/trial number, so re-running a block replaces its own slots without touching other blocks). Run the three blocks against the same raw file in turn to build the full dataset.

`--output-raw-json` and `--output-raw-csv` are required; there are no default paths, so a run cannot overwrite the historical October dataset (`results/raw_trials.{json,csv}`).

The raw files are re-exported after every recorded trial (same merge/dedupe rules), so an abort keeps the trials completed so far. If k6 exits with code 99 (a `k6_benchmark.js` threshold was crossed), the run is still a valid measurement: the trial is recorded with `"threshold_breached": true` (otherwise `false`). Any other non-zero k6 exit aborts the matrix and exits non-zero.

Outputs:
- Per-target warmup summaries as `warmup_<target>.json` and individual trial JSON summaries in `--output-dir`
- Aggregated raw dataset in the raw JSON file (ready for `results/analyze.py`), with `block`, `seed`, `started_at_utc` (ISO-8601, UTC) and `threshold_breached` fields per trial, and `trials_per_block` metadata (distinct trial numbers per block, i.e. runs per arm per block). The 2026-11-18 rerun dataset predates the `seed`/`started_at_utc`/`threshold_breached` fields.
- Aggregated tabular CSV alongside the raw JSON
