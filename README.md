# Ray Serve Model Serving Benchmark

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **TL;DR Executive Summary:**
> - **Workloads Evaluated:** CPU Tabular Inference (`xgboost`) and Lightweight GPU Embeddings (`all-MiniLM-L6-v2` FP16 on NVIDIA L4).
> - **CPU Routing Overhead (Phase 2, Ray 2.60.0, 31 trials, 0 failures):** against block-matched FastAPI/Uvicorn baselines, Ray Serve's HAProxy ingress on `:8000` (verified live in the request path) adds +0.314 ms p50 / +0.550 ms p99, and direct ingress on `:30000` adds +0.138 ms p50 / +0.137 ms p99. Both pass the absolute <= 3.0 ms gate. The default Python proxy adds +3.517 ms p50 / +4.484 ms p99 and fails it. No Ray arm passes the 10% relative gate at a ~1.1 ms baseline. See the [Phase 2 Report](results/phase2-rerun-2026-11-18-report.md).
> - **GPU Micro-Batching (Phase 3, Ray 2.60.0, 60 trials, 0 failures):** Ray Serve behind its HAProxy ingress (`:8000`) vs a FastAPI/asyncio batching baseline, one NVIDIA L4 each, C=1–64. **Low-load latency PASS** (+0.344 ms p95 at C=1, +0.874 ms at C=4). **C=64 saturation PASS**: Ray Serve p99 70.050 ms vs Direct 132.829 ms. **Goodput FAIL** (≥ 95% only at C=1, 66.16% at C=32). **Batch scaling FAIL** (×15.63 throughput, but p95 33.890 ms at C=32 against a 20 ms ceiling). See the [Phase 3 Report](results/phase3-gpu-rerun-2026-11-19-report.md).
> - **Key Reports:** [Phase 2 CPU Rerun Report](results/phase2-rerun-2026-11-18-report.md) | [Phase 2 Statistical Summary](results/phase2-rerun-2026-11-18/summary_report.md) | [Phase 3 GPU Rerun Report](results/phase3-gpu-rerun-2026-11-19-report.md) | [Phase 3 Statistical Summary](results/phase3-gpu-rerun-2026-11-19/summary.md).
[![Ray](https://img.shields.io/badge/Ray-2.60.0-blue.svg)](https://docs.ray.io/)
[![KubeRay](https://img.shields.io/badge/KubeRay-v1.7.2-blue.svg)](https://ray-project.github.io/kuberay-helm/)
[![k6](https://img.shields.io/badge/k6-v0.54.0-purple.svg)](https://k6.io/)
[![XGBoost](https://img.shields.io/badge/XGBoost-2.1.1-red.svg)](https://xgboost.readthedocs.io/)
[![PyTorch](https://img.shields.io/badge/PyTorch-2.2.0-ee4c2c.svg)](https://pytorch.org/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.34-326ce5.svg)](https://kubernetes.io/)

A rigorous, open-source performance benchmark evaluating the proxy, queueing, and network overhead of **Ray Serve** against high-performance **Direct Kubernetes Serving** baselines (FastAPI + Uvicorn + orjson) across CPU tabular and GPU embedding workloads on strictly matched AWS EKS infrastructure.

---

## Table of Contents

- [Benchmark Goal & Scope](#benchmark-goal--scope)
- [Cluster Topology & Request Path](#cluster-topology--request-path)
- [Phase 2: CPU Tabular Benchmark (XGBoost on c6i.2xlarge)](#phase-2-cpu-tabular-benchmark-xgboost-on-c6i2xlarge)
  - [1. Setup & Configuration Proof](#1-setup--configuration-proof)
  - [2. Serving Overhead vs Block-Matched Baselines](#2-serving-overhead-vs-block-matched-baselines)
  - [3. HAProxy-Only Control](#3-haproxy-only-control)
  - [4. Per-Arm Summary & Transit](#4-per-arm-summary--transit)
  - [5. Attributing the HAProxy Arm](#5-attributing-the-haproxy-arm)
  - [6. Limitations & Disclosures](#6-limitations--disclosures)
- [Phase 3: GPU Embeddings Benchmark (Sentence-Transformers on NVIDIA L4)](#phase-3-gpu-embeddings-benchmark-sentence-transformers-on-nvidia-l4)
  - [1. Setup & Gate Proof](#1-setup--gate-proof)
  - [2. Gate Verdicts](#2-gate-verdicts)
  - [3. Results by Concurrency Level](#3-results-by-concurrency-level)
  - [4. Charts](#4-charts)
  - [5. Limitations & Disclosures](#5-limitations--disclosures)
- [How to Reproduce](#how-to-reproduce)
  - [Reproducing Phase 2 (CPU Tabular)](#reproducing-phase-2-cpu-tabular)
  - [Reproducing Phase 3 (GPU Embeddings)](#reproducing-phase-3-gpu-embeddings)
- [Pinned Environment & Versions](#pinned-environment--versions)
- [Future Roadmap (Phases 4+)](#future-roadmap-phases-4)
- [Repository Structure](#repository-structure)
- [License](#license)

---

## Benchmark Goal & Scope

The objective of this benchmark suite is to empirically evaluate whether Ray Serve's platform capabilities—multi-model orchestration, dynamic micro-batching, and logical queue-based autoscaling—justify its reverse-proxy routing and serialization costs compared to native Kubernetes serving.

We evaluate two distinct operational regimes:
1. **Sub-Millisecond CPU Tabular Inference (Phase 2):** Minimizes model execution time (~0.5–0.8 ms) to isolate bare reverse-proxy and network routing hops.
2. **Dynamic GPU Micro-Batching (Phase 3):** Evaluates GPU Tensor Core scaling, queue accumulation, and overload protection under concurrency levels from 1 to 64.

---

## Cluster Topology & Request Path

Full provenance, port citations, and capture commands: [docs/topology-and-request-path.md](docs/topology-and-request-path.md).

```
us-west-2a (single AZ)
┌────────────────────────────────────────────────────────────────────────┐
│ head-1       c6i.2xlarge   bench-role=head                            │
│              KubeRay operator, Ray head (one RayService at a time)    │
│                                                                       │
│ worker-w1    c6i.2xlarge   bench-role=worker  bench-slot=w1            │
│ worker-w2    c6i.2xlarge   bench-role=worker  bench-slot=w2           │
│              one server pod per node (anti-affinity)                  │
│                                                                       │
│ loadgen-1    c6i.2xlarge   bench-role=loadgen                         │
│              k6 driver only                                           │
└────────────────────────────────────────────────────────────────────────┘
```

Blocks are measured one deployment at a time: Block 1 (Direct + HAProxy RayService) measures the Direct, HAProxy `:8000`, and direct-ingress `:30000` arms; Block 2 (Direct + Python-proxy RayService) measures Direct and the Python proxy; Block 3 is a single-replica local control.

**Port roles (Ray 2.60.0):** `:8000` is the Ray Serve HTTP ingress — HAProxy when `RAY_SERVE_ENABLE_HA_PROXY=1`, otherwise the Python proxy. `:30000–31000` is the direct-ingress HTTP server inside each ingress-deployment replica.

**Request paths:**

- **Direct:** k6 pod → ClusterIP `:8000` → Uvicorn pod → FastAPI → model → response.
- **Ray default Python proxy:** k6 pod → `ray-python-proxy-svc :8000` → per-pod Python proxy → Ray actor RPC → replica.
- **Ray HAProxy:** k6 pod → `ray-haproxy-svc :8000` → **HAProxy `:8000`** → replica's direct-ingress server (`:300xx`, leastconn across replicas).
- **Ray direct ingress:** k6 pod → `ray-direct-ingress-svc :8000` → **targetPort 30000** → replica's own HTTP server (no proxy, no HAProxy).

ClusterIP balances per TCP connection, and k6 uses keep-alive, so each VU stays pinned to one backend for the life of its connection.

---

## Phase 2: CPU Tabular Benchmark (XGBoost on c6i.2xlarge)

Evaluates an open-source **XGBoost Binary Classifier** (50 float features, 100 trees, depth 6) at 50 req/s over **31 trials in 3 blocks** (46,519 requests, 0.00% failures) with 15 s unrecorded warmups per arm. Full statistics: [Phase 2 Report](results/phase2-rerun-2026-11-18-report.md), [Statistical Summary](results/phase2-rerun-2026-11-18/summary_report.md). Raw data and per-block run logs are committed under `results/phase2-rerun-2026-11-18/`.

### 1. Setup & Configuration Proof

- **Stack:** Ray **2.60.0** / KubeRay **1.7.2** / EKS **1.34**, 4 × `c6i.2xlarge` pinned to a single AZ (`us-west-2a`), worker `rayStartParams num-cpus: "1"` so replicas spread one-per-worker.
- **Serve config:** `RAY_SERVE_ENABLE_HA_PROXY=1` + `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`, `max_ongoing_requests: 1000`, `proxy_location: EveryNode`, 2 replicas (1 in the local control).
- **HAProxy verified in the path, not assumed:** preflight gates committed as evidence — 3 HAProxyManager actors (head + both replica-hosting workers), one replica per worker each binding `:30000`, HAProxy request counters rising across the gate's requests (352 → 374), 10/10 inferences through each arm, and live pod env showing `RAY_SERVE_ENABLE_HA_PROXY=1` / `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`.

### 2. Serving Overhead vs Block-Matched Baselines

Each Ray arm is compared with the Direct Serving (Uvicorn) trials of the **same block** (median deltas, seeded bootstrap 95% CIs):

| Arm | Metric | Direct baseline | Arm | Delta [95% CI] | Relative | Abs gate ≤ 3.0 ms | Rel gate ≤ 10% |
|---|---|---|---|---|---|---|---|
| **Ray Serve (Direct Ingress :30000)** | p50 | 1.074 ms | 1.212 ms | **+0.138 ms** [+0.031, +0.232] | +12.8% | PASS | FAIL |
| | p95 | 1.322 ms | 1.419 ms | +0.097 ms [+0.052, +0.119] | +7.3% | PASS | PASS |
| | p99 | 1.478 ms | 1.615 ms | **+0.137 ms** [+0.085, +0.187] | +9.3% | PASS | PASS |
| **Ray Serve (HAProxy :8000)** | p50 | 1.074 ms | 1.388 ms | **+0.314 ms** [+0.219, +0.408] | +29.2% | PASS | FAIL |
| | p95 | 1.322 ms | 1.845 ms | **+0.523 ms** [+0.478, +0.570] | +39.6% | PASS | FAIL |
| | p99 | 1.478 ms | 2.028 ms | **+0.550 ms** [+0.473, +0.623] | +37.2% | PASS | FAIL |
| **Ray Serve (Default Python Proxy :8000)** | p50 | 1.138 ms | 4.655 ms | **+3.517 ms** [+2.969, +3.983] | +309.1% | **FAIL** | FAIL |
| | p95 | 1.342 ms | 5.752 ms | **+4.410 ms** [+4.321, +4.540] | +328.6% | **FAIL** | FAIL |
| | p99 | 1.579 ms | 6.063 ms | **+4.484 ms** [+4.396, +4.778] | +284.0% | **FAIL** | FAIL |

**Reading the gates:**

- **HAProxy and direct ingress pass the 3.0 ms absolute gate** on every percentile. At a ~1.1 ms baseline they fail the 10% relative gate — at this scale a fraction of a millisecond is a double-digit percentage.
- **The default Python proxy is the expensive component**, roughly ten times HAProxy: failing even the absolute gate (~+3.5 ms at p50). Prefer HAProxy ingress (`RAY_SERVE_ENABLE_HA_PROXY=1`) when serving latency matters.
- Goodput held at the 50 req/s offer on every arm (per-arm medians 50.00–50.03 req/s, 0.00% failures).

### 3. HAProxy-Only Control

A single-replica local pair (both arms served from the same worker, cross-node balancing removed from the comparison) measures the HAProxy process's own cost:

| Metric | Direct ingress local (median) | HAProxy local (median) | Delta [95% CI] |
|---|---|---|---|
| **p50** | 1.271 ms | 1.412 ms | **+0.141 ms** [+0.118, +0.159] |
| **p95** | 1.477 ms | 1.589 ms | **+0.112 ms** [+0.069, +0.147] |
| **p99** | 1.779 ms | 1.810 ms | +0.031 ms [-0.033, +0.124] — CI crosses zero |

Diagnostic only (n=3 per arm, sequential sub-blocks, 1 replica); not part of the headline gate table.

### 4. Per-Arm Summary & Transit

Medians over each arm's trials (n=10 Direct pooled across blocks; n=5 per Ray arm; see the [summary](results/phase2-rerun-2026-11-18/summary_report.md) for IQRs, t-CIs, and all 31 individual trials):

| Arm | Goodput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Transit (p50) |
|---|---|---|---|---|---|
| **Direct Serving (Uvicorn)** | 50.03 | **1.131** | **1.333** | **1.513** | 0.555 ms |
| **Ray Serve (Direct Ingress :30000)** | 50.03 | **1.212** | **1.419** | **1.615** | 0.680 ms |
| **Ray Serve (HAProxy :8000)** | 50.03 | **1.388** | **1.845** | **2.028** | 0.877 ms |
| **Ray Serve (Default Python Proxy :8000)** | 50.03 | **4.655** | **5.752** | **6.063** | 2.774 ms |

### 5. Attributing the HAProxy Arm

Two subtractions locate the parts (indicative; derived from cross-block summary medians):

- **Ray Serve (HAProxy `:8000`) vs Direct (Uvicorn): +0.314 ms p50** — the full cost of putting Ray Serve with HAProxy ingress in the path.
- **HAProxy arm minus direct-ingress arm (both Ray arms; +0.176 ms p50 / +0.426 ms p95 / +0.413 ms p99)** — the HAProxy ingress plus cross-node balancing on top of Ray Serve's replica stack. The **share depends on the percentile**: at the median, HAProxy itself is most of this difference (about 80%; about 45% of the full `:8000` delta), while in the tail cross-node balancing dominates (HAProxy is about 26% at p95 and about 8% at p99).

### 6. Limitations & Disclosures

- 31 short trials (30 s each; 5 per arm, 3 for the control) at a single 50 req/s rate; CIs are percentile bootstraps on those n.
- The local control has no same-block Direct baseline, ran 1 replica vs 2 in Block 1, and its two arms ran as **sequential sub-blocks**, so time drift is not separated from the HAProxy effect. The subtraction mixes blocks and is labeled indicative rather than measured.
- Ray workers got 2 vCPU vs Direct's 1 vCPU (deliberate: raylet + HAProxy + actor vs model-only); disclosed asymmetry, not a hidden advantage.
- The Python-proxy arm sets `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` — the one deviation from the throughput bundle, required to keep per-node Python proxies comparable to the HAProxy arm.
- Direct and Ray worker pods share the two worker nodes; Direct's baseline drifted between blocks (server_total +0.11 ms block1→block2); the Python proxy routes to a local replica by default while HAProxy balances across nodes; only the Ray app records a `Histogram.observe` in the request path.
- Timer boundaries are aligned across Direct and Ray (`t0` before body read; total excludes response serialization). Arm B's server-side total includes the proxy-to-replica transfer, so its transit column understates the proxy cost.
- Scope: one small XGBoost model on one instance family at one load point. Do not generalize to heavier workloads where compute dominates.
- Trial seeds (42/43/44) and each block's execution order are recorded in the report and in `results/phase2-rerun-2026-11-18/run-logs/`. Per-trial timestamps and pod-to-node mapping for the measured deployments were not captured.

---

## Phase 3: GPU Embeddings Benchmark (Sentence-Transformers on NVIDIA L4)

Compares **Direct Serving (GPU)** (FastAPI + PyTorch with an asyncio micro-batching queue) against **Ray Serve (HAProxy :8000)** (`@serve.batch` behind Ray 2.60's HAProxy ingress). Both serve `sentence-transformers/all-MiniLM-L6-v2` in FP16 on one NVIDIA L4 each. The run covered **60 trials in one block**: 2 arms × 6 closed-loop concurrency levels (C=1, 4, 8, 16, 32, 64) × 5 trials of 30 s, 1,683,922 requests, 0 failed. Full statistics: [Phase 3 Report](results/phase3-gpu-rerun-2026-11-19-report.md), [Statistical Summary](results/phase3-gpu-rerun-2026-11-19/summary.md). Raw data, the run log, and preflight/teardown evidence are committed under `results/phase3-gpu-rerun-2026-11-19/`.

### 1. Setup & Gate Proof

- **Stack:** Ray **2.60.0** / KubeRay **1.7.2** / EKS **1.34**, NVIDIA device plugin v0.17.1, `torch==2.2.0` in both arms. Four nodes in `us-west-2a`: a `c6i.2xlarge` head, two `g6.xlarge` GPU nodes (Direct on one, the Ray worker on the other), and a `c6i.2xlarge` k6 load generator.
- **Serve config:** `RAY_SERVE_ENABLE_HA_PROXY=1` + `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`, `max_ongoing_requests: 1000`, 1 replica. Both arms batch with `max_batch_size=32` and a 5 ms wait (untuned). Ray Serve runs `max_concurrent_batches=1`.
- **Verified before load, not assumed** ([`gpu-preflight.log`](results/phase3-gpu-rerun-2026-11-19/preflight/gpu-preflight.log)): both serving pods see an NVIDIA L4 with CUDA available. HAProxy request counters rose 170 → 190 across 10/10 requests through `ray-ha-svc`. There are 2 HAProxyManager actors and the throughput flags are live in the running head and worker containers. The cluster was deleted before analysis (`all cluster resources were deleted`, 08:30:45 UTC, [`teardown.log`](results/phase3-gpu-rerun-2026-11-19/preflight/teardown.log)).

### 2. Gate Verdicts

| Gate | Criterion | Verdict | Numbers |
|---|---|---|---|
| **Low-load latency** | C=1, C=4: Δp95 ≤ 3.0 ms or ≤ 10% | **PASS** | +0.344 ms (+3.2%) at C=1; +0.874 ms (+7.6%) at C=4 |
| **Goodput** | Ray Serve ≥ 95% of Direct at every level | **FAIL** | 98.39% at C=1; 94.37% at C=4; 89.74% / 90.54% / 66.16% / 89.73% at C=8 / 16 / 32 / 64 |
| **Batch scaling** | ×3 throughput C=1 → C=32 **and** p95 ≤ 20 ms at C=32 | **FAIL** | ×15.63 (PASS), p95 33.890 ms (FAIL) |
| **Saturation (C=64)** | Ray Serve p99 ≤ Direct p99 + 3.0 ms | **PASS** | 70.050 ms vs 132.829 ms; Δp99 −62.779 ms [−66.202, −60.983] |

**Reading the gates:**

- **At low load, Ray Serve's HAProxy path costs under a millisecond at p95.** The gap widens as batches fill: Ray Serve's p99 is +9.7 ms at C=8 and +21.2 ms at C=16, and its goodput falls below 95% from C=4 on.
- **Direct peaks higher:** 2,163 req/s at C=32 against Ray Serve's best of 1,494 req/s at C=64. Direct then drops to 1,665 req/s at C=64.
- **Ray Serve keeps a much shorter tail at saturation.** At C=64 its p99 is about half of Direct's, with both arms offloading encoding off the event loop.
- **Direct has a hidden tail at C=32.** p95 is about 18.4 ms but p99 is about 106 ms in all 5 trials. k6's flag only checks p95 < 25 ms, so it doesn't catch this.
- **15 of 60 trials crossed k6's p95 < 25 ms budget** (Direct C=64, and Ray Serve at C=32 and C=64, 5 trials each). None had failed requests. All 15 are kept in the statistics.

### 3. Results by Concurrency Level

Medians over 5 trials per cell. Throughput is the per-level mean. The [summary](results/phase3-gpu-rerun-2026-11-19/summary.md) has IQRs, t-CIs, and bootstrap CIs on every delta.

| Level | Direct req/s | Ray Serve req/s | Direct p95 | Ray Serve p95 | Direct p99 | Ray Serve p99 |
|---|---|---|---|---|---|---|
| C=1 | 93.02 | 91.52 | 10.851 ms | 11.195 ms | 11.507 ms | 12.330 ms |
| C=4 | 349.00 | 329.33 | 11.552 ms | 12.426 ms | 12.101 ms | 13.746 ms |
| C=8 | 698.32 | 626.68 | 11.441 ms | 13.266 ms | 12.284 ms | 21.957 ms |
| C=16 | 1,196.41 | 1,083.25 | 13.366 ms | 16.263 ms | 14.428 ms | 35.580 ms |
| C=32 | 2,162.53 | 1,430.80 | 18.334 ms | 33.890 ms | 106.011 ms | 43.056 ms |
| C=64 | 1,664.73 | 1,493.79 | 122.541 ms | 60.999 ms | 132.829 ms | 70.050 ms |

### 4. Charts

<p align="center">
  <img src="docs/images/phase3_gpu_rerun_throughput_scaling.svg" alt="Throughput vs concurrency for Direct Serving (GPU) and Ray Serve (HAProxy :8000), C=1 to C=64, with per-trial points and 95% CI bands" width="760">
</p>

<p align="center">
  <img src="docs/images/phase3_gpu_rerun_latency_tail.svg" alt="p95 and p99 latency vs concurrency for both arms, with per-trial points and 95% CI bands" width="950">
</p>

### 5. Limitations & Disclosures

- 5 trials per (arm × level), 30 s each, closed-loop at fixed concurrency, one 15 s warmup per (arm × level). CIs are computed on n = 5.
- Scope: one small model (22M parameters) on one GPU type, one replica per arm. Batching parameters are untuned (32 / 5 ms, serial batches on Ray Serve). Different settings could change the C=4–C=32 picture.
- The Direct baseline is a hand-built asyncio queue, not Triton or TorchServe. Both arms offload encoding with `asyncio.to_thread`.
- Nodes are separated by type and role only. Each arm had its own GPU node, so per-host variance is not separated from the arm effect.
- On the Ray worker, CUDA was confirmed via `nvidia-smi` plus app readiness, not an in-container torch check.
- Seed 42 on every trial, a seeded shuffled run order with a 3 s cooldown, logged in [`run-logs/gblock1-run.log`](results/phase3-gpu-rerun-2026-11-19/run-logs/gblock1-run.log).

The October snapshot (`results/phase3-gpu-embeddings-report.md`, `results/gpu_embed/`) is retained unchanged for provenance.

---

## How to Reproduce

### Reproducing Phase 2 (CPU Tabular)

#### 1. Create the cluster and install KubeRay

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r model/requirements.txt -r requirements-dev.txt
python model/train.py --seed 42   # trains the 50-feature XGBoost model + 1,000 sample payloads

export AWS_PROFILE=<aws-profile>  # requires EKS/EC2/IAM permissions
eksctl create cluster -f infra/phase2-rerun/eksctl-cluster.yaml --timeout 40m
aws eks update-kubeconfig --name ray-serve-phase2-rerun --region us-west-2 --profile "$AWS_PROFILE" --kubeconfig ~/.kube/ray-serve-phase2-rerun
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.2 -n kuberay --create-namespace --set nodeSelector.bench-role=head
```

#### 2. Deploy a block and run the preflight gate

```bash
kubectl apply -f benchmarks/phase2-cpu-rerun/00-namespace.yaml   # if not applied via manifest below
kubectl create configmap direct-serving-code --from-file=direct_app.py=k8s/app/direct_app.py -n model-serving-benchmark
kubectl create configmap ray-serve-code --from-file=ray_serve_app.py=k8s/app/ray_serve_app.py -n model-serving-benchmark
kubectl create configmap benchmark-model-artifact --from-file=model.json=model/artifacts/model.json -n model-serving-benchmark

kubectl apply -f benchmarks/phase2-cpu-rerun/01-direct-serving.yaml
kubectl apply -f benchmarks/phase2-cpu-rerun/03-ray-haproxy.yaml   # Block 1
KUBECTL=$(command -v kubectl) bash benchmarks/phase2-cpu-rerun/preflight.sh haproxy   # must exit 0 before any load
```

No load runs unless the gate passes. Full block/redeployment walkthrough (including the Block 3 local control and the Block 2 Python-proxy swap): [`benchmarks/phase2-cpu-rerun/README.md`](benchmarks/phase2-cpu-rerun/README.md); cluster teardown and leak checks: [`infra/phase2-rerun/README.md`](infra/phase2-rerun/README.md).

#### 3. Run the blocks and analyze

Run one block per deployment, in the order 1, 3, 2 (runner paths are required — there are no output defaults):

```bash
# in-cluster from the loadgen driver (benchmarks/phase2-cpu-rerun/04-loadgen-driver.yaml)
python3 /workspace/loadtest/run_matrix.py --block block1 --trials 5 --rate-rps 50 --duration 30s \
  --warmup-duration 15s --seed 42 \
  --output-dir results/phase2-rerun-2026-11-18/block1 \
  --output-raw-json results/phase2-rerun-2026-11-18/raw_trials.json \
  --output-raw-csv results/phase2-rerun-2026-11-18/raw_trials.csv \
  --append --az us-west-2a
# block3: --block block3 --trials 3 --seed 43 --output-dir .../block3   (same raw paths, --append)
# block2: --block block2 --trials 5 --seed 44 --output-dir .../block2   (same raw paths, --append)

python3 results/analyze.py \
  --data results/phase2-rerun-2026-11-18/raw_trials.json \
  --output results/phase2-rerun-2026-11-18/summary_report.md
```

---

### Reproducing Phase 3 (GPU Embeddings)

#### 1. Create the GPU cluster

Cluster definition, kubeconfig, NVIDIA device plugin, KubeRay install, and the delete/leak-check walkthrough: [`infra/phase3-gpu-rerun/README.md`](infra/phase3-gpu-rerun/README.md) (4 nodes in `us-west-2a`: a `c6i.2xlarge` head, two `g6.xlarge` GPU nodes, and a `c6i.2xlarge` load generator).

#### 2. Deploy both arms and run the preflight gate

Follow [`benchmarks/phase3-gpu-rerun/README.md`](benchmarks/phase3-gpu-rerun/README.md) (ConfigMaps, `00`–`03` manifests). No load runs unless the gate passes:

```bash
NS=model-serving-benchmark KUBECTL=kubectl \
  bash benchmarks/phase3-gpu-rerun/preflight.sh gpu
```

#### 3. Measure, tear down, then analyze

The `gblock1` matrix (6 levels × 2 arms × 5 trials, seed 42) runs in-cluster from the driver pod via `benchmarks/phase3-gpu-rerun/loadgen/run_gpu_matrix.py`. Delete the cluster as soon as the raw data is copied out. Analysis and charts are offline:

```bash
python3 results/analyze_gpu.py \
  --data results/phase3-gpu-rerun-2026-11-19/phase3-raw.json \
  --output results/phase3-gpu-rerun-2026-11-19/summary.md
python3 benchmarks/phase3-gpu-rerun/make_charts.py \
  --data results/phase3-gpu-rerun-2026-11-19/phase3-raw.json
```

---

## Pinned Environment & Versions

| Component | Pinned Version | Role / Description |
|---|---|---|
| **Ray / Ray Serve** | `2.60.0` | Distributed serving runtime (HAProxy ingress on `:8000` under `RAY_SERVE_ENABLE_HA_PROXY=1`) |
| **ray-haproxy** | `2.8.25` | HAProxy binary bundled with the Ray image for its HAProxy ingress; verified live on `:8000` |
| **KubeRay Operator** | `v1.7.2` | Kubernetes operator managing `RayService` CRDs |
| **k6** | `v0.54.0` | Open-loop load generator (`constant-arrival-rate` & VU stages) |
| **XGBoost** | `2.1.1` | CPU tabular runtime (`tree_method: hist`) |
| **PyTorch** | `2.2.0` (CUDA 12.1) | GPU Tensor Core runtime (`torch.float16`) |
| **Sentence-Transformers** | `2.7.0` | Embedding pipeline (`all-MiniLM-L6-v2`, 384-dim) |
| **FastAPI / Uvicorn** | `0.110.0` / `0.28.0` | Direct serving web framework and ASGI server |
| **orjson** | `3.9.0` | High-performance C-based JSON serialization |
| **Kubernetes** | `1.34` | Container orchestration platform |
| **Python** | `3.11` | Application runtime |
| **AWS CPU Compute** | `c6i.2xlarge` | Intel Xeon 8375C @ 2.90GHz (8 vCPUs, 16 GiB RAM), single AZ us-west-2a |
| **AWS GPU Compute** | `g6.xlarge` | NVIDIA L4 24GB Tensor Core GPU (4 vCPUs, 16 GiB RAM) |

---

## Future Roadmap (Phases 4+)

- [x] **Phase 1: Open-Source CPU XGBoost Tabular Baseline**
- [x] **Phase 2: CPU Saturation & 4-Arm Routing Comparison** (Direct vs. Ray Default Proxy vs. Ray HAProxy vs. Ray Direct Ingress, plus a local HAProxy control) — Ray 2.60.0 / KubeRay 1.7.2, HAProxy verified in the path.
- [x] **Phase 3: GPU Dynamic Micro-Batching** (`sentence-transformers/all-MiniLM-L6-v2` on NVIDIA L4, Direct vs. Ray Serve HAProxy `:8000`): multi-trial matrix measured; saturation gate resolved (PASS). Low-load latency PASS; goodput and batch-scaling FAIL.
- [ ] **Phase 4: Large Language Model Serving (vLLM + Llama 3.1 8B)** — TTFT, TPOT, and official Ray LLM Perf benchmark methodology.
- [ ] **Phase 5: Multi-Model Orchestration & Resource Contention** — Composite DAG pipelines combining CPU tabular preprocessing, GPU vector embeddings, and LLM generative reranking under shared cluster contention.
- [ ] **Phase 6: Elastic Autoscaling & Cloud Economics** — Cost per 1,000 requests and cost per million tokens comparing Ray Serve logical queue autoscaling against native Kubernetes HPA.

---

## Repository Structure

```
ray-serve-model-serving-benchmark/
├── LICENSE                      # MIT License
├── README.md                    # Benchmark overview, headline results, & guides
├── requirements-dev.txt         # pytest / PyYAML for the manifest-invariant tests
├── docs/
│   ├── plan.md                  # Original benchmark plan & methodology
│   ├── topology-and-request-path.md # Cluster topology, port roles, and request paths
│   ├── phase3-gpu-embeddings-plan.md # Phase 3 GPU embeddings plan
│   ├── superpowers/             # Design/spec/plan documents for the Phase 2 and Phase 3 reruns
│   └── images/                  # Benchmark vector charts
├── infra/phase2-rerun/
│   ├── eksctl-cluster.yaml      # 4-node single-AZ EKS cluster definition
│   └── README.md                 # Create / kubeconfig / delete walkthrough
├── infra/phase3-gpu-rerun/       # GPU cluster (2× g6.xlarge), NVIDIA device plugin, walkthrough
├── benchmarks/
│   ├── phase2-cpu-rerun/         # Phase 2 rerun manifests & preflight gate
│   │   ├── 00..04-*.yaml         # namespace, Direct, Python proxy, HAProxy+local driver
│   │   ├── preflight.sh          # HAProxy/runtime verification gate (must pass before load)
│   │   └── README.md             # Block-by-block deployment + reproduce steps
│   ├── phase3-gpu-rerun/         # Phase 3 manifests, GPU preflight gate, matrix runner, make_charts.py
│   └── gpu-embed/                # GPU serving app sources (direct/app.py, ray/serve_embed.py), fixtures
├── model/
│   ├── train.py                  # Seeded synthetic XGBoost generator
│   └── artifacts/model.json      # Pinned model weights
├── k8s/                          # Serving apps (direct_app.py, ray_serve_app.py) + original manifests
├── loadtest/
│   ├── k6_benchmark.js           # k6 constant-arrival-rate script
│   ├── run_matrix.py             # Block-aware multi-trial runner (required output paths)
│   └── fixtures/sample_payloads.jsonl
├── tests/                        # Manifest invariants, timer alignment, runner & analyzer tests
└── results/
    ├── analyze.py                # Block-aware statistical analysis (required --data/--output)
    ├── analyze_gpu.py            # Phase 3 GPU analysis + four gates (required --data/--output)
    ├── phase2-rerun-2026-11-18-report.md   # Phase 2 report (latest run)
    ├── phase3-gpu-rerun-2026-11-19-report.md # Phase 3 report (latest run)
    ├── phase2-rerun-2026-11-18/            # Phase 2 raw data, summary, run logs, preflight evidence
    └── phase3-gpu-rerun-2026-11-19/        # Phase 3 raw data, summary, run log, preflight/teardown evidence
```

The original October 2026 Phase 2 dataset and reports (`results/raw_trials.{json,csv}`, `results/summary_report.md`, `docs/report.md`) are retained in the repository for provenance, unchanged apart from the earlier port-role label correction; all Phase 2 conclusions should be drawn from the rerun artifacts linked above. The legacy deployment manifests under `k8s/` and its README document that original setup.

---

## License

This project is licensed under the [MIT License](LICENSE).
