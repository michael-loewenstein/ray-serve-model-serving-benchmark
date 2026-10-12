# Ray Serve Model Serving Benchmark

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Ray](https://img.shields.io/badge/Ray-2.55.1-blue.svg)](https://docs.ray.io/)
[![KubeRay](https://img.shields.io/badge/KubeRay-v1.4.2-blue.svg)](https://ray-project.github.io/kuberay-helm/)
[![k6](https://img.shields.io/badge/k6-v0.54.0-purple.svg)](https://k6.io/)
[![XGBoost](https://img.shields.io/badge/XGBoost-2.1.1-red.svg)](https://xgboost.readthedocs.io/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.30-326ce5.svg)](https://kubernetes.io/)

A rigorous, open-source performance benchmark evaluating the proxy and network overhead of **Ray Serve** against a high-performance **Direct Kubernetes Serving** baseline (FastAPI + Uvicorn + orjson) on strictly matched AWS EKS infrastructure (`c6i.2xlarge`).

---

## Goal

The objective of this benchmark is to empirically measure whether Ray Serve's architectural benefits—multi-model orchestration, dynamic batching, and logical queue-based autoscaling—justify its reverse-proxy routing cost compared to native Kubernetes serving.

By evaluating a sub-millisecond CPU tabular inference workload (XGBoost binary classifier, 50 features, 100 trees, depth 6), server execution time is minimized (~0.5–0.8 ms), isolating socket, event loop, serialization, and proxy hops.

---

## Headline Results

All measurements reflect **15 independent randomized trials** (5 per architecture, 22,508 total requests) at 50 requests/second on 100% matched AWS `c6i.2xlarge` hardware with single-zone co-location (`us-east-1d`).

### 1. Absolute Latency First (Worker HAProxy vs. Direct Baseline)

When utilizing Ray Serve's worker-colocated HAProxy routing (`:30000`), absolute latency overhead is negligible:

| Metric | Direct Serving Baseline | Ray Serve (Worker HAProxy :30000) | Absolute Overhead | Absolute Gate ($\le 3.0\text{ ms}$) |
|---|---|---|---|---|
| **p50 (Median)** | **0.973 ms** (Mean: 1.001 ± 0.083) | **1.274 ms** (Mean: 1.315 ± 0.097) | **+0.301 ms** (~300 µs) | **PASS** |
| **p90** | **1.151 ms** (Mean: 1.183 ± 0.110) | **1.768 ms** (Mean: 1.761 ± 0.087) | **+0.617 ms** | **PASS** |
| **p95** | **1.246 ms** (Mean: 1.302 ± 0.170) | **1.856 ms** (Mean: 1.835 ± 0.087) | **+0.610 ms** | **PASS** |
| **p99** | **1.444 ms** (Mean: 1.533 ± 0.242) | **2.595 ms** (Mean: 2.527 ± 0.702) | **+1.151 ms** (~1.15 ms) | **PASS** |

*By comparison, Ray Serve's default Python Proxy (head node) exhibited **+0.505 ms** at p50 (Median: 1.478 ms, Mean: 1.530 ± 0.145) and **+3.421 ms** at p99 (Median: 4.865 ms, Mean: 6.320 ± 7.338).*

### 2. Relative Overhead Second

| Metric | Direct Serving Baseline | Ray Serve (Worker HAProxy :30000) | Relative Overhead | Relative Gate ($\le 10\%$) |
|---|---|---|---|---|
| **p50** | 0.973 ms | 1.274 ms | **+30.9%** | **FAIL** |
| **p90** | 1.151 ms | 1.768 ms | **+53.6%** | **FAIL** |
| **p95** | 1.246 ms | 1.856 ms | **+49.0%** | **FAIL** |
| **p99** | 1.444 ms | 2.595 ms | **+79.7%** | **FAIL** |

### Contextual Analysis & Scoped Workload Takeaway

Direct Serving on bare Linux sockets achieves an ultra-low baseline ($p50 = 0.973\text{ ms}$, $p99 = 1.444\text{ ms}$). Because the baseline latency is sub-millisecond:
- An incremental ~300 µs reverse-proxy hop mathematically constitutes a **+30.9% relative delta**.
- An added 1.15 ms at p99 constitutes a **+79.7% relative delta**.

**Scoped Takeaway for This Workload:**
This benchmark intentionally minimizes model compute (~0.5–0.8 ms inference) to stress reverse-proxy and network routing hops. In this sub-millisecond regime, an absolute overhead of ~0.30 ms (p50) and ~1.15 ms (p99) represents a large relative increase (+31% to +80%) over raw Linux sockets.
- For ultra-low latency CPU tabular workloads (<2 ms total SLA), teams must weigh this ~0.3 ms proxy cost against Ray Serve's operational benefits.
- For heavier workloads where inference compute dominates (e.g. 10–500+ ms for neural nets or LLMs), this fixed ~0.3 ms routing overhead represents an increasingly negligible fraction of total latency.

---

## Serving Architecture Comparison

```
                         [ External Client: k6 Load Generator ]
                                            │
                   ┌────────────────────────┴────────────────────────┐
                   │           Kubernetes Service (ClusterIP)        │
                   └────────────────────────┬────────────────────────┘
                                            │
       ┌────────────────────────────────────┼────────────────────────────────────┐
       ▼                                    ▼                                    ▼
┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
│     ARCHITECTURE A      │    │     ARCHITECTURE B      │    │     ARCHITECTURE C      │
│     DIRECT SERVING      │    │ RAY SERVE PYTHON PROXY  │    │ RAY SERVE WORKER HAPROXY│
├─────────────────────────┤    ├─────────────────────────┤    ├─────────────────────────┤
│ • Ingress/ClusterIP     │    │ • Ingress/ClusterIP     │    │ • Ingress/ClusterIP     │
│       │                 │    │       │                 │    │       │                 │
│       ▼                 │    │       ▼                 │    │       ▼ (Port 30000)    │
│  [ Uvicorn Pod ]        │    │  [ Ray Head Node ]      │    │  [ Ray Worker Pod ]     │
│   FastAPI + orjson      │    │   SERVE_PROXY_ACTOR     │    │   HAProxy (C process)   │
│   XGBoost (1 vCPU)      │    │   (Python/asyncio)      │    │       │                 │
│                         │    │       │ (gRPC RPC mesh) │    │       ▼ (Local socket)  │
│                         │    │       ▼                 │    │   Worker Actor          │
│                         │    │  [ Ray Worker Pod ]     │    │   (Python Starlette)    │
│                         │    │   Worker Actor (Python) │    │   XGBoost (1 vCPU)      │
└─────────────────────────┘    └─────────────────────────┘    └─────────────────────────┘
```

### What Worker HAProxy Bypasses vs. Retains

| Component | Default Python Proxy | Worker HAProxy (`:30000`) | Impact |
|---|---|---|---|
| **Head Proxy Actor** | Traversed | **Bypassed** | Eliminates Python asyncio event-loop queueing |
| **Inter-Node RPC Mesh** | Traversed | **Bypassed** | Eliminates cross-pod gRPC forwarding hops |
| **Actor Lifecycle Management** | Active | **Active** | Managed by KubeRay and Ray GCS controller |
| **Dynamic Batching (`@serve.batch`)**| Supported | **Supported, not tested** | Available in framework; not decorated in this unbatched CPU test |
| **Concurrency Ceiling** | `max_ongoing_requests: 50` | `max_ongoing_requests: 50` | Application backpressure enforced locally |
| **Metrics & Prometheus** | Active (port 8080) | **Active (port 8080)** | Full observability retained |

---

## Exact Latency Measurement Boundaries

To ensure complete experimental transparency, the timing boundaries are defined as follows:

1. **Pure Model Latency (`server_model_latency_ms`):**
   - **Direct Serving:** Timed immediately before `xgb.DMatrix([features])` construction and stopped immediately after `booster.predict(dmat)`.
   - **Ray Serve:** Timed immediately before `xgb.DMatrix([features])` construction and stopped immediately after `self.booster.predict(dmat)`.
   - Captures CPU memory buffer construction from a Python list of 50 floats plus XGBoost C++ histogram booster inference.
2. **Server Total Latency (`server_total_latency_ms`):**
   - **Direct Serving:** Starts upon receiving the HTTP request body (`await request.body()`), parses JSON with `orjson.loads()`, executes DMatrix + predict, and stops after JSON response serialization (`orjson.dumps()`).
   - **Ray Serve:** Starts inside `XGBoostDeployment.__call__` upon reading the raw Starlette request body, parses JSON with `orjson.loads()`, executes DMatrix + predict, and stops after JSON serialization (`orjson.dumps()`).
3. **Client HTTP Duration (`http_req_duration`):**
   Full open-loop round-trip duration measured by `k6` from TCP socket transmission to receipt of the final response byte.
4. **Estimated Network & Proxy Transit Overhead:**
   Per-request difference $\\Delta_{\\text{transit}} = T_{\\text{client}} - T_{\\text{server}}$, reflecting network interface transit, Kubernetes ClusterIP routing, reverse-proxy queuing (HAProxy / Python proxy actor), and inter-process socket communication.

---

## Raw Data Limits & Tail Latency Interpretation

- **Trial-Level Summaries:** The raw dataset stores trial-level percentile summaries generated by k6's summary export across 5 independent trials of 30 seconds each (150s total per architecture, ~7,500 requests per architecture). Individual per-request timestamp traces were not persisted to disk.
- **Warmup Protocol:** Initial socket establishment and JIT / model warmup occurred during the initial connection phase before steady-state recording; trials ran for 30s each under 50 RPS.
- **Softened p99 Conclusions:** In a 30-second trial at 50 req/s (1,500 requests), p99 reflects approximately the 15 slowest requests. Across 5 sample trials, rare anomalies (such as OS thread preemption, GC activity, or TCP window adjustments) heavily influence p99 variance (as seen in Python Proxy Trial 4). Tail measurements should be understood as sensitivity indicators under micro-bursts rather than steady-state upper bounds.

---

## How to Reproduce in a Few Commands

### 1. Clone & Set Up Environment

```bash
git clone https://github.com/michael-loewenstein/ray-serve-model-serving-benchmark.git
cd ray-serve-model-serving-benchmark

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r model/requirements.txt
```

### 2. Generate Deterministic Model & Test Fixtures

```bash
# Trains 50-feature XGBoost model and exports 1,000 JSON sample payloads (script-relative paths)
python model/train.py --seed 42
```

Outputs:
- `model/artifacts/model.json` (~550 KB)
- `loadtest/fixtures/sample_payloads.jsonl` (1,000 requests with deterministic seeded IDs)

### 3. Deploy to Kubernetes

Ensure [KubeRay Operator v1.4.2](https://ray-project.github.io/kuberay-helm/) is installed.

```bash
# Create namespace and application ConfigMaps
kubectl apply -f k8s/00-namespace.yaml
kubectl create configmap direct-serving-code --from-file=direct_app.py=k8s/app/direct_app.py -n model-serving-benchmark
kubectl create configmap ray-serve-code --from-file=ray_serve_app.py=k8s/app/ray_serve_app.py -n model-serving-benchmark
kubectl create configmap benchmark-model-artifact --from-file=model.json=model/artifacts/model.json -n model-serving-benchmark

# Deploy all 3 architectures
kubectl apply -f k8s/01-direct-serving.yaml
kubectl apply -f k8s/02-ray-serve-python-proxy.yaml
kubectl apply -f k8s/03-ray-serve-worker-haproxy.yaml

# Wait for readiness
kubectl wait --for=condition=available deployment/direct-serving-cpu -n model-serving-benchmark --timeout=180s
kubectl wait --for=condition=Ready rayservice/ray-serve-python-proxy -n model-serving-benchmark --timeout=300s
kubectl wait --for=condition=Ready rayservice/ray-serve-worker-haproxy -n model-serving-benchmark --timeout=300s
```

### 4. Execute the Randomized Load Test Matrix

> **Warning:** Do NOT test through `kubectl port-forward`. Port-forwarding routes through the Kubernetes API server, injecting multi-millisecond proxy jitter. Run in-cluster via `k8s/04-loadgen-job.yaml` or from an EC2 instance in `us-east-1d`.

```bash
python loadtest/run_matrix.py --trials 5 --rate-rps 50 --duration 30s
```

### 5. Reconstruct Tables and Confidence Intervals

```bash
python results/analyze.py
```

Outputs the complete statistical breakdown, Student's t 95% confidence intervals, and hypothesis evaluations.

---

## Limitations

1. **Workload Scope:** Evaluated exclusively on a single small CPU XGBoost tabular model (50 features, 100 trees, depth 6).
2. **Traffic Rate:** Tested at a steady arrival rate of 50 req/s. Higher saturation loads (100–500+ req/s) may exhibit different queueing dynamics.
3. **Single Model Topology:** Evaluates single-model direct HTTP serving; does not benchmark multi-model composite DAGs, fractional GPU sharing, or pipeline parallelism.
4. **Dynamic Batching:** Not evaluated in this phase; individual unbatched requests were served.

---

## Phase 3 Roadmap

- [ ] **Saturation Sweeps & Concurrency Limits:** Benchmark 100 to 500+ req/s to determine maximum sustainable goodput before queue saturation.
- [ ] **GPU Dynamic Micro-Batching:** Benchmark `sentence-transformers/all-MiniLM-L6-v2` on AWS G5/G6 instances comparing native PyTorch async queues vs. Ray Serve `@serve.batch`.
- [ ] **Multi-Model Pipelines (DAGs):** End-to-end inference pipelines combining CPU tabular preprocessing, GPU vector embeddings, and LLM generative reranking.
- [ ] **Autoscaling Economics:** Cost-per-request comparison between Ray Serve's queue-based autoscaler and Kubernetes Horizontal Pod Autoscaler (HPA) under diurnal load spikes.

---

## Pinned Environment & Versions

| Component | Pinned Version | Description |
|---|---|---|
| **Ray / Ray Serve** | `2.55.1` | Distributed serving runtime (`RAY_SERVE_ENABLE_HA_PROXY=1`) |
| **ray-haproxy** | `2.8.25` | Worker-colocated HAProxy reverse proxy wheel |
| **KubeRay Operator** | `v1.4.2` | Kubernetes operator managing `RayService` CRDs |
| **k6** | `v0.54.0` | Open-loop load generator (`constant-arrival-rate`) |
| **XGBoost** | `2.1.1` | Model runtime (`tree_method: hist`) |
| **FastAPI / Uvicorn** | `0.110.0` / `0.28.0` | Direct serving web framework and ASGI server |
| **orjson** | `3.9.0` | High-performance C-based JSON serialization |
| **Kubernetes** | `1.30` | Cloud container orchestration platform |
| **Python** | `3.11` | Application runtime |
| **AWS Compute** | `c6i.2xlarge` | Intel Xeon 8375C @ 2.90GHz (8 vCPUs, 16 GiB RAM) |

---

## Repository Structure

```
ray-serve-model-serving-benchmark/
├── LICENSE                      # MIT License
├── README.md                    # Benchmark overview and headline results
├── .gitignore                   # Ignore rules
├── docs/
│   ├── plan.md                  # Comprehensive benchmark plan & methodology
│   └── report.md                # Phase 2 15-trial statistical benchmark report
├── model/
│   ├── README.md                # Model training details
│   ├── train.py                 # Seeded synthetic XGBoost generator
│   ├── requirements.txt         # Pinned training dependencies
│   └── artifacts/
│       └── model.json           # Pinned model weights (550 KB)
├── k8s/
│   ├── README.md                # Manifest documentation
│   ├── 00-namespace.yaml        # Benchmark namespace
│   ├── 01-direct-serving.yaml   # Architecture A: FastAPI + Uvicorn
│   ├── 02-ray-serve-python-proxy.yaml   # Architecture B: Ray Serve Head Proxy
│   ├── 03-ray-serve-worker-haproxy.yaml # Architecture C: Ray Serve Worker HAProxy
│   ├── 04-loadgen-job.yaml      # In-cluster k6 load generator Job
│   └── app/
│       ├── direct_app.py        # Direct serving FastAPI application
│       ├── ray_serve_app.py     # Ray Serve deployment application
│       ├── Dockerfile.direct    # Container image for Direct Serving
│       └── Dockerfile.ray       # Container image for Ray Serve
├── loadtest/
│   ├── README.md                # Load testing instructions
│   ├── k6_benchmark.js          # k6 constant-arrival-rate script
│   ├── run_matrix.py            # Randomized multi-trial execution runner
│   └── fixtures/
│       └── sample_payloads.jsonl# 1,000 synthetic request payloads
└── results/
    ├── README.md                # Results documentation
    ├── raw_trials.json          # Complete 15-trial raw dataset (22,508 requests)
    ├── raw_trials.csv           # Tabular CSV of all 15 trials
    ├── summary_report.md        # Reconstructed statistical summary
    └── analyze.py               # Statistical analysis & CI generator
```

---

## License

This project is licensed under the [MIT License](LICENSE).
