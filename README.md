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

All measurements reflect **15 independent randomized trials** (5 per architecture, 22,507 total requests) at 50 requests/second on 100% matched AWS `c6i.2xlarge` hardware with single-zone co-location (`us-east-1d`).

### 1. Absolute Latency First (Worker HAProxy vs. Direct Baseline)

When utilizing Ray Serve's worker-colocated HAProxy routing (`:30000`), absolute latency overhead is negligible:

| Metric | Direct Serving Baseline | Ray Serve (Worker HAProxy :30000) | Absolute Overhead | Absolute Gate ($\le 3.0\text{ ms}$) |
|---|---|---|---|---|
| **p50 (Median)** | **0.973 ms** (±0.083) | **1.274 ms** (±0.097) | **+0.301 ms** (~300 µs) | **PASS** |
| **p90** | **1.151 ms** | **1.768 ms** | **+0.617 ms** | **PASS** |
| **p95** | **1.246 ms** (±0.170) | **1.856 ms** (±0.087) | **+0.610 ms** | **PASS** |
| **p99** | **1.444 ms** (±0.242) | **2.595 ms** (±0.702) | **+1.151 ms** (~1.15 ms) | **PASS** |

*By comparison, Ray Serve's default Python Proxy (head node) exhibited **+0.505 ms** at p50 and **+3.421 ms** at p99.*

### 2. Relative Overhead Second

| Metric | Direct Serving Baseline | Ray Serve (Worker HAProxy :30000) | Relative Overhead | Relative Gate ($\le 10\%$) |
|---|---|---|---|---|
| **p50** | 0.973 ms | 1.274 ms | **+30.9%** | **FAIL** |
| **p90** | 1.151 ms | 1.768 ms | **+53.6%** | **FAIL** |
| **p95** | 1.246 ms | 1.856 ms | **+49.0%** | **FAIL** |
| **p99** | 1.444 ms | 2.595 ms | **+79.7%** | **FAIL** |

### Contextual Analysis on Baseline Scale

Direct Serving on bare Linux sockets achieves an ultra-low baseline ($p50 = 0.973\text{ ms}$, $p99 = 1.444\text{ ms}$). Because the baseline latency is sub-millisecond:
- An incremental ~300 µs reverse-proxy hop mathematically constitutes a **+30.9% relative delta**.
- An added 1.15 ms at p99 constitutes a **+79.7% relative delta**.

**Practical takeaway:** In almost all real-world machine learning systems (where model inference takes 10 ms to 500+ ms), an added ~0.3 ms median overhead is imperceptible. However, if platform rules mandate passing *both* an absolute ($\le 3.0\text{ ms}$) and a relative ($\le 10\%$) gate on sub-millisecond workloads, the relative gate will fail purely due to baseline scale.

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
| **Dynamic Batching (`@serve.batch`)**| Active | **Active** | Micro-batching queues run inside worker actors |
| **Concurrency Ceiling** | `max_ongoing_requests` | `max_ongoing_requests` | Application backpressure enforced locally |
| **Metrics & Prometheus** | Active (port 8080) | **Active (port 8080)** | Full observability retained |

---

## How to Reproduce in a Few Commands

### 1. Clone & Set Up Environment

```bash
git clone https://github.com/your-org/ray-serve-model-serving-benchmark.git
cd ray-serve-model-serving-benchmark

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r model/requirements.txt
```

### 2. Generate Deterministic Model & Test Fixtures

```bash
# Trains 50-feature XGBoost model and exports 1,000 JSON sample payloads
python model/train.py --seed 42
```

Outputs:
- `model/artifacts/model.json` (~550 KB)
- `loadtest/fixtures/sample_payloads.jsonl` (1,000 requests)

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

Run 5 randomized trials per architecture (15 trials total) using `k6`:

```bash
cd loadtest
python run_matrix.py --trials 5 --rate-rps 50 --duration 30s
```

### 5. Reconstruct Tables and Confidence Intervals

```bash
python results/analyze.py --data results/raw_trials.json
```

Outputs the complete statistical breakdown, Student's t 95% confidence intervals, and hypothesis evaluations.

---

## Limitations

1. **Workload Scope:** Evaluated exclusively on a single small CPU XGBoost tabular model (50 features, 100 trees, depth 6).
2. **Traffic Rate:** Tested at a steady arrival rate of 50 req/s. Higher saturation loads (100–500+ req/s) may exhibit different queueing dynamics.
3. **Single Model Topology:** Evaluates single-model direct HTTP serving; does not benchmark multi-model composite DAGs, fractional GPU sharing, or pipeline parallelism.

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
    ├── raw_trials.json          # Complete 15-trial raw dataset
    ├── raw_trials.csv           # Tabular CSV of all 15 trials
    ├── summary_report.md        # Reconstructed statistical summary
    └── analyze.py               # Statistical analysis & CI generator
```

---

## License

This project is licensed under the [MIT License](LICENSE).
