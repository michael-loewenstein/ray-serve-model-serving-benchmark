# Phase 3: Lightweight GPU Embeddings Implementation Plan
## (Sentence-Transformers `all-MiniLM-L6-v2` — Dynamic Micro-Batching & GPU Efficiency)

**Objective:** Benchmark GPU scheduling, Tensor Core utilization, and dynamic micro-batching efficiency by comparing **Direct Serving (FastAPI + PyTorch + Async Queue Batching)** vs. **Ray Serve (`@serve.batch` + Worker HAProxy :30000)** on cloud infrastructure.

---

## 1. Architectural & Infrastructure Specifications

### 1.1 Hardware & Node Pool
Both architectures and the benchmark driver run under 100% hardware and zone parity using Karpenter on AWS:
- **Node Architecture:** AWS **`g6`** (e.g. `g6.xlarge`: 4 vCPUs, 16 GiB RAM, 1x NVIDIA L4 24GB Tensor Core GPU)
- **Zone:** **`us-east-1d`** (co-located, zero inter-AZ hops)
- **Capacity Type:** **`on-demand`**
- **Node Selector:**
  ```yaml
  nodeSelector:
    node.kubernetes.io/instance-type: g6.xlarge
    topology.kubernetes.io/zone: us-east-1d
    karpenter.sh/capacity-type: "on-demand"
  ```
- **Compute Resources per Replica:**
  - GPU: `nvidia.com/gpu: "1"`
  - CPU: `requests: 2000m, limits: 4000m`
  - Memory: `requests: 8Gi, limits: 12Gi`
- **Replicas:** Exactly **1 GPU replica** per architecture (evaluating single-GPU batching efficiency).

### 1.2 Model & Workload Specification
- **Model:** Hugging Face `sentence-transformers/all-MiniLM-L6-v2` (384-dimensional dense embeddings).
- **Precision:** FP16 (`torch.float16`).
- **Batching Window:** **Max batch size: 32**, **Wait timeout: 5 ms** (`0.005s`).
- **Input Payload:** JSON catalog search query:
  ```json
  {"id": "<uuid>", "text": "fresh organic italian olive oil 500ml"}
  ```
- **Output Payload:** JSON embedding vector:
  ```json
  {"id": "<uuid>", "embedding": [0.024, -0.118, ...], "model_latency_ms": 1.45, "total_latency_ms": 1.62}
  ```

---

## 2. Granular Task Breakdown

### Task 3.1: Dataset & Fixture Generation
- **Target File:** `benchmarks/gpu-embed/fixtures/generate_fixtures.py`
- **Output Fixture:** `benchmarks/gpu-embed/fixtures/queries.jsonl` (2,000 synthetic search query strings with varying sequence lengths from 16 to 128 tokens).
- **Execution Command:**
  ```bash
  python3 benchmarks/gpu-embed/fixtures/generate_fixtures.py --count 2000 --output benchmarks/gpu-embed/fixtures/queries.jsonl
  ```

---

### Task 3.2: Direct Serving GPU Implementation
- **Target Files:**
  - `benchmarks/gpu-embed/direct/app.py`: FastAPI server implementing an async background consumer loop with `asyncio.Queue`. Arriving requests queue up; the worker dequeues up to 32 requests or after a 5 ms timeout, invokes `model.encode(texts, batch_size=len(texts))` offloaded to `asyncio.to_thread` to preserve HTTP event loop responsiveness, and resolves client futures.
  - `benchmarks/gpu-embed/direct/Dockerfile`: Lightweight container based on `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`.
  - `benchmarks/gpu-embed/direct/k8s/deployment.yaml`: Deployment targeting `g6.xlarge` in `us-east-1d` requesting 1 GPU.
  - `benchmarks/gpu-embed/direct/k8s/service.yaml`: ClusterIP service `direct-gpu-embed-svc` on port 8000.
- **Verification:**
  ```bash
  kubectl apply -f benchmarks/gpu-embed/direct/k8s/ -n model-serving-benchmark
  kubectl wait --for=condition=ready pod -l app=direct-gpu-embed -n model-serving-benchmark --timeout=300s
  curl -s http://direct-gpu-embed-svc:8000/readiness | jq .
  ```

---

### Task 3.3: Ray Serve GPU Implementation (`@serve.batch`)
- **Target Files:**
  - `benchmarks/gpu-embed/ray/serve_embed.py`: Ray Serve deployment using:
    ```python
    @serve.deployment(
        name="EmbeddingDeployment",
        ray_actor_options={"num_gpus": 1, "num_cpus": 2},
        max_ongoing_requests=100
    )
    class EmbeddingDeployment:
        def __init__(self):
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cuda:0")
            self.model.half()

        @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.005)
        async def batch_embed(self, texts: list[str]) -> list[list[float]]:
            with torch.inference_mode():
                embeddings = self.model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
            return embeddings.tolist()
    ```
  - `benchmarks/gpu-embed/ray/rayservice-embed.yaml`: KubeRay `RayService` CRD configured with:
    - Worker group requesting 1 GPU on `g6` in `us-east-1d`.
    - `RAY_SERVE_ENABLE_HA_PROXY=1`.
    - Worker direct service on port 30000: `rayservice-gpu-embed-haproxy-svc`.
- **Verification:**
  ```bash
  kubectl apply -f benchmarks/gpu-embed/ray/rayservice-embed.yaml -n model-serving-benchmark
  kubectl wait --for=condition=Ready rayservice/rayservice-gpu-embed -n model-serving-benchmark --timeout=300s
  curl -s http://rayservice-gpu-embed-haproxy-svc:8000/readiness | jq .
  ```

---

### Task 3.4: Automated k6 Test Suite & GPU Telemetry
- **Target Files:**
  - `benchmarks/gpu-embed/loadgen/k6_embed.js`: k6 script reading `queries.jsonl`, asserting HTTP 200, validating 384-dim array length, and recording latency metrics.
  - `benchmarks/gpu-embed/loadgen/run_embed_matrix.py`: Test runner orchestrating concurrency sweeps (C in [1, 4, 8, 16, 32, 64]) and throughput sweeps.

---

### Task 3.5: Reporting & Decision Gates
- **Target Report:** `results/phase3-gpu-embeddings-report.md`.
- **Decision Gates:**
  1. **Latency Gate:** Ray Serve adds <= 10% p95 latency (or <= 3.0 ms) over Direct Serving under low load.
  2. **Goodput Gate:** Ray Serve reaches >= 95% of Direct Serving maximum embedding throughput.
  3. **Batch Scaling:** Both architectures achieve >= 3x throughput increase when scaling from concurrency 1 to concurrency 32.
