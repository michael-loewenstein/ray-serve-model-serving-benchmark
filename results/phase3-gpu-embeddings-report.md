# Phase 3 Benchmark Report: Lightweight GPU Embeddings (`all-MiniLM-L6-v2`)

**Target Environment:** Kubernetes on AWS (EKS, `us-east-1`), namespace `model-serving-benchmark`  
**Workload:** Hugging Face `sentence-transformers/all-MiniLM-L6-v2` in FP16 on NVIDIA L4 (384-dimensional dense embeddings)  
**Hardware & Nodes:** Dedicated AWS **`g6.xlarge` (NVIDIA L4 24GB Tensor Core GPU, 4 vCPUs, 16 GiB RAM)** in `us-east-1d`.  
**Co-Location Standard:** Both GPU pods and the `k6-bench-gpu` driver co-located in `us-east-1d` on matched `g6.xlarge` instances with zero cross-AZ hops.  
**Parity Standard:** Exactly 1 GPU replica per architecture, 1x NVIDIA L4, Requests: 2000m CPU / 8 GiB RAM, Limits: 4000m CPU / 12 GiB RAM, PyTorch 2.2.0.  
**Batching Configuration:** Max batch size = 32, Batch wait timeout = 5 ms (`0.005s`).  
**Load Methodology:** Closed-loop client concurrency scaling (`k6` `constant-vus`).

> **Exploratory Preliminary Baseline Notice (Pre-Event Loop Decoupling):**  
> The measurements reported below reflect an initial exploratory single-run benchmark conducted prior to decoupling `model.encode()` from the asyncio event loop in Direct Serving (`asyncio.to_thread`). Because the initial Direct Serving baseline synchronously executed model inference on the event loop, its C=32 and C=64 tail latency (p99 = 111.65 ms and 138.18 ms) contains event loop starvation artifacts. In the codebase, Direct Serving has since been decoupled via `asyncio.to_thread` and manifest version parity pinned (`torch==2.2.0`). Verified multi-trial conclusions require executing the updated 5-trial matrix runner (`benchmarks/gpu-embed/loadgen/run_embed_matrix.py`).

---

## 1. Executive Summary & Plain Headline Findings

Phase 3 evaluated the efficiency of dynamic request micro-batching on modern NVIDIA Tensor Cores (AWS G6 / NVIDIA L4) across client concurrencies $C \in [1, 4, 8, 16, 32, 64]$.

### Plain Headline Findings:
1. **Direct serving (custom asyncio queue) wins raw goodput:** 2,271 vs 1,672 embeddings/s at C=32. Ray Serve reaches **73.6%** of direct serving throughput, **failing the >= 95% goodput gate**.
2. **At low load, Ray Serve adds ~2.7 ms p95 at C=1 (passing the <= 3.0 ms absolute gate), and +3.4 ms at C=4 (failing the gate).**
3. **Ray Serve fails the Batch Scaling latency gate at C=32:** While throughput scaled 19.5x, its p95 was 28.41 ms (exceeding the <= 20 ms gate ceiling; Direct Serving passed at 17.76 ms).
4. **Tail latency under C=64 closed-loop saturation reflects pre-fix baseline artifacts:** The 50% lower p99 on Ray Serve (69.04 ms vs 138.18 ms) in the preliminary single-run was measured before Direct Serving was decoupled from the event loop via asyncio.to_thread. Final conclusions require executing the 5-trial matrix runner.

### Decision Gate Summary Table (Open Reporting, No Spin):

| Gate | Criterion | Direct Serving Result | Ray Serve Result | Gate Evaluation |
|---|---|---|---|---|
| **Batch Scaling Gate** | Throughput scales >= 3x from C=1 to C=32 with p95 <= 20 ms | **24.2x** scaling (93.6 -> 2,271.5 req/s), p95 = 17.76 ms | **19.5x** scaling (85.6 -> 1,671.6 req/s), p95 = 28.41 ms | **FAIL for Ray Serve** (Throughput scaled 19.5x, but p95 = 28.41 ms exceeded 20 ms ceiling; Direct PASS) |
| **Goodput Ratio Gate** | Ray Serve reaches >= 95% of Direct Serving goodput | Peak: 2,271.5 req/s | Peak: 1,671.6 req/s (**73.6% of Direct**) | **FAIL** (Direct is ~26% faster at peak batching) |
| **Latency Gate (Absolute)** | Ray Serve adds <= 3.0 ms p95 latency at low load (C <= 4) | C=1: 10.87 ms<br>C=4: 11.96 ms | C=1: 13.57 ms (+2.70 ms)<br>C=4: 15.34 ms (+3.38 ms) | **PASS at C=1**, **FAIL at C=4** |
| **Closed-Loop Saturation (C=64)** | Tail latency (p99) under C=64 closed-loop saturation [Pre-Fix Snapshot] | p99 = 138.18 ms | p99 = **69.04 ms** (50% lower tail in pre-fix run) | **INCONCLUSIVE / PENDING RE-RUN** (Pre-fix Direct baseline had event loop blocking; re-run required) |

---



### 1.1 Specialized C++ Inference Engine Context (Triton / TEI)
For architectural perspective, dynamic micro-batching on general Python HTTP servers (both FastAPI and Ray Serve) operates under Python runtime constraints. Dedicated C++ tensor serving runtimes—such as Hugging Face **Text Embeddings Inference (TEI)** or **NVIDIA Triton Inference Server** with dynamic batching and TensorRT/ONNX-Runtime—typically achieve an additional 20-35% lower p95 latency and higher GPU saturation on the same NVIDIA L4 by completely eliminating Python GIL contention and multi-process serialization.

## 2. Concurrency Sweep Results ($C \in [1 \to 64]$)

Evaluated under **single 30-second sustained load runs per concurrency level** using 2,000 realistic e-commerce search query payloads:

| Architecture | Concurrency ($C$) | Throughput (embeddings/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Server Reported Batch Time (ms) | Failure Rate |
|---|---|---|---|---|---|---|---|---|
| **Direct Serving (Async Queue)** | 1 | **93.60** | **10.41** | 10.79 | **10.87** | 11.25 | 4.49 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 1 | **85.62** | **10.93** | 13.23 | **13.57** | 15.75 | 10.40 ms | 0.00% |
| **Direct Serving (Async Queue)** | 4 | **350.18** | **11.09** | 11.73 | **11.96** | 12.68 | 4.98 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 4 | **309.59** | **12.17** | 14.57 | **15.34** | 17.70 | 11.22 ms | 0.00% |
| **Direct Serving (Async Queue)** | 8 | **715.72** | **10.70** | 11.46 | **11.80** | 13.40 | 4.99 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 8 | **563.44** | **13.40** | 16.41 | **17.54** | 21.22 | 11.59 ms | 0.00% |
| **Direct Serving (Async Queue)** | 16 | **1,266.70** | **12.06** | 13.27 | **14.04** | 17.31 | 5.37 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 16 | **951.83** | **15.89** | 20.08 | **21.75** | 26.18 | 12.29 ms | 0.00% |
| **Direct Serving (Async Queue)** | 32 | **2,271.48** | **11.75** | 16.13 | **17.76** | **111.65** | 5.72 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 32 | **1,671.63** | **16.92** | 25.58 | **28.41** | **37.50** | 9.26 ms | 0.00% |
| **Direct Serving (Async Queue)** | 64 (Saturated) | **1,826.88** | **32.15** | 35.86 | **46.57** | **138.18** | 5.61 ms | 0.00% |
| **Ray Serve (@serve.batch)** | 64 (Saturated) | **1,664.16** | **34.63** | 52.17 | **58.38** | **69.04** | 21.68 ms | 0.00% |

---


<p align="center">
  <img src="../docs/images/phase3_gpu_throughput_scaling.svg" alt="Phase 3 GPU Throughput Scaling" width="850">
</p>
<p align="center">
  <img src="../docs/images/phase3_gpu_latency_tail_overload.svg" alt="Phase 3 GPU Latency and Overload Tail" width="950">
</p>

## 3. Honesty & Reproducibility Disclosures

Before interpreting or publishing these results, the following technical caveats must be explicitly understood:

1. **Single 30-Second Runs (No Confidence Intervals):**
   - Each concurrency level was measured across a **single 30-second run**.
   - Unlike the 15-trial statistical matrix in Phase 2, this sweep does **not** have multi-trial confidence intervals. Small differences (e.g. ±1–2 ms) are indicative rather than definitive.
   - **Outlier Spike at $C=32$ for Direct Serving:** Direct serving exhibited $p95 = 17.76\text{ ms}$ but jumped to $p99 = 111.65\text{ ms}$. This represents an isolated tail excursion during a single 30-second test window and was **not rerun**.

2. **Explanation of the "Server Reported Batch Time" Difference:**
   - In the table, Direct Serving reports ~4.5 ms batch time at $C=1$, whereas Ray reports ~10.4 ms.
   - **This is a measurement boundary difference, not a 2x difference in CUDA execution:**
     * In Direct Serving (`direct/app.py`), the timer brackets strictly `model.encode()` around the tensor batch:
       ```python
       t0 = time.perf_counter()
       with torch.inference_mode():
           embeddings = model.encode(texts, batch_size=len(texts))
       t1 = time.perf_counter()
       # Reported as model_latency_ms: ~4.5 ms
       ```
     * In Ray Serve (`ray/serve_embed.py`), the timer brackets the async call `await self.batched_encode(text)`:
       ```python
       t_m0 = time.perf_counter()
       embedding = await self.batched_encode(text)
       t_m1 = time.perf_counter()
       # Reported as model_latency_ms: ~10.4 ms
       ```
       Because `await self.batched_encode(text)` includes Ray's `@serve.batch` wait window (`batch_wait_timeout_s=0.005` = 5 ms), the reported number includes the 5 ms batch accumulation timeout plus the ~5.4 ms model execution.
     * On the GPU, pure tensor execution times are comparable.

3. **Dynamic Batching Configuration & Tuning:**
   - Both implementations used identical batching parameters: `max_batch_size: 32` and `batch_wait_timeout_s: 0.005` (5 ms).
   - Ray Serve was run with its default `@serve.batch` queuing behavior; no hyperparameter tuning of batch timeouts or internal buffers was performed.

4. **Scope of the Direct Baseline:**
   - The Direct Serving baseline is a **custom Python asyncio queue implementation** (`asyncio.Queue` background consumer). It is not an enterprise-grade C++ inference server like NVIDIA Triton Inference Server or TorchServe. Conclusions apply strictly to this custom FastAPI/PyTorch setup versus Ray Serve.

5. **No Generalization Beyond This Workload:**
   - These conclusions apply strictly to **one small embedding model** (`all-MiniLM-L6-v2`, 22M parameters) on **one GPU instance type** (AWS G6 / NVIDIA L4). Larger models (e.g., 7B LLMs or multi-GPU pipeline parallel workloads) exhibit fundamentally different compute-to-overhead ratios.
