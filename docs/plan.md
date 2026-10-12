# Benchmark Plan: Ray Serve vs. Direct Serving on AWS EKS

## Open-Source Workload Specification & Official Ray Serve Evaluation Methodology

**Target Infrastructure:** Kubernetes on AWS (EKS, `us-east-1`), namespace `model-serving-benchmark`  
**Node Architecture:** Dedicated compute node pool on AWS `c6i.2xlarge` (Intel Xeon 8375C @ 2.90GHz, On-Demand), Karpenter node provisioning, Prometheus metrics pipeline  
**Core Standard:** Zero proprietary dependencies; aligned with [Ray Serve Official Benchmarks](https://docs.ray.io/en/latest/serve/llm/benchmarks.html) and [`anyscale/ray-serve-llm-perf-examples`](https://github.com/anyscale/ray-serve-llm-perf-examples).

---

## 1. Executive Summary & Hypotheses

The objective of this benchmark is to determine whether Ray Serve's multi-model orchestration, dynamic batching, and logical queue autoscaling justify its proxy and control-plane overhead compared to native Kubernetes serving on cloud infrastructure.

```
                      [ External Client: k6 / load generator ]
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │     Kubernetes Ingress / Gateway API      │
                    └─────────────────────┬─────────────────────┘
                                          │
         ┌────────────────────────────────┴────────────────────────────────┐
         ▼                                                                 ▼
┌────────────────────────────────────────┐       ┌────────────────────────────────────────┐
│     ARCHITECTURE A: DIRECT SERVING     │       │       ARCHITECTURE B: RAY SERVE        │
│        (Native K8s Deployments)        │       │      (KubeRay RayService v1.4.2)       │
├────────────────────────────────────────┤       ├────────────────────────────────────────┤
│ • CPU Tabular: FastAPI + XGBoost       │       │ • Head: GCS, Serve Controller, Metrics │
│ • GPU Embeddings: FastAPI + PyTorch    │       │ • Worker: Ray Serve Deployments        │
│   (all-MiniLM-L6-v2)                   │       │   - XGBoost (CPU actor)                │
│ • GPU LLM: Standalone vLLM Container   │       │   - all-MiniLM-L6-v2 (@serve.batch)    │
│   (vllm/vllm-openai:v0.6.3)            │       │   - Llama-3.1-8B (Ray Serve vLLM)      │
│ • Ingress -> K8s Service -> Pod IP     │       │ • Ingress -> HAProxy (:30000) -> Actor │
└────────────────────────────────────────┘       └────────────────────────────────────────┘
```

### Hypotheses to Prove:
1. **Serving Overhead (H1):** Evaluated against both relative and absolute limits: added latency $\le 10\%$ relative overhead AND/OR $\le 3.0\text{ ms}$ absolute overhead over Direct serving. (For sub-millisecond baseline workloads, the absolute limit may pass while the relative limit fails due to baseline scale).
2. **Inference Efficiency (H2):** Both architectures achieve $\ge 95\%$ identical throughput when using identical inference runtimes.
3. **Platform Value:** Ray Serve simplifies multi-model orchestration, fractional resource sharing, and prevents noisy-neighbor tail spikes under mixed load.
4. **Economics:** Ray Serve's queue-based autoscaling and replica density result in lower cost per 1,000 requests under variable load.

---

## 2. Workload Definitions

| Workload | Model Architecture | Hardware & Resource Envelope | Test Objective |
|---|---|---|---|
| **Workload 1:<br>CPU Tabular** | **XGBoost Classifier** (100 trees, depth 6)<br>50-dimensional float feature vector | `c6i.2xlarge` (On-Demand)<br>1 vCPU, 2 GiB RAM per replica | Serving overhead, JSON parsing efficiency (`orjson`), proxy hops. |
| **Workload 2:<br>GPU Embeddings** | **`sentence-transformers/all-MiniLM-L6-v2`**<br>(384-dim, FP16) | `g5.2xlarge` / `g6.2xlarge` (NVIDIA A10G / L4)<br>2 vCPU, 8 GiB RAM, 1 GPU | Dynamic micro-batching efficiency, GPU utilization. |
| **Workload 3:<br>GPU LLM** | **`meta-llama/Meta-Llama-3.1-8B-Instruct`**<br>vLLM v0.6.x (TP=1, AWQ/FP16) | `g5.2xlarge` / `g6.2xlarge`<br>6 vCPU, 28 GiB RAM, 1 GPU | TTFT, TPOT, Tokens/s (TPS), KV cache dynamics, scaling linearity. |
| **Workload 4:<br>Mixed Multi-Model** | All 3 models deployed concurrently<br>(60% CPU / 25% Embed / 15% LLM) | Mixed CPU & GPU pools on Kubernetes | Cross-workload contention, tail latency amplification, goodput. |

---

## 3. Official Ray Serve Evaluation Alignment

Following standard model serving evaluation frameworks, the benchmarks are structured around four primary metrics and test axes:

1. **Time To First Token (TTFT) / Time to Inference:** Measures prefill time + queueing delay.
2. **Time Per Output Token (TPOT / ITL):** Measures autoregressive token generation time.
3. **Throughput (Requests per Second & Tokens per Second):** Evaluates horizontal scaling linearity ($1\times \to 2\times \to 4\times$ replicas).
4. **Replica Startup Latency:** Measures cold-start provisioning from artifact store to active traffic serving.

---

## 4. Phased Implementation Roadmap

* **Phase 1: Open-Source CPU Inference (XGBoost Tabular)** — Pure HTTP and proxy overhead isolation.
* **Phase 2: CPU Saturation & 3-Way Routing Architecture Benchmark** — Rigorous 15-trial matrix under 100% matched `c6i` hardware and single-zone co-location.
* **Phase 3: Lightweight GPU Embeddings (`all-MiniLM-L6-v2`)** — Dynamic micro-batching (`@serve.batch`) and GPU scheduling.
* **Phase 4: Large Language Model Serving (vLLM + Llama 3.1 8B)** — Standard LLM performance benchmark methodology.
* **Phase 5: Multi-Model Orchestration & Resource Contention** — 60/25/15 mixed traffic on shared Kubernetes cluster.
* **Phase 6: Elastic Autoscaling & Cloud Economics** — Cost per 1,000 requests and cost per million tokens.

---

## 5. Decision Gates

* **Adopt Ray Serve if:**
  - Latency overhead on CPU and LLM is $\le 10\%$ relative or $\le 3.0\text{ ms}$ absolute at target RPS.
  - Goodput reaches $\ge 95\%$ of Direct serving.
  - Multi-model mixed goodput shows lower degradation than uncoordinated K8s deployments.
  - Horizontal scaling linearity is $\ge 92\%$ across 4 replicas.
* **Retain Direct Kubernetes Serving if:**
  - Ray Serve breaches the latency gates or adds unnecessary control-plane complexity without proportional ROI.
