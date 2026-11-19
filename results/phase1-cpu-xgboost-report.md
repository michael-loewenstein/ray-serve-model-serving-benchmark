# Phase 1 Benchmark Report: Initial Exploratory Discovery (CPU XGBoost)

**Target Environment:** Kubernetes on AWS (EKS, us-east-1), namespace model-serving-benchmark  
**Workload:** Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, max depth 6)  
**Execution Date:** 2026-10-12  
**Driver:** In-cluster open-loop load generator (k6 constant arrival rate at 50 req/s)  

---

## 1. Executive Summary & The Routing Discovery

Phase 1 was the initial proof-of-concept run designed to measure the baseline reverse-proxy and routing overhead introduced by Ray Serve compared to native Kubernetes serving (FastAPI + Uvicorn + orjson).

In this initial phase, the **default Ray Serve ingress path showed high tail latency**:
- Traffic entered the KubeRay-generated Serve Service on `:8000`, which routes to the per-pod Python Serve proxy (`proxy_location: EveryNode`). The proxy parses HTTP on an asyncio event loop and forwards requests to replicas via Ray actor RPC.
- This creates an extra hop, serialization overhead, and event-loop queueing under load.
- Connecting directly to the replica's **direct-ingress HTTP server on `:30000`** bypasses the Serve proxy and HAProxy entirely.

> **Port-role correction (2026-11-18):** Phase 1 was originally described as using a "C-based HAProxy reverse proxy listening on worker port 30000." Verification against Ray 2.55.1 shows that `:8000` is the HAProxy frontend when `RAY_SERVE_ENABLE_HA_PROXY=1`, and `:30000–31000` are the direct-ingress HTTP servers inside Serve replicas. The `8000 → 30000` Service therefore tests **direct ingress, not HAProxy**. The numbers below are unchanged; only the label and attribution are corrected. See [Cluster Topology & Request Path](../docs/topology-and-request-path.md).

---

## 2. 3-Way Architectural Comparison Matrix (50 RPS Exploratory Run)

| Metric | Architecture A: Direct Serving (K8s) | Architecture B.1: Ray Serve (Default Python Proxy :8000) | Architecture B.2: Ray Serve (Direct Ingress :30000) | Ray Direct Ingress vs. Direct Delta |
|---|---|---|---|---|
| **Routing Path** | ClusterIP to Uvicorn Pod | KubeRay `serve-svc` to per-pod Python proxy (`:8000`) to replica actor | Service to replica direct-ingress server (`:30000`) | Fast-path direct-to-replica routing |
| **Total Requests** | 4,624 | 4,624 | 4,624 | 100% load equivalence |
| **Completed Throughput** | 49.98 req/s | 49.98 req/s | 49.98 req/s | 100% goodput match |
| **p50 (Median Latency)** | **1.81 ms** | 2.47 ms | **1.60 ms** | **-0.21 ms (Ray direct ingress faster)** |
| **p90 Latency** | **2.19 ms** | 3.59 ms | **2.31 ms** | **+0.12 ms (Within 120 µs)** |
| **p95 Latency** | **2.33 ms** | 5.96 ms | **3.41 ms** | **+1.08 ms** (vs +3.63 ms on Python proxy) |
| **p99 Latency** | **2.76 ms** | 15.27 ms | **10.03 ms** | **-5.24 ms reduction vs. Python proxy** |
| **Average Latency** | **1.84 ms** | 3.06 ms | **2.03 ms** | **+0.19 ms** |
| **Min Latency** | 0.61 ms | 1.64 ms | 1.22 ms | |
| **Max Latency** | 49.41 ms | 91.78 ms | 52.43 ms | |
| **Error Rate** | 0.02% | 1.01% | **0.00% (0 errors)** | **Perfect 0.00%** |
| **Checks Succeeded** | 99.97% | 98.98% | **100.00%** | **100% Passed** |

---

## 3. Key Findings & Why Phase 1 Was Superseded

1. **Direct ingress eliminates the default-proxy bottleneck:**
   - In Architecture B.1, routing through the default Python proxy added ~0.66 ms to median latency (1.81 ms to 2.47 ms) and pushed p99 latency to **15.27 ms**.
   - Connecting directly to the replica's direct-ingress server on port 30000 reduced median latency down to **1.60 ms** (outperforming Direct Serving 1.81 ms in this initial run) and cut p99 latency by over 5 ms.

2. **Why Phase 1 Was Superseded by Phase 2:**
   - **Cross-AZ Transit Noise:** In Phase 1, Karpenter provisioned worker pods across multiple AWS Availability Zones (us-east-1a, us-east-1d), introducing inter-AZ network hops that varied between trials.
   - **Hardware Heterogeneity:** Direct serving ran on c6i.2xlarge instances, while the Ray head ran on an ARM m8g.xlarge instance.
   - **Single-Trial Scope:** Phase 1 was an exploratory continuous run without randomized trials or confidence intervals.
   - **Conclusion:** Phase 1 indicated that **bypassing the default Serve proxy is necessary for low-latency serving**. It did not measure HAProxy. To obtain publication-grade numbers with zero AZ noise and strictly matched CPU hardware, Phase 1 was formally superseded by the **Phase 2 15-trial single-zone matrix** (docs/report.md).
