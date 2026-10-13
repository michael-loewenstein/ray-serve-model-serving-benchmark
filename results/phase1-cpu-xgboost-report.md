# Phase 1 Benchmark Report: Initial Exploratory Discovery (CPU XGBoost)

**Target Environment:** Kubernetes on AWS (EKS, us-east-1), namespace model-serving-benchmark  
**Workload:** Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, max depth 6)  
**Execution Date:** 2026-10-12  
**Driver:** In-cluster open-loop load generator (k6 constant arrival rate at 50 req/s)  

---

## 1. Executive Summary & The Routing Discovery

Phase 1 was the initial proof-of-concept run designed to measure the baseline reverse-proxy and routing overhead introduced by Ray Serve compared to native Kubernetes serving (FastAPI + Uvicorn + orjson).

In this initial phase, we discovered that **KubeRay default HTTP routing architecture introduces significant proxy bottlenecks**:
- By default, traffic enters a ClusterIP service targeting Ray head node Python Proxy actor.
- The Python proxy parses incoming HTTP connections on an asyncio event loop and forwards requests to worker actors via Ray RPCs.
- This creates an extra network hop, serialization overhead, and event-loop queueing under load.

By activating Ray Serve worker-colocated HAProxy routing (RAY_SERVE_ENABLE_HA_PROXY=1), ingress traffic routes directly to a C-based HAProxy reverse proxy listening on worker port 30000, bypassing the head node and Python proxy entirely.

---

## 2. 3-Way Architectural Comparison Matrix (50 RPS Exploratory Run)

| Metric | Architecture A: Direct Serving (K8s) | Architecture B.1: Ray Serve (Head Python Proxy) | Architecture B.2: Ray Serve (Worker HAProxy :30000) | Ray HAProxy vs. Direct Delta |
|---|---|---|---|---|
| **Routing Path** | ClusterIP to Uvicorn Pod | KubeRay serve-svc to Head Proxy Actor to Worker Actor | haproxy-svc to Worker HAProxy (:30000) to Worker Actor | Fast-path direct worker routing |
| **Total Requests** | 4,624 | 4,624 | 4,624 | 100% load equivalence |
| **Completed Throughput** | 49.98 req/s | 49.98 req/s | 49.98 req/s | 100% goodput match |
| **p50 (Median Latency)** | **1.81 ms** | 2.47 ms | **1.60 ms** | **-0.21 ms (Ray HAProxy faster)** |
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

1. **HAProxy Eliminates the Python Proxy Head Bottleneck:**
   - In Architecture B.1, routing through the head node Python proxy added ~0.66 ms to median latency (1.81 ms to 2.47 ms) and pushed p99 latency to **15.27 ms**.
   - Bypassing the head node via Worker HAProxy on port 30000 reduced median latency down to **1.60 ms** (outperforming Direct Serving 1.81 ms in this initial run) and cut p99 latency by over 5 ms.

2. **Why Phase 1 Was Superseded by Phase 2:**
   - **Cross-AZ Transit Noise:** In Phase 1, Karpenter provisioned worker pods across multiple AWS Availability Zones (us-east-1a, us-east-1d), introducing inter-AZ network hops that varied between trials.
   - **Hardware Heterogeneity:** Direct serving ran on c6i.2xlarge instances, while the Ray head ran on an ARM m8g.xlarge instance.
   - **Single-Trial Scope:** Phase 1 was an exploratory continuous run without randomized trials or confidence intervals.
   - **Conclusion:** Phase 1 proved that **Worker HAProxy is mandatory for low-latency serving**. To obtain publication-grade numbers with zero AZ noise and strictly matched CPU hardware, Phase 1 was formally superseded by the **Phase 2 15-trial single-zone matrix** (docs/report.md).
