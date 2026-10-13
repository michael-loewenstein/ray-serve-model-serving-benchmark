# Statistical Benchmark Summary Report

- **Workload:** Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, depth 6)
- **Hardware:** AWS c6i.2xlarge On-Demand (Intel Xeon 8375C @ 2.90GHz)
- **Co-Location:** Single Availability Zone (us-east-1d), zero cross-AZ transit
- **Total Requests Reconciled:** 22508 across 15 randomized trials
- **Trials per Architecture:** 5 independent trials

## 1. Formal Evaluation of Hypothesis H1 (Serving Overhead Gate)

Hypothesis H1 evaluates whether Ray Serve satisfies the absolute gate (overhead <= 3.0 ms) and/or the relative gate (overhead <= 10.0% over Direct Serving baseline).

| Metric | Direct Serving Baseline (Median) | Ray Serve Worker HAProxy (Median) | Absolute Delta [Bootstrap 95% CI] | Relative Delta | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |
|---|---|---|---|---|---|---|
| **p50** | **0.973 ms** | **1.274 ms** | **+0.301 ms** [+0.154, +0.445] | **+30.9%** | **PASS** | **FAIL** |
| **p90** | **1.151 ms** | **1.768 ms** | **+0.617 ms** [+0.427, +0.691] | **+53.6%** | **PASS** | **FAIL** |
| **p95** | **1.246 ms** | **1.856 ms** | **+0.610 ms** [+0.310, +0.670] | **+49.0%** | **PASS** | **FAIL** |
| **p99** | **1.444 ms** | **2.595 ms** | **+1.151 ms** [+0.404, +1.813] | **+79.7%** | **PASS** | **FAIL** |

## 2. Aggregate Latency & Goodput Across Independent Trials

Statistical Note: To maintain statistical rigor with n=5 independent trials, we report both:
- Non-parametric: **Median [IQR]** and [Min, Max]
- Parametric: **Mean ± 95% CI** using Student's t distribution (df = 4, t = 2.776)

| Architecture | Goodput (req/s) | p50 Median [IQR] (ms) | p50 Mean ± 95% CI (ms) | p90 Median (ms) | p95 Median [IQR] (ms) | p95 Mean ± 95% CI (ms) | p99 Median [IQR] (ms) | p99 Mean ± 95% CI (ms) | Failure Rate |
|---|---|---|---|---|---|---|---|---|---|
| **Direct Serving (Uvicorn)** | 50.00 (Mean: 50.01 ± 0.017) | **0.973** [0.006] | 1.001 ± 0.083 | 1.151 | **1.246** [0.017] | 1.302 ± 0.170 | **1.444** [0.022] | 1.533 ± 0.242 | 0.00% (0/7501) |
| **Ray Serve (Python Proxy)** | 50.03 (Mean: 50.02 ± 0.020) | **1.478** [0.129] | 1.530 ± 0.145 | 1.959 | **2.069** [0.465] | 3.094 ± 2.781 | **4.865** [3.537] | 6.320 ± 7.338 | 0.00% (0/7503) |
| **Ray Serve (Worker HAProxy :30000)** | 50.03 (Mean: 50.02 ± 0.017) | **1.274** [0.119] | 1.315 ± 0.097 | 1.768 | **1.856** [0.116] | 1.835 ± 0.087 | **2.595** [0.771] | 2.527 ± 0.702 | 0.00% (0/7504) |

## 3. Server Compute vs. Network Transit Breakdown (Median ms)

| Architecture | Pure Booster Predict (p50) | Server Total incl. JSON (p50) | Network & Proxy Transit Overhead (p50) |
|---|---|---|---|
| **Direct Serving (Uvicorn)** | 0.503 ms | 0.527 ms | 0.444 ms |
| **Ray Serve (Python Proxy)** | 0.612 ms | 0.627 ms | 0.896 ms |
| **Ray Serve (Worker HAProxy :30000)** | 0.603 ms | 0.618 ms | 0.709 ms |

## 4. Complete Individual Trial Measurements (15 Trials)

| Architecture | Trial # | Reqs | Goodput (req/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|---|---|---|---|
| Direct Serving (Uvicorn) | 1 | 1500 | 50.00 | 1.120 | 1.341 | 1.546 | 1.881 | 12.87 |
| Direct Serving (Uvicorn) | 2 | 1500 | 50.00 | 0.973 | 1.151 | 1.239 | 1.435 | 16.42 |
| Direct Serving (Uvicorn) | 3 | 1500 | 50.00 | 0.965 | 1.129 | 1.256 | 1.464 | 14.19 |
| Direct Serving (Uvicorn) | 4 | 1501 | 50.03 | 0.971 | 1.139 | 1.225 | 1.444 | 15.02 |
| Direct Serving (Uvicorn) | 5 | 1500 | 50.00 | 0.977 | 1.156 | 1.246 | 1.442 | 13.95 |
| Ray Serve (Python Proxy) | 1 | 1501 | 50.03 | 1.443 | 1.821 | 1.954 | 2.268 | 21.04 |
| Ray Serve (Python Proxy) | 2 | 1500 | 50.00 | 1.442 | 1.838 | 1.942 | 2.163 | 18.72 |
| Ray Serve (Python Proxy) | 3 | 1501 | 50.03 | 1.478 | 1.959 | 2.069 | 4.865 | 29.41 |
| Ray Serve (Python Proxy) | 4 | 1500 | 50.00 | 1.716 | 4.312 | 7.087 | 16.501 | 48.91 |
| Ray Serve (Python Proxy) | 5 | 1501 | 50.03 | 1.572 | 2.115 | 2.419 | 5.805 | 32.16 |
| Ray Serve (Worker HAProxy :30000) | 1 | 1500 | 50.00 | 1.274 | 1.691 | 1.763 | 2.071 | 2.86 |
| Ray Serve (Worker HAProxy :30000) | 2 | 1501 | 50.03 | 1.245 | 1.688 | 1.760 | 1.868 | 2.91 |
| Ray Serve (Worker HAProxy :30000) | 3 | 1501 | 50.03 | 1.259 | 1.768 | 1.856 | 3.257 | 14.81 |
| Ray Serve (Worker HAProxy :30000) | 4 | 1501 | 50.03 | 1.418 | 1.815 | 1.879 | 2.842 | 11.23 |
| Ray Serve (Worker HAProxy :30000) | 5 | 1501 | 50.03 | 1.378 | 1.842 | 1.916 | 2.595 | 9.47 |
