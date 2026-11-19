# Statistical Benchmark Summary Report

- **Workload:** Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, depth 6)
- **Hardware:** AWS c6i.2xlarge On-Demand (Intel Xeon 8375C @ 2.90GHz)
- **Co-Location:** Single Availability Zone (us-west-2a), zero cross-AZ transit
- **Total Requests Reconciled:** 46519 across 31 trials in 3 blocks
- **Trials per Architecture:** Direct Serving (Uvicorn): 10, Ray Serve (Direct Ingress :30000): 5, Ray Serve (HAProxy :8000): 5, Ray Serve (Default Python Proxy :8000): 5, Ray Serve (Direct Ingress local control): 3, Ray Serve (HAProxy local control): 3
- **Trials per Block:** block1: 15, block2: 10, block3: 6
- **Runs per Arm per Block:** block1: 5, block2: 5, block3: 3

## 1. Formal Evaluation of Hypothesis H1 (Serving Overhead Gate)

Hypothesis H1 evaluates whether Ray Serve satisfies the absolute gate (overhead <= 3.0 ms) and/or the relative gate (overhead <= 10.0% over Direct Serving baseline).

Each Ray arm is compared against the Direct Serving trials of the **same block**. Local-control arms whose block has no Direct baseline are skipped here and reported in the HAProxy-only control table below.

| Arm | Block | Metric | Direct-serving baseline (median) | Arm (median) | Absolute delta [Bootstrap 95% CI] | Relative delta | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |
|---|---|---|---|---|---|---|---|---|
| **Ray Serve (Direct Ingress :30000)** | block1 | **p50** | **1.074 ms** | **1.212 ms** | **+0.138 ms** [+0.031, +0.232] | **+12.8%** | **PASS** | **FAIL** |
| **Ray Serve (Direct Ingress :30000)** | block1 | **p90** | **1.281 ms** | **1.375 ms** | **+0.094 ms** [+0.063, +0.111] | **+7.3%** | **PASS** | **PASS** |
| **Ray Serve (Direct Ingress :30000)** | block1 | **p95** | **1.322 ms** | **1.419 ms** | **+0.097 ms** [+0.052, +0.119] | **+7.3%** | **PASS** | **PASS** |
| **Ray Serve (Direct Ingress :30000)** | block1 | **p99** | **1.478 ms** | **1.615 ms** | **+0.137 ms** [+0.085, +0.187] | **+9.3%** | **PASS** | **PASS** |
| **Ray Serve (HAProxy :8000)** | block1 | **p50** | **1.074 ms** | **1.388 ms** | **+0.314 ms** [+0.219, +0.408] | **+29.2%** | **PASS** | **FAIL** |
| **Ray Serve (HAProxy :8000)** | block1 | **p90** | **1.281 ms** | **1.799 ms** | **+0.518 ms** [+0.487, +0.562] | **+40.4%** | **PASS** | **FAIL** |
| **Ray Serve (HAProxy :8000)** | block1 | **p95** | **1.322 ms** | **1.845 ms** | **+0.523 ms** [+0.478, +0.570] | **+39.6%** | **PASS** | **FAIL** |
| **Ray Serve (HAProxy :8000)** | block1 | **p99** | **1.478 ms** | **2.028 ms** | **+0.550 ms** [+0.473, +0.623] | **+37.2%** | **PASS** | **FAIL** |
| **Ray Serve (Default Python Proxy :8000)** | block2 | **p50** | **1.138 ms** | **4.655 ms** | **+3.517 ms** [+2.969, +3.983] | **+309.1%** | **FAIL** | **FAIL** |
| **Ray Serve (Default Python Proxy :8000)** | block2 | **p90** | **1.296 ms** | **5.588 ms** | **+4.292 ms** [+4.217, +4.427] | **+331.2%** | **FAIL** | **FAIL** |
| **Ray Serve (Default Python Proxy :8000)** | block2 | **p95** | **1.342 ms** | **5.752 ms** | **+4.410 ms** [+4.321, +4.540] | **+328.6%** | **FAIL** | **FAIL** |
| **Ray Serve (Default Python Proxy :8000)** | block2 | **p99** | **1.579 ms** | **6.063 ms** | **+4.484 ms** [+4.396, +4.778] | **+284.0%** | **FAIL** | **FAIL** |

## 2. HAProxy-only control (HAProxy local vs. Direct-Ingress local)

Isolates the HAProxy routing overhead within the local-control block: `ray_haproxy_local` minus `ray_direct_local`, both served from the same worker node.

| Metric | Direct Ingress local control (median) | HAProxy local control (median) | Absolute delta [Bootstrap 95% CI] |
|---|---|---|---|
| **p50** | **1.271 ms** | **1.412 ms** | **+0.141 ms** [+0.118, +0.159] |
| **p90** | **1.397 ms** | **1.526 ms** | **+0.129 ms** [+0.114, +0.165] |
| **p95** | **1.477 ms** | **1.589 ms** | **+0.112 ms** [+0.069, +0.147] |
| **p99** | **1.779 ms** | **1.810 ms** | **+0.031 ms** [-0.033, +0.124] |

## 3. Aggregate Latency & Goodput Across Independent Trials

Statistical Note: n is the number of independent trials per arm; we report both:
- Non-parametric: **Median [IQR]** and [Min, Max]
- Parametric: **Mean ± 95% CI** using Student's t distribution with df = n − 1 per arm
- Per-arm n: Direct Serving (Uvicorn) n=10 (t = 2.262); Ray Serve (Direct Ingress :30000) n=5 (t = 2.776); Ray Serve (HAProxy :8000) n=5 (t = 2.776); Ray Serve (Default Python Proxy :8000) n=5 (t = 2.776); Ray Serve (Direct Ingress local control) n=3 (t = 4.303); Ray Serve (HAProxy local control) n=3 (t = 4.303)
- Each row pools an architecture's trials across every block it ran in; the H1 deltas in Section 1 use same-block Direct baselines only.

| Architecture | Goodput (req/s) | p50 Median [IQR] (ms) | p50 Mean ± 95% CI (ms) | p90 Median (ms) | p95 Median [IQR] (ms) | p95 Mean ± 95% CI (ms) | p99 Median [IQR] (ms) | p99 Mean ± 95% CI (ms) | Failure Rate |
|---|---|---|---|---|---|---|---|---|---|
| **Direct Serving (Uvicorn)** | 50.03 (Mean: 50.02 ± 0.011) | **1.131** [0.097] | 1.105 ± 0.053 | 1.289 | **1.333** [0.045] | 1.342 ± 0.018 | **1.513** [0.101] | 1.523 ± 0.049 | 0.00% (0/15006) |
| **Ray Serve (Direct Ingress :30000)** | 50.03 (Mean: 50.02 ± 0.020) | **1.212** [0.037] | 1.211 ± 0.048 | 1.375 | **1.419** [0.005] | 1.422 ± 0.015 | **1.615** [0.008] | 1.609 ± 0.035 | 0.00% (0/7503) |
| **Ray Serve (HAProxy :8000)** | 50.03 (Mean: 50.02 ± 0.020) | **1.388** [0.007] | 1.393 ± 0.014 | 1.799 | **1.845** [0.046] | 1.858 ± 0.031 | **2.028** [0.076] | 2.022 ± 0.061 | 0.00% (0/7503) |
| **Ray Serve (Default Python Proxy :8000)** | 50.03 (Mean: 50.02 ± 0.020) | **4.655** [0.451] | 4.703 ± 0.503 | 5.588 | **5.752** [0.153] | 5.776 ± 0.117 | **6.063** [0.178] | 6.137 ± 0.192 | 0.00% (0/7503) |
| **Ray Serve (Direct Ingress local control)** | 50.03 (Mean: 50.02 ± 0.043) | **1.271** [0.020] | 1.268 ± 0.026 | 1.397 | **1.477** [0.054] | 1.475 ± 0.067 | **1.779** [0.077] | 1.767 ± 0.099 | 0.00% (0/4502) |
| **Ray Serve (HAProxy local control)** | 50.03 (Mean: 50.02 ± 0.043) | **1.412** [0.021] | 1.407 ± 0.028 | 1.526 | **1.589** [0.024] | 1.584 ± 0.031 | **1.810** [0.080] | 1.808 ± 0.099 | 0.00% (0/4502) |

## 4. Server Compute vs. Network Transit Breakdown (Median ms)

| Architecture | Pure Booster Predict (p50) | Server Total incl. JSON (p50) | Network & Proxy Transit Overhead (p50) |
|---|---|---|---|
| **Direct Serving (Uvicorn)** | 0.554 ms | 0.576 ms | 0.555 ms |
| **Ray Serve (Direct Ingress :30000)** | 0.501 ms | 0.542 ms | 0.680 ms |
| **Ray Serve (HAProxy :8000)** | 0.514 ms | 0.556 ms | 0.877 ms |
| **Ray Serve (Default Python Proxy :8000)** | 0.494 ms | 1.904 ms | 2.774 ms |
| **Ray Serve (Direct Ingress local control)** | 0.534 ms | 0.577 ms | 0.691 ms |
| **Ray Serve (HAProxy local control)** | 0.533 ms | 0.578 ms | 0.829 ms |

## 5. Complete Individual Trial Measurements (31 Trials)

| Architecture | Block | Trial # | Reqs | Goodput (req/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|---|---|---|---|---|
| Direct Serving (Uvicorn) | block1 | 1 | 1501 | 50.03 | 1.074 | 1.275 | 1.334 | 1.500 | 17.19 |
| Direct Serving (Uvicorn) | block1 | 2 | 1501 | 50.03 | 1.169 | 1.312 | 1.367 | 1.503 | 2.28 |
| Direct Serving (Uvicorn) | block1 | 3 | 1500 | 50.00 | 1.129 | 1.287 | 1.322 | 1.478 | 2.24 |
| Direct Serving (Uvicorn) | block1 | 4 | 1500 | 50.00 | 0.980 | 1.267 | 1.314 | 1.445 | 2.55 |
| Direct Serving (Uvicorn) | block1 | 5 | 1500 | 50.00 | 1.072 | 1.281 | 1.316 | 1.429 | 2.43 |
| Direct Serving (Uvicorn) | block2 | 1 | 1501 | 50.03 | 1.190 | 1.341 | 1.390 | 1.600 | 2.10 |
| Direct Serving (Uvicorn) | block2 | 2 | 1501 | 50.03 | 1.138 | 1.292 | 1.330 | 1.524 | 2.05 |
| Direct Serving (Uvicorn) | block2 | 3 | 1501 | 50.03 | 1.133 | 1.284 | 1.332 | 1.523 | 1.90 |
| Direct Serving (Uvicorn) | block2 | 4 | 1500 | 50.00 | 1.175 | 1.315 | 1.369 | 1.644 | 2.15 |
| Direct Serving (Uvicorn) | block2 | 5 | 1501 | 50.03 | 0.989 | 1.296 | 1.342 | 1.579 | 2.22 |
| Ray Serve (Direct Ingress :30000) | block1 | 1 | 1501 | 50.03 | 1.255 | 1.375 | 1.418 | 1.641 | 2.25 |
| Ray Serve (Direct Ingress :30000) | block1 | 2 | 1500 | 50.00 | 1.212 | 1.363 | 1.408 | 1.563 | 2.09 |
| Ray Serve (Direct Ingress :30000) | block1 | 3 | 1500 | 50.00 | 1.237 | 1.386 | 1.441 | 1.615 | 2.74 |
| Ray Serve (Direct Ingress :30000) | block1 | 4 | 1501 | 50.03 | 1.200 | 1.381 | 1.423 | 1.608 | 1.97 |
| Ray Serve (Direct Ingress :30000) | block1 | 5 | 1501 | 50.03 | 1.153 | 1.369 | 1.419 | 1.616 | 6.94 |
| Ray Serve (HAProxy :8000) | block1 | 1 | 1500 | 50.00 | 1.388 | 1.799 | 1.845 | 2.028 | 2.78 |
| Ray Serve (HAProxy :8000) | block1 | 2 | 1500 | 50.00 | 1.383 | 1.789 | 1.838 | 1.969 | 2.81 |
| Ray Serve (HAProxy :8000) | block1 | 3 | 1501 | 50.03 | 1.395 | 1.829 | 1.886 | 2.084 | 2.55 |
| Ray Serve (HAProxy :8000) | block1 | 4 | 1501 | 50.03 | 1.411 | 1.839 | 1.884 | 2.052 | 2.70 |
| Ray Serve (HAProxy :8000) | block1 | 5 | 1501 | 50.03 | 1.388 | 1.785 | 1.838 | 1.976 | 2.57 |
| Ray Serve (Default Python Proxy :8000) | block2 | 1 | 1500 | 50.00 | 5.043 | 5.723 | 5.876 | 6.233 | 8.80 |
| Ray Serve (Default Python Proxy :8000) | block2 | 2 | 1501 | 50.03 | 5.116 | 5.558 | 5.752 | 6.063 | 8.50 |
| Ray Serve (Default Python Proxy :8000) | block2 | 3 | 1500 | 50.00 | 4.107 | 5.527 | 5.663 | 5.975 | 8.38 |
| Ray Serve (Default Python Proxy :8000) | block2 | 4 | 1501 | 50.03 | 4.655 | 5.588 | 5.717 | 6.055 | 8.13 |
| Ray Serve (Default Python Proxy :8000) | block2 | 5 | 1501 | 50.03 | 4.592 | 5.700 | 5.870 | 6.357 | 9.99 |
| Ray Serve (Direct Ingress local control) | block3 | 1 | 1501 | 50.03 | 1.276 | 1.398 | 1.501 | 1.779 | 2.52 |
| Ray Serve (Direct Ingress local control) | block3 | 2 | 1501 | 50.03 | 1.256 | 1.366 | 1.447 | 1.723 | 2.40 |
| Ray Serve (Direct Ingress local control) | block3 | 3 | 1500 | 50.00 | 1.271 | 1.397 | 1.477 | 1.800 | 2.80 |
| Ray Serve (HAProxy local control) | block3 | 1 | 1501 | 50.03 | 1.412 | 1.526 | 1.589 | 1.847 | 2.36 |
| Ray Serve (HAProxy local control) | block3 | 2 | 1501 | 50.03 | 1.394 | 1.512 | 1.570 | 1.767 | 2.48 |
| Ray Serve (HAProxy local control) | block3 | 3 | 1500 | 50.00 | 1.415 | 1.531 | 1.594 | 1.810 | 3.24 |
