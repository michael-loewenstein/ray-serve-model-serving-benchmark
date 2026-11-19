# Phase 3 GPU Rerun — Statistical Summary (gblock1)

- **Model:** sentence-transformers/all-MiniLM-L6-v2
- **Batching:** max_batch_size=32, batch_wait_timeout_s=0.005, max_concurrent_batches=1
- **Co-Location:** Single Availability Zone (us-west-2a), zero cross-AZ transit
- **Total Trials (reconciled):** 60
- **Concurrency Levels:** C=1, C=4, C=8, C=16, C=32, C=64
- **Trials per Arm:** Direct Serving (GPU): 30, Ray Serve (HAProxy :8000): 30
- **Trials per (Arm x Level):** Direct Serving (GPU): 5; Ray Serve (HAProxy :8000): 5

## 1. Per-Arm × Level Latency & Goodput (Median [IQR] / Mean ± 95% CI)

Median [IQR] is non-parametric; mean ± 95% CI uses Student's t with df = n − 1 per (arm, level) group.

| Arm | Level | n | p50 (ms) | p95 (ms) | p99 (ms) | Goodput mean (req/s) | Failed | Flags |
|---|---|---|---|---|---|---|---|---|
| **Direct Serving (GPU)** | C=1 | 5 | 10.317 [0.052] / 10.323 ± 0.074 | 10.851 [0.041] / 10.841 ± 0.054 | 11.507 [0.039] / 11.492 ± 0.229 | 93.02 ± 0.52 | 0 | — |
| **Direct Serving (GPU)** | C=4 | 5 | 10.997 [0.065] / 11.007 ± 0.052 | 11.552 [0.084] / 11.569 ± 0.059 | 12.101 [0.218] / 12.184 ± 0.177 | 349.00 ± 1.21 | 0 | — |
| **Direct Serving (GPU)** | C=8 | 5 | 10.824 [0.008] / 10.821 ± 0.019 | 11.441 [0.019] / 11.442 ± 0.021 | 12.284 [0.067] / 12.305 ± 0.149 | 698.32 ± 1.20 | 0 | — |
| **Direct Serving (GPU)** | C=16 | 5 | 12.411 [0.087] / 12.421 ± 0.102 | 13.366 [0.061] / 13.346 ± 0.058 | 14.428 [0.231] / 14.406 ± 0.197 | 1196.41 ± 10.66 | 0 | — |
| **Direct Serving (GPU)** | C=32 | 5 | 11.814 [0.175] / 11.819 ± 0.120 | 18.334 [0.419] / 18.403 ± 0.302 | 106.011 [0.599] / 105.995 ± 0.520 | 2162.53 ± 14.27 | 0 | — |
| **Direct Serving (GPU)** | C=64 | 5 | 34.531 [0.006] / 34.538 ± 0.042 | 122.541 [0.523] / 121.416 ± 2.822 | 132.829 [0.159] / 132.789 ± 0.286 | 1664.73 ± 4.49 | 0 | **THRESHOLD BREACHED on 5 trial(s)** |
| **Ray Serve (HAProxy :8000)** | C=1 | 5 | 10.406 [0.026] / 10.407 ± 0.023 | 11.195 [0.027] / 11.192 ± 0.025 | 12.330 [0.011] / 12.326 ± 0.016 | 91.52 ± 0.17 | 0 | — |
| **Ray Serve (HAProxy :8000)** | C=4 | 5 | 11.407 [0.003] / 11.404 ± 0.008 | 12.426 [0.145] / 12.430 ± 0.126 | 13.746 [0.133] / 13.773 ± 0.146 | 329.33 ± 0.40 | 0 | — |
| **Ray Serve (HAProxy :8000)** | C=8 | 5 | 11.672 [0.021] / 11.669 ± 0.022 | 13.266 [0.025] / 13.261 ± 0.026 | 21.957 [0.296] / 22.082 ± 0.255 | 626.68 ± 1.45 | 0 | — |
| **Ray Serve (HAProxy :8000)** | C=16 | 5 | 13.431 [0.040] / 13.502 ± 0.230 | 16.263 [0.572] / 16.478 ± 0.655 | 35.580 [0.231] / 35.508 ± 0.177 | 1083.25 ± 15.42 | 0 | — |
| **Ray Serve (HAProxy :8000)** | C=32 | 5 | 19.661 [0.515] / 20.336 ± 2.023 | 33.890 [1.032] / 34.105 ± 0.954 | 43.056 [1.528] / 43.820 ± 2.687 | 1430.80 ± 91.36 | 0 | **THRESHOLD BREACHED on 5 trial(s)** |
| **Ray Serve (HAProxy :8000)** | C=64 | 5 | 40.942 [0.755] / 39.960 ± 2.920 | 60.999 [0.635] / 60.367 ± 2.107 | 70.050 [2.338] / 69.816 ± 2.597 | 1493.79 ± 56.57 | 0 | **THRESHOLD BREACHED on 5 trial(s)** |

## 2. Delta Table (ray_ha − Direct per Level, Bootstrap 95% CI of Median Difference)

Deltas are median differences; CIs are seeded percentile bootstrap (10,000 resamples, seed 42), same algorithm as the Phase 2 analyzer.

| Level | Δp50 (ms) | Δp95 (ms) | Δp99 (ms) |
|---|---|---|---|
| C=1 | +0.089 ms [+0.013, +0.175] | +0.344 ms [+0.298, +0.412] | +0.823 ms [+0.594, +1.112] |
| C=4 | +0.410 ms [+0.346, +0.443] | +0.874 ms [+0.739, +0.999] | +1.645 ms [+1.340, +1.844] |
| C=8 | +0.848 ms [+0.821, +0.875] | +1.825 ms [+1.789, +1.850] | +9.673 ms [+9.454, +10.074] |
| C=16 | +1.020 ms [+0.908, +1.419] | +2.897 ms [+2.649, +3.936] | +21.152 ms [+20.864, +21.350] |
| C=32 | +7.847 ms [+7.513, +11.403] | +15.556 ms [+14.871, +16.754] | -62.955 ms [-64.518, -58.795] |
| C=64 | +6.411 ms [+1.262, +6.793] | -61.542 ms [-65.099, -56.370] | -62.779 ms [-66.202, -60.983] |

## 3. Gate Verdicts

### Gate 1: Low-Load Overhead (C=1 and C=4, Δp95 <= 3.0 ms OR <= 10%)

**Verdict: PASS (absolute)**

| Level | Direct p95 (median, ms) | ray_ha p95 (median, ms) | Δp95 (ms) | Δp95 (relative) | Absolute Gate (<= 3.0 ms) | Relative Gate (<= 10%) |
|---|---|---|---|---|---|---|
| C=1 | 10.851 | 11.195 | +0.344 | +3.2% | PASS | PASS |
| C=4 | 11.552 | 12.426 | +0.874 | +7.6% | PASS | PASS |

### Gate 2: Goodput Retention (ray_ha >= 95% of direct mean throughput per level)

| Level | Verdict |
|---|---|
| C=1 | PASS (ray_ha 91.52 req/s = 98.39% of direct 93.02 req/s; gate >= 95%) |
| C=4 | FAIL (ray_ha 329.33 req/s = 94.37% of direct 349.00 req/s; gate >= 95%) |
| C=8 | FAIL (ray_ha 626.68 req/s = 89.74% of direct 698.32 req/s; gate >= 95%) |
| C=16 | FAIL (ray_ha 1083.25 req/s = 90.54% of direct 1196.41 req/s; gate >= 95%) |
| C=32 | FAIL (ray_ha 1430.80 req/s = 66.16% of direct 2162.53 req/s; gate >= 95%) |
| C=64 | FAIL (ray_ha 1493.79 req/s = 89.73% of direct 1664.73 req/s; gate >= 95%) |

**Overall Verdict: FAIL (at least one level below 95% goodput retention)**

### Gate 3: Batch Scaling (ray arm, C=32 vs C=1)

Throughput(C=32) >= 3 × throughput(C=1) **and** p95(C=32) <= 20.0 ms.

- ray_ha mean throughput C=1: **91.52 req/s**, C=32: **1430.80 req/s** (x15.63; gate: >= x3) → PASS
- ray_ha p95 median at C=32: **33.890 ms** (gate: <= 20.0 ms) → FAIL
**Verdict: FAIL**

### Gate 4: C=64 Saturation (ray_ha p99 <= direct p99 + 3.0 ms)

- direct p99 median at C=64: **132.829 ms**
- ray_ha p99 median at C=64: **70.050 ms**
- Δp99 (ray_ha − direct) at C=64: **-62.779 ms** (gate: <= +3.0 ms)
**Verdict: PASS**
