# Phase 2 Rerun (2026-11-18): HAProxy Ingress Verified on `:8000`

**Status:** Final. 31 trials across 3 blocks on a dedicated single-AZ EKS cluster; all three preflight gates green before data collection; 0.00% failures on every arm.

**Data:** `results/phase2-rerun-2026-11-18/raw_trials.json` (31 trial records), per-trial k6 JSONs in `results/phase2-rerun-2026-11-18/block{1,2,3}/`, generated statistics in `results/phase2-rerun-2026-11-18/summary_report.md` (regenerated on 2026-11-19 after a header/formatting fix in `analyze.py`; no measured value changed). Measured numbers below are quoted from the generated summary. Some numbers were computed by hand and are labeled *derived* where they appear: the §5.4 cross-arm differences and HAProxy shares, the control's relative delta (11.1%), and the goodput ratios and block-specific goodput medians in §5.3 and §6.

**Why this rerun exists:** The October 2026 Phase 2 run labeled its `:30000` arm "Worker HAProxy." Port-role verification showed that `:30000` is the replica's direct-ingress HTTP server, not HAProxy — no arm in that dataset ever traversed HAProxy. This rerun measures Ray Serve's actual HAProxy ingress on `:8000` (Ray 2.60.0, `RAY_SERVE_ENABLE_HA_PROXY=1`), retaining direct ingress `:30000` and the default Python proxy `:8000` as comparison arms, and adds a single-replica local control pair that isolates the HAProxy process from cross-node load balancing. The October dataset is historical and superseded; it is unchanged apart from the earlier label correction (14faddd).

---

## 1. Environment

| Component | Version |
|---|---|
| Ray / Ray Serve | 2.60.0 |
| KubeRay operator | 1.7.2 (`quay.io/kuberay/operator:v1.7.2`) |
| Kubernetes (EKS) | 1.34 (nodes: `v1.34.12-eks-9adbfea`, Amazon Linux 2023, kernel 6.12.115, containerd 2.3.7) |
| k6 | 0.54.0 |
| HAProxy | 2.8.25 (bundled with Ray, resolved via the `ray_haproxy` wheel binary) |
| Workload model | Open-Source CPU XGBoost Binary Classifier (50 numeric features, 100 trees, depth 6) |

**Topology (from `results/phase2-rerun-2026-11-18/preflight/nodes.txt`):**

- 4 dedicated AWS `c6i.2xlarge` nodes (Intel Xeon 8375C @ 2.90 GHz), all in a single Availability Zone, `us-west-2a`: 1 head, 2 workers (`bench-slot` w1 and w2), 1 load generator. Zero cross-AZ hops in the data plane.
- **Region deviation (disclosed):** the original placement in `us-east-1` was blocked by account quota limits — "VPCs per Region" and "Internet gateways per Region" were each at 30/30 in use, and cluster creation failed at the internet-gateway stage ($0 spent; the failed CFN stack was rolled back). The run moved to `us-west-2`, where quotas had ample headroom. The eksctl control-plane VPC spans `us-west-2a`+`us-west-2b`; all data-plane nodes carry `ZONE=us-west-2a` in the capture.
- Block 1 ran 2 Serve replicas spread one-per-worker under HAProxy `leastconn` balancing, so `:8000` requests crossed node boundaries to reach replicas. Block 3 pinned 1 replica to worker w1 and served the HAProxy control from the same node, removing cross-node transit from that comparison.

## 2. Preflight gates (all green)

Every block was preceded by a `preflight.sh` gate run that exits 0 and prints banner PASS only if all checks pass. Evidence lives in `results/phase2-rerun-2026-11-18/preflight/`.

| Gate | Result | Key evidence | Evidence file |
|---|---|---|---|
| `haproxy` (block 1) | PASS | 2 healthy replicas, `max_ongoing_requests=1000`; **HAProxy live request counters moved 352 → 374 across 10 gate requests** (per-worker 178→188, 174→186) — proof HAProxy is in the request path on `:8000`; 10/10 HTTP 200 with numeric predictions via `ray-haproxy-svc`, `ray-direct-ingress-svc`, and `direct-serving-svc` | `preflight/haproxy-preflight.log` (green run + close-out confirmation), `preflight/haproxy-preflight-diagnostics.log` (earlier placement diagnostics) |
| `local` (block 3) | PASS | 1 healthy replica on w1; HAProxy counters 16 → 36 across 10 gate requests on `ray-haproxy-w1-svc`; 10/10 HTTP 200 via `ray-haproxy-w1-svc`, `ray-direct-w1-svc`, `direct-serving-svc` | `preflight/local-preflight.log` |
| `python` (block 2) | PASS | 3 Python proxy actors and 0 HAProxyManager; `:8000` listener on both workers; direct ingress disabled by configuration (`RAY_SERVE_ENABLE_DIRECT_INGRESS=0`); the python-mode gate does not assert listener absence; `ray-python-proxy-svc` has 2 worker endpoints; 2 RUNNING replicas; 10/10 HTTP 200 | `preflight/python-preflight.log` |

**Live runtime env captures** (per a mid-run ruling, `env | grep RAY_SERVE` was captured from the head and one worker during block 1; see `preflight/ray-serve-live-env-head.txt` and `preflight/ray-serve-live-env-worker.txt`):

```
RAY_SERVE_KV_TIMEOUT_S=5
RAY_SERVE_THROUGHPUT_OPTIMIZED=1
RAY_SERVE_ENABLE_HA_PROXY=1
```

The HAProxy flag was live at runtime, not just declared in the manifest.

Other preflight captures: `preflight/cluster-create.log`, `preflight/kuberay-image.txt`, `preflight/nodes.txt`, and `preflight/us-west-2-clusters-before.json` / `preflight/us-west-2-clusters-after-teardown.json`. The two cluster lists were redacted on 2026-11-19 to a count, a SHA-256 of the sorted names, and a flag for whether `ray-serve-phase2-rerun` is present, because they listed unrelated clusters in a shared account. Both files record 25 clusters with equal hashes, so the before and after sets are identical, and neither contains the rerun cluster.

## 3. Trial protocol

- 31 trial records total: block 1 = 15 (5 Direct + 5 direct ingress + 5 HAProxy), block 2 = 10 (5 Direct + 5 Python proxy), block 3 = 6 (3 + 3 local controls). Five 30-second trials per benchmark arm, three per local-control arm, each preceded by a warmup (recorded as `warmup_*.json`, excluded from analysis).
- Trial order was shuffled within each block by `loadtest/run_matrix.py`. The runs were invoked with block 1 `--seed 42`, block 3 `--seed 43`, and block 2 `--seed 44` (blocks ran in the order 1, 3, 2). Each block's execution order is recorded in the runner's console logs, committed in `results/phase2-rerun-2026-11-18/run-logs/`. Blocks 1 and 2 were interleaved. **Block 3 was not:** the seed-43 shuffle produced `ray_direct_local#3, #2, #1, ray_haproxy_local#2, #3, #1`, so all direct-ingress-local trials ran before all HAProxy-local trials (see §5.3 and §8).
- Open-loop load: k6 `constant-arrival-rate` at 50 req/s, 25 VUs, ~1,500 requests per trial. Total reconciled requests across all arms: 46,519 across 31 trials in 3 blocks (summary header). Per arm: 15,006 Direct across its two baseline blocks, 7,503 per Ray arm × 3, and 4,502 per control arm × 2.
- **Measurement boundaries changed from the October run (timer alignment).** Before any data was collected, commit 21ee7d1 moved the Ray app's `t0` to the first line of the request handler, before `await request.body()` (design §9.5). The Direct app already started its timer there, so both apps now time the same span. `server_total` covers the body read, JSON parse and model predict, and excludes response serialization. `server_model_latency_ms` brackets XGBoost predict, `http_req_duration` is the full k6 round trip, and transit is the per-request difference between client duration and `server_total`. In October, the Ray timer started after the body read, so **October transit and `server_total` numbers are not like-for-like with the rerun**.
- **Arm B (default Python proxy) caveat:** the replica's `await request.body()` includes the proxy-to-replica transfer of the body. Arm B's `server_total` is therefore inflated (1.904 ms median), and its "transit" column (2.774 ms) understates the proxy's cost. Compare arm B on end-to-end latency, not on the server/transit split.

## 4. Absolute results per arm

Quoted verbatim from Section 3 of the generated summary (`results/phase2-rerun-2026-11-18/summary_report.md`). Direct Serving spans its two baseline blocks (block 1 and block 2); each Ray arm is compared against the Direct trials of its own block in Section 5.

| Architecture | Goodput (req/s) | p50 Median [IQR] (ms) | p50 Mean ± 95% CI (ms) | p95 Median [IQR] (ms) | p95 Mean ± 95% CI (ms) | p99 Median [IQR] (ms) | p99 Mean ± 95% CI (ms) | Failure Rate |
|---|---|---|---|---|---|---|---|---|
| **Direct Serving (Uvicorn)** | 50.03 (Mean: 50.02 ± 0.011) | **1.131** [0.097] | 1.105 ± 0.053 | **1.333** [0.045] | 1.342 ± 0.018 | **1.513** [0.101] | 1.523 ± 0.049 | 0.00% (0/15006) |
| **Ray Serve (Direct Ingress :30000)** | 50.03 (Mean: 50.02 ± 0.020) | **1.212** [0.037] | 1.211 ± 0.048 | **1.419** [0.005] | 1.422 ± 0.015 | **1.615** [0.008] | 1.609 ± 0.035 | 0.00% (0/7503) |
| **Ray Serve (HAProxy :8000)** | 50.03 (Mean: 50.02 ± 0.020) | **1.388** [0.007] | 1.393 ± 0.014 | **1.845** [0.046] | 1.858 ± 0.031 | **2.028** [0.076] | 2.022 ± 0.061 | 0.00% (0/7503) |
| **Ray Serve (Default Python Proxy :8000)** | 50.03 (Mean: 50.02 ± 0.020) | **4.655** [0.451] | 4.703 ± 0.503 | **5.752** [0.153] | 5.776 ± 0.117 | **6.063** [0.178] | 6.137 ± 0.192 | 0.00% (0/7503) |
| **Ray Serve (Direct Ingress local control)** | 50.03 (Mean: 50.02 ± 0.043) | **1.271** [0.020] | 1.268 ± 0.026 | **1.477** [0.054] | 1.475 ± 0.067 | **1.779** [0.077] | 1.767 ± 0.099 | 0.00% (0/4502) |
| **Ray Serve (HAProxy local control)** | 50.03 (Mean: 50.02 ± 0.043) | **1.412** [0.021] | 1.407 ± 0.028 | **1.589** [0.024] | 1.584 ± 0.031 | **1.810** [0.080] | 1.808 ± 0.099 | 0.00% (0/4502) |

Goodput is statistically indistinguishable at this 50 req/s admission-free rate — differences between arms appear entirely in latency, not throughput. Transit/breakdown medians from the summary (Section 4): Direct transit 0.555 ms; direct ingress 0.680 ms; HAProxy 0.877 ms; Python proxy 2.774 ms. The Python proxy's server-total median is 1.904 ms, against 0.542–0.578 ms elsewhere, because its replica-side body read includes the proxy-to-replica transfer (§3). Its transit figure therefore understates the proxy's cost.

## 5. Deltas vs. block-specific Direct baselines (H1)

Quoted from Section 1 of the generated summary. Each Ray arm is compared only against the Direct Serving trials of the same block; the block 3 local-control arms have no Direct baseline in-block and are therefore reported in the HAProxy-only control table (Section 5.3).

| Arm | Block | Metric | Direct baseline (median) | Arm (median) | Absolute delta [Bootstrap 95% CI] | Relative delta |
|---|---|---|---|---|---|---|
| Ray Serve (Direct Ingress :30000) | block1 | p50 | 1.074 ms | 1.212 ms | **+0.138 ms** [+0.031, +0.232] | +12.8% |
| Ray Serve (Direct Ingress :30000) | block1 | p95 | 1.322 ms | 1.419 ms | **+0.097 ms** [+0.052, +0.119] | +7.3% |
| Ray Serve (Direct Ingress :30000) | block1 | p99 | 1.478 ms | 1.615 ms | **+0.137 ms** [+0.085, +0.187] | +9.3% |
| Ray Serve (HAProxy :8000) | block1 | p50 | 1.074 ms | 1.388 ms | **+0.314 ms** [+0.219, +0.408] | +29.2% |
| Ray Serve (HAProxy :8000) | block1 | p95 | 1.322 ms | 1.845 ms | **+0.523 ms** [+0.478, +0.570] | +39.6% |
| Ray Serve (HAProxy :8000) | block1 | p99 | 1.478 ms | 2.028 ms | **+0.550 ms** [+0.473, +0.623] | +37.2% |
| Ray Serve (Default Python Proxy :8000) | block2 | p50 | 1.138 ms | 4.655 ms | **+3.517 ms** [+2.969, +3.983] | +309.1% |
| Ray Serve (Default Python Proxy :8000) | block2 | p95 | 1.342 ms | 5.752 ms | **+4.410 ms** [+4.321, +4.540] | +328.6% |
| Ray Serve (Default Python Proxy :8000) | block2 | p99 | 1.579 ms | 6.063 ms | **+4.484 ms** [+4.396, +4.778] | +284.0% |

At p90 (per the summary's full table): direct ingress +0.094 ms [+0.063, +0.111] / +7.3%; HAProxy +0.518 ms [+0.487, +0.562] / +40.4%; Python proxy +4.292 ms [+4.217, +4.427] / +331.2%.

### 5.3 HAProxy-only control (block 3): `ray_haproxy_local` − `ray_direct_local`

Quoted from Section 2 of the generated summary. Both arms are single-replica and served from the same worker node (w1), which removes cross-node load balancing from this comparison. **The control is blocked, not interleaved:** the seed-43 shuffle ran `ray_direct_local#3, #2, #1, ray_haproxy_local#2, #3, #1` (`run-logs/block3-run.log`), so all three direct-ingress-local trials ran before all three HAProxy-local trials. Any time drift between those two sub-blocks cannot be told apart from the HAProxy effect, so treat these deltas as indicative.

| Metric | Direct Ingress local control (median) | HAProxy local control (median) | Absolute delta [Bootstrap 95% CI] |
|---|---|---|---|
| p50 | 1.271 ms | 1.412 ms | **+0.141 ms** [+0.118, +0.159] |
| p90 | 1.397 ms | 1.526 ms | **+0.129 ms** [+0.114, +0.165] |
| p95 | 1.477 ms | 1.589 ms | **+0.112 ms** [+0.069, +0.147] |
| p99 | 1.779 ms | 1.810 ms | **+0.031 ms** [-0.033, +0.124] |

This control is diagnostic (design §12) and is not part of the headline gate table in §6. Against the same gate definitions, with the direct-ingress local control as the baseline: Δp99 is +0.031 ms [-0.033, +0.124], under the 3.0 ms absolute threshold. Δp50 is +0.141 ms, or +11.1% of 1.271 ms (*derived*), over the 10% relative threshold. Both arms recorded 0.00% failures (0/4502 each), and their goodput medians are 50.03 vs 50.03 req/s (100%, *derived*).

### 5.4 Overhead decomposition (indicative)

This section subtracts arms that ran in different blocks and configurations, and the block 3 control it subtracts was run blocked rather than interleaved (§5.3). The split is therefore **indicative**, not measured. §8 lists the confounds. All differences and shares below are *derived* by hand from the summary's per-arm medians (Section 3) and control deltas (Section 2). No bootstrap CI exists for the cross-arm differences or for the subtraction.

1. **The default Python proxy is an alternative ingress (block 2).** Serving `:8000` through the Python proxy instead of HAProxy or direct ingress costs **+3.517 ms p50** [+2.969, +3.983] over the block 2 Direct baseline (+309.1%), rising to +4.484 ms p99 [+4.396, +4.778]. This is a different request path, not a term that adds on top of HAProxy. It fails the absolute gate outright.
2. **HAProxy `:8000` minus direct ingress `:30000` (block 1, "C − D" in the design's arm naming: C = HAProxy, D = direct ingress).** Both arms ran in block 1 against the same 2-replica set, one replica per worker, with HAProxy balancing `leastconn`. The design (docs/superpowers/specs/2026-11-18-phase2-haproxy-rerun-design.md, §7) therefore labels C − D "HAProxy + cross-node balancing": it bundles the HAProxy process with cross-node transit and load-balancing spread.
3. **HAProxy only (block 3 control).** `ray_haproxy_local` − `ray_direct_local`, single replica, both arms served from worker w1 (§5.3).

| Percentile | Full `:8000` delta vs block 1 Direct (summary §1) | C − D (*derived*) | HAProxy-only control (summary §2) | Control as share of C − D (*derived*) | Control as share of full `:8000` delta (*derived*) | C − D minus control (*derived*) |
|---|---|---|---|---|---|---|
| p50 | +0.314 ms | +0.176 ms (1.388 − 1.212) | +0.141 ms [+0.118, +0.159] | ~80% | ~45% | +0.035 ms |
| p90 | +0.518 ms | +0.424 ms (1.799 − 1.375) | +0.129 ms [+0.114, +0.165] | ~30% | ~25% | +0.295 ms |
| p95 | +0.523 ms | +0.426 ms (1.845 − 1.419) | +0.112 ms [+0.069, +0.147] | ~26% | ~21% | +0.314 ms |
| p99 | +0.550 ms | +0.413 ms (2.028 − 1.615) | +0.031 ms [-0.033, +0.124] | ~8% | ~6% | +0.382 ms |

Mean-based C − D (*derived*): +0.182 ms p50, +0.436 ms p95, +0.413 ms p99.

How to read this: **at the median, HAProxy itself is most of C − D** (about 80%) and about 45% of the full +0.314 ms `:8000` delta. **In the tail, cross-node balancing dominates**: the HAProxy share of C − D falls to about 30% at p90, 26% at p95 and 8% at p99, where the control's CI crosses zero. The cross-node attribution is inferred from the design, not measured; no HAProxy per-server stats were captured. The control term itself may carry time drift, because block 3 ran all direct-ingress-local trials before all HAProxy-local trials (§5.3); that drift is indistinguishable from the HAProxy effect.

## 6. Gate table

Gates per the plan: absolute Δp99 ≤ 3.0 ms; relative Δp50 ≤ 10% of the block baseline; goodput ≥ 95% of the block baseline; failure rate 0.00%. The goodput gate compares each arm with its own block's Direct median, which the summary does not tabulate because its Section 3 pools Direct across blocks (50.03). From `raw_trials.json` (*derived*), the block 1 Direct goodput median is 50.00 req/s, against 50.03 for both block 1 Ray arms (≈100.06%). The block 2 Direct median is 50.03, against 50.03 for the Python proxy (100%). Every arm recorded 0.00% failures, so the goodput and failure gates pass everywhere; the latency gates differ by arm. The relative gate is expected to fail against a ~1.1 ms Direct p50 baseline with sub-millisecond model compute, and it does for every Ray arm — reported as such, per the gate definitions.

| Arm (block, vs block-specific baseline) | Δp99 (abs gate ≤ 3.0 ms) | Δp50 (rel gate ≤ 10%) | Goodput ≥ 95% baseline | Failures = 0.00% | Absolute gate | Relative gate |
|---|---|---|---|---|---|---|
| Ray Serve (Direct Ingress :30000) (block1) | +0.137 ms [+0.085, +0.187] — **PASS** | +0.138 ms = +12.8% — **FAIL** | ≈100.06% (50.03 / 50.00, *derived*) — **PASS** | 0.00% (0/7503) — **PASS** | PASS | FAIL |
| Ray Serve (HAProxy :8000) (block1) | +0.550 ms [+0.473, +0.623] — **PASS** | +0.314 ms = +29.2% — **FAIL** | ≈100.06% (50.03 / 50.00, *derived*) — **PASS** | 0.00% (0/7503) — **PASS** | PASS | FAIL |
| Ray Serve (Default Python Proxy :8000) (block2) | +4.484 ms [+4.396, +4.778] — **FAIL** | +3.517 ms = +309.1% — **FAIL** | 100% (50.03 / 50.03, *derived*) — **PASS** | 0.00% (0/7503) — **PASS** | FAIL | FAIL |
| Direct Serving (Uvicorn) (baseline) | — | — | baseline | 0.00% (0/15006) | — | — |

Summary of the gate verdicts: **direction of the absolute gate** — direct ingress `:30000` and HAProxy `:8000` pass Δp99 ≤ 3.0 ms; **the default Python proxy fails both latency gates** and is the only arm to do so. **Direction of the relative gate** — no Ray arm satisfies Δp50 ≤ 10% at this baseline; the smallest miss is direct ingress at +12.8% (0.138 ms), which at p95 is +7.3% (a pass at that percentile per the summary's row-level verdicts). (The diagnostic HAProxy-local control is reported in §5.3, not here.) These relative failures are arithmetic consequences of a ~1.1–1.3 ms baseline: rounding +0.1 ms of platform overhead to a percentage makes the relative gate unusable as a pass/fail signal in this regime, which is exactly why the absolute gate exists.

## 7. Findings, stated plainly

1. **HAProxy was in the request path and is cheap.** For the first time in this benchmark suite, the Ray Serve HAProxy ingress was exercised on `:8000` with live counter evidence (352 → 374 across the gate requests), and over 5 randomized 30 s trials it added **+0.314 ms p50 / +0.523 ms p95 / +0.550 ms p99** over the block-matched Direct baseline — comfortably inside the 3.0 ms absolute gate, with 0.00% failures and goodput at its block baseline (50.03 vs 50.00 req/s, ≈100.06%, *derived*).
2. **Direct ingress remains the leanest Ray Serve path** at +0.138 ms p50 / +0.137 ms p99 over Direct.
3. **The default Python proxy is the expensive component by an order of magnitude**: +3.517 ms p50 over Direct and +4.484 ms at p99, failing the absolute gate outright. It replaces the other ingress paths rather than adding to them.
4. **Decomposition (indicative, §5.4).** HAProxy `:8000` − direct ingress `:30000` in block 1 ("HAProxy + cross-node balancing") is +0.176 ms p50 / +0.426 ms p95 / +0.413 ms p99 (*derived*). The block 3 HAProxy-only control is +0.141 ms p50 / +0.112 ms p95 / +0.031 ms p99. The HAProxy share depends on the percentile. At the median, HAProxy itself is most of C − D (about 80%, and about 45% of the full +0.314 ms `:8000` delta). In the tail, cross-node balancing dominates: HAProxy is about 26% of C − D at p95 and about 8% at p99 (*derived*). The confounds in §8 apply.

## 8. Disclosures and limitations

- **vCPU envelope (deliberate, not matched):** Ray workers got 2 vCPU while Direct pods got 1 vCPU. This is intentional — each Ray worker co-hosts the raylet, an HAProxy (or Python proxy) process, and the Serve actor, while the Direct pod runs only Uvicorn + FastAPI + model. Ray overhead is therefore measured with headroom, not under CPU contention; the comparison favors Ray slightly on CPU budget.
- **Arm B deviation:** the Default Python Proxy arm set `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` — the one deviation from the throughput-mode env bundle (Section 2) — because without it Ray 2.60 would not keep per-node Python proxy servers in the default request path. All other arms ran the bundle unchanged.
- **Block 3 ran 1 replica per arm.** The dataset's `benchmark_metadata.replicas_per_architecture` field is a stale hardcode of 2 (written by earlier blocks and not consumed by the analysis); the local-control block ran a single replica pinned to worker w1 by design, as the preflight log confirms.
- **Decomposition confounds (§5.4 is indicative):**
  - Block 3 has no Direct baseline to correct for drift.
  - Block 3 ran 1 replica at 50 rps; block 1 ran 2 replicas at about 25 rps each.
  - Measured drift between blocks is as large as the effect: Direct `server_total` was 0.487 ms in block 1 vs 0.599 ms in block 2, and direct-ingress p50 was 1.212 ms (block 1) vs 1.271 ms (block 3 local) (*derived* per-block medians from `raw_trials.json`, except the two p50s, which are in the summary).
  - The control CIs are percentile bootstraps on n=3.
  - The block 3 control is blocked, not interleaved: the seed-43 shuffle ran `ray_direct_local#3, #2, #1, ray_haproxy_local#2, #3, #1` (`run-logs/block3-run.log`). Time drift between the direct-ingress-local and HAProxy-local sub-blocks is indistinguishable from the HAProxy effect. Blocks 1 and 2 were interleaved (`run-logs/block1-run.log`, `run-logs/block2-run.log`).
  - The cross-node split is inferred from the design, not measured: no HAProxy per-server stats were captured.
- **Direct baseline drift:** Direct's `server_total` median rose by about 0.11 ms from block 1 to block 2 (0.487 → 0.599 ms, *derived*). This is why each arm is compared only with its own block's Direct trials.
- **Shared worker nodes:** the Direct and Ray worker pods share the two worker nodes.
- **Routing differs between arms B and C:** arm B's Python proxy routes to a local replica by default (prefer-local), while arm C's HAProxy balances across both replicas.
- **Server-side instrumentation is not symmetric:** only the Ray app records a `Histogram.observe` in the request path.
- **Timer alignment:** the Ray server timer was moved before the body read for this rerun (§3), so the October transit/`server_total` columns are not comparable with these.
- **Traceability gaps:** each block's execution order was recovered from the committed run logs (`results/phase2-rerun-2026-11-18/run-logs/`), and the run seeds (42/43/44) are recorded in §3. Per-trial timestamps and pod-to-node mapping for the measured deployments were not captured and cannot be recaptured (the cluster is deleted). From now on, `run_matrix.py` records `seed` and `started_at_utc` on every trial; this dataset predates that change.
- **Trial counts are small:** 5 independent 30 s trials per benchmark arm (~1,500 requests each) and only 3 per local-control arm. Bootstrap CIs are reported for the H1 rows and the control table; the cross-arm "HAProxy + cross-node balancing" difference has no pooled CI (Section 5.4). p99 in a 1,500-request trial reflects roughly the 15 slowest requests — a tail sensitivity measure, not a steady-state bound.
- **Scope:** one workload (sub-millisecond XGBoost inference), one instance type (`c6i.2xlarge`), one arrival rate (50 req/s open-loop), one day, one cluster. No claim is made beyond this workload/hardware/rate.
- **October 2026 dataset is historical and superseded.** `results/raw_trials.json`, `results/raw_trials.csv`, `results/summary_report.md`, and `docs/report.md` are unchanged apart from the earlier label correction (14faddd) and must not be edited further; its "Worker HAProxy :30000" arm measured direct ingress, per the label correction referenced in the top-level README. This rerun supersedes it for all routing-overhead conclusions.
