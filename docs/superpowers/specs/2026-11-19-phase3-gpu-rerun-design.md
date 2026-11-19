# Phase 3 GPU Embeddings Rerun — Design

**Date:** 2026-11-19
**Status:** Draft for approval
**Branch:** `feat/phase3-gpu-rerun` (stacked on `feat/phase2-haproxy-rerun`)
**Replaces:** the exploratory single-run Phase 3 snapshot as the source of README conclusions

---

## 1. Intent

Re-measure the Phase 3 GPU embeddings benchmark (sentence-transformers
`all-MiniLM-L6-v2`, FP16, dynamic micro-batching) with the corrected experiment
setup from the Phase 2 rerun: Ray 2.60.0 correctly configured, HAProxy verified
live as the measured ingress, block-matched baselines, a hardened runner, a
preflight gate, and provenance-clean evidence.

The October Phase 3 numbers are labeled exploratory and known to carry event-loop
starvation artifacts (Direct ran `model.encode()` on the event loop). The code
fixes exist; the measurements were never redone. This rerun produces the
5-trial-per-level dataset that the Phase 3 report has called pending since the
original TODO list.

**User decision (2026-11-19): arm scope is 2-arm** — Direct Serving vs Ray Serve
via HAProxy `:8000`. The direct-ingress arm, the Python-proxy arm, and the
batch-timeout/open-loop sweeps are out of scope.

## 2. Goals

- Run on Ray **2.60.0** (`rayproject/ray:2.60.0-py311-gpu`), KubeRay **1.7.2**,
  EKS **1.34**, KubeRay operator on the head node.
- **Two arms** at six concurrency levels each: Direct (FastAPI + asyncio.Queue
  batcher) and Ray Serve (`@serve.batch`, `max_batch_size: 32`,
  `batch_wait_timeout_s: 0.005`) measured through **HAProxy `:8000`**.
- **5 trials per level per arm** (60 measured trials), 30 s per trial, 15 s
  unrecorded warmup per (arm, level), interleaved random execution order,
  seeds recorded.
- Prove configuration before load with a GPU-mode **preflight gate**: HAProxy on
  `:8000`, CUDA on all serving pods, one replica, live env flags.
- Track and analyze per-trial rows (seed, start time, threshold-breach flag) and
  produce the report with medians, IQRs, t-CIs, and bootstrap CIs over trials.
- Tear the cluster down afterwards with region-wide leak checks.
- **Budget cap: $10.** Expected ~$6–8 (see §9).

## 3. Non-goals

- The direct-ingress `:30000` arm; the Python-proxy arm; any local-control block
  (with one replica, HAProxy has one local backend — no cross-node balancing to
  isolate).
- Batch-wait-timeout sensitivity sweep and open-loop (ramping-arrival-rate) sweep.
- Multi-replica GPU scaling, autoscaling, Triton/TEI comparisons.
- Editing the October Phase 3 artifacts (`results/phase3-gpu-embeddings-report.md`,
  the preliminary `results/gpu_embed/` JSONs). They remain in the repo as
  provenance; the README's Phase 3 section is rewritten to the new results.

## 4. Anti-goals (failure conditions)

The run is invalid if any of these happen:

- Load is applied before the GPU preflight gate passes.
- HAProxy is not the measured ingress (Service pointing at `:30000`, or HAProxy
  bypassed by an incorrect targetPort).
- Any GPU-relevant config drifts from the specs in §6 without disclosure.
- Results are written to the preliminary Phase 3 result paths, or any October
  Phase 3 artifact is modified.
- The cluster outlives the session.
- %GPU identifiers, account IDs, or unrelated cluster names end up in committed
  evidence.

## 5. Cluster topology

```
us-west-2a (single AZ, on-demand)
┌──────────────────────────────────────────────────────────────────────┐
│ head-1     c6i.2xlarge   bench-role=head                            │
│            KubeRay operator + Ray head (control plane, no GPU)      │
│                                                                     │
│ gpu-w1     g6.xlarge     bench-role=gpu   bench-slot=gpu1           │
│            Direct GPU pod (FastAPI + asyncio.Queue batcher, 1 L4)   │
│                                                                     │
│ gpu-w2     g6.xlarge     bench-role=gpu   bench-slot=gpu2           │
│            Ray GPU worker pod (1 replica, 1 L4) + HAProxy :8000     │
│                                                                     │
│ loadgen-1  c6i.2xlarge   bench-role=loadgen                         │
│            k6 driver only                                            │
└──────────────────────────────────────────────────────────────────────┘
```

- eksctl managed nodegroups, static (desired = min = max = 1), same pattern as
  `infra/phase2-rerun/eksctl-cluster.yaml`, region us-west-2, control plane
  VPC across `us-west-2a` + `us-west-2b`, all nodegroups pinned to
  `us-west-2a`. Quotas verified: VPC 500/187, IGW 500/186, G-family 30000.
  `g6.xlarge` offered in us-west-2a/b/c/d.
- **GPU nodegroups pin the EKS-optimized AL2023 NVIDIA AMI** — eksctl does not
  auto-select a GPU image under `amiFamily: AmazonLinux2023`, and eksctl AL2
  auto-GPU resolution is legacy. The two `gpu-*` nodegroups set `ami:` to the
  newest `amazon-eks-node-al2023-x86_64-nvidia-1.34-v*` image (resolved 2026-11-19:
  `ami-0bd8ae7fb660d28d7`, build v20261112, us-west-2). Refresh procedure and
  the resolution command are documented in the cluster README; the AMI bakes in
  the NVIDIA driver and container toolkit for g6 (L4).
- NVIDIA k8s-device-plugin DaemonSet applied to the two GPU nodes (pinned
  `nvidia/k8s-device-plugin:v0.17.1`, static manifest) so `nvidia.com/gpu`
  resources are allocatable (the AMI provides driver + toolkit; the plugin
  registers the device).
- Node anti-affinity pins the Direct pod to `bench-slot=gpu1` and the Ray worker
  to `bench-slot=gpu2`; the pods cannot share a node (each node has exactly one
  L4 anyway).

## 6. Application & Service configuration

**Direct GPU** (`benchmarks/phase3-gpu-rerun/01-direct-gpu.yaml`):

- Image `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`, pip-installed
  `fastapi`/`uvicorn[standard]`/`orjson` at startup (same scheme as Phase 2's
  direct pods).
- App: `benchmarks/gpu-embed/direct/app.py` **as-is** (event-loop fix in place:
  `asyncio.to_thread` around encode; `MAX_BATCH_SIZE=32`,
  `BATCH_WAIT_TIMEOUT_S=0.005`).
- Requests: 2 CPU / 8 GiB, limits 4 CPU / 12 GiB, `nvidia.com/gpu: 1`.
- Service `direct-gpu-embed-svc`, 8000→8000, selector the direct pod's labels.

**High-throughput Serve settings** (auditable, mirroring the Phase 2 rerun):

| Setting | Value | Source |
|---|---|---|
| `RAY_SERVE_ENABLE_HA_PROXY` | `1` | HAProxy ingress on `:8000` (head + worker) |
| `RAY_SERVE_THROUGHPUT_OPTIMIZED` | `1` | The recommended bundle (see below) |
| `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD` | `0` | set by the bundle |
| `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` | `0` | set by the bundle |
| `RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE` | `1000` | set by the bundle |
| `RAY_SERVE_LOG_TO_STDERR` | `0` | set by the bundle |
| `RAY_SERVE_USE_GRPC_BY_DEFAULT` | `1` | set by the bundle (irrelevant: single deployment) |
| `RAY_SERVE_ENABLE_DIRECT_INGRESS` | `1` | forced by/bundled with HAProxy (backend servers only; not a measured Service) |
| `RAY_SERVE_FREEZE_GC_ON_STARTUP` | `1` | set by the bundle |
| `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING` | default `1` | not set explicitly |
| `RAY_IGNORE_VERSION_CHECK` | never set | removed from the old manifest |
| `ray-haproxy` in `runtime_env.pip` | never set | the image's `ray[all]` provides it |
| `max_ongoing_requests` | **1000** | **raised from the old 100** to match the Phase 2 standard; with closed-loop C ≤ 64 it never binds — the change is uniformity and headroom, not a throttle it would have been |
| `max_batch_size` / `batch_wait_timeout_s` / `max_concurrent_batches` | 32 / 0.005 s / 1 | pinned to the October spec's values for gate comparability; `max_concurrent_batches: 1` now pinned explicitly (it sets serial GPU batches — the right choice on a single L4) |
| `RAY_SERVE_HAPROXY_BALANCE_ALGORITHM` | default (`leastconn`) | single backend; moot but left default |

**Ray Serve GPU** (`benchmarks/phase3-gpu-rerun/02-ray-ha.yaml`):

- `rayVersion: "2.60.0"`, images `rayproject/ray:2.60.0-py311` (head) / `-gpu`
  (worker).
- Head: `num-cpus: "0"`, `bench-role=head`, 1–2 CPU/3–5 GiB.
- Worker: 1 replica pod on `bench-slot=gpu2`; `rayStartParams`:
  `num-gpus: "1"`, `num-cpus: "2"` (explicit, so the 2-CPU replica request fits
  exactly), `resources: '{"worker_only":1}'` — the replica's
  `ray_actor_options` demand `worker_only: 1` (keeps it off the head), and the
  Serve config preserves the same contract: `ray_actor_options:
  {num_gpus: 1, num_cpus: 2, resources: {"worker_only": 1}}`. The old group's
  `ignore-version-check: "true"` rayStartParam is **removed** alongside the env
  var. Requests: 2 CPU / 8 GiB, limits 4 CPU / 12 GiB, `nvidia.com/gpu: 1`;
  readiness by `:30000/readiness` with a long `initialDelaySeconds` (image pull
  + pip + model download).
- Env on head and worker: `RAY_SERVE_ENABLE_HA_PROXY=1`,
  `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`,
  `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD: "0"`. **`RAY_IGNORE_VERSION_CHECK`
  is removed** (the old manifest set it; Phase 2 already excised it).
- `runtime_env.pip`: `torch==2.2.0`, `transformers>=4.40.0,<4.49.0`,
  `sentence-transformers>=2.5.0,<3.5.0`, `accelerate`, `orjson>=3.9.0`.
- Serve config: `proxy_location: EveryNode`, `max_ongoing_requests: 1000`,
  `num_replicas: 1`, `ray_actor_options: {num_gpus: 1, num_cpus: 2}`.
- App: `benchmarks/gpu-embed/ray/serve_embed.py` with two pinned changes,
  Phase-2 precedent: the `@serve.deployment` decorator's
  `max_ongoing_requests` goes 100 → **1000** (decorator and Serve config must
  agree; a pinning test asserts both), and the `@serve.batch` decorator pins
  `max_concurrent_batches=1` explicitly. Event-loop handling stays as-is.
- **Measured Service** `ray-ha-svc`: worker-pod selector, **port 8000 →
  targetPort 8000** (HAProxy). The old manifest's only Service mapped
  8000→30000 (direct ingress): that object is *not* carried over.

**Config pinning tests** (in `tests/`, Phase-2 pattern): the manifest test
asserts `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` and `RAY_SERVE_ENABLE_HA_PROXY=1` on
head and worker, `max_ongoing_requests == 1000` in `serveConfigV2` **and** in the
`serve_embed.py` decorator, `RAY_IGNORE_VERSION_CHECK` absent, no
`ray-haproxy` pip entry, and the measured Service `targetPort == 8000`.

**Driver** (`benchmarks/phase3-gpu-rerun/03-loadgen-driver.yaml`): same pattern
as Phase 2's `04-loadgen-driver.yaml` — `python:3.11-slim`, k6 v0.54.0 at
startup, `workingDir: /workspace/loadgen`, ConfigMaps for runner + script +
fixtures, emptyDir `/results`, `sleep infinity`, pinned to `bench-role=loadgen`.
The runner/k6/fixtures are staged via fresh ConfigMaps at driver-deploy time.

**Cluster ConfigMap**: fixtures from `benchmarks/gpu-embed/fixtures/queries.jsonl`
(2,000 payloads), unchanged.

## 7. Runner, gate, analysis

**Runner** (`benchmarks/phase3-gpu-rerun/loadgen/run_gpu_matrix.py` +
`k6_embed.js` copied with one change — `TEST_PROFILE=concurrency` stays the
default; thresholds unchanged):

- Port the Phase-2 hardened semantics: `--output-raw-json`/`--output-raw-csv`
  **required**; `--append` across invocations with dedupe on
  `(block, target_key, concurrency, trial_number)`; per-trial incremental
  export; k6 exit 99 recorded as `threshold_breached: true`, other non-zero
  exits abort; every recorded trial carries `block`, `target_key`,
  `concurrency`, `trial_number`, `seed`, `started_at_utc`.
- One block (`gblock1`), two targets: `direct` →
  `http://direct-gpu-embed-svc:8000`, `ray_ha` → `http://ray-ha-svc:8000`.
- Execution plan: for each (arm × level) one warmup (15 s, unrecorded); 5
  recorded trials per (arm × level, 30 s); the full (arm, level, trial) queue
  is shuffled with a recorded seed, subject to the constraint that a level's
  warmup precedes its trials of the same (arm, level). Order, as run, is
  printed and the block run log is committed as evidence.
- Python 3.11 in-cluster, Python 3.9-compatible source.

**Preflight gate** (`benchmarks/phase3-gpu-rerun/preflight.sh`, mode `gpu`),
fail-closed like Phase 2's script (explicit `-c` execs, digits-only stat sums,
`/proc/net/tcp` port scans, ray_haproxy wheel-path binary check):
- 4 nodes: 2× `g6.xlarge` + 2× `c6i.2xlarge`, all labeled/zoned correctly.
- CUDA acceptance (in both serving pods): a `python3 -c` check of
  `torch.cuda.is_available()` printing `torch.cuda.get_device_name(0)` —
  the application's own view of the L4. `nvidia-smi` is logged as supplementary
  evidence if reachable, but torch seeing the device is the real acceptance
  (the driver is injected by the node image's container toolkit, not baked into
  the container images).
- Ray 2.60.0 on head; HAProxy 2.8.x on head + worker; **2 HAProxyManager actors**
  (head + the one replica-hosting GPU worker), 0 Python-proxy fallbacks.
- Listener set: Direct pod has `:8000`; Ray worker has `:8000` (HAProxy) and
  `:300xx`; endpoints: direct 1, ray-ha-svc 1 (worker only).
- RayService Serve status: 1 replica RUNNING, `max_ongoing_requests: 1000`.
- Live env capture: head + worker `RAY_SERVE_ENABLE_HA_PROXY=1` +
  `RAY_SERVE_THROUGHPUT_OPTIMIZED=1` committed.
- HAProxy binary present in the **-gpu** image: resolved via `command -v
  haproxy` or the `ray_haproxy` wheel path (Phase-2 pattern). **If it is
  missing, the gate FAILS and the run stops** — adding `ray-haproxy` to
  `runtime_env.pip` is a controller-level amendment, not an in-task fix.
- 10 warm-up `/embed` requests return HTTP 200 with a 384-dim embedding through
  **each of**: `direct-gpu-embed-svc`, `ray-ha-svc`.
- HAProxy counters increase through the ray-ha-svc traffic (the HAProxy-in-path
  proof).

**Analysis & report**: new `results/phase3-gpu-rerun-2026-11-19/` and
`results/phase3-gpu-rerun-2026-11-19-report.md`:
- Per (arm × level): median [IQR] and mean ± t-CI for p50/p95/p99, throughput,
  server-side batch time; delta of HA arm vs Direct per level with bootstrap CIs
  over the 5 trials; transit decomposition per arm (RoundTrip − server total).
- Gates (carried from the Phase 3 plan):
  1. **Low-load latency** (C=1 and C=4): HA arm ≤ 3.0 ms added at p95, or ≤ 10%.
  2. **Goodput**: HA arm ≥ 95% of Direct per level (or explicit failure).
  3. **Batch scaling**: ≥ 3x scaling from C=1 → C=32 with p95 ≤ 20 ms at C=32.
  4. **C=64 saturation gate:** the HA arm's p99 at C=64 must be within
     **+3.0 ms of Direct's p99** (absolute-difference rule; at closed-loop
     saturation a relative-% gate is meaningless because both arms are pinned).
     Both p99s are reported regardless, resolving the October run's
     "INCONCLUSIVE" with a definite verdict and a disclosed criterion.
- Charts regenerated with the corrected arm name, 95% CI bands, and per-trial
  scatter (throughput vs level, p95/p99 vs level). Read the `dataviz` skill
  before the chart script is written.
- README's Phase 3 section is rewritten to the new results (latest-only
  convention; one provenance line pointing to the retained October snapshot,
  unchanged).
- Seed, order, and run logs committed as evidence; report discloses:
  per-trial counts, single-model/single-GPU scope, untuned 32/5 ms batching,
  `max_concurrent_batches: 1`, `max_ongoing_requests: 1000`, Direct's asyncio
  baseline frame, and node non-isolation beyond types.

## 8. Execution blocks

One deployment, no block swaps:

1. **Prep:** build cluster, apply device plugin, install kubectl 1.34.13 +
   KubeRay 1.7.2, deploy ConfigMaps + Direct + RayService, run preflight `gpu`
   gate (expect long first-boot: image pull + `pip install` + Hugging Face
   model download can take 10–20 min for each serving pod; waits must be
   patient).
2. **Measure:** deploy driver, stage runner ConfigMaps, run the block
   (`--block gblock1 --concurrencies 1,4,8,16,32,64 --trials 5`), copy
   results out (60 trials + 12 warmups), verify integrity (60 recorded trials,
   30 per arm, 0 unflagged failures), commit raw data.
3. **Teardown — immediately after measurement**, before analysis: delete
   cluster, sweep leaks (EC2/NAT/volumes/CFN/kubeconfig, GPU and CPU instance
   inventory), commit evidence. Rationale: the analysis is offline and the GPUs
   must not idle through it (~$2.39/hr). If analysis later finds an anomaly
   that needs re-measurement, that is a new billed session, escalated to the
   user first.
4. **Analyze & report (cluster gone):** run the analyzer, write the report,
   regenerate charts, rewrite README Phase 3 section, commit.

## 9. Cost

| Item | Rate | Expected |
|---|---|---|
| 2 × `g6.xlarge` | ~$0.804/hr ea. | ~$1.61/hr |
| 2 × `c6i.2xlarge` | ~$0.34/hr ea. | ~$0.68/hr |
| EKS control plane | $0.10/hr | ~$0.10/hr |
| NAT data processing (tens of GB of image + wheel + model pulls) | ~$0.045/GB | ~$1 one-time |
| **Total** | ~$2.39/hr | **~$7–9 for a 2.5–3.5 h session incl. NAT** |

**$10 is a hard cap, inclusive of all charges.** Happy path leaves ~20% slack;
one failed gate plus a 20–30 min re-deploy (runtime_env pip re-runs) consumes
most of it. If the cap is hit mid-measurement: abort, tear down, and report
partial trials with what completed. Any stop after cluster creation runs the
teardown block before reporting.

## 10. Risks & fallbacks

| Risk | Fallback |
|---|---|
| First boot slow (pull/pip/model DL) | Patient waits up to 30 min per pod; the gate doesn't run until both arms are Ready |
| Pinned NVIDIA AMI wrong/stale for K8s 1.34 | Re-resolve newest `amazon-eks-node-al2023-x86_64-nvidia-1.34-v*`, or fall back to the 1.33 build (node skew is supported); caught before preflight by the CUDA acceptance check |
| GPU device plugin fails to register `nvidia.com/gpu` | Preflight CUDA check catches it; re-apply plugin manifest, re-gate. Max one retry, then stop-and-report |
| `ray-haproxy` binary missing from the **-gpu** image (verified only on CPU in Phase 2) | Gate FAILS and stops. Controller-level amendment may add `ray-haproxy` to `runtime_env.pip` as a disclosed change; never an in-task fix |
| Serving containers lack `ss`/diagnostics | Preflight already uses `/proc/net/tcp` and wheel-path patterns from Phase 2 |
| k6 thresholds exit 99 at high C | Recorded as `threshold_breached` trials, not aborts; the analyzer excludes nothing but flags them |
| HF model download fails in-pod | Stop-and-report; do not bake/write tokens into manifests |
| On-demand capacity for g6 in us-west-2a | Quota 30000; retry once in an adjacent AZ, **moving all nodes together**, by controller ruling |
| Replica strands PENDING (resource contract) | `worker_only` is pinned in both `rayStartParams` and `ray_actor_options`; preflight requires 1 RUNNING replica |
| Cost overrun (30+ min debugging) | Teardown fires immediately post-measurement (§8 step 3); hard cap rule in §9 |
| Analysis finds an anomaly needing re-measurement | Cluster is already torn down (§8 ordering); escalate to user for a new billed session |

## 11. Decisions already made (user)

- 2-arm scope (Direct vs Ray HAProxy) — user, 2026-11-19.
- Same eksctl/static-nodegroup pattern as Phase 2 ("our new experiment setup") — user.
- Batch-wait sweep, open-loop sweep, Python-proxy arm: out of scope.
- README: latest-only presentation, as established in the Phase 2 cleanup.

## 12. Left to the builder

- Exact analyzer/report script shape (schema follows the runner's trial rows).
- The device-plugin manifest's tolerations for any cluster-provided GPU taints.
- Chart script implementation details (post-`dataviz`).
- Report prose.
