# Phase 2 HAProxy Rerun — Design

**Date:** 2026-11-18
**Status:** Approved with one amendment
**Branch:** `feat/phase2-haproxy-rerun`
**Supersedes:** the October 2026 Phase 2 dataset and the “Worker HAProxy” label

> **Amendment (2026-11-18, user decision):** the us-east-1/us-east-1d placement
> in Section 5 is unreachable — that region is at 30/30 VPC and 30/30
> internet-gateway quotas, and one VPC+IGW cannot be created. The run moves to
> **us-west-2** (VPC quota 500 with 187 in use; IGW quota 500 with 186 in use),
> with every nodegroup pinned to **`us-west-2a`** (`c6i.2xlarge` offered in
> us-west-2a/b/c/d) and the control-plane VPC spanning `us-west-2a` and
> `us-west-2b`. The measurement is unaffected: it is single-AZ and entirely
> in-cluster. All other constraints, including the never-touch list, now cover
> the existing clusters in us-west-2 as well.

---

## 1. Intent

Re-measure the CPU tabular benchmark with a Ray Serve configuration that is
actually correct, and with HAProxy actually in the request path.

Two problems motivate the rerun:

1. **The old “HAProxy” arm was mislabeled.** The Services mapped
   `port 8000 → targetPort 30000`. In Ray, 30000 is the replica’s direct-ingress
   HTTP server, not HAProxy. HAProxy was enabled but bypassed. The old dataset
   therefore never measured HAProxy.
2. **The Ray configuration was not tuned for serving.** `max_ongoing_requests`
   was 50; Ray 2.60’s documented high-throughput example is 1000.
   The worker container had a **1 vCPU limit while the replica actor requested
   1 CPU**, leaving the Raylet, object store, HAProxy, and the actor to share a
   single core. `ray-haproxy` was installed again in `runtime_env` even though
   the Ray image already ships it. Ray/KubeRay were pinned to 2.55.1/1.4.2.

The rerun answers one question: **with Ray 2.60 correctly configured and
HAProxy on `:8000`, how does Ray Serve compare to Direct Uvicorn on the same
hardware, and how much of the difference is HAProxy itself?**

---

## 2. Goals

- Run Phase 2 on Ray **2.60.0**, KubeRay **1.7.2**, Kubernetes **1.34**, in a new
  EKS cluster in `us-east-1d`.
- Measure **four arms** on matched `c6i.2xlarge` nodes:
  - **A — Direct:** FastAPI + Uvicorn + orjson.
  - **B — Ray default Python proxy:** HAProxy off, Python proxy on `:8000`.
  - **C — Ray HAProxy:** HAProxy on `:8000`, measured through HAProxy.
  - **D — Ray direct ingress:** same deployment as C, Service targets `:30000`.
- Enable Serve’s documented high-throughput settings and a high
  `max_ongoing_requests`.
- Prove at runtime that HAProxy is the listener on `:8000` before collecting
  any benchmark data.
- Produce versioned raw data, a new statistical summary, a new report, and a
  captured topology record.
- Preserve the October 2026 dataset and reports as historical; do not overwrite
  them.
- Delete the cluster after the run. Target total AWS spend under **$10**.

## 3. Non-goals

- Rerunning Phase 3 GPU embeddings. That gets its own GPU cluster later.
- Tuning batch timeouts, autoscaling, HPA, or multi-model pipelines.
- Making HAProxy faster than direct ingress. The direct-ingress arm exists to
  quantify HAProxy’s cost, not to win.
- Production hardening, multi-AZ resilience, or TLS.
- Rewriting the application code beyond what correctness requires.

## 4. Anti-goals (failure conditions)

The run is invalid if any of these happen:

- Benchmarking while HAProxy is not actually serving `:8000` (e.g. silently
  falling back to the Python proxy, or the Service still targeting `:30000`).
- Mixing arms with different resource envelopes without disclosure.
- Overwriting `results/raw_trials.*`, `results/summary_report.md`, or
  `docs/report.md`.
- Collecting measurements without saving the preflight evidence.
- Leaving the EKS cluster, nodes, NAT gateway, or EBS volumes running.
- Reporting a number without stating which block’s Direct baseline it is
  compared against.

## 5. Constraints

- AWS profile: `<aws-profile>`.
  - It has `PowerUserAccess`, `IAMReadOnlyAccess`, and an inline policy granting
    the IAM role/instance-profile/policy actions needed by EKS.
  - Do not write credentials to disk or embed them in manifests.
  - Do not touch the existing `<existing-shared-cluster>`.
- Region `us-east-1`, data plane in **`us-east-1d`**.
  `c6i.2xlarge` is offered there; the standard on-demand vCPU quota is 320,000.
- EKS supports 1.37; `eksctl` 0.230.0 defaults to **1.34**. Use 1.34 unless
  KubeRay 1.7.2 rejects it.
- Local `kubectl` is 1.28; install **1.34.13** for the run.
- Latest stable versions verified on 2026-11-18: Ray **2.60.0**, KubeRay
  **1.7.2** (chart 1.7.2), `ray-haproxy` **2.8.25**.
- The CPU model artifact (`model/artifacts/model.json`, 553 KB) and the 1,000
  request fixtures are present and valid.

---

## 6. Approaches considered

### A. eksctl + static managed nodegroups — **selected**

One checked-in `eksctl` config creates a new VPC, EKS 1.34 control plane, and
four explicit `c6i.2xlarge` nodes in `us-east-1d`: one head, two workers, one
loadgen. Labels select workloads; no Karpenter.

- **Why:** fastest path, reproducible, and the static node count directly fixes
  the topology ambiguity in the old run.
- **Trade-off:** not a permanent Terraform module; the config is eksctl-specific.

### B. Terraform EKS + nodegroups

Same topology written as Terraform. More reusable, but there are no modules in
the repo and it adds roughly an hour before the first measurement. Rejected for
a validation rerun.

### C. Reuse `<existing-shared-cluster>` or Karpenter

Rejected. The cluster is not ours, and Karpenter packing reintroduces the
“which pods shared a node?” ambiguity this rerun is meant to remove.

---

## 7. Cluster topology

```
us-east-1d
┌────────────────────────────────────────────────────────────────────┐
│ head-1       c6i.2xlarge   bench-role=head                         │
│              KubeRay operator, Ray head (one RayService at a time)  │
│                                                                    │
│ worker-1     c6i.2xlarge   bench-role=worker                       │
│ worker-2     c6i.2xlarge   bench-role=worker                       │
│              one server pod per node (anti-affinity)               │
│                                                                    │
│ loadgen-1    c6i.2xlarge   bench-role=loadgen                      │
│              k6 Job only                                           │
└────────────────────────────────────────────────────────────────────┘
```

- VPC spans `us-east-1a` and `us-east-1d` so EKS can create the control plane
  across two AZs; **every nodegroup is pinned to `us-east-1d`**.
- Managed nodegroups, Amazon Linux 2023, on-demand, no autoscaling beyond the
  fixed desired size.
- KubeRay operator pinned to the head node via Helm `nodeSelector`.
- Server pods use pod anti-affinity so each worker node hosts at most one Direct
  pod and one Ray worker pod.
- The loadgen node hosts only the k6 Job.

### Execution blocks

Direct and one RayService can coexist on the two worker nodes because each pod
is CPU-capped and the offered load is 50 req/s.

| Block | Deployed | Arms measured | Trials |
|---|---|---|---|
| 1 | Direct Deployment + HAProxy RayService (2 replicas) | A Direct, C HAProxy, D direct ingress | 5 per arm, interleaved randomly |
| 2 | Direct Deployment + Python-proxy RayService (2 replicas) | A Direct, B Python proxy | 5 per arm, interleaved randomly |
| 3 (control) | Direct + HAProxy RayService with **1 replica pinned to worker-1**; HAProxy Service selects worker-1 only | C-local, D-local | 3 per arm, interleaved |

- Direct is measured again in Block 2 so each Ray arm has a contemporaneous
  baseline. Analysis uses the **block-specific Direct trials**, not a pooled
  Direct mean.
- The HAProxy RayService is deleted before the Python-proxy RayService is
  created. Only one Ray head runs at a time.
- HAProxy’s direct-ingress listeners exist in Block 1 as a side effect, which is
  what makes arm D free.

**Cross-node caveat and control.** HAProxy builds each backend from every
replica in the routing table and balances with `leastconn`; it uses `localhost`
only when the target happens to be on the same node. With two replicas on two
workers, each worker’s HAProxy sends roughly half its requests to the replica on
the other worker. Arm D hits the replica’s own HTTP server, so it is local by
construction. **C − D therefore includes HAProxy plus cross-node transit, not
HAProxy alone.**

Block 3 isolates them: one replica on worker-1, and a HAProxy Service that
selects only worker-1, so HAProxy has exactly one local backend. **C-local −
D-local is the HAProxy-only delta.** The headline remains the 4-arm matrix in
Blocks 1–2; Block 3 is a diagnostic control and is reported separately. If the
control is dropped, the report must label C − D as “HAProxy + cross-node
balancing” and make no HAProxy-only claim.

---

## 8. Resource envelope

| Pod | CPU request / limit | Memory request / limit | Ray logical CPUs | Notes |
|---|---|---|---|---|
| Direct Uvicorn | 1 / 1 | 2 GiB / 2 GiB | — | model only |
| Ray head | 1 / 1 | 3 GiB / 3 GiB | `num-cpus: "0"` | control plane + HAProxy/Python proxy |
| Ray worker | **2 / 2** | 4 GiB / 4 GiB | `num-cpus: "1"` | Raylet + object store + HAProxy + 1-CPU replica actor |
| k6 loadgen | 1 / 2 | 2 GiB / 2 GiB | — | same node as nothing else |

**Why the Ray worker gets 2 vCPU:** the replica actor requests 1 CPU. The
Raylet, object store, and HAProxy need CPU too. The old 1-vCPU limit meant the
actor and Ray runtime shared a single core, which can show up as CFS throttling
rather than Serve overhead. The Direct pod keeps 1 vCPU because it runs only the
model. This is an intentional, disclosed asymmetry: Ray is given enough CPU to
run its runtime, not a hidden advantage.

- `OMP_NUM_THREADS=1` everywhere.
- Exactly 2 replicas per architecture.
- No autoscaling.

---

## 9. Ray Serve configuration

### 9.1 Version and image

| Component | Old | New |
|---|---|---|
| Ray / Ray Serve | 2.55.1 | **2.60.0** |
| KubeRay | v1.4.2 | **v1.7.2** |
| Ray image | `rayproject/ray:2.55.1-py311` | **`rayproject/ray:2.60.0-py311`** |
| Kubernetes | 1.30 | **1.34** |

### 9.2 Environment variables

Set on **both head and worker containers** so they are present before `ray start`
(also mirrored into `runtime_env.env_vars` for the application process):

| Variable | HAProxy arm (C/D) | Python-proxy arm (B) | Source |
|---|---|---|---|
| `RAY_SERVE_ENABLE_HA_PROXY` | `1` | `0` | Ray 2.60 HAProxy opt-in |
| `RAY_SERVE_THROUGHPUT_OPTIMIZED` | `1` | `1` | recommended settings bundle |
| `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD` | `0` (set by bundle) | `0` | bundle |
| `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` | `0` (set by bundle) | `0` | bundle |
| `RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE` | `1000` (bundle) | `1000` | bundle |
| `RAY_SERVE_LOG_TO_STDERR` | `0` (bundle) | `0` | bundle |
| `RAY_SERVE_USE_GRPC_BY_DEFAULT` | `1` (bundle) | `1` | bundle |
| `RAY_SERVE_ENABLE_DIRECT_INGRESS` | forced `1` | **`0` (explicit override)** | see below |
| `RAY_SERVE_FREEZE_GC_ON_STARTUP` | `1` (bundle) | `1` | bundle |
| `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING` | default (`1`) | default | not set |
| `RAY_SERVE_HAPROXY_BALANCE_ALGORITHM` | default (`leastconn`) | n/a | not set |
| `RAY_IGNORE_VERSION_CHECK` | never set | never set | — |

`RAY_SERVE_THROUGHPUT_OPTIMIZED=1` is the documented switch; the individual
variables are applied by Ray. The table documents what the bundle does so the
config is auditable. Do not set undocumented HAProxy thread/maxconn knobs.

**Why arm B overrides direct ingress.** In Ray 2.60, when direct ingress is
enabled and HAProxy is off, the controller forces `proxy_location = HeadOnly`
(`controller.py`: “Direct ingress is enabled in ServeController, enabling proxy
on head node only”). The throughput bundle enables direct ingress, so arm B
would otherwise have a single Python proxy on the head and no listener on the
worker pods — not comparable to arm C, and not the per-node proxy the October
run measured. Setting `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` for arm B restores
`EveryNode` proxy placement. This is the **one deliberate deviation** from the
throughput bundle for B; it is recorded in the report, and B and C still share
all other Serve settings.

**Proxy counts.** `EveryNode` runs a proxy on every node with an active replica
**plus the head node** (`controller.py` adds `_controller_node_id` to
`_proxy_nodes`). With the head and two workers, preflight expects **3**
HAProxy managers for C/D and **3** Python proxy actors for B. The head has no
replica and is not a Service endpoint; its proxy is present but not measured.

**Caveat:** throughput-optimized mode runs user code in the replica’s main event
loop. `booster.predict` is synchronous (~0.5 ms). At 50 req/s with two replicas
that is well below saturation, so the run proceeds with the documented
configuration. If the HAProxy arm shows unexplained p99 spikes, the contingency
is to set `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=1` for both Ray arms and
rerun, documenting the deviation.

### 9.3 Serve config

```yaml
proxy_location: EveryNode
http_options:
  host: "0.0.0.0"
  port: 8000
applications:
  - name: default
    route_prefix: /
    import_path: ray_serve_app:app
    runtime_env:
      env_vars: { ... }        # same flags as container env
      pip:
        - "xgboost==2.1.1"
        - "orjson==3.9.0"
    deployments:
      - name: XGBoostDeployment
        num_replicas: 2
        max_ongoing_requests: 1000
        ray_actor_options:
          num_cpus: 1.0
```

- **No `ray-haproxy` in `runtime_env`.** Ray 2.60’s `serve` extra depends on
  `ray-haproxy>=2.8.25,<2.9.0`, and the official image installs `ray[all]`.
- `max_ongoing_requests=1000`, set in both the Serve config and the
  `@serve.deployment` decorator.
- `proxy_location: EveryNode` (documented default).
- No `RAY_IGNORE_VERSION_CHECK`.

### 9.4 Services

| Arm | Service | Selector | Service port | targetPort |
|---|---|---|---|---|
| B Python proxy | `ray-python-proxy-svc` | Ray worker pods | 8000 | 8000 |
| C HAProxy | `ray-haproxy-svc` | Ray worker pods | 8000 | **8000** |
| C-local control | `ray-haproxy-w1-svc` | worker-1 pod only (`bench-slot=w1`) | 8000 | 8000 |
| D direct ingress | `ray-direct-ingress-svc` | Ray worker pods | 8000 | **30000** |
| D-local control | `ray-direct-w1-svc` | worker-1 pod only (`bench-slot=w1`) | 8000 | 30000 |
| A Direct | `direct-serving-svc` | Direct pods | 8000 | 8000 |

- C and D select worker pods only, not the head. The head’s HAProxy/proxy is
  present but not an endpoint.
- C does **not** guarantee a local replica: each worker HAProxy balances across
  both replicas (see Block 3 and the cross-node caveat).
- **Direct-ingress port rule:** both worker replicas must bind the same
  direct-ingress port. Preflight reads `ss` on both workers. If they differ,
  recreate the RayService once and re-check. If they still differ, drop arm D
  (and D-local) and report C − D attribution as unavailable. Do not patch a
  single Service to one worker’s port while it has two endpoints.
- The control Services select a single worker pod. The single-replica
  RayService pins its worker group to `bench-slot=w1` and carries the same
  `bench-slot=w1` pod label, so the Service has exactly one endpoint and
  HAProxy has exactly one local backend.

### 9.5 Application timer alignment

The October apps used different boundaries: Direct started `t0` before
`await request.body()`, Ray started after it; both excluded response
serialization from the client-reported total. That biases
`transit = client duration − server total`.

For the rerun, align both apps before collecting data:

- `t0` is taken at the first line of the request handler, **before**
  `await request.body()`.
- `total_latency_ms = t_model_end − t0` in both apps: body read + JSON parse +
  model execution, excluding response serialization.
- Both return the same `total_latency_ms` field in the JSON body, and the
  k6/transit calculation uses that field.
- Server histograms may record serialization separately; the client-reported
  field is the aligned one.
- This is a small change to `k8s/app/direct_app.py` and
  `k8s/app/ray_serve_app.py`; model logic is untouched.

---

## 10. Preflight gate (no load until all pass)

Run from the cluster; save output under
`results/phase2-rerun-2026-11-18/preflight/`.

1. **Nodes:** four nodes, one per role, all `c6i.2xlarge`, all `us-east-1d`;
   save `kubectl get nodes -o wide -L bench-role,topology.kubernetes.io/zone`.
2. **KubeRay:** operator image tag `v1.7.2`.
3. **Ray:** `ray --version` in the head pod → `2.60.0`.
4. **HAProxy binary:** `haproxy -v` in head and both worker pods → 2.8.x.
5. **Proxy actors:** for arm C/D, `ray list actors | grep HAProxyManager` → **3**
   (head + 2 workers). For arm B, `ray list actors | grep -i proxy` → **3**
   Python proxy actors and **0** HAProxyManager. The head proxy is expected but
   is not a Service endpoint.
6. **Ports:** in **both** workers, `ss -ltnp` shows the proxy/HAProxy on
   `:8000` and the replica server on `:300xx`. Record the direct-ingress port
   from each worker; they must match.
7. **Endpoints:** `kubectl get endpoints` shows both worker pods for C/D; the
   HAProxy Service `targetPort` is 8000, and the direct-ingress Service
   `targetPort` equals the common port observed on both workers.
8. **Serve status:** `ray serve status` and the JSON status show 2 healthy
   replicas and `max_ongoing_requests=1000`.
9. **Functional:** 10 sequential `/infer` requests through each Service return
   HTTP 200 with a numeric `prediction`; 0 failures.
10. **HAProxy is in the path:** capture HAProxy stats before and after the 10
    requests (`curl -s 127.0.0.1:8404/stats;csv` on each worker) and require the
    frontend/backend request counters to increase. Also confirm the worker’s
    `ss` shows HAProxy on `:8000`. This is the gate that distinguishes C from D.

If any check fails: stop, fix, and rerun preflight. Do not collect data.

---

## 11. Benchmark protocol

- k6 `constant-arrival-rate`, **50 req/s**, **30 s** per trial.
- **5 trials per arm** in Blocks 1–2; **3 trials per arm** in the Block 3
  control. Trials are interleaved randomly within each block (seed recorded).
- **15 s warmup** per arm before its first measured trial.
- Same 1,000 fixtures, keep-alive connections.
- No code, manifest, or cluster changes during a block.
- Raw k6 JSON per trial retained.
- Trial timestamps and pod-to-node mapping saved with the data.
- Total: **31 measured trials** (~15.5 min of load) plus warmups.

---

## 12. Analysis and reporting

- Extend `loadtest/run_matrix.py`:
  - add `ray_haproxy` (8000), `ray_direct` (30000), `ray_haproxy_local`,
    `ray_direct_local`, keep `ray_python` and `direct`;
  - add `--targets`, `--warmup-duration`, `--block`, and explicit output paths;
  - support block-aware execution so a run never targets an undeployed arm.
- Extend `results/analyze.py`:
  - H1 table with one row group per Ray arm, each compared to its block’s
    Direct baseline;
  - keep the existing bootstrap median-difference CI and Student-t mean CI.
- New outputs:
  - `results/phase2-rerun-2026-11-18/raw_trials.json`
  - `results/phase2-rerun-2026-11-18/raw_trials.csv`
  - `results/phase2-rerun-2026-11-18/summary_report.md`
  - `results/phase2-rerun-2026-11-18/preflight/`
  - `results/phase2-rerun-2026-11-18/k6/`
  - `results/phase2-rerun-2026-11-18-report.md`
- Do not edit `docs/report.md`, `results/summary_report.md`, or
  `results/raw_trials.*`. After review, update `README.md` to point at the new
  report and mark the October dataset as historical.
- Gate semantics (all deltas are **Ray arm minus its block’s Direct baseline**):
  - **Absolute gate:** Δp99 ≤ **3.0 ms**.
  - **Relative gate:** Δp50 ≤ **10%** of the block Direct p50.
  - **Goodput gate:** arm goodput ≥ **95%** of the block Direct goodput **and**
    0.00% failures.
  - Report the arm’s absolute p50 and p99 alongside the deltas; a gate verdict
    without the absolute values is incomplete.
  - The Block 3 control reports **C-local − D-local** as the HAProxy-only delta.
    It is diagnostic and is not part of the headline gate table.
- “Configured correctly” additionally requires preflight to pass and the HAProxy
  frontend/backend counters to move during the functional check.

---

## 13. Deliverables and file layout

```
infra/phase2-rerun/
  eksctl-cluster.yaml
  README.md                     # create / kubeconfig / delete commands
benchmarks/phase2-cpu-rerun/
  00-namespace.yaml
  01-direct-serving.yaml
  02-ray-python-proxy.yaml
  03-ray-haproxy.yaml
  04-loadgen-job.yaml
  preflight.sh
  README.md
loadtest/run_matrix.py          # extended, backward-compatible defaults
results/analyze.py              # extended H1 table
results/phase2-rerun-2026-11-18/...
results/phase2-rerun-2026-11-18-report.md
```

The existing `k8s/` manifests stay in place as the historical October setup,
with a note pointing to the rerun.

---

## 14. Cost and teardown

| Item | Rate | Expected |
|---|---|---|
| 4 × `c6i.2xlarge` on-demand | ~$0.34/hr each | ~$1.36/hr |
| EKS control plane | $0.10/hr | ~$0.10/hr |
| NAT gateway / EBS / logs | ~$0.10/hr | ~$0.10/hr |
| **Total** | ~$1.56/hr | **~$2–3 for a 1.5–2 h session** |

- Hard budget cap: **$10**. If creation or debugging exceeds that, stop and
  report.
- Teardown command:
  `eksctl delete cluster --name ray-serve-phase2-rerun --region us-east-1 --wait`
- After deletion, verify no tagged EC2 instances, NAT gateways, or unattached
  EBS volumes remain; delete the CloudWatch log group if eksctl created one.
- Phase 3 gets a separate GPU cluster later; do not keep this cluster for it.

## 15. Risks and fallbacks

| Risk | Fallback |
|---|---|
| EKS cannot place 4 × `c6i.2xlarge` in `us-east-1d` | Retry; if capacity is unavailable, use `us-east-1a` for **all** nodes and record the AZ change |
| `rayproject/ray:2.60.0-py311` lacks the HAProxy binary | Install `ray-haproxy` on the nodes (not `runtime_env`) and document; preflight catches it |
| KubeRay 1.7.2 / Ray 2.60 incompatibility | Step back to Ray 2.59.x or the latest 1.7.x chart; record the version change |
| EKS/IAM creation denied | Stop before any load; report the exact failing action |
| `kubectl` 1.28 client skew | Install 1.34.13 to `~/.local/bin` and use it for the run |
| Sync `predict` interacts badly with throughput mode | Offload to a thread in **both** Ray arms, rerun those arms, disclose |
| Direct-ingress ports differ between the two workers | Recreate the RayService once and re-check; if they still differ, drop arm D and D-local and report C − D attribution as unavailable. Never patch a single Service `targetPort` while it has two endpoints |
| HAProxy cross-node balancing confounds C − D | Run Block 3 (single replica pinned to worker-1, local-only HAProxy). If the control is dropped, label C − D as “HAProxy + cross-node balancing” and make no HAProxy-only claim |
| Forcing `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` for arm B breaks proxy placement | Record the failure; run B head-only and disclose the topology confound rather than silently comparing different placements |

## 16. Decisions already made

- Approach **A** (eksctl + static nodegroups).
- **4-arm headline** comparison: Direct, Ray Python proxy, Ray HAProxy, Ray
  direct ingress — plus a **3-trial Block 3 control** to isolate HAProxy from
  cross-node balancing.
- Arm B sets `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` (the one deviation from the
  throughput bundle) so it keeps per-node proxies and is comparable to C.
- Align the server-side timer boundaries in both apps before collecting data.
- Gate semantics are delta-based versus the block-specific Direct baseline:
  Δp99 ≤ 3.0 ms, Δp50 ≤ 10%, goodput ≥ 95%, 0.00% failures.
- No `ray-haproxy` in `runtime_env`; the image provides it.
- New versioned results and report; historical data untouched.
- Cluster deleted after Phase 2.

## 17. Left to the builder

- Exact eksctl VPC/subnet stanza and nodegroup AMI settings.
- HAProxy stats endpoint path and counter names used in preflight.
- Service/port names and manifest organization details.
- Prose and chart generation for the final report.
- Any retry/backoff logic for nodegroup creation.
