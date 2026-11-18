# Cluster Topology & Request Path

**Last updated:** 2026-11-18
**Applies to:** Phase 1, Phase 2, and Phase 3 benchmark runs.

This document records the cluster topology actually implied by the manifests, the
Kubernetes and Ray Serve request paths used by each architecture, and the port roles
that were previously mislabeled in the reports.

---

## 0. Provenance & known gaps

- The EKS cluster (`noted-marlin`, `us-east-1`, namespace `model-serving-benchmark`)
  was provisioned with Karpenter. **No Terraform, eksctl, or NodePool manifests are
  stored in this repository.**
- **Node counts were not captured during the original runs.** The tables below are
  derived from the applied pod specs and the teardown record in
  `STATUS_AND_TODO.txt`. They describe scheduling constraints, not a measured node
  inventory.
- **Node isolation was not enforced.** The manifests pin only
  `node.kubernetes.io/instance-type` and `topology.kubernetes.io/zone`. There is no
  pod anti-affinity, no `topologySpreadConstraints`, and no dedicated taint. Pods
  belonging to different architectures could therefore share a node, and the
  k6 driver could share a node with a server. Do not assume "one architecture per
  node" from the instance-type label alone.
- Exact node assignment must be captured from `kubectl get pods -o wide` during a
  run; see [Section 6](#6-how-to-capture-topology-for-the-next-run).

---

## 1. Phase 2 topology (CPU tabular inference)

All pods are pinned to `c6i.2xlarge` on-demand nodes in `us-east-1d`.

| Component | Pods | CPU / memory request per pod | Notes |
|---|---|---|---|
| Direct Serving (Uvicorn) | 2 | 1 vCPU / 2 GiB | `direct-serving-svc:8000` |
| Ray Serve B (default proxy) | 1 head + 2 workers | head 1 vCPU / 3 GiB; workers 1 vCPU / 2 GiB | `num-cpus: "0"` on the head keeps model replicas off it |
| Ray Serve C (direct ingress) | 1 head + 2 workers | same as B | separate RayService |
| k6 load generator Job | 1 | 1 vCPU request / 2 vCPU limit | same instance type and zone |

- **Total benchmark pods:** 9.
- A `c6i.2xlarge` provides 8 vCPU / 16 GiB. After DaemonSets, all nine pods can fit
  on **two nodes**; the actual count was not recorded. Karpenter was free to pack
  pods from different architectures onto the same node.
- The two Ray model replicas are Ray actors scheduled inside the worker pods, not
  separate Kubernetes pods. The head has `num-cpus: "0"`, so neither replica can be
  placed there.
- ClusterIP load-balances **per TCP connection**, not per request. k6 uses
  keep-alive, so each VU is pinned to one backend pod for the life of its connection.

---

## 2. Phase 3 topology (GPU embeddings)

| Component | Pods | Node selector | Resources |
|---|---|---|---|
| Direct GPU embed | 1 | `g6.xlarge`, `us-east-1d`, on-demand | 1 GPU, 2 vCPU request / 4 limit, 8 GiB request / 12 limit |
| Ray Serve GPU head | 1 | `karpenter.k8s.aws/instance-family: c6i`, `us-east-1d` | 1 vCPU request / 2 limit, 3 GiB / 5 GiB |
| Ray Serve GPU worker | 1 | `g6.xlarge`, `us-east-1d`, on-demand | 1 GPU, 2 vCPU request / 4 limit, 8 GiB / 12 GiB |
| k6 driver Job | 1 | `us-east-1d`, on-demand (instance type historically unpinned) | 2 vCPU request / 4 limit, 4 GiB / 8 GiB |

- **Minimum GPU nodes:** 2 × `g6.xlarge` (one GPU pod per architecture). The Ray
  head needs a `c6i` node. The driver can land on any `us-east-1d` node, including a
  GPU node or the Ray head's node.
- The Phase 3 report previously claimed the driver ran on a matched `g6.xlarge`.
  That was never enforced: the Job manifest pinned only the zone. The historical
  driver node is not recorded. `job-k6-embed.yaml` now pins `c6i.2xlarge` so future
  runs are deterministic and the driver stays off the GPU nodes.
- **Unresolved discrepancy:** `STATUS_AND_TODO.txt` records teardown of
  **1 × `g6.8xlarge` + 2 × `g6.xlarge`**, while the current manifests specify only
  `g6.xlarge`. The role and run provenance of the `g6.8xlarge` are not documented.
  Resolve from AWS Cost Explorer / EC2 launch history before citing node counts.

---

## 3. Port map (Ray 2.55.1 with HAProxy)

| Port | Role | Who binds it |
|---|---|---|
| **8000** | Ray Serve HTTP ingress (`http_options.port`) | **HAProxy** when `RAY_SERVE_ENABLE_HA_PROXY=1`; otherwise the Python Serve proxy. One per Ray pod under `proxy_location: EveryNode`. |
| **30000–31000** | Direct-ingress HTTP server range, one server inside each ingress-deployment replica | The Serve replica itself; HAProxy forwards to these ports |
| 40000–41000 | Direct-ingress gRPC server range | The Serve replica (not used in this benchmark) |
| 8080 | Serve metrics | Head and workers |
| 8265 | Ray dashboard | Head |
| 6379 | Ray GCS | Head |
| 8404 | HAProxy stats | Pods running HAProxy, when enabled |
| 9101 | HAProxy metrics | Pods running HAProxy, when enabled |

**Source of truth (Ray 2.55.1):**

- `ray/serve/_private/constants.py`: `DEFAULT_HTTP_PORT = 8000`;
  `RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT = 30000`;
  `RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT = 31000`;
  `if RAY_SERVE_ENABLE_HA_PROXY: RAY_SERVE_ENABLE_DIRECT_INGRESS = True`.
- `ray/serve/_private/haproxy.py`: `frontend_port` returns `self.http_options.port`;
  startup log: `HAProxy starting on node ... (HTTP port: ...)`.
- `ray/serve/_private/haproxy_templates.py`: `bind {{ frontend_host }}:{{ frontend_port }}`
  and `server {{ server.host }}:{{ server.port }}` for backends.
- `ray/serve/_private/replica.py`: `_maybe_start_direct_ingress_servers()` starts the
  replica HTTP server and logs `Started HTTP server on port {self._http_port}`.
- Ray documentation: "each Ray pod's proxy ingress (serving on port 8000 by
  default) is now HAProxy ingress."
- Anyscale HAProxy deep dive: "Ingress deployments now contain HTTP servers with
  ports in the `:30000` range, to which HAProxy routes incoming requests."

### Correction to the earlier reports

The benchmark Services expose `port: 8000, targetPort: 30000`. That mapping connects
clients **straight to the replica's direct-ingress HTTP server** and **skips HAProxy**.
The earlier label **"Worker HAProxy :30000"** inverted the two ports. The correct
label is **"Ray Serve Direct Ingress :30000"**.

The resource names `ray-serve-worker-haproxy` and
`ray-serve-worker-haproxy-svc` are retained for reproducibility; they are historical
names for an arm that tests direct ingress.

---

## 4. Request paths

### 4.1 Phase 2 — Direct Serving baseline

```
k6 pod (c6i.2xlarge, us-east-1d)
  │ POST /infer, keep-alive TCP
  ▼
ClusterIP direct-serving-svc:8000          ← kube-proxy DNAT per connection
  ▼
Uvicorn pod #1 or #2 (1 vCPU cgroup limit)
  FastAPI route → await request.body() → orjson.loads
  → xgb.DMatrix → booster.predict → orjson.dumps
  ▼
k6 records http_req_duration client-side
```

No proxy, no Ray, no head node.

### 4.2 Phase 2 — Ray Serve B (default Python proxy)

`serveConfigV2` sets `proxy_location: EveryNode` and does **not** enable HAProxy, so a
Python Serve proxy listens on `:8000` in every Ray pod.

```
k6 pod
  ▼
ClusterIP <rayservice>-serve-svc:8000
  │ KubeRay-generated Service: selector ray.io/cluster=<name>, ray.io/serve=true
  │ Endpoints: worker pods always; head once its proxy health check passes
  ▼
Python Serve proxy :8000 (whichever pod the TCP connection landed on)
  ▼ Serve router / DeploymentHandle → Ray actor RPC
XGBoostDeployment replica (1 of 2, on a worker pod)
  __call__ → _handle_infer
```

The head is **not** the only ingress endpoint. The earlier "head-node Python proxy"
wording described the path only when the connection landed on the head; it was not a
property of the architecture. If an endpoint list shows head-only, inspect
`kubectl get endpoints <service>` and the `ray.io/serve` labels rather than assuming
the design.

### 4.3 Phase 2 — Ray Serve C (direct ingress)

```
k6 pod
  ▼
ClusterIP ray-serve-worker-haproxy-svc:8000
  ▼ targetPort 30000 (kube-proxy picks one worker pod)
Worker pod :30000
  │ Serve replica's direct-ingress HTTP server (ASGI/uvicorn-style)
  ▼
XGBoostDeployment replica.__call__ → _handle_infer

HAProxy on worker :8000   ── present, NOT in the path
Ray head / Python proxy   ── NOT in the path
```

Because this is a single-deployment application, the ingress deployment *is* the
model deployment. The `:30000` server belongs to the XGBoost replica itself, so there
is no separate proxy actor and no `DeploymentHandle` hop for these requests.

### 4.4 Phase 3 — Direct GPU embeddings

```
k6 pod (us-east-1d)
  ▼
ClusterIP direct-gpu-embed-svc:8000
  ▼
Uvicorn pod on g6.xlarge :8000
  /embed → asyncio.Queue → batch worker (max 32, 5 ms)
  → asyncio.to_thread(model.encode FP16 on L4) → future → response
```

### 4.5 Phase 3 — Ray Serve GPU

```
k6 pod
  ▼
ClusterIP rayservice-gpu-embed-haproxy-svc:8000
  ▼ targetPort 30000
GPU worker pod :30000
  │ EmbeddingDeployment replica's direct-ingress HTTP server
  ▼
__call__ → _handle_embed → @serve.batch (max 32, 5 ms)
  → asyncio.to_thread(model.encode FP16 on L4) → response

Ray head (c6i): GCS, Serve controller, dashboard. Its HAProxy on :8000 is not used.
```

---

## 5. What this means for the results

- The measured latency and throughput numbers are unchanged. Only the architectural
  label and the causal attribution change.
- Architecture C measures **Ray Serve direct ingress**, not HAProxy. HAProxy is
  enabled and listening on each worker's `:8000`, but the test Service does not use
  it.
- The +0.301 ms p50 / +1.151 ms p99 delta versus Uvicorn is therefore the cost of
  Ray Serve's replica HTTP/ASGI stack, request handling, metrics, and service routing.
  It is **not** an HAProxy overhead.
- The benchmark does not yet contain a true HAProxy arm. To measure it, expose a
  Service that selects worker pods with `targetPort: 8000` (or target KubeRay's
  generated `serve-svc` while HAProxy is enabled).

### Measurement-boundary note

The server-side timers are not perfectly matched:

- Direct CPU (`k8s/app/direct_app.py`): `t0` is taken **before** `await request.body()`;
  the reported `total_latency_ms` ends after `booster.predict` and **excludes**
  response serialization.
- Ray CPU (`k8s/app/ray_serve_app.py`): `t0` is taken **after** `await request.body()`;
  the reported body field also excludes serialization (the internal histogram
  includes it).
- Direct GPU (`benchmarks/gpu-embed/direct/app.py`): `t0` before body read; reported
  total ends before `orjson.dumps`.
- Ray GPU (`benchmarks/gpu-embed/ray/serve_embed.py`): `t0` after body read; reported
  total ends before `orjson.dumps`.

`network_transit_ms = http_req_duration - total_latency_ms` therefore carries a small
boundary bias (body-read time counted on the Direct side, not on the Ray side). For
these payload sizes the bias is small relative to the reported medians, but the
earlier claim of exact boundary parity should be read as approximate. Make the timers
identical before publishing hop-by-hop attributions.

---

## 6. How to capture topology for the next run

Run these during the measurement window and store the output with the raw results:

```bash
kubectl get nodes -o wide \
  -L topology.kubernetes.io/zone,node.kubernetes.io/instance-type,karpenter.sh/capacity-type

kubectl get pods -n model-serving-benchmark -o wide \
  -o custom-columns='NAME:.metadata.name,NODE:.spec.nodeName,POD_IP:.status.podIP,IMAGE:.spec.containers[0].image'

kubectl get endpoints -n model-serving-benchmark
kubectl get svc -n model-serving-benchmark -o wide
```

For each Service, confirm which pods are actual endpoints and, for Ray, which pod is
listening on `:8000` versus `:30000`:

```bash
kubectl exec -n model-serving-benchmark <pod> -- sh -c \
  'ss -ltnp 2>/dev/null | grep -E ":(8000|3000[0-9]|8080)\b"'
```

---

## 7. Open items

1. Capture and publish actual node assignment for every trial.
2. Decide whether to enforce node isolation (anti-affinity or dedicated NodePool) for
   the final run.
3. Add a true HAProxy arm (`targetPort: 8000`) to separate HAProxy cost from direct
   ingress.
4. Resolve the `g6.8xlarge` provenance in `STATUS_AND_TODO.txt`.
5. Align the server-side timing boundaries across Direct and Ray before publishing
   hop-by-hop transit numbers.
