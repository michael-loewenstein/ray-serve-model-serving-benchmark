# Phase 2 HAProxy Rerun Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rerun Phase 2 on Ray 2.60.0 / KubeRay 1.7.2 with HAProxy actually serving the measured path, four headline arms plus a local-only HAProxy control, and versioned results.

**Architecture:** A new eksctl cluster with four static `c6i.2xlarge` nodes in `us-west-2a` (head, two workers, loadgen). Direct and one RayService coexist on the two worker nodes; blocks are run sequentially with interleaved trials. A preflight gate proves HAProxy is on `:8000` before load. Results go to a new versioned directory; the October dataset is untouched.

**Tech Stack:** AWS EKS 1.34, eksctl 0.230.0, kubectl 1.34.13, Helm 3.19.2, KubeRay 1.7.2, Ray 2.60.0, k6 0.54.0, Python 3.9 locally / 3.11 in-cluster, pytest 8.4.2, PyYAML 6.0.3.

**Spec:** `docs/superpowers/specs/2026-11-18-phase2-haproxy-rerun-design.md`

## Global Constraints

- AWS profile: `<aws-profile>`; region `us-west-2`; data plane `us-west-2a`. Never touch `<existing-shared-cluster>` or the other existing clusters in us-west-2. Never write credentials to disk or manifests.
- **Amendment (2026-11-18, user decision):** the original us-east-1/us-east-1d placement is unreachable — that region is at 30/30 VPC and 30/30 internet-gateway quotas. The run moves to us-west-2 (VPC quota 500, usage 187; IGW quota 500, usage 186), with every nodegroup pinned to `us-west-2a` (`c6i.2xlarge` offered there). The measurement is unaffected: it is single-AZ and in-cluster.
- Ray **2.60.0**, KubeRay **1.7.2**, EKS **1.34**, image `rayproject/ray:2.60.0-py311`.
- HAProxy arm: `RAY_SERVE_ENABLE_HA_PROXY=1`; Python-proxy arm: `0`; both Ray arms: `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`.
- Arm B only: `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` (the one deviation from the bundle).
- `max_ongoing_requests=1000`; Ray worker CPU request/limit **2**; Direct pod **1**; `OMP_NUM_THREADS=1`.
- Never set `RAY_IGNORE_VERSION_CHECK`; never put `ray-haproxy` in `runtime_env.pip`.
- Services: HAProxy `8000→8000`; direct ingress `8000→30000`; Python proxy `8000→8000`; Direct `8000→8000`.
- Do not modify `docs/report.md`, `results/summary_report.md`, `results/raw_trials.json`, or `results/raw_trials.csv`.
- Results only under `results/phase2-rerun-2026-11-18/`.
- Gates are deltas vs the **block-specific** Direct baseline: Δp99 ≤ 3.0 ms, Δp50 ≤ 10%, goodput ≥ 95%, 0.00% failures.
- Hard AWS budget cap **$10**; delete the cluster at the end.

## Review Focus

These are the failure modes the spec implies but no single unit test fully covers. Each has a pinning test or gate noted.

1. **HAProxy silently not in the path** — the Service targets `:30000`, HAProxy falls back, or the Python proxy serves `:8000`. Expected: requests traverse HAProxy. Pinned by `test_manifests.py` (target ports and env flags) and the preflight HAProxy-actor/stats gate.
2. **Arm B becomes head-only** — throughput mode enables direct ingress, which forces `HeadOnly` when HAProxy is off. Expected: per-node Python proxies. Pinned by `test_manifests.py` asserting `RAY_SERVE_ENABLE_DIRECT_INGRESS=0` for B.
3. **Cross-node HAProxy balancing inflates C−D** — HAProxy balances across both replicas. Expected: a local-only control quantifies HAProxy alone. Pinned by the Block 3 control and its report label.
4. **Direct-ingress ports diverge between workers** — one Service `targetPort` cannot serve two ports. Expected: preflight aborts or arm D is dropped. Pinned by `preflight.sh` checking both workers.
5. **Timer boundary regresses** — Ray samples `t0` after the body read. Expected: both apps sample before `request.body()`. Pinned by `test_timer_alignment.py`.

---

## File Structure

```
requirements-dev.txt                         # pytest, PyYAML
tests/conftest.py                            # repo/loadtest import path
tests/test_infra.py                          # eksctl config assertions
tests/test_timer_alignment.py                # AST timer-boundary assertions
tests/test_manifests.py                      # RayService/Service/env assertions
tests/test_run_matrix.py                     # block resolution, parsing, append
tests/test_analyze.py                        # block-aware H1 deltas
infra/phase2-rerun/eksctl-cluster.yaml       # cluster + 4 nodegroups
infra/phase2-rerun/README.md                 # create/kubeconfig/delete
benchmarks/phase2-cpu-rerun/
  00-namespace.yaml
  01-direct-serving.yaml
  02-ray-python-proxy.yaml
  03-ray-haproxy.yaml
  03b-ray-haproxy-local.yaml
  04-loadgen-driver.yaml
  preflight.sh
  README.md
loadtest/run_matrix.py                       # modify: blocks, warmup, append
results/analyze.py                           # modify: block-aware baselines
results/phase2-rerun-2026-11-18/             # created during the run
  preflight/
  raw_trials.json
  raw_trials.csv
  summary_report.md
  block1/ block2/ block3/
results/phase2-rerun-2026-11-18-report.md    # final report
```

---

### Task 1: Test harness and cluster config

**Files:**
- Create: `requirements-dev.txt`
- Create: `tests/conftest.py`
- Create: `tests/test_infra.py`
- Create: `infra/phase2-rerun/eksctl-cluster.yaml`
- Create: `infra/phase2-rerun/README.md`

**Interfaces:**
- Consumes: none
- Produces: `infra/phase2-rerun/eksctl-cluster.yaml` consumed by Task 6.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_infra.py
import yaml
from pathlib import Path

CONFIG = Path(__file__).parents[1] / "infra/phase2-rerun/eksctl-cluster.yaml"

def load_config():
    return yaml.safe_load(CONFIG.read_text())

def test_cluster_identity():
    c = load_config()
    assert c["metadata"]["name"] == "ray-serve-phase2-rerun"
    assert c["metadata"]["region"] == "us-west-2"
    assert c["metadata"]["version"] == "1.34"

def test_four_single_node_groups_in_one_az():
    c = load_config()
    groups = {g["name"]: g for g in c["managedNodeGroups"]}
    assert set(groups) == {"head", "worker-w1", "worker-w2", "loadgen"}
    for g in groups.values():
        assert g["instanceType"] == "c6i.2xlarge"
        assert g["desiredCapacity"] == 1
        assert g["availabilityZones"] == ["us-west-2a"]

def test_worker_slot_labels():
    c = load_config()
    groups = {g["name"]: g for g in c["managedNodeGroups"]}
    assert groups["worker-w1"]["labels"]["bench-slot"] == "w1"
    assert groups["worker-w2"]["labels"]["bench-slot"] == "w2"
    assert groups["head"]["labels"]["bench-role"] == "head"
    assert groups["loadgen"]["labels"]["bench-role"] == "loadgen"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_infra.py -v`
Expected: FAIL with `FileNotFoundError` for the config.

- [ ] **Step 3: Write the config, README, and test harness files**

Also create `requirements-dev.txt`:

```
pytest==8.4.2
PyYAML==6.0.3
```

and `tests/conftest.py` (empty is fine; tests insert their own import paths).

`infra/phase2-rerun/eksctl-cluster.yaml`:

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: ray-serve-phase2-rerun
  region: us-west-2
  version: "1.34"
  tags:
    project: ray-serve-benchmark
    purpose: phase2-haproxy-rerun
availabilityZones:
  - us-west-2a
  - us-west-2b
vpc:
  cidr: 10.42.0.0/16
  nat:
    gateway: Single
iam:
  withOIDC: true
managedNodeGroups:
  - name: head
    instanceType: c6i.2xlarge
    amiFamily: AmazonLinux2023
    desiredCapacity: 1
    minSize: 1
    maxSize: 1
    availabilityZones: ["us-west-2a"]
    labels: {bench-role: head}
    tags: {project: ray-serve-benchmark}
  - name: worker-w1
    instanceType: c6i.2xlarge
    amiFamily: AmazonLinux2023
    desiredCapacity: 1
    minSize: 1
    maxSize: 1
    availabilityZones: ["us-west-2a"]
    labels: {bench-role: worker, bench-slot: w1}
    tags: {project: ray-serve-benchmark}
  - name: worker-w2
    instanceType: c6i.2xlarge
    amiFamily: AmazonLinux2023
    desiredCapacity: 1
    minSize: 1
    maxSize: 1
    availabilityZones: ["us-west-2a"]
    labels: {bench-role: worker, bench-slot: w2}
    tags: {project: ray-serve-benchmark}
  - name: loadgen
    instanceType: c6i.2xlarge
    amiFamily: AmazonLinux2023
    desiredCapacity: 1
    minSize: 1
    maxSize: 1
    availabilityZones: ["us-west-2a"]
    labels: {bench-role: loadgen}
    tags: {project: ray-serve-benchmark}
```

`infra/phase2-rerun/README.md` documents create, kubeconfig, and delete commands with the profile.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_infra.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add requirements-dev.txt tests/conftest.py tests/test_infra.py infra/phase2-rerun/
git commit -m "test: add rerun cluster config and assertions"
```

---

### Task 2: Align the Ray server timer with Direct

**Files:**
- Modify: `k8s/app/ray_serve_app.py:53-69`
- Verify: `k8s/app/direct_app.py:39-42` already assigns `t0` before `await request.body()`
- Create: `tests/test_timer_alignment.py`

**Interfaces:**
- Consumes: none
- Produces: Ray `total_latency_ms` includes body read, matching Direct; consumed by Tasks 3, 8-10.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_timer_alignment.py
import ast
from pathlib import Path

ROOT = Path(__file__).parents[1]

def function(tree, cls, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name:
                    return item
    raise AssertionError(f"{cls}.{name} not found")

def first_line(node, predicate):
    for child in ast.walk(node):
        if predicate(child):
            return child.lineno
    return None

def test_direct_samples_before_body_read():
    tree = ast.parse((ROOT / "k8s/app/direct_app.py").read_text())
    fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "infer":
            fn = node
    assert fn is not None
    t0 = first_line(fn, lambda n: isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "t0" for t in n.targets))
    body = first_line(fn, lambda n: isinstance(n, ast.Await)
                      and isinstance(n.value, ast.Call)
                      and getattr(n.value.func, "attr", "") == "body")
    assert t0 < body

def test_ray_samples_before_body_read():
    tree = ast.parse((ROOT / "k8s/app/ray_serve_app.py").read_text())
    fn = function(tree, "XGBoostDeployment", "__call__")
    t0 = first_line(fn, lambda n: isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "t0" for t in n.targets))
    body = first_line(fn, lambda n: isinstance(n, ast.Await)
                      and isinstance(n.value, ast.Call)
                      and getattr(n.value.func, "attr", "") == "body")
    assert t0 is not None and body is not None and t0 < body
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_timer_alignment.py -v`
Expected: `test_ray_samples_before_body_read` FAILS (`t0` is None in `__call__`); Direct passes.

- [ ] **Step 3: Move `t0` into `__call__`**

In `k8s/app/ray_serve_app.py`, set `t0 = time.perf_counter()` as the first statement of `async def __call__`, and pass it:
`return await self._handle_infer(t0, await request.body())`.
Change the signature to `async def _handle_infer(self, t0: float, raw: bytes) -> Response:` and delete its local `t0 = time.perf_counter()`. Keep `total_latency_ms` as `(t2 - t0) * 1000.0`. Leave the histogram's `t3 - t0` total as the server-internal measure.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_timer_alignment.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add k8s/app/ray_serve_app.py tests/test_timer_alignment.py
git commit -m "fix: align Ray server timer with Direct baseline"
```

---

### Task 3: Rerun manifests and preflight script

**Files:**
- Create: `benchmarks/phase2-cpu-rerun/00-namespace.yaml`
- Create: `benchmarks/phase2-cpu-rerun/01-direct-serving.yaml`
- Create: `benchmarks/phase2-cpu-rerun/02-ray-python-proxy.yaml`
- Create: `benchmarks/phase2-cpu-rerun/03-ray-haproxy.yaml`
- Create: `benchmarks/phase2-cpu-rerun/03b-ray-haproxy-local.yaml`
- Create: `benchmarks/phase2-cpu-rerun/04-loadgen-driver.yaml`
- Create: `benchmarks/phase2-cpu-rerun/preflight.sh`
- Create: `benchmarks/phase2-cpu-rerun/README.md`
- Modify: `k8s/app/ray_serve_app.py` (decorator `max_ongoing_requests=1000`)
- Create: `tests/test_manifests.py`

**Interfaces:**
- Consumes: config from Task 1, app timer from Task 2.
- Produces: Service names consumed by Task 4:
  `direct-serving-svc`, `ray-python-proxy-svc`, `ray-haproxy-svc`,
  `ray-direct-ingress-svc`, `ray-haproxy-w1-svc`, `ray-direct-w1-svc`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_manifests.py
import yaml
from pathlib import Path

ROOT = Path(__file__).parents[1] / "benchmarks/phase2-cpu-rerun"

def docs(name):
    return [d for d in yaml.safe_load_all((ROOT / name).read_text()) if d]

def rayservice(name):
    return [d for d in docs(name) if d["kind"] == "RayService"][0]

def env(container):
    return {e["name"]: e.get("value") for e in container.get("env", [])}

def test_ray_version_and_image():
    for name in ["02-ray-python-proxy.yaml", "03-ray-haproxy.yaml", "03b-ray-haproxy-local.yaml"]:
        rs = rayservice(name)
        assert rs["spec"]["rayClusterConfig"]["rayVersion"] == "2.60.0"
        head = rs["spec"]["rayClusterConfig"]["headGroupSpec"]["template"]["spec"]["containers"][0]
        assert "rayproject/ray:2.60.0-py311" == head["image"]

def test_haproxy_and_python_proxy_flags():
    hap = env(rayservice("03-ray-haproxy.yaml")["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0])
    py = env(rayservice("02-ray-python-proxy.yaml")["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0])
    assert hap["RAY_SERVE_ENABLE_HA_PROXY"] == "1"
    assert "RAY_SERVE_ENABLE_DIRECT_INGRESS" not in hap or hap["RAY_SERVE_ENABLE_DIRECT_INGRESS"] == "1"
    assert py["RAY_SERVE_ENABLE_HA_PROXY"] == "0"
    assert py["RAY_SERVE_ENABLE_DIRECT_INGRESS"] == "0"
    for c in (hap, py):
        assert c["RAY_SERVE_THROUGHPUT_OPTIMIZED"] == "1"

def test_max_ongoing_and_worker_cpu():
    rs = rayservice("03-ray-haproxy.yaml")
    app = yaml.safe_load(rs["spec"]["serveConfigV2"])["applications"][0]
    dep = app["deployments"][0]
    assert dep["max_ongoing_requests"] == 1000
    worker = rs["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]
    assert worker["resources"]["limits"]["cpu"] == "2"
    app_src = (ROOT.parents[1] / "k8s/app/ray_serve_app.py").read_text()
    assert "max_ongoing_requests=1000" in app_src

def test_services_target_correct_ports():
    expected = {
        "ray-haproxy-svc": 8000,
        "ray-direct-ingress-svc": 30000,
        "ray-python-proxy-svc": 8000,
        "ray-haproxy-w1-svc": 8000,
        "ray-direct-w1-svc": 30000,
        "direct-serving-svc": 8000,
    }
    found = {}
    for f in ROOT.glob("*.yaml"):
        for d in docs(f.name):
            if d and d["kind"] == "Service":
                found[d["metadata"]["name"]] = d["spec"]["ports"][0]["targetPort"]
    assert {k: found[k] for k in expected} == expected

def test_no_redundant_haproxy_pip_or_version_override():
    for f in ROOT.glob("0*.yaml"):
        text = (ROOT / f.name).read_text()
        assert "RAY_IGNORE_VERSION_CHECK" not in text
        assert "ray-haproxy==" not in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_manifests.py -v`
Expected: FAIL with `FileNotFoundError`.

- [ ] **Step 3: Write the manifests and update the decorator**

Change the existing `@serve.deployment(...)` in `k8s/app/ray_serve_app.py` from
`max_ongoing_requests=50` to `max_ongoing_requests=1000` so the decorator and the
Serve config agree.

Required values, per manifest:

| Manifest | Name | Key settings |
|---|---|---|
| `00-namespace.yaml` | `model-serving-benchmark` | namespace |
| `01-direct-serving.yaml` | `direct-serving-rerun` | 2 replicas, `bench-role=worker`, anti-affinity, 1 CPU/2 GiB, `python:3.11-slim`, `uvicorn direct_app:app --port 8000`, Service `direct-serving-svc:8000→8000` |
| `02-ray-python-proxy.yaml` | `ray-python-proxy-rerun` | Ray head `bench-role=head` 1 CPU/3 GiB; 2 workers `bench-role=worker` 2 CPU/4 GiB, anti-affinity; HAProxy `0`, direct ingress `0`, throughput `1`; Service `ray-python-proxy-svc:8000→8000` selecting `ray.io/node-type=worker` |
| `03-ray-haproxy.yaml` | `ray-haproxy-rerun` | Same head/workers; HAProxy `1`, throughput `1`; Services `ray-haproxy-svc:8000→8000` and `ray-direct-ingress-svc:8000→30000` |
| `03b-ray-haproxy-local.yaml` | `ray-haproxy-local-rerun` | **1 worker replica**, `nodeSelector bench-slot=w1`, pod label `bench-slot: w1`; same env; Serve deployment `num_replicas: 1`; Services `ray-haproxy-w1-svc:8000→8000`, `ray-direct-w1-svc:8000→30000` selecting `bench-slot=w1` |
| `04-loadgen-driver.yaml` | `loadgen-driver` | 1 replica `bench-role=loadgen`, `python:3.11-slim`, `workingDir: /workspace/loadtest`, main container installs `curl ca-certificates tar` + downloads k6 v0.54.0 to `/usr/local/bin`, mounts ConfigMap `loadgen-scripts` at `/workspace/loadtest/run_matrix.py` and `/workspace/loadtest/k6_benchmark.js`, ConfigMap `loadgen-fixtures` at `/workspace/loadtest/fixtures/sample_payloads.jsonl`, emptyDir at `/results`, command `sleep infinity` |

Shared Serve config for all RayServices:

```yaml
proxy_location: EveryNode
http_options: {host: "0.0.0.0", port: 8000}
applications:
  - name: default
    route_prefix: /
    import_path: ray_serve_app:app
    runtime_env:
      env_vars: { ...same flags as container... }
      pip: ["xgboost==2.1.1", "orjson==3.9.0"]
    deployments:
      - name: XGBoostDeployment
        num_replicas: 2
        max_ongoing_requests: 1000
        ray_actor_options: {num_cpus: 1.0}
```

Worker container env: `MY_POD_IP`, `PYTHONPATH=/opt/ray_serve_app`, `OMP_NUM_THREADS=1`, `MODEL_PATH=/models/model.json`, `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`, `RAY_SERVE_ENABLE_HA_PROXY=1|0`, and for B `RAY_SERVE_ENABLE_DIRECT_INGRESS=0`. Head container gets the same flags. Mount `ray-serve-code` at `/opt/ray_serve_app/ray_serve_app.py` and `benchmark-model-artifact` at `/models`.

All three RayServices use the valid KubeRay v1.7.2 field
`serviceUnhealthySecondThreshold: 300`. Do **not** use
`serviceUnhealthyThreshold`: it is not in the CRD, and kubectl 1.34 strict
validation rejects it.

**Worker `rayStartParams` must include `num-cpus: "1"`** (head keeps
`num-cpus: "0"`). Without it each ~8-vCPU worker registers ~8 logical CPUs, both
1-CPU replica actors pack onto a single worker. Ray 2.60's `EveryNode` proxy
placement covers exactly the nodes hosting a replica actor, so that packing
also strands one worker without an HAProxy/Python proxy and fails its readiness
probe. With 1 logical CPU per worker the second replica cannot fit on the first
worker, giving one replica per worker, 3 proxies, and 2 endpoints per Service.
This value was in the design's resource table but was missing here — it caused
the first two Task 7 preflight failures (2/3 HAProxyManager actors, both
replicas co-located).

`preflight.sh` accepts `haproxy|python|local` and checks:
- node count/roles/AZ/instance type;
- `ray --version` is 2.60.0;
- `haproxy -v` on head and workers (HAProxy modes);
- `ray list actors | grep HAProxyManager` = **3** (head + 2 workers) in
  `haproxy` mode, **2** (head + w1) in `local` mode, and 0 in `python` mode;
  Python proxy actors = 3 in `python` mode. The head proxy is present but is
  not a Service endpoint.
- `ss -ltnp` on every relevant worker shows 8000; in `haproxy` mode both
  workers must also show the same 300xx port; in `local` mode check the
  `bench-slot=w1` worker only. In `python` mode require only `:8000`: arm B sets
  `RAY_SERVE_ENABLE_DIRECT_INGRESS=0`, so Ray 2.60 starts no direct-ingress
  server and there is no 300xx listener;
- endpoints and targetPorts;
- `ray serve status` shows 2 replicas (1 for local) and the RayService config
  contains `max_ongoing_requests: 1000`;
- 10 `/infer` requests return 200;
- HAProxy stats counters increase (HAProxy modes). Parse the CSV header with
  `lstrip('#').split(',')` and strip each field; count only non-`stats`/
  non-`prometheus` frontends and backends.

The script exits non-zero on any failed check and writes evidence to stdout.

- [ ] **Step 4: Run tests and shell syntax check**

Run: `python3 -m pytest tests/test_manifests.py -v && bash -n benchmarks/phase2-cpu-rerun/preflight.sh`
Expected: all tests pass; `bash -n` exits 0.

- [ ] **Step 5: Commit**

```bash
git add benchmarks/phase2-cpu-rerun/ k8s/app/ray_serve_app.py tests/test_manifests.py
git commit -m "feat: add Phase 2 rerun manifests and preflight"
```

---

### Task 4: Block-aware matrix runner

**Files:**
- Modify: `loadtest/run_matrix.py`
- Create: `tests/test_run_matrix.py`

**Interfaces:**
- Consumes: Service names from Task 3.
- Produces:
  - `TARGETS: Dict[str, Dict[str, str]]`
  - `BLOCKS: Dict[str, List[str]]`
  - `resolve_targets(block: str, override: Optional[str] = None) -> List[str]`
  - `parse_k6_json(json_path, target_key, profile, trial_num, block) -> Dict[str, Any]`
  - `export_raw_trials_schema(..., append: bool = False)`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_run_matrix.py
import json, sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "loadtest"))
import run_matrix as rm

def test_blocks_resolve():
    assert rm.resolve_targets("block1") == ["direct", "ray_haproxy", "ray_direct"]
    assert rm.resolve_targets("block2") == ["direct", "ray_python"]
    assert rm.resolve_targets("block3") == ["ray_haproxy_local", "ray_direct_local"]

def test_override_must_belong_to_block():
    assert rm.resolve_targets("block1", "direct") == ["direct"]
    try:
        rm.resolve_targets("block1", "ray_python")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")

def test_parse_k6_json_reads_metrics(tmp_path):
    payload = {"metrics": {
        "http_reqs": {"count": 1500, "rate": 50.0},
        "http_req_duration": {"med": 1.2, "p(90)": 1.5, "p(95)": 1.8, "p(99)": 2.4, "max": 9.0},
        "server_model_latency_ms": {"med": 0.5},
        "server_total_latency_ms": {"med": 0.6},
        "network_roundtrip_latency_ms": {"med": 0.6},
        "successful_requests": {"count": 1500},
        "failed_requests": {"count": 0},
    }}
    p = tmp_path / "t.json"; p.write_text(json.dumps(payload))
    r = rm.parse_k6_json(str(p), "direct", "50rps", 1, "block1")
    assert r["p50_ms"] == 1.2 and r["p99_ms"] == 2.4
    assert r["block"] == "block1" and r["target_key"] == "direct"

def test_append_deduplicates(tmp_path):
    raw = tmp_path / "raw.json"; csv = tmp_path / "raw.csv"
    t = {"architecture": "Direct Serving (Uvicorn)", "target_key": "direct",
         "trial_number": 1, "total_requests": 1, "successful_requests": 1,
         "failed_requests": 0, "goodput_rps": 1.0, "p50_ms": 1.0, "p90_ms": 1.0,
         "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0,
         "pure_booster_predict_p50_ms": 0.5, "server_total_p50_ms": 0.6,
         "network_transit_p50_ms": 0.4, "block": "block1"}
    rm.export_raw_trials_schema([t], str(raw), str(csv), 50, "30s", 5, append=False)
    rm.export_raw_trials_schema([t], str(raw), str(csv), 50, "30s", 5, append=True)
    assert len(json.loads(raw.read_text())["trials"]) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_run_matrix.py -v`
Expected: FAIL (`resolve_targets` missing).

- [ ] **Step 3: Implement block support**

Add the targets and block map, `resolve_targets`, pass `block` into `parse_k6_json`, add `--block`, `--targets`, `--warmup-duration`, `--append`, and a warmup pass that does not record trials. `export_raw_trials_schema` merges existing trials when `append=True`, dedupes by `(block, target_key, trial_number)`, and sorts. Update `TARGETS` names to the corrected labels. Keep the module Python 3.9-compatible.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_run_matrix.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add loadtest/run_matrix.py tests/test_run_matrix.py
git commit -m "feat: add block-aware matrix runner"
```

---

### Task 5: Block-aware analysis

**Files:**
- Modify: `results/analyze.py`
- Create: `tests/test_analyze.py`

**Interfaces:**
- Consumes: raw trials with optional `block` fields.
- Produces: `generate_tables(meta, grouped) -> str` with one H1 row group per Ray arm, compared to that arm's block Direct baseline.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analyze.py
import sys
from pathlib import Path
ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "results"))
import analyze

def trial(arch, key, block, p50):
    return {"architecture": arch, "target_key": key, "block": block,
            "trial_number": 1, "total_requests": 1, "successful_requests": 1,
            "failed_requests": 0, "goodput_rps": 50.0, "p50_ms": p50,
            "p90_ms": p50, "p95_ms": p50, "p99_ms": p50, "max_ms": p50,
            "pure_booster_predict_p50_ms": 0.5, "server_total_p50_ms": 0.6,
            "network_transit_p50_ms": 0.4}

def test_h1_uses_block_baseline():
    trials = [
        trial("Direct Serving (Uvicorn)", "direct", "block1", 1.0),
        trial("Ray Serve (HAProxy :8000)", "ray_haproxy", "block1", 1.3),
        trial("Direct Serving (Uvicorn)", "direct", "block2", 2.0),
        trial("Ray Serve (Default Python Proxy :8000)", "ray_python", "block2", 2.5),
        trial("Ray Serve (HAProxy local control)", "ray_haproxy_local", "block3", 1.1),
        trial("Ray Serve (Direct Ingress local control)", "ray_direct_local", "block3", 1.0),
    ]
    grouped = {}
    for t in trials:
        grouped.setdefault(t["architecture"], []).append(t)
    md = analyze.generate_tables({}, grouped)
    assert "Ray Serve (HAProxy :8000)" in md
    assert "+0.300 ms" in md
    assert "Ray Serve (Default Python Proxy :8000)" in md
    assert "+0.500 ms" in md
    assert "HAProxy-only control" in md
    assert "+0.100 ms" in md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_analyze.py -v`
Expected: FAIL (the current H1 table hardcodes one arm and uses the global Direct median).

- [ ] **Step 3: Implement block-aware baselines**

Add `baseline_for(arch, runs, grouped)` returning Direct trials in the same block; default block is `"legacy"` when absent. Build the H1 table by iterating Ray arms, using their block's Direct median for each metric. Keep the bootstrap CI and the existing summary/transit/individual tables. Use `target_key == "direct"` for the baseline. Also update the report header to print `meta.get("trials_per_block", {})` when present, so Block 1/2 (5) and Block 3 (3) are not misreported; `export_raw_trials_schema` in Task 4 computes that mapping from the trials.

Add a separate **HAProxy-only control** table: `ray_haproxy_local` minus `ray_direct_local` for p50/p90/p95/p99, with the same bootstrap CI. Arms whose block has no Direct baseline are skipped by the H1 table and appear only in this control table.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_analyze.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add results/analyze.py tests/test_analyze.py
git commit -m "feat: compare each Ray arm to its block baseline"
```

---

### Task 6: Create the cluster and install KubeRay

**Files:**
- Create: `results/phase2-rerun-2026-11-18/preflight/` (logs)
- No source changes.

**Interfaces:**
- Consumes: Task 1 config.
- Produces: a ready cluster for Tasks 7-10.

> Tasks 7-12 assume `export KUBECONFIG=~/.kube/ray-serve-phase2-rerun` and
> `K=~/.local/bin/kubectl-1.34.13` in every shell. A dedicated kubeconfig
> avoids accidentally acting on `<existing-shared-cluster>`.

- [ ] **Step 1: Install matching kubectl**

```bash
mkdir -p ~/.local/bin
curl -sL https://dl.k8s.io/release/v1.34.13/bin/linux/amd64/kubectl -o ~/.local/bin/kubectl-1.34.13
chmod +x ~/.local/bin/kubectl-1.34.13
~/.local/bin/kubectl-1.34.13 version --client
```
Expected: client v1.34.13.

- [ ] **Step 2: Create the cluster**

```bash
export AWS_PROFILE=<aws-profile>
export KUBECONFIG=~/.kube/ray-serve-phase2-rerun
mkdir -p results/phase2-rerun-2026-11-18/preflight
eksctl create cluster -f infra/phase2-rerun/eksctl-cluster.yaml --timeout 40m 2>&1 | tee results/phase2-rerun-2026-11-18/preflight/cluster-create.log
aws eks update-kubeconfig --name ray-serve-phase2-rerun --region us-west-2 --profile "$AWS_PROFILE" --kubeconfig "$KUBECONFIG"
```
Expected: four nodes; `kubectl get nodes` shows `bench-role` head/worker/loadgen and all in `us-west-2a`.

- [ ] **Step 3: Install KubeRay 1.7.2**

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ >/dev/null
helm repo update >/dev/null
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.2 \
  --namespace kuberay --create-namespace \
  --set nodeSelector.bench-role=head
```
Expected: operator pod Running on the head node; image tag `v1.7.2`.

- [ ] **Step 4: Record topology**

```bash
~/.local/bin/kubectl-1.34.13 get nodes -o wide \
  -L bench-role,bench-slot,topology.kubernetes.io/zone,node.kubernetes.io/instance-type \
  | tee results/phase2-rerun-2026-11-18/preflight/nodes.txt
~/.local/bin/kubectl-1.34.13 -n kuberay get deploy kuberay-operator \
  -o jsonpath='{.spec.template.spec.containers[0].image}' \
  | tee results/phase2-rerun-2026-11-18/preflight/kuberay-image.txt
```
Expected: 4 nodes, image `quay.io/kuberay/operator:v1.7.2`.

- [ ] **Step 5: Commit evidence**

```bash
git add results/phase2-rerun-2026-11-18/preflight/
git commit -m "chore: record rerun cluster creation evidence"
```

---

### Task 7: Deploy Direct + HAProxy and pass preflight

**Files:**
- No source changes; evidence under `results/phase2-rerun-2026-11-18/preflight/`.

**Interfaces:**
- Consumes: Task 3 manifests, Task 6 cluster.
- Produces: a passed HAProxy preflight for Task 8.

- [ ] **Step 1: Create namespace and ConfigMaps**

```bash
K=~/.local/bin/kubectl-1.34.13
$K apply -f benchmarks/phase2-cpu-rerun/00-namespace.yaml
$K create configmap direct-serving-code --from-file=direct_app.py=k8s/app/direct_app.py -n model-serving-benchmark
$K create configmap ray-serve-code --from-file=ray_serve_app.py=k8s/app/ray_serve_app.py -n model-serving-benchmark
$K create configmap benchmark-model-artifact --from-file=model.json=model/artifacts/model.json -n model-serving-benchmark
```

- [ ] **Step 2: Deploy Direct and the HAProxy RayService**

```bash
$K apply -f benchmarks/phase2-cpu-rerun/01-direct-serving.yaml
$K apply -f benchmarks/phase2-cpu-rerun/03-ray-haproxy.yaml
$K wait --for=condition=available deployment/direct-serving-rerun -n model-serving-benchmark --timeout=300s
$K wait --for=condition=Ready rayservice/ray-haproxy-rerun -n model-serving-benchmark --timeout=600s
```

- [ ] **Step 3: Run preflight**

```bash
bash benchmarks/phase2-cpu-rerun/preflight.sh haproxy \
  | tee results/phase2-rerun-2026-11-18/preflight/haproxy-preflight.log
```
Expected: exit 0; 3 HAProxyManager actors; `:8000` HAProxy and `:300xx` replica on both workers; both workers report the same `300xx`; 10/10 requests succeed; HAProxy stats counters increase.

- [ ] **Step 4: Stop if preflight fails**

If any check fails, do not proceed. Fix the manifest or cluster and rerun preflight. Record the failure and resolution in the log.

- [ ] **Step 5: Commit evidence**

```bash
git add results/phase2-rerun-2026-11-18/preflight/
git commit -m "chore: record HAProxy preflight evidence"
```

---

### Task 8: Run Block 1 (Direct, HAProxy, direct ingress)

**Files:**
- Create: `results/phase2-rerun-2026-11-18/block1/`
- Create: `results/phase2-rerun-2026-11-18/raw_trials.json` / `.csv`

**Interfaces:**
- Consumes: Task 4 runner, Task 7 deployment.
- Produces: 15 trials for Task 11.

- [ ] **Step 1: Deploy the loadgen driver**

```bash
$K create configmap loadgen-scripts \
  --from-file=run_matrix.py=loadtest/run_matrix.py \
  --from-file=k6_benchmark.js=loadtest/k6_benchmark.js \
  -n model-serving-benchmark
$K create configmap loadgen-fixtures \
  --from-file=sample_payloads.jsonl=loadtest/fixtures/sample_payloads.jsonl \
  -n model-serving-benchmark
$K apply -f benchmarks/phase2-cpu-rerun/04-loadgen-driver.yaml
$K wait --for=condition=available deployment/loadgen-driver -n model-serving-benchmark --timeout=300s
```
Expected: the driver pod is on the `bench-role=loadgen` node.

- [ ] **Step 2: Run Block 1**

```bash
$K exec deploy/loadgen-driver -n model-serving-benchmark -- \
  python3 /workspace/loadtest/run_matrix.py \
    --block block1 --trials 5 --rate-rps 50 --duration 30s \
    --warmup-duration 15s --seed 42 \
    --output-dir /results/block1 \
    --output-raw-json /results/raw_trials.json \
    --output-raw-csv /results/raw_trials.csv \
    --append
```
Expected: 15 measured trials, 3 warmups, 0 failures.

- [ ] **Step 3: Copy results**

```bash
POD=$($K get pod -n model-serving-benchmark -l app=loadgen-driver -o jsonpath='{.items[0].metadata.name}')
$K cp "$POD:/results/." results/phase2-rerun-2026-11-18/ -n model-serving-benchmark
```
Expected: `raw_trials.json` has 15 trials, all with `block: block1`.

- [ ] **Step 4: Verify and commit**

```bash
python3 - <<'PY'
import json
d=json.load(open('results/phase2-rerun-2026-11-18/raw_trials.json'))
print(len(d['trials']), {t['target_key'] for t in d['trials']})
assert len(d['trials']) == 15
assert sum(t['failed_requests'] for t in d['trials']) == 0
PY
git add results/phase2-rerun-2026-11-18/
git commit -m "data: add Phase 2 rerun block 1 trials"
```

---

### Task 9: Run Block 3 control (local HAProxy vs local direct ingress)

**Files:**
- Create: `results/phase2-rerun-2026-11-18/block3/`
- Modify: `results/phase2-rerun-2026-11-18/raw_trials.json` / `.csv`

**Interfaces:**
- Consumes: Task 4 runner.
- Produces: 6 control trials for Task 11.

- [ ] **Step 1: Swap to the single-replica local RayService**

```bash
$K delete -f benchmarks/phase2-cpu-rerun/03-ray-haproxy.yaml --wait=true
$K apply -f benchmarks/phase2-cpu-rerun/03b-ray-haproxy-local.yaml
$K wait --for=condition=Ready rayservice/ray-haproxy-local-rerun -n model-serving-benchmark --timeout=600s
```
Expected: one Ray worker on the `bench-slot=w1` node; **2 HAProxyManager actors** (head + w1), with w1 the only Service endpoint.

- [ ] **Step 2: Preflight the local control**

```bash
bash benchmarks/phase2-cpu-rerun/preflight.sh local \
  | tee results/phase2-rerun-2026-11-18/preflight/local-preflight.log
```
Expected: exit 0; one replica; one HAProxy; `:8000` and `:300xx` on w1.

- [ ] **Step 3: Run Block 3**

```bash
$K exec deploy/loadgen-driver -n model-serving-benchmark -- \
  python3 /workspace/loadtest/run_matrix.py \
    --block block3 --trials 3 --rate-rps 50 --duration 30s \
    --warmup-duration 15s --seed 43 \
    --output-dir /results/block3 \
    --output-raw-json /results/raw_trials.json \
    --output-raw-csv /results/raw_trials.csv \
    --append
```
Expected: 6 measured trials, 0 failures.

- [ ] **Step 4: Copy, verify, commit**

```bash
POD=$($K get pod -n model-serving-benchmark -l app=loadgen-driver -o jsonpath='{.items[0].metadata.name}')
$K cp "$POD:/results/." results/phase2-rerun-2026-11-18/ -n model-serving-benchmark
python3 - <<'PY'
import json
d=json.load(open('results/phase2-rerun-2026-11-18/raw_trials.json'))
print(len(d['trials']))
assert len(d['trials']) == 21
assert sum(t['failed_requests'] for t in d['trials']) == 0
PY
git add results/phase2-rerun-2026-11-18/
git commit -m "data: add Phase 2 rerun local HAProxy control trials"
```

---

### Task 10: Run Block 2 (Direct, Python proxy)

**Files:**
- Create: `results/phase2-rerun-2026-11-18/block2/`
- Modify: `results/phase2-rerun-2026-11-18/raw_trials.json` / `.csv`

**Interfaces:**
- Consumes: Task 4 runner.
- Produces: 10 trials for Task 11.

- [ ] **Step 1: Swap to the Python-proxy RayService**

```bash
$K delete -f benchmarks/phase2-cpu-rerun/03b-ray-haproxy-local.yaml --wait=true
$K apply -f benchmarks/phase2-cpu-rerun/02-ray-python-proxy.yaml
$K wait --for=condition=Ready rayservice/ray-python-proxy-rerun -n model-serving-benchmark --timeout=600s
```

- [ ] **Step 2: Preflight the Python proxy**

```bash
bash benchmarks/phase2-cpu-rerun/preflight.sh python \
  | tee results/phase2-rerun-2026-11-18/preflight/python-preflight.log
```
Expected: exit 0; 3 Python proxy actors, 0 HAProxyManager; `:8000` on both workers.

- [ ] **Step 3: Run Block 2**

```bash
$K exec deploy/loadgen-driver -n model-serving-benchmark -- \
  python3 /workspace/loadtest/run_matrix.py \
    --block block2 --trials 5 --rate-rps 50 --duration 30s \
    --warmup-duration 15s --seed 44 \
    --output-dir /results/block2 \
    --output-raw-json /results/raw_trials.json \
    --output-raw-csv /results/raw_trials.csv \
    --append
```
Expected: 10 measured trials, 0 failures.

- [ ] **Step 4: Copy, verify, commit**

```bash
POD=$($K get pod -n model-serving-benchmark -l app=loadgen-driver -o jsonpath='{.items[0].metadata.name}')
$K cp "$POD:/results/." results/phase2-rerun-2026-11-18/ -n model-serving-benchmark
python3 - <<'PY'
import json
d=json.load(open('results/phase2-rerun-2026-11-18/raw_trials.json'))
print(len(d['trials']))
assert len(d['trials']) == 31
assert sum(t['failed_requests'] for t in d['trials']) == 0
PY
git add results/phase2-rerun-2026-11-18/
git commit -m "data: add Phase 2 rerun Python proxy trials"
```

---

### Task 11: Analyze and write the report

**Files:**
- Create: `results/phase2-rerun-2026-11-18/summary_report.md` (generated)
- Create: `results/phase2-rerun-2026-11-18-report.md`
- Modify: `README.md` (link the new report; mark October as historical)

**Interfaces:**
- Consumes: Tasks 5, 8-10.
- Produces: final report.

- [ ] **Step 1: Generate the summary**

```bash
python3 results/analyze.py \
  --data results/phase2-rerun-2026-11-18/raw_trials.json \
  --output results/phase2-rerun-2026-11-18/summary_report.md
```
Expected: H1 rows for HAProxy, Python proxy, direct ingress, and the local control, each using its block Direct baseline; 31 trials.

- [ ] **Step 2: Write the report**

`results/phase2-rerun-2026-11-18-report.md` must contain:
- Versions, topology (4 nodes), preflight evidence summary, and the HAProxy `:8000` proof.
- Absolute p50/p95/p99 and goodput for all arms.
- Deltas vs block-specific Direct with bootstrap CIs.
- Gate table using Δp99 ≤ 3.0 ms, Δp50 ≤ 10%, goodput ≥ 95%, 0.00% failures.
- C − D labeled “HAProxy + cross-node balancing”; C-local − D-local labeled “HAProxy only.”
- Disclosure of the Ray worker 2-vCPU vs Direct 1-vCPU envelope and the arm B direct-ingress override.
- Limitations and the October dataset marked historical.

- [ ] **Step 3: Verify report consistency**

```bash
python3 - <<'PY'
from pathlib import Path
r=Path('results/phase2-rerun-2026-11-18-report.md').read_text()
for s in ['Direct Serving','HAProxy','Python Proxy','Direct Ingress','HAProxy only','cross-node']:
    assert s in r, s
print('report sections present')
PY
```
Expected: `report sections present`.

- [ ] **Step 4: Update README links**

Add a "Phase 2 Rerun (2026-11-18)" section linking the new report and stating the October Phase 2 data is historical/superseded.

- [ ] **Step 5: Commit**

```bash
git add results/phase2-rerun-2026-11-18/ results/phase2-rerun-2026-11-18-report.md README.md
git commit -m "docs: add Phase 2 rerun results and report"
```

---

### Task 12: Tear down and verify

**Files:**
- Create: `results/phase2-rerun-2026-11-18/preflight/teardown.log`

**Interfaces:**
- Consumes: the live cluster.
- Produces: no running AWS resources.

- [ ] **Step 1: Delete the cluster**

```bash
export AWS_PROFILE=<aws-profile>
eksctl delete cluster --name ray-serve-phase2-rerun --region us-west-2 --wait 2>&1 \
  | tee results/phase2-rerun-2026-11-18/preflight/teardown.log
```
Expected: `[✔]  all cluster resources were deleted`.

- [ ] **Step 2: Verify no leftovers**

```bash
aws ec2 describe-instances --profile "$AWS_PROFILE" --region us-west-2 \
  --filters Name=tag:project,Values=ray-serve-benchmark Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output json
aws ec2 describe-nat-gateways --profile "$AWS_PROFILE" --region us-west-2 \
  --filter Name=tag:project,Values=ray-serve-benchmark Name=state,Values=available \
  --query 'NatGateways[].NatGatewayId' --output json
```
Expected: `[]` for both.

- [ ] **Step 3: Commit teardown evidence**

```bash
git add results/phase2-rerun-2026-11-18/preflight/teardown.log
git commit -m "chore: record rerun cluster teardown"
```

---

## Self-Review

**Spec coverage:**
- 4-arm headline → Tasks 3, 8, 10, 11.
- Block 3 control → Tasks 3, 9, 11.
- Ray 2.60 / KubeRay 1.7.2 → Tasks 1, 3, 6.
- HAProxy on `:8000`, no `ray-haproxy` runtime pip → Tasks 3, 7.
- Arm B direct-ingress override → Tasks 3, 10.
- max_ongoing_requests 1000, worker 2 vCPU → Tasks 3, 7.
- Timer alignment → Task 2.
- Direct-ingress port rule → Task 3 preflight.
- Gates → Task 5, 11.
- Versioned results, historical untouched → Tasks 8-11.
- Teardown → Task 12.

**Step scan:** Each step names one action and its check. Infra steps have exact commands and expected output; code steps have test names/assertions and signatures.

**Type consistency:** `resolve_targets`, `parse_k6_json(..., block)`, `export_raw_trials_schema(..., append)`, and `generate_tables(meta, grouped)` are used consistently across Tasks 4, 5, 8-11.

**Review Focus:** Each of the five lines has a pinning test or preflight gate in Tasks 2, 3, 7, 9.

**Proportion:** The plan is longer than the spec because most of it is external infrastructure commands rather than code; code steps specify signatures and tests, not bodies.
