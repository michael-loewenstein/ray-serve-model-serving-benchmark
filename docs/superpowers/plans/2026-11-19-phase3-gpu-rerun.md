# Phase 3 GPU Embeddings Rerun Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-measure the GPU embeddings benchmark (all-MiniLM-L6-v2, FP16, `@serve.batch`) as a 2-arm comparison — Direct vs Ray Serve through verified HAProxy `:8000` — on Ray 2.60.0, with preflight-gated deployment, a hardened matrix runner, and teardown before analysis.

**Architecture:** Same pattern as the Phase 2 rerun: an eksctl 4-node cluster (2 GPU + 2 CPU, single AZ, role labels) built from a committed config; fail-closed preflight gate that proves CUDA, HAProxy in the measured path, and the Serve config before any load; a block-aware runner with required output paths, per-trial export, and threshold-breach capture; a GPU analysis script producing per-level medians/CIs and gate verdicts; teardown immediately after measurement; report, charts, and README rewrite offline.

**Tech Stack:** AWS EKS 1.34 via eksctl 0.230.0, KubeRay v1.7.2, Ray 2.60.0 (p y311 / py311-gpu images), NVIDIA k8s-device-plugin v0.17.1, k6 v0.54.0, Python 3.11 in-cluster / 3.9 local, pytest 8.4.2, PyYAML 6.0.3.

**Spec:** `docs/superpowers/specs/2026-11-19-phase3-gpu-rerun-design.md` (user-approved 2026-11-19, 2-arm scope; high-throughput settings table in §6)

## Global Constraints

- Cluster name `ray-serve-phase3-rerun`; region `us-west-2`; control-plane VPC AZs `[us-west-2a, us-west-2b]`; **all four nodegroups pinned to `us-west-2a`**; kubeconfig `~/.kube/ray-serve-phase3-rerun`; kubectl `-c`-explicit `~/.local/bin/kubectl-1.34.13`.
- Nodegroups (all `desired=min=max=1`, all tagged `project: ray-serve-benchmark`): `head` (`c6i.2xlarge`, label `bench-role: head`), `gpu-w1` (`g6.xlarge`, labels `bench-role: gpu`, `bench-slot: gpu1`), `gpu-w2` (same, `bench-slot: gpu2`), `loadgen` (`c6i.2xlarge`, `bench-role: loadgen`).
- **Both GPU nodegroups pin `ami: ami-0bd8ae7fb660d28d7`** (`amazon-eks-node-al2023-x86_64-nvidia-1.34-v20261112`, us-west-2; resolution command documented in the infra README).
- Ray `2.60.0`; images `rayproject/ray:2.60.0-py311` (head) and `rayproject/ray:2.60.0-py311-gpu` (worker); KubeRay operator `v1.7.2` with `--set nodeSelector.bench-role=head`; NVIDIA device plugin manifest copied from tag `v0.17.1`.
- Ray env on head and worker: `RAY_SERVE_ENABLE_HA_PROXY=1`, `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`, `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0`. **Never** `RAY_IGNORE_VERSION_CHECK` (env var) or `ignore-version-check` (rayStartParam).
- **No `ray-haproxy` entry in any `runtime_env.pip`.** If the gate finds the binary missing from the -gpu image: FAIL, stop, escalate; adding the pip is a controller-level amendment.
- `max_ongoing_requests: 1000` in `serveConfigV2` **and** in the `@serve.deployment` decorator; `@serve.batch(max_batch_size=32, batch_wait_timeout_s=0.005, max_concurrent_batches=1)` pinned explicitly.
- `worker_only` resource contract: `rayStartParams.resources: '{"worker_only":1}'` on the worker group **and** `resources: {"worker_only": 1}` in `ray_actor_options`.
- Measured Services: `ray-ha-svc` port 8000 → **targetPort 8000** (worker pods only); `direct-gpu-embed-svc` 8000→8000.
- Runner display names/target keys exactly: `Direct Serving (GPU)`/`direct`, `Ray Serve (HAProxy :8000)`/`ray_ha`; block `gblock1`.
- Results only under `results/phase3-gpu-rerun-2026-11-19/`; runner raw-output args **required**; append dedupes on `(block, target_key, concurrency, trial_number)`.
- **Budget: $10 hard cap, inclusive of NAT charges.** **Teardown runs immediately after measurement, before analysis.** Cap-hit mid-measurement = abort, tear down, report partial trials.
- Never touch the pre-existing us-west-2 clusters or any us-east-1 resources; never write credentials into manifests.
- October Phase 3 artifacts (`results/phase3-gpu-embeddings-report.md`, `results/gpu_embed/`, `benchmarks/gpu-embed/*/k8s`, `benchmarks/gpu-embed/loadgen/`) are **not modified**, except `benchmarks/gpu-embed/ray/serve_embed.py` which takes exactly the two pinned §6 changes.
- Python 3.9-compatible source (local tests run on 3.9.21); pytest 8.4.2; PyYAML 6.0.3. Commit trailer: end every commit message with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.

## Review Focus

The failure modes the spec implies; each is pinned by the named test or gate.

1. **HAProxy silently not the measured ingress** (Service pointing at `:30000`, or HAProxy bypassed). Expected: traffic traverses HAProxy. Pinned by Task 2's `test_service_targets` (targetPort 8000) and Task 6's live HAProxy counter gate.
2. **Replica strands PENDING** (missing `worker_only` on either side). Pinned by Task 2 tests asserting the resource in both `rayStartParams` and `ray_actor_options`, and Task 6's 1-RUNNING-replica gate.
3. **No NVIDIA driver / invisible GPU** (wrong AMI, device plugin not registering). Pinned by Task 1's AMI pin test + device-plugin file test, and Task 6's CUDA acceptance (torch must see the L4 by name).
4. **Runner can overwrite the October Phase 3 artifacts** (`results/gpu_embed/`). Expected: bare invocation errors rather than defaulting into historical paths. Pinned by Task 3's CLI tests (argument-parse exit 2 without `--output-raw-json`/`--output-raw-csv`).
5. **A k6 threshold breach (exit 99) aborts and discards the block.** Pinned by Task 3's mocked-subprocess tests: exit 99 recorded as `threshold_breached: true`, incremental per-trial export keeps completed trials on abort, appends dedupe across concurrencies.

---

## File Structure

```
infra/phase3-gpu-rerun/
  eksctl-cluster.yaml              # 4 nodes, us-west-2a, pinned NVIDIA AMI
  nvidia-device-plugin.yaml        # v0.17.1 static manifest, nodeSelector bench-role=gpu
  README.md                        # create / AMI refresh / device plugin / delete
benchmarks/phase3-gpu-rerun/
  00-namespace.yaml
  01-direct-gpu.yaml               # Direct deployment + direct-gpu-embed-svc
  02-ray-ha.yaml                   # RayService + ray-ha-svc (targetPort 8000)
  03-loadgen-driver.yaml           # k6 driver Deployment on the loadgen node
  preflight.sh                     # mode: gpu (fail-closed)
  README.md
  loadgen/run_gpu_matrix.py        # hardened matrix runner
  loadgen/k6_embed.js               # copy of gpu-embed script; TEST_PROFILE defaults to concurrency
  make_charts.py                   # read dataviz skill BEFORE writing this
benchmarks/gpu-embed/ray/serve_embed.py   # two pinned decorator changes only
results/analyze_gpu.py             # block/level-aware analysis, required --data/--output
results/phase3-gpu-rerun-2026-11-19/      # evidence + raw data + summary (created live)
results/phase3-gpu-rerun-2026-11-19-report.md   # final report
docs/images/phase3_gpu_rerun_throughput_scaling.svg
docs/images/phase3_gpu_rerun_latency_tail.svg
tests/test_phase3_infra.py
tests/test_phase3_manifests.py
tests/test_gpu_runner.py
tests/test_phase3_analysis.py
```

---

### Task 1: Cluster config and device plugin

**Files:**
- Create: `infra/phase3-gpu-rerun/eksctl-cluster.yaml`
- Create: `infra/phase3-gpu-rerun/nvidia-device-plugin.yaml`
- Create: `infra/phase3-gpu-rerun/README.md`
- Create: `tests/test_phase3_infra.py`

**Interfaces:**
- Consumes: none.
- Produces: the eksctl config consumed by Task 5; the device-plugin manifest applied in Task 5.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_phase3_infra.py
import yaml
from pathlib import Path

INFRA = Path(__file__).parents[1] / "infra/phase3-gpu-rerun"

def load_config():
    return yaml.safe_load((INFRA / "eksctl-cluster.yaml").read_text())

def test_cluster_identity():
    c = load_config()
    assert c["metadata"]["name"] == "ray-serve-phase3-rerun"
    assert c["metadata"]["region"] == "us-west-2"
    assert c["metadata"]["version"] == "1.34"
    assert c["availabilityZones"] == ["us-west-2a", "us-west-2b"]

def test_nodegroups():
    c = load_config()
    groups = {g["name"]: g for g in c["managedNodeGroups"]}
    assert set(groups) == {"head", "gpu-w1", "gpu-w2", "loadgen"}
    for g in groups.values():
        assert g["desiredCapacity"] == 1 and g["minSize"] == 1 and g["maxSize"] == 1
        assert g["availabilityZones"] == ["us-west-2a"]
        assert g["tags"]["project"] == "ray-serve-benchmark"
    assert groups["head"]["instanceType"] == "c6i.2xlarge"
    assert groups["head"]["labels"]["bench-role"] == "head"
    assert groups["loadgen"]["instanceType"] == "c6i.2xlarge"
    assert groups["loadgen"]["labels"]["bench-role"] == "loadgen"
    for name in ("gpu-w1", "gpu-w2"):
        assert groups[name]["instanceType"] == "g6.xlarge"
        assert groups[name]["ami"] == "ami-0bd8ae7fb660d28d7"
        assert groups[name]["labels"]["bench-role"] == "gpu"
    assert groups["gpu-w1"]["labels"]["bench-slot"] == "gpu1"
    assert groups["gpu-w2"]["labels"]["bench-slot"] == "gpu2"

def test_device_plugin_manifest():
    d = yaml.safe_load((INFRA / "nvidia-device-plugin.yaml").read_text())
    assert d["kind"] == "DaemonSet"
    assert d["spec"]["template"]["spec"]["nodeSelector"]["bench-role"] == "gpu"
    images = [c["image"] for c in d["spec"]["template"]["spec"]["containers"]]
    assert len(images) == 1 and "v0.17" in images[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_phase3_infra.py -v`
Expected: FAIL with `FileNotFoundError`.

- [ ] **Step 3: Write the config, plugin manifest, and README**

`eksctl-cluster.yaml`: `apiVersion: eksctl.io/v1alpha5`, `kind: ClusterConfig`, metadata/version/AZs exactly as pinned above; `managedNodeGroups` exactly as the test asserts (GPU groups add `amiFamily: AmazonLinux2` or omit the family — but must carry the explicit `ami:`); everything else mirrors `infra/phase2-rerun/eksctl-cluster.yaml` (same `iam.withOIDC: true`, `vpc.cidr: 10.42.0.0/16`, `nat.gateway: Single`).

`nvidia-device-plugin.yaml`: download
`https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.17.1/deployments/static/nvidia-device-plugin.yml`
into the file, then add `nodeSelector: {bench-role: gpu}` to the DaemonSet pod spec. Nothing else is changed (keep the upstream image pin and tolerations).

`README.md`: cluster-create command (backgrounded, `--timeout 40m`), kubeconfig command, device-plugin apply (`kubectl apply -f infra/phase3-gpu-rerun/nvidia-device-plugin.yaml`), KubeRay install command, delete command, and the **AMI refresh procedure**:
`aws ec2 describe-images --region us-west-2 --owners amazon --filters "Name=name,Values=amazon-eks-node-al2023-x86_64-nvidia-1.34-v*" --query 'sort_by(Images,&CreationDate)[-1].[ImageId,Name]' --output text` (region-scoped; re-pin into the config on refresh). No credentials or account IDs in any file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_phase3_infra.py -v && python3 -m pytest tests/ -q`
Expected: new tests pass; full suite green (38 passing total: 28 existing + 3 new).

- [ ] **Step 5: Commit**

```bash
git add infra/phase3-gpu-rerun/ tests/test_phase3_infra.py
git commit -m "feat: add Phase 3 rerun cluster config and device plugin"
```

---

### Task 2: Apps, manifests, preflight gate, and pinning tests

**Files:**
- Modify: `benchmarks/gpu-embed/ray/serve_embed.py` (two decorator changes only)
- Create: `benchmarks/phase3-gpu-rerun/00-namespace.yaml`
- Create: `benchmarks/phase3-gpu-rerun/01-direct-gpu.yaml`
- Create: `benchmarks/phase3-gpu-rerun/02-ray-ha.yaml`
- Create: `benchmarks/phase3-gpu-rerun/03-loadgen-driver.yaml`
- Create: `benchmarks/phase3-gpu-rerun/preflight.sh`
- Create: `benchmarks/phase3-gpu-rerun/README.md`
- Create: `tests/test_phase3_manifests.py`

**Interfaces:**
- Consumes: Task 1 labels (`bench-role`, `bench-slot`); Phase 2's `benchmarks/phase2-cpu-rerun/preflight.sh` as the template for the gate script.
- Produces: Service names consumed by Task 3 — `direct-gpu-embed-svc`, `ray-ha-svc`; ConfigMap names `direct-gpu-embed-code`, `ray-gpu-embed-code`, `phase3-loadgen-scripts`, `gpu-embed-fixtures`; container names pinned for the gate's explicit `-c` execs: head `ray-head`, worker `ray-gpu-worker`, direct `server`, driver `runner`.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_phase3_manifests.py
import yaml
from pathlib import Path

ROOT = Path(__file__).parents[1] / "benchmarks/phase3-gpu-rerun"
APP = Path(__file__).parents[1] / "benchmarks/gpu-embed"

def docs(name):
    return [d for d in yaml.safe_load_all((ROOT / name).read_text()) if d]

def env(container):
    return {e["name"]: e.get("value") for e in container.get("env", [])}

def rayservice():
    return [d for d in docs("02-ray-ha.yaml") if d["kind"] == "RayService"][0]

def test_ray_version_images_and_flags():
    rs = rayservice()
    rc = rs["spec"]["rayClusterConfig"]
    assert rc["rayVersion"] == "2.60.0"
    head = rc["headGroupSpec"]["template"]["spec"]["containers"][0]
    worker = rc["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]
    assert head["image"] == "rayproject/ray:2.60.0-py311"
    assert worker["image"] == "rayproject/ray:2.60.0-py311-gpu"
    for c in (head, worker):
        assert env(c)["RAY_SERVE_ENABLE_HA_PROXY"] == "1"
        assert env(c)["RAY_SERVE_THROUGHPUT_OPTIMIZED"] == "1"
        assert env(c)["RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD"] == "0"
    for c in (head, worker):
        assert "RAY_IGNORE_VERSION_CHECK" not in env(c)

def test_serve_config_and_app_pins():
    rs = rayservice()
    app = yaml.safe_load(rs["spec"]["serveConfigV2"])["applications"][0]
    dep = app["deployments"][0]
    assert dep["max_ongoing_requests"] == 1000
    assert dep["num_replicas"] == 1
    assert dep["ray_actor_options"]["resources"] == {"worker_only": 1}
    assert dep["ray_actor_options"]["num_gpus"] == 1
    assert dep["ray_actor_options"]["num_cpus"] == 2
    text = (APP / "ray/serve_embed.py").read_text()
    assert "max_ongoing_requests=1000" in text
    assert "max_batch_size=32" in text
    assert "batch_wait_timeout_s=0.005" in text
    assert "max_concurrent_batches=1" in text
    assert "max_ongoing_requests=100" not in text.replace("max_ongoing_requests=1000", "")
    runtime_pip = app["runtime_env"]["pip"]
    assert not any("ray-haproxy" in p for p in runtime_pip)

def test_worker_resource_contract_and_no_version_override():
    rs = rayservice()
    wsp = rs["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]
    assert wsp["rayStartParams"]["resources"] == '{"worker_only":1}'
    assert wsp["rayStartParams"]["num-gpus"] == "1"
    assert wsp["rayStartParams"]["num-cpus"] == "2"
    assert "ignore-version-check" not in wsp["rayStartParams"]
    tmpl = wsp["template"]["spec"]
    assert tmpl["nodeSelector"]["bench-slot"] == "gpu2"
    assert tmpl["containers"][0]["resources"]["limits"]["nvidia.com/gpu"] == 1

def test_service_targets():
    expected = {"ray-ha-svc": 8000, "direct-gpu-embed-svc": 8000}
    found = {}
    for f in ROOT.glob("*.yaml"):
        for d in docs(f.name):
            if d["kind"] == "Service":
                found[d["metadata"]["name"]] = d["spec"]["ports"][0]["targetPort"]
    assert found == expected

def test_direct_pinned_to_gpu1_and_batch_env():
    d = [d for d in docs("01-direct-gpu.yaml") if d["kind"] == "Deployment"][0]
    c = d["spec"]["template"]["spec"]["containers"][0]
    assert c["image"] == "pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime"
    assert d["spec"]["template"]["spec"]["nodeSelector"]["bench-slot"] == "gpu1"
    e = env(c)
    assert e["MAX_BATCH_SIZE"] == "32"
    assert e["BATCH_WAIT_TIMEOUT_S"] == "0.005"
    assert c["resources"]["limits"]["nvidia.com/gpu"] == 1

def test_driver_manifest():
    d = [d for d in docs("03-loadgen-driver.yaml") if d["kind"] == "Deployment"][0]
    spec = d["spec"]["template"]["spec"]
    assert spec["nodeSelector"]["bench-role"] == "loadgen"
    assert spec["containers"][0]["name"] == "runner"
    assert spec["containers"][0]["image"] == "python:3.11-slim"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_phase3_manifests.py -v`
Expected: FAIL with `FileNotFoundError`.

- [ ] **Step 3: Pin the app changes**

In `benchmarks/gpu-embed/ray/serve_embed.py`: change the decorator to
`max_ongoing_requests=1000`, and pin the batch decorator to
`@serve.batch(max_batch_size=32, batch_wait_timeout_s=0.005, max_concurrent_batches=1)`. No other lines change (the `asyncio.to_thread` offload stays).

- [ ] **Step 4: Write the manifests**

Required values, per manifest (namespace `model-serving-benchmark` throughout):

| Manifest | Key settings |
|---|---|
| `00-namespace.yaml` | namespace `model-serving-benchmark` |
| `01-direct-gpu.yaml` | Deployment `direct-gpu-embed` (labels `app: direct-gpu-embed`, `bench-arm: direct`), 1 replica, `nodeSelector {bench-slot: gpu1}`; container `server`, image `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`; command pip-installs `transformers>=4.40.0,<4.49.0`, `sentence-transformers>=2.5.0,<3.5.0`, `accelerate`, `fastapi`, `uvicorn[standard]`, `orjson`, then `exec uvicorn app:app --host "::" --port 8000 --workers 1 --no-access-log`; `workingDir /app`; env `MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2`, `MAX_BATCH_SIZE=32`, `BATCH_WAIT_TIMEOUT_S=0.005`; readiness `/readiness :8000` (initialDelay 240, period 5, failureThreshold 60); resources 2000m/8 GiB req, 4000m/12 GiB lim, `nvidia.com/gpu: 1`; ConfigMap `direct-gpu-embed-code` mounted at `/app`. **In the same file**: Service `direct-gpu-embed-svc` 8000→8000, selector `app: direct-gpu-embed` |
| `02-ray-ha.yaml` | RayService `rayservice-gpu-embed` per the Global Constraints block: serveConfigV2 (EveryNode, `::`:8000, `serve_embed:app`, runtime_env pip `torch==2.2.0`, transformers, sentence-transformers, accelerate, orjson + the three env vars; deployment `EmbeddingDeployment` num_replicas 1 / max_ongoing 1000 / ray_actor_options `{num_gpus: 1, num_cpus: 2, resources: {"worker_only": 1}}`); head group (`num-cpus: "0"`, `bench-role=head`, container `ray-head`, ports gcs / dashboard / serve / metrics); worker group per the tests (container `ray-gpu-worker`, pod labels `ray.io/node-type: worker` + `bench-arm: ray`, ports 8000 `http-ha` + 8080 `metrics`, readiness `127.0.0.1:8000/readiness` initialDelay 300 failureThreshold 60, resources, app-code volume). **In the same file**: Service `ray-ha-svc` 8000→**8000**, selector `ray.io/node-type: worker, bench-arm: ray`. **No 8000→30000 Service exists in this file.** |
| `03-loadgen-driver.yaml` | Deployment `loadgen-driver` (labels `app: loadgen-driver`), 1 replica, `bench-role=loadgen`, container `runner` (`python:3.11-slim`), installs `curl ca-certificates tar` + k6 v0.54.0, `workingDir /workspace/loadgen`, command `sleep infinity`; ConfigMap mounts: `phase3-loadgen-scripts` → `/workspace/loadtest/run_gpu_matrix.py` and `/workspace/loadtest/k6_embed.js` (subPath), `gpu-embed-fixtures` → `/workspace/loadgen` (= the script's `../fixtures` relative open) / `queries.jsonl` subPath; emptyDir `/results`; resources 1000m/2 GiB req, 2000m/4 GiB lim |
| `preflight.sh` | Mode set is just `gpu`. Adapt `benchmarks/phase2-cpu-rferun/preflight.sh` (typo aside, the Phase 2 script) keeping its fail-closed conventions (`set -euo pipefail`, explicit `-c` execs, `/proc/net/tcp` listening scan, wheel-path `haproxy` resolution, digits-only HAProxy stats). Checks: 4 nodes with expected roles/types/zone (`EXPECTED_ZONE` default `us-west-2a`); CUDA acceptance in BOTH serving pods via `python3 -c` running `import torch; assert torch.cuda.is_available(); print(torch.cuda.get_device_name(0))` — expecting `NVIDIA L4`; Ray `--version` → 2.60.0 (head); HAProxy binary present on head + worker; **2 HAProxyManager actors** (head + the one replica-hosting worker), 0 Python-proxy fallbacks; listening: direct pod `:8000`, worker `:8000` and `:300xx`; endpoints `direct-gpu-embed-svc` 1, `ray-ha-svc` 1; Serve status: `EmbeddingDeployment` 1 RUNNING; serveConfigV2 must contain `max_ongoing_requests: 1000`; live env capture from head + worker with `RAY_SERVE_ENABLE_HA_PROXY=1` and `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`; 10 POST `/embed` per Service returning 200 with a 384-dim embedding; HAProxy stats counters increase (worker, digits-only, ≥10 frontend delta pattern from Phase 2). **HAProxy-binary missing → die** (stops; escalated). Exits 0 with a `[preflight:gpu] PASS` banner. Writes evidence to stdout. |
| `README.md` | Block walkthrough: deploy, gate, measure, teardown-pointer; explains why teardown precedes analysis |

- [ ] **Step 5: Run tests and lint**

Run: `python3 -m pytest tests/test_phase3_manifests.py -v && bash -n benchmarks/phase3-gpu-rerun/preflight.sh && python3 -m pytest tests/ -q`
Expected: new tests pass; `bash -n` clean; full suite green (44 passing total).

- [ ] **Step 6: Commit**

```bash
git add benchmarks/gpu-embed/ray/serve_embed.py benchmarks/phase3-gpu-rerun/ tests/test_phase3_manifests.py
git commit -m "feat: add Phase 3 rerun manifests, gate, and app pins"
```

---

### Task 3: Hardened GPU matrix runner

**Files:**
- Create: `benchmarks/phase3-gpu-rerun/loadgen/run_gpu_matrix.py`
- Create: `benchmarks/phase3-gpu-rerun/loadgen/k6_embed.js`
- Create: `tests/test_gpu_runner.py`

**Interfaces:**
- Consumes: Service names from Task 2 (`http://direct-gpu-embed-svc:8000`, `http://ray-ha-svc:8000`).
- Produces (Task 4 depends on these exactly):
  - `TARGETS`, `BLOCKS`, `resolve_targets(block, override=None)`, `parse_k6_json(...)`-style helpers (Group/Python-3.9 compatible)
  - Trial record keys: `block`, `target_key`, `architecture`, `concurrency`, `trial_number`, `seed`, `started_at_utc`, `total_requests`, `successful_requests`, `failed_requests`, `threshold_breached` (bool), `throughput_rps`, `p50_ms`, `p90_ms`, `p95_ms`, `p99_ms`, `max_ms`, `server_model_p50_ms`, `server_total_p50_ms`, `network_transit_p50_ms`
  - Raw JSON shape: `{"benchmark_metadata": {...}, "trials": [...]}` with metadata computing: `total_trials`, `concurrency_levels`, `trials_per_level`, `warmup_duration`, `seed`, `max_batch_size: 32`, `batch_wait_timeout_s: 0.005`, `max_concurrent_batches: 1`, `model: sentence-transformers/all-MiniLM-L6-v2`, `co_location: f"Single Availability Zone ({az}), zero cross-AZ transit"`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_gpu_runner.py
import json, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "benchmarks/phase3-gpu-rerun/loadgen"))
import run_gpu_matrix as rg

def test_targets_and_blocks():
    assert rg.resolve_targets("gblock1") == ["direct", "ray_ha"]
    assert rg.TARGETS["direct"]["name"] == "Direct Serving (GPU)"
    assert rg.TARGETS["ray_ha"]["name"] == "Ray Serve (HAProxy :8000)"
    assert rg.TARGETS["ray_ha"]["url"] == "http://ray-ha-svc:8000"
    try:
        rg.resolve_targets("nope")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")

def test_exit_99_recorded_other_exits_abort(monkeypatch, tmp_path):
    calls = iter([0, 0, 99, 1])
    def fake_run(cmd, **kw):
        return subprocess.SimpleNamespace(returncode=next(calls), stdout="")
    monkeypatch.setattr(rg.subprocess, "run", fake_run)
    rg.main_with_args(["--concurrencies", "4", "--trials", "2", "--duration", "30s",
                       "--warmup-duration", "1s", "--seed", "7", "--az", "us-west-2a",
                       "--output-dir", str(tmp_path),
                       "--output-raw-json", str(tmp_path / "raw.json"),
                       "--output-raw-csv", str(tmp_path / "raw.csv")])
    d = json.loads((tmp_path / "raw.json").read_text())
    flagged = [t for t in d["trials"] if t["threshold_breached"]]
    assert len(d["trials"]) == 2 and len(flagged) == 1

def test_incremental_export_survives_abort(monkeypatch, tmp_path):
    codes = iter([0, 0, 1])  # trial1 ok, trial2 ok, trial3 aborts
    monkeypatch.setattr(rg.subprocess, "run",
                        lambda cmd, **kw: subprocess.SimpleNamespace(returncode=next(codes), stdout=""))
    try:
        rg.main_with_args(["--concurrencies", "4", "--trials", "3", "--duration", "30s",
                           "--skip-warmup", "--seed", "7", "--az", "us-west-2a",
                           "--output-dir", str(tmp_path),
                           "--output-raw-json", str(tmp_path / "raw.json"),
                           "--output-raw-csv", str(tmp_path / "raw.csv")])
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected abort")
    assert len(json.loads((tmp_path / "raw.json").read_text())["trials"]) == 2

def test_append_dedupes_on_concurrency(tmp_path):
    base = {"block": "gblock1", "target_key": "ray_ha", "architecture": "Ray Serve (HAProxy :8000)",
            "concurrency": 8, "trial_number": 1, "seed": 7, "started_at_utc": "2026-11-19T00:00:00Z",
            "total_requests": 1, "successful_requests": 1, "failed_requests": 0,
            "threshold_breached": False, "throughput_rps": 50.0, "p50_ms": 1.0,
            "p90_ms": 1.0, "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0,
            "server_model_p50_ms": 0.5, "server_total_p50_ms": 0.6, "network_transit_p50_ms": 0.4}
    rg.export_raw_trials([base], tmp_path / "raw.json", tmp_path / "raw.csv", append=False)
    rg.export_raw_trials([base], tmp_path / "raw.json", tmp_path / "raw.csv", append=True)
    other = dict(base, concurrency=16)
    rg.export_raw_trials([other], tmp_path / "raw.json", tmp_path / "raw.csv", append=True)
    d = json.loads((tmp_path / "raw.json").read_text())
    assert len(d["trials"]) == 2
    assert d["benchmark_metadata"]["co_location"].startswith("Single Availability Zone (us-west-2a)")

def test_cli_requires_output_paths(tmp_path, capsys):
    for extra in ([], ["--output-raw-json", str(tmp_path / "a.json")],
                  ["--output-raw-csv", str(tmp_path / "a.csv")]):
        try:
            rg.main_with_args(["--concurrencies", "4", "--trials", "1", "--skip-warmup"] + extra)
        except SystemExit as e:
            assert e.code == 2
        else:
            raise AssertionError("expected SystemExit 2")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_gpu_runner.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the runner**

Copy `benchmarks/gpu-embed/loadgen/k6_embed.js` → `benchmarks/phase3-gpu-rerun/loadgen/k6_embed.js` with exactly one change: the `TEST_PROFILE` default becomes `'concurrency'` (thresholds untouched).

Implement `run_gpu_matrix.py` on the Phase-2 `loadtest/run_matrix.py` skeleton, adapted:
- `TARGETS`/`BLOCKS` as pinned; `resolve_targets` identical semantics to Phase 2.
- CLI: `--block` (required, `choices=["gblock1"]`), `--concurrencies 1,4,8,16,32,64`, `--trials 5`, `--duration 30s`, `--warmup-duration 15s`, `--skip-warmup`, `--seed 42`, `--az us-west-2a`, `--k6-script` (default beside the runner), `--output-dir` (default `results/gpu_matrix`), **`--output-raw-json` and `--output-raw-csv` required**; `main_with_args(argv)` helper so tests drive it without argparse's `sys.argv`.
- k6 invocation per trial: `k6 run -e TARGET_HOST=<url> -e EMBED_PATH=/embed -e CONCURRENCY=<c> -e STAGE_DURATION=<duration> -e TEST_PROFILE=concurrency --summary-trend-stats min,avg,med,max,p(90),p(95),p(99) --summary-export <trial.json> <script>`. **Exit 99 → record the trial with `threshold_breached: true`; any other non-zero exit raises** (abort, completed trials already exported).
- Each recorded trial gets `seed` (this run's `--seed`), `started_at_utc` (ISO-8601 UTC), `concurrency`; warmups run k6 for `--warmup-duration` at the same CONCURRENCY and are never recorded.
- **Scheduling:** all (arm × level) warmup runs first (shuffled with the seed), then the full 60-trial queue shuffled (seeded) — warmups for a pair always precede its trials because all warmups precede all trials. Print and log the random order; the log is committed as evidence.
- **Incremental export:** after each recorded trial, call `export_raw_trials` (append + dedupe on `(block, target_key, concurrency, trial_number)`, newer run wins), so an abort never loses completed trials.
- k6 metric extraction mirrors Phase 2 (plus `http_reqs.rate` for `throughput_rps`, `successful_embeddings`/`failed_embeddings` counters) — the metrics names are in `k6_embed.js`.
- Python 3.9-compatible; no defaults that can write into `results/gpu_embed/`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_gpu_runner.py -v && python3 -m pytest tests/ -q`
Expected: 5 new tests pass; full suite green (49 passing total).

- [ ] **Step 5: Commit**

```bash
git add benchmarks/phase3-gpu-rerun/loadgen/ tests/test_gpu_runner.py
git commit -m "feat: add hardened Phase 3 GPU matrix runner"
```

---

### Task 4: GPU analysis script

**Files:**
- Create: `results/analyze_gpu.py`
- Create: `tests/test_phase3_analysis.py`

**Interfaces:**
- Consumes: the Task 3 trial-record schema verbatim.
- Produces: `generate_gpu_tables(meta, grouped) -> str` (markdown) with: header computed from the trials; per (arm × level) medians [IQR] / mean ± t-CI; delta table (`ray_ha` − `direct` per level, bootstrap 95% CIs); gate verdicts; flagging of any `threshold_breached` trial. Bootstrap helpers local to this file (self-contained module, same algorithm as Phase 2's analyzer; the duplication is deliberate so the two analyzers can evolve independently — note it in the module docstring).

- [ ] **Step 1: Write failing tests**

```python
# tests/test_phase3_analysis.py
import sys
from pathlib import Path
ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "results"))
import analyze_gpu as ag

def trial(key, arch, conc, p50, p95, p99, tp):
    return {"block": "gblock1", "target_key": key, "architecture": arch,
            "concurrency": conc, "trial_number": 1, "seed": 7,
            "started_at_utc": "2026-11-19T00:00:00Z",
            "total_requests": 1500, "successful_requests": 1500,
            "failed_requests": 0, "threshold_breached": False,
            "throughput_rps": tp, "p50_ms": p50, "p90_ms": p95,
            "p95_ms": p95, "p99_ms": p99, "max_ms": p99,
            "server_model_p50_ms": 5.0, "server_total_p50_ms": 6.0,
            "network_transit_p50_ms": 1.0}

def test_tables_and_gate_verdicts():
    trials = []
    for key, arch in (("direct", "Direct Serving (GPU)"), ("ray_ha", "Ray Serve (HAProxy :8000)")):
        for conc in (1, 4, 8, 16, 32, 64):
            for i in range(5):
                p50, p95, p99, tp = (10.0 + i * 0.01, 11.0, 12.0, 90.0 + conc) if key == "direct" \
                    else (12.0, 13.5, 14.0, 80.0 + conc)
                trials.append(trial(key, arch, conc, p50, p95, p99, tp))
    grouped = {}
    for t in trials:
        grouped.setdefault((t["target_key"], t["concurrency"]), []).append(t)
    md = ag.generate_gpu_tables({}, grouped)
    assert "Direct Serving (GPU)" in md and "Ray Serve (HAProxy :8000)" in md
    assert "+2.000 ms" in md            # ray_ha − direct p50 delta at every level
    assert "FAIL" in md                 # goodput 80+x vs 90+x < 95% at least somewhere
    assert "Batch Scaling" in md
    assert "C=64" in md                 # saturation gate section
    assert ag.SUPPORT_ARMS == ["direct", "ray_ha"]

def test_gate1_either_passes():
    # 2.9 ms absolute delta at 100% relative — absolute passes, relative fails
    rows = {("direct", 1): [trial("direct", "Direct Serving (GPU)", 1, 1.0, 1.0, 1.1, 90.0)],
            ("ray_ha", 1): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 1, 3.9, 3.9, 4.0, 90.0)]}
    verdict = ag.gate_low_load(rows)
    assert verdict == "PASS (absolute)"

def test_threshold_flag_rendered():
    rows = {("ray_ha", 32): [dict(trial("ray_ha", "Ray Serve (HAProxy :8000)", 32, 15.0, 17.0, 18.0, 1400.0),
                                   threshold_breached=True)]}
    md = ag.generate_gpu_tables({}, rows)
    assert "threshold" in md.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_phase3_analysis.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `results/analyze_gpu.py`**

`python3 results/analyze_gpu.py --data <raw.json> --output <md>` — **both args required** (argparse errors otherwise, same pattern as the Phase 2 analyzer). `load_raw_data(path)` groups trials on `(target_key, concurrency)`. `generate_gpu_tables(meta, grouped)` renders:

1. **Header** computed from trials (total trials, levels, per-arm counts, model, batching 32/0.005/1, AZ).
2. **Per-arm × level table**: median [IQR] and mean ± t-CI (df = n−1 per group) for p50/p95/p99; goodput mean; failure count; any `threshold_breached` flagged on the row.
3. **Delta table**: `ray_ha` − `direct` per level for p50/p95/p99 with seeded bootstrap 95% CIs of the median difference (10k resamples, seed 42, Phase 2 algorithm).
4. **Gates**, each PASS/FAIL with the numbers:
   - `gate_low_load(rows)`: at C=1 and C=4, Δp95 ≤ 3.0 ms **or** Δp95 ≤ 10% — returns the verdict; label output "PASS (absolute)" / "PASS (relative)" / "FAIL".
   - **Goodput gate**: per level, `ray_ha` mean throughput ≥ 95% of `direct` — verdict per level plus overall.
   - **Batch scaling gate** (ray arm): throughput(C=32) ≥ 3 × throughput(C=1) **and** p95(C=32) ≤ 20 ms.
   - **C=64 saturation gate**: `ray_ha` p99(C=64) ≤ `direct` p99(C=64) + 3.0 ms; both p99s printed regardless.
- Levels run per-arm; if an arm is missing a level, render a `missing` note at that cell and skip its gates (never KeyErrors).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_phase3_analysis.py -v && python3 -m pytest tests/ -q`
Expected: 3 new tests pass; full suite green (52 passing total).

- [ ] **Step 5: Commit**

```bash
git add results/analyze_gpu.py tests/test_phase3_analysis.py
git commit -m "feat: add Phase 3 GPU rerun analysis with gate verdicts"
```

---

### Task 5: Create the cluster and install KubeRay

**Files:**
- Create: `results/phase3-gpu-rerun-2026-11-19/preflight/` (evidence)
- No source changes.

**Interfaces:**
- Consumes: Task 1 config + device-plugin manifest.
- Produces: a ready cluster for Tasks 6–8. The AWS clock starts here.

- [ ] **Step 1: Preconditions**

```bash
export AWS_PROFILE=<aws-profile>
export KUBECONFIG=~/.kube/ray-serve-phase3-rerun
K=~/.local/bin/kubectl-1.34.13
$K version --client   # expect v1.34.13
mkdir -p results/phase3-gpu-rerun-2026-11-19/preflight
aws eks list-clusters --region us-west-2 --output json > results/phase3-gpu-rerun-2026-11-19/preflight/us-west-2-clusters-before.json
```
Expected: kubectl client present; cluster absent from the list.

- [ ] **Step 2: Create the cluster (backgrounded), kubeconfig, device plugin, KubeRay**

```bash
eksctl create cluster -f infra/phase3-gpu-rerun/eksctl-cluster.yaml --timeout 40m 2>&1 | tee results/phase3-gpu-rerun-2026-11-19/preflight/cluster-create.log
aws eks update-kubeconfig --name ray-serve-phase3-rerun --region us-west-2 --profile "$AWS_PROFILE" --kubeconfig "$KUBECONFIG"
$K apply -f infra/phase3-gpu-rerun/nvidia-device-plugin.yaml
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ >/dev/null; helm repo update >/dev/null
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.2 --namespace kuberay --create-namespace --set nodeSelector.bench-role=head
```
Expected: cluster ready in under 40 min (run backgrounded and wait); driver plugin pods Running on the two GPU nodes; operator Running on the head node.

- [ ] **Step 3: Record topology evidence**

```bash
$K get nodes -o wide -L bench-role,bench-slot,topology.kubernetes.io/zone,node.kubernetes.io/instance-type \
  | tee results/phase3-gpu-rerun-2026-11-19/preflight/nodes.txt
$K get ds -n kube-system -o wide | tee results/phase3-gpu-rerun-2026-11-19/preflight/device-plugin.txt
$K -n kuberay get deploy kuberay-operator -o jsonpath='{.spec.template.spec.containers[0].image}' | tee results/phase3-gpu-rerun-2026-11-19/preflight/kuberay-image.txt
```
Expected: 4 nodes (2 × `g6.xlarge`, 2 × `c6i.2xlarge`, all `us-west-2a`, labels per the config); device-plugin pods only on GPU nodes; operator image `quay.io/kuberay/operator:v1.7.2`.

- [ ] **Step 4: Commit evidence**

```bash
git add results/phase3-gpu-rerun-2026-11-19/preflight/
git commit -m "chore: record Phase 3 cluster creation evidence"
```

---

### Task 6: Deploy both arms and pass the GPU gate

**Files:**
- Create: evidence under `results/phase3-gpu-rerun-2026-11-19/preflight/`.
- No source changes unless the gate fails.

**Interfaces:**
- Consumes: Task 2 manifests + gate script; Task 5 cluster.
- Produces: a passed `[preflight:gpu]` gate for Task 7.

- [ ] **Step 1: ConfigMaps + deployment**

```bash
$K apply -f benchmarks/phase3-gpu-rerun/00-namespace.yaml
$K create configmap direct-gpu-embed-code --from-file=app.py=benchmarks/gpu-embed/direct/app.py -n model-serving-benchmark
$K create configmap ray-gpu-embed-code --from-file=serve_embed.py=benchmarks/gpu-embed/ray/serve_embed.py -n model-serving-benchmark
$K apply -f benchmarks/phase3-gpu-rerun/01-direct-gpu.yaml
$K apply -f benchmarks/phase3-gpu-rerun/02-ray-ha.yaml
```
Wait for readiness — **be patient**: image pull + pip + Hugging Face model download can take 10–30 min per pod. Re-run waits while `PROVISIONING`/not-Ready (up to 40 min total each) before concluding failure; identify a specific error (`image pull`, `node selector mismatch`, replica PENDING) before any delete/recreate.

- [ ] **Step 2: Run the gate**

```bash
KUBECTL=~/.local/bin/kubectl-1.34.13 bash benchmarks/phase3-gpu-rerun/preflight.sh gpu \
  2>&1 | tee results/phase3-gpu-rerun-2026-11-19/preflight/gpu-preflight.log
```
Expected: exit 0 with `[preflight:gpu] PASS` — CUDA visible (L4 by name) in both serving pods, 2 HAProxyManager actors, `:8000` direct, `:8000`+`:300xx` worker, 1-endpoint Services, 1 RUNNING replica, `max_ongoing_requests: 1000`, live env flags, 10/10 `/embed` per Service, HAProxy counters moving.

- [ ] **Step 3: On failure — stop; no load.** One 30 s retry only. Reruns of the manifest or cluster changes are controller-level decisions; report BLOCKED with the log tail and the exact dying check. **The cluster stays up** (Task 5 cost rule) while reporting.

- [ ] **Step 4: Commit evidence**

```bash
git add results/phase3-gpu-rerun-2026-11-19/preflight/
git commit -m "chore: record Phase 3 GPU preflight evidence"
```

---

### Task 7: Measure gblock1

**Files:**
- Create: `results/phase3-gpu-rerun-2026-11-19/gblock1/`, `raw.json`/`raw.csv`, `run-logs/`

**Interfaces:**
- Consumes: Task 3 runner + driver manifest (Task 2), the Task 6-passed arms.
- Produces: 60 recorded trials (30 per arm) for Tasks 4/8/9 consumers, and the raw JSON in the repo.

- [ ] **Step 1: Driver + ConfigMaps**

```bash
$K create configmap phase3-loadgen-scripts -n model-serving-benchmark \
  --from-file=run_gpu_matrix.py=benchmarks/phase3-gpu-rerun/loadgen/run_gpu_matrix.py \
  --from-file=k6_embed.js=benchmarks/phase3-gpu-rerun/loadgen/k6_embed.js
$K create configmap gpu-embed-fixtures -n model-serving-benchmark \
  --from-file=queries.jsonl=benchmarks/gpu-embed/fixtures/queries.jsonl
$K apply -f benchmarks/phase3-gpu-rerun/03-loadgen-driver.yaml
$K wait --for=condition=available deployment/loadgen-driver -n model-serving-benchmark --timeout=300s
```
Verify in-pod: `k6 --version` (v0.54.0) and the mounted fixture file exist (k6 downloads at startup, allow a few minutes).

- [ ] **Step 2: Run the block (backgrounded)**

```bash
$K exec deploy/loadgen-driver -c runner -n model-serving-benchmark -- \
  python3 /workspace/loadtest/run_gpu_matrix.py \
    --block gblock1 --concurrencies 1,4,8,16,32,64 --trials 5 \
    --duration 30s --warmup-duration 15s --seed 42 --az us-west-2a \
    --output-dir /results/gblock1 \
    --output-raw-json /results/phase3-raw.json \
    --output-raw-csv /results/phase3-raw.csv \
    --append \
  2>&1 | tee results/phase3-gpu-rerun-2026-11-19/run-logs/gblock1-run.log
```
Expected: 12 warmups (unrecorded) + 60 recorded trials ≈ 45–55 min wall time; background the exec and wait. Any per-trial failure aborts (runner semantics); a k6 exit 99 is recorded, not fatal.

- [ ] **Step 3: Copy, verify, commit**

```bash
POD=$($K get pod -n model-serving-benchmark -l app=loadgen-driver -o jsonpath='{.items[0].metadata.name}')
$K cp "$POD:/results/." results/phase3-gpu-rerun-2026-11-19/ -n model-serving-benchmark
python3 - <<'PY'
import json, collections
d = json.load(open('results/phase3-gpu-rerun-2026-11-19/phase3-raw.json'))
t = d['trials']
assert len(t) == 60, len(t)
assert all(x['block'] == 'gblock1' for x in t)
assert all(x['concurrency'] in (1, 4, 8, 16, 32, 64) for x in t)
counts = collections.Counter((x['target_key'], x['concurrency']) for x in t)
assert set(counts.values()) == {5}
assert sum(x['failed_requests'] for x in t) == 0
print('60 trials, 5 per (arm, level), 0 hard failures;',
      sum(x['threshold_breached'] for x in t), 'threshold-flagged')
PY
git add results/phase3-gpu-rerun-2026-11-19/
git commit -m "data: add Phase 3 GPU rerun gblock1 trials"
```

---

### Task 8: Teardown (immediately after measurement)

**Files:**
- Create: `results/phase3-gpu-rerun-2026-11-19/preflight/teardown.log`, `us-west-2-clusters-after.json`

This task runs **before** analysis (design §8 step 3) and gates Task 9 — no analysis while billable.

- [ ] **Step 1: Delete the cluster (backgrounded)**

```bash
export AWS_PROFILE=<aws-profile>
eksctl delete cluster --name ray-serve-phase3-rerun --region us-west-2 --wait 2>&1 \
  | tee results/phase3-gpu-rerun-2026-11-19/preflight/teardown.log
```
Expected: all cluster resources deleted.

- [ ] **Step 2: Leak sweep + after-list, appended to the log**

```bash
aws eks list-clusters --region us-west-2 --output json > results/phase3-gpu-rerun-2026-11-19/preflight/us-west-2-clusters-after.json
aws ec2 describe-instances --region us-west-2 --filter Name=tag:project,Values=ray-serve-benchmark \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,InstanceType]' --output text
aws ec2 describe-nat-gateways --region us-west-2 --filter Name=tag:project,Values=ray-serve-benchmark \
  --query 'NatGateways[].[NatGatewayId,State]' --output text
aws cloudformation list-stacks --region us-west-2 --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE DELETE_IN_PROGRESS DELETE_FAILED ROLLBACK_COMPLETE \
  --query 'StackSummaries[?starts_with(StackName,`eksctl-ray-serve-phase3-rerun`)].StackName' --output text
aws ec2 describe-volumes --region us-west-2 --filters Name=tag:project,Values=ray-serve-benchmark --query 'Volumes[].VolumeId' --output text
rm ~/.kube/ray-serve-phase3-rerun
```
Expected: instances/volumes lists empty; NAT shows `deleted` (or is absent); only `DELETE_COMPLETE` stacks remain; after-cluster list matches the before list (set-equality check, appending it and the raw command output to the log).

- [ ] **Step 3: Commit evidence.** Same pattern as Task 5. `git add results/phase3-gpu-rerun-2026-11-19/preflight/ && git commit -m "chore: record Phase 3 cluster teardown evidence"`.

---

### Task 9: Analyze, chart, report, README (cluster gone)

**Files:**
- Create: `results/phase3-gpu-rerun-2026-11-19/summary.md` (generated), `results/phase3-gpu-rerun-2026-11-19-report.md`, `docs/images/phase3_gpu_rerun_throughput_scaling.svg`, `docs/images/phase3_gpu_rerun_latency_tail.svg`
- Create: `benchmarks/phase3-gpu-rerun/make_charts.py`
- Modify: `README.md` (Phase 3 section only)

**Interfaces:**
- Consumes: Task 4 analyzer; Task 7 raw data (`results/phase3-gpu-rerun-2026-11-19/phase3-raw.json`); Task 8's teardown evidence for report citations.

- [ ] **Step 1: Generate the summary**

```bash
python3 results/analyze_gpu.py \
  --data results/phase3-gpu-rerun-2026-11-19/phase3-raw.json \
  --output results/phase3-gpu-rerun-2026-11-19/summary.md
```

- [ ] **Step 2: Charts — read `dataviz` first**

Invoke the `dataviz` skill (Skill tool) **before writing any chart code**, then write `benchmarks/phase3-gpu-rerun/make_charts.py` (`--data` required; paths per the Global Constraints) generating the two SVGs: throughput vs level and p95/p99 vs level, each with per-trial scatter points and 95% CI bands, correct arm names. No Python version issues (3.9; stdlib + any plotting code consistent with the skill's guidance).

- [ ] **Step 3: Write `results/phase3-gpu-rerun-2026-11-19-report.md`**

Must contain (verification strings): `Direct Serving (GPU)`, `Ray Serve (HAProxy :8000)`, `C=64`, `Batch Scaling`, `Goodput`, `Low-Load Latency`, `Saturation`, and cite the preflight evidence (CUDA/L4, HAProxy counters, live env flags) and the teardown log. Structure: stack + topology + gate proof (pointers into `preflight/`); per-arm × level table (quote the summary, don't recompute); deltas with bootstrap CIs; the four gate verdicts with numbers; charts embeds; disclosures per design §7 (per-trial counts, single-model/single-GPU scope, untuned 32/5 ms batching + serial `max_concurrent_batches: 1`, `max_ongoing_requests: 1000`, Direct's asyncio baseline frame, event-loop fixes both sides since the October snapshot, node non-isolation beyond types, seeds 42 + run-order pointer to `run-logs/gblock1-run.log`); one provenance line: the October snapshot (`results/phase3-gpu-embeddings-report.md`, `results/gpu_embed/`) is retained unchanged for provenance. Report any `threshold_breached` trials explicitly.

- [ ] **Step 4: Rewrite the README Phase 3 section**

Replace the section body with the rerun's headline numbers (low-load latency, goodput, batch scaling, C=64 verdicts) and links to the new report/summary/charts; update the TL;DR Phase 3 bullet, version table's Ray/KubeRay/K8s entries if not already current, and the roadmap Phase 3 checkbox wording ("multi-trial matrix measured; saturation gate resolved" if the gates pass). Keep Phase 2 content untouched. One provenance line for the October snapshot.

- [ ] **Step 5: Verify and commit**

```bash
python3 - <<'PY'
from pathlib import Path
r = Path('results/phase3-gpu-rerun-2026-11-19-report.md').read_text()
for s in ('Direct Serving (GPU)', 'Ray Serve (HAProxy :8000)', 'C=64', 'Batch Scaling',
          'Goodput', 'Low-Load Latency', 'Saturation', 'threshold'):
    assert s in r, s
print('report sections present')
PY
git diff --stat HEAD~0 -- results/phase3-gpu-embeddings-report.md results/gpu_embed/ | grep -q . && { echo "OCTOBER ARTIFACTS TOUCHED"; exit 1; } || true
python3 -m pytest tests/ -q
git add results/phase3-gpu-rerun-2026-11-19/ results/phase3-gpu-rerun-2026-11-19-report.md \
        docs/images/phase3_gpu_rerun_throughput_scaling.svg docs/images/phase3_gpu_rerun_latency_tail.svg \
        benchmarks/phase3-gpu-rerun/make_charts.py README.md
git commit -m "docs: add Phase 3 GPU rerun results, charts, and report"
```

---

## Self-Review

- **Spec coverage:** preflight-gated deployment (T6), HAProxy measured path (T2 test + T6 gate), high-throughput settings incl. `max_ongoing_requests: 1000` and `max_concurrent_batches: 1` (T2), worker_only contract (T2), pinned NVIDIA AMI + device plugin (T1, T5), CUDA acceptance (T6), runner hardening (T3), analyzer + four gates (T4, T9), teardown-before-analysis (T8 before T9), budget/dislosure/report (T9). Out-of-scope items (arms/sweeps) confirmed absent from tasks.
- **Step scan:** every step is one action with one checkable result; infra steps carry exact commands and expected output; code steps name tests and signatures, not bodies (the runner's scheduling rule and the analyzer's gate expressions are the two spelled-out algorithms the tests don't determine).
- **Type consistency:** trial-record keys identical in T3 Interfaces and T4 consumer; Service names/Target URLs identical in T2 manifest tests and T3 `TARGETS`; container names (`ray-head`, `ray-gpu-worker`, `server`, `runner`) consistent across T2 manifests and the T6 gate; block id `gblock1` consistent T2 runner/T3/T7/T9.
- **Review Focus:** each of the five lines has its pinning test or gate (T1, T2, T3, T6).
- **Proportion:** same shape as the Phase 2 plan minus the fix-loop scaffolding; code blocks are interfaces/values only.
