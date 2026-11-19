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
    # torch 2.2.0 was compiled against numpy 1.x; unpinned pip resolves
    # numpy 2.x (live-observed 2.1.3 in the Serve runtime_env venv) and every
    # sentence-transformers encode then dies with "RuntimeError: Numpy is not
    # available", failing every /embed with HTTP 500. Direct arm is safe
    # because the pytorch:2.2.0 image ships numpy 1.26.3.
    assert "numpy<2" in runtime_pip

def test_worker_resource_contract_and_no_version_override():
    rs = rayservice()
    wsp = rs["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]
    # KubeRay v1.7.2 interpolates rayStartParams verbatim into the pod's
    # `bash -c "ray start ..."` command line. A value containing bare double
    # quotes (e.g. '{"worker_only":1}') loses them to shell parsing and ray
    # PANIC's on `--resources={worker_only:1}` (invalid JSON) at startup, so
    # the worker crashloops. The single quotes MUST ship inside the value so
    # bash passes the JSON through intact. Pinned after live-cluster proof.
    assert wsp["rayStartParams"]["resources"] == '\'{"worker_only":1}\''
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
    # The kubelet readiness probe and the ClusterIP Service path are IPv4.
    # With the unpinned uvicorn[standard] a `--host "::"` bind ends up
    # IPv6-only (live-verified: ::1 connects, 127.0.0.1 refused while
    # bindv6only=0), so the pod can never go Ready and the Service never
    # gets endpoints. Bind IPv4, like the passing Phase 2 variant.
    command_text = "\n".join(str(part) for part in c["command"])
    assert '--host "0.0.0.0"' in command_text
    assert '--host "::"' not in command_text

def test_driver_manifest():
    d = [d for d in docs("03-loadgen-driver.yaml") if d["kind"] == "Deployment"][0]
    spec = d["spec"]["template"]["spec"]
    assert spec["nodeSelector"]["bench-role"] == "loadgen"
    assert spec["containers"][0]["name"] == "runner"
    assert spec["containers"][0]["image"] == "python:3.11-slim"

def test_driver_mount_layout():
    d = [doc for doc in docs("03-loadgen-driver.yaml") if doc["kind"] == "Deployment"][0]
    spec = d["spec"]["template"]["spec"]
    volumes = {v["name"]: v for v in spec["volumes"]}
    mounts = {}
    for m in spec["containers"][0]["volumeMounts"]:
        config_map = volumes[m["name"]].get("configMap")
        if config_map is not None:
            mounts[m["mountPath"]] = (config_map["name"], m.get("subPath"))
    # The k6 script opens fixtures relative to its own path at
    # /workspace/loadgen/k6_embed.js, so the fixture MUST live at
    # /workspace/fixtures/queries.jsonl; Task 3's runner download lands
    # alongside the scripts under /workspace/loadgen/.
    assert mounts["/workspace/fixtures/queries.jsonl"] == ("gpu-embed-fixtures", "queries.jsonl")
    assert mounts["/workspace/loadgen/k6_embed.js"] == ("phase3-loadgen-scripts", "k6_embed.js")
    assert mounts["/workspace/loadgen/run_gpu_matrix.py"] == ("phase3-loadgen-scripts", "run_gpu_matrix.py")

def test_driver_installs_k6():
    d = [doc for doc in docs("03-loadgen-driver.yaml") if doc["kind"] == "Deployment"][0]
    command = d["spec"]["template"]["spec"]["containers"][0]["command"]
    command_text = "\n".join(str(part) for part in command)
    assert "k6-v0.54.0-linux-amd64" in command_text
