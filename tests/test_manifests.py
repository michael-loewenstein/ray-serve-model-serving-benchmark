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

def test_workers_advertise_one_logical_cpu():
    for name in ["02-ray-python-proxy.yaml", "03-ray-haproxy.yaml", "03b-ray-haproxy-local.yaml"]:
        rs = rayservice(name)
        worker = rs["spec"]["rayClusterConfig"]["workerGroupSpecs"][0]
        assert worker["rayStartParams"]["num-cpus"] == "1"

def test_no_redundant_haproxy_pip_or_version_override():
    for f in ROOT.glob("0*.yaml"):
        text = (ROOT / f.name).read_text()
        assert "RAY_IGNORE_VERSION_CHECK" not in text
        assert "ray-haproxy==" not in text
