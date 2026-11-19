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
