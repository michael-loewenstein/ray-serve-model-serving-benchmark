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
    assert c["availabilityZones"] == ["us-west-2a", "us-west-2b"]

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
