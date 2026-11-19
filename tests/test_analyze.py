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


def _grouped(trials):
    grouped = {}
    for t in trials:
        grouped.setdefault(t["architecture"], []).append(t)
    return grouped


def test_header_reflects_real_trial_and_request_counts():
    trials = []
    for n in (1, 2):
        t = trial("Direct Serving (Uvicorn)", "direct", "block1", 1.0)
        t["trial_number"] = n
        t["total_requests"] = 1500
        trials.append(t)
        t = trial("Ray Serve (HAProxy :8000)", "ray_haproxy", "block1", 1.3)
        t["trial_number"] = n
        t["total_requests"] = 1501
        trials.append(t)
    t = trial("Ray Serve (HAProxy local control)", "ray_haproxy_local", "block3", 1.1)
    t["total_requests"] = 1499
    trials.append(t)
    t = trial("Ray Serve (Direct Ingress local control)", "ray_direct_local", "block3", 1.0)
    t["total_requests"] = 1499
    trials.append(t)
    # Stale/template metadata must not leak into the header.
    meta = {"total_requests": 22508, "trials_per_architecture": 5,
            "trials_per_block": {"block1": 2, "block3": 1}}
    md = analyze.generate_tables(meta, _grouped(trials))
    header = md.split("## 1.")[0]
    assert "22508" not in md
    assert "15 randomized trials" not in md
    assert "Total Requests Reconciled:** 9000 across 6 trials in 2 blocks" in header
    assert "Direct Serving (Uvicorn): 2" in header
    assert "Ray Serve (HAProxy local control): 1" in header
    assert "Trials per Block:** block1: 4, block3: 2" in header
    assert "Runs per Arm per Block:** block1: 2, block3: 1" in header
    assert "df = 4, t = 2.776" not in md
    assert "df = n − 1" in md
    assert "## 5. Complete Individual Trial Measurements (6 Trials)" in md
    assert "| Architecture | Block | Trial # |" in md
    assert "| Ray Serve (HAProxy :8000) | block1 | 2 | 1501 |" in md


def test_negative_delta_renders_with_single_sign():
    trials = [
        trial("Direct Serving (Uvicorn)", "direct", "block1", 1.4),
        trial("Ray Serve (HAProxy :8000)", "ray_haproxy", "block1", 1.3),
        trial("Ray Serve (HAProxy local control)", "ray_haproxy_local", "block3", 1.0),
        trial("Ray Serve (Direct Ingress local control)", "ray_direct_local", "block3", 1.1),
    ]
    md = analyze.generate_tables({}, _grouped(trials))
    assert "+-" not in md
    assert "-0.100 ms" in md
    assert "-7.1%" in md


def test_cli_requires_data_and_output(monkeypatch, tmp_path):
    import pytest
    data = tmp_path / "raw.json"
    data.write_text('{"benchmark_metadata": {}, "trials": []}')
    # Guard: if defaults ever come back, they must not point at protected files.
    monkeypatch.setattr(analyze, "DEFAULT_DATA_PATH", str(data), raising=False)
    monkeypatch.setattr(analyze, "DEFAULT_OUTPUT_PATH", str(tmp_path / "default.md"), raising=False)
    monkeypatch.chdir(tmp_path)
    for argv in (["analyze.py"],
                 ["analyze.py", "--data", str(data)],
                 ["analyze.py", "--output", str(tmp_path / "out.md")]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as exc:
            analyze.main()
        assert exc.value.code == 2
