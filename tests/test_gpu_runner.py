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

def test_inter_trial_cooldown_flag_defaults_to_zero():
    # Parse-only pin: the flag exists, takes plain seconds, and defaults to 0
    # (no sleep), so the pinned mocked-runtime tests stay at their fast runtimes.
    parser = rg.build_parser()
    base = ["--output-raw-json", "raw.json", "--output-raw-csv", "raw.csv"]
    assert parser.parse_args(base).inter_trial_cooldown == 0
    args = parser.parse_args(base + ["--inter-trial-cooldown", "3"])
    assert args.inter_trial_cooldown == 3
