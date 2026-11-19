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

def test_metadata_records_az(tmp_path):
    raw = tmp_path / "raw.json"; csv = tmp_path / "raw.csv"
    t = {"architecture": "Direct Serving (Uvicorn)", "target_key": "direct",
         "trial_number": 1, "total_requests": 1, "successful_requests": 1,
         "failed_requests": 0, "goodput_rps": 1.0, "p50_ms": 1.0, "p90_ms": 1.0,
         "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0,
         "pure_booster_predict_p50_ms": 0.5, "server_total_p50_ms": 0.6,
         "network_transit_p50_ms": 0.4, "block": "block1"}
    rm.export_raw_trials_schema([t], str(raw), str(csv), 50, "30s", 5)
    meta = json.loads(raw.read_text())["benchmark_metadata"]
    assert meta["co_location"] == "Single Availability Zone (us-west-2a), zero cross-AZ transit"


# --- I5 / I6 / I7: traceability, required paths, threshold exit, incremental export ---
import datetime
import subprocess as _subprocess

import pytest

K6_SUMMARY = {"metrics": {
    "http_reqs": {"count": 1500, "rate": 50.0},
    "http_req_duration": {"med": 1.2, "p(90)": 1.5, "p(95)": 1.8, "p(99)": 2.4, "max": 9.0},
    "server_model_latency_ms": {"med": 0.5},
    "server_total_latency_ms": {"med": 0.6},
    "network_roundtrip_latency_ms": {"med": 0.6},
    "successful_requests": {"count": 1500},
    "failed_requests": {"count": 0},
}}


def _fake_k6(returncodes):
    """Return a subprocess.run stand-in that writes a k6 summary and yields the given exit codes."""
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        out = cmd[cmd.index("--summary-export") + 1]
        Path(out).write_text(json.dumps(K6_SUMMARY))
        code = returncodes[len(calls) - 1] if len(calls) - 1 < len(returncodes) else 0
        return _subprocess.CompletedProcess(cmd, code, stdout="k6 output", stderr=None)

    fake_run.calls = calls
    return fake_run


def test_exit_99_threshold_breach_is_recorded(tmp_path, monkeypatch):
    monkeypatch.setattr(rm.subprocess, "run", _fake_k6([99]))
    r = rm.run_k6_trial("direct", "http://x", 50, "30s", str(tmp_path / "t.json"), 1,
                        block="block1", seed=44)
    assert r["threshold_breached"] is True
    assert r["p50_ms"] == 1.2


def test_exit_0_records_no_threshold_breach_and_traceability(tmp_path, monkeypatch):
    monkeypatch.setattr(rm.subprocess, "run", _fake_k6([0]))
    r = rm.run_k6_trial("direct", "http://x", 50, "30s", str(tmp_path / "t.json"), 1,
                        block="block1", seed=43)
    assert r["threshold_breached"] is False
    assert r["seed"] == 43
    started = datetime.datetime.fromisoformat(r["started_at_utc"])
    assert started.utcoffset() == datetime.timedelta(0)


def test_other_nonzero_exit_still_aborts(tmp_path, monkeypatch):
    monkeypatch.setattr(rm.subprocess, "run", _fake_k6([1]))
    with pytest.raises(RuntimeError):
        rm.run_k6_trial("direct", "http://x", 50, "30s", str(tmp_path / "t.json"), 1,
                        block="block1", seed=42)


def test_incremental_export_keeps_completed_trials_on_abort(tmp_path, monkeypatch):
    raw = tmp_path / "raw.json"
    csv_path = tmp_path / "raw.csv"
    # block3 with 2 trials per arm: 2 warmups, then 4 measured trials.
    # Measured trial 1 breaches thresholds (99), trial 2 passes, trial 3 crashes.
    fake = _fake_k6([0, 0, 99, 0, 1])
    monkeypatch.setattr(rm.subprocess, "run", fake)
    monkeypatch.setattr(rm.time, "sleep", lambda s: None)
    monkeypatch.setattr(sys, "argv", [
        "run_matrix.py", "--block", "block3", "--trials", "2", "--seed", "43",
        "--output-dir", str(tmp_path / "out"),
        "--output-raw-json", str(raw), "--output-raw-csv", str(csv_path),
    ])
    with pytest.raises(SystemExit) as exc:
        rm.main()
    assert exc.value.code == 1
    trials = json.loads(raw.read_text())["trials"]
    assert len(trials) == 2
    assert sorted(t["threshold_breached"] for t in trials) == [False, True]
    for t in trials:
        assert t["seed"] == 43
        assert t["block"] == "block3"
        datetime.datetime.fromisoformat(t["started_at_utc"])
    header = csv_path.read_text().splitlines()[0].split(",")
    for col in ("threshold_breached", "seed", "started_at_utc"):
        assert col in header
    assert len(csv_path.read_text().splitlines()) == 3


def test_incremental_export_with_append_merges_existing(tmp_path, monkeypatch):
    raw = tmp_path / "raw.json"
    csv_path = tmp_path / "raw.csv"
    prior = {"architecture": "Direct Serving (Uvicorn)", "target_key": "direct",
             "trial_number": 1, "total_requests": 1, "successful_requests": 1,
             "failed_requests": 0, "goodput_rps": 1.0, "p50_ms": 1.0, "p90_ms": 1.0,
             "p95_ms": 1.0, "p99_ms": 1.0, "max_ms": 1.0,
             "pure_booster_predict_p50_ms": 0.5, "server_total_p50_ms": 0.6,
             "network_transit_p50_ms": 0.4, "block": "block1"}
    rm.export_raw_trials_schema([prior], str(raw), str(csv_path), 50, "30s", 5)
    monkeypatch.setattr(rm.subprocess, "run", _fake_k6([0, 0, 0, 1]))
    monkeypatch.setattr(rm.time, "sleep", lambda s: None)
    monkeypatch.setattr(sys, "argv", [
        "run_matrix.py", "--block", "block3", "--trials", "1", "--append",
        "--output-dir", str(tmp_path / "out"),
        "--output-raw-json", str(raw), "--output-raw-csv", str(csv_path),
    ])
    with pytest.raises(SystemExit):
        rm.main()
    trials = json.loads(raw.read_text())["trials"]
    assert len(trials) == 2
    assert {t["block"] for t in trials} == {"block1", "block3"}


@pytest.mark.parametrize("missing", ["--output-raw-json", "--output-raw-csv"])
def test_cli_requires_raw_output_paths(tmp_path, monkeypatch, missing):
    def must_not_run(*a, **k):
        raise AssertionError("k6 must not run when required args are missing")
    monkeypatch.setattr(rm.subprocess, "run", must_not_run)
    monkeypatch.chdir(tmp_path)
    argv = ["run_matrix.py", "--block", "block1", "--output-dir", str(tmp_path / "out"),
            "--output-raw-json", str(tmp_path / "raw.json"),
            "--output-raw-csv", str(tmp_path / "raw.csv")]
    i = argv.index(missing)
    del argv[i:i + 2]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        rm.main()
    assert exc.value.code == 2
