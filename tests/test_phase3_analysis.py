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

def test_gate_low_load_mixed_passes_consolidate_relative():
    # Both levels pass per spec — C=1 via the absolute gate, C=4 via the
    # relative gate — so the consolidation must return the worst PASSING
    # verdict, never a (false) FAIL.
    rows = {
        ("direct", 1): [trial("direct", "Direct Serving (GPU)", 1, 1.0, 50.0, 1.1, 90.0)],
        ("ray_ha", 1): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 1, 3.5, 52.5, 4.0, 90.0)],  # Δp95 2.5 ms, abs pass
        ("direct", 4): [trial("direct", "Direct Serving (GPU)", 4, 1.0, 50.0, 1.1, 90.0)],
        ("ray_ha", 4): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 4, 5.0, 54.0, 5.5, 90.0)],  # Δp95 4.0 ms = 8%, rel pass
    }
    assert ag.gate_low_load(rows) == "PASS (relative)"

def test_gate_low_load_mixed_pass_fail_consolidates_fail():
    # C=1 passes absolute; C=4 exceeds both gates (Δp95 12.0 ms = 24%), so the
    # consolidated verdict must be the synthesized FAIL naming C=4 and its numbers.
    rows = {
        ("direct", 1): [trial("direct", "Direct Serving (GPU)", 1, 1.0, 50.0, 1.1, 90.0)],
        ("ray_ha", 1): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 1, 3.5, 52.5, 4.0, 90.0)],
        ("direct", 4): [trial("direct", "Direct Serving (GPU)", 4, 1.0, 50.0, 1.1, 90.0)],
        ("ray_ha", 4): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 4, 12.0, 62.0, 13.0, 90.0)],  # Δp95 12.0 ms = 24%
    }
    verdict = ag.gate_low_load(rows)
    assert verdict.startswith("FAIL")
    assert "C=4" in verdict
    assert "12.000" in verdict
    assert "24.0%" in verdict

def test_delta_formatting_single_sign():
    # Signed like Phase 2: exactly one leading sign, never "+-" or "0.000-" tails.
    assert ag.fmt_signed(2.0) == "+2.000"
    assert ag.fmt_signed(-1.5) == "-1.500"
    assert ag.fmt_signed(0.0) == "+0.000"
    rows = {("direct", 4): [trial("direct", "Direct Serving (GPU)", 4, 6.0, 7.0, 8.0, 100.0)],
            ("ray_ha", 4): [trial("ray_ha", "Ray Serve (HAProxy :8000)", 4, 4.5, 5.5, 6.5, 99.0)]}
    md = ag.generate_gpu_tables({}, rows)
    assert "-1.500 ms" in md          # ray_ha − direct p50 delta, negative
    assert "+-" not in md             # no double-sign tails anywhere
    assert "missing" in md.lower() or "C=4" in md  # renders without KeyError for absent levels
