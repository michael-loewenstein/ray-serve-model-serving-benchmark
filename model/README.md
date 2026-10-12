# XGBoost CPU Tabular Model

This directory contains the synthetic tabular training pipeline used for isolating and measuring serving overhead.

## Workload Specification

- **Algorithm:** XGBoost Binary Classification (`binary:logistic`, `tree_method: hist`)
- **Features:** 50 continuous numeric features ($X \sim \mathcal{N}(0, 1)$)
- **Trees (`n_estimators`):** 100
- **Max Depth:** 6
- **Threads:** `nthread: 1` during inference (`OMP_NUM_THREADS=1`)
- **Inference Time:** ~0.50 ms – 0.80 ms on AWS `c6i.2xlarge` (Intel Xeon 8375C @ 2.90GHz)

Because pure model inference completes in sub-millisecond time, this workload is sensitive to socket, event-loop, serialization, and reverse-proxy transit overheads.

## Quickstart

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Train model and generate sample fixtures
python train.py
```

Outputs:
- `artifacts/model.json`: Pinned, deterministic model weights (~140 KB)
- `../loadtest/fixtures/sample_payloads.jsonl`: 1,000 JSON lines formatted for `k6` load testing.
