# Phase 3 GPU Embeddings Benchmark Reproduction Guide

This directory contains the code, manifests, and test scripts to reproduce the **Lightweight GPU Embeddings Benchmark** comparing Direct Serving (FastAPI + PyTorch) against Ray Serve (`@serve.batch`).

## Structure

```
benchmarks/gpu-embed/
├── fixtures/
│   ├── generate_fixtures.py
│   └── queries.jsonl
├── direct/
│   ├── app.py
│   ├── Dockerfile
│   └── k8s/
│       ├── deployment.yaml
│       └── service.yaml
├── ray/
│   ├── serve_embed.py
│   ├── Dockerfile
│   └── rayservice-embed.yaml
└── loadgen/
    ├── k6_embed.js
    └── run_embed_matrix.py
```

## Reproduction Steps

### 1. Generate Query Fixtures
```bash
python3 fixtures/generate_fixtures.py --count 2000 --output fixtures/queries.jsonl
```

### 2. Deploy Direct Serving Baseline
```bash
kubectl apply -f direct/k8s/ -n model-serving-benchmark
kubectl wait --for=condition=ready pod -l app=direct-gpu-embed -n model-serving-benchmark --timeout=300s
curl -s http://direct-gpu-embed-svc:8000/readiness
```

### 3. Deploy Ray Serve GPU Candidate
```bash
kubectl apply -f ray/rayservice-embed.yaml -n model-serving-benchmark
kubectl wait --for=condition=Ready rayservice/rayservice-gpu-embed -n model-serving-benchmark --timeout=300s
curl -s http://rayservice-gpu-embed-haproxy-svc:8000/readiness
```

### 4. Execute the Concurrency Sweep (C in [1, 4, 8, 16, 32, 64])
From a load generator pod in the same AWS availability zone (`us-east-1d`):
```bash
python3 loadgen/run_embed_matrix.py \
  --concurrencies 1,4,8,16,32,64 \
  --duration 30s \
  --output-dir results/gpu_embed
```
