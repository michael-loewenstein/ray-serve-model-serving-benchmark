# Load Testing Suite with k6

This directory provides the open-loop benchmark harness using [k6](https://k6.io/) to measure latency and throughput with high statistical rigor.

## Workload Model

- **Executor:** `constant-arrival-rate`
- **Rate:** 50 req/s (default)
- **Duration:** 30s per trial (nominal 1,500 requests per trial)
- **Pre-Allocated VUs:** 25
- **Max VUs:** 100
- **Traffic Nature:** Open-loop arrival (independent of server response time, avoiding closed-loop coordinated omission bias).

## Prerequisites

Install `k6` (pinned: `v0.54.0`):
```bash
# macOS
brew install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

## Running an Isolated Trial

To test a single architecture manually:

```bash
# Direct Serving
k6 run -e TARGET_HOST=http://localhost:8000 -e RATE_RPS=50 -e STAGE_DURATION=30s k6_benchmark.js

# Ray Serve Python Proxy
k6 run -e TARGET_HOST=http://localhost:8000 -e RATE_RPS=50 -e STAGE_DURATION=30s k6_benchmark.js

# Ray Serve Worker HAProxy
k6 run -e TARGET_HOST=http://localhost:8000 -e RATE_RPS=50 -e STAGE_DURATION=30s k6_benchmark.js
```

## Running the Automated Randomized Matrix

To eliminate temporal bias (such as cloud noisy neighbor shifts or thermal throttling), `run_matrix.py` shuffles the execution order across the 3 architectures and runs 5 independent trials each:

```bash
python run_matrix.py --trials 5 --rate-rps 50 --duration 30s
```
