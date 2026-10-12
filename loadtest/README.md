# Load Testing Suite with k6

This directory provides the open-loop benchmark harness using [k6](https://k6.io/) to measure latency and throughput with high statistical rigor.

## Workload Model

- **Executor:** `constant-arrival-rate`
- **Rate:** 50 req/s (default)
- **Duration:** 30s per trial (nominal 1,500 requests per trial)
- **Pre-Allocated VUs:** 25
- **Max VUs:** 100
- **Traffic Nature:** Open-loop arrival (independent of server response time, avoiding closed-loop coordinated omission bias).

## IMPORTANT: Network Topology Requirement

> **Do NOT run load tests via `kubectl port-forward`.**  
> Port-forwarding routes traffic through the Kubernetes API server control plane and workstation network interfaces, injecting multi-millisecond connection queuing and latency jitter. Tests must be run **in-cluster** (via `k8s/04-loadgen-job.yaml`) or from a host co-located in the same AWS Availability Zone (`us-east-1d`).

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

## Running the Automated Randomized Matrix

To eliminate temporal bias (such as cloud noisy neighbor shifts or thermal throttling), `run_matrix.py` shuffles the execution order across the 3 architectures and runs 5 independent trials each:

```bash
python loadtest/run_matrix.py --trials 5 --rate-rps 50 --duration 30s
```

Outputs:
- Individual trial JSON summaries in `results/matrix_runs/`
- Aggregated raw dataset in `results/raw_trials.json` (ready for `results/analyze.py`)
- Aggregated tabular CSV in `results/raw_trials.csv`
