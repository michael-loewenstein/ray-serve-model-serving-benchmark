# Kubernetes Manifests: 3 Serving Architectures

This directory contains clean, production-grade Kubernetes manifests for evaluating the three model serving architectures:

| Architecture | Manifest | Routing Topology | In-Cluster Service Endpoint |
|---|---|---|---|
| **A: Direct Serving** | `01-direct-serving.yaml` | Client $\to$ K8s Service $\to$ Uvicorn Pod | `http://direct-serving-svc:8000/infer` |
| **B: Ray Serve Python Proxy** | `02-ray-serve-python-proxy.yaml` | Client $\to$ K8s Service $\to$ Ray Head Node (`SERVE_PROXY_ACTOR`) $\to$ Worker Actor | `http://ray-serve-python-proxy-serve-svc:8000/infer` |
| **C: Ray Serve Worker HAProxy** | `03-ray-serve-worker-haproxy.yaml` | Client $\to$ Dedicated Service $\to$ Worker HAProxy (`:30000`) $\to$ Worker Actor | `http://ray-serve-worker-haproxy-svc:8000/infer` |

---

## Prerequisites

1. **Kubernetes Cluster:** v1.30 (or v1.28+)
2. **KubeRay Operator:** `v1.4.2` installed in the cluster:
   ```bash
   helm repo add kuberay https://ray-project.github.io/kuberay-helm/
   helm repo update
   helm install kuberay-operator kuberay/kuberay-operator --version 1.4.2
   ```
3. **Hardware & Zone Matching:**
   All manifests pin `nodeSelector` to dedicated compute nodes on AWS `c6i.2xlarge` in `us-east-1d` with parity resource allocations (1 vCPU / 2 GiB memory per replica) to eliminate cross-AZ transit jitter. Adjust `instance-type` and `zone` to match your cluster topology.

---

## Deployment Steps

### 1. Create Namespace & ConfigMaps

```bash
# 1. Create benchmark namespace
kubectl apply -f 00-namespace.yaml

# 2. Create ConfigMaps for application code
kubectl create configmap direct-serving-code \
  --from-file=direct_app.py=app/direct_app.py \
  -n model-serving-benchmark

kubectl create configmap ray-serve-code \
  --from-file=ray_serve_app.py=app/ray_serve_app.py \
  -n model-serving-benchmark

# 3. Create ConfigMap for the trained model artifact
kubectl create configmap benchmark-model-artifact \
  --from-file=model.json=../model/artifacts/model.json \
  -n model-serving-benchmark
```

### 2. Deploy Direct Serving (Architecture A)

```bash
kubectl apply -f 01-direct-serving.yaml
kubectl wait --for=condition=available deployment/direct-serving-cpu -n model-serving-benchmark --timeout=180s
```

### 3. Deploy Ray Serve with Python Proxy (Architecture B)

```bash
kubectl apply -f 02-ray-serve-python-proxy.yaml
kubectl wait --for=condition=Ready rayservice/ray-serve-python-proxy -n model-serving-benchmark --timeout=300s
```

### 4. Deploy Ray Serve with Worker HAProxy (Architecture C)

```bash
kubectl apply -f 03-ray-serve-worker-haproxy.yaml
kubectl wait --for=condition=Ready rayservice/ray-serve-worker-haproxy -n model-serving-benchmark --timeout=300s
```

---

## CRITICAL: In-Cluster Benchmarking vs. Port-Forwarding

> **Important Note on Network Topology:**  
> **Do NOT run benchmark load tests through `kubectl port-forward`.**  
> Port-forwarding tunnels all HTTP traffic through the Kubernetes API server and local workstation loopbacks. This introduces 2–10+ ms of socket proxy latency and arbitrary scheduling jitter that completely swamps sub-millisecond serving differences.  
> 
> To obtain accurate measurements, execute tests **in-cluster** using the co-located load generator Job (`04-loadgen-job.yaml`) or from an EC2 instance in the exact same Availability Zone.

### Running In-Cluster via k6 Job

```bash
# 1. Create ConfigMaps for load testing script and fixtures
kubectl create configmap k6-benchmark-script \
  --from-file=k6_benchmark.js=../loadtest/k6_benchmark.js \
  -n model-serving-benchmark

kubectl create configmap k6-benchmark-fixtures \
  --from-file=sample_payloads.jsonl=../loadtest/fixtures/sample_payloads.jsonl \
  -n model-serving-benchmark

# 2. Run the benchmark Job (configured for Worker HAProxy by default)
kubectl apply -f 04-loadgen-job.yaml

# 3. Stream k6 execution logs
kubectl logs -f job/loadgen-k6-benchmark -n model-serving-benchmark
```
