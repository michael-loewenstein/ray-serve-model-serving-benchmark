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
3. **Hardware Matching (Recommended):**
   For rigorous latency benchmarks, run all deployments on dedicated compute nodes of identical instance families (e.g., AWS `c6i.2xlarge`) within the same Availability Zone to eliminate cross-AZ transit jitter.

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

## Verifying Readiness

```bash
# Direct Serving
kubectl run curl-test --rm -it --restart=Never --image=curlimages/curl -n model-serving-benchmark -- \
  curl -s http://direct-serving-svc:8000/healthz

# Ray Python Proxy
kubectl run curl-test --rm -it --restart=Never --image=curlimages/curl -n model-serving-benchmark -- \
  curl -s http://ray-serve-python-proxy-serve-svc:8000/healthz

# Ray Worker HAProxy
kubectl run curl-test --rm -it --restart=Never --image=curlimages/curl -n model-serving-benchmark -- \
  curl -s http://ray-serve-worker-haproxy-svc:8000/healthz
```
