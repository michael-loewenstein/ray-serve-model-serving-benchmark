# Phase 2 CPU Rerun Manifests

Kubernetes manifests for the Phase 2 HAProxy rerun on the dedicated
`ray-serve-phase2-rerun` cluster (Ray 2.60.0, KubeRay 1.7.2, four
`c6i.2xlarge` nodes in `us-west-2a`). The Direct baseline and one RayService
are deployed at a time; blocks run sequentially and the Direct Deployment stays
up throughout.

## Files

| File | Contents |
|---|---|
| `00-namespace.yaml` | `model-serving-benchmark` namespace |
| `01-direct-serving.yaml` | Two-replica Uvicorn Deployment (`direct-serving-rerun`) + `direct-serving-svc` |
| `02-ray-python-proxy.yaml` | RayService `ray-python-proxy-rerun` (Python proxy on `:8000`) + `ray-python-proxy-svc` |
| `03-ray-haproxy.yaml` | RayService `ray-haproxy-rerun` (HAProxy on `:8000`) + `ray-haproxy-svc` and `ray-direct-ingress-svc` |
| `03b-ray-haproxy-local.yaml` | Single-replica control RayService `ray-haproxy-local-rerun` pinned to `bench-slot=w1` + `ray-haproxy-w1-svc` and `ray-direct-w1-svc` |
| `04-loadgen-driver.yaml` | Idle `loadgen-driver` Deployment with k6 v0.54.0, the matrix runner, and fixtures mounted |
| `preflight.sh` | Runtime gate for `haproxy`, `python`, and `local` modes |

## Routing

| Service | Port | targetPort | Selects |
|---|---|---|---|
| `direct-serving-svc` | 8000 | 8000 | Direct pods (`app=direct-serving-rerun`) |
| `ray-python-proxy-svc` | 8000 | 8000 | Ray workers with `bench-arm=python-proxy` |
| `ray-haproxy-svc` | 8000 | 8000 | Ray workers with `bench-arm=haproxy` |
| `ray-direct-ingress-svc` | 8000 | 30000 | Ray workers with `bench-arm=haproxy` |
| `ray-haproxy-w1-svc` | 8000 | 8000 | Worker `bench-arm=local,bench-slot=w1` |
| `ray-direct-w1-svc` | 8000 | 30000 | Worker `bench-arm=local,bench-slot=w1` |

Ray Services select `ray.io/node-type=worker` as well, so the head proxy is
never a Service endpoint. All Ray containers run
`RAY_SERVE_THROUGHPUT_OPTIMIZED=1`; `RAY_SERVE_ENABLE_HA_PROXY` is `1` for the
HAProxy arms and `0` for the Python-proxy arm, which also sets
`RAY_SERVE_ENABLE_DIRECT_INGRESS=0` to keep `proxy_location: EveryNode`.
`ray-haproxy` is not installed through `runtime_env` because the official Ray
image already ships it, and `RAY_IGNORE_VERSION_CHECK` is never set.

## ConfigMaps

The manifests mount ConfigMaps created by the run, not files in this directory:

```bash
kubectl create configmap direct-serving-code --from-file=direct_app.py=k8s/app/direct_app.py -n model-serving-benchmark
kubectl create configmap ray-serve-code --from-file=ray_serve_app.py=k8s/app/ray_serve_app.py -n model-serving-benchmark
kubectl create configmap benchmark-model-artifact --from-file=model.json=model/artifacts/model.json -n model-serving-benchmark
kubectl create configmap loadgen-scripts \
  --from-file=run_matrix.py=loadtest/run_matrix.py \
  --from-file=k6_benchmark.js=loadtest/k6_benchmark.js \
  -n model-serving-benchmark
kubectl create configmap loadgen-fixtures \
  --from-file=sample_payloads.jsonl=loadtest/fixtures/sample_payloads.jsonl \
  -n model-serving-benchmark
```

## Deploy order

1. `00-namespace.yaml`, then the ConfigMaps above.
2. `01-direct-serving.yaml` (stays up for every block).
3. Block 1: `03-ray-haproxy.yaml` → preflight `haproxy` → run `block1`.
4. Block 3 control: delete `03-ray-haproxy.yaml`, apply
   `03b-ray-haproxy-local.yaml` → preflight `local` → run `block3`.
5. Block 2: delete `03b-ray-haproxy-local.yaml`, apply
   `02-ray-python-proxy.yaml` → preflight `python` → run `block2`.

Delete whole manifest files between Ray arms so stale Services do not linger.
`04-loadgen-driver.yaml` is applied once and exec'd for each block.

## Preflight

```bash
NS=model-serving-benchmark KUBECTL=kubectl \
  bash benchmarks/phase2-cpu-rerun/preflight.sh haproxy   # or python / local
```

The script exits non-zero on the first failed check and prints its evidence:

- four nodes, one per role, all `c6i.2xlarge` in `us-west-2a`;
- `ray --version` is `2.60.0`;
- `haproxy -v` reports 2.8.x on the head and workers (HAProxy modes);
- `ray list actors`: 3 `HAProxyManager` actors in `haproxy` mode, 2 in `local`
  mode (head + `w1`), 0 in `python` mode with 3 Python proxy actors;
- a listening-port scan shows `:8000` on the relevant workers (the stock Ray
  image has no `ss`/iproute2, so the script runs `python3` in the `ray-worker`
  container to read sockets in state `0A` (LISTEN) from `/proc/net/tcp` and
  `/proc/net/tcp6`); in `haproxy` and `local` modes it also requires a
  `:300xx` direct-ingress listener, and both workers agree on `:300xx`. `python` mode sets `RAY_SERVE_ENABLE_DIRECT_INGRESS=0`, so only
  `:8000` is required there, and `local` mode checks `w1` only;
- every Service has the expected targetPort and endpoint pods;
- the RayService `serveConfigV2` sets `max_ongoing_requests: 1000`, and
  `ray serve status` reports the expected healthy replicas;
- 10 `/infer` requests per Service return HTTP 200 with a numeric prediction,
  run from the Direct pod using `python3`/`urllib`;
- HAProxy request counters increase across the 10 requests (HAProxy modes).
