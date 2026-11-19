# Phase 3 GPU Rerun Manifests

Kubernetes manifests for the Phase 3 GPU embeddings rerun on the dedicated
`ray-serve-phase3-rerun` cluster (Ray 2.60.0, KubeRay 1.7.2, four nodes in
`us-west-2a`: one `c6i.2xlarge` head, two `g6.xlarge` GPU nodes labeled
`bench-slot=gpu1`/`gpu2` — NVIDIA L4 — and one `c6i.2xlarge` loadgen node).
Two arms run against the same model (`sentence-transformers/all-MiniLM-L6-v2`,
FP16, dynamic micro-batching at 32 / 5 ms), both live side by side:

- **Direct** (`01-direct-gpu.yaml`): FastAPI + asyncio.Queue batcher, pinned
  to `bench-slot=gpu1`, served via `direct-gpu-embed-svc` (:8000 → :8000).
- **Ray Serve HAProxy** (`02-ray-ha.yaml`): RayService `rayservice-gpu-embed`
  with a single replica pinned to the `bench-slot=gpu2` worker via the
  `worker_only` custom resource, measured through the worker's HAProxy on
  `:8000` via `ray-ha-svc` (:8000 → :8000). The head proxy exists
  (`proxy_location: EveryNode`) but is never a Service endpoint.

No 8000→30000 direct-ingress Service exists in this rerun; the replica's
`:30000` direct ingress is only checked by the gate as evidence the replica
is up.

## Files

| File | Contents |
|---|---|
| `00-namespace.yaml` | `model-serving-benchmark` namespace |
| `01-direct-gpu.yaml` | One-replica `direct-gpu-embed` Deployment (on `bench-slot=gpu1`) + `direct-gpu-embed-svc` |
| `02-ray-ha.yaml` | RayService `rayservice-gpu-embed` (single-replica HAProxy arm on `bench-slot=gpu2`) + `ray-ha-svc` |
| `03-loadgen-driver.yaml` | Idle `loadgen-driver` Deployment with k6 v0.54.0, the runner script, and fixtures mounted |
| `preflight.sh` | Runtime gate, mode `gpu` |
| `README.md` | This walkthrough |

Container names are pinned because the gate execs them explicitly with `-c`:
head `ray-head`, worker `ray-gpu-worker`, direct `server`, driver `runner`.

## ConfigMaps

The manifests mount ConfigMaps created by the run, not files in this
directory:

```bash
kubectl create configmap direct-gpu-embed-code \
  --from-file=app.py=benchmarks/gpu-embed/direct/app.py -n model-serving-benchmark
kubectl create configmap ray-gpu-embed-code \
  --from-file=serve_embed.py=benchmarks/gpu-embed/ray/serve_embed.py -n model-serving-benchmark
kubectl create configmap gpu-embed-fixtures \
  --from-file=queries.jsonl=benchmarks/gpu-embed/fixtures/queries.jsonl -n model-serving-benchmark
kubectl create configmap phase3-loadgen-scripts \
  --from-file=k6_embed.js=benchmarks/phase3-gpu-rerun/loadgen/k6_embed.js \
  --from-file=run_gpu_matrix.py=benchmarks/phase3-gpu-rerun/loadgen/run_gpu_matrix.py \
  -n model-serving-benchmark
```

The driver mounts the script and fixture ConfigMaps so that the k6 script at
`/workspace/loadgen/k6_embed.js` can resolve its fixture with
`open('../fixtures/queries.jsonl')` → `/workspace/fixtures/queries.jsonl`, and
the runner downloaded by the measurement task lands alongside
`run_gpu_matrix.py` in `/workspace/loadgen/`. Results are written to the
`/results` emptyDir before upload.

## Block walkthrough

One deployment, no block swaps (contrast with Phase 2):

1. **Deploy.** `00-namespace.yaml`, then the ConfigMaps above, then
   `01-direct-gpu.yaml` and `02-ray-ha.yaml` together, then
   `03-loadgen-driver.yaml`. Expect a patient first boot: each serving pod
   spends 10–20 min on image pull, `pip install`/`runtime_env`, and the
   Hugging Face model download before its readiness probe passes.
2. **Gate.** Run the fail-closed preflight before any load:

   ```bash
   NS=model-serving-benchmark KUBECTL=kubectl \
     bash benchmarks/phase3-gpu-rerun/preflight.sh gpu
   ```

   It exits non-zero on the first failed check and prints its evidence:
   4 nodes with the expected roles, instance types, and zone (`us-west-2a`
   unless `EXPECTED_ZONE` is overridden); `torch.cuda.is_available()` printing
   `NVIDIA L4` in **both** serving pods; Ray 2.60.0; HAProxy 2.8.x present on
   head and worker (missing binary → hard stop: Serve would fall back to the
   Python proxy and the arm would be mislabeled); exactly 2 HAProxyManager
   actors and 0 Python-proxy fallbacks; `:8000` on the direct pod and
   `:8000` + `:30000` (replica direct ingress) on the worker, via a
   `/proc/net/tcp` scan (the Ray image has no `ss`); live container env
   (`RAY_SERVE_ENABLE_HA_PROXY=1`, `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`) on head
   and worker, not just in the manifest; 1 endpoint per Service with
   `targetPort 8000`; Serve reporting 1 RUNNING `EmbeddingDeployment` replica
   and `max_ongoing_requests: 1000` in `serveConfigV2`; 10 POST `/embed` per
   Service returning HTTP 200 with a 384-dim embedding; and HAProxy stats
   counters increasing by at least the 10 requests sent through
   `ray-ha-svc`.
3. **Measure.** From the driver pod's `runner` container, the measurement
   task stages the runner, runs the interleaved 6-level × 5-trial block
   against both Services (`--block gblock1`), copies the results out of
   `/results`, verifies integrity, and commits the raw data.
4. **Teardown — before analysis.** The measurement task uploads results and
   immediately deletes the cluster and sweeps for leaked EC2/NAT/volume/CFN
   resources (see `benchmarks/cleanup_all_resources.sh` and the run's
   teardown section). **Teardown precedes analysis on purpose:** analysis is
   purely offline over the committed raw rows, the GPUs must not idle through
   it (~$2.39/hr against a hard $10 session cap), and with the cluster gone
   the analyzer cannot quietly re-query a warm serving pod instead of the
   recorded evidence. If analysis finds an anomaly that needs
   re-measurement, that is a new billed session, escalated to the user first.

## Preflight failure = no load

Any `FAIL` from `preflight.sh` means the run stops before a single k6 request
is issued. The block walkthrough is deploy → gate → measure → teardown →
analyze, in that order, and the gate is the only thing standing between a
misconfigured cluster and 60 billed trials of worthless numbers.
