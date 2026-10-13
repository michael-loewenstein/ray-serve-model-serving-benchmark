#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-model-serving-benchmark}"
echo "==> Cleaning up all benchmark deployments, RayServices, and Jobs in '${NAMESPACE}'..."

# Delete RayServices (triggers removal of Ray head and worker GPU pods)
kubectl delete rayservice --all -n "${NAMESPACE}" --ignore-not-found

# Delete benchmark deployments, services, pods, and loadgen jobs
kubectl delete deployment -l app=direct-gpu-embed -n "${NAMESPACE}" --ignore-not-found
kubectl delete deployment -l app=direct-serving -n "${NAMESPACE}" --ignore-not-found
kubectl delete service -l app=direct-gpu-embed -n "${NAMESPACE}" --ignore-not-found
kubectl delete service -l app=direct-serving -n "${NAMESPACE}" --ignore-not-found
kubectl delete job --all -n "${NAMESPACE}" --ignore-not-found

echo "==> Clean-up requested. Karpenter will consolidate and terminate idle EC2 GPU/CPU instances."
