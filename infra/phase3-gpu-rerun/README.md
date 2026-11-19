# Phase 3 GPU Rerun: EKS Cluster

`eksctl-cluster.yaml` defines the cluster for the Phase 3 GPU rerun: EKS
1.34, a new VPC, and four dedicated nodes in `us-west-2a` — two
`g6.xlarge` nodes with one NVIDIA L4 GPU each, plus `c6i.2xlarge` head and
loadgen nodes. A dedicated kubeconfig keeps this cluster isolated from any
other cluster in the account.

## Topology

| Nodegroup | Node | Labels | Role |
|---|---|---|---|
| `head` | 1 × `c6i.2xlarge` | `bench-role=head` | KubeRay operator and Ray head |
| `gpu-w1` | 1 × `g6.xlarge` | `bench-role=gpu`, `bench-slot=gpu1` | Serving workloads (1 × NVIDIA L4) |
| `gpu-w2` | 1 × `g6.xlarge` | `bench-role=gpu`, `bench-slot=gpu2` | Serving workloads (1 × NVIDIA L4) |
| `loadgen` | 1 × `c6i.2xlarge` | `bench-role=loadgen` | k6 load generator only |

The VPC spans `us-west-2a` and `us-west-2b` so the control plane can be created
across two AZs; every nodegroup is pinned to `us-west-2a` to keep all benchmark
traffic in one zone.

The GPU nodegroups use an explicit, region-scoped EKS AMI pinned in the config
(see the AMI refresh procedure below). That AMI family ships the NVIDIA driver;
`nvidia-device-plugin.yaml` then exposes each node's L4 to Kubernetes as an
allocatable `nvidia.com/gpu` resource.

Expected cost while running: ~$2.50/hr (2 GPU nodes + 2 CPU nodes + control
plane + NAT/EBS). Delete the cluster as soon as the run finishes.

## Prerequisites

- `eksctl` 0.230.0 or later
- AWS profile `<aws-profile>` configured in `~/.aws/config`
  (`<aws-profile>` is a placeholder; substitute your own profile in every
  command below)
- `kubectl` 1.34.13 (matches the cluster version)
- `helm` 3.19.2 (for the KubeRay operator)

Every command below passes the AWS profile explicitly. Also use a dedicated
kubeconfig so `kubectl` never acts on another cluster in the account:

```bash
export KUBECONFIG=~/.kube/ray-serve-phase3-rerun
```

## Create the cluster

Run it backgrounded and wait — creation takes roughly 20-30 minutes:

```bash
eksctl create cluster \
  --config-file infra/phase3-gpu-rerun/eksctl-cluster.yaml \
  --profile <aws-profile> \
  --timeout 40m
```

When it finishes, eksctl writes the new cluster's credentials into
`$KUBECONFIG`.

## Fetch the kubeconfig

Re-run this whenever the kubeconfig needs to be rebuilt, or after creating the
cluster from a shell that did not have `KUBECONFIG` set:

```bash
aws eks update-kubeconfig \
  --name ray-serve-phase3-rerun \
  --region us-west-2 \
  --profile <aws-profile> \
  --kubeconfig ~/.kube/ray-serve-phase3-rerun
```

## Apply the NVIDIA device plugin

The driver plugin turns each GPU node's L4 into an allocatable
`nvidia.com/gpu` resource. The manifest is the upstream NVIDIA v0.17.1 static
deployment with one change: a `nodeSelector: {bench-role: gpu}` so the
DaemonSet only runs on the two GPU nodes.

```bash
kubectl apply -f infra/phase3-gpu-rerun/nvidia-device-plugin.yaml
```

Verify the plugin pods are running on exactly the two GPU nodes, and that each
GPU node advertises `nvidia.com/gpu: 1`:

```bash
kubectl get ds -n kube-system -o wide
kubectl get nodes -o wide \
  -L bench-role,bench-slot,topology.kubernetes.io/zone,node.kubernetes.io/instance-type
```

## Install KubeRay

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ >/dev/null
helm repo update >/dev/null
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.2 \
  --namespace kuberay --create-namespace \
  --set nodeSelector.bench-role=head
```

The operator is pinned to the head node via `bench-role=head`.

## Delete the cluster

```bash
eksctl delete cluster \
  --name ray-serve-phase3-rerun \
  --region us-west-2 \
  --profile <aws-profile> \
  --wait
```

Deletion removes the nodegroups, the VPC created for the cluster, and the NAT
gateway. Afterwards, confirm no resources remain:

```bash
aws ec2 describe-instances \
  --region us-west-2 \
  --profile <aws-profile> \
  --filters "Name=tag:project,Values=ray-serve-benchmark" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text

aws ec2 describe-nat-gateways \
  --region us-west-2 \
  --profile <aws-profile> \
  --filter "Name=state,Values=available" \
  --query "NatGateways[].NatGatewayId" \
  --output text

aws ec2 describe-volumes \
  --region us-west-2 \
  --profile <aws-profile> \
  --filters "Name=status,Values=available" \
  --query "Volumes[].VolumeId" \
  --output text
```

All three commands should return no resources. Delete the CloudWatch log group
if eksctl created one:

```bash
aws logs delete-log-group \
  --log-group-name /aws/eks/ray-serve-phase3-rerun/cluster \
  --region us-west-2 \
  --profile <aws-profile>
```

Also delete the kubeconfig context once the cluster is gone:

```bash
kubectl config delete-cluster ray-serve-phase3-rerun.us-west-2.eksctl.io
```

## AMI refresh

The GPU nodegroups pin an explicit EKS-optimized AMI (with the NVIDIA driver),
so the GPU nodes come up with a working driver without any bootstrap AMI search.
AMIs are region-scoped and periodically replaced; on a new run, resolve the
current AMI for EKS 1.34 in `us-west-2` and re-pin it into
`eksctl-cluster.yaml` (`gpu-w1` and `gpu-w2` use the same AMI):

```bash
aws ec2 describe-images \
  --region us-west-2 \
  --owners amazon \
  --profile <aws-profile> \
  --filters "Name=name,Values=amazon-eks-node-al2023-x86_64-nvidia-1.34-v*" \
  --query 'sort_by(Images,&CreationDate)[-1].[ImageId,Name]' \
  --output text
```

This prints the newest matching image ID and its name (region-scoped). Put the
image ID into the `ami:` field of both GPU nodegroups in
`infra/phase3-gpu-rerun/eksctl-cluster.yaml`.
