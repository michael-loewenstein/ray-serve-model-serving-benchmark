# Phase 2 Rerun: EKS Cluster

`eksctl-cluster.yaml` defines the cluster for the Phase 2 HAProxy rerun: EKS
1.34, a new VPC, and four dedicated `c6i.2xlarge` nodes, all in `us-west-2a`.
A dedicated kubeconfig keeps this cluster isolated from any other cluster in
the account.

## Topology

| Nodegroup | Node | Labels | Role |
|---|---|---|---|
| `head` | 1 × `c6i.2xlarge` | `bench-role=head` | KubeRay operator and Ray head |
| `worker-w1` | 1 × `c6i.2xlarge` | `bench-role=worker`, `bench-slot=w1` | Serving workloads |
| `worker-w2` | 1 × `c6i.2xlarge` | `bench-role=worker`, `bench-slot=w2` | Serving workloads |
| `loadgen` | 1 × `c6i.2xlarge` | `bench-role=loadgen` | k6 load generator only |

The VPC spans `us-west-2a` and `us-west-2b` so the control plane can be created
across two AZs; every nodegroup is pinned to `us-west-2a` to keep all benchmark
traffic in one zone.

Expected cost while running: ~$1.56/hr (4 nodes + control plane + NAT/EBS).
Delete the cluster as soon as the run finishes.

## Prerequisites

- `eksctl` 0.230.0 or later
- AWS profile `<aws-profile>` configured in `~/.aws/config`
  (`<aws-profile>` is a placeholder; the account-specific profile name was redacted on 2026-11-19. Substitute your own profile in every command below.)
- `kubectl` 1.34.13 (matches the cluster version)

Every command below passes the AWS profile explicitly. Also use a dedicated
kubeconfig so `kubectl` never acts on another cluster in the account:

```bash
export KUBECONFIG=~/.kube/ray-serve-phase2-rerun
```

## Create the cluster

```bash
eksctl create cluster \
  --config-file infra/phase2-rerun/eksctl-cluster.yaml \
  --profile <aws-profile> \
  --timeout 40m
```

Creation takes roughly 20-30 minutes. When it finishes, eksctl writes the new
cluster's credentials into `$KUBECONFIG`.

## Fetch the kubeconfig

Re-run this whenever the kubeconfig needs to be rebuilt, or after creating the
cluster from a shell that did not have `KUBECONFIG` set:

```bash
aws eks update-kubeconfig \
  --name ray-serve-phase2-rerun \
  --region us-west-2 \
  --profile <aws-profile> \
  --kubeconfig ~/.kube/ray-serve-phase2-rerun
```

Verify the four nodes and their labels and zone:

```bash
kubectl get nodes -o wide \
  -L bench-role,bench-slot,topology.kubernetes.io/zone,node.kubernetes.io/instance-type
```

Expected: four `c6i.2xlarge` nodes, all in `us-west-2a`, with `bench-role`
values of `head`, `worker`, `worker`, and `loadgen`.

## Delete the cluster

```bash
eksctl delete cluster \
  --name ray-serve-phase2-rerun \
  --region us-west-2 \
  --profile <aws-profile> \
  --wait
```

Deletion removes the nodegroups, the VPC created for the cluster, and the NAT
gateway. Afterwards, confirm no resources remain and remove the CloudWatch log
group if eksctl created one:

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
  --log-group-name /aws/eks/ray-serve-phase2-rerun/cluster \
  --region us-west-2 \
  --profile <aws-profile>
```

Also delete the kubeconfig context once the cluster is gone:

```bash
kubectl config delete-cluster ray-serve-phase2-rerun.us-west-2.eksctl.io
```
