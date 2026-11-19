#!/usr/bin/env bash
# Preflight gate for the Phase 3 GPU rerun.
#
# Usage:
#   NS=model-serving-benchmark KUBECTL=kubectl \
#     bash benchmarks/phase3-gpu-rerun/preflight.sh gpu
#
# The expected provider zone defaults to us-west-2a; override with EXPECTED_ZONE
# only if the cluster's single-AZ placement ever moves (do not invent new
# regions).
#
# The script proves the cluster topology, Ray version, CUDA availability on
# both serving pods, HAProxy/proxy placement, listener ports, Service
# endpoints, and Serve health before any load is run. It exits non-zero on the
# first failed check and writes its evidence to stdout.
set -euo pipefail

MODE=${1:-}
NS=${NS:-model-serving-benchmark}
KUBECTL=${KUBECTL:-kubectl}
EXPECTED_ZONE=${EXPECTED_ZONE:-us-west-2a}

usage() {
  echo "usage: $0 gpu" >&2
  exit 2
}

case "$MODE" in
  gpu)
    RAYSERVICE=rayservice-gpu-embed
    EXPECTED_WORKERS=1
    EXPECTED_DIRECT_PODS=1
    EXPECTED_REPLICAS=1
    EXPECTED_HAPROXY_MANAGERS=2
    EXPECTED_PYTHON_PROXIES=0
    HAPROXY_SERVICE=ray-ha-svc
    DIRECT_INGRESS_SERVICE=direct-gpu-embed-svc
    WORKER_SELECTOR="ray.io/node-type=worker,bench-arm=ray"
    ;;
  *)
    usage
    ;;
esac

step() { printf '\n[preflight:%s] %s\n' "$MODE" "$*"; }
info() { printf '  info  %s\n' "$*"; }
ok() { printf '  PASS  %s\n' "$*"; }
die() {
  printf '  FAIL  %s\n' "$*" >&2
  exit 1
}

count_lines() {
  local n
  n=$(printf '%s\n' "$1" | grep -c . || true)
  printf '%s' "$n"
}

pods_for() {
  local selector=$1 out
  out=$("$KUBECTL" -n "$NS" get pods -l "$selector" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}') \
    || die "kubectl get pods -l $selector failed"
  printf '%s\n' "$out" | sed '/^$/d' | sort
}

# ---------------------------------------------------------------------------
step "Cluster nodes"
node_count=$("$KUBECTL" get nodes -o name | wc -l | tr -d ' ')
[ "$node_count" -eq 4 ] || die "expected 4 nodes, found $node_count"
for role in head gpu loadgen; do
  role_count=$("$KUBECTL" get nodes -l "bench-role=$role" -o name | wc -l | tr -d ' ')
  role_count=$(printf '%s' "$role_count" | tr -d ' ')
  [ "$role_count" -ge 1 ] || die "no node with bench-role=$role"
  info "bench-role=$role: $role_count node(s)"
done
head_node_count=$("$KUBECTL" get nodes -l bench-role=head -o name | wc -l | tr -d ' ')
gpu_node_count=$("$KUBECTL" get nodes -l bench-role=gpu -o name | wc -l | tr -d ' ')
loadgen_node_count=$("$KUBECTL" get nodes -l bench-role=loadgen -o name | wc -l | tr -d ' ')
[ "$head_node_count" -eq 1 ] || die "expected 1 bench-role=head node, found $head_node_count"
[ "$gpu_node_count" -eq 2 ] || die "expected 2 bench-role=gpu nodes, found $gpu_node_count"
[ "$loadgen_node_count" -eq 1 ] || die "expected 1 bench-role=loadgen node, found $loadgen_node_count"
gpu1_count=$("$KUBECTL" get nodes -l bench-slot=gpu1 -o name | wc -l | tr -d ' ')
gpu2_count=$("$KUBECTL" get nodes -l bench-slot=gpu2 -o name | wc -l | tr -d ' ')
[ "$gpu1_count" -eq 1 ] || die "expected 1 bench-slot=gpu1 node, found $gpu1_count"
[ "$gpu2_count" -eq 1 ] || die "expected 1 bench-slot=gpu2 node, found $gpu2_count"
# The bench-slot expression uses {{with ...}}{{else}}-{{end}} so head/loadgen
# rows (which have no bench-slot label) emit "-" instead of an empty column:
# `read` would collapse an empty field, shift the following columns, and die
# with a bogus instance-type/zone mismatch.
node_report=$("$KUBECTL" get nodes -o go-template='{{range .items}}{{.metadata.name}}{{"\t"}}{{index .metadata.labels "bench-role"}}{{"\t"}}{{with index .metadata.labels "bench-slot"}}{{.}}{{else}}-{{end}}{{"\t"}}{{index .metadata.labels "node.kubernetes.io/instance-type"}}{{"\t"}}{{index .metadata.labels "topology.kubernetes.io/zone"}}{{"\n"}}{{end}}') \
  || die "could not read node labels"
while IFS=$'\t' read -r name role slot instance_type zone; do
  [ -n "$name" ] || continue
  [ -n "$slot" ] || die "node $name produced an empty bench-slot column; node report parsing is broken"
  case "$role" in
    head|loadgen) expected_type=c6i.2xlarge ;;
    gpu) expected_type=g6.xlarge ;;
    *) die "node $name has unexpected bench-role=$role" ;;
  esac
  [ "$instance_type" = "$expected_type" ] \
    || die "node $name instance-type=$instance_type, expected $expected_type (bench-role=$role)"
  [ "$zone" = "$EXPECTED_ZONE" ] || die "node $name zone=$zone, expected $EXPECTED_ZONE"
  info "$name bench-role=$role bench-slot=$slot $instance_type $zone"
done <<< "$node_report"
ok "4 nodes: 1 c6i.2xlarge head, 2 g6.xlarge GPU nodes (bench-slot gpu1/gpu2), 1 c6i.2xlarge loadgen, all in $EXPECTED_ZONE"

# ---------------------------------------------------------------------------
step "Pods for arm $MODE"
head_pods=$(pods_for "ray.io/node-type=head")
worker_pods=$(pods_for "$WORKER_SELECTOR")
direct_pods=$(pods_for "app=direct-gpu-embed")
head_count=$(count_lines "$head_pods")
worker_count=$(count_lines "$worker_pods")
direct_count=$(count_lines "$direct_pods")
[ "$head_count" -eq 1 ] || die "expected 1 Ray head pod, found $head_count: ${head_pods:-<none>}"
[ "$worker_count" -eq "$EXPECTED_WORKERS" ] || die "expected $EXPECTED_WORKERS Ray GPU worker pod(s), found $worker_count: ${worker_pods:-<none>}"
[ "$direct_count" -eq "$EXPECTED_DIRECT_PODS" ] || die "expected $EXPECTED_DIRECT_PODS direct-gpu-embed pod(s), found $direct_count: ${direct_pods:-<none>}"
head_pod=$(printf '%s\n' "$head_pods" | head -n1)
worker_pod=$(printf '%s\n' "$worker_pods" | head -n1)
direct_pod=$(printf '%s\n' "$direct_pods" | head -n1)
info "head: $head_pod"
info "worker: $worker_pod"
info "direct: $direct_pod"
ok "RayService $RAYSERVICE pods and the Direct GPU pod are present"
info "head proxy is present but is not a Service endpoint (ray-ha-svc selects ray.io/node-type=worker)"

# ---------------------------------------------------------------------------
step "Ray version"
ray_version=$("$KUBECTL" -n "$NS" exec "$head_pod" -c ray-head -- ray --version 2>&1) \
  || die "ray --version failed in $head_pod: $ray_version"
printf '%s\n' "$ray_version" | grep -q '2\.60\.0' \
  || die "expected Ray 2.60.0, got: $ray_version"
info "$ray_version"
ok "Ray 2.60.0"

# ---------------------------------------------------------------------------
step "CUDA acceptance in both serving pods"
# Both serving pods must see the same L4 GPU the load will actually use:
# the Ray replica-hosting worker (container ray-gpu-worker) and the Direct
# baseline (container server). The check must capture the APPLICATION's view
# of the GPU, not just the node's: the Ray worker only gets torch inside the
# serveConfigV2 runtime_env pip venv, so a bare `python3 -c 'import torch'`
# can fail on a perfectly healthy cluster. Therefore, on the worker:
#   1. try the system python3;
#   2. else try every Ray runtime_env pip venv interpreter in turn;
#   3. else fall back to nvidia-smi's device name PLUS the app's own
#      /readiness payload reporting "cuda": true.
# Any path that cannot show an NVIDIA L4 dies (fail-closed).
cuda_torch_probe() {
  # $1 = pod, $2 = container, $3 = python interpreter.
  # Prints the CUDA device name on stdout and returns 0, or returns nonzero.
  local pod=$1 container=$2 interpreter=$3 out
  if out=$("$KUBECTL" -n "$NS" exec "$pod" -c "$container" -- \
    "$interpreter" -c 'import torch; assert torch.cuda.is_available(); print(torch.cuda.get_device_name(0))' 2>&1); then
    printf '%s\n' "$out" | sed '/^[[:space:]]*$/d' | tail -n1
    return 0
  fi
  return 1
}

accept_gpu_name() {
  local pod=$1 gpu_name=$2
  printf '%s\n' "$gpu_name" | grep -q 'NVIDIA L4' \
    || die "unexpected GPU in $pod (expected NVIDIA L4): $gpu_name"
  printf '%s\n' "$gpu_name"
}

# Direct pod: the pytorch:2.2.0 image ships torch, so the system python3 IS
# the application's interpreter.
direct_gpu=$(cuda_torch_probe "$direct_pod" server python3) \
  || die "CUDA unavailable in $direct_pod (torch.cuda.is_available() failed with the image's own python3)"
direct_gpu=$(accept_gpu_name "$direct_pod" "$direct_gpu") \
  || exit 1
info "$direct_pod (server): $direct_gpu"

worker_gpu=""
worker_gpu_source=""
if worker_gpu=$(cuda_torch_probe "$worker_pod" ray-gpu-worker python3); then
  worker_gpu_source="system python3"
else
  # Enumerate the runtime_env pip venvs Ray has materialized in the worker's
  # session dir and try each interpreter until one imports torch.
  runtime_pythons=$("$KUBECTL" -n "$NS" exec "$worker_pod" -c ray-gpu-worker -- \
    bash -c 'ls -d /tmp/ray/session_latest/runtime_resources/pip/*/bin/python 2>/dev/null' 2>&1 || true)
  runtime_pythons=$(printf '%s\n' "$runtime_pythons" | sed '/^[[:space:]]*$/d')
  for candidate in $runtime_pythons; do
    if worker_gpu=$(cuda_torch_probe "$worker_pod" ray-gpu-worker "$candidate"); then
      worker_gpu_source="runtime_env $candidate"
      break
    fi
  done
fi
if [ -z "$worker_gpu" ]; then
  # No torch-capable interpreter anywhere in the worker. Fall back to the
  # design's documented alternative: the node's GPU name via nvidia-smi plus
  # the SERVING APPLICATION's own /readiness response asserting CUDA is live.
  smi=$("$KUBECTL" -n "$NS" exec "$worker_pod" -c ray-gpu-worker -- \
    nvidia-smi --query-gpu=name --format=csv,noheader 2>&1) \
    || die "no interpreter in $worker_pod could import torch and nvidia-smi failed: $smi"
  smi=$(printf '%s\n' "$smi" | sed '/^[[:space:]]*$/d' | tail -n1)
  printf '%s\n' "$smi" | grep -q 'NVIDIA L4' \
    || die "no torch-capable interpreter in $worker_pod and nvidia-smi reports '$smi' (expected NVIDIA L4)"
  readiness_cuda=$("$KUBECTL" -n "$NS" exec "$worker_pod" -c ray-gpu-worker -- \
    python3 -c '
import json
import urllib.request
body = json.loads(urllib.request.urlopen("http://127.0.0.1:8000/readiness", timeout=5).read())
if body.get("cuda") is not True:
    raise SystemExit("readiness reports cuda=%r" % (body.get("cuda"),))
print("ok")
' 2>&1) \
    || die "no torch-capable interpreter in $worker_pod; nvidia-smi saw an L4 but the app readiness check failed: $readiness_cuda"
  readiness_cuda=$(printf '%s\n' "$readiness_cuda" | sed '/^[[:space:]]*$/d' | tail -n1)
  [ "$readiness_cuda" = "ok" ] \
    || die "app readiness did not confirm cuda=true in $worker_pod: $readiness_cuda"
  worker_gpu="NVIDIA L4"
  worker_gpu_source="nvidia-smi + app readiness (cuda=true); no torch-capable interpreter found"
fi
# Defense in depth: re-apply the L4 expectation to whichever worker path ran.
worker_gpu=$(accept_gpu_name "$worker_pod" "$worker_gpu") \
  || exit 1
info "$worker_pod (ray-gpu-worker): $worker_gpu (via $worker_gpu_source)"
ok "the serving application sees an NVIDIA L4 with CUDA available in the worker and direct pods"

# ---------------------------------------------------------------------------
step "HAProxy binary"
# The head and the replica-hosting worker must both resolve a haproxy binary.
# A missing binary is a hard stop: without it Serve would fall back to the
# Python proxy and the HAProxy-arm measurements would be invalid.
for pod in "$head_pod" "$worker_pod"; do
  pod_container=ray-gpu-worker
  if [ "$pod" = "$head_pod" ]; then
    pod_container=ray-head
  fi
  # Always exec an explicit container: implicit defaulting makes kubectl
  # print an advisory on stderr for the multi-container Ray pods.
  # The stock Ray image has no haproxy on $PATH; the binary ships inside the
  # ray_haproxy wheel. Resolve its path inside the pod, either from PATH or
  # from the wheel, and exec that path. No anaconda path is hardcoded.
  haproxy_bin=$("$KUBECTL" -n "$NS" exec "$pod" -c "$pod_container" -- bash -lc \
    'command -v haproxy || python3 -c "import ray_haproxy, pathlib; print(pathlib.Path(ray_haproxy.__file__).parent / \"bin\" / \"haproxy\")"') \
    || die "no haproxy binary found in $pod (not on PATH and ray_haproxy wheel not importable); HAProxy arm is invalid, stop and escalate"
  haproxy_bin=$(printf '%s\n' "$haproxy_bin" | sed '/^[[:space:]]*$/d' | tail -n1 | tr -d '[:space:]')
  [ -n "$haproxy_bin" ] \
    || die "could not resolve the haproxy binary path in $pod; HAProxy arm is invalid, stop and escalate"
  haproxy_version=$("$KUBECTL" -n "$NS" exec "$pod" -c "$pod_container" -- "$haproxy_bin" -v 2>&1) \
    || die "haproxy -v failed in $pod at $haproxy_bin: $haproxy_version"
  printf '%s\n' "$haproxy_version" | grep -q 'HAProxy version 2\.8\.' \
    || die "expected HAProxy 2.8.x in $pod, got: $haproxy_version"
  info "$pod ($pod_container): $haproxy_bin -> $(printf '%s\n' "$haproxy_version" | head -n1)"
done
ok "HAProxy 2.8.x present on head and GPU worker"

# ---------------------------------------------------------------------------
step "Serve proxy actors"
actors=$("$KUBECTL" -n "$NS" exec "$head_pod" -c ray-head -- ray list actors 2>&1) \
  || die "ray list actors failed in $head_pod: $actors"
# Ray's actor table intentionally retains DEAD actor records: every Serve
# redeploy (including in-place serveConfigV2 updates) restarts each node's
# proxy manager and leaves the old DEAD tombstone row next to its ALIVE
# replacement. Tombstones must NOT fail the count — only ALIVE rows prove
# the live proxy fleet. The listing's state column is exactly ALIVE or DEAD.
live_actors=$(printf '%s\n' "$actors" | grep -w ALIVE || true)
dead_tombstones=$(printf '%s\n' "$actors" | grep 'DEAD' | grep -ci 'proxy' || true)
haproxy_actors=$(printf '%s\n' "$live_actors" | grep -c 'HAProxyManager' || true)
# Ray 2.60 always runs a fallback Python proxy (RAY_SERVE_FALLBACK_PROXY,
# :8500) alongside HAProxy as a safety net. It is by design, never in the
# measured path (ray-ha-svc selects worker pods only, and the head's :8000 is
# held by HAProxy), and it can outlive the first deployment if HAProxy wins
# the initial port race. Tolerate it, but log its presence at info level so
# the evidence shows it exists. Any OTHER (non-fallback) Python proxy actor
# genuinely means Serve did not place HAProxy everywhere, and dies.
fallback_proxies=$(printf '%s\n' "$live_actors" | grep -i 'proxy' | grep -vi 'HAProxyManager' | grep -i 'fallback' | grep -c . || true)
python_proxies=$(printf '%s\n' "$live_actors" | grep -i 'proxy' | grep -vi 'HAProxyManager' | grep -vi 'fallback' | grep -c . || true)
info "ALIVE HAProxyManager actors: $haproxy_actors"
info "non-fallback Python proxy actors: $python_proxies (head fallback excluded)"
info "Ray 2.60 fallback Python proxy actors present (tolerated by design): $fallback_proxies"
info "dead actor tombstones excluded: $dead_tombstones"
[ "$haproxy_actors" -eq "$EXPECTED_HAPROXY_MANAGERS" ] \
  || die "expected $EXPECTED_HAPROXY_MANAGERS ALIVE HAProxyManager actors (head + the replica-hosting worker), found $haproxy_actors"
[ "$python_proxies" -eq "$EXPECTED_PYTHON_PROXIES" ] \
  || die "expected $EXPECTED_PYTHON_PROXIES non-fallback Python proxy actors, found $python_proxies; Serve did not place HAProxy everywhere"
ok "2 ALIVE HAProxyManager actors (head + worker) and 0 non-fallback Python proxies (Ray 2.60 fallback and DEAD tombstones excluded)"

# ---------------------------------------------------------------------------
# The stock Ray image has no ss/iproute2; read the listening ports from
# /proc/net/tcp{,6} instead. Output: space-separated decimal ports, sorted.
scan_listening() {
  local pod=$1 container=$2 out
  out=$("$KUBECTL" -n "$NS" exec "$pod" -c "$container" -- python3 -c '
ports = set()
for path in ("/proc/net/tcp", "/proc/net/tcp6"):
    try:
        with open(path) as handle:
            rows = handle.readlines()[1:]
    except OSError:
        continue
    for row in rows:
        parts = row.split()
        if len(parts) > 3 and parts[3] == "0A":
            ports.add(int(parts[1].rsplit(":", 1)[1], 16))
print(" ".join(str(port) for port in sorted(ports)))
' 2>&1) \
    || die "could not scan listening ports in $pod via /proc/net/tcp: $out"
  out=$(printf '%s\n' "$out" | sed '/^[[:space:]]*$/d' | tail -n1)
  [ -n "$out" ] \
    || die "no listening ports reported in $pod (/proc/net/tcp scan returned nothing)"
  printf '%s\n' "$out"
}

step "Listener ports on direct and worker pods"
direct_listen=$(scan_listening "$direct_pod" server)
case " $direct_listen " in
  *" 8000 "*) ;;
  *) die "no listener on :8000 in $direct_pod (listening: $direct_listen)" ;;
esac
info "$direct_pod (server): :8000 present; listening: $direct_listen"

worker_listen=$(scan_listening "$worker_pod" ray-gpu-worker)
case " $worker_listen " in
  *" 8000 "*) ;;
  *) die "no :8000 HAProxy listener in $worker_pod (listening: $worker_listen)" ;;
esac
worker_direct_ports=$(printf '%s\n' "$worker_listen" \
  | tr ' ' '\n' \
  | grep -E '^(30[0-9]{3})$' \
  | sort -u \
  | tr '\n' ' ' \
  | sed 's/ *$//' \
  || true)
[ -n "$worker_direct_ports" ] || die "no replica direct-ingress :300xx listener in $worker_pod (listening: $worker_listen)"
# Replica direct-ingress ports allocate sequentially per replica life (30000
# on first boot; redeploys get 30001+) and are never reused — the uniqueness
# of the listener is the invariant, not the literal value.
worker_direct_port_count=$(printf '%s\n' "$worker_direct_ports" | wc -w | tr -d ' ')
[ "$worker_direct_port_count" -eq 1 ] \
  || die "expected exactly one distinct replica direct-ingress :300xx listener in $worker_pod (single replica), found: $worker_direct_ports"
info "worker replica direct-ingress port: $worker_direct_ports"
info "$worker_pod (ray-gpu-worker): :8000 (HAProxy) and :$worker_direct_ports (replica direct ingress) present"
ok "direct pod listens on :8000; worker listens on :8000 and one :300xx replica ingress"

# ---------------------------------------------------------------------------
step "Live Serve env capture on head and worker"
for pod in "$head_pod" "$worker_pod"; do
  pod_container=ray-gpu-worker
  if [ "$pod" = "$head_pod" ]; then
    pod_container=ray-head
  fi
  # Capture the LIVE container env, not the manifest: prove the values the
  # proxies actually run with. printenv prints one variable per line; keep
  # digits-only lines so stray advisories can never satisfy the check.
  live_env=$("$KUBECTL" -n "$NS" exec "$pod" -c "$pod_container" \
    -- printenv RAY_SERVE_ENABLE_HA_PROXY RAY_SERVE_THROUGHPUT_OPTIMIZED 2>&1) \
    || die "printenv failed in $pod: $live_env"
  live_env=$(printf '%s\n' "$live_env" | grep -E '^[01]$' || true)
  enable_flag=$(printf '%s\n' "$live_env" | sed -n 1p)
  throughput_flag=$(printf '%s\n' "$live_env" | sed -n 2p)
  [ "$enable_flag" = "1" ] \
    || die "$pod has RAY_SERVE_ENABLE_HA_PROXY='$enable_flag' (or unset), expected 1"
  [ "$throughput_flag" = "1" ] \
    || die "$pod has RAY_SERVE_THROUGHPUT_OPTIMIZED='$throughput_flag' (or unset), expected 1"
  info "$pod ($pod_container): RAY_SERVE_ENABLE_HA_PROXY=$enable_flag RAY_SERVE_THROUGHPUT_OPTIMIZED=$throughput_flag"
done
ok "RAY_SERVE_ENABLE_HA_PROXY=1 and RAY_SERVE_THROUGHPUT_OPTIMIZED=1 live on head and worker"

# ---------------------------------------------------------------------------
step "Service endpoints and targetPorts"
require_target_port() {
  local svc=$1 want=$2 actual
  actual=$("$KUBECTL" -n "$NS" get svc "$svc" -o jsonpath='{.spec.ports[0].targetPort}') \
    || die "could not read Service $svc"
  [ "$actual" = "$want" ] || die "Service $svc targetPort=$actual, expected $want"
  info "$svc targetPort=$actual"
}

require_endpoints() {
  local svc=$1 want=$2 expected_names=$3 eps count ep
  eps=$("$KUBECTL" -n "$NS" get endpoints "$svc" \
    -o jsonpath='{range .subsets[*]}{range .addresses[*]}{.targetRef.name}{"\n"}{end}{end}') \
    || die "could not read endpoints for Service $svc"
  eps=$(printf '%s\n' "$eps" | sed '/^$/d' | sort)
  count=$(count_lines "$eps")
  [ "$count" -eq "$want" ] \
    || die "Service $svc has $count endpoints, expected $want: ${eps:-<none>}"
  for ep in $eps; do
    printf '%s\n' "$expected_names" | grep -qx "$ep" \
      || die "Service $svc endpoint $ep is not one of the expected pods"
  done
  info "$svc endpoints ($count): $(printf '%s\n' "$eps" | tr '\n' ' ' | sed 's/ *$//')"
}

require_target_port "$HAPROXY_SERVICE" 8000
require_endpoints "$HAPROXY_SERVICE" "$EXPECTED_WORKERS" "$worker_pods"
require_target_port "$DIRECT_INGRESS_SERVICE" 8000
require_endpoints "$DIRECT_INGRESS_SERVICE" "$EXPECTED_DIRECT_PODS" "$direct_pods"
ok "all Services have the expected targetPorts and endpoints"

# ---------------------------------------------------------------------------
step "Serve configuration and replica health"
serve_config=$("$KUBECTL" -n "$NS" get rayservice "$RAYSERVICE" \
  -o jsonpath='{.spec.serveConfigV2}') \
  || die "could not read RayService $RAYSERVICE"
printf '%s\n' "$serve_config" | grep -q 'max_ongoing_requests: 1000' \
  || die "RayService $RAYSERVICE serveConfigV2 does not set max_ongoing_requests: 1000"
info "serveConfigV2 sets max_ongoing_requests: 1000"

serve_status=$("$KUBECTL" -n "$NS" exec "$head_pod" -c ray-head -- ray serve status 2>&1) \
  || die "ray serve status failed in $head_pod: $serve_status"
printf '%s\n' "$serve_status" | grep -q 'EmbeddingDeployment' \
  || die "ray serve status does not report EmbeddingDeployment"
deployment_healthy=$(printf '%s\n' "$serve_status" | awk '
  function indent(line,    copy) {
    copy = line
    sub(/[^ ].*$/, "", copy)
    return length(copy)
  }
  /^[[:space:]]*EmbeddingDeployment:[[:space:]]*$/ {
    dep_indent = indent($0)
    in_dep = 1
    next
  }
  in_dep {
    if ($0 ~ /^[[:space:]]*$/) next
    if (indent($0) <= dep_indent) exit
    if ($0 ~ /^[[:space:]]*status:[[:space:]]*HEALTHY[[:space:]]*$/) {
      found = 1
      exit
    }
  }
  END { print found + 0 }
')
[ "$deployment_healthy" -eq 1 ] \
  || die "ray serve status does not report EmbeddingDeployment as HEALTHY"
printf '%s\n' "$serve_status" | grep -q 'status: RUNNING' \
  || die "ray serve status does not report a RUNNING application"
running_replicas=$(printf '%s\n' "$serve_status" | awk '
  /replica_states:/ { in_states = 1; next }
  in_states && /^[[:space:]]+[A-Z_]+:[[:space:]]*[0-9]+[[:space:]]*$/ {
    split($0, kv, ":")
    if (kv[1] ~ /RUNNING/) total += kv[2]
    next
  }
  in_states { in_states = 0 }
  END { print total + 0 }
')
[ "$running_replicas" -eq "$EXPECTED_REPLICAS" ] \
  || die "expected $EXPECTED_REPLICAS RUNNING replicas, found $running_replicas"
info "RUNNING replicas: $running_replicas"
ok "Serve reports 1 healthy EmbeddingDeployment replica and max_ongoing_requests=1000"

# ---------------------------------------------------------------------------
# Runs 10 sequential /embed requests from the Direct pod (python3 + urllib).
# Every response must be HTTP 200 with a 384-dim embedding (all-MiniLM-L6-v2).
embed_10() {
  local url=$1 out
  out=$("$KUBECTL" -n "$NS" exec "$direct_pod" -c server -- python3 -c '
import json
import sys
import urllib.request

url = sys.argv[1]
payload = json.dumps({"text": "preflight probe: a short sentence to embed"}).encode()
ok = 0
for _ in range(10):
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read())
        if response.status != 200:
            raise SystemExit("HTTP status %s" % response.status)
        embedding = body.get("embedding")
        if not isinstance(embedding, list) or len(embedding) != 384:
            raise SystemExit("response embedding is not a 384-dim list: %r" % (embedding,))
        ok += 1
print(ok)
' "$url" 2>&1) || die "10 /embed requests to $url failed: $out"
  # Defense in depth: keep a digits-only line so stray advisory output can
  # never masquerade as a success count.
  out=$(printf '%s\n' "$out" | grep -E '^[0-9]+$' | tail -n1 || true)
  [ "$out" = "10" ] || die "expected 10 successful /embed requests to $url, got: ${out:-<none>}"
  info "$url: 10/10 HTTP 200 with a 384-dim embedding"
}

# Sums the HAProxy request counters across the real frontend/backend rows.
# The stats/prometheus listeners are excluded so scraping cannot fake an
# increase.
haproxy_total() {
  local pod=$1 out
  out=$("$KUBECTL" -n "$NS" exec "$pod" -c ray-gpu-worker -- python3 -c '
import urllib.request

data = urllib.request.urlopen("http://127.0.0.1:8404/stats;csv", timeout=5).read().decode()
lines = [line for line in data.splitlines() if line.strip()]
header = [name.strip() for name in lines[0].lstrip("#").split(",")]
index = {name: i for i, name in enumerate(header)}
total = 0
for line in lines[1:]:
    fields = line.split(",")
    pxname = fields[index["pxname"]] if "pxname" in index else ""
    svname = fields[index["svname"]] if "svname" in index else ""
    if pxname in ("stats", "prometheus") or svname not in ("FRONTEND", "BACKEND"):
        continue
    for column in ("req_tot", "stot"):
        if column in index and index[column] < len(fields):
            value = fields[index[column]]
            if value.isdigit():
                total += int(value)
                break
print(total)
' 2>&1) || die "HAProxy stats scrape failed in $pod: $out"
  # Defense in depth: keep digits-only lines so stray advisory output can never
  # reach the arithmetic in the caller.
  out=$(printf '%s\n' "$out" | grep -E '^[0-9]+$' | tail -n1 || true)
  [ -n "$out" ] || die "HAProxy stats scrape returned no numeric counter total in $pod"
  printf '%s\n' "$out"
}

# ---------------------------------------------------------------------------
step "HAProxy is in the request path ($HAPROXY_SERVICE)"
stats_before=$(haproxy_total "$worker_pod")
info "HAProxy request counters before: $stats_before"
embed_10 "http://$HAPROXY_SERVICE:8000/embed"
stats_after=$(haproxy_total "$worker_pod")
info "HAProxy request counters after: $stats_after"
# 10 accepted requests must each traverse the HAProxy frontend, so the
# counter total has to grow by at least 10.
[ "$stats_after" -ge $((stats_before + 10)) ] \
  || die "HAProxy request counters did not increase by at least 10 ($stats_before -> $stats_after); HAProxy is not in the request path"
ok "HAProxy request counters increased by at least 10 ($stats_before -> $stats_after)"

# ---------------------------------------------------------------------------
step "Functional /embed requests"
embed_10 "http://$DIRECT_INGRESS_SERVICE:8000/embed"
ok "10/10 HTTP 200 with 384-dim embeddings for every Service"

printf '\n[preflight:%s] PASS\n' "$MODE"
