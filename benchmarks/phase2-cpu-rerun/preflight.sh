#!/usr/bin/env bash
# Preflight gate for the Phase 2 HAProxy rerun.
#
# Usage:
#   NS=model-serving-benchmark KUBECTL=kubectl \
#     bash benchmarks/phase2-cpu-rerun/preflight.sh haproxy|python|local
#
# The expected provider zone defaults to us-west-2a; override with EXPECTED_ZONE
# if the cluster's single-AZ placement ever moves again.
#
# The script proves the cluster topology, Ray version, HAProxy/proxy placement,
# listener ports, Service endpoints, and Serve health before any load is run.
# It exits non-zero on the first failed check and writes its evidence to stdout.
set -euo pipefail

MODE=${1:-}
NS=${NS:-model-serving-benchmark}
KUBECTL=${KUBECTL:-kubectl}
EXPECTED_ZONE=${EXPECTED_ZONE:-us-west-2a}

usage() {
  echo "usage: $0 haproxy|python|local" >&2
  exit 2
}

case "$MODE" in
  haproxy)
    ARM=haproxy
    RAYSERVICE=ray-haproxy-rerun
    EXPECTED_WORKERS=2
    EXPECTED_REPLICAS=2
    EXPECTED_HAPROXY_MANAGERS=3
    EXPECTED_PYTHON_PROXIES=""
    CHECK_HAPROXY_BINARY=1
    HAPROXY_SERVICE=ray-haproxy-svc
    DIRECT_INGRESS_SERVICE=ray-direct-ingress-svc
    WORKER_SELECTOR="ray.io/node-type=worker,bench-arm=haproxy"
    ;;
  python)
    ARM=python-proxy
    RAYSERVICE=ray-python-proxy-rerun
    EXPECTED_WORKERS=2
    EXPECTED_REPLICAS=2
    EXPECTED_HAPROXY_MANAGERS=0
    EXPECTED_PYTHON_PROXIES=3
    CHECK_HAPROXY_BINARY=0
    HAPROXY_SERVICE=""
    DIRECT_INGRESS_SERVICE=""
    WORKER_SELECTOR="ray.io/node-type=worker,bench-arm=python-proxy"
    ;;
  local)
    ARM=local
    RAYSERVICE=ray-haproxy-local-rerun
    EXPECTED_WORKERS=1
    EXPECTED_REPLICAS=1
    EXPECTED_HAPROXY_MANAGERS=2
    EXPECTED_PYTHON_PROXIES=""
    CHECK_HAPROXY_BINARY=1
    HAPROXY_SERVICE=ray-haproxy-w1-svc
    DIRECT_INGRESS_SERVICE=ray-direct-w1-svc
    WORKER_SELECTOR="ray.io/node-type=worker,bench-arm=local,bench-slot=w1"
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
for role in head worker loadgen; do
  role_count=$("$KUBECTL" get nodes -l "bench-role=$role" -o name | wc -l | tr -d ' ')
  [ "$role_count" -ge 1 ] || die "no node with bench-role=$role"
  info "bench-role=$role: $role_count node(s)"
done
worker_node_count=$("$KUBECTL" get nodes -l bench-role=worker -o name | wc -l | tr -d ' ')
[ "$worker_node_count" -eq 2 ] || die "expected 2 bench-role=worker nodes, found $worker_node_count"
node_report=$("$KUBECTL" get nodes -o go-template='{{range .items}}{{.metadata.name}}{{"\t"}}{{index .metadata.labels "node.kubernetes.io/instance-type"}}{{"\t"}}{{index .metadata.labels "topology.kubernetes.io/zone"}}{{"\n"}}{{end}}') \
  || die "could not read node labels"
while IFS=$'\t' read -r name instance_type zone; do
  [ -n "$name" ] || continue
  [ "$instance_type" = "c6i.2xlarge" ] || die "node $name instance-type=$instance_type, expected c6i.2xlarge"
  [ "$zone" = "$EXPECTED_ZONE" ] || die "node $name zone=$zone, expected $EXPECTED_ZONE"
  info "$name $instance_type $zone"
done <<< "$node_report"
ok "4 nodes, one head / two workers / one loadgen, all c6i.2xlarge in $EXPECTED_ZONE"

# ---------------------------------------------------------------------------
step "Pods for arm $ARM"
head_pods=$(pods_for "ray.io/node-type=head,bench-arm=$ARM")
worker_pods=$(pods_for "$WORKER_SELECTOR")
direct_pods=$(pods_for "app=direct-serving-rerun")
head_count=$(count_lines "$head_pods")
worker_count=$(count_lines "$worker_pods")
direct_count=$(count_lines "$direct_pods")
[ "$head_count" -eq 1 ] || die "expected 1 Ray head pod for arm $ARM, found $head_count: ${head_pods:-<none>}"
[ "$worker_count" -eq "$EXPECTED_WORKERS" ] || die "expected $EXPECTED_WORKERS Ray worker pod(s) for arm $ARM, found $worker_count: ${worker_pods:-<none>}"
[ "$direct_count" -eq 2 ] || die "expected 2 direct-serving-rerun pods, found $direct_count: ${direct_pods:-<none>}"
head_pod=$(printf '%s\n' "$head_pods" | head -n1)
direct_pod=$(printf '%s\n' "$direct_pods" | head -n1)
info "head: $head_pod"
info "workers:$(printf ' %s' $worker_pods)"
info "direct client pod: $direct_pod"
ok "RayService $RAYSERVICE pods and Direct baseline pods are present"
info "head proxy is present but is not a Service endpoint (Services select ray.io/node-type=worker)"

# ---------------------------------------------------------------------------
step "Ray version"
ray_version=$("$KUBECTL" -n "$NS" exec "$head_pod" -c ray-head -- ray --version 2>&1) \
  || die "ray --version failed in $head_pod: $ray_version"
printf '%s\n' "$ray_version" | grep -q '2\.60\.0' \
  || die "expected Ray 2.60.0, got: $ray_version"
info "$ray_version"
ok "Ray 2.60.0"

# ---------------------------------------------------------------------------
if [ "$CHECK_HAPROXY_BINARY" -eq 1 ]; then
  step "HAProxy binary"
  for pod in "$head_pod" $worker_pods; do
    # Always exec an explicit container: implicit defaulting makes kubectl
    # print an advisory on stderr for the multi-container Ray pods.
    pod_container=ray-worker
    if [ "$pod" = "$head_pod" ]; then
      pod_container=ray-head
    fi
    # The stock Ray image has no haproxy on $PATH; the binary ships inside the
    # ray_haproxy wheel. Resolve its path inside the pod, either from PATH or
    # from the wheel, and exec that path. No anaconda path is hardcoded.
    haproxy_bin=$("$KUBECTL" -n "$NS" exec "$pod" -c "$pod_container" -- bash -lc \
      'command -v haproxy || python3 -c "import ray_haproxy, pathlib; print(pathlib.Path(ray_haproxy.__file__).parent / \"bin\" / \"haproxy\")"') \
      || die "no haproxy binary found in $pod (not on PATH and ray_haproxy wheel not importable)"
    haproxy_bin=$(printf '%s\n' "$haproxy_bin" | sed '/^[[:space:]]*$/d' | tail -n1 | tr -d '[:space:]')
    [ -n "$haproxy_bin" ] \
      || die "could not resolve the haproxy binary path in $pod"
    haproxy_version=$("$KUBECTL" -n "$NS" exec "$pod" -c "$pod_container" -- "$haproxy_bin" -v 2>&1) \
      || die "haproxy -v failed in $pod at $haproxy_bin: $haproxy_version"
    printf '%s\n' "$haproxy_version" | grep -q 'HAProxy version 2\.8\.' \
      || die "expected HAProxy 2.8.x in $pod, got: $haproxy_version"
    info "$pod: $haproxy_bin -> $(printf '%s\n' "$haproxy_version" | head -n1)"
  done
  ok "HAProxy 2.8.x on head and workers"
fi

# ---------------------------------------------------------------------------
step "Serve proxy actors"
actors=$("$KUBECTL" -n "$NS" exec "$head_pod" -c ray-head -- ray list actors 2>&1) \
  || die "ray list actors failed in $head_pod: $actors"
haproxy_actors=$(printf '%s\n' "$actors" | grep -c 'HAProxyManager' || true)
python_proxies=$(printf '%s\n' "$actors" | grep -ci 'proxy' || true)
info "HAProxyManager actors: $haproxy_actors"
info "proxy actors (grep -i proxy): $python_proxies"
[ "$haproxy_actors" -eq "$EXPECTED_HAPROXY_MANAGERS" ] \
  || die "expected $EXPECTED_HAPROXY_MANAGERS HAProxyManager actors, found $haproxy_actors"
if [ -n "$EXPECTED_PYTHON_PROXIES" ]; then
  [ "$python_proxies" -eq "$EXPECTED_PYTHON_PROXIES" ] \
    || die "expected $EXPECTED_PYTHON_PROXIES Python proxy actors, found $python_proxies"
fi
ok "proxy actor count matches mode $MODE"

# ---------------------------------------------------------------------------
step "Listener ports on worker pods"
observed_direct_ports=""
for pod in $worker_pods; do
  # The stock Ray image has no ss/iproute2; read the listening ports from
  # /proc/net/tcp{,6} instead. Output: space-separated decimal ports, sorted,
  # e.g. "8000 30000".
  listening_ports=$("$KUBECTL" -n "$NS" exec "$pod" -c ray-worker -- python3 -c '
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
    || die "could not scan listening ports in $pod via /proc/net/tcp: $listening_ports"
  listening_ports=$(printf '%s\n' "$listening_ports" | sed '/^[[:space:]]*$/d' | tail -n1)
  [ -n "$listening_ports" ] \
    || die "no listening ports reported in $pod (/proc/net/tcp scan returned nothing)"
  case " $listening_ports " in
    *" 8000 "*) ;;
    *) die "no listener on :8000 in $pod (listening: $listening_ports)" ;;
  esac
  if [ -n "$DIRECT_INGRESS_SERVICE" ]; then
    direct_ports=$(printf '%s\n' "$listening_ports" \
      | tr ' ' '\n' \
      | grep -E '^(30[0-9]{3}|31000)$' \
      | sort -u \
      | tr '\n' ' ' \
      | sed 's/ *$//' \
      || true)
    [ -n "$direct_ports" ] || die "no direct-ingress :300xx listener in $pod"
    info "$pod: :8000 present; direct-ingress port set: $direct_ports"
    if [ -z "$observed_direct_ports" ]; then
      observed_direct_ports=$direct_ports
    else
      [ "$direct_ports" = "$observed_direct_ports" ] \
        || die "direct-ingress ports differ between workers: '$observed_direct_ports' vs '$direct_ports'"
    fi
  else
    info "$pod: :8000 present (direct ingress disabled in $MODE mode)"
  fi
done
if [ -n "$DIRECT_INGRESS_SERVICE" ]; then
  [ "$observed_direct_ports" = "30000" ] \
    || die "$DIRECT_INGRESS_SERVICE targets 30000 but the workers listen on: $observed_direct_ports"
  ok ":8000 and a common :300xx listener on every checked worker"
else
  ok ":8000 listener on every checked worker (no direct-ingress listener expected)"
fi

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
      || die "Service $svc endpoint $ep is not one of the expected worker pods"
  done
  info "$svc endpoints ($count): $(printf '%s\n' "$eps" | tr '\n' ' ' | sed 's/ *$//')"
}

require_target_port direct-serving-svc 8000
require_endpoints direct-serving-svc 2 "$direct_pods"

case "$MODE" in
  haproxy)
    require_target_port ray-haproxy-svc 8000
    require_endpoints ray-haproxy-svc 2 "$worker_pods"
    require_target_port ray-direct-ingress-svc 30000
    require_endpoints ray-direct-ingress-svc 2 "$worker_pods"
    ;;
  python)
    require_target_port ray-python-proxy-svc 8000
    require_endpoints ray-python-proxy-svc 2 "$worker_pods"
    ;;
  local)
    require_target_port ray-haproxy-w1-svc 8000
    require_endpoints ray-haproxy-w1-svc 1 "$worker_pods"
    require_target_port ray-direct-w1-svc 30000
    require_endpoints ray-direct-w1-svc 1 "$worker_pods"
    ;;
esac
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
printf '%s\n' "$serve_status" | grep -q 'XGBoostDeployment' \
  || die "ray serve status does not report XGBoostDeployment"
deployment_healthy=$(printf '%s\n' "$serve_status" | awk '
  function indent(line,    copy) {
    copy = line
    sub(/[^ ].*$/, "", copy)
    return length(copy)
  }
  /^[[:space:]]*XGBoostDeployment:[[:space:]]*$/ {
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
  || die "ray serve status does not report XGBoostDeployment as HEALTHY"
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
ok "Serve reports $EXPECTED_REPLICAS healthy replicas and max_ongoing_requests=1000"

# ---------------------------------------------------------------------------
# Runs 10 sequential /infer requests from the Direct pod (python3 + urllib).
infer_10() {
  local url=$1 out
  out=$("$KUBECTL" -n "$NS" exec "$direct_pod" -c server -- python3 -c '
import json
import sys
import urllib.request

url = sys.argv[1]
payload = json.dumps({"features": [0.0] * 50}).encode()
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
        if not isinstance(body.get("prediction"), (int, float)):
            raise SystemExit("response missing numeric prediction: %r" % body)
        ok += 1
print(ok)
' "$url" 2>&1) || die "10 /infer requests to $url failed: $out"
  [ "$out" = "10" ] || die "expected 10 successful /infer requests to $url, got: $out"
  info "$url: 10/10 HTTP 200 with a numeric prediction"
}

# Sums the HAProxy request counters across the real frontend/backend rows.
# The stats/prometheus listeners are excluded so scraping cannot fake an increase.
haproxy_total() {
  local pod=$1 out
  out=$("$KUBECTL" -n "$NS" exec "$pod" -c ray-worker -- python3 -c '
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

if [ -n "$HAPROXY_SERVICE" ]; then
  step "HAProxy is in the request path ($HAPROXY_SERVICE)"
  stats_before=""
  for pod in $worker_pods; do
    stats_before="$stats_before $(haproxy_total "$pod")"
  done
  info "HAProxy request counters before:$(printf '%s' "$stats_before" | sed 's/^/ /')"
  infer_10 "http://$HAPROXY_SERVICE:8000/infer"
  stats_after=""
  for pod in $worker_pods; do
    stats_after="$stats_after $(haproxy_total "$pod")"
  done
  info "HAProxy request counters after:$(printf '%s' "$stats_after" | sed 's/^/ /')"
  before_sum=0
  for value in $stats_before; do before_sum=$((before_sum + value)); done
  after_sum=0
  for value in $stats_after; do after_sum=$((after_sum + value)); done
  [ "$after_sum" -gt "$before_sum" ] \
    || die "HAProxy request counters did not increase ($before_sum -> $after_sum); HAProxy is not in the request path"
  ok "HAProxy request counters increased ($before_sum -> $after_sum)"
fi

# ---------------------------------------------------------------------------
step "Functional /infer requests"
case "$MODE" in
  haproxy)
    infer_10 "http://ray-direct-ingress-svc:8000/infer"
    ;;
  python)
    infer_10 "http://ray-python-proxy-svc:8000/infer"
    ;;
  local)
    infer_10 "http://ray-direct-w1-svc:8000/infer"
    ;;
esac
infer_10 "http://direct-serving-svc:8000/infer"
ok "10/10 HTTP 200 for every Service in this mode"

printf '\n[preflight:%s] PASS\n' "$MODE"
