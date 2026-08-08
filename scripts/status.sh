#!/usr/bin/env bash
#
# What the controller thinks the world looks like, and what is actually running.
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-temporal-demo}"
WD_NAME="${WORKER_DEPLOYMENT:-greeting-worker}"

echo "=== WorkerDeployment ==="
kubectl get workerdeployment "$WD_NAME" -n "$NAMESPACE"

echo
echo "=== Conditions ==="
kubectl get workerdeployment "$WD_NAME" -n "$NAMESPACE" \
  -o jsonpath='{range .status.conditions[*]}  {.type}={.status}  {.reason}: {.message}{"\n"}{end}'

echo
echo "=== Versions known to the controller ==="
status_json="$(mktemp)"
trap 'rm -f "$status_json"' EXIT
kubectl get workerdeployment "$WD_NAME" -n "$NAMESPACE" -o json > "$status_json"

python3 - "$status_json" <<'PY'
import json, sys

status = json.load(open(sys.argv[1])).get("status", {})
target = status.get("targetVersion") or {}
current = status.get("currentVersion") or {}

rows = []
if current.get("buildID"):
    rows.append((current["buildID"], "current", current.get("status", "")))
if target.get("buildID") and target.get("buildID") != current.get("buildID"):
    ramp = target.get("rampPercentage") or 0
    rows.append((target["buildID"], "target", f'{target.get("status", "")} (ramp {ramp}%)'))
# Deprecated versions no longer take new executions. The controller scales them
# down and deletes them once Temporal reports them drained, per spec.sunset.
for dep in status.get("deprecatedVersions") or []:
    rows.append((dep.get("buildID", "?"), "deprecated", dep.get("status", "")))

print(f'  {"BUILD ID":<28} {"ROLE":<11} STATUS')
for build_id, role, state in rows:
    print(f"  {build_id:<28} {role:<11} {state}")
PY

echo
echo "=== Pods (one Deployment per version, created by the controller) ==="
kubectl get deployments -n "$NAMESPACE" -l "temporal.io/deployment-name=${WD_NAME}" \
  -L temporal.io/build-id
