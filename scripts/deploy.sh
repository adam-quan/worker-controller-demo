#!/usr/bin/env bash
#
# Build the worker image and point the WorkerDeployment resource at it.
#
#   scripts/deploy.sh [image-tag]     (default: short Git SHA)
#
# That is the entire deployment. Everything after `kubectl apply` -- deriving a
# Build ID, creating a versioned Deployment, waiting for pollers, running the
# gate workflow, ramping traffic, promoting to Current, sunsetting the old
# version -- is done by the Temporal Worker Controller, not by this script.
#
# The rest of the file is just waiting for the controller and reporting what it
# did, so that CI fails when a rollout does.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${K8S_NAMESPACE:-temporal-demo}"
WD_NAME="${WORKER_DEPLOYMENT:-greeting-worker}"
TIMEOUT="${ROLLOUT_TIMEOUT_SECONDS:-900}"

IMAGE_TAG="${1:-$(git -C "$REPO_ROOT" rev-parse --short=7 HEAD)}"
export IMAGE="worker-versioning-demo:${IMAGE_TAG}"

wd() { kubectl get workerdeployment "$WD_NAME" -n "$NAMESPACE" "$@"; }
field() { wd -o jsonpath="{$1}" 2>/dev/null || true; }

echo "==> Building ${IMAGE} inside minikube's Docker daemon"
eval "$(minikube docker-env)"
docker build -t "${IMAGE}" "${REPO_ROOT}/worker"

echo "==> Pointing WorkerDeployment/${WD_NAME} at ${IMAGE}"
# The only change CI makes. A new image means a new pod template, which means a
# new Build ID, which is what triggers the controller to roll out a version.
envsubst < "${REPO_ROOT}/k8s/workerdeployment.template.yaml" | kubectl apply -f -

echo "==> Waiting for the controller to roll out the new version (timeout ${TIMEOUT}s)"
deadline=$(( SECONDS + TIMEOUT ))
last=""
while true; do
  target="$(field .status.targetVersion.buildID)"
  current="$(field .status.currentVersion.buildID)"
  ramp="$(field .status.targetVersion.rampPercentage)"
  reason="$(field '.status.conditions[?(@.type=="Progressing")].reason')"

  # The target version becoming the current version is the definition of done.
  if [[ -n "$target" && "$target" == "$current" ]]; then
    echo "    rollout complete: ${current} is Current"
    break
  fi

  line="target=${target:-<pending>} current=${current:-<none>} ramp=${ramp:-0}% ${reason:+(${reason})}"
  [[ "$line" != "$last" ]] && echo "    ${line}" && last="$line"

  if (( SECONDS > deadline )); then
    echo
    echo "ERROR: rollout did not complete within ${TIMEOUT}s." >&2
    echo "A stalled rollout usually means the gate workflow failed or the new" >&2
    echo "pods never became ready. Conditions:" >&2
    wd -o jsonpath='{range .status.conditions[*]}  {.type}={.status} {.reason}: {.message}{"\n"}{end}' >&2
    echo >&2
    echo "Gate workflow executions (look for a failed RolloutGate):" >&2
    kubectl exec -n "$NAMESPACE" deploy/temporal -- temporal --address localhost:7233 \
      workflow list --query "WorkflowType='RolloutGate'" 2>/dev/null | head -10 >&2 || true
    exit 1
  fi
  sleep 5
done

echo
"${REPO_ROOT}/scripts/status.sh"
