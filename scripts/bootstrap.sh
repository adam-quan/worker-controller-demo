#!/usr/bin/env bash
#
# One-time setup: minikube, a Temporal dev server, and the Temporal Worker
# Controller. Safe to re-run -- every step is idempotent.
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-temporal-demo}"
CONTROLLER_NAMESPACE="${CONTROLLER_NAMESPACE:-temporal-system}"
CONTROLLER_VERSION="${CONTROLLER_VERSION:-0.27.1}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.20.0}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! minikube status >/dev/null 2>&1; then
  echo "==> Starting minikube"
  minikube start --cpus=4 --memory=4096
fi

# cert-manager issues the TLS certificate the controller's webhook uses.
if ! kubectl get deployment -n cert-manager cert-manager-webhook >/dev/null 2>&1; then
  echo "==> Installing cert-manager ${CERT_MANAGER_VERSION}"
  helm repo add jetstack https://charts.jetstack.io >/dev/null 2>&1 || true
  helm repo update jetstack >/dev/null
  helm upgrade --install cert-manager jetstack/cert-manager \
    --namespace cert-manager --create-namespace \
    --version "$CERT_MANAGER_VERSION" --set crds.enabled=true --wait
else
  echo "==> cert-manager already installed"
fi

echo "==> Installing Temporal Worker Controller ${CONTROLLER_VERSION}"
# CRDs ship as their own chart so that they can be upgraded independently.
helm upgrade --install temporal-worker-controller-crds \
  oci://docker.io/temporalio/temporal-worker-controller-crds \
  --version "$CONTROLLER_VERSION" \
  --namespace "$CONTROLLER_NAMESPACE" --create-namespace --wait

helm upgrade --install temporal-worker-controller \
  oci://docker.io/temporalio/temporal-worker-controller \
  --version "$CONTROLLER_VERSION" \
  --namespace "$CONTROLLER_NAMESPACE" --wait

echo "==> Deploying Temporal server"
kubectl apply -f "${REPO_ROOT}/k8s/temporal-server.yaml"
kubectl rollout status -n "$NAMESPACE" deployment/temporal --timeout=300s

echo "==> Registering the Temporal connection"
kubectl apply -f "${REPO_ROOT}/k8s/connection.yaml"

cat <<'EOF'

Cluster is ready.

To reach Temporal from this machine (UI on :8233, gRPC on :7233):

    ./scripts/port-forward.sh

Then deploy the first worker version:

    ./scripts/deploy.sh v1
EOF
