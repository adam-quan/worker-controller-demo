#!/usr/bin/env bash
#
# Forward the in-cluster Temporal server to localhost so you can run
# worker/starter.py and open the Web UI from this machine.
#
#   Web UI:  http://localhost:8233
#   gRPC:    localhost:7233   (the SDK default, so no TEMPORAL_ADDRESS needed)
#
# Runs in the foreground; Ctrl-C to stop.
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-temporal-demo}"

echo "Web UI:  http://localhost:8233"
echo "gRPC:    localhost:7233"
echo
exec kubectl port-forward -n "$NAMESPACE" service/temporal 7233:7233 8233:8233
