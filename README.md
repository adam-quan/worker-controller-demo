# Temporal Worker Versioning with the Worker Controller + GitHub Actions

A small Temporal Python application showing the whole loop:

> **push a workflow change → GitHub Actions builds an image and updates one Kubernetes resource → the [Temporal Worker Controller](https://github.com/temporalio/temporal-worker-controller) rolls out a new Worker Deployment Version in minikube → executions already running finish on the old version.**

Rolling out new workflow code is not a normal rolling update: an execution that
started on old code may need that old code for hours or weeks. Worker
Versioning keeps both versions running and lets the server decide which one
each execution belongs to. The controller is the piece that automates it.

## What CI actually does

```bash
docker build -t worker-versioning-demo:$SHA worker/
kubectl apply -f -   # WorkerDeployment, with the new image
```

That's it. Everything after that is the controller's job, driven by the
`WorkerDeployment` resource:

| Step | Who does it |
|---|---|
| Derive a Build ID | Controller (image ref + pod template hash) |
| Create a Deployment for the new version | Controller |
| Wait for its workers to poll | Controller |
| Run a smoke test on the new version | Controller (gate workflow) |
| Ramp traffic 10% → 50% → 100% | Controller |
| Promote to Current | Controller |
| Scale down and delete drained versions | Controller |

Because the policy lives in a manifest rather than in pipeline code, changing
how you deploy is a reviewable change to
[k8s/workerdeployment.template.yaml](k8s/workerdeployment.template.yaml).

## What's here

| Path | What it is |
|---|---|
| [worker/workflows.py](worker/workflows.py) | The workflows. **Edit this to trigger the demo.** |
| [worker/run_worker.py](worker/run_worker.py) | Worker entrypoint — reads its identity from the controller |
| [worker/starter.py](worker/starter.py) | CLI to start / signal / inspect workflows |
| [k8s/workerdeployment.template.yaml](k8s/workerdeployment.template.yaml) | **The rollout policy.** Steps, gate, sunset, pod template |
| [k8s/connection.yaml](k8s/connection.yaml) | How to reach Temporal |
| [k8s/temporal-server.yaml](k8s/temporal-server.yaml) | A Temporal dev server for minikube |
| [scripts/bootstrap.sh](scripts/bootstrap.sh) | minikube + cert-manager + controller + server |
| [scripts/deploy.sh](scripts/deploy.sh) | Build, apply, wait. What CI runs. |
| [scripts/status.sh](scripts/status.sh) | Versions, conditions, pods |
| [.github/workflows/deploy-worker-version.yml](.github/workflows/deploy-worker-version.yml) | The CI pipeline |

## How versioning is wired up

The worker declares nothing about *which* version it is. The controller injects
`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_DEPLOYMENT_NAME` and
`TEMPORAL_WORKER_BUILD_ID` into the pod, and
[run_worker.py](worker/run_worker.py) just registers with them:

```python
worker = Worker(
    client,
    task_queue=TASK_QUEUE,
    workflows=[GreetingWorkflow, HealthCheckWorkflow, RolloutGate],
    activities=[compose_greeting, record_result],
    deployment_config=WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(
            deployment_name=os.environ["TEMPORAL_DEPLOYMENT_NAME"],
            build_id=os.environ["TEMPORAL_WORKER_BUILD_ID"],
        ),
        use_worker_versioning=True,
    ),
)
```

Don't set those four yourself — the controller overwrites them, and a mismatch
means you register a version the controller isn't routing to.

Each workflow declares what should happen to in-flight runs when a newer
version becomes Current:

```python
@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class GreetingWorkflow: ...        # stays on its original version, forever

@workflow.defn(versioning_behavior=VersioningBehavior.AUTO_UPGRADE)
class HealthCheckWorkflow: ...     # moves to the new Current version
```

`PINNED` is the safe default: you can change the workflow's code however you
like, because runs in flight never see the change. `AUTO_UPGRADE` is for
workflows you only ever change in [deterministically compatible](https://docs.temporal.io/workflow-definition#deterministic-constraints)
ways.

### The gate workflow

Before *any* traffic moves, the controller starts `RolloutGate` — once per task
queue, **pinned to the new Build ID**, so it runs on the new code specifically:

```yaml
rollout:
  strategy: Progressive
  steps:
    - rampPercentage: 10
      pauseDuration: 60s
    - rampPercentage: 50
      pauseDuration: 60s
  gate:
    workflowType: RolloutGate
```

If the gate fails, ramping never starts and the old version stays Current — the
bad build takes **zero** real traffic. Keep the gate fast and representative;
anything it raises blocks the rollout.

## Prerequisites

`minikube`, `kubectl`, `docker`, `helm`, `envsubst` (`brew install gettext`),
and Python 3.9+. Temporal Server 1.29.1+ is required by the controller; the
bundled dev server is newer.

## Quickstart

```bash
./scripts/bootstrap.sh      # minikube, cert-manager, controller, Temporal
./scripts/deploy.sh v1      # build and roll out the first version
```

Then, in a second terminal, leave this running — Web UI on
<http://localhost:8233>, gRPC on `localhost:7233`:

```bash
./scripts/port-forward.sh
pip install -r worker/requirements.txt
```

> Use `port-forward`, not `minikube service --url`: on minikube's `docker`
> driver the node IP isn't routable from the host, and `minikube service`
> blocks holding a tunnel open.

## The demo

**1. Start a long-running workflow.** It greets, then parks on a signal.

```bash
cd worker && python starter.py greeting Alice
# started greeting-582cadfb
```

**2. Change the workflow.** In [worker/workflows.py](worker/workflows.py):

```python
GREETING = "Howdy"   # was "Hello"
```

**3. Ship it.**

```bash
git commit -am "change greeting" && git push
```

Locally, the same thing: `./scripts/deploy.sh v2`. Either way you can watch the
controller work:

```
target=v2-ddb6 current=v1-fb6c ramp=0%  (WaitingForPollers)
target=v2-ddb6 current=v1-fb6c ramp=0%  (WaitingForPromotion)   <- gate running
target=v2-ddb6 current=v1-fb6c ramp=10% (Ramping)
target=v2-ddb6 current=v1-fb6c ramp=50% (Ramping)
rollout complete: v2-ddb6 is Current
```

**4. Watch what happened.**

```bash
./scripts/status.sh
```

A *new* workflow gets the new code:

```bash
python starter.py greeting Bob
#   progress: ['Howdy, Bob! (served by build v2-ddb6)']
```

The *old* workflow, still parked, finishes on the version it started on — old
greeting, old build — even though v2 is now Current:

```bash
python starter.py approve greeting-582cadfb
# result: {'greeting': 'Hello, Alice! (served by build v1-fb6c)', 'recorded_by': 'v1-fb6c'}
```

That is Worker Versioning doing its job. Without it, that run would have picked
up the new code mid-execution and risked a non-determinism error.

**5. Nothing to clean up.** Once Alice's workflow finishes, nothing is pinned
to `v1-fb6c` any more; Temporal reports it drained and the controller scales it
to zero and deletes it, per `spec.sunset`.

### Watch a bad version get blocked

Break something the gate exercises — say, make `compose_greeting` raise — and
deploy it:

```bash
./scripts/deploy.sh v3
```

The rollout stalls at `WaitingForPromotion`, `deploy.sh` exits non-zero, and:

```
Failed     test-.../greeting-worker:v3-9965-greeting-tq  RolloutGate
```

`CURRENT` stays on the good version while `TARGET` sits at the broken one, and
new executions keep going to the good version. Recover by shipping a fix, or by
re-deploying the previous image (`./scripts/deploy.sh v2`) — the controller
treats a return to a known-good version as a rollback and applies it at once.

## Running this for real

**The runner has to reach your cluster.** minikube lives on your machine, so the
CI job uses `runs-on: self-hosted`. Register a runner on the same machine:

```bash
# from your repo: Settings → Actions → Runners → New self-hosted runner
./config.sh --url https://github.com/<you>/<repo> --token <token>
./run.sh
```

It needs `minikube`, `kubectl`, `docker`, `helm` and `envsubst` on its `PATH`,
and the cluster already bootstrapped.

On GitHub-hosted runners the shape is unchanged — swap the two
environment-specific bits: push the image to a real registry instead of
building into `minikube docker-env`, and point `kubectl` at a cluster the
runner can reach.

**Moving to Temporal Cloud / a real cluster:**

- Delete [k8s/temporal-server.yaml](k8s/temporal-server.yaml) and point
  [k8s/connection.yaml](k8s/connection.yaml) at your endpoint, adding
  `apiKeySecretRef` or `mutualTLSSecretRef`. The controller injects the
  resulting TLS/API-key settings into every worker pod.
- Use real image tags from a registry and drop `imagePullPolicy: IfNotPresent`.
- Consider raising `sunset.deleteDelay` well above the demo's `60s`, so a
  version's pods stick around long enough to debug after it drains.

## Notes and caveats

- The in-cluster Temporal server is a **dev server** (single pod, SQLite on a
  PVC). Fine for a demo; use the Helm chart or Temporal Cloud for anything real.
  It also shortens Temporal's drainage-check interval via
  `--dynamic-config-value` so sunsetting is observable in a live walkthrough —
  leave those at their defaults in production.
- Build IDs look like `v2-ddb6`: the image tag plus a hash of the pod template.
  Changing *anything* in the pod template (env vars, resources) produces a new
  version, not just a new image.
- The controller needs cert-manager for its webhook's TLS; `bootstrap.sh`
  installs it if absent.
- Version state lives in Temporal, not just Kubernetes. If you delete the
  Temporal server's PVC you will reset routing.
- **Deleting a `WorkerDeployment` is not instant.** A
  `temporal.io/delete-protection` finalizer holds the resource while the
  controller removes its versions from the Temporal server, and a version
  cannot be removed while it still has *active pollers*. So the resource sits
  in `Terminating` until its workers have stopped and the server has expired
  their poller records — minutes, not seconds. To hurry it along, scale the
  versioned Deployments to zero first:

  ```bash
  kubectl scale deployment -n temporal-demo -l temporal.io/deployment-name=greeting-worker --replicas=0
  kubectl delete workerdeployment greeting-worker -n temporal-demo
  ```

  You rarely need to delete it at all — shipping a new image rolls the existing
  resource forward, which is the normal path.

## Cleanup

```bash
kubectl delete namespace temporal-demo
helm uninstall temporal-worker-controller -n temporal-system
helm uninstall temporal-worker-controller-crds -n temporal-system
minikube stop
```
