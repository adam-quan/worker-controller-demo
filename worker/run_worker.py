"""Worker entrypoint.

Note what is *absent* here: nothing computes a Build ID, and nothing decides
which version is current. The Temporal Worker Controller injects all four of
these variables into the pod it creates:

    TEMPORAL_ADDRESS            from the referenced Connection resource
    TEMPORAL_NAMESPACE          from spec.workerOptions.temporalNamespace
    TEMPORAL_DEPLOYMENT_NAME    "<k8s-namespace>/<WorkerDeployment name>"
    TEMPORAL_WORKER_BUILD_ID    derived from the image + pod template hash

The worker's only job is to register with them. Do not set them yourself --
the controller overwrites them, and a mismatch means the version you register
is not the one the controller is routing traffic to.
"""

import asyncio
import logging
import os
import signal
from datetime import timedelta

from temporalio.client import Client
from temporalio.common import WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from activities import compose_greeting, record_result
from workflows import GreetingWorkflow

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "greeting-tq")
DEPLOYMENT_NAME = os.environ.get("TEMPORAL_DEPLOYMENT_NAME", "greeting-worker")
BUILD_ID = os.environ.get("TEMPORAL_WORKER_BUILD_ID")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))


async def start_health_server() -> asyncio.AbstractServer:
    """Serve the readiness probe the WorkerDeployment template points at.

    The controller only considers a version healthy once its pods are ready, so
    this is deliberately started *after* the worker is polling: it is the signal
    the controller waits on before shifting any traffic.
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readline()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n"
                b"Content-Type: text/plain\r\n\r\nok\n"
            )
            await writer.drain()
        except Exception:  # a probe hanging up early must not spam the log
            pass
        finally:
            writer.close()

    return await asyncio.start_server(handle, "0.0.0.0", HEALTH_PORT)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [build=" + (BUILD_ID or "-") + "] %(message)s",
    )
    log = logging.getLogger("worker")

    if not BUILD_ID:
        raise SystemExit(
            "TEMPORAL_WORKER_BUILD_ID is not set. In-cluster the Temporal Worker "
            "Controller injects it; to run this worker by hand, export it "
            "yourself (e.g. TEMPORAL_WORKER_BUILD_ID=dev-1)."
        )

    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting, record_result],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(
                deployment_name=DEPLOYMENT_NAME,
                build_id=BUILD_ID,
            ),
            # Opt in to version-aware task routing. Without this the worker is
            # "unversioned" and the controller cannot route to it at all.
            use_worker_versioning=True,
        ),
        # Let in-flight activities finish when Kubernetes sends SIGTERM, which
        # is how the controller scales down a sunset version.
        graceful_shutdown_timeout=timedelta(seconds=30),
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info(
        "starting worker: deployment=%s build=%s task_queue=%s -> %s",
        DEPLOYMENT_NAME,
        BUILD_ID,
        TASK_QUEUE,
        TEMPORAL_ADDRESS,
    )
    async with worker:
        health = await start_health_server()
        log.info("readiness endpoint listening on :%d", HEALTH_PORT)
        try:
            await stop.wait()
        finally:
            health.close()
            await health.wait_closed()
    log.info("worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
