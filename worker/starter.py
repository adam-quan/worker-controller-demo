"""Start workflows against the demo task queue.

Usage:
    python starter.py greeting  <name>      # PINNED, long-running
    python starter.py health                # AUTO_UPGRADE, polls for ~5 min
    python starter.py approve  <workflow-id> # release a parked greeting run
    python starter.py status   <workflow-id>
"""

import asyncio
import os
import sys
import uuid

from temporalio.client import Client

from workflows import GreetingWorkflow, HealthCheckWorkflow

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "greeting-tq")


async def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "greeting"
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)

    if command == "greeting":
        name = sys.argv[2] if len(sys.argv) > 2 else "Temporal"
        workflow_id = f"greeting-{uuid.uuid4().hex[:8]}"
        handle = await client.start_workflow(
            GreetingWorkflow.run,
            name,
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
        print(f"started {handle.id}")
        print(f"  it is now parked on a signal. Deploy a new version, then run:")
        print(f"    python starter.py approve {handle.id}")

    elif command == "health":
        handle = await client.start_workflow(
            HealthCheckWorkflow.run,
            20,
            id=f"health-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )
        print(f"started {handle.id}")

    elif command == "approve":
        handle = client.get_workflow_handle(sys.argv[2])
        await handle.signal(GreetingWorkflow.approve)
        print(f"signalled {sys.argv[2]}; result: {await handle.result()}")

    elif command == "status":
        handle = client.get_workflow_handle(sys.argv[2])
        desc = await handle.describe()
        print(f"{desc.id}: status={desc.status.name}")
        print(f"  progress: {await handle.query(GreetingWorkflow.progress)}")

    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    asyncio.run(main())
