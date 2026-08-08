"""Activities for the Worker Versioning demo.

Every activity stamps its result with the Build ID of the worker process that
executed it, which is what makes the versioning behavior visible from the
outside: a PINNED run keeps reporting its original Build ID long after a newer
version has become Current.

TEMPORAL_WORKER_BUILD_ID is injected into the pod by the Temporal Worker
Controller; it is derived from the container image and pod template, not set
by us.
"""

import os

from temporalio import activity

BUILD_ID = os.environ.get("TEMPORAL_WORKER_BUILD_ID", "unversioned")


@activity.defn
async def compose_greeting(greeting: str, name: str) -> str:
    activity.logger.info("composing greeting on build %s", BUILD_ID)
    return f"{greeting}, {name}! (served by build {BUILD_ID})"


@activity.defn
async def record_result(greeting: str) -> str:
    activity.logger.info("recording %r on build %s", greeting, BUILD_ID)
    return BUILD_ID
