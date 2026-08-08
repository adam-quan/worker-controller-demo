"""Workflow definitions for the Worker Versioning demo.

This is the file you edit to trigger the demo. Change ``GREETING`` below (or
anything else in here), push to ``main``, and GitHub Actions builds a new image
and points the WorkerDeployment resource at it. The Temporal Worker Controller
notices the change and rolls out a *new* Worker Deployment Version in minikube
-- without disturbing runs already in flight.

``GreetingWorkflow`` is PINNED: a run that started on Build ID ``abc1234``
keeps executing on ``abc1234`` for its entire life, even after a newer version
becomes Current. That is what makes it safe to change this file however you
like. The alternative behavior, ``AUTO_UPGRADE``, moves in-flight runs to
whatever version is Current at their next workflow task, and is only safe for
workflows you change compatibly.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, VersioningBehavior

with workflow.unsafe.imports_passed_through():
    from activities import compose_greeting, record_result

# ---------------------------------------------------------------------------
# Edit me to produce a new workflow version, then `git push`.
#GREETING = "你好"
GREETING = "Hello"
# ---------------------------------------------------------------------------


@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class GreetingWorkflow:
    """A deliberately long-lived workflow, pinned to the version that started it.

    It greets, then parks on a signal so you have time to deploy a new version
    and watch this run stay on the old one.
    """

    def __init__(self) -> None:
        self._approved = False
        self._log: list[str] = []

    @workflow.run
    async def run(self, name: str) -> dict:
        greeting = await workflow.execute_activity(
            compose_greeting,
            args=[GREETING, name],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        self._log.append(greeting)

        # Park here until signalled (or 30 minutes pass). This is the window in
        # which you deploy a new version and observe that this run does not move.
        await workflow.wait_condition(
            lambda: self._approved, timeout=timedelta(minutes=30)
        )

        summary = await workflow.execute_activity(
            record_result,
            args=[greeting],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return {"greeting": greeting, "recorded_by": summary}

    @workflow.signal
    def approve(self) -> None:
        """Release the workflow from its wait so it can finish."""
        self._approved = True

    @workflow.query
    def progress(self) -> list[str]:
        return self._log
