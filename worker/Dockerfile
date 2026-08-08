FROM python:3.12-slim

# No Build ID is baked in here. The Temporal Worker Controller derives it from
# this image reference plus a hash of the pod template, and injects it as
# TEMPORAL_WORKER_BUILD_ID at runtime.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY activities.py workflows.py run_worker.py starter.py ./

# Run unprivileged so SIGTERM-based draining behaves like a normal workload.
RUN useradd --create-home --uid 10001 worker
USER worker

# The readiness probe the WorkerDeployment template points at. The controller
# treats a version as healthy only once this passes.
EXPOSE 8080

CMD ["python", "run_worker.py"]
