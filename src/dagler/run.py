"""DagRun — persistent pipeline run model backed by sqler."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import PrivateAttr
from sqler import AsyncSQLerModel

if TYPE_CHECKING:
    from qler.queue import Queue

from qler.models.job import Job


class DagRun(AsyncSQLerModel):
    """Persistent record of a DAG pipeline run.

    Tracks the lifecycle of a submitted DAG from pending → running →
    completed/failed/cancelled.  Provides :meth:`wait` and :meth:`cancel`
    for pipeline-level control.

    Backward-compatible with the frozen-dataclass interface from M2:
    ``run.jobs``, ``run.job_map``, ``run.dag_name``, ``run.correlation_id``
    all work identically.
    """

    __promoted__: ClassVar[dict[str, str]] = {
        "correlation_id": "TEXT UNIQUE NOT NULL",
        "dag_name": "TEXT NOT NULL",
        "status": "TEXT NOT NULL DEFAULT 'pending'",
        "idempotency_key": "TEXT",
    }
    __checks__: ClassVar[dict[str, str]] = {
        "status": "status IN ('pending','running','completed','failed','cancelled')",
    }

    # Promoted fields (hot-path queries)
    correlation_id: str = ""
    dag_name: str = ""
    status: str = "pending"
    idempotency_key: Optional[str] = None

    # JSON fields
    jobs_manifest: list[dict[str, str]] = []
    created_at: int = 0
    updated_at: int = 0
    finished_at: Optional[int] = None

    # Non-persistent caches (PrivateAttr — excluded from serialization)
    _jobs: tuple = PrivateAttr(default=())
    _job_map: dict = PrivateAttr(default_factory=dict)
    _queue_ref: Any = PrivateAttr(default=None)

    # ── Backward-compatible properties ────────────────────────────────

    @property
    def job_ulids(self) -> list[str]:
        """Derive job ULID list from manifest."""
        return [e["ulid"] for e in self.jobs_manifest]

    @property
    def task_names(self) -> list[str]:
        """Derive task name list from manifest."""
        return [e["task_name"] for e in self.jobs_manifest]

    @property
    def jobs(self) -> tuple[Job, ...]:
        """All jobs in submission order (topological order)."""
        return self._jobs

    @property
    def job_map(self) -> dict[str, Job]:
        """task_name → Job for O(1) lookup by name."""
        return dict(self._job_map)

    # ── Status computation ────────────────────────────────────────────

    @staticmethod
    def _compute_status(statuses: list[str]) -> str:
        """Derive pipeline status from constituent job statuses.

        Rules (terminal-first):
        1. No jobs → "completed" (empty DAG)
        2. All terminal + any failed → "failed"
        3. All terminal + any cancelled → "cancelled"
        4. All terminal + all completed → "completed"
        5. Any running → "running"
        6. Otherwise → "pending"
        """
        if not statuses:
            return "completed"

        status_set = set(statuses)
        terminal = {"completed", "failed", "cancelled"}

        if status_set <= terminal:
            if "failed" in status_set:
                return "failed"
            if "cancelled" in status_set:
                return "cancelled"
            return "completed"

        if "running" in status_set:
            return "running"

        return "pending"

    # ── refresh_status ────────────────────────────────────────────────

    async def refresh_status(self) -> str:
        """Re-fetch jobs from DB, recompute and persist status."""
        from sqler import F

        jobs = await Job.query().filter(
            F("ulid").in_list(self.job_ulids),
        ).all()

        # Rebuild caches preserving submission order
        ulid_to_job = {j.ulid: j for j in jobs}
        self._jobs = tuple(
            ulid_to_job[u] for u in self.job_ulids if u in ulid_to_job
        )
        self._job_map = {
            name: ulid_to_job[ulid]
            for name, ulid in zip(self.task_names, self.job_ulids)
            if ulid in ulid_to_job
        }

        new_status = self._compute_status([j.status for j in jobs])
        self.status = new_status
        self.updated_at = _now_epoch()
        if new_status in ("completed", "failed", "cancelled") and self.finished_at is None:
            self.finished_at = _now_epoch()
        await self.save()
        return new_status

    # ── wait ──────────────────────────────────────────────────────────

    async def wait(self, *, timeout: float | None = None, poll_interval: float = 0.5) -> DagRun:
        """Wait for all jobs to reach terminal state.

        Sequential with shared timeout budget.  Catches per-job errors so
        ALL jobs are waited on (cascade-cancelled children return immediately).

        After initial jobs complete, re-fetches from DB to discover any
        dynamically added jobs (from fan-out dispatcher) and waits for those too.
        """
        from qler.exceptions import JobCancelledError, JobFailedError
        from sqler import F

        if not self._jobs:
            return self

        start = time.monotonic()
        waited_ulids: set[str] = set()

        async def _wait_jobs(jobs: tuple[Job, ...]) -> None:
            for job in jobs:
                if job.ulid in waited_ulids:
                    continue
                waited_ulids.add(job.ulid)
                remaining = None
                if timeout is not None:
                    elapsed = time.monotonic() - start
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        msg = f"DagRun {self.correlation_id!r} timed out"
                        raise TimeoutError(msg)
                try:
                    await job.wait(timeout=remaining, poll_interval=poll_interval)
                except (JobFailedError, JobCancelledError):
                    pass
                except TimeoutError:
                    msg = f"DagRun {self.correlation_id!r} timed out"
                    raise TimeoutError(msg) from None

        # Wait for initial (static) jobs
        await _wait_jobs(self._jobs)

        # Loop: keep discovering dynamically added jobs until stable.
        # Cascading dispatchers add jobs in waves — wave 2 jobs don't
        # exist when wave 1 check runs, so we loop until no new jobs appear.
        while True:
            refreshed = await DagRun.query().filter(
                F("correlation_id") == self.correlation_id,
            ).first()

            if not refreshed or len(refreshed.job_ulids) <= len(waited_ulids):
                break

            new_ulids = [u for u in refreshed.job_ulids if u not in waited_ulids]
            if not new_ulids:
                break

            self.jobs_manifest = refreshed.jobs_manifest
            new_jobs = await Job.query().filter(
                F("ulid").in_list(new_ulids),
            ).all()
            # Sort by ULID to preserve submission order
            ulid_order = {u: i for i, u in enumerate(new_ulids)}
            new_jobs.sort(key=lambda j: ulid_order.get(j.ulid, 0))
            await _wait_jobs(tuple(new_jobs))

        await self.refresh_status()
        return self

    # ── retry ─────────────────────────────────────────────────────────

    async def retry(self, queue: Queue | None = None) -> DagRun:
        """Retry failed/cancelled jobs. Skip completed jobs.

        Resets retryable jobs to PENDING with correct ``pending_dep_count``
        so qler's dependency machinery re-drives the pipeline from the
        failure point.  Already-completed jobs are untouched.
        """
        from sqler import F

        from dagler.exceptions import (
            DaglerError,
            RetryCompletedRunError,
            RetryNoFailuresError,
        )

        q = queue or self._queue_ref
        if q is None:
            msg = "Queue required for retry"
            raise DaglerError(msg)

        await self.refresh_status()

        if self.status == "completed":
            raise RetryCompletedRunError("Cannot retry a completed run")

        retryable = [j for j in self._jobs if j.status in ("failed", "cancelled")]
        if not retryable:
            raise RetryNoFailuresError("No failed or cancelled jobs to retry")

        completed_ulids = {j.ulid for j in self._jobs if j.status == "completed"}

        for job in retryable:
            # Count parents that are NOT completed (i.e., also being retried)
            non_completed_parents = sum(
                1 for dep_ulid in job.dependencies
                if dep_ulid not in completed_ulids
            )
            await Job.query().filter(
                (F("ulid") == job.ulid)
                & (F("status").in_list(["failed", "cancelled"]))
            ).update_one(
                status="pending",
                pending_dep_count=non_completed_parents,
                finished_at=None,
                retry_count=0,
                last_error=None,
                last_failure_kind=None,
                updated_at=_now_epoch(),
                progress=None,
                progress_message="",
            )

        self.status = "pending"
        self.finished_at = None
        self.updated_at = _now_epoch()
        await self.save()
        await self.refresh_status()
        return self

    # ── cancel ────────────────────────────────────────────────────────

    async def cancel(self, queue: Queue | None = None) -> DagRun:
        """Cancel all non-terminal jobs in this run."""
        from dagler.exceptions import DaglerError

        q = queue or self._queue_ref
        if q is None:
            msg = "Queue required for cancellation"
            raise DaglerError(msg)

        for job in self._jobs:
            if job.status in ("pending", "running"):
                await q.cancel_job(job)

        await self.refresh_status()
        return self


def _now_epoch() -> int:
    """Current UTC time as integer epoch seconds."""
    return int(time.time())
