"""Tests for DagRun model (M4).

Task functions are defined at module level because @dag.task rejects nested
functions (qualname != name).
"""

from __future__ import annotations

import asyncio
import time

import pytest
from dagler import DAG, DagRun
from dagler.exceptions import (
    DaglerError,
    RetryCompletedRunError,
    RetryNoFailuresError,
)

# ---------------------------------------------------------------------------
# Module-level task functions
# ---------------------------------------------------------------------------


async def step_a():
    return {"step": "a"}


async def step_b(step_a=None):
    return {"step": "b"}


async def step_c(step_b=None):
    return {"step": "c"}


async def failing_task():
    raise ValueError("intentional")


async def slow_task():
    await asyncio.sleep(300)


# ---------------------------------------------------------------------------
# _compute_status — pure unit tests (no DB, no IO)
# ---------------------------------------------------------------------------


class TestComputeStatus:
    def test_no_jobs_is_completed(self):
        assert DagRun._compute_status([]) == "completed"

    def test_all_completed(self):
        assert DagRun._compute_status(["completed", "completed"]) == "completed"

    def test_any_failed_when_all_terminal(self):
        assert DagRun._compute_status(["completed", "failed"]) == "failed"

    def test_any_cancelled_when_all_terminal(self):
        assert DagRun._compute_status(["completed", "cancelled"]) == "cancelled"

    def test_failed_takes_precedence_over_cancelled(self):
        assert DagRun._compute_status(["failed", "cancelled"]) == "failed"

    def test_any_running_means_running(self):
        assert DagRun._compute_status(["completed", "running"]) == "running"

    def test_all_pending(self):
        assert DagRun._compute_status(["pending", "pending"]) == "pending"

    def test_pending_and_completed_is_pending(self):
        assert DagRun._compute_status(["pending", "completed"]) == "pending"

    def test_single_completed(self):
        assert DagRun._compute_status(["completed"]) == "completed"

    def test_single_failed(self):
        assert DagRun._compute_status(["failed"]) == "failed"

    def test_single_cancelled(self):
        assert DagRun._compute_status(["cancelled"]) == "cancelled"

    def test_single_running(self):
        assert DagRun._compute_status(["running"]) == "running"

    def test_single_pending(self):
        assert DagRun._compute_status(["pending"]) == "pending"

    def test_mixed_running_and_pending(self):
        assert DagRun._compute_status(["running", "pending"]) == "running"

    def test_all_three_terminal(self):
        assert DagRun._compute_status(["completed", "failed", "cancelled"]) == "failed"


# ---------------------------------------------------------------------------
# Persistence — DagRun is saved to SQLite
# ---------------------------------------------------------------------------


class TestDagRunPersistence:
    async def test_run_creates_record(self, queue):
        from sqler import F

        dag = DAG("persist")
        dag.task(step_a)

        now = int(time.time())
        run = await dag.run(queue)

        assert run._id is not None
        assert run.dag_name == "persist"
        assert run.status == "pending"
        assert len(run.job_ulids) == 1
        assert len(run.task_names) == 1
        assert run.task_names == ["step_a"]
        assert abs(run.created_at - now) < 5

        # Round-trip: verify actually persisted to SQLite
        reloaded = await DagRun.query().filter(
            F("correlation_id") == run.correlation_id,
        ).first()
        assert reloaded is not None
        assert reloaded._id == run._id
        assert reloaded.dag_name == "persist"

    async def test_unique_run_ids(self, queue):
        dag = DAG("unique")
        dag.task(step_a)

        run1 = await dag.run(queue)
        run2 = await dag.run(queue)

        assert run1.correlation_id != run2.correlation_id
        assert len(run1.correlation_id) == 26  # ULID format
        assert len(run2.correlation_id) == 26
        assert run1._id != run2._id

    async def test_empty_dag_persists_as_completed(self, queue):
        dag = DAG("empty")

        run = await dag.run(queue)

        assert run._id is not None
        assert run.status == "completed"
        assert run.dag_name == "empty"
        assert run.finished_at is not None
        assert run.job_ulids == []
        assert run.task_names == []

    async def test_run_persists_all_fields(self, queue):
        dag = DAG("fields")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])

        now = int(time.time())
        run = await dag.run(queue)

        assert len(run.job_ulids) == 2
        assert run.task_names == ["step_a", "step_b"]
        assert abs(run.created_at - now) < 5
        assert abs(run.updated_at - now) < 5
        assert run.finished_at is None  # not terminal yet


# ---------------------------------------------------------------------------
# wait() — waits for all jobs to complete
# ---------------------------------------------------------------------------


class TestDagRunWait:
    async def test_wait_completed(self, queue, worker):
        dag = DAG("wait_ok")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])
        dag.task(step_c, depends_on=[step_b])

        run = await dag.run(queue)
        result = await run.wait(timeout=10.0, poll_interval=0.02)

        assert result is run
        assert run.status == "completed"
        assert run.finished_at is not None
        assert len(run.jobs) == 3
        for job in run.jobs:
            assert job.status == "completed"
        assert set(run.job_map.keys()) == {"step_a", "step_b", "step_c"}

    async def test_wait_with_failure(self, queue, worker):
        dag = DAG("wait_fail")
        dag.task(failing_task)

        run = await dag.run(queue)
        result = await run.wait(timeout=10.0, poll_interval=0.02)

        assert result is run
        assert run.status == "failed"
        assert run.finished_at is not None
        assert len(run.jobs) == 1
        assert run.job_map["failing_task"].status == "failed"
        assert run.job_map["failing_task"].last_error is not None

    async def test_wait_empty_dag(self, queue):
        dag = DAG("wait_empty")

        run = await dag.run(queue)
        result = await run.wait(timeout=5.0)

        assert result is run
        assert run.status == "completed"
        assert run.finished_at is not None

    async def test_wait_timeout_raises(self, queue):
        dag = DAG("wait_timeout")
        dag.task(slow_task)

        run = await dag.run(queue)
        # No worker → jobs stay pending → timeout
        with pytest.raises(TimeoutError, match="timed out"):
            await run.wait(timeout=0.1, poll_interval=0.02)

        # Timeout should NOT change run status
        assert run.status == "pending"
        assert run.finished_at is None

    async def test_wait_with_cancelled_children(self, queue, worker):
        """Parent fails → children cascade-cancelled → wait still completes."""
        dag = DAG("cascade")
        dag.task(failing_task)
        dag.task(step_b, depends_on=[failing_task])

        run = await dag.run(queue)
        result = await run.wait(timeout=10.0, poll_interval=0.02)

        assert result is run
        assert run.status == "failed"
        assert len(run.jobs) == 2
        assert run.job_map["failing_task"].status == "failed"
        assert run.job_map["step_b"].status == "cancelled"


# ---------------------------------------------------------------------------
# cancel() — cancels non-terminal jobs
# ---------------------------------------------------------------------------


class TestDagRunCancel:
    async def test_cancel_pending_jobs(self, queue):
        dag = DAG("cancel_pending")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])

        run = await dag.run(queue)
        # No worker → all jobs pending
        result = await run.cancel(queue)

        assert result is run
        assert run.status == "cancelled"
        assert len(run.jobs) == 2
        for job in run.jobs:
            assert job.status == "cancelled"

    async def test_cancel_with_stored_queue_ref(self, queue):
        dag = DAG("cancel_ref")
        dag.task(step_a)

        run = await dag.run(queue)
        # _queue_ref is set by dag.run() — no explicit queue needed
        result = await run.cancel()

        assert result is run
        assert run.status == "cancelled"

    async def test_cancel_no_queue_raises(self, queue):
        dag = DAG("cancel_err")
        dag.task(step_a)

        run = await dag.run(queue)
        run._queue_ref = None  # simulate missing ref

        with pytest.raises(DaglerError, match="Queue required"):
            await run.cancel()

    async def test_cancel_cascades_to_dependents(self, queue):
        dag = DAG("cancel_cascade")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])
        dag.task(step_c, depends_on=[step_b])

        run = await dag.run(queue)
        # Cancel via dagler's cancel() — should cancel all pending jobs
        await run.cancel(queue)

        assert run.status == "cancelled"
        assert len(run.jobs) == 3
        for job in run.jobs:
            assert job.status == "cancelled"

    async def test_cancel_skips_terminal_jobs(self, queue, worker):
        """cancel() only cancels non-terminal jobs; completed jobs stay completed."""
        dag = DAG("cancel_mixed")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])

        run = await dag.run(queue)
        # Wait for step_a to complete (step_b stays pending — has unresolved dep)
        await run.jobs[0].wait(timeout=5.0, poll_interval=0.02)

        await run.cancel(queue)

        # step_a was already completed — should remain so
        assert run.job_map["step_a"].status == "completed"
        # step_b was pending — should be cancelled
        assert run.job_map["step_b"].status == "cancelled"


# ---------------------------------------------------------------------------
# refresh_status() — re-fetches jobs and recomputes
# ---------------------------------------------------------------------------


class TestDagRunRefreshStatus:
    async def test_refresh_updates_status(self, queue, worker):
        dag = DAG("refresh")
        dag.task(step_a)

        run = await dag.run(queue)
        assert run.status == "pending"

        # Wait for worker to complete the job
        await run.jobs[0].wait(timeout=5.0, poll_interval=0.02)

        new_status = await run.refresh_status()
        assert new_status == "completed"
        assert run.status == "completed"
        assert run.finished_at is not None

    async def test_refresh_persists_to_db(self, queue, worker):
        from sqler import F

        dag = DAG("refresh_persist")
        dag.task(step_a)

        run = await dag.run(queue)
        await run.jobs[0].wait(timeout=5.0, poll_interval=0.02)
        await run.refresh_status()

        # Re-load from DB and verify persisted
        reloaded = await DagRun.query().filter(
            F("correlation_id") == run.correlation_id,
        ).first()

        assert reloaded is not None
        assert reloaded.status == "completed"
        assert reloaded.finished_at is not None

    async def test_refresh_rebuilds_caches(self, queue, worker):
        dag = DAG("refresh_cache")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        # Caches should be rebuilt with fresh job objects
        assert len(run.jobs) == 2
        assert set(run.job_map.keys()) == {"step_a", "step_b"}
        assert run.job_map["step_a"].status == "completed"
        assert run.job_map["step_b"].status == "completed"
        assert run.job_map["step_a"].task.endswith("step_a")
        assert run.job_map["step_b"].task.endswith("step_b")


# ---------------------------------------------------------------------------
# Backward compatibility — M2 interface preserved
# ---------------------------------------------------------------------------


class TestDagRunBackwardCompat:
    async def test_isinstance(self, queue):
        dag = DAG("compat")
        dag.task(step_a)

        run = await dag.run(queue)
        assert isinstance(run, DagRun)

    async def test_jobs_is_tuple(self, queue):
        dag = DAG("compat_tuple")
        dag.task(step_a)

        run = await dag.run(queue)
        assert isinstance(run.jobs, tuple)
        assert len(run.jobs) == 1
        assert run.jobs[0].task.endswith("step_a")
        assert run.jobs[0].status == "pending"

    async def test_job_map_is_dict(self, queue):
        dag = DAG("compat_dict")
        dag.task(step_a)

        run = await dag.run(queue)
        assert isinstance(run.job_map, dict)
        assert set(run.job_map.keys()) == {"step_a"}
        assert run.job_map["step_a"].ulid == run.jobs[0].ulid

    async def test_correlation_id_preserved(self, queue):
        dag = DAG("compat_cid")
        dag.task(step_a)

        run = await dag.run(queue, correlation_id="my-custom-id")
        assert run.correlation_id == "my-custom-id"
        assert run.jobs[0].correlation_id == "my-custom-id"

    async def test_dag_name_preserved(self, queue):
        dag = DAG("compat_name")
        dag.task(step_a)

        run = await dag.run(queue)
        assert run.dag_name == "compat_name"

    async def test_empty_dag_jobs_is_empty_tuple(self, queue):
        dag = DAG("compat_empty")

        run = await dag.run(queue)
        assert run.jobs == ()
        assert run.job_map == {}

    async def test_job_map_returns_copy(self, queue):
        dag = DAG("compat_copy")
        dag.task(step_a)

        run = await dag.run(queue)
        map1 = run.job_map
        map2 = run.job_map
        assert map1 == map2
        assert map1 is not map2  # defensive copy


# ---------------------------------------------------------------------------
# retry() — retry failed/cancelled jobs
# ---------------------------------------------------------------------------


class TestDagRunRetry:
    async def test_retry_resets_failed_jobs(self, queue, worker):
        """Failed jobs go to PENDING, completed jobs stay completed."""
        dag = DAG("retry_fail")
        dag.task(step_a)
        dag.task(failing_task, depends_on=[step_a])

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert len(run.jobs) == 2
        assert run.status == "failed"
        assert run.job_map["step_a"].status == "completed"
        assert run.job_map["failing_task"].status == "failed"

        await run.retry(queue)

        assert len(run.jobs) == 2
        assert run.job_map["step_a"].status == "completed"
        assert run.job_map["failing_task"].status == "pending"

    async def test_retry_resets_cancelled_jobs(self, queue, worker):
        """Cascade-cancelled children also reset to PENDING."""
        dag = DAG("retry_cascade")
        dag.task(failing_task)
        dag.task(step_b, depends_on=[failing_task])
        dag.task(step_c, depends_on=[step_b])

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert len(run.jobs) == 3
        assert run.status == "failed"
        assert run.job_map["failing_task"].status == "failed"
        assert run.job_map["step_b"].status == "cancelled"
        assert run.job_map["step_c"].status == "cancelled"

        await run.retry(queue)

        assert len(run.jobs) == 3
        assert run.job_map["failing_task"].status == "pending"
        assert run.job_map["step_b"].status == "pending"
        assert run.job_map["step_c"].status == "pending"

    async def test_retry_pending_dep_count(self, queue, worker):
        """Retried child has pending_dep_count = number of non-completed parents."""
        dag = DAG("retry_deps")
        dag.task(failing_task)
        dag.task(step_b, depends_on=[failing_task])

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert len(run.jobs) == 2
        await run.retry(queue)

        assert len(run.jobs) == 2
        # step_b depends on failing_task (also retried, not completed)
        # so pending_dep_count should be 1
        step_b_job = run.job_map["step_b"]
        assert step_b_job.pending_dep_count == 1

        # failing_task has no parents → pending_dep_count = 0
        fail_job = run.job_map["failing_task"]
        assert fail_job.pending_dep_count == 0

    async def test_retry_clears_error_and_retry_count(self, queue, worker):
        """Retried jobs have last_error, last_failure_kind, and retry_count reset."""
        dag = DAG("retry_clears")
        dag.task(failing_task)

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert len(run.jobs) == 1
        assert run.job_map["failing_task"].last_error is not None

        await run.retry(queue)

        job = run.job_map["failing_task"]
        assert job.status == "pending"
        assert job.last_error is None
        assert job.last_failure_kind is None
        assert job.retry_count == 0

    async def test_retry_completed_run_raises(self, queue, worker):
        """Cannot retry a fully completed run."""
        dag = DAG("retry_completed")
        dag.task(step_a)

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert run.status == "completed"
        with pytest.raises(RetryCompletedRunError, match="Cannot retry a completed run"):
            await run.retry(queue)

    async def test_retry_no_failures_raises(self, queue):
        """Cannot retry a run with no failed/cancelled jobs (all pending)."""
        dag = DAG("retry_pending")
        dag.task(step_a)
        dag.task(step_b, depends_on=[step_a])

        run = await dag.run(queue)
        # No worker → all pending, none failed

        with pytest.raises(RetryNoFailuresError, match="No failed or cancelled jobs"):
            await run.retry(queue)

    async def test_retry_no_queue_raises(self, queue, worker):
        """Retry without queue or _queue_ref raises DaglerError."""
        dag = DAG("retry_no_q")
        dag.task(failing_task)

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)
        run._queue_ref = None

        with pytest.raises(DaglerError, match="Queue required"):
            await run.retry()

    async def test_retry_preserves_correlation_id(self, queue, worker):
        """Correlation ID unchanged after retry — verified via DB round-trip."""
        from sqler import F

        dag = DAG("retry_cid")
        dag.task(failing_task)

        run = await dag.run(queue)
        original_cid = run.correlation_id
        assert len(original_cid) == 26  # valid ULID
        await run.wait(timeout=10.0, poll_interval=0.02)

        await run.retry(queue)

        assert run.correlation_id == original_cid

        # Verify persisted to DB
        reloaded = await DagRun.query().filter(
            F("correlation_id") == original_cid,
        ).first()
        assert reloaded is not None
        assert reloaded.correlation_id == original_cid

    async def test_retry_updates_dagrun_status(self, queue, worker):
        """DagRun status resets to 'pending' after retry — verified via DB round-trip."""
        from sqler import F

        dag = DAG("retry_status")
        dag.task(failing_task)

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert run.status == "failed"

        await run.retry(queue)

        # In-memory check
        assert run.status == "pending"
        assert run.finished_at is None

        # DB round-trip check
        reloaded = await DagRun.query().filter(
            F("correlation_id") == run.correlation_id,
        ).first()
        assert reloaded is not None
        assert reloaded.status == "pending"
        assert reloaded.finished_at is None

    async def test_retry_with_stored_queue_ref(self, queue, worker):
        """Retry works using stored _queue_ref (no explicit queue arg)."""
        dag = DAG("retry_ref")
        dag.task(failing_task)

        run = await dag.run(queue)
        await run.wait(timeout=10.0, poll_interval=0.02)

        assert run.status == "failed"
        result = await run.retry()  # uses _queue_ref set by dag.run()

        assert result is run
        assert run.status == "pending"
