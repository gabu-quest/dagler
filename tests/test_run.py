"""Tests for DAG submission (M2).

Task functions are defined at module level because @dag.task rejects nested
functions (qualname != name). Each test creates a fresh DAG + queue.
"""

import json

import pytest
from dagler import DAG, DagRun
from dagler.exceptions import CycleError
from dagler.task import TaskNode

# ---------------------------------------------------------------------------
# Module-level task functions (required: @dag.task rejects nested functions)
# ---------------------------------------------------------------------------


async def extract():
    return {"data": [1, 2, 3]}


async def transform(extract_result):
    return {"transformed": True}


async def load(transform_result):
    return {"loaded": True}


async def branch_b():
    pass


async def branch_c():
    pass


async def merge_d():
    pass


async def solo():
    return "done"


# ---------------------------------------------------------------------------
# Single task
# ---------------------------------------------------------------------------


class TestRunSingleTask:
    async def test_run_single_task(self, queue):
        dag = DAG("single")
        dag.task(solo)

        run = await dag.run(queue)

        assert len(run.jobs) == 1
        assert run.dag_name == "single"
        assert len(run.correlation_id) == 26
        assert run.jobs[0].task == "tests.test_run.solo"
        assert run.jobs[0].status == "pending"
        assert run.jobs[0].pending_dep_count == 0
        assert run.jobs[0].dependencies == []


# ---------------------------------------------------------------------------
# Linear chain: E → T → L
# ---------------------------------------------------------------------------


class TestRunLinearChain:
    async def test_run_linear_chain(self, queue):
        dag = DAG("etl")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        run = await dag.run(queue)

        assert len(run.jobs) == 3

        extract_job = run.job_map["extract"]
        transform_job = run.job_map["transform"]
        load_job = run.job_map["load"]

        # Extract has no deps
        assert extract_job.dependencies == []
        assert extract_job.pending_dep_count == 0

        # Transform depends on extract (exact list, not just containment)
        assert transform_job.dependencies == [extract_job.ulid]
        assert transform_job.pending_dep_count == 1

        # Load depends on transform
        assert load_job.dependencies == [transform_job.ulid]
        assert load_job.pending_dep_count == 1


# ---------------------------------------------------------------------------
# Diamond: A → B, C → D
# ---------------------------------------------------------------------------


class TestRunDiamond:
    async def test_run_diamond(self, queue):
        dag = DAG("diamond")
        dag.task(extract)
        dag.task(branch_b, depends_on=[extract])
        dag.task(branch_c, depends_on=[extract])
        dag.task(merge_d, depends_on=[branch_b, branch_c])

        run = await dag.run(queue)

        assert len(run.jobs) == 4

        extract_job = run.job_map["extract"]
        b_job = run.job_map["branch_b"]
        c_job = run.job_map["branch_c"]
        d_job = run.job_map["merge_d"]

        # Root has no deps
        assert extract_job.dependencies == []
        assert extract_job.pending_dep_count == 0

        # Branches depend on extract (exact)
        assert b_job.dependencies == [extract_job.ulid]
        assert c_job.dependencies == [extract_job.ulid]
        assert b_job.pending_dep_count == 1
        assert c_job.pending_dep_count == 1

        # Merge depends on both branches (set equality — order may vary)
        assert set(d_job.dependencies) == {b_job.ulid, c_job.ulid}
        assert d_job.pending_dep_count == 2

        # All jobs should be in pending state
        for job in run.jobs:
            assert job.status == "pending"


# ---------------------------------------------------------------------------
# DagRun handle
# ---------------------------------------------------------------------------


class TestDagRunHandle:
    async def test_run_returns_dag_run(self, queue):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])

        run = await dag.run(queue)

        assert isinstance(run, DagRun)  # guard
        assert run.dag_name == "test"
        assert len(run.correlation_id) == 26
        assert len(run.jobs) == 2
        assert set(run.job_map.keys()) == {"extract", "transform"}

    async def test_run_job_map(self, queue):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        run = await dag.run(queue)

        assert set(run.job_map.keys()) == {"extract", "transform", "load"}
        assert run.job_map["extract"].task == "tests.test_run.extract"
        assert run.job_map["transform"].task == "tests.test_run.transform"
        assert run.job_map["load"].task == "tests.test_run.load"


# ---------------------------------------------------------------------------
# Correlation ID
# ---------------------------------------------------------------------------


class TestCorrelationId:
    async def test_run_shared_correlation_id(self, queue):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        run = await dag.run(queue)

        assert len(run.jobs) == 3
        cids = {job.correlation_id for job in run.jobs}
        assert cids == {run.correlation_id}
        assert len(run.correlation_id) == 26

    async def test_run_custom_correlation_id(self, queue):
        dag = DAG("test")
        dag.task(extract)

        run = await dag.run(queue, correlation_id="my-custom-id")

        assert run.correlation_id == "my-custom-id"
        assert run.jobs[0].correlation_id == "my-custom-id"

    async def test_run_auto_correlation_id(self, queue):
        dag = DAG("test")
        dag.task(extract)

        run = await dag.run(queue)

        assert len(run.correlation_id) == 26  # ULID length


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


class TestPayload:
    async def test_run_with_payload(self, queue):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])

        run = await dag.run(queue, payload={"extract": {"url": "http://example.com"}})

        extract_job = run.job_map["extract"]
        payload = json.loads(extract_job.payload_json)
        assert payload["kwargs"] == {"url": "http://example.com"}

        # transform has no explicit payload → empty kwargs
        transform_job = run.job_map["transform"]
        t_payload = json.loads(transform_job.payload_json)
        assert t_payload["kwargs"] == {}

    async def test_run_payload_on_non_root_task(self, queue):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])

        run = await dag.run(queue, payload={"transform": {"mode": "fast"}})

        transform_job = run.job_map["transform"]
        payload = json.loads(transform_job.payload_json)
        assert payload["kwargs"] == {"mode": "fast"}

        # extract got no payload
        extract_job = run.job_map["extract"]
        e_payload = json.loads(extract_job.payload_json)
        assert e_payload["kwargs"] == {}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestRunValidation:
    async def test_run_validates_dag(self, queue):
        dag = DAG("cyclic")
        # Manually inject a cycle (decorator prevents it through public API)
        dag._tasks["a"] = TaskNode(
            fn=extract, name="a", task_path="test.a", dependencies=("b",)
        )
        dag._tasks["b"] = TaskNode(
            fn=transform, name="b", task_path="test.b", dependencies=("a",)
        )

        with pytest.raises(CycleError, match="Cycle detected"):
            await dag.run(queue)

    async def test_run_validates_indirect_cycle(self, queue):
        dag = DAG("cyclic3")
        dag._tasks["a"] = TaskNode(
            fn=extract, name="a", task_path="test.a", dependencies=("c",)
        )
        dag._tasks["b"] = TaskNode(
            fn=transform, name="b", task_path="test.b", dependencies=("a",)
        )
        dag._tasks["c"] = TaskNode(
            fn=load, name="c", task_path="test.c", dependencies=("b",)
        )

        with pytest.raises(CycleError, match="Cycle detected"):
            await dag.run(queue)


# ---------------------------------------------------------------------------
# Empty DAG
# ---------------------------------------------------------------------------


class TestRunEmptyDag:
    async def test_run_empty_dag(self, queue):
        dag = DAG("empty")

        run = await dag.run(queue)

        assert isinstance(run, DagRun)  # guard
        assert run.dag_name == "empty"
        assert len(run.correlation_id) == 26
        assert run.jobs == ()
        assert run.job_map == {}


# ---------------------------------------------------------------------------
# Ordering and dependency correctness
# ---------------------------------------------------------------------------


class TestRunOrdering:
    async def test_run_jobs_in_topo_order(self, queue):
        dag = DAG("etl")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        run = await dag.run(queue)

        # Jobs tuple should be in same order as topo sort
        assert run.jobs[0].ulid == run.job_map["extract"].ulid
        assert run.jobs[1].ulid == run.job_map["transform"].ulid
        assert run.jobs[2].ulid == run.job_map["load"].ulid

    async def test_run_dependency_ulids_correct(self, queue):
        dag = DAG("etl")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        run = await dag.run(queue)

        extract_job = run.job_map["extract"]
        transform_job = run.job_map["transform"]
        load_job = run.job_map["load"]

        # Integer indices were resolved to actual ULIDs by qler
        assert transform_job.dependencies == [extract_job.ulid]
        assert load_job.dependencies == [transform_job.ulid]

    async def test_run_pending_dep_count(self, queue):
        dag = DAG("diamond")
        dag.task(extract)
        dag.task(branch_b, depends_on=[extract])
        dag.task(branch_c, depends_on=[extract])
        dag.task(merge_d, depends_on=[branch_b, branch_c])

        run = await dag.run(queue)

        assert run.job_map["extract"].pending_dep_count == 0
        assert run.job_map["branch_b"].pending_dep_count == 1
        assert run.job_map["branch_c"].pending_dep_count == 1
        assert run.job_map["merge_d"].pending_dep_count == 2


# ---------------------------------------------------------------------------
# DAG reuse
# ---------------------------------------------------------------------------


class TestDagReuse:
    async def test_two_runs_are_independent(self, queue):
        dag = DAG("reusable")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])

        run1 = await dag.run(queue)
        run2 = await dag.run(queue)

        # Different correlation IDs
        assert run1.correlation_id != run2.correlation_id
        assert len(run1.correlation_id) == 26
        assert len(run2.correlation_id) == 26

        # Disjoint job ULIDs
        ulids1 = {j.ulid for j in run1.jobs}
        ulids2 = {j.ulid for j in run2.jobs}
        assert ulids1.isdisjoint(ulids2)
