"""Tests for result injection (M3).

Task functions are defined at module level because @dag.task rejects nested
functions (qualname != name).  A module-level ``_capture`` dict collects values
during task execution so tests can assert on injected results.
"""

from __future__ import annotations

from typing import Any

import pytest
from dagler import DAG

# ---------------------------------------------------------------------------
# Module-level capture dict — reset per test via autouse fixture
# ---------------------------------------------------------------------------

_capture: dict[str, Any] = {}


@pytest.fixture(autouse=True)
def _reset_capture():
    _capture.clear()
    yield
    _capture.clear()


async def _wait_all_completed(jobs):
    """Wait for all jobs and assert each finished as 'completed'."""
    for job in jobs:
        finished = await job.wait(timeout=5.0, poll_interval=0.02)
        assert finished.status == "completed", (
            f"Job {finished.task!r} ended as {finished.status!r}, expected 'completed'"
        )


# ---------------------------------------------------------------------------
# Module-level task functions (required: @dag.task rejects nested functions)
# ---------------------------------------------------------------------------


# --- Single parent injection ---


async def single_extract():
    return {"data": [1, 2, 3]}


async def single_transform(single_extract=None):
    _capture["single_transform_input"] = single_extract
    return {"transformed": True}


# --- Linear chain: a → b → c ---


async def chain_a():
    return {"step": "a"}


async def chain_b(chain_a=None):
    _capture["chain_b_input"] = chain_a
    return {"step": "b", "prev": chain_a}


async def chain_c(chain_b=None):
    _capture["chain_c_input"] = chain_b
    return {"step": "c"}


# --- Diamond: root → left, right → merge ---


async def diamond_root():
    return {"val": "root"}


async def diamond_left(diamond_root=None):
    _capture["diamond_left_input"] = diamond_root
    return {"val": "left"}


async def diamond_right(diamond_root=None):
    _capture["diamond_right_input"] = diamond_root
    return {"val": "right"}


async def diamond_merge(diamond_left=None, diamond_right=None):
    _capture["diamond_merge_left"] = diamond_left
    _capture["diamond_merge_right"] = diamond_right
    return {"merged": True}


# --- inject_as rename ---


async def ias_extract():
    return {"raw": "data"}


async def ias_transform(data=None):
    _capture["ias_transform_input"] = data
    return {"processed": True}


# --- Root task (no injection) ---


async def root_only():
    _capture["root_only_called"] = True
    return {"root": True}


# --- Payload override ---


async def ovr_parent():
    return {"injected_result": True}


async def ovr_child(ovr_parent=None):
    _capture["ovr_child_input"] = ovr_parent
    return {"done": True}


# --- Parent returns None ---


async def none_parent():
    return None


async def none_child(none_parent=None):
    _capture["none_child_input"] = none_parent
    _capture["none_child_called"] = True
    return {"handled": True}


# --- Complex types ---


async def complex_parent():
    return {"nested": {"list": [1, "two", 3.0]}, "flag": True, "count": 42}


async def complex_child(complex_parent=None):
    _capture["complex_child_input"] = complex_parent
    return {"received": True}


# ---------------------------------------------------------------------------
# Tests — Single parent
# ---------------------------------------------------------------------------


class TestSingleParentInjection:
    async def test_child_receives_parent_result(self, queue, worker):
        dag = DAG("single_inject")
        dag.task(single_extract)
        dag.task(single_transform, depends_on=[single_extract])

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["single_transform_input"] == {"data": [1, 2, 3]}


# ---------------------------------------------------------------------------
# Tests — Linear chain
# ---------------------------------------------------------------------------


class TestLinearChainResultPassing:
    async def test_results_flow_through_all_stages(self, queue, worker):
        dag = DAG("chain")
        dag.task(chain_a)
        dag.task(chain_b, depends_on=[chain_a])
        dag.task(chain_c, depends_on=[chain_b])

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["chain_b_input"] == {"step": "a"}
        assert _capture["chain_c_input"] == {"step": "b", "prev": {"step": "a"}}


# ---------------------------------------------------------------------------
# Tests — Diamond
# ---------------------------------------------------------------------------


class TestDiamondMultipleParentInjection:
    async def test_merge_receives_both_branch_results(self, queue, worker):
        dag = DAG("diamond")
        dag.task(diamond_root)
        dag.task(diamond_left, depends_on=[diamond_root])
        dag.task(diamond_right, depends_on=[diamond_root])
        dag.task(diamond_merge, depends_on=[diamond_left, diamond_right])

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["diamond_left_input"] == {"val": "root"}
        assert _capture["diamond_right_input"] == {"val": "root"}
        assert _capture["diamond_merge_left"] == {"val": "left"}
        assert _capture["diamond_merge_right"] == {"val": "right"}


# ---------------------------------------------------------------------------
# Tests — inject_as
# ---------------------------------------------------------------------------


class TestInjectAs:
    async def test_inject_as_renames_kwarg(self, queue, worker):
        dag = DAG("inject_as")
        dag.task(ias_extract)
        dag.task(
            ias_transform,
            depends_on=[ias_extract],
            inject_as={"ias_extract": "data"},
        )

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["ias_transform_input"] == {"raw": "data"}


# ---------------------------------------------------------------------------
# Tests — Root task (no injection)
# ---------------------------------------------------------------------------


class TestRootTaskNoInjection:
    async def test_root_task_executes_without_injection(self, queue, worker):
        dag = DAG("root_only")
        dag.task(root_only)

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["root_only_called"] is True


# ---------------------------------------------------------------------------
# Tests — Payload override
# ---------------------------------------------------------------------------


class TestPayloadOverride:
    async def test_payload_kwargs_override_injection(self, queue, worker):
        dag = DAG("override")
        dag.task(ovr_parent)
        dag.task(ovr_child, depends_on=[ovr_parent])

        run = await dag.run(
            queue,
            payload={"ovr_child": {"ovr_parent": "from_payload"}},
        )

        await _wait_all_completed(run.jobs)

        assert _capture["ovr_child_input"] == "from_payload"


# ---------------------------------------------------------------------------
# Tests — Parent returns None
# ---------------------------------------------------------------------------


class TestParentReturnsNone:
    async def test_none_result_injected_as_none(self, queue, worker):
        dag = DAG("none_result")
        dag.task(none_parent)
        dag.task(none_child, depends_on=[none_parent])

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        assert _capture["none_child_called"] is True
        assert _capture["none_child_input"] is None


# ---------------------------------------------------------------------------
# Tests — Complex types survive JSON round-trip
# ---------------------------------------------------------------------------


class TestComplexTypes:
    async def test_complex_result_survives_json_roundtrip(self, queue, worker):
        dag = DAG("complex")
        dag.task(complex_parent)
        dag.task(complex_child, depends_on=[complex_parent])

        run = await dag.run(queue)

        await _wait_all_completed(run.jobs)

        expected = {"nested": {"list": [1, "two", 3.0]}, "flag": True, "count": 42}
        assert _capture["complex_child_input"] == expected


# ---------------------------------------------------------------------------
# Tests — Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    async def test_register_idempotent(self, queue):
        dag = DAG("idempotent")
        dag.task(root_only)

        dag.register(queue)
        task_count_after_first = len(queue._tasks)

        dag.register(queue)

        # Second call is a no-op: no new tasks registered with qler
        assert len(queue._tasks) == task_count_after_first
        assert len(dag._registered_queues) == 1

    async def test_run_auto_registers(self, queue):
        dag = DAG("auto_reg")
        dag.task(root_only)

        assert len(dag._registered_queues) == 0

        run = await dag.run(queue)

        assert id(queue) in dag._registered_queues
        assert len(run.jobs) == 1
