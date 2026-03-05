"""Tests for M6+M9: Dynamic fan-out (map/reduce), multi-stage."""

import asyncio
import contextlib
import json

import pytest
import pytest_asyncio
from dagler import DAG, DaglerError, DagRun, DependencyError, TaskNode

# ── Module-level task functions (decorators reject nested) ─────────────

_capture: dict[str, object] = {}


async def _fetch():
    return ["a", "b", "c"]


async def _fetch_empty():
    return []


async def _fetch_single():
    return ["only"]


async def _fetch2():
    return ["x", "y"]


async def _process(item):
    return item.upper()


async def _combine(results):
    return ",".join(results)


async def _finalize(_combine=None):
    _capture["finalize"] = _combine
    return f"done:{_combine}"


async def _identity(x):
    return x


async def _no_params():
    return 42


async def _post_reduce_a(_combine=None):
    _capture["post_a"] = _combine
    return f"a:{_combine}"


async def _post_reduce_b(_post_reduce_a=None):
    _capture["post_b"] = _post_reduce_a
    return f"b:{_post_reduce_a}"


async def _root_a():
    return 1


async def _root_b():
    return 2


async def _child_a(_root_a=None):
    return _root_a + 1


async def _map_item(item):
    return item


async def _reduce_r(results):
    return results


# ── M9 multi-stage task functions ─────────────────────────────────────

async def _fetch_nums():
    return [1, 2, 3]


async def _double(n):
    return n * 2


async def _sum_all(results):
    return sum(results)


async def _fetch_words():
    return ["hello", "world"]


async def _shout(word):
    return word.upper()


async def _join_words(results):
    return " ".join(results)


async def _split_chars(text):
    """Takes a string, returns list of chars for next map stage."""
    return list(text)


async def _char_upper(c):
    return c.upper()


async def _join_chars(results):
    return "".join(results)


async def _transform(_sum_all=None):
    """Regular task between stages — doubles the sum."""
    _capture["transform"] = _sum_all
    return [_sum_all, _sum_all * 10]


async def _stage2_map(item):
    return item + 1


async def _stage2_reduce(results):
    return results


async def _stage3_map(item):
    return item * 100


async def _stage3_reduce(results):
    return sorted(results)


# ── Fixtures ───────────────────────────────────────────────────────────

@pytest_asyncio.fixture(autouse=True)
async def _isolate():
    """Clear DAG registry and capture dict before each test."""
    DAG._registry.clear()
    _capture.clear()
    yield
    DAG._registry.clear()
    _capture.clear()


@pytest_asyncio.fixture
async def fanout_worker(queue):
    """Worker with concurrency=4 for fan-out tests.  Auto-stopped after test."""
    from qler.worker import Worker

    w = Worker(queue, poll_interval=0.01, concurrency=4, shutdown_timeout=2.0)
    worker_task = asyncio.create_task(w.run())
    yield w
    w._running = False
    await asyncio.sleep(0.05)
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker_task


# ── TestMapTaskDecoration ──────────────────────────────────────────────


class TestMapTaskDecoration:

    def test_kind_is_map(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        node = dag.tasks["_process"]
        assert node.kind == "map"

    def test_inject_param_from_signature(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        node = dag.tasks["_process"]
        assert node.inject_param == "item"

    def test_plain_task_has_default_kind(self):
        dag = DAG("t")
        dag.task(_fetch)
        node = dag.tasks["_fetch"]
        assert node.kind == "task"
        assert node.inject_param == ""

    def test_requires_exactly_one_dependency(self):
        dag = DAG("t")
        dag.task(_root_a)
        dag.task(_root_b)
        with pytest.raises(DependencyError, match="exactly 1 parent"):
            dag.map_task(depends_on=[_root_a, _root_b])(_map_item)

    def test_requires_at_least_one_dependency(self):
        dag = DAG("t")
        with pytest.raises(DependencyError, match="exactly 1 parent"):
            dag.map_task(depends_on=[])(_map_item)

    def test_rejects_nested_function(self):
        dag = DAG("t")
        dag.task(_fetch)

        with pytest.raises(DaglerError, match="nested function"):
            def make():
                async def inner(item):
                    return item
                dag.map_task(depends_on=[_fetch])(inner)
            make()

    def test_rejects_no_params(self):
        dag = DAG("t")
        dag.task(_fetch)
        with pytest.raises(DaglerError, match="at least one parameter"):
            dag.map_task(depends_on=[_fetch])(_no_params)

    def test_parameterized_decorator_form(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        assert dag.tasks["_process"].kind == "map"


# ── TestReduceTaskDecoration ───────────────────────────────────────────


class TestReduceTaskDecoration:

    def test_kind_is_reduce(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)
        node = dag.tasks["_combine"]
        assert node.kind == "reduce"

    def test_inject_param_from_signature(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)
        node = dag.tasks["_combine"]
        assert node.inject_param == "results"

    def test_must_depend_on_map_task(self):
        dag = DAG("t")
        dag.task(_fetch)
        with pytest.raises(DependencyError, match="must depend on a map_task"):
            dag.reduce_task(depends_on=[_fetch])(_reduce_r)

    def test_requires_exactly_one_dependency(self):
        dag = DAG("t")
        dag.task(_root_a)
        dag.map_task(depends_on=[_root_a])(_map_item)
        dag.task(_root_b)
        with pytest.raises(DependencyError, match="exactly 1 task"):
            dag.reduce_task(depends_on=[_map_item, _root_b])(_reduce_r)

    def test_rejects_no_params(self):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        with pytest.raises(DaglerError, match="at least one parameter"):
            dag.reduce_task(depends_on=[_process])(_no_params)


# ── TestValidation ─────────────────────────────────────────────────────


class TestValidation:

    def test_two_independent_stages_validate(self):
        """Multiple map/reduce pairs in one DAG should validate."""
        dag = DAG("t")
        dag.task(_root_a)
        dag.task(_root_b)
        dag.map_task(depends_on=[_root_a])(_map_item)
        dag.reduce_task(depends_on=[_map_item])(_reduce_r)

        # Second map/reduce pair
        node_m2 = TaskNode(
            fn=_process,
            name="m2",
            task_path="tests.test_fanout.m2",
            dependencies=("_root_b",),
            kind="map",
            inject_param="item",
        )
        dag._tasks["m2"] = node_m2

        node_r2 = TaskNode(
            fn=_combine,
            name="r2",
            task_path="tests.test_fanout.r2",
            dependencies=("m2",),
            kind="reduce",
            inject_param="results",
        )
        dag._tasks["r2"] = node_r2

        order = dag.validate()
        assert len(order) == 6
        assert "_root_a" in order
        assert "_root_b" in order
        assert "_map_item" in order
        assert "_reduce_r" in order
        assert "m2" in order
        assert "r2" in order

    def test_chained_stages_validate(self):
        """reduce → map chain should validate (M9 core scenario)."""
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        # Second stage: map depends on reduce output
        node_m2 = TaskNode(
            fn=_double,
            name="stage2_map",
            task_path="tests.test_fanout.stage2_map",
            dependencies=("_combine",),
            kind="map",
            inject_param="n",
        )
        dag._tasks["stage2_map"] = node_m2

        order = dag.validate()
        assert "stage2_map" in order
        # stage2_map must come after _combine
        assert order.index("stage2_map") > order.index("_combine")

    def test_map_depends_on_map_rejected(self):
        """map_task depending on another map_task is not supported."""
        dag = DAG("t")
        dag.task(_root_a)
        dag.map_task(depends_on=[_root_a])(_map_item)

        # Force a second map that depends on the first map
        node = TaskNode(
            fn=_process,
            name="m2",
            task_path="tests.test_fanout.m2",
            dependencies=("_map_item",),
            kind="map",
            inject_param="item",
        )
        dag._tasks["m2"] = node

        with pytest.raises(DaglerError, match="map.*map.*not supported"):
            dag.validate()

    def test_reduce_without_map_still_rejected(self):
        """Existing constraint: reduce without map is still invalid."""
        dag = DAG("t")
        dag.task(_root_a)

        # Force a reduce node without any map
        node = TaskNode(
            fn=_reduce_r,
            name="r",
            task_path="tests.test_fanout.r",
            dependencies=("_root_a",),
            kind="reduce",
            inject_param="results",
        )
        dag._tasks["r"] = node

        with pytest.raises(DaglerError, match="reduce_task without a map_task"):
            dag.validate()

    def test_normal_dag_still_validates(self):
        dag = DAG("t")
        dag.task(_root_a)
        dag.task(depends_on=[_root_a])(_child_a)
        order = dag.validate()
        assert order == ["_root_a", "_child_a"]


# ── TestFanOutSubmission ───────────────────────────────────────────────


class TestFanOutSubmission:

    async def test_correct_job_count(self, queue):
        """Static tasks + dispatcher = static_count + 1."""
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        # 1 static (fetch) + 1 dispatcher = 2
        assert len(run.jobs) == 2
        assert len(run.job_ulids) == 2
        assert run.task_names == ["_fetch", "_dispatcher__process"]

    async def test_dispatcher_in_task_names(self, queue):
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        assert "_dispatcher__process" in run.task_names

    async def test_dispatcher_depends_on_parent(self, queue):
        """Dispatcher job depends on the map parent (exact dep list)."""
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        fetch_idx = run.task_names.index("_fetch")
        dispatcher_idx = run.task_names.index("_dispatcher__process")
        fetch_job = run.jobs[fetch_idx]
        dispatcher_job = run.jobs[dispatcher_idx]
        assert dispatcher_job.dependencies == [fetch_job.ulid]

    async def test_dispatcher_payload_has_spec(self, queue):
        """Dispatcher payload contains correct _dagler_spec with exact parent ULID."""
        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        fetch_job = run.jobs[run.task_names.index("_fetch")]
        dispatcher_job = run.jobs[run.task_names.index("_dispatcher__process")]
        payload = json.loads(dispatcher_job.payload_json)
        spec = payload["kwargs"]["_dagler_spec"]
        # M9 format: stage is nested
        stage = spec["stage"]
        assert stage["map_task_name"] == "_process"
        assert stage["map_inject_param"] == "item"
        assert spec["correlation_id"] == run.correlation_id
        assert spec["parent_job_ulid"] == fetch_job.ulid

    async def test_dispatcher_payload_persisted_to_db(self, queue):
        """Dispatcher payload survives the DB round-trip (save after mutation)."""
        from qler.models.job import Job
        from sqler import F

        dag = DAG("t")
        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        fetch_job = run.jobs[run.task_names.index("_fetch")]
        dispatcher_ulid = run.job_ulids[run.task_names.index("_dispatcher__process")]

        # Re-fetch from DB to verify save() persisted correctly
        db_job = await Job.query().filter(F("ulid") == dispatcher_ulid).first()
        assert db_job is not None
        payload = json.loads(db_job.payload_json)
        spec = payload["kwargs"]["_dagler_spec"]
        assert spec["parent_job_ulid"] == fetch_job.ulid

    async def test_static_path_unchanged(self, queue):
        """Non-fanout DAGs still use the static path (no dispatcher)."""
        dag = DAG("t")
        dag.task(_root_a)
        dag.task(depends_on=[_root_a])(_child_a)

        run = await dag.run(queue)
        assert not any(n.startswith("_dispatcher") for n in run.task_names)
        assert len(run.jobs) == 2
        assert run.task_names == ["_root_a", "_child_a"]


# ── TestFanOutExecution ────────────────────────────────────────────────


class TestFanOutExecution:

    async def test_basic_map_reduce(self, queue, fanout_worker):
        """fetch → map(process) → reduce(combine): the canonical test."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"

        # Verify reduce collected results from all 3 map jobs
        combine_idx = run.task_names.index("_combine")
        combine_job = run._jobs[combine_idx]
        result = json.loads(combine_job.result_json)
        assert result == "A,B,C"

    async def test_map_reduce_with_post_reduce(self, queue, fanout_worker):
        """fetch → map → reduce → finalize chain."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)
        dag.task(depends_on=[_combine])(_finalize)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        assert _capture["finalize"] == "A,B,C"

    async def test_empty_parent_result(self, queue, fanout_worker):
        """Empty parent result → no map jobs, reduce runs with []."""
        dag = DAG("t")

        dag.task(_fetch_empty)
        dag.map_task(depends_on=[_fetch_empty])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        combine_idx = run.task_names.index("_combine")
        combine_job = run._jobs[combine_idx]
        result = json.loads(combine_job.result_json)
        # ",".join([]) == ""
        assert result == ""

    async def test_empty_parent_result_map_only(self, queue, fanout_worker):
        """Empty parent + no reduce → dispatcher creates zero map jobs."""
        dag = DAG("t")

        dag.task(_fetch_empty)
        dag.map_task(depends_on=[_fetch_empty])(_process)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        # fetch + dispatcher only, no map jobs created
        assert len(run.job_ulids) == 2

    async def test_single_item(self, queue, fanout_worker):
        """Single item → one map job, reduce gets [result]."""
        dag = DAG("t")

        dag.task(_fetch_single)
        dag.map_task(depends_on=[_fetch_single])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        combine_idx = run.task_names.index("_combine")
        combine_job = run._jobs[combine_idx]
        result = json.loads(combine_job.result_json)
        assert result == "ONLY"

    async def test_order_preservation(self, queue, fanout_worker):
        """Map results should be in the same order as the parent's iterable."""
        dag = DAG("t")

        dag.task(_fetch)  # returns ["a", "b", "c"]
        dag.map_task(depends_on=[_fetch])(_process)  # each .upper()
        dag.reduce_task(depends_on=[_process])(_combine)  # ",".join

        run = await dag.run(queue)
        await run.wait(timeout=10)

        combine_idx = run.task_names.index("_combine")
        combine_job = run._jobs[combine_idx]
        result = json.loads(combine_job.result_json)
        # Order preserved: A,B,C (not some random permutation)
        assert result == "A,B,C"

    async def test_map_without_reduce(self, queue, fanout_worker):
        """map_task without reduce — fan-out only, no collection."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        # Should have: fetch + dispatcher + 3 process jobs = 5 total
        assert len(run.job_ulids) == 5
        assert run.task_names.count("_process") == 3
        assert "_combine" not in run.task_names

    async def test_post_reduce_chain(self, queue, fanout_worker):
        """fetch → map → reduce → post_a → post_b: full chain."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)
        dag.task(depends_on=[_combine])(_post_reduce_a)
        dag.task(depends_on=[_post_reduce_a])(_post_reduce_b)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"
        assert _capture["post_a"] == "A,B,C"
        assert _capture["post_b"] == "a:A,B,C"


# ── TestDagRunTracking ─────────────────────────────────────────────────


class TestDagRunTracking:

    async def test_dynamic_jobs_in_dagrun(self, queue, fanout_worker):
        """DagRun.job_ulids includes dynamically created jobs after wait()."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        initial_count = len(run.job_ulids)
        assert initial_count == 2  # fetch + dispatcher

        await run.wait(timeout=10)

        # After wait, should include: fetch + dispatcher + 3 process + combine = 6
        assert len(run.job_ulids) == 6

    async def test_status_reflects_all_jobs(self, queue, fanout_worker):
        """Status is 'completed' only when ALL jobs (static + dynamic) finish."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        assert run.status == "completed"

    async def test_dagrun_persisted_with_dynamic_jobs(self, queue, fanout_worker):
        """DagRun record in DB reflects dynamic jobs after completion."""
        from sqler import F

        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        # Re-fetch from DB
        db_run = await DagRun.query().filter(
            F("correlation_id") == run.correlation_id,
        ).first()

        assert db_run is not None
        assert db_run.status == "completed"
        assert len(db_run.job_ulids) == 6  # fetch + dispatcher + 3 process + combine

    async def test_task_names_updated(self, queue, fanout_worker):
        """DagRun.task_names includes dynamic task names after wait()."""
        dag = DAG("t")

        dag.task(_fetch)
        dag.map_task(depends_on=[_fetch])(_process)
        dag.reduce_task(depends_on=[_process])(_combine)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        # Should include: _fetch, _dispatcher__process, _process (×3), _combine
        assert "_fetch" in run.task_names
        assert "_dispatcher__process" in run.task_names
        assert run.task_names.count("_process") == 3
        assert "_combine" in run.task_names


# ── TestMultiStageExecution (M9) ──────────────────────────────────────


class TestMultiStageExecution:

    async def test_two_independent_map_reduce(self, queue, fanout_worker):
        """Two independent fan-out stages execute, results isolated."""
        dag = DAG("t")

        # Stage A: fetch_nums → double → sum_all
        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)

        # Stage B: fetch_words → shout → join_words
        dag.task(_fetch_words)
        dag.map_task(depends_on=[_fetch_words])(_shout)
        dag.reduce_task(depends_on=[_shout])(_join_words)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        assert run.status == "completed"

        # Stage A: [1,2,3] → [2,4,6] → sum=12
        sum_idx = run.task_names.index("_sum_all")
        sum_job = run._jobs[sum_idx]
        assert json.loads(sum_job.result_json) == 12

        # Stage B: ["hello","world"] → ["HELLO","WORLD"] → "HELLO WORLD"
        join_idx = run.task_names.index("_join_words")
        join_job = run._jobs[join_idx]
        assert json.loads(join_job.result_json) == "HELLO WORLD"

    async def test_chained_map_reduce(self, queue, fanout_worker):
        """reduce(A) output feeds map(B), final result correct.

        fetch_nums → double → sum_all → transform → stage2_map → stage2_reduce
        [1,2,3]  → [2,4,6] → 12      → [12, 120] → [13, 121] → [13, 121]
        """
        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)
        dag.task(depends_on=[_sum_all])(_transform)
        dag.map_task(depends_on=[_transform])(_stage2_map)
        dag.reduce_task(depends_on=[_stage2_map])(_stage2_reduce)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        assert run.status == "completed"

        # Verify transform ran
        assert _capture["transform"] == 12

        # Stage 2 reduce: [12, 120] → stage2_map(+1) → [13, 121]
        reduce_idx = run.task_names.index("_stage2_reduce")
        reduce_job = run._jobs[reduce_idx]
        result = json.loads(reduce_job.result_json)
        assert sorted(result) == [13, 121]

    async def test_three_chained_stages(self, queue, fanout_worker):
        """A→B→C chain with cascading dispatchers.

        fetch_nums → double → sum_all → transform → stage2_map → stage2_reduce
                                                              ↓
                                              stage3_map → stage3_reduce
        """
        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)
        dag.task(depends_on=[_sum_all])(_transform)
        dag.map_task(depends_on=[_transform])(_stage2_map)
        dag.reduce_task(depends_on=[_stage2_map])(_stage2_reduce)
        # stage2_reduce returns [13, 121]
        dag.map_task(depends_on=[_stage2_reduce])(_stage3_map)
        dag.reduce_task(depends_on=[_stage3_map])(_stage3_reduce)

        run = await dag.run(queue)
        await run.wait(timeout=20)

        assert run.status == "completed"

        # Stage 3: [13, 121] → [1300, 12100] → sorted → [1300, 12100]
        reduce_idx = run.task_names.index("_stage3_reduce")
        reduce_job = run._jobs[reduce_idx]
        result = json.loads(reduce_job.result_json)
        assert result == [1300, 12100]

    async def test_regular_task_between_stages(self, queue, fanout_worker):
        """reduce(A) → transform → map(B) works — regular task between stages."""
        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)
        dag.task(depends_on=[_sum_all])(_transform)  # returns [12, 120]
        dag.map_task(depends_on=[_transform])(_stage2_map)  # each +1
        dag.reduce_task(depends_on=[_stage2_map])(_stage2_reduce)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        assert run.status == "completed"
        assert _capture["transform"] == 12

        reduce_idx = run.task_names.index("_stage2_reduce")
        reduce_job = run._jobs[reduce_idx]
        result = json.loads(reduce_job.result_json)
        assert sorted(result) == [13, 121]

    async def test_map_only_stage(self, queue, fanout_worker):
        """Map-only stage (no reduce) still works in multi-stage context."""
        dag = DAG("t")

        # Independent stages: one with reduce, one without
        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)

        dag.task(_fetch_words)
        dag.map_task(depends_on=[_fetch_words])(_shout)
        dag.reduce_task(depends_on=[_shout])(_join_words)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        assert run.status == "completed"

        # Map-only: 3 _double jobs created
        assert run.task_names.count("_double") == 3

        # Map+reduce: words joined
        join_idx = run.task_names.index("_join_words")
        join_job = run._jobs[join_idx]
        assert json.loads(join_job.result_json) == "HELLO WORLD"


# ── TestMultiStageDagRunTracking (M9) ─────────────────────────────────


class TestMultiStageDagRunTracking:

    async def test_wait_discovers_cascading_jobs(self, queue, fanout_worker):
        """wait() loop catches all waves of cascading dispatchers."""
        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)
        dag.task(depends_on=[_sum_all])(_transform)
        dag.map_task(depends_on=[_transform])(_stage2_map)
        dag.reduce_task(depends_on=[_stage2_map])(_stage2_reduce)

        run = await dag.run(queue)

        # Initially: fetch_nums + dispatcher for stage 1 = 2 jobs
        assert len(run.job_ulids) == 2

        await run.wait(timeout=15)

        # After wait: all jobs from both waves discovered
        # Wave 1: 3 _double + _sum_all + _transform + dispatcher_stage2 = 6
        # Wave 2: 2 _stage2_map + _stage2_reduce = 3
        # Total: 2 (static) + 6 (wave 1) + 3 (wave 2) = 11
        assert len(run.job_ulids) == 11
        assert run.status == "completed"

    async def test_all_dynamic_jobs_in_dagrun(self, queue, fanout_worker):
        """job_ulids includes jobs from all dispatchers."""
        from sqler import F

        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)

        dag.task(_fetch_words)
        dag.map_task(depends_on=[_fetch_words])(_shout)
        dag.reduce_task(depends_on=[_shout])(_join_words)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        # Re-fetch from DB
        db_run = await DagRun.query().filter(
            F("correlation_id") == run.correlation_id,
        ).first()

        assert db_run is not None
        assert db_run.status == "completed"
        # fetch_nums + fetch_words + 2 dispatchers = 4 static
        # Stage A: 3 _double + _sum_all = 4 dynamic
        # Stage B: 2 _shout + _join_words = 3 dynamic
        # Total: 4 + 4 + 3 = 11
        assert len(db_run.job_ulids) == 11

    async def test_task_names_include_all_dispatchers(self, queue, fanout_worker):
        """Multiple _dispatcher_* entries present for independent stages."""
        dag = DAG("t")

        dag.task(_fetch_nums)
        dag.map_task(depends_on=[_fetch_nums])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)

        dag.task(_fetch_words)
        dag.map_task(depends_on=[_fetch_words])(_shout)
        dag.reduce_task(depends_on=[_shout])(_join_words)

        run = await dag.run(queue)

        # Before execution: should have 2 dispatchers in static names
        dispatcher_names = [n for n in run.task_names if n.startswith("_dispatcher")]
        assert len(dispatcher_names) == 2
        assert "_dispatcher__double" in dispatcher_names
        assert "_dispatcher__shout" in dispatcher_names


# ── Stress test task functions ───────────────────────────────────────


async def _fetch_100():
    return list(range(100))


async def _fetch_200():
    return list(range(200))


async def _square(n):
    return n * n


async def _sum_results(results):
    return sum(results)


async def _fetch_alpha():
    return ["a", "b", "c", "d", "e"]


async def _upper(letter):
    return letter.upper()


async def _concat(results):
    return "".join(sorted(results))


async def _failing_fetch():
    return [1, 2, 3]


async def _explode(item):
    if item == 2:
        raise ValueError("boom on item 2")
    return item * 10


async def _should_not_run(results):
    _capture["should_not_run"] = True
    return results


async def _non_iterable_parent():
    return 42  # not iterable — dispatcher should fail


async def _identity_map(x):
    return x


async def _fetch_20():
    return list(range(20))


async def _collect_results(results):
    """Identity reduce — returns the list as-is for order verification."""
    return results


async def _fetch_50():
    return list(range(50))


# ── TestStress ───────────────────────────────────────────────────────


class TestStress:

    async def test_large_fanout_100_items(self, queue, fanout_worker):
        """100-item fan-out: all map jobs execute, reduce collects all results."""
        dag = DAG("t")

        dag.task(_fetch_100)
        dag.map_task(depends_on=[_fetch_100])(_square)
        dag.reduce_task(depends_on=[_square])(_sum_results)

        run = await dag.run(queue)
        await run.wait(timeout=30)

        assert run.status == "completed"

        # Verify exact result: sum of squares 0² + 1² + ... + 99²
        expected = sum(i * i for i in range(100))
        assert len(run.jobs) == 103  # guard before indexing
        reduce_job = run.jobs[run.task_names.index("_sum_results")]
        result = json.loads(reduce_job.result_json)
        assert result == expected  # 328350

        # Verify job count: fetch + dispatcher + 100 maps + reduce = 103
        assert len(run.job_ulids) == 103
        assert run.task_names.count("_square") == 100

    async def test_large_fanout_map_only(self, queue, fanout_worker):
        """100-item map-only (no reduce): wait() discovers all dynamic jobs."""
        dag = DAG("t")

        dag.task(_fetch_100)
        dag.map_task(depends_on=[_fetch_100])(_identity_map)

        run = await dag.run(queue)
        await run.wait(timeout=30)

        assert run.status == "completed"

        # fetch + dispatcher + 100 maps = 102
        assert len(run.job_ulids) == 102
        assert run.task_names.count("_identity_map") == 100

        # All jobs in terminal state
        assert len(run.jobs) == 102
        for job in run.jobs:
            assert job.status == "completed"

    async def test_order_preservation_large(self, queue, fanout_worker):
        """Order of reduce results matches original item order under concurrency.

        Uses an identity map + list-collect reduce over 20 items.
        Non-commutative: exact list equality proves ULID-sort order preservation.
        """
        dag = DAG("t")

        dag.task(_fetch_20)
        dag.map_task(depends_on=[_fetch_20])(_identity_map)
        dag.reduce_task(depends_on=[_identity_map])(_collect_results)

        run = await dag.run(queue)
        await run.wait(timeout=30)

        assert run.status == "completed"

        assert len(run.jobs) == 23  # fetch + dispatcher + 20 maps + reduce
        reduce_job = run.jobs[run.task_names.index("_collect_results")]
        result = json.loads(reduce_job.result_json)
        assert result == list(range(20))  # EXACT order, not set equality

    async def test_concurrent_dag_runs_isolated(self, queue, fanout_worker):
        """Two concurrent fan-out DAG runs: results don't cross-contaminate."""
        from qler.models.job import Job
        from sqler import F

        dag1 = DAG("dag_nums")
        dag1.task(_fetch_100)
        dag1.map_task(depends_on=[_fetch_100])(_square)
        dag1.reduce_task(depends_on=[_square])(_sum_results)

        dag2 = DAG("dag_alpha")
        dag2.task(_fetch_alpha)
        dag2.map_task(depends_on=[_fetch_alpha])(_upper)
        dag2.reduce_task(depends_on=[_upper])(_concat)

        # Submit both concurrently
        run1 = await dag1.run(queue)
        run2 = await dag2.run(queue)

        # Different correlation IDs
        assert run1.correlation_id != run2.correlation_id

        # Wait concurrently
        await asyncio.gather(
            run1.wait(timeout=30),
            run2.wait(timeout=30),
        )

        assert run1.status == "completed"
        assert run2.status == "completed"

        # dag1: sum of 100 squares
        assert len(run1.jobs) == 103
        reduce1_job = run1.jobs[run1.task_names.index("_sum_results")]
        assert json.loads(reduce1_job.result_json) == sum(i * i for i in range(100))

        # dag2: sorted uppercase letters
        assert len(run2.jobs) == 8
        reduce2_job = run2.jobs[run2.task_names.index("_concat")]
        assert json.loads(reduce2_job.result_json) == "ABCDE"

        # Job counts are independent
        assert len(run1.job_ulids) == 103  # fetch + disp + 100 maps + reduce
        assert len(run2.job_ulids) == 8   # fetch + disp + 5 maps + reduce

        # Isolation proof: every job belongs to its owning run's correlation_id
        all_run1_jobs = await Job.query().filter(
            F("ulid").in_list(run1.job_ulids),
        ).all()
        assert len(all_run1_jobs) == 103
        assert all(j.correlation_id == run1.correlation_id for j in all_run1_jobs)

        all_run2_jobs = await Job.query().filter(
            F("ulid").in_list(run2.job_ulids),
        ).all()
        assert len(all_run2_jobs) == 8
        assert all(j.correlation_id == run2.correlation_id for j in all_run2_jobs)

    async def test_error_propagation_cancels_reduce(self, queue, fanout_worker):
        """When a map job fails, downstream reduce is cancelled via qler cascade."""
        dag = DAG("t")

        dag.task(_failing_fetch)
        dag.map_task(depends_on=[_failing_fetch])(_explode)
        dag.reduce_task(depends_on=[_explode])(_should_not_run)

        run = await dag.run(queue)
        await run.wait(timeout=15)

        assert run.status == "failed"
        assert "should_not_run" not in _capture

        # Assert terminal state of every dynamic job.
        # Note: qler cascade-cancels DEPENDENTS, not siblings.
        # Sibling map jobs may complete before the cascade runs.
        explode_jobs = [
            (i, run.jobs[i])
            for i, name in enumerate(run.task_names)
            if name == "_explode"
        ]
        assert len(explode_jobs) == 3  # all three map jobs created
        explode_statuses = {j.status for _, j in explode_jobs}
        # All must be terminal (no pending/running left over)
        assert explode_statuses <= {"failed", "completed", "cancelled"}
        assert any(j.status == "failed" for _, j in explode_jobs)  # item 2 exploded

        # The reduce must be cancelled (depends on ALL maps, one failed → cascade)
        reduce_idx = run.task_names.index("_should_not_run")
        assert run.jobs[reduce_idx].status == "cancelled"

    async def test_non_iterable_parent_fails_dispatcher(self, queue, fanout_worker):
        """Dispatcher fails gracefully when parent result is not iterable."""
        dag = DAG("t")

        dag.task(_non_iterable_parent)
        dag.map_task(depends_on=[_non_iterable_parent])(_identity_map)

        run = await dag.run(queue)
        await run.wait(timeout=10)

        # The dispatcher should fail because int is not iterable
        assert run.status == "failed"

        # Verify the dispatcher job has an error
        disp_idx = run.task_names.index("_dispatcher__identity_map")
        disp_job = run.jobs[disp_idx]
        assert disp_job.status == "failed"
        assert disp_job.last_error is not None

    async def test_large_chained_fanout(self, queue, fanout_worker):
        """Large chained fan-out: 100 items through 2 stages."""
        dag = DAG("t")

        dag.task(_fetch_100)
        dag.map_task(depends_on=[_fetch_100])(_double)
        dag.reduce_task(depends_on=[_double])(_sum_all)
        # _sum_all returns a single int → _transform turns it into a list
        dag.task(depends_on=[_sum_all])(_transform)
        dag.map_task(depends_on=[_transform])(_stage2_map)
        dag.reduce_task(depends_on=[_stage2_map])(_stage2_reduce)

        run = await dag.run(queue)
        await run.wait(timeout=60)

        assert run.status == "completed"

        # Stage 1: sum([0*2, 1*2, ..., 99*2]) = 2 * sum(0..99) = 2 * 4950 = 9900
        assert len(run.jobs) > 0
        sum_job = run.jobs[run.task_names.index("_sum_all")]
        assert json.loads(sum_job.result_json) == 9900

        # Transform: [9900, 99000]
        assert _capture["transform"] == 9900

        # Stage 2: [9900+1, 99000+1] = [9901, 99001]
        reduce_job = run.jobs[run.task_names.index("_stage2_reduce")]
        assert sorted(json.loads(reduce_job.result_json)) == [9901, 99001]

    async def test_order_preservation_50_items(self, queue, fanout_worker):
        """50-item order preservation: non-commutative reduce proves exact ordering."""
        dag = DAG("t")

        dag.task(_fetch_50)
        dag.map_task(depends_on=[_fetch_50])(_square)
        dag.reduce_task(depends_on=[_square])(_collect_results)

        run = await dag.run(queue)
        await run.wait(timeout=30)

        assert run.status == "completed"

        # fetch + dispatcher + 50 maps + reduce = 53
        assert len(run.jobs) == 53

        reduce_job = run.jobs[run.task_names.index("_collect_results")]
        result = json.loads(reduce_job.result_json)
        # Must be in exact submission order: [0², 1², 2², ..., 49²]
        assert result == [i * i for i in range(50)]
