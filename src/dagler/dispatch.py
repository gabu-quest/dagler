"""Fan-out dispatcher — creates map/reduce/trailing/next-stage jobs at runtime."""

from __future__ import annotations

import json
import logging

from qler import current_job, current_queue
from qler.models.job import Job
from sqler import F

logger = logging.getLogger(__name__)


async def _dagler_fanout_dispatch(**kwargs: object) -> None:
    """Internal dispatcher task: reads parent result and fans out dynamically.

    Receives ``_dagler_spec`` via payload kwargs with the fan-out
    specification.  Creates map jobs (one per parent result item), an
    optional reduce job, trailing tasks, and optionally a next-stage
    dispatcher (cascading chain) via ``current_queue().enqueue_many()``.
    Updates the DagRun record with new job ULIDs and task names.

    Supports two spec formats:
    - **M9 staged**: ``{"stage": {...}, "remaining_stages": [...], ...}``
    - **M6 legacy**: flat dict with ``map_task_path`` etc. at top level
    """
    spec = kwargs["_dagler_spec"]

    current_job()
    queue = current_queue()

    # Detect format: M9 staged vs M6 legacy
    if "stage" in spec:
        stage = spec["stage"]
        remaining = spec.get("remaining_stages", [])
    else:
        # M6 backward compat: flat format
        stage = spec
        remaining = []

    # Read the map-parent's result from DB
    parent_ulid = spec["parent_job_ulid"]
    parent_job = await Job.query().filter(F("ulid") == parent_ulid).first()
    if parent_job is None:
        msg = f"Dispatcher: parent job {parent_ulid} not found"
        raise RuntimeError(msg)

    parent_result = json.loads(parent_job.result_json) if parent_job.result_json else []

    # Coerce to list (TypeError if not iterable)
    items = list(parent_result)

    correlation_id = spec["correlation_id"]
    map_task_path = stage["map_task_path"]
    map_task_name = stage["map_task_name"]
    map_inject_param = stage["map_inject_param"]

    reduce_task_path = stage.get("reduce_task_path")
    reduce_task_name = stage.get("reduce_task_name")

    # M9 uses "trailing_tasks", M6 uses "post_reduce_tasks"
    trailing_tasks = stage.get("trailing_tasks") or stage.get("post_reduce_tasks", [])

    # Build job specs
    all_specs: list[dict] = []
    new_task_names: list[str] = []

    # Map jobs: one per item
    map_indices: list[int] = []
    for item in items:
        idx = len(all_specs)
        map_indices.append(idx)
        all_specs.append({
            "task_path": map_task_path,
            "kwargs": {map_inject_param: item},
            "correlation_id": correlation_id,
        })
        new_task_names.append(map_task_name)

    # Reduce job: depends on all map jobs
    reduce_index: int | None = None
    if reduce_task_path is not None:
        reduce_index = len(all_specs)
        all_specs.append({
            "task_path": reduce_task_path,
            "depends_on": map_indices,
            "correlation_id": correlation_id,
        })
        new_task_names.append(reduce_task_name)

    # Trailing tasks: depend on reduce (or on all maps if no reduce)
    prev_index = reduce_index if reduce_index is not None else None
    for pt in trailing_tasks:
        idx = len(all_specs)
        dep = [prev_index] if prev_index is not None else map_indices
        all_specs.append({
            "task_path": pt["task_path"],
            "depends_on": dep,
            "correlation_id": correlation_id,
        })
        new_task_names.append(pt["name"])
        prev_index = idx

    # Cascading: create next-stage dispatcher if remaining stages exist
    next_dispatcher_index: int | None = None
    if remaining:
        # The next dispatcher depends on the last job of this stage
        # (last trailing task, or reduce, or all maps)
        if prev_index is not None:
            next_dep = [prev_index]
        else:
            next_dep = map_indices

        next_stage_spec = remaining[0]
        next_dispatcher_spec = {
            "stage": next_stage_spec,
            "remaining_stages": remaining[1:],
            "correlation_id": correlation_id,
            "dag_name": spec.get("dag_name", "?"),
        }

        next_dispatcher_index = len(all_specs)
        next_map_name = next_stage_spec["map_task_name"]
        all_specs.append({
            "task_path": "dagler.dispatch._dagler_fanout_dispatch",
            "depends_on": next_dep,
            "correlation_id": correlation_id,
            "kwargs": {"_dagler_spec": next_dispatcher_spec},
        })
        new_task_names.append(f"_dispatcher_{next_map_name}")

    # Pre-generate ULIDs so we can set parent_job_ulid in next-stage
    # dispatcher spec BEFORE enqueue_many() — same race-elimination
    # strategy as dag.py _run_fanout().
    from qler import generate_ulid

    for s in all_specs:
        s["ulid"] = generate_ulid()

    if next_dispatcher_index is not None and all_specs:
        # Determine parent ULID for the next-stage dispatcher
        if prev_index is not None and prev_index != next_dispatcher_index:
            parent_ulid_for_next = all_specs[prev_index]["ulid"]
        elif reduce_index is not None:
            parent_ulid_for_next = all_specs[reduce_index]["ulid"]
        elif map_indices:
            parent_ulid_for_next = all_specs[map_indices[-1]]["ulid"]
        else:
            parent_ulid_for_next = parent_ulid
        all_specs[next_dispatcher_index]["kwargs"]["_dagler_spec"]["parent_job_ulid"] = parent_ulid_for_next

    if all_specs:
        new_jobs = await queue.enqueue_many(all_specs)
    else:
        new_jobs = []

    # Update DagRun with new job ULIDs and task names
    from dagler.run import DagRun

    runs = await DagRun.query().filter(
        F("correlation_id") == correlation_id,
    ).all()

    if runs:
        dag_run = runs[0]
        dag_run.jobs_manifest = dag_run.jobs_manifest + [
            {"ulid": j.ulid, "task_name": name}
            for j, name in zip(new_jobs, new_task_names)
        ]
        await dag_run.save()

    logger.info(
        "Dispatcher created %d dynamic jobs for DAG %r (correlation=%s)",
        len(new_jobs),
        spec.get("dag_name", "?"),
        correlation_id,
    )
