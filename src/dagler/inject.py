"""Result injection — wraps task functions to inject parent results as kwargs."""

import functools
import json

from qler import current_job
from qler.models.job import Job
from sqler import F


def _make_result_injector(fn, dep_names, inject_as):
    """Wrap *fn* so parent task results are injected as kwargs before calling it.

    For each dependency in *dep_names*, the wrapper reads the parent job's
    ``result_json`` and passes it as a keyword argument.  By default the kwarg
    name matches the parent task name; entries in *inject_as* override this
    (``{"parent_name": "kwarg_name"}``).

    Explicit payload kwargs take priority over injected results (via
    ``setdefault``).
    """
    name_map = {dep: inject_as.get(dep, dep) for dep in dep_names}

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        job = current_job()

        if job.dependencies:
            parent_jobs = await Job.query().filter(
                F("ulid").in_list(job.dependencies),
            ).all()

            for parent in parent_jobs:
                parent_name = parent.task.rsplit(".", 1)[-1]
                if parent_name in name_map:
                    kwarg_name = name_map[parent_name]
                    result = (
                        json.loads(parent.result_json)
                        if parent.result_json is not None
                        else None
                    )
                    kwargs.setdefault(kwarg_name, result)

        return await fn(*args, **kwargs)

    # Prevent inspect.signature from seeing fn's original params.
    # Without this, qler's signature validation would fail because
    # parent-result kwargs aren't in the job payload yet.
    del wrapper.__wrapped__

    return wrapper


def _make_reduce_injector(fn, inject_param):
    """Wrap *fn* so all parent (map) job results are collected into a list.

    Reads every parent job's ``result_json``, sorts by ULID (preserves
    submission order → item order), and injects the collected list as
    a single kwarg named *inject_param*.
    """

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        job = current_job()
        results = []

        if job.dependencies:
            parent_jobs = await Job.query().filter(
                F("ulid").in_list(job.dependencies),
            ).all()

            # Sort by ULID to preserve submission order
            parent_jobs.sort(key=lambda j: j.ulid)

            for parent in parent_jobs:
                result = (
                    json.loads(parent.result_json)
                    if parent.result_json is not None
                    else None
                )
                results.append(result)

        kwargs.setdefault(inject_param, results)
        return await fn(*args, **kwargs)

    del wrapper.__wrapped__
    return wrapper
