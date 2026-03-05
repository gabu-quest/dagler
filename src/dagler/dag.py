"""DAG — directed acyclic graph definition and validation."""

from __future__ import annotations

import inspect
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from dagler.exceptions import CycleError, DaglerError, DependencyError, DuplicateTaskError
from dagler.task import TaskNode

if TYPE_CHECKING:
    from qler.queue import Queue

    from dagler.run import DagRun

logger = logging.getLogger(__name__)


@dataclass
class FanoutStage:
    """One map(/reduce) fan-out stage within a DAG."""

    map_node: TaskNode
    reduce_node: TaskNode | None
    trailing_tasks: list[str] = field(default_factory=list)
    parent_stage_index: int = -1  # -1 = root (independent), else index of parent stage


class DAG:
    """A directed acyclic graph of task definitions.

    Tasks are registered via the :meth:`task` decorator. Dependencies are
    validated eagerly at decoration time; cycle detection runs lazily via
    :meth:`validate` (Kahn's algorithm).
    """

    _registry: ClassVar[dict[str, DAG]] = {}

    def __init__(self, name: str) -> None:
        self.name = name
        self._tasks: dict[str, TaskNode] = {}
        self._registered_queues: set[int] = set()
        DAG._registry[name] = self

    # ── Shared validation helpers ──────────────────────────────────────

    def _validate_common(self, fn: Callable, depends_on: list[Callable] | None) -> tuple[str, list[str]]:
        """Shared validation for all decorator types. Returns (name, dep_names)."""
        name = fn.__name__

        if fn.__qualname__ != fn.__name__:
            msg = (
                f"Cannot register nested function {fn.__qualname__!r}. "
                f"Use module-level functions."
            )
            raise DaglerError(msg)

        if name in self._tasks:
            msg = f"Task {name!r} already registered in DAG {self.name!r}"
            raise DuplicateTaskError(msg)

        dep_names: list[str] = []
        if depends_on:
            for dep in depends_on:
                dep_name = dep.__name__

                if dep_name == name:
                    msg = f"Task {name!r} cannot depend on itself"
                    raise DependencyError(msg)

                if dep_name not in self._tasks:
                    msg = (
                        f"Dependency {dep_name!r} not registered in DAG "
                        f"{self.name!r}. Register dependencies before dependents."
                    )
                    raise DependencyError(msg)

                dep_names.append(dep_name)

        return name, dep_names

    # ── @dag.task ──────────────────────────────────────────────────────

    def task(
        self,
        fn: Callable | None = None,
        *,
        depends_on: list[Callable] | None = None,
        inject_as: dict[str, str] | None = None,
    ) -> Callable:
        """Register a function as a task in this DAG.

        Supports both bare and parameterized decorator forms::

            @dag.task
            async def extract(): ...

            @dag.task(depends_on=[extract])
            async def transform(): ...
        """

        def decorator(fn: Callable) -> Callable:
            name, dep_names = self._validate_common(fn, depends_on)

            task_path = f"{fn.__module__}.{fn.__qualname__}"
            node = TaskNode(
                fn=fn,
                name=name,
                task_path=task_path,
                dependencies=tuple(dep_names),
                inject_as=inject_as or {},
            )
            self._tasks[name] = node

            logger.debug(
                "Registered task %r in DAG %r (deps: %s)",
                name,
                self.name,
                dep_names or "none",
            )

            return fn

        if fn is not None:
            return decorator(fn)
        return decorator

    # ── @dag.map_task ──────────────────────────────────────────────────

    def map_task(
        self,
        fn: Callable | None = None,
        *,
        depends_on: list[Callable] | None = None,
    ) -> Callable:
        """Register a map task that receives one item from its parent's result.

        Must depend on exactly one parent. The first parameter of *fn*
        becomes the injection point for each item.
        """

        def decorator(fn: Callable) -> Callable:
            name, dep_names = self._validate_common(fn, depends_on)

            if len(dep_names) != 1:
                msg = f"map_task {name!r} must depend on exactly 1 parent, got {len(dep_names)}"
                raise DependencyError(msg)

            # Determine the inject_param from function signature
            sig = inspect.signature(fn)
            params = [
                p.name for p in sig.parameters.values()
                if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY, p.KEYWORD_ONLY)
            ]
            if not params:
                msg = f"map_task {name!r} must accept at least one parameter"
                raise DaglerError(msg)

            inject_param = params[0]

            task_path = f"{fn.__module__}.{fn.__qualname__}"
            node = TaskNode(
                fn=fn,
                name=name,
                task_path=task_path,
                dependencies=tuple(dep_names),
                kind="map",
                inject_param=inject_param,
            )
            self._tasks[name] = node

            logger.debug(
                "Registered map_task %r in DAG %r (parent: %s, inject: %s)",
                name, self.name, dep_names[0], inject_param,
            )

            return fn

        if fn is not None:
            return decorator(fn)
        return decorator

    # ── @dag.reduce_task ───────────────────────────────────────────────

    def reduce_task(
        self,
        fn: Callable | None = None,
        *,
        depends_on: list[Callable] | None = None,
    ) -> Callable:
        """Register a reduce task that collects all map results into a list.

        Must depend on exactly one map_task.
        """

        def decorator(fn: Callable) -> Callable:
            name, dep_names = self._validate_common(fn, depends_on)

            if len(dep_names) != 1:
                msg = f"reduce_task {name!r} must depend on exactly 1 task, got {len(dep_names)}"
                raise DependencyError(msg)

            dep_node = self._tasks[dep_names[0]]
            if dep_node.kind != "map":
                msg = (
                    f"reduce_task {name!r} must depend on a map_task, "
                    f"but {dep_names[0]!r} has kind={dep_node.kind!r}"
                )
                raise DependencyError(msg)

            sig = inspect.signature(fn)
            params = [
                p.name for p in sig.parameters.values()
                if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY, p.KEYWORD_ONLY)
            ]
            if not params:
                msg = f"reduce_task {name!r} must accept at least one parameter"
                raise DaglerError(msg)

            inject_param = params[0]

            task_path = f"{fn.__module__}.{fn.__qualname__}"
            node = TaskNode(
                fn=fn,
                name=name,
                task_path=task_path,
                dependencies=tuple(dep_names),
                kind="reduce",
                inject_param=inject_param,
            )
            self._tasks[name] = node

            logger.debug(
                "Registered reduce_task %r in DAG %r (map: %s, inject: %s)",
                name, self.name, dep_names[0], inject_param,
            )

            return fn

        if fn is not None:
            return decorator(fn)
        return decorator

    # ── register ───────────────────────────────────────────────────────

    def register(self, queue: Queue) -> None:
        """Register all DAG tasks with a qler Queue for worker execution.

        Creates wrapper functions for tasks with dependencies that inject
        parent results as kwargs.  Idempotent per queue instance.
        """
        if id(queue) in self._registered_queues:
            return

        from qler import task as qler_task

        from dagler.inject import _make_reduce_injector, _make_result_injector

        has_fanout = any(n.kind == "map" for n in self._tasks.values())

        for node in self._tasks.values():
            if node.kind == "map":
                # Map tasks receive items via payload kwargs — register directly
                qler_task(queue)(node.fn)
            elif node.kind == "reduce":
                # Reduce tasks collect all parent results
                wrapped = _make_reduce_injector(node.fn, node.inject_param)
                qler_task(queue)(wrapped)
            elif node.dependencies:
                wrapped = _make_result_injector(
                    node.fn, node.dependencies, node.inject_as,
                )
                qler_task(queue)(wrapped)
            else:
                qler_task(queue)(node.fn)

        # Register the dispatcher task if this DAG uses fan-out
        if has_fanout:
            from dagler.dispatch import _dagler_fanout_dispatch

            if "dagler.dispatch._dagler_fanout_dispatch" not in queue._tasks:
                qler_task(queue)(_dagler_fanout_dispatch)

        self._registered_queues.add(id(queue))

    # ── validate ───────────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Validate the DAG and return a topological ordering of task names.

        Uses Kahn's algorithm (BFS). Raises :class:`CycleError` if the graph
        contains a cycle.
        """
        if not self._tasks:
            return []

        # Fan-out constraints
        map_nodes = [n for n in self._tasks.values() if n.kind == "map"]
        reduce_nodes = [n for n in self._tasks.values() if n.kind == "reduce"]

        if reduce_nodes and not map_nodes:
            msg = f"DAG {self.name!r} has a reduce_task without a map_task"
            raise DaglerError(msg)

        # No map_task may depend on another map_task (nested fan-out — deferred)
        for mn in map_nodes:
            for dep in mn.dependencies:
                if self._tasks[dep].kind == "map":
                    msg = (
                        f"map_task {mn.name!r} depends on map_task {dep!r}. "
                        f"Nested fan-out (map→map) is not supported."
                    )
                    raise DaglerError(msg)

        # Build adjacency list and in-degree map
        in_degree: dict[str, int] = {name: 0 for name in self._tasks}
        children: dict[str, list[str]] = {name: [] for name in self._tasks}

        for name, node in self._tasks.items():
            for dep in node.dependencies:
                children[dep].append(name)
                in_degree[name] += 1

        # Seed queue with zero in-degree nodes
        queue = deque(name for name, deg in in_degree.items() if deg == 0)
        order: list[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for child in children[current]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(order) != len(self._tasks):
            cycle_nodes = [name for name, deg in in_degree.items() if deg > 0]
            msg = f"Cycle detected in DAG {self.name!r} involving tasks: {cycle_nodes}"
            raise CycleError(msg)

        logger.debug(
            "DAG %r validated. Topological order: %s", self.name, order
        )
        return order

    # ── run ────────────────────────────────────────────────────────────

    async def run(
        self,
        queue: Queue,
        *,
        payload: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> DagRun:
        """Submit tasks as qler jobs.

        For DAGs without fan-out, all jobs are submitted atomically.
        For DAGs with map/reduce, static tasks + a dispatcher job are
        submitted; the dispatcher dynamically creates map/reduce/post-reduce
        jobs when the map parent completes.

        If *idempotency_key* is provided and a DagRun with that key already
        exists, the existing run is returned (short-circuit).
        """
        import time

        from qler import generate_ulid
        from sqler import F

        from dagler.run import DagRun

        DagRun.set_db(queue.db, "dagler_runs")

        # Idempotency check: return existing run if key matches
        if idempotency_key:
            existing = await DagRun.query().filter(
                F("idempotency_key") == idempotency_key,
            ).first()
            if existing is not None:
                return existing

        self.register(queue)

        topo_order = self.validate()

        cid = correlation_id or generate_ulid()
        now = int(time.time())

        if not topo_order:
            run = DagRun(
                dag_name=self.name,
                correlation_id=cid,
                status="completed",
                created_at=now,
                updated_at=now,
                finished_at=now,
                idempotency_key=idempotency_key,
            )
            await run.save()
            return run

        # Detect fan-out
        has_fanout = any(self._tasks[n].kind == "map" for n in topo_order)

        if not has_fanout:
            return await self._run_static(queue, topo_order, cid, now, payload, idempotency_key)
        return await self._run_fanout(queue, topo_order, cid, now, payload, None, idempotency_key)

    async def _run_static(
        self,
        queue: Queue,
        topo_order: list[str],
        cid: str,
        now: int,
        payload: dict[str, Any] | None,
        idempotency_key: str | None = None,
    ) -> DagRun:
        """Submit all jobs atomically — the M2 path for non-fanout DAGs."""
        from dagler.run import DagRun

        name_to_index = {name: i for i, name in enumerate(topo_order)}

        job_specs: list[dict[str, Any]] = []
        for task_name in topo_order:
            node = self._tasks[task_name]
            dep_indices = [name_to_index[dep] for dep in node.dependencies]

            spec: dict[str, Any] = {
                "task_path": node.task_path,
                "depends_on": dep_indices,
                "correlation_id": cid,
            }
            if idempotency_key:
                spec["idempotency_key"] = f"{idempotency_key}:{task_name}"
            if payload and task_name in payload:
                spec["kwargs"] = payload[task_name]

            job_specs.append(spec)

        jobs = await queue.enqueue_many(job_specs)

        run = DagRun(
            dag_name=self.name,
            correlation_id=cid,
            status="pending",
            jobs_manifest=[
                {"ulid": j.ulid, "task_name": name}
                for j, name in zip(jobs, topo_order)
            ],
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
        )
        await run.save()

        run._jobs = tuple(jobs)
        run._job_map = {name: jobs[i] for i, name in enumerate(topo_order)}
        run._queue_ref = queue

        return run

    def _build_fanout_stages(self, topo_order: list[str]) -> list[FanoutStage]:
        """Group map/reduce pairs into ordered fan-out stages.

        Each stage has a map node, optional reduce node, trailing regular
        tasks between this stage's end and the next, and a parent stage index
        (-1 for root/independent stages).
        """
        # 1. Collect (map, reduce) pairs in topo order
        map_names = [n for n in topo_order if self._tasks[n].kind == "map"]
        # Map each map to its reduce (if any)
        map_to_reduce: dict[str, str | None] = {}
        for n in topo_order:
            node = self._tasks[n]
            if node.kind == "reduce":
                # reduce depends on exactly one map
                map_dep = node.dependencies[0]
                map_to_reduce[map_dep] = n

        stages: list[FanoutStage] = []
        for mn in map_names:
            rn = map_to_reduce.get(mn)
            stages.append(FanoutStage(
                map_node=self._tasks[mn],
                reduce_node=self._tasks[rn] if rn else None,
            ))

        # 2. Determine parent_stage_index for chained stages
        # A stage is chained if its map's parent is downstream of a previous
        # stage's reduce (or map if no reduce).
        stage_terminal: dict[int, str] = {}
        for i, s in enumerate(stages):
            stage_terminal[i] = s.reduce_node.name if s.reduce_node else s.map_node.name

        # Build set of all names that are "owned" by each stage (dynamic names)
        stage_dynamic_names: dict[int, set[str]] = {}
        for i, s in enumerate(stages):
            names = {s.map_node.name}
            if s.reduce_node:
                names.add(s.reduce_node.name)
            stage_dynamic_names[i] = names

        # For each stage, check if map's parent is reachable from a prior stage's terminal.
        # Iterate in REVERSE order so the closest (most recent) parent stage wins.
        # Without reverse: A→B→C would assign C.parent=A (wrong) because
        # _is_downstream_of(C_map_parent, A_terminal) is True transitively.
        for i, s in enumerate(stages):
            map_parent = s.map_node.dependencies[0]
            for j in range(i - 1, -1, -1):
                terminal = stage_terminal[j]
                # Direct: map parent IS the terminal of stage j
                if map_parent == terminal:
                    stages[i].parent_stage_index = j
                    break
                # Indirect: map parent is a regular task downstream of stage j's terminal
                if self._is_downstream_of(map_parent, terminal, topo_order):
                    stages[i].parent_stage_index = j
                    break

        # 3. Assign trailing tasks between stages
        # Trailing tasks are regular tasks between a stage's terminal and
        # the next chained stage's map (or end of DAG).
        all_dynamic = set()
        for s in stages:
            all_dynamic.add(s.map_node.name)
            if s.reduce_node:
                all_dynamic.add(s.reduce_node.name)

        for i, s in enumerate(stages):
            terminal = stage_terminal[i]
            terminal_idx = topo_order.index(terminal)

            # Find the next chained stage that depends on this one
            next_chained_map: str | None = None
            for j in range(i + 1, len(stages)):
                if stages[j].parent_stage_index == i:
                    next_chained_map = stages[j].map_node.name
                    break

            # Collect regular tasks between terminal and next boundary
            trailing: list[str] = []
            for n in topo_order[terminal_idx + 1:]:
                if n in all_dynamic:
                    continue
                if next_chained_map and n == next_chained_map:
                    break
                # Only include if downstream of this stage's terminal
                if self._is_downstream_of(n, terminal, topo_order):
                    trailing.append(n)

            stages[i].trailing_tasks = trailing

        return stages

    def _is_downstream_of(self, name: str, ancestor: str, topo_order: list[str]) -> bool:
        """Check if *name* is reachable from *ancestor* via dependency edges."""
        if name == ancestor:
            return True
        # BFS from ancestor following children
        children: dict[str, list[str]] = {n: [] for n in topo_order}
        for n in topo_order:
            for dep in self._tasks[n].dependencies:
                children[dep].append(n)

        visited: set[str] = set()
        queue = deque([ancestor])
        while queue:
            current = queue.popleft()
            if current == name:
                return True
            for child in children.get(current, []):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
        return False

    async def _run_fanout(
        self,
        queue: Queue,
        topo_order: list[str],
        cid: str,
        now: int,
        payload: dict[str, Any] | None,
        _map_node: TaskNode | None = None,
        idempotency_key: str | None = None,
    ) -> DagRun:
        """Submit static tasks + dispatchers for fan-out DAGs (multi-stage)."""

        from qler import generate_ulid

        from dagler.run import DagRun

        stages = self._build_fanout_stages(topo_order)

        # Collect all dynamic task names (handled by dispatchers)
        all_dynamic: set[str] = set()
        for s in stages:
            all_dynamic.add(s.map_node.name)
            if s.reduce_node:
                all_dynamic.add(s.reduce_node.name)
            all_dynamic.update(s.trailing_tasks)

        # Static tasks: everything NOT in any dynamic stage
        static_names = [n for n in topo_order if n not in all_dynamic]

        # Build static job specs
        name_to_index: dict[str, int] = {}
        job_specs: list[dict[str, Any]] = []

        for task_name in static_names:
            node = self._tasks[task_name]
            dep_indices = [name_to_index[dep] for dep in node.dependencies]
            idx = len(job_specs)
            name_to_index[task_name] = idx

            spec: dict[str, Any] = {
                "task_path": node.task_path,
                "depends_on": dep_indices,
                "correlation_id": cid,
            }
            if payload and task_name in payload:
                spec["kwargs"] = payload[task_name]

            job_specs.append(spec)

        # Build stage specs for dispatcher payload
        def _make_stage_spec(stage: FanoutStage) -> dict[str, Any]:
            return {
                "map_task_path": stage.map_node.task_path,
                "map_task_name": stage.map_node.name,
                "map_inject_param": stage.map_node.inject_param,
                "reduce_task_path": stage.reduce_node.task_path if stage.reduce_node else None,
                "reduce_task_name": stage.reduce_node.name if stage.reduce_node else None,
                "trailing_tasks": [
                    {"task_path": self._tasks[n].task_path, "name": n}
                    for n in stage.trailing_tasks
                ],
            }

        # Build dispatcher specs for root stages only (chained stages are
        # created at runtime by their parent dispatcher).
        static_task_names = list(static_names)
        dispatcher_indices: list[tuple[int, FanoutStage, int]] = []  # (job_idx, stage, stage_idx)

        for stage_idx, stage in enumerate(stages):
            if stage.parent_stage_index != -1:
                continue  # chained stage — created by parent dispatcher

            map_parent_name = stage.map_node.dependencies[0]

            # Build the remaining_stages chain for chained descendants
            remaining: list[dict[str, Any]] = []
            next_idx = stage_idx
            while True:
                # Find next chained stage
                chained = None
                for j in range(next_idx + 1, len(stages)):
                    if stages[j].parent_stage_index == next_idx:
                        chained = j
                        break
                if chained is None:
                    break
                remaining.append(_make_stage_spec(stages[chained]))
                next_idx = chained

            dispatcher_name = f"_dispatcher_{stage.map_node.name}"
            dispatcher_spec: dict[str, Any] = {
                "task_path": "dagler.dispatch._dagler_fanout_dispatch",
                "depends_on": [name_to_index[map_parent_name]],
                "correlation_id": cid,
                "kwargs": {
                    "_dagler_spec": {
                        "stage": _make_stage_spec(stage),
                        "remaining_stages": remaining,
                        "correlation_id": cid,
                        "dag_name": self.name,
                        "parent_task_name": map_parent_name,
                    },
                },
            }
            disp_idx = len(job_specs)
            job_specs.append(dispatcher_spec)
            static_task_names.append(dispatcher_name)
            dispatcher_indices.append((disp_idx, stage, stage_idx))

        # Pre-generate ULIDs so we can set parent_job_ulid in dispatcher
        # specs BEFORE enqueue_many() — eliminates the race where the Worker
        # claims the dispatcher before a post-enqueue payload patch.
        for spec in job_specs:
            spec["ulid"] = generate_ulid()

        for disp_idx, stage, _stage_idx in dispatcher_indices:
            map_parent_name = stage.map_node.dependencies[0]
            parent_ulid = job_specs[name_to_index[map_parent_name]]["ulid"]
            job_specs[disp_idx]["kwargs"]["_dagler_spec"]["parent_job_ulid"] = parent_ulid

        jobs = await queue.enqueue_many(job_specs)

        run = DagRun(
            dag_name=self.name,
            correlation_id=cid,
            status="pending",
            jobs_manifest=[
                {"ulid": j.ulid, "task_name": name}
                for j, name in zip(jobs, static_task_names)
            ],
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
        )
        await run.save()

        run._jobs = tuple(jobs)
        run._job_map = {
            name: jobs[i] for i, name in enumerate(static_task_names)
        }
        run._queue_ref = queue

        return run

    @property
    def tasks(self) -> dict[str, TaskNode]:
        """Read-only copy of registered tasks."""
        return dict(self._tasks)
