"""Benchmark scenarios for dagler.

Measures DAG definition/validation, submission, and fan-out execution.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from benchmarks.core import AsyncTimer, BenchmarkResult, compute_stats

# ── Task functions for benchmarks ──────────────────────────────────────


async def _bench_root():
    return list(range(10))


async def _bench_identity(x):
    return x


async def _bench_sum(results):
    return sum(results)


async def _bench_passthrough(**kwargs):
    return 1


# ── Helpers ────────────────────────────────────────────────────────────


async def _make_db_and_queue():
    """Create a fresh in-memory DB + queue for each measurement."""
    from qler.queue import Queue
    from sqler import AsyncSQLerDB

    db = AsyncSQLerDB.in_memory(shared=False)
    await db.connect()
    q = Queue(db)
    await q.init_db()
    return db, q


async def _teardown_db(db):
    await db.close()


# ── Scenario: DAG Definition Scaling ──────────────────────────────────


class DagDefinitionScaling:
    """How fast can we define + validate DAGs of increasing size?"""

    name = "dag_definition_scaling"
    suite = "definition"
    description = "DAG() + N chained tasks + validate() at increasing N"

    async def run(self, warmup: int, iterations: int) -> list[BenchmarkResult]:
        from dagler import DAG

        timer = AsyncTimer(warmup=warmup, iterations=iterations)
        results: list[BenchmarkResult] = []

        for n_tasks in (10, 50, 100, 200, 500):
            # Pre-create task functions with unique names
            task_fns = []
            for i in range(n_tasks):
                # Module-level-like function: fake __qualname__ == __name__
                async def fn(**kwargs):
                    return 1
                fn.__name__ = f"task_{i}"
                fn.__qualname__ = f"task_{i}"
                fn.__module__ = "benchmarks.scenarios"
                task_fns.append(fn)

            async def measure():
                DAG._registry.clear()
                dag = DAG("bench")
                dag.task(task_fns[0])
                for j in range(1, n_tasks):
                    dag.task(depends_on=[task_fns[j - 1]])(task_fns[j])
                dag.validate()

            stats = await timer.measure(measure)
            throughput = n_tasks / (stats.median_ms / 1000) if stats.median_ms > 0 else 0

            results.append(BenchmarkResult(
                scenario=self.name,
                suite=self.suite,
                parameter="tasks",
                value=n_tasks,
                timing=stats,
                ops=n_tasks,
                throughput=round(throughput, 1),
            ))

        DAG._registry.clear()
        return results


# ── Scenario: DAG Submission Scaling ──────────────────────────────────


class DagSubmissionScaling:
    """How fast can we submit DAGs (enqueue_many) at increasing task count?"""

    name = "dag_submission_scaling"
    suite = "submission"
    description = "dag.run(queue) at increasing task count (non-fanout, cold DB)"

    async def run(self, warmup: int, iterations: int) -> list[BenchmarkResult]:
        from dagler import DAG

        results: list[BenchmarkResult] = []

        for n_tasks in (5, 10, 25, 50, 100):
            task_fns = []
            for i in range(n_tasks):
                async def fn(**kwargs):
                    return 1
                fn.__name__ = f"sub_task_{i}"
                fn.__qualname__ = f"sub_task_{i}"
                fn.__module__ = "benchmarks.scenarios"
                task_fns.append(fn)

            timings: list[float] = []

            # Manual warmup + measurement (each needs fresh DB)
            for iteration in range(warmup + iterations):
                DAG._registry.clear()
                db, q = await _make_db_and_queue()

                dag = DAG("bench")
                dag.task(task_fns[0])
                for j in range(1, n_tasks):
                    dag.task(depends_on=[task_fns[j - 1]])(task_fns[j])

                start = time.perf_counter()
                await dag.run(q)
                elapsed_ms = (time.perf_counter() - start) * 1000

                if iteration >= warmup:
                    timings.append(elapsed_ms)

                await _teardown_db(db)

            stats = compute_stats(timings)
            throughput = n_tasks / (stats.median_ms / 1000) if stats.median_ms > 0 else 0

            results.append(BenchmarkResult(
                scenario=self.name,
                suite=self.suite,
                parameter="tasks",
                value=n_tasks,
                timing=stats,
                ops=n_tasks,
                throughput=round(throughput, 1),
            ))

        DAG._registry.clear()
        return results


# ── Scenario: Fan-Out Execution ───────────────────────────────────────


class FanOutExecution:
    """End-to-end fan-out pipeline: submit + execute + wait at varying fan-out width."""

    name = "fanout_execution"
    suite = "fanout"
    description = "fetch → map(N) → reduce: full cycle including worker execution"

    async def run(self, warmup: int, iterations: int) -> list[BenchmarkResult]:
        from dagler import DAG
        from qler.worker import Worker

        results: list[BenchmarkResult] = []

        # Smaller iteration count for execution benchmarks (they're slow)
        exec_iterations = min(iterations, 10)
        exec_warmup = min(warmup, 2)

        for fan_width in (5, 10, 25, 50):
            # Task functions that produce fan_width items
            async def fetch_fn():
                return list(range(fan_width))
            fetch_fn.__name__ = "bench_fetch"
            fetch_fn.__qualname__ = "bench_fetch"
            fetch_fn.__module__ = "benchmarks.scenarios"

            async def map_fn(item):
                return item * 2
            map_fn.__name__ = "bench_map"
            map_fn.__qualname__ = "bench_map"
            map_fn.__module__ = "benchmarks.scenarios"

            async def reduce_fn(results_list):
                return sum(results_list)
            reduce_fn.__name__ = "bench_reduce"
            reduce_fn.__qualname__ = "bench_reduce"
            reduce_fn.__module__ = "benchmarks.scenarios"

            timings: list[float] = []

            for iteration in range(exec_warmup + exec_iterations):
                DAG._registry.clear()
                db, q = await _make_db_and_queue()

                dag = DAG("bench")
                dag.task(fetch_fn)
                dag.map_task(depends_on=[fetch_fn])(map_fn)
                dag.reduce_task(depends_on=[map_fn])(reduce_fn)

                w = Worker(q, poll_interval=0.005, concurrency=8, shutdown_timeout=1.0)
                worker_task = asyncio.create_task(w.run())

                start = time.perf_counter()
                run = await dag.run(q)
                await run.wait(timeout=30)
                elapsed_ms = (time.perf_counter() - start) * 1000

                if iteration >= exec_warmup:
                    timings.append(elapsed_ms)
                    assert run.status == "completed"

                w._running = False
                await asyncio.sleep(0.02)
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

                await _teardown_db(db)

            stats = compute_stats(timings)
            # throughput = total jobs per second (fetch + dispatcher + N maps + reduce)
            total_jobs = fan_width + 3  # fetch + dispatcher + N maps + reduce
            throughput = total_jobs / (stats.median_ms / 1000) if stats.median_ms > 0 else 0

            results.append(BenchmarkResult(
                scenario=self.name,
                suite=self.suite,
                parameter="fan_width",
                value=fan_width,
                timing=stats,
                ops=total_jobs,
                throughput=round(throughput, 1),
                metadata={"total_jobs": total_jobs},
            ))

        DAG._registry.clear()
        return results


# ── Scenario: Multi-Stage Execution ───────────────────────────────────


class MultiStageExecution:
    """Chained fan-out stages: fetch → map → reduce → map → reduce."""

    name = "multistage_execution"
    suite = "fanout"
    description = "Chained 2-stage fan-out: stage1(N) → stage2(M) with cascading dispatchers"

    async def run(self, warmup: int, iterations: int) -> list[BenchmarkResult]:
        from dagler import DAG
        from qler.worker import Worker

        results: list[BenchmarkResult] = []

        exec_iterations = min(iterations, 8)
        exec_warmup = min(warmup, 2)

        for fan_width in (5, 10, 25):
            async def fetch_fn():
                return list(range(fan_width))
            fetch_fn.__name__ = "ms_fetch"
            fetch_fn.__qualname__ = "ms_fetch"
            fetch_fn.__module__ = "benchmarks.scenarios"

            async def map1_fn(item):
                return item * 2
            map1_fn.__name__ = "ms_map1"
            map1_fn.__qualname__ = "ms_map1"
            map1_fn.__module__ = "benchmarks.scenarios"

            async def reduce1_fn(results_list):
                return list(range(fan_width // 2 + 1))  # produce items for stage 2
            reduce1_fn.__name__ = "ms_reduce1"
            reduce1_fn.__qualname__ = "ms_reduce1"
            reduce1_fn.__module__ = "benchmarks.scenarios"

            async def map2_fn(item):
                return item + 100
            map2_fn.__name__ = "ms_map2"
            map2_fn.__qualname__ = "ms_map2"
            map2_fn.__module__ = "benchmarks.scenarios"

            async def reduce2_fn(results_list):
                return sum(results_list)
            reduce2_fn.__name__ = "ms_reduce2"
            reduce2_fn.__qualname__ = "ms_reduce2"
            reduce2_fn.__module__ = "benchmarks.scenarios"

            timings: list[float] = []

            for iteration in range(exec_warmup + exec_iterations):
                DAG._registry.clear()
                db, q = await _make_db_and_queue()

                dag = DAG("bench")
                dag.task(fetch_fn)
                dag.map_task(depends_on=[fetch_fn])(map1_fn)
                dag.reduce_task(depends_on=[map1_fn])(reduce1_fn)
                dag.map_task(depends_on=[reduce1_fn])(map2_fn)
                dag.reduce_task(depends_on=[map2_fn])(reduce2_fn)

                w = Worker(q, poll_interval=0.005, concurrency=8, shutdown_timeout=1.0)
                worker_task = asyncio.create_task(w.run())

                start = time.perf_counter()
                run = await dag.run(q)
                await run.wait(timeout=30)
                elapsed_ms = (time.perf_counter() - start) * 1000

                if iteration >= exec_warmup:
                    timings.append(elapsed_ms)
                    assert run.status == "completed"

                w._running = False
                await asyncio.sleep(0.02)
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

                await _teardown_db(db)

            stats = compute_stats(timings)
            stage2_width = fan_width // 2 + 1
            # fetch + disp1 + N maps + reduce1 + disp2 + M maps + reduce2
            total_jobs = 1 + 1 + fan_width + 1 + 1 + stage2_width + 1
            throughput = total_jobs / (stats.median_ms / 1000) if stats.median_ms > 0 else 0

            results.append(BenchmarkResult(
                scenario=self.name,
                suite=self.suite,
                parameter="stage1_width",
                value=fan_width,
                timing=stats,
                ops=total_jobs,
                throughput=round(throughput, 1),
                metadata={
                    "stage1_width": fan_width,
                    "stage2_width": stage2_width,
                    "total_jobs": total_jobs,
                },
            ))

        DAG._registry.clear()
        return results


# ── Suite registry ─────────────────────────────────────────────────────


ALL_SCENARIOS: list = [
    DagDefinitionScaling(),
    DagSubmissionScaling(),
    FanOutExecution(),
    MultiStageExecution(),
]


def get_scenarios(names: list[str] | None = None) -> list:
    """Get scenarios filtered by suite name, or all if None."""
    if names is None:
        return list(ALL_SCENARIOS)
    return [s for s in ALL_SCENARIOS if s.suite in names or s.name in names]
