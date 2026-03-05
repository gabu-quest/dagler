# dagler North Star

**One sentence:** Pipeline orchestration without Airflow, built on qler + SQLite.

---

## What dagler Is

dagler is a **DAG pipeline orchestration library** for Python that:

1. **Uses qler as the execution engine** — Each DAG task runs as a qler job. No separate scheduler infrastructure.

2. **Uses SQLite as the only infrastructure** — Inherited from qler. No Redis, no external message brokers.

3. **Provides declarative DAG definitions** — Python decorators, not YAML. Git-friendly, code-reviewable.

4. **Passes results between tasks** — Parent task results automatically available to child tasks.

5. **Tracks pipeline runs** — Each `dag.run()` invocation is a tracked DagRun with status rollup.

---

## What dagler Is NOT

| dagler is NOT | Why not |
|---------------|---------|
| Airflow/Prefect/Temporal | Those are distributed, multi-node orchestrators. dagler is single-process. |
| A general workflow engine | dagler is specifically for data/compute pipelines with dependencies. |
| A replacement for qler | dagler is built ON TOP of qler. qler handles execution; dagler adds structure. |

---

## Core Principles

### 1. qler Is The Execution Engine

dagler NEVER bypasses qler. Every task node runs as a qler job. Dependencies use qler's `depends_on` + `pending_dep_count`. Results use qler's `result_json`.

### 2. Declarative Over Imperative

DAGs are defined with decorators and dependency declarations, not imperative `submit()` calls. The DAG structure is knowable before execution.

### 3. Result Passing Is Automatic

When task B depends on task A, B's handler receives A's result automatically. No manual `get_result()` calls.

### 4. Pipeline Identity

Every `dag.run()` creates a DagRun with a unique ID. All jobs in that run share a `correlation_id`. Status rollup (pending/running/completed/failed) is computed from constituent jobs.

### 5. SQLite Is The Design Center

Inherited from qler. No features that require Postgres or Redis.

---

## API Vision

```python
from dagler import DAG

dag = DAG("etl_pipeline")

@dag.task
async def extract():
    return await fetch_data()

@dag.task(depends_on=[extract])
async def transform(data):
    return process(data)

@dag.task(depends_on=[transform])
async def load(data):
    await write_to_db(data)

# Submit a pipeline run
run = await dag.run(queue)
await run.wait()
```

---

## The Gap: qler Has vs dagler Adds

| Capability | qler (exists) | dagler (adds) |
|-----------|---------------|---------------|
| Job execution | Yes | Inherited |
| Job dependencies | `depends_on`, `pending_dep_count` | Declarative `@dag.task(depends_on=[...])` |
| Result storage | `result_json` on Job | Automatic result passing to children |
| Correlation IDs | `correlation_id` field | Pipeline-scoped correlation (DagRun ID) |
| Batch enqueue | `enqueue_many()` with intra-batch deps | `dag.run()` → single `enqueue_many()` call |
| Status tracking | Per-job status | Pipeline-level status rollup (DagRun model) |
| Cancel propagation | `cascade_cancel_dependents()` | `run.cancel()` → cancel all jobs in run |
| Dynamic fan-out | Not supported | `@dag.map_task` → N children from parent result |
| CLI | `qler jobs/status` | `dagler runs/run-info/cancel` |
