# dagler Roadmap

## Milestones

### M0: Project Scaffold ✅

Minimal package structure so `import dagler` works.

- pyproject.toml (uv, PEP 621, entry point `dagler`)
- src/dagler/__init__.py — public API stub
- tests/conftest.py — shared fixtures with qler Queue
- NORTH_STAR.md — vision and philosophy
- ROADMAP.md — this file

**Exit criteria:** `uv run python -c "import dagler"` works.

### M1: DAG Definition ✅

Declarative DAG structure with `@dag.task` decorator.

- `DAG` class with name, task registry, edge validation
- `@dag.task` decorator that registers a node
- `@dag.task(depends_on=[parent])` for dependency edges
- Cycle detection at definition time (topological sort)
- DAG validation: all dependencies registered, no orphans

**Exit criteria:** Can define a DAG with tasks and dependencies, validation catches cycles and missing deps.

### M2: DAG Submission ✅

`dag.run()` submits all tasks as qler jobs in a single transaction.

- `dag.run(queue)` → `enqueue_many()` with intra-batch deps + shared `correlation_id`
- Returns a `DagRun` handle with the correlation ID and job ULIDs
- Maps DAG node names to job ULIDs
- Topological ordering for `enqueue_many()` dependency resolution

**Exit criteria:** `dag.run()` creates all jobs atomically with correct dependencies.

### M3: Result Passing ✅

Automatic result injection from parent to child tasks.

- When a child task starts, read parent job's `result_json`
- Inject as keyword arguments (single parent → positional, multiple → by name)
- Support for explicit result key naming

**Exit criteria:** Child tasks receive parent results without manual queries.

### M4: DagRun Model ✅

Persistent run tracking with status rollup.

- `DagRun` sqler model (run_id, dag_name, status, created_at, finished_at)
- Status rollup: pending (all pending), running (any running), completed (all completed), failed (any failed)
- `run.wait()` — waits for all jobs to reach terminal state
- `run.cancel()` — cancels all non-terminal jobs in the run
- `run.status` — computed from constituent job statuses

**Exit criteria:** DagRun tracks pipeline lifecycle from submission to completion.

### M5: CLI ✅

Command-line interface for pipeline management.

- `dagler init` — create tables
- `dagler run <dag_name>` — submit a pipeline run (requires --app/--module)
- `dagler runs` — list recent runs
- `dagler run-info <run_id>` — show run details with per-task status
- `dagler cancel <run_id>` — cancel a running pipeline
- Human-first output, `--json` flag on all commands

**Exit criteria:** All commands work, human output readable, --json parseable.

### M6: Dynamic Fan-Out ✅

Map/reduce pattern for data-parallel pipelines.

- `@dag.map_task(depends_on=[parent])` — parent result is iterable, spawn N children
- Each child receives one item from parent's result
- `@dag.reduce_task(depends_on=[map_step])` — collects all map results
- Dynamic job creation at runtime (not at DAG definition time)

**Exit criteria:** Can define map→reduce pipelines where fan-out count is determined by parent output.

### M7: Agent-First CLI ✅

Make the CLI a first-class interface for LLM agents and programmatic consumers.

- Auto-detect non-TTY → default to JSON output (no `--json` flag needed)
- `dagler wait <run_id>` — blocking wait that returns final status as JSON
- `dagler schema` — machine-readable command/arg/output schema for tool discovery
- `--idempotency-key` on `dagler run` — safe retries (wire through qler)
- `run-info` shows dynamic fan-out jobs (not just static jobs)
- Structured error responses with error codes, available options, hints
- Exit codes: 0=success, 1=user error, 2=system error
- Stderr for diagnostics, stdout for data (never mixed)

**Exit criteria:** An LLM agent can discover, invoke, poll, and parse every CLI command without human-formatted output or guesswork.

### M8: Partial Reruns ✅

Resume failed pipelines from the point of failure.

- `dagler retry <run_id>` — resubmit only failed/cancelled jobs
- `run-info` shows per-job failure reasons (error message, traceback)
- `dagler runs --status failed` already works; `run-info` needs richer failure detail
- Preserve original correlation_id on retries for traceability
- Skip already-completed jobs (don't re-execute successful work)

**Exit criteria:** A failed pipeline can be retried without re-running successful jobs, and the failure reason is visible via CLI.

### M9: Multiple Map/Reduce Stages ✅

Lift the single map/reduce constraint.

- Allow N map_task + N reduce_task per DAG
- Support chained fan-out: `fetch → map(A) → reduce(A) → map(B) → reduce(B)`
- Support independent fan-outs: two unrelated map/reduce pairs in one DAG
- Cascading dispatchers: each stage's dispatcher creates the next stage's dispatcher at runtime
- Regular tasks between stages (e.g., `reduce(A) → transform → map(B)`)
- Multi-wave wait loop discovers dynamically added jobs across cascading dispatchers
- Validation: map→map rejected (nested fan-out deferred), reduce-without-map still rejected
- Backward-compatible dispatcher spec format (detects M6 legacy vs M9 staged)

**Exit criteria:** DAGs with multiple independent or chained map/reduce stages execute correctly.

---

## Deferred

### Nested Fan-Out (map inside map)

**Status:** Deferred indefinitely.

**What it is:** A map job's output is itself iterable, spawning another level of fan-out. Tree-shaped execution instead of flat.

**Why deferred:**
- The dispatcher pattern would need recursive dispatchers or a dispatcher stack, adding significant complexity to the execution model.
- No concrete use case has demanded it — a single map/reduce covers the vast majority of data-parallel pipelines.
- The workaround (flatten in the first map, fan out once) is simple and explicit.
- Debugging nested fan-out is genuinely hard — job trees are harder to reason about than flat fan-out.

Will revisit if a real use case emerges that can't be solved with flatten-first.

---

## Status Key

- ✅ Done
- 🔄 In progress
- ⬚ Not started
