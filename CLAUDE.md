# Claude Context: dagler

**Version:** 0.0.1
**Type:** Python library — DAG pipeline orchestration on qler + SQLite

---

## What dagler Is

dagler is a **DAG pipeline orchestration library** for Python, built on qler (job queue) and sqler (SQLite ORM).

**One sentence:** "Pipeline orchestration without Airflow, built on qler + SQLite."

- qler is the **execution engine** — every DAG task runs as a qler job
- SQLite is the **only infrastructure** — inherited from qler
- Declarative DAG definitions with Python decorators
- Automatic result passing between dependent tasks
- Pipeline-level tracking via DagRun model
- Dynamic fan-out via `@dag.map_task` / `@dag.reduce_task`

---

## LLM-First Philosophy

**The primary consumer of every -ler CLI is a program — including LLM agents.** Humans are a special case that gets pretty-printing. This is not an afterthought; it is a core design principle.

### What this means in practice

| Principle | Implementation |
|-----------|---------------|
| **Structured output is the default** | JSON when stdout is not a TTY. Human tables are the exception, not the rule. `--json` should never be required for programmatic use. |
| **Errors are actionable** | Structured error responses with error codes, available options, and hints. `{"error": "dag_not_found", "available": ["etl_v2"]}` not `"DAG not found"`. |
| **Commands are discoverable** | `dagler schema` returns machine-readable command/arg/output descriptions. An agent hitting the tool for the first time understands the full API in one call. |
| **Retries are safe** | Idempotency keys on mutations. An agent that retries `dagler run` doesn't create duplicates. |
| **Polling is unnecessary** | `dagler wait <id>` blocks and returns final state. One command instead of a poll loop. |
| **Correlation is first-class** | `--correlation-id` is prominent, not buried in advanced options. Agents need to find their work later. |
| **Stdout is data, stderr is diagnostics** | Never mix human messages into the data stream. Agents parse stdout; humans read stderr. |
| **Exit codes are meaningful** | 0=success, 1=user error (bad args, not found), 2=system error (DB failure). Agents branch on exit codes. |

### Anti-patterns (things we never do)

- `--json` as the only way to get structured output (should auto-detect TTY)
- Error messages that require string parsing to understand
- Commands that only work interactively
- Pretty output on stdout that breaks `jq` / JSON parsing
- Undocumented output schemas that change between versions

---

## Core Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Execution engine | qler jobs | Reuse existing queue, retry, lease, metrics infrastructure |
| Dependencies | qler's `depends_on` + `pending_dep_count` | Already implemented and tested |
| Result passing | qler's `result_json` | Already stored per-job, just need to read at dispatch |
| Pipeline identity | `correlation_id` | Already exists on Job, natural pipeline run ID |
| Batch submission | qler's `enqueue_many()` | Atomic, supports intra-batch deps |
| Storage | sqler model (DagRun) | Consistent with -ler stack |
| Fan-out | Dispatcher job pattern | Lightweight internal job creates map/reduce jobs at runtime |

---

## The -ler Stack

| Layer | Tool | Responsibility |
|-------|------|----------------|
| Storage | **sqler** | SQLite ORM, async, optimistic locking |
| Queues | **qler** | Background job execution |
| Pipelines | **dagler** | DAG orchestration (this project) |
| Logs | **logler** | Log aggregation, correlation IDs |
| Processes | **procler** | Process management, health checks |

---

## Non-Negotiable Rules

### 1. Never bypass qler

All job operations go through qler's Queue API. No direct Job model manipulation for execution-related operations.

### 2. Never bypass sqler

All DB operations go through sqler's model API. No raw SQL.

### 3. SQLite is the design center

Inherited from qler. No features that need Postgres.

### 4. Debuggability over features

Every design decision must answer: "Does this make pipeline debugging easier?"

### 5. LLM agents are first-class consumers

Every CLI command must be usable by an LLM agent without human intervention. Structured output, actionable errors, discoverable schemas.

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12+ | Runtime |
| asyncio | stdlib | Async foundation |
| qler | latest | Job execution engine |
| sqler | latest | Database operations |
| SQLite | WAL mode | Storage (via sqler via qler) |
| uv | latest | Package management |

---

## File Structure

```
src/dagler/
├── __init__.py          # Public API
├── py.typed             # PEP 561 marker
├── dag.py               # DAG class, task/map_task/reduce_task decorators
├── task.py              # TaskNode dataclass
├── run.py               # DagRun model (sqler), wait(), cancel(), retry()
├── inject.py            # Result injection + reduce injection
├── dispatch.py          # Fan-out dispatcher (dynamic job creation)
├── cli.py               # Click CLI (dagler init/run/runs/run-info/cancel/retry)
└── exceptions.py        # DaglerError, CycleError, DependencyError, DuplicateTaskError
tests/
├── __init__.py
├── conftest.py          # Shared fixtures (db, queue, worker)
├── test_dag.py          # M1: DAG definition tests
├── test_run.py          # M2: DAG submission tests
├── test_inject.py       # M3: Result injection tests
├── test_dagrun.py       # M4: DagRun model tests
├── test_cli.py          # M5: CLI tests
└── test_fanout.py       # M6: Fan-out map/reduce tests
benchmarks/
├── __init__.py
├── __main__.py          # Benchmark runner entry point
├── core.py              # Benchmark infrastructure
└── scenarios.py         # Benchmark scenarios
```

---

## Active Roadmaps

- [ROADMAP.md](./ROADMAP.md) — M0-M9 complete.
