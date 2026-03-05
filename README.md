# dagler

Pipeline orchestration without Airflow, built on qler + SQLite.

dagler is a DAG pipeline orchestration library for Python. Each task runs as a [qler](https://github.com/gabu-quest/qler) job, SQLite is the only infrastructure, and pipelines are defined with Python decorators.

## The -ler Stack

| Layer | Tool | Responsibility |
|-------|------|----------------|
| Storage | [sqler](https://github.com/gabu-quest/sqler) | SQLite ORM, async, optimistic locking |
| Queues | [qler](https://github.com/gabu-quest/qler) | Background job execution |
| Pipelines | **dagler** | DAG orchestration (this project) |

## Install

```bash
uv add dagler
```

## Quick Start

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

## Features

- **Declarative DAGs** — Python decorators, not YAML
- **Automatic result passing** — parent results injected into child tasks
- **Dynamic fan-out** — `@dag.map_task` splits work, `@dag.reduce_task` collects results
- **Pipeline tracking** — every `dag.run()` creates a tracked DagRun with status rollup
- **LLM-agent friendly CLI** — structured JSON output, actionable errors, discoverable schemas
- **SQLite-only** — no Redis, no Postgres, no external brokers

## CLI

```bash
dagler init          # Initialize the database
dagler run <dag>     # Submit a pipeline run
dagler runs          # List pipeline runs
dagler run-info <id> # Inspect a specific run
dagler cancel <id>   # Cancel a running pipeline
dagler retry <id>    # Retry a failed pipeline
dagler wait <id>     # Block until a run completes
```

## Contributing

See [CLAUDE.md](./CLAUDE.md) for project architecture and contributor docs.

## License

MIT
