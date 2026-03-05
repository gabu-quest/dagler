"""dagler CLI — agent-first pipeline management.

JSON output by default (auto-detected via TTY). Use --human for human-readable
tables. All responses use the envelope format: {"ok": true, "data": ...} or
{"ok": false, "error": "code", "message": "...", "hint": "..."}.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
from qler.queue import Queue

from dagler.run import DagRun

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MODULE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


def _run(coro: Any) -> Any:
    """Bridge async -> sync for Click commands."""
    return asyncio.run(coro)


def _is_json_mode(ctx: click.Context) -> bool:
    """Determine if output should be JSON based on context and TTY."""
    mode = ctx.obj.get("output_mode", "auto")
    if mode == "json":
        return True
    if mode == "human":
        return False
    # auto: JSON when stdout is not a TTY
    return not sys.stdout.isatty()


def _echo_result(
    ctx: click.Context,
    data: Any = None,
    *,
    ok: bool = True,
    error: str | None = None,
    message: str | None = None,
    hint: str | None = None,
    headers: list[str] | None = None,
    rows: list[list[str]] | None = None,
    count: int | None = None,
    human_text: str | None = None,
) -> None:
    """Unified output helper. JSON envelope or human-readable text.

    JSON mode: writes envelope to stdout.
    Human mode: writes tables/messages to stdout.
    """
    if _is_json_mode(ctx):
        envelope: dict[str, Any] = {"ok": ok}
        if ok:
            envelope["data"] = data
            if count is not None:
                envelope["count"] = count
        else:
            if error:
                envelope["error"] = error
            if message:
                envelope["message"] = message
            if hint:
                envelope["hint"] = hint
        click.echo(json.dumps(envelope, default=str))
    else:
        if not ok:
            parts = []
            if message:
                parts.append(message)
            if hint:
                parts.append(f"Hint: {hint}")
            click.echo("\n".join(parts), err=True)
        elif human_text:
            click.echo(human_text)
        elif headers and rows is not None:
            _echo_table(headers, rows)
        elif data is not None:
            click.echo(str(data))


def _exit_error(
    ctx: click.Context,
    code: str,
    message: str,
    hint: str | None = None,
    exit_code: int = 1,
) -> None:
    """Output a structured error and exit."""
    _echo_result(ctx, ok=False, error=code, message=message, hint=hint)
    sys.exit(exit_code)


def _echo_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a simple aligned text table."""
    if not rows:
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    click.echo(fmt.format(*headers))
    click.echo(fmt.format(*["-" * w for w in widths]))
    for row in rows:
        click.echo(fmt.format(*row))


def _format_ts(epoch: int | None) -> str:
    """Epoch seconds -> ISO 8601 UTC string."""
    if epoch is None:
        return "-"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(raw: str | None) -> Any:
    """Parse JSON safely."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw[:200] if raw else None


def _validate_module_path(mod_path: str) -> None:
    """Validate a module path is a valid dotted Python identifier."""
    if not _MODULE_PATTERN.fullmatch(mod_path):
        raise click.BadParameter(
            f"Invalid module path '{mod_path}'. Only dotted Python identifiers are allowed."
        )


def _import_app(app_string: str) -> Queue:
    """Import 'module:attribute' -> Queue instance."""
    module_path, _, attr = app_string.partition(":")
    if not attr:
        raise click.BadParameter(f"Expected 'module:attribute', got '{app_string}'")
    _validate_module_path(module_path)
    try:
        mod = importlib.import_module(module_path)
    except ImportError as exc:
        raise click.BadParameter(f"Cannot import module '{module_path}': {exc}") from exc
    try:
        obj = getattr(mod, attr)
    except AttributeError as exc:
        raise click.BadParameter(
            f"Module '{module_path}' has no attribute '{attr}'"
        ) from exc
    if not isinstance(obj, Queue):
        raise click.BadParameter(f"'{app_string}' is not a Queue instance")
    return obj


def _import_modules(modules: tuple[str, ...]) -> None:
    """Import modules to trigger DAG registration."""
    for mod_path in modules:
        _validate_module_path(mod_path)
        try:
            importlib.import_module(mod_path)
        except ImportError as exc:
            raise click.BadParameter(f"Cannot import module '{mod_path}': {exc}") from exc


def _dagrun_to_dict(run: DagRun) -> dict[str, Any]:
    """Convert a DagRun to a JSON-friendly dict."""
    result: dict[str, Any] = {
        "correlation_id": run.correlation_id,
        "dag_name": run.dag_name,
        "status": run.status,
        "job_count": len(run.job_ulids),
        "task_names": run.task_names,
        "job_ulids": run.job_ulids,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "finished_at": run.finished_at,
    }
    if run.idempotency_key:
        result["idempotency_key"] = run.idempotency_key
    return result


# ---------------------------------------------------------------------------
# CLI Group
# ---------------------------------------------------------------------------

@click.group(name="dagler")
@click.version_option(package_name="dagler")
@click.option("--human", is_flag=True, help="Force human-readable output")
@click.option("--json", "force_json", is_flag=True, help="Force JSON output")
@click.pass_context
def cli(ctx: click.Context, human: bool, force_json: bool) -> None:
    """dagler -- DAG pipeline orchestration on qler + SQLite."""
    ctx.ensure_object(dict)
    if human:
        ctx.obj["output_mode"] = "human"
    elif force_json:
        ctx.obj["output_mode"] = "json"
    else:
        ctx.obj["output_mode"] = "auto"


# ---------------------------------------------------------------------------
# dagler init
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.pass_context
def init(ctx: click.Context, db: str) -> None:
    """Create the dagler database tables."""

    async def _init() -> None:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        promoted = getattr(DagRun, "__promoted__", {})
        checks = getattr(DagRun, "__checks__", {})
        await q.db._ensure_table_with_promoted("dagler_runs", promoted, checks)
        await q.close()

    _run(_init())

    # Update .gitignore
    gitignore_updated = False
    db_path = Path(db)
    gitignore = db_path.parent / ".gitignore"
    db_name = db_path.name.replace("\n", "").replace("\r", "").strip()

    if gitignore.exists():
        content = gitignore.read_text()
        if db_name not in content.splitlines():
            with gitignore.open("a") as f:
                if not content.endswith("\n"):
                    f.write("\n")
                f.write(f"{db_name}\n")
            gitignore_updated = True
    else:
        gitignore.write_text(f"{db_name}\n")
        gitignore_updated = True

    data = {"db": db, "created": True, "gitignore_updated": gitignore_updated}
    human_lines = [f"Created database: {db}"]
    if gitignore_updated:
        human_lines.append(f"Added '{db_name}' to {gitignore}")
    _echo_result(ctx, data, human_text="\n".join(human_lines))


# ---------------------------------------------------------------------------
# dagler run
# ---------------------------------------------------------------------------

@cli.command("run")
@click.argument("dag_name")
@click.option("--app", "app_string", help="Queue instance as 'module:attribute'")
@click.option("--module", "-m", "modules", multiple=True, help="Import module(s) to register DAGs")
@click.option("--db", envvar="DAGLER_DB", help="Database file path")
@click.option("--payload", default=None, help="JSON payload string (keyed by task name)")
@click.option("--correlation-id", "correlation_id", default=None, help="Custom correlation ID")
@click.option("--idempotency-key", "idempotency_key", default=None, help="Idempotency key for safe retries")
@click.pass_context
def run_dag(
    ctx: click.Context,
    dag_name: str,
    app_string: str | None,
    modules: tuple[str, ...],
    db: str | None,
    payload: str | None,
    correlation_id: str | None,
    idempotency_key: str | None,
) -> None:
    """Submit a DAG pipeline run."""
    from dagler.dag import DAG

    # Resolve queue
    if app_string:
        queue = _import_app(app_string)
    elif db:
        queue = Queue(db)
    else:
        raise click.UsageError("Provide --app or --db")

    # Import modules to populate DAG registry
    if modules:
        _import_modules(modules)

    # Look up DAG by name
    if dag_name not in DAG._registry:
        available = sorted(DAG._registry.keys())
        hint = f"Available: {', '.join(available)}" if available else None
        _exit_error(ctx, "dag_not_found", f"DAG '{dag_name}' not found in registry", hint=hint)

    dag = DAG._registry[dag_name]

    # Parse payload
    parsed_payload = None
    if payload:
        try:
            parsed_payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            _exit_error(ctx, "invalid_payload", f"Invalid JSON payload: {exc}")

    async def _run_dag() -> dict[str, Any]:
        await queue.init_db()
        try:
            dag_run = await dag.run(
                queue,
                payload=parsed_payload,
                correlation_id=correlation_id,
                idempotency_key=idempotency_key,
            )
            return _dagrun_to_dict(dag_run)
        finally:
            if not app_string:
                await queue.close()

    result = _run(_run_dag())

    human_text = (
        f"Submitted DAG '{result['dag_name']}'\n"
        f"  Run ID:  {result['correlation_id']}\n"
        f"  Status:  {result['status']}\n"
        f"  Jobs:    {result['job_count']}"
    )
    _echo_result(ctx, result, human_text=human_text)


# ---------------------------------------------------------------------------
# dagler runs
# ---------------------------------------------------------------------------

@cli.command("runs")
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.option("--limit", default=20, type=click.IntRange(1, 1000), help="Max rows to return (1-1000)")
@click.option(
    "--status", "status_filter", default=None,
    type=click.Choice(["pending", "running", "completed", "failed", "cancelled"]),
    help="Filter by status",
)
@click.pass_context
def list_runs(ctx: click.Context, db: str, limit: int, status_filter: str | None) -> None:
    """List recent pipeline runs."""
    from sqler import F

    async def _list() -> list[dict[str, Any]]:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        try:
            qs = DagRun.query()
            if status_filter:
                qs = qs.filter(F("status") == status_filter)
            runs = await qs.order_by("-_id").limit(limit).all()
            return [_dagrun_to_dict(r) for r in runs]
        finally:
            await q.close()

    results = _run(_list())

    headers = ["CORRELATION_ID", "DAG", "STATUS", "JOBS", "CREATED", "FINISHED"]
    rows = [
        [
            r["correlation_id"],
            r["dag_name"],
            r["status"],
            str(r["job_count"]),
            _format_ts(r["created_at"]),
            _format_ts(r["finished_at"]),
        ]
        for r in results
    ]
    human_text = "No runs found." if not results else None
    _echo_result(ctx, results, headers=headers, rows=rows, count=len(results),
                 human_text=human_text)


# ---------------------------------------------------------------------------
# dagler run-info
# ---------------------------------------------------------------------------

@cli.command("run-info")
@click.argument("run_id")
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.pass_context
def run_info(ctx: click.Context, run_id: str, db: str) -> None:
    """Show details for a specific pipeline run."""
    from qler.models.job import Job
    from sqler import F

    async def _info() -> dict[str, Any] | None:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        try:
            run = await DagRun.query().filter(
                F("correlation_id") == run_id,
            ).first()
            if run is None:
                return None

            # Build ulid->task_name map from paired lists
            ulid_to_name: dict[str, str] = {}
            for ulid, name in zip(run.job_ulids, run.task_names):
                ulid_to_name[ulid] = name

            # Primary: fetch jobs by ulid list
            jobs: list[Job] = []
            if run.job_ulids:
                jobs = await Job.query().filter(
                    F("ulid").in_list(run.job_ulids),
                ).all()

            # Fallback: also query by correlation_id to catch dynamically
            # added jobs not yet reflected in DagRun's job_ulids
            known_ulids = set(run.job_ulids)
            extra_jobs = await Job.query().filter(
                F("correlation_id") == run_id,
            ).all()
            for ej in extra_jobs:
                if ej.ulid not in known_ulids:
                    jobs.append(ej)
                    known_ulids.add(ej.ulid)

            # Build ulid->job map for ordered output
            ulid_map = {j.ulid: j for j in jobs}

            # Ordered by DagRun's ulid list first, then extras
            ordered_ulids = list(run.job_ulids)
            for j in jobs:
                if j.ulid not in set(run.job_ulids):
                    ordered_ulids.append(j.ulid)

            job_dicts = []
            for ulid in ordered_ulids:
                if ulid not in ulid_map:
                    continue
                j = ulid_map[ulid]
                task_name = ulid_to_name.get(ulid, j.task or "unknown")
                job_dicts.append({
                    "task_name": task_name,
                    "ulid": j.ulid,
                    "status": j.status,
                    "result": _safe_json(j.result_json),
                    "error": j.last_error,
                    "error_kind": j.last_failure_kind,
                    "started_at": j.lease_expires_at,
                    "finished_at": j.finished_at,
                })

            result = _dagrun_to_dict(run)
            result["jobs"] = job_dicts
            return result
        finally:
            await q.close()

    result = _run(_info())

    if result is None:
        _exit_error(ctx, "run_not_found", f"Run '{run_id}' not found")

    if _is_json_mode(ctx):
        _echo_result(ctx, result)
    else:
        human_lines = [
            f"Run:     {result['correlation_id']}",
            f"DAG:     {result['dag_name']}",
            f"Status:  {result['status']}",
            f"Created: {_format_ts(result['created_at'])}",
            f"Finished: {_format_ts(result['finished_at'])}",
            "",
        ]
        click.echo("\n".join(human_lines))

        if result["jobs"]:
            headers = ["TASK", "STATUS", "ULID", "RESULT", "ERROR"]
            rows = [
                [
                    j["task_name"],
                    j["status"],
                    j["ulid"],
                    str(j["result"])[:40] if j["result"] is not None else "-",
                    str(j["error"])[:40] if j["error"] is not None else "-",
                ]
                for j in result["jobs"]
            ]
            _echo_table(headers, rows)
        else:
            click.echo("No jobs.")


# ---------------------------------------------------------------------------
# dagler cancel
# ---------------------------------------------------------------------------

@cli.command("cancel")
@click.argument("run_id")
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.pass_context
def cancel_run(ctx: click.Context, run_id: str, db: str) -> None:
    """Cancel a pipeline run."""
    from sqler import F

    async def _cancel() -> dict[str, Any] | None:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        try:
            run = await DagRun.query().filter(
                F("correlation_id") == run_id,
            ).first()
            if run is None:
                return None

            # Refresh to populate job caches before cancel
            await run.refresh_status()
            await run.cancel(q)
            return _dagrun_to_dict(run)
        finally:
            await q.close()

    result = _run(_cancel())

    if result is None:
        _exit_error(ctx, "run_not_found", f"Run '{run_id}' not found")

    human_text = (
        f"Cancelled run: {result['correlation_id']}\n"
        f"  Status: {result['status']}"
    )
    _echo_result(ctx, result, human_text=human_text)


# ---------------------------------------------------------------------------
# dagler retry
# ---------------------------------------------------------------------------


@cli.command("retry")
@click.argument("run_id")
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.pass_context
def retry_run(ctx: click.Context, run_id: str, db: str) -> None:
    """Retry failed/cancelled jobs in a pipeline run."""
    from sqler import F

    from dagler.exceptions import RetryCompletedRunError, RetryNoFailuresError

    async def _retry() -> dict[str, Any] | str:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        try:
            run = await DagRun.query().filter(
                F("correlation_id") == run_id,
            ).first()
            if run is None:
                return "not_found"

            # Refresh to populate job caches before retry
            await run.refresh_status()

            # Count retryable jobs before retry
            retryable_count = sum(
                1 for j in run.jobs if j.status in ("failed", "cancelled")
            )

            try:
                await run.retry(q)
            except RetryCompletedRunError:
                return "completed"
            except RetryNoFailuresError:
                return "no_failures"

            result = _dagrun_to_dict(run)
            result["retried_count"] = retryable_count
            return result
        finally:
            await q.close()

    result = _run(_retry())

    if result == "not_found":
        _exit_error(ctx, "run_not_found", f"Run '{run_id}' not found")
    elif result == "completed":
        _exit_error(ctx, "retry_not_needed", f"Run '{run_id}' is already completed")
    elif result == "no_failures":
        _exit_error(
            ctx, "retry_not_needed",
            f"Run '{run_id}' has no failed or cancelled jobs to retry",
        )
    else:
        human_text = (
            f"Retried run: {result['correlation_id']}\n"
            f"  Status:  {result['status']}\n"
            f"  Retried: {result['retried_count']} of {result['job_count']} jobs"
        )
        _echo_result(ctx, result, human_text=human_text)


# ---------------------------------------------------------------------------
# dagler wait
# ---------------------------------------------------------------------------

@cli.command("wait")
@click.argument("run_id")
@click.option("--db", required=True, envvar="DAGLER_DB", help="Database file path")
@click.option("--timeout", default=None, type=float, help="Max seconds to wait")
@click.option("--poll-interval", default=0.5, type=float, help="Poll interval in seconds")
@click.pass_context
def wait_run(ctx: click.Context, run_id: str, db: str, timeout: float | None, poll_interval: float) -> None:
    """Wait for a pipeline run to reach terminal state."""
    from sqler import F

    async def _wait() -> dict[str, Any] | None:
        q = Queue(db)
        await q.init_db()
        DagRun.set_db(q.db, "dagler_runs")
        try:
            run = await DagRun.query().filter(
                F("correlation_id") == run_id,
            ).first()
            if run is None:
                return None

            # Populate job caches
            await run.refresh_status()
            await run.wait(timeout=timeout, poll_interval=poll_interval)
            return _dagrun_to_dict(run)
        finally:
            await q.close()

    try:
        result = _run(_wait())
    except TimeoutError:
        _exit_error(ctx, "timeout", f"Timed out waiting for run '{run_id}'")
        return  # unreachable, but helps type checker

    if result is None:
        _exit_error(ctx, "run_not_found", f"Run '{run_id}' not found")

    human_text = (
        f"Run:     {result['correlation_id']}\n"
        f"Status:  {result['status']}\n"
        f"Jobs:    {result['job_count']}"
    )
    _echo_result(ctx, result, human_text=human_text)


# ---------------------------------------------------------------------------
# dagler schema
# ---------------------------------------------------------------------------

# Output schemas per command — hardcoded to prevent drift.
# A test verifies every command has an entry.
_RUN_SUCCESS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "correlation_id": {"type": "string"},
        "dag_name": {"type": "string"},
        "status": {"type": "string"},
        "job_count": {"type": "integer"},
        "task_names": {"type": "array", "items": {"type": "string"}},
        "job_ulids": {"type": "array", "items": {"type": "string"}},
        "created_at": {"type": "integer"},
        "updated_at": {"type": "integer"},
        "finished_at": {"type": ["integer", "null"]},
        "idempotency_key": {"type": ["string", "null"]},
    },
}

_OUTPUT_SCHEMAS: dict[str, dict[str, Any]] = {
    "init": {
        "success": {
            "type": "object",
            "properties": {
                "db": {"type": "string"},
                "created": {"type": "boolean"},
                "gitignore_updated": {"type": "boolean"},
            },
        },
        "errors": [],
    },
    "run": {
        "success": _RUN_SUCCESS_SCHEMA,
        "errors": ["dag_not_found", "invalid_payload"],
    },
    "runs": {
        "success": {
            "type": "array",
            "items": _RUN_SUCCESS_SCHEMA,
        },
        "errors": [],
    },
    "run-info": {
        "success": {
            "type": "object",
            "properties": {
                "correlation_id": {"type": "string"},
                "dag_name": {"type": "string"},
                "status": {"type": "string"},
                "job_count": {"type": "integer"},
                "jobs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "task_name": {"type": "string"},
                            "ulid": {"type": "string"},
                            "status": {"type": "string"},
                            "result": {"type": ["object", "array", "string", "number", "boolean", "null"]},
                            "error": {"type": ["string", "null"]},
                            "error_kind": {"type": ["string", "null"]},
                            "started_at": {"type": ["integer", "null"], "description": "Lease expiry epoch seconds"},
                            "finished_at": {"type": ["integer", "null"], "description": "UTC epoch seconds"},
                        },
                    },
                },
            },
        },
        "errors": ["run_not_found"],
    },
    "retry": {
        "success": {
            "type": "object",
            "properties": {
                **_RUN_SUCCESS_SCHEMA["properties"],
                "retried_count": {"type": "integer", "description": "Number of jobs retried"},
            },
        },
        "errors": ["run_not_found", "retry_not_needed"],
    },
    "cancel": {
        "success": _RUN_SUCCESS_SCHEMA,
        "errors": ["run_not_found"],
    },
    "wait": {
        "success": _RUN_SUCCESS_SCHEMA,
        "errors": ["run_not_found", "timeout"],
    },
    "schema": {
        "success": {"type": "object", "description": "Full CLI schema"},
        "errors": [],
    },
}


@cli.command("schema")
@click.pass_context
def schema_cmd(ctx: click.Context) -> None:
    """Machine-readable CLI schema for tool discovery."""
    from dagler import __version__

    def _serialize_param(param: click.Parameter) -> dict[str, Any]:
        """Build a JSON-safe dict for a Click parameter."""
        info: dict[str, Any] = {
            "type": param.type.name if hasattr(param.type, "name") else str(param.type),
            "required": param.required,
        }
        if hasattr(param, "help") and param.help:
            info["help"] = param.help
        # Guard against Click's internal sentinel objects
        default = param.default
        if default is not None and type(default).__name__ not in ("Sentinel",):
            info["default"] = default
        if hasattr(param.type, "choices") and param.type.choices:
            info["choices"] = list(param.type.choices)
        return info

    commands: dict[str, Any] = {}
    group = ctx.parent.command if ctx.parent else cli

    for cmd_name, cmd in sorted(group.commands.items()):
        cmd_info: dict[str, Any] = {
            "description": cmd.help or "",
            "arguments": {},
            "options": {},
        }

        for param in cmd.params:
            param_info = _serialize_param(param)

            if isinstance(param, click.Argument):
                cmd_info["arguments"][param.name] = param_info
            elif isinstance(param, click.Option):
                opt_name = max(param.opts, key=len) if param.opts else param.name
                cmd_info["options"][opt_name] = param_info

        if cmd_name in _OUTPUT_SCHEMAS:
            cmd_info["output"] = _OUTPUT_SCHEMAS[cmd_name]

        commands[cmd_name] = cmd_info

    # Include group-level options (--human, --json)
    global_options: dict[str, Any] = {}
    for param in group.params:
        if isinstance(param, click.Option):
            opt_name = max(param.opts, key=len) if param.opts else param.name
            global_options[opt_name] = _serialize_param(param)

    schema_data = {
        "name": "dagler",
        "version": __version__,
        "global_options": global_options,
        "commands": commands,
        "envelope": {
            "success": {"ok": True, "data": "..."},
            "error": {"ok": False, "error": "code", "message": "...", "hint": "..."},
        },
        "exit_codes": {
            "0": "success",
            "1": "user_error",
            "2": "system_error",
        },
    }

    # schema always outputs JSON regardless of --human
    click.echo(json.dumps(schema_data, default=str, indent=2))
