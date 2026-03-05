"""Tests for dagler CLI (M5 + M7).

Task functions are defined at module level because @dag.task rejects nested
functions (qualname != name).

CliRunner is non-TTY, so auto-detect defaults to JSON output. Tests that
check human output pass --human explicitly.
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner
from dagler import DAG
from dagler.cli import _OUTPUT_SCHEMAS, cli

# ---------------------------------------------------------------------------
# Module-level task functions (for DAG registration)
# ---------------------------------------------------------------------------


async def cli_extract():
    return {"extracted": True}


async def cli_transform(cli_extract=None):
    return {"transformed": True}


async def cli_load(cli_transform=None):
    return {"loaded": True}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear DAG registry before and after each test."""
    saved = dict(DAG._registry)
    DAG._registry.clear()
    yield
    DAG._registry.clear()
    DAG._registry.update(saved)


@pytest.fixture
def sample_dag():
    """Create a 3-task DAG registered in the global registry."""
    dag = DAG("etl")
    dag.task(cli_extract)
    dag.task(cli_transform, depends_on=[cli_extract])
    dag.task(cli_load, depends_on=[cli_transform])
    return dag


def _init_db(runner, db_path):
    """Helper to init DB, returns parsed envelope. Asserts creation succeeded."""
    result = runner.invoke(cli, ["init", "--db", db_path])
    assert result.exit_code == 0
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["data"]["created"] is True
    return envelope


def _run_dag(runner, db_path, dag_name="etl", extra_args=None):
    """Helper to submit a DAG run, returns parsed envelope."""
    args = ["run", dag_name, "--db", db_path]
    if extra_args:
        args.extend(extra_args)
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    return json.loads(result.output)


# ---------------------------------------------------------------------------
# TestJSONEnvelope
# ---------------------------------------------------------------------------


class TestJSONEnvelope:
    def test_success_envelope_has_data_fields(self, runner, db_path):
        result = runner.invoke(cli, ["init", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["db"] == db_path
        assert envelope["data"]["created"] is True

    def test_error_envelope_has_code_and_message(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "dag_not_found"
        assert "nonexistent" in envelope["message"]

    def test_explicit_json_flag(self, runner, db_path):
        """--json flag forces JSON output."""
        result = runner.invoke(cli, ["--json", "init", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["db"] == db_path

    def test_list_envelope_has_count(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        _run_dag(runner, db_path)

        result = runner.invoke(cli, ["runs", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["count"] == 1
        assert len(envelope["data"]) == 1
        assert envelope["data"][0]["dag_name"] == "etl"


# ---------------------------------------------------------------------------
# TestStdoutStderrSeparation
# ---------------------------------------------------------------------------


class TestStdoutStderrSeparation:
    def test_json_error_on_stdout(self, runner, db_path):
        """In JSON mode, errors go to stdout as envelope."""
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        # stdout has the JSON error envelope
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        # stderr is empty
        assert result.stderr == "" or result.stderr is None

    def test_human_error_on_stderr(self, runner, db_path):
        """In human mode, errors go to stderr."""
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["--human", "run", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        # stderr has the error message
        assert "not found" in result.stderr


# ---------------------------------------------------------------------------
# TestExitCodes
# ---------------------------------------------------------------------------


class TestExitCodes:
    def test_success_exit_0(self, runner, db_path):
        result = runner.invoke(cli, ["init", "--db", db_path])
        assert result.exit_code == 0

    def test_user_error_exit_1(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "nonexistent", "--db", db_path])
        assert result.exit_code == 1

    def test_not_found_exit_1(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run-info", "nonexistent", "--db", db_path])
        assert result.exit_code == 1

    def test_missing_required_exit_2(self, runner, sample_dag):
        """click.UsageError for missing --db/--app -> exit 2."""
        result = runner.invoke(cli, ["run", "etl"])
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# TestInit
# ---------------------------------------------------------------------------


class TestInit:
    def test_creates_db_json(self, runner, db_path):
        result = runner.invoke(cli, ["init", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["db"] == db_path
        assert envelope["data"]["created"] is True
        assert "gitignore_updated" in envelope["data"]

    def test_creates_db_human(self, runner, db_path):
        result = runner.invoke(cli, ["--human", "init", "--db", db_path])

        assert result.exit_code == 0
        assert "Created database" in result.output

    def test_idempotent(self, runner, db_path):
        runner.invoke(cli, ["init", "--db", db_path])
        result = runner.invoke(cli, ["init", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["created"] is True

    def test_gitignore_created(self, runner, tmp_path):
        db = str(tmp_path / "pipeline.db")
        runner.invoke(cli, ["init", "--db", db])

        gitignore = tmp_path / ".gitignore"
        assert gitignore.exists()
        assert "pipeline.db" in gitignore.read_text()

    def test_gitignore_appended(self, runner, tmp_path):
        gitignore = tmp_path / ".gitignore"
        gitignore.write_text("*.pyc\n")

        db = str(tmp_path / "pipeline.db")
        runner.invoke(cli, ["init", "--db", db])

        content = gitignore.read_text()
        assert "*.pyc" in content
        assert "pipeline.db" in content

    def test_gitignore_not_duplicated(self, runner, tmp_path):
        gitignore = tmp_path / ".gitignore"
        gitignore.write_text("pipeline.db\n")

        db = str(tmp_path / "pipeline.db")
        result = runner.invoke(cli, ["init", "--db", db])

        envelope = json.loads(result.output)
        assert envelope["data"]["gitignore_updated"] is False
        lines = gitignore.read_text().strip().split("\n")
        assert lines.count("pipeline.db") == 1

    def test_init_via_env_var(self, runner, db_path):
        result = runner.invoke(cli, ["init"], env={"DAGLER_DB": db_path})

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True


# ---------------------------------------------------------------------------
# TestRun
# ---------------------------------------------------------------------------


class TestRun:
    def test_submit_dag_json(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "etl", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        data = envelope["data"]
        assert data["dag_name"] == "etl"
        assert data["status"] == "pending"
        assert data["job_count"] == 3
        assert data["task_names"] == ["cli_extract", "cli_transform", "cli_load"]
        assert len(data["correlation_id"]) == 26  # ULID

    def test_submit_dag_human(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["--human", "run", "etl", "--db", db_path])

        assert result.exit_code == 0
        assert "Submitted DAG 'etl'" in result.output
        assert "Jobs:    3" in result.output

    def test_missing_dag_error(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "dag_not_found"
        assert "nonexistent" in envelope["message"]

    def test_missing_dag_shows_available(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["hint"] is not None
        assert "etl" in envelope["hint"]

    def test_custom_correlation_id(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        result = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--correlation-id", "my-run-123",
        ])

        assert result.exit_code == 0
        data = json.loads(result.output)["data"]
        assert data["correlation_id"] == "my-run-123"

    def test_payload_passing(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        payload = json.dumps({"cli_extract": {"url": "http://example.com"}})
        result = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--payload", payload,
        ])

        assert result.exit_code == 0
        data = json.loads(result.output)["data"]
        assert data["dag_name"] == "etl"
        assert data["job_count"] == 3

        # Verify payload reached the job via run-info
        cid = data["correlation_id"]
        info = runner.invoke(cli, ["run-info", cid, "--db", db_path])
        info_data = json.loads(info.output)["data"]
        assert len(info_data["jobs"]) == 3
        assert info_data["jobs"][0]["task_name"] == "cli_extract"

    def test_no_db_or_app_error(self, runner, sample_dag):
        result = runner.invoke(cli, ["run", "etl"])
        assert result.exit_code == 2  # click.UsageError

    def test_invalid_payload_json(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        result = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--payload", "not-json",
        ])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "invalid_payload"


# ---------------------------------------------------------------------------
# TestRuns
# ---------------------------------------------------------------------------


class TestRuns:
    def test_list_runs_json(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        _run_dag(runner, db_path)
        _run_dag(runner, db_path)

        result = runner.invoke(cli, ["runs", "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["count"] == 2
        assert len(envelope["data"]) == 2

    def test_list_runs_human(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        _run_dag(runner, db_path)

        result = runner.invoke(cli, ["--human", "runs", "--db", db_path])

        assert result.exit_code == 0
        assert "etl" in result.output
        assert "pending" in result.output
        assert "CORRELATION_ID" in result.output

    def test_limit(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        for _ in range(5):
            _run_dag(runner, db_path)

        result = runner.invoke(cli, ["runs", "--db", db_path, "--limit", "2"])

        envelope = json.loads(result.output)
        assert len(envelope["data"]) == 2
        assert envelope["count"] == 2

    def test_status_filter(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        _run_dag(runner, db_path)

        # Filter for completed — should return empty since runs are pending
        result = runner.invoke(cli, [
            "runs", "--db", db_path, "--status", "completed",
        ])

        envelope = json.loads(result.output)
        assert len(envelope["data"]) == 0
        assert envelope["count"] == 0

    def test_empty_list_human(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["--human", "runs", "--db", db_path])

        assert result.exit_code == 0
        assert "No runs found" in result.output

    def test_empty_list_json(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["runs", "--db", db_path])

        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"] == []
        assert envelope["count"] == 0


# ---------------------------------------------------------------------------
# TestRunInfo
# ---------------------------------------------------------------------------


class TestRunInfo:
    def test_show_details_json(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["run-info", cid, "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        data = envelope["data"]
        assert data["correlation_id"] == cid
        assert data["dag_name"] == "etl"
        assert len(data["jobs"]) == 3
        assert data["jobs"][0]["task_name"] == "cli_extract"
        assert data["jobs"][1]["task_name"] == "cli_transform"
        assert data["jobs"][2]["task_name"] == "cli_load"
        for job in data["jobs"]:
            assert job["status"] == "pending"
            assert len(job["ulid"]) == 26  # ULID format

    def test_show_details_human(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["--human", "run-info", cid, "--db", db_path])

        assert result.exit_code == 0
        assert cid in result.output
        assert "etl" in result.output
        assert "cli_extract" in result.output
        assert "cli_transform" in result.output
        assert "cli_load" in result.output

    def test_missing_run_error(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["run-info", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "run_not_found"
        assert "nonexistent" in envelope["message"]

    def test_missing_run_human_error(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["--human", "run-info", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        assert "not found" in result.stderr


# ---------------------------------------------------------------------------
# TestCancel
# ---------------------------------------------------------------------------


class TestCancel:
    def test_cancel_pending_run_json(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["cancel", cid, "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["correlation_id"] == cid
        assert envelope["data"]["status"] == "cancelled"

    def test_cancel_pending_run_human(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["--human", "cancel", cid, "--db", db_path])

        assert result.exit_code == 0
        assert "Cancelled run" in result.output
        assert "cancelled" in result.output

    def test_missing_run_error(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["cancel", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "run_not_found"

    def test_cancel_verifies_job_states(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        runner.invoke(cli, ["cancel", cid, "--db", db_path])

        info = runner.invoke(cli, ["run-info", cid, "--db", db_path])
        info_data = json.loads(info.output)["data"]
        assert len(info_data["jobs"]) == 3
        for job in info_data["jobs"]:
            assert job["status"] == "cancelled"

    def test_cancel_reflects_in_runs_list(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        runner.invoke(cli, ["cancel", cid, "--db", db_path])

        result = runner.invoke(cli, ["runs", "--db", db_path])
        data = json.loads(result.output)["data"]
        assert len(data) == 1
        assert data[0]["status"] == "cancelled"


# ---------------------------------------------------------------------------
# TestWait
# ---------------------------------------------------------------------------


class TestWait:
    def test_wait_not_found(self, runner, db_path):
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["wait", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "run_not_found"

    def test_wait_already_cancelled(self, runner, db_path, sample_dag):
        """Wait on a cancelled run returns immediately with final state."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        runner.invoke(cli, ["cancel", cid, "--db", db_path])
        result = runner.invoke(cli, ["wait", cid, "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["status"] == "cancelled"

    def test_wait_timeout(self, runner, db_path, sample_dag):
        """Wait with short timeout on pending run exits 1 with timeout error."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, [
            "wait", cid, "--db", db_path,
            "--timeout", "0.01", "--poll-interval", "0.01",
        ])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "timeout"

    def test_wait_human_output(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        # Cancel so wait returns immediately
        runner.invoke(cli, ["cancel", cid, "--db", db_path])
        result = runner.invoke(cli, ["--human", "wait", cid, "--db", db_path])

        assert result.exit_code == 0
        assert "Run:" in result.output
        assert "Status:" in result.output


# ---------------------------------------------------------------------------
# TestIdempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_same_key_returns_same_run(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)

        result1 = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--idempotency-key", "test-key-1",
        ])
        result2 = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--idempotency-key", "test-key-1",
        ])

        assert result1.exit_code == 0
        assert result2.exit_code == 0
        cid1 = json.loads(result1.output)["data"]["correlation_id"]
        cid2 = json.loads(result2.output)["data"]["correlation_id"]
        assert cid1 == cid2

    def test_different_keys_different_runs(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)

        result1 = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--idempotency-key", "key-a",
        ])
        result2 = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--idempotency-key", "key-b",
        ])

        assert result1.exit_code == 0
        assert result2.exit_code == 0
        cid1 = json.loads(result1.output)["data"]["correlation_id"]
        cid2 = json.loads(result2.output)["data"]["correlation_id"]
        assert cid1 != cid2

    def test_no_key_no_dedup(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)

        result1 = runner.invoke(cli, ["run", "etl", "--db", db_path])
        result2 = runner.invoke(cli, ["run", "etl", "--db", db_path])

        cid1 = json.loads(result1.output)["data"]["correlation_id"]
        cid2 = json.loads(result2.output)["data"]["correlation_id"]
        assert cid1 != cid2

    def test_idempotency_key_in_response(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)

        result = runner.invoke(cli, [
            "run", "etl", "--db", db_path,
            "--idempotency-key", "my-key",
        ])

        data = json.loads(result.output)["data"]
        assert data["idempotency_key"] == "my-key"


# ---------------------------------------------------------------------------
# TestSchema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_schema_returns_valid_json(self, runner):
        result = runner.invoke(cli, ["schema"])

        assert result.exit_code == 0
        schema = json.loads(result.output)
        assert schema["name"] == "dagler"
        assert "version" in schema

    def test_all_commands_present(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        expected_commands = {"init", "run", "runs", "run-info", "cancel", "retry", "wait", "schema"}
        assert set(schema["commands"].keys()) == expected_commands

    def test_command_has_options(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        init_cmd = schema["commands"]["init"]
        assert "--db" in init_cmd["options"]
        assert init_cmd["options"]["--db"]["required"] is True

    def test_command_has_arguments(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        run_cmd = schema["commands"]["run"]
        assert "dag_name" in run_cmd["arguments"]

    def test_envelope_documented(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        assert "envelope" in schema
        assert schema["envelope"]["success"]["ok"] is True
        assert schema["envelope"]["error"]["ok"] is False

    def test_exit_codes_documented(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        assert schema["exit_codes"]["0"] == "success"
        assert schema["exit_codes"]["1"] == "user_error"
        assert schema["exit_codes"]["2"] == "system_error"

    def test_every_command_has_output_schema(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        for cmd_name, cmd_info in schema["commands"].items():
            assert "output" in cmd_info, f"Command '{cmd_name}' missing output schema"

    def test_schema_ignores_human_flag(self, runner):
        """Schema always outputs JSON even with --human."""
        result = runner.invoke(cli, ["--human", "schema"])

        assert result.exit_code == 0
        schema = json.loads(result.output)
        assert schema["name"] == "dagler"

    def test_global_options_present(self, runner):
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        assert "global_options" in schema
        assert "--human" in schema["global_options"]
        assert "--json" in schema["global_options"]

    def test_no_sentinel_in_defaults(self, runner):
        """Schema defaults must not leak Click's internal sentinel objects."""
        result = runner.invoke(cli, ["schema"])
        schema = json.loads(result.output)

        for cmd_name, cmd_info in schema["commands"].items():
            for opt_name, opt_info in cmd_info.get("options", {}).items():
                if "default" in opt_info:
                    assert "Sentinel" not in str(opt_info["default"]), (
                        f"Sentinel leaked in {cmd_name} {opt_name}: {opt_info['default']}"
                    )


# ---------------------------------------------------------------------------
# TestDAGRegistry
# ---------------------------------------------------------------------------


class TestDAGRegistry:
    def test_registry_populated_on_creation(self):
        dag = DAG("reg_test")
        assert "reg_test" in DAG._registry
        assert DAG._registry["reg_test"] is dag

    def test_registry_overwrite_on_same_name(self):
        dag1 = DAG("dup")
        dag2 = DAG("dup")
        assert DAG._registry["dup"] is dag2
        assert DAG._registry["dup"] is not dag1

    def test_registry_multiple_dags(self):
        dag_a = DAG("alpha")
        dag_b = DAG("beta")
        assert DAG._registry["alpha"] is dag_a
        assert DAG._registry["beta"] is dag_b
        assert len(DAG._registry) == 2

    def test_registry_clear_isolation(self):
        DAG("temp")
        assert "temp" in DAG._registry
        DAG._registry.clear()
        assert len(DAG._registry) == 0


# ---------------------------------------------------------------------------
# TestHelpOutput
# ---------------------------------------------------------------------------


class TestHelpOutput:
    def test_main_help(self, runner):
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "dagler" in result.output
        assert "init" in result.output
        assert "run" in result.output
        assert "runs" in result.output
        assert "run-info" in result.output
        assert "cancel" in result.output
        assert "retry" in result.output
        assert "wait" in result.output
        assert "schema" in result.output

    def test_main_help_shows_output_flags(self, runner):
        result = runner.invoke(cli, ["--help"])

        assert "--human" in result.output
        assert "--json" in result.output

    def test_init_help(self, runner):
        result = runner.invoke(cli, ["init", "--help"])

        assert result.exit_code == 0
        assert "--db" in result.output
        # Per-command --json should be gone
        assert "--json" not in result.output

    def test_run_help(self, runner):
        result = runner.invoke(cli, ["run", "--help"])

        assert result.exit_code == 0
        assert "--app" in result.output
        assert "--module" in result.output
        assert "--db" in result.output
        assert "--payload" in result.output
        assert "--correlation-id" in result.output
        assert "--idempotency-key" in result.output
        # Per-command --json should be gone
        assert "--json" not in result.output

    def test_runs_human_table_headers(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        _run_dag(runner, db_path)

        result = runner.invoke(cli, ["--human", "runs", "--db", db_path])

        assert "CORRELATION_ID" in result.output
        assert "DAG" in result.output
        assert "STATUS" in result.output
        assert "JOBS" in result.output

    def test_run_info_human_table_headers(self, runner, db_path, sample_dag):
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["--human", "run-info", cid, "--db", db_path])

        assert "TASK" in result.output
        assert "STATUS" in result.output
        assert "ULID" in result.output


# ---------------------------------------------------------------------------
# TestOutputSchemaCompleteness
# ---------------------------------------------------------------------------


class TestOutputSchemaCompleteness:
    def test_all_commands_have_schemas(self):
        """Every command registered in the CLI group has an entry in _OUTPUT_SCHEMAS."""
        for cmd_name in cli.commands:
            assert cmd_name in _OUTPUT_SCHEMAS, (
                f"Command '{cmd_name}' missing from _OUTPUT_SCHEMAS"
            )


# ---------------------------------------------------------------------------
# TestRetry
# ---------------------------------------------------------------------------


def _set_job_status(db_path, ulid, status, error=None, error_kind=None):
    """Helper to manually set a job's status (simulates failure/cancel)."""
    import asyncio

    from qler.models.job import Job
    from qler.queue import Queue
    from sqler import F

    async def _update():
        q = Queue(db_path)
        await q.init_db()
        try:
            updates = {"status": status}
            if error is not None:
                updates["last_error"] = error
            if error_kind is not None:
                updates["last_failure_kind"] = error_kind
            if status in ("failed", "cancelled", "completed"):
                import time
                updates["finished_at"] = int(time.time())
            await Job.query().filter(F("ulid") == ulid).update_one(**updates)
        finally:
            await q.close()

    asyncio.run(_update())


class TestRetry:
    def test_retry_success(self, runner, db_path, sample_dag):
        """retry returns envelope with updated DagRun; jobs are actually pending in DB."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]
        assert len(ulids) == 3

        # Simulate: first job completed, second failed, third cancelled
        _set_job_status(db_path, ulids[0], "completed")
        _set_job_status(db_path, ulids[1], "failed", error="boom", error_kind="exception")
        _set_job_status(db_path, ulids[2], "cancelled")

        result = runner.invoke(cli, ["retry", cid, "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        data = envelope["data"]
        assert data["correlation_id"] == cid
        assert data["status"] == "pending"
        assert data["retried_count"] == 2

        # Verify via run-info that jobs are actually in correct states
        info = runner.invoke(cli, ["run-info", cid, "--db", db_path])
        info_data = json.loads(info.output)["data"]
        assert len(info_data["jobs"]) == 3
        assert info_data["jobs"][0]["status"] == "completed"
        assert info_data["jobs"][1]["status"] == "pending"
        assert info_data["jobs"][2]["status"] == "pending"

    def test_retry_not_found(self, runner, db_path):
        """run_not_found error for bad ID."""
        _init_db(runner, db_path)
        result = runner.invoke(cli, ["retry", "nonexistent", "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "run_not_found"

    def test_retry_no_failures(self, runner, db_path, sample_dag):
        """retry_not_needed error on a fully pending run."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]

        result = runner.invoke(cli, ["retry", cid, "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "retry_not_needed"

    def test_retry_completed_run(self, runner, db_path, sample_dag):
        """retry_not_needed error on a completed run."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]
        assert len(ulids) == 3

        for ulid in ulids:
            _set_job_status(db_path, ulid, "completed")

        result = runner.invoke(cli, ["retry", cid, "--db", db_path])

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["error"] == "retry_not_needed"

    def test_retry_human_output(self, runner, db_path, sample_dag):
        """Human-readable output includes retry count."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]
        assert len(ulids) == 3

        _set_job_status(db_path, ulids[0], "completed")
        _set_job_status(db_path, ulids[1], "failed")
        _set_job_status(db_path, ulids[2], "cancelled")

        result = runner.invoke(cli, ["--human", "retry", cid, "--db", db_path])

        assert result.exit_code == 0
        assert "Retried run:" in result.output
        assert "Retried: 2 of 3 jobs" in result.output


# ---------------------------------------------------------------------------
# TestRunInfoErrors — run-info error/error_kind fields
# ---------------------------------------------------------------------------


class TestRunInfoErrors:
    def test_run_info_shows_error_fields(self, runner, db_path, sample_dag):
        """run-info includes error and error_kind for failed jobs."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]
        assert len(ulids) == 3

        _set_job_status(db_path, ulids[0], "completed")
        _set_job_status(
            db_path, ulids[1], "failed",
            error="ValueError: bad input", error_kind="exception",
        )

        result = runner.invoke(cli, ["run-info", cid, "--db", db_path])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        jobs = envelope["data"]["jobs"]
        assert len(jobs) == 3

        # Completed job has null error
        assert jobs[0]["error"] is None
        assert jobs[0]["error_kind"] is None

        # Failed job has error details
        assert jobs[1]["error"] == "ValueError: bad input"
        assert jobs[1]["error_kind"] == "exception"

        # Pending job has null error
        assert jobs[2]["error"] is None
        assert jobs[2]["error_kind"] is None

    def test_run_info_error_human_table(self, runner, db_path, sample_dag):
        """Human table shows ERROR column."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]

        _set_job_status(
            db_path, ulids[1], "failed",
            error="Something went wrong", error_kind="exception",
        )

        result = runner.invoke(cli, ["--human", "run-info", cid, "--db", db_path])

        assert result.exit_code == 0
        assert "ERROR" in result.output
        assert "Something went wrong" in result.output

    def test_run_info_error_cleared_after_retry(self, runner, db_path, sample_dag):
        """Error fields are null after a failed job is retried."""
        _init_db(runner, db_path)
        run_data = _run_dag(runner, db_path)
        cid = run_data["data"]["correlation_id"]
        ulids = run_data["data"]["job_ulids"]
        assert len(ulids) == 3

        _set_job_status(db_path, ulids[0], "completed")
        _set_job_status(
            db_path, ulids[1], "failed",
            error="ValueError: bad input", error_kind="exception",
        )
        _set_job_status(db_path, ulids[2], "cancelled")

        # Retry the run
        retry_result = runner.invoke(cli, ["retry", cid, "--db", db_path])
        assert retry_result.exit_code == 0

        # Check run-info: error fields should be cleared on retried jobs
        info = runner.invoke(cli, ["run-info", cid, "--db", db_path])
        info_data = json.loads(info.output)["data"]
        assert len(info_data["jobs"]) == 3

        # Previously-failed job now pending with cleared errors
        assert info_data["jobs"][1]["status"] == "pending"
        assert info_data["jobs"][1]["error"] is None
        assert info_data["jobs"][1]["error_kind"] is None

        # Previously-cancelled job also cleared
        assert info_data["jobs"][2]["status"] == "pending"
        assert info_data["jobs"][2]["error"] is None
