"""Tests for DAG definition (M1).

Task functions are defined at module level because @dag.task rejects nested
functions (qualname != name). Each test creates a fresh DAG instance.
"""

import pytest
from dagler import DAG, TaskNode
from dagler.exceptions import CycleError, DaglerError, DependencyError, DuplicateTaskError

# ---------------------------------------------------------------------------
# Module-level task functions (required: @dag.task rejects nested functions)
# ---------------------------------------------------------------------------


async def extract():
    return {"data": [1, 2, 3]}


async def transform(extract_result):
    return {"transformed": True}


async def load(transform_result):
    return {"loaded": True}


async def branch_b():
    pass


async def branch_c():
    pass


async def merge_d():
    pass


async def self_dep_task():
    pass


async def duplicate_task():
    pass


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestTaskRegistration:
    def test_register_single_task(self):
        dag = DAG("test")
        dag.task(extract)

        assert len(dag.tasks) == 1
        node = dag.tasks["extract"]
        assert node.name == "extract"
        assert node.task_path == "tests.test_dag.extract"
        assert node.dependencies == ()
        assert node.fn is extract

    def test_register_task_with_parens(self):
        dag = DAG("test")
        decorator = dag.task()
        result = decorator(extract)

        assert len(dag.tasks) == 1
        node = dag.tasks["extract"]
        assert node.name == "extract"
        assert node.task_path == "tests.test_dag.extract"
        assert node.dependencies == ()
        assert node.fn is extract
        assert result is extract

    def test_register_with_dependencies(self):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])

        node = dag.tasks["transform"]
        assert node.dependencies == ("extract",)

    def test_register_with_multiple_dependencies(self):
        dag = DAG("test")
        dag.task(extract)
        dag.task(branch_b, depends_on=[extract])
        dag.task(branch_c, depends_on=[extract])
        dag.task(merge_d, depends_on=[branch_b, branch_c])

        node = dag.tasks["merge_d"]
        assert node.dependencies == ("branch_b", "branch_c")

    def test_register_with_empty_depends_on(self):
        dag = DAG("test")
        dag.task(extract, depends_on=[])

        node = dag.tasks["extract"]
        assert node.dependencies == ()

    def test_diamond_dag(self):
        dag = DAG("diamond")
        dag.task(extract)
        dag.task(branch_b, depends_on=[extract])
        dag.task(branch_c, depends_on=[extract])
        dag.task(merge_d, depends_on=[branch_b, branch_c])

        assert len(dag.tasks) == 4


# ---------------------------------------------------------------------------
# Decorator validation (eager, at decoration time)
# ---------------------------------------------------------------------------


class TestValidation:
    def test_reject_nested_function(self):
        dag = DAG("test")

        def inner():
            pass

        with pytest.raises(
            DaglerError,
            match=r"Cannot register nested function.*inner.*module-level",
        ):
            dag.task(inner)

    def test_reject_duplicate_name(self):
        dag = DAG("test")
        dag.task(duplicate_task)

        with pytest.raises(
            DuplicateTaskError,
            match=r"Task 'duplicate_task' already registered in DAG 'test'",
        ):
            dag.task(duplicate_task)

    def test_reject_unknown_dependency(self):
        dag = DAG("test")

        with pytest.raises(
            DependencyError,
            match=r"Dependency 'extract' not registered in DAG 'test'",
        ):
            dag.task(transform, depends_on=[extract])

    def test_reject_self_dependency(self):
        dag = DAG("test")

        with pytest.raises(
            DependencyError,
            match=r"Task 'self_dep_task' cannot depend on itself",
        ):
            dag.task(self_dep_task, depends_on=[self_dep_task])


# ---------------------------------------------------------------------------
# Cycle detection (lazy, at validate() time)
# ---------------------------------------------------------------------------


class TestCycleDetection:
    def test_cycle_detection_direct(self):
        """A→B→A raises CycleError."""
        dag = DAG("test")
        # Construct cycle by directly inserting nodes — the decorator's eager
        # validation prevents cycles from forming through the public API.
        dag._tasks["a"] = TaskNode(
            fn=extract, name="a", task_path="test.a", dependencies=("b",)
        )
        dag._tasks["b"] = TaskNode(
            fn=transform, name="b", task_path="test.b", dependencies=("a",)
        )

        with pytest.raises(CycleError, match=r"Cycle detected in DAG 'test'"):
            dag.validate()

    def test_cycle_detection_indirect(self):
        """A→B→C→A raises CycleError."""
        dag = DAG("test")
        dag._tasks["a"] = TaskNode(
            fn=extract, name="a", task_path="test.a", dependencies=("c",)
        )
        dag._tasks["b"] = TaskNode(
            fn=transform, name="b", task_path="test.b", dependencies=("a",)
        )
        dag._tasks["c"] = TaskNode(
            fn=load, name="c", task_path="test.c", dependencies=("b",)
        )

        with pytest.raises(CycleError, match=r"Cycle detected in DAG 'test'"):
            dag.validate()


# ---------------------------------------------------------------------------
# Topological ordering
# ---------------------------------------------------------------------------


class TestTopologicalOrder:
    def test_topological_order(self):
        dag = DAG("test")
        dag.task(extract)
        dag.task(transform, depends_on=[extract])
        dag.task(load, depends_on=[transform])

        order = dag.validate()
        assert order == ["extract", "transform", "load"]

    def test_topological_order_diamond(self):
        dag = DAG("diamond")
        dag.task(extract)
        dag.task(branch_b, depends_on=[extract])
        dag.task(branch_c, depends_on=[extract])
        dag.task(merge_d, depends_on=[branch_b, branch_c])

        order = dag.validate()
        assert len(order) == 4
        assert order.index("extract") < order.index("branch_b")
        assert order.index("extract") < order.index("branch_c")
        assert order.index("branch_b") < order.index("merge_d")
        assert order.index("branch_c") < order.index("merge_d")


# ---------------------------------------------------------------------------
# DAG properties
# ---------------------------------------------------------------------------


class TestDAGProperties:
    def test_tasks_property_readonly(self):
        dag = DAG("test")
        dag.task(extract)

        tasks_copy = dag.tasks
        tasks_copy["injected"] = TaskNode(
            fn=transform,
            name="injected",
            task_path="test.injected",
            dependencies=(),
        )

        assert "injected" not in dag.tasks
        assert len(dag.tasks) == 1

    def test_empty_dag_validates(self):
        dag = DAG("empty")
        order = dag.validate()
        assert order == []

    def test_dag_name(self):
        dag = DAG("my-pipeline")
        assert dag.name == "my-pipeline"

    def test_dags_are_isolated(self):
        dag_a = DAG("a")
        dag_b = DAG("b")
        dag_a.task(extract)

        assert "extract" not in dag_b.tasks
        assert len(dag_a.tasks) == 1
        assert len(dag_b.tasks) == 0


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    def test_all_exceptions_inherit_from_dagler_error(self):
        assert issubclass(CycleError, DaglerError)
        assert issubclass(DuplicateTaskError, DaglerError)
        assert issubclass(DependencyError, DaglerError)
