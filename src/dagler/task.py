"""TaskNode — lightweight metadata wrapper for registered DAG tasks."""

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class TaskNode:
    """Immutable metadata for a task registered in a DAG.

    Attributes:
        fn: The original async function.
        name: Function name (used as DAG node key).
        task_path: ``module.qualname`` path for qler worker resolution.
        dependencies: Names of parent TaskNodes this task depends on.
        inject_as: Mapping of ``{parent_name: kwarg_name}`` for renaming
            injected parent results.  Defaults to using parent task names.
    """

    fn: Callable
    name: str
    task_path: str
    dependencies: tuple[str, ...]
    inject_as: dict[str, str] = field(default_factory=dict)
    kind: str = "task"          # "task" | "map" | "reduce"
    inject_param: str = ""      # map: item param name, reduce: results param name
