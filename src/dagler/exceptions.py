"""Exceptions for dagler DAG operations."""


class DaglerError(Exception):
    """Base exception for dagler."""


class CycleError(DaglerError):
    """Cycle detected in DAG."""


class DuplicateTaskError(DaglerError):
    """Task name already registered in DAG."""


class DependencyError(DaglerError):
    """Unknown or invalid dependency reference."""


class RetryCompletedRunError(DaglerError):
    """Cannot retry a run that has already completed."""


class RetryNoFailuresError(DaglerError):
    """No failed or cancelled jobs to retry."""
