"""Benchmark framework core — timer, stats, runner, system info.

Adapted from the -ler benchmark pattern (sqler/logler) for async workloads.
"""

from __future__ import annotations

import gc
import json
import math
import os
import platform
import sqlite3
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

# ── TimingStats ────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class TimingStats:
    """Statistical summary of timing measurements."""

    iterations: int
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    stddev_ms: float
    total_ms: float
    reliable_p95: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def _percentile(sorted_data: list[float], p: float) -> float:
    """Linear interpolation percentile on sorted data."""
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    idx = p * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])


def compute_stats(timings_ms: list[float]) -> TimingStats:
    """Build TimingStats from a list of ms values."""
    n = len(timings_ms)
    if n == 0:
        return TimingStats(
            iterations=0,
            min_ms=0, max_ms=0, mean_ms=0, median_ms=0,
            p95_ms=0, p99_ms=0, stddev_ms=0, total_ms=0,
            reliable_p95=False,
        )

    sorted_t = sorted(timings_ms)
    reliable = n >= 20

    if reliable:
        p95 = _percentile(sorted_t, 0.95)
        p99 = _percentile(sorted_t, 0.99)
    else:
        p95 = float("nan")
        p99 = float("nan")

    return TimingStats(
        iterations=n,
        min_ms=round(sorted_t[0], 4),
        max_ms=round(sorted_t[-1], 4),
        mean_ms=round(statistics.mean(sorted_t), 4),
        median_ms=round(statistics.median(sorted_t), 4),
        p95_ms=round(p95, 4) if not math.isnan(p95) else p95,
        p99_ms=round(p99, 4) if not math.isnan(p99) else p99,
        stddev_ms=round(statistics.stdev(sorted_t), 4) if n > 1 else 0,
        total_ms=round(sum(sorted_t), 4),
        reliable_p95=reliable,
    )


# ── AsyncTimer ─────────────────────────────────────────────────────────


class AsyncTimer:
    """Precision timer for async benchmark measurements.

    Runs warmup iterations (discarded), then measured iterations.
    GC is isolated during measurement for reduced noise.
    """

    def __init__(self, warmup: int = 3, iterations: int = 20):
        self.warmup = warmup
        self.iterations = iterations

    async def measure(self, fn, *, gc_isolate: bool = True) -> TimingStats:
        """Run async fn with warmup + measured iterations, return stats."""
        for _ in range(self.warmup):
            await fn()

        timings_ms: list[float] = []

        if gc_isolate:
            gc.collect()
            gc.disable()

        try:
            for _ in range(self.iterations):
                start = time.perf_counter()
                await fn()
                elapsed = (time.perf_counter() - start) * 1000
                timings_ms.append(elapsed)
        finally:
            if gc_isolate:
                gc.enable()

        return compute_stats(timings_ms)


# ── BenchmarkResult ────────────────────────────────────────────────────


@dataclass(slots=True)
class BenchmarkResult:
    """Single benchmark measurement at one parameter point."""

    scenario: str
    suite: str
    parameter: str
    value: int | float | str
    timing: TimingStats
    ops: int = 0
    throughput: float = 0.0  # ops/sec
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ── Scenario Protocol ──────────────────────────────────────────────────


@runtime_checkable
class Scenario(Protocol):
    """Protocol for benchmark scenarios."""

    name: str
    suite: str
    description: str

    async def run(self, warmup: int, iterations: int) -> list[BenchmarkResult]: ...


# ── SystemInfo ─────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SystemInfo:
    """System information captured at benchmark run time."""

    python_version: str
    dagler_version: str
    qler_version: str
    sqler_version: str
    sqlite_version: str
    platform_system: str
    platform_machine: str
    cpu_count: int
    timestamp: str

    @classmethod
    def collect(cls) -> SystemInfo:
        from importlib.metadata import version as pkg_version

        def _ver(pkg: str) -> str:
            try:
                return pkg_version(pkg)
            except Exception:
                return "dev"

        return cls(
            python_version=sys.version.split()[0],
            dagler_version=_ver("dagler"),
            qler_version=_ver("qler"),
            sqler_version=_ver("sqler"),
            sqlite_version=sqlite3.sqlite_version,
            platform_system=platform.system(),
            platform_machine=platform.machine(),
            cpu_count=os.cpu_count() or 1,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def summary_line(self) -> str:
        return (
            f"Python {self.python_version} | "
            f"dagler {self.dagler_version} | "
            f"qler {self.qler_version} | "
            f"SQLite {self.sqlite_version} | "
            f"{self.platform_system} {self.platform_machine} ({self.cpu_count} cores)"
        )


# ── BenchmarkRunner ────────────────────────────────────────────────────


class BenchmarkRunner:
    """Discovers and runs async benchmark scenarios, saves results."""

    def __init__(self, warmup: int = 3, iterations: int = 20, output_dir: str = "benchmarks/results"):
        self.warmup = warmup
        self.iterations = iterations
        self.output_dir = output_dir
        self.system_info = SystemInfo.collect()

    async def run(self, scenarios: list[Scenario]) -> list[BenchmarkResult]:
        """Execute all scenarios and return combined results."""
        all_results: list[BenchmarkResult] = []
        total = len(scenarios)

        for i, scenario in enumerate(scenarios, 1):
            label = f"[{i}/{total}] {scenario.suite}/{scenario.name}"
            print(f"\n{'='*60}")
            print(f"  {label}")
            print(f"  {scenario.description}")
            print(f"{'='*60}")

            gc.collect()

            try:
                results = await scenario.run(self.warmup, self.iterations)
                all_results.extend(results)
                print(f"  -> {len(results)} results")

                for r in results:
                    p95_str = (
                        f"{r.timing.p95_ms:.2f}"
                        if r.timing.reliable_p95
                        else "n/a"
                    )
                    print(
                        f"     {r.parameter}={r.value}: "
                        f"median={r.timing.median_ms:.2f}ms "
                        f"p95={p95_str}ms "
                        f"({r.throughput:.0f} ops/s)"
                    )
            except Exception as e:
                import traceback
                print(f"  !! FAILED: {e}")
                traceback.print_exc()

        return all_results

    def save_results(self, results: list[BenchmarkResult], tag: str | None = None) -> Path:
        """Save results to JSON file, return the path."""
        output_dir = Path(self.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        suffix = f"_{tag}" if tag else ""
        filename = f"bench{suffix}_{ts}.json"
        path = output_dir / filename

        payload = {
            "system": self.system_info.to_dict(),
            "config": {
                "warmup": self.warmup,
                "iterations": self.iterations,
            },
            "results": [r.to_dict() for r in results],
            "summary": self._build_summary(results),
        }

        path.write_text(json.dumps(payload, indent=2))

        latest = output_dir / "latest.json"
        latest.write_text(json.dumps(payload, indent=2))

        print(f"\nResults saved to {path}")
        return path

    def _build_summary(self, results: list[BenchmarkResult]) -> dict:
        by_suite: dict[str, list[dict]] = {}
        for r in results:
            if r.suite not in by_suite:
                by_suite[r.suite] = []
            by_suite[r.suite].append({
                "scenario": r.scenario,
                "parameter": r.parameter,
                "value": r.value,
                "median_ms": r.timing.median_ms,
                "throughput": r.throughput,
            })
        return {
            "total_scenarios": len({r.scenario for r in results}),
            "total_measurements": len(results),
            "suites": by_suite,
        }
