"""CLI entry point: uv run python -m benchmarks"""

from __future__ import annotations

import argparse
import asyncio


def cmd_run(args: argparse.Namespace) -> None:
    """Run benchmark scenarios."""
    from benchmarks.core import BenchmarkRunner
    from benchmarks.scenarios import get_scenarios

    runner = BenchmarkRunner(
        warmup=args.warmup,
        iterations=args.iterations,
        output_dir=args.output,
    )

    print("\n  dagler benchmark suite")
    print(f"  Warmup: {runner.warmup}, Iterations: {runner.iterations}")
    print(f"  {runner.system_info.summary_line()}")

    scenarios = get_scenarios(args.suite or None)
    print(f"  Running {len(scenarios)} scenarios...\n")

    results = asyncio.run(runner.run(scenarios))
    path = runner.save_results(results, tag=args.tag)
    print(f"\n  Done! {len(results)} measurements saved to {path}")


def cmd_list(args: argparse.Namespace) -> None:
    """List available scenarios."""
    from benchmarks.scenarios import ALL_SCENARIOS

    print("\n  Available benchmark scenarios:\n")
    by_suite: dict[str, list] = {}
    for s in ALL_SCENARIOS:
        by_suite.setdefault(s.suite, []).append(s)

    for suite_name, scenarios in by_suite.items():
        print(f"  [{suite_name}]")
        for s in scenarios:
            print(f"    - {s.name}: {s.description}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="benchmarks",
        description="dagler benchmark suite — DAG orchestration performance",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run benchmark scenarios")
    run_p.add_argument("--suite", action="append", help="Run specific suite(s)")
    run_p.add_argument("--warmup", type=int, default=3)
    run_p.add_argument("--iterations", type=int, default=20)
    run_p.add_argument("--output", default="benchmarks/results")
    run_p.add_argument("--tag", help="Tag for output filename")
    run_p.set_defaults(func=cmd_run)

    list_p = sub.add_parser("list", help="List available scenarios")
    list_p.set_defaults(func=cmd_list)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
