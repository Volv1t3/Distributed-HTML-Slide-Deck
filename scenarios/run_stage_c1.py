"""Run every Stage C.1 scenario against its own clean local cluster.

Each scenario starts from a clean three-replica cluster with the Stage C.1
timing profile (LOCK_INACTIVITY_TIMEOUT=0.80s, LOCK_RENEWAL_INTERVAL=0.20s),
archives independently, and validates all final replicas after writes cease.

Five scenarios (issues #22, #23, #24):
  stage_c_normal       — normal renewal and convergence
  stage_c_replica      — preserved-disk follower catch-up
  stage_c_lider        — leader failure during lock concurrency
  stage_c_particion    — partition recovery and stale-token rejection
  stage_c_mayoria_loss — majority loss safety and quorum restore

Usage:
  python scenarios/run_stage_c1.py
  python scenarios/run_stage_c1.py --scenario stage_c_normal
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent

# Stage C.1 timing profile environment variables passed to every cluster node
_STAGE_C1_ENV: dict[str, str] = {
    "LOCK_INACTIVITY_TIMEOUT": "0.80",
    "LOCK_RENEWAL_INTERVAL": "0.20",
}

SCENARIOS = (
    "stage_c_normal",
    "stage_c_replica",
    "stage_c_lider",
    "stage_c_particion",
    "stage_c_mayoria_loss",
)


def run_scenario(name: str) -> dict:
    """Start a clean cluster with Stage C.1 timing, run one scenario, then archive.

    Returns a timing dict with ``scenario``, ``duration_s``, and ``ok`` keys.
    """
    print(f"\n=== {name}: clean three-replica cluster (Stage C.1 timing) ===", flush=True)
    env = {**os.environ, **_STAGE_C1_ENV}
    subprocess.run(
        [sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
        cwd=ROOT,
        check=True,
        env=env,
    )
    t0 = time.monotonic()
    ok = False
    try:
        subprocess.run(
            [sys.executable, str(ROOT / "scenarios" / f"{name}.py")],
            cwd=ROOT,
            check=True,
            env=env,
        )
        ok = True
    finally:
        subprocess.run(
            [sys.executable, str(ROOT / "cluster.py"), "down"],
            cwd=ROOT,
            check=True,
        )
    return {"scenario": name, "duration_s": round(time.monotonic() - t0, 3), "ok": ok}


def collect_metrics(results: list[dict]) -> dict:
    """Aggregate run-level metrics from scenario timing results."""
    return {
        "scenarios_run": len(results),
        "scenarios_passed": sum(1 for r in results if r.get("ok")),
        "total_duration_s": round(sum(r.get("duration_s", 0) for r in results), 3),
        "per_scenario": results,
        "note": (
            "Per-scenario latency measurements (renewal p50/p95, local-read p50/p95, "
            "expiration-to-reacquisition, election unavailability, log growth) are "
            "recorded in each scenario's archive metrics.json file."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Stage C.1 scenarios against clean local clusters."
    )
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        action="append",
        help="Run only this scenario (repeatable); default: all.",
    )
    parser.add_argument(
        "--metrics-out",
        metavar="FILE",
        help="Write aggregate metrics JSON to FILE (default: historiales/stage_c1_metrics.json).",
    )
    arguments = parser.parse_args()
    targets = arguments.scenario or list(SCENARIOS)

    results: list[dict] = []
    for scenario in targets:
        result = run_scenario(scenario)
        results.append(result)

    metrics = collect_metrics(results)
    metrics_path = Path(
        arguments.metrics_out
        or ROOT / "historiales" / "stage_c1_metrics.json"
    )
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")
    print(f"\nMetrics written to {metrics_path}", flush=True)

    failed = [r["scenario"] for r in results if not r.get("ok")]
    if failed:
        print(f"\nFAILED scenarios: {failed}", file=sys.stderr)
        sys.exit(1)
    print(f"\nAll {len(results)} scenario(s) passed.", flush=True)


if __name__ == "__main__":
    main()
