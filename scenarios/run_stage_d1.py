"""Run Stage D.1 scenarios against clean local clusters.

Each scenario starts from a clean three-replica cluster, archives
independently, and validates all final replicas after writes cease.

Stage D.1 scenarios:
  stage_d_normal    — normal settings + ordering (Scenario 1)
  stage_d_concurrency — concurrent settings conflicts (Scenario 2)
  stage_d_replica   — preserved-disk follower catch-up (Scenario 3)
  stage_d_lider     — leader failure + exact-ID settings retry (Scenario 4)
  stage_d_particion — partition + healing convergence (Scenario 5)

Usage:
  python scenarios/run_stage_d1.py
  python scenarios/run_stage_d1.py --scenario stage_d_normal
  python scenarios/run_stage_d1.py --scenario stage_d_replica
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

_SCENARIOS_DIR = Path(__file__).resolve().parent
if str(_SCENARIOS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCENARIOS_DIR))

ROOT = Path(__file__).resolve().parent.parent

from cluster_support import wait_for_activation  # noqa: E402

SCENARIOS = (
    "stage_d_normal",
    "stage_d_concurrency",
    "stage_d_replica",
    "stage_d_lider",
    "stage_d_particion",
)


def _latest_archive_dir() -> Path | None:
    """Return the most recently created run archive directory under historiales/."""
    historiales = ROOT / "historiales"
    if not historiales.is_dir():
        return None
    candidates = sorted(historiales.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def run_scenario(name: str) -> dict:
    """Start a clean cluster, run one scenario, then archive.

    Returns a timing dict with ``scenario``, ``duration_s``, and ``ok`` keys.
    """
    print(f"\n=== {name}: clean three-replica cluster ===", flush=True)
    # Stage F regression fixture: identical binaries on every voter, so the
    # leader commits the v3 activation fence before the scenario publishes.
    env = {**os.environ, "STAGE_F_ALL_VOTERS_READY": "1"}
    subprocess.run(
        [sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
        cwd=ROOT,
        check=True,
        env=env,
    )
    t0 = time.monotonic()
    ok = False
    try:
        wait_for_activation(ROOT / "run")
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
            "Per-scenario latency measurements (settings-write p50/p95, local-read p50/p95, "
            "election unavailability) are recorded in each scenario's archive metrics.json file."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Stage D.1 scenarios against clean local clusters."
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
        help="Write aggregate metrics JSON to FILE (default: historiales/stage_d1_metrics.json).",
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
        or ROOT / "historiales" / "stage_d1_metrics.json"
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
