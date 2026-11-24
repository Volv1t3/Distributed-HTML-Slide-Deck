"""Run Stage D.1 scenarios against clean local clusters.

Each scenario starts from a clean three-replica cluster, archives
independently, and validates all final replicas after writes cease.

Stage D.1 scenarios (issue #34 — follower recovery and leader retry):
  stage_d_replica   — preserved-disk follower catch-up (Scenario 3)
  stage_d_lider     — leader failure + exact-ID settings retry (Scenario 4)

Usage:
  python scenarios/run_stage_d1.py
  python scenarios/run_stage_d1.py --scenario stage_d_replica
  python scenarios/run_stage_d1.py --scenario stage_d_lider
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

SCENARIOS = (
    "stage_d_replica",
    "stage_d_lider",
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
    env = dict(os.environ)
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
