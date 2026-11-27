"""Run each Stage E.A scenario on a fresh local three-replica cluster.

Usage: python3 scenarios/run_stage_ea.py --scenario stage_e_normal
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

import cluster_support  # noqa: E402
from cluster_support import wait_for_activation  # noqa: E402

SCENARIOS = (
    "stage_e_normal", "stage_e_rotation", "stage_e_failover",
    "stage_e_lost_login", "stage_e_partition",
)


def archive_evidence(name: str) -> Path:
    """Preserve the actual safe Raft disks and scenario output before down removes run/."""
    return cluster_support.archive_evidence(name, root=ROOT)


def run_scenario(name: str) -> None:
    if name not in SCENARIOS:
        raise ValueError(name)
    if (ROOT / "run").exists():
        raise RuntimeError("run/ already exists; preserve or archive it before starting a clean scenario")
    # Stage F regression fixture: every voter runs the same Stage F binary, so
    # the leader may commit the v3 activation fence before the first publish.
    env = {**os.environ, "STAGE_F_ALL_VOTERS_READY": "1"}
    started = False
    try:
        started = True
        subprocess.run([sys.executable, str(ROOT / "cluster.py"), "up", "--local"], cwd=ROOT, check=True, env=env)
        wait_for_activation(ROOT / "run")
        subprocess.run([sys.executable, str(ROOT / "scenarios" / f"{name}.py")], cwd=ROOT, check=True, env=env)
        print(f"Stage E.A evidence archived at {archive_evidence(name).relative_to(ROOT)}", flush=True)
    finally:
        if started:
            subprocess.run([sys.executable, str(ROOT / "cluster.py"), "down"], cwd=ROOT, check=True, env=env)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    for name in args.scenario or SCENARIOS:
        run_scenario(name)


if __name__ == "__main__":
    main()
