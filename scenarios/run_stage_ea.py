"""Run each Stage E.A scenario on a fresh local three-replica cluster.

Usage: python3 scenarios/run_stage_ea.py --scenario stage_e_normal
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = (
    "stage_e_normal", "stage_e_rotation", "stage_e_failover",
    "stage_e_lost_login", "stage_e_partition",
)


def run_scenario(name: str) -> None:
    if name not in SCENARIOS:
        raise ValueError(name)
    if (ROOT / "run").exists():
        raise RuntimeError("run/ already exists; preserve or archive it before starting a clean scenario")
    started = False
    try:
        started = True
        subprocess.run([sys.executable, str(ROOT / "cluster.py"), "up", "--local"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, str(ROOT / "scenarios" / f"{name}.py")], cwd=ROOT, check=True)
    finally:
        if started:
            subprocess.run([sys.executable, str(ROOT / "cluster.py"), "down"], cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    for name in args.scenario or SCENARIOS:
        run_scenario(name)


if __name__ == "__main__":
    main()
