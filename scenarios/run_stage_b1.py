"""Run every Stage B.1 scenario against its own clean local cluster."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = (
    "stage_b_normal",
    "stage_b_concurrency",
    "stage_b_replica",
    "stage_b_lider",
    "stage_b_particion",
    "stage_b_client_journal",
)


def run_scenario(name: str) -> None:
    """Start clean, run one scenario, then archive through ``cluster.py down``."""

    print(f"\n=== {name}: clean three-replica cluster ===", flush=True)
    subprocess.run(
        [sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
        cwd=ROOT,
        check=True,
    )
    try:
        subprocess.run(
            [sys.executable, str(ROOT / "scenarios" / f"{name}.py")],
            cwd=ROOT,
            check=True,
        )
    finally:
        subprocess.run(
            [sys.executable, str(ROOT / "cluster.py"), "down"],
            cwd=ROOT,
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=SCENARIOS, action="append")
    arguments = parser.parse_args()
    for scenario in arguments.scenario or SCENARIOS:
        run_scenario(scenario)


if __name__ == "__main__":
    main()
