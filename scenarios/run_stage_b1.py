"""Run every Stage B.1 scenario against its own clean local cluster."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

from cluster_support import wait_for_activation  # noqa: E402

# Stage F regression fixture: every voter runs the same binary, so the leader
# may commit the v3 activation fence before the scenario publishes a deck.
STAGE_F_ENV = {"STAGE_F_ALL_VOTERS_READY": "1"}
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
    env = {**os.environ, **STAGE_F_ENV}
    subprocess.run(
        [sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
        cwd=ROOT,
        check=True,
        env=env,
    )
    try:
        wait_for_activation(ROOT / "run")
        subprocess.run(
            [sys.executable, str(ROOT / "scenarios" / f"{name}.py")],
            cwd=ROOT,
            check=True,
            env=env,
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
