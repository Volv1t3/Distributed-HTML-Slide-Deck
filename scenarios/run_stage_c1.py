"""Run every Stage C.1 scenario against its own clean local cluster.

Each scenario starts from a clean three-replica cluster with the Stage C.1
timing profile (LOCK_INACTIVITY_TIMEOUT=0.80s, LOCK_RENEWAL_INTERVAL=0.20s),
archives independently, and validates all final replicas after writes cease.

Scenarios #22 (normal + replica) are implemented here. Stubs for #23 and #24
will be replaced when those issues are implemented.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent

# Stage C.1 timing profile environment variables passed to every cluster node
_STAGE_C1_ENV: dict[str, str] = {
    "LOCK_INACTIVITY_TIMEOUT": "0.80",
    "LOCK_RENEWAL_INTERVAL": "0.20",
}

SCENARIOS = (
    "stage_c_normal",
    "stage_c_replica",
    "stage_c_lider",           # issue #23
    "stage_c_particion",       # issue #24 (partition)
    "stage_c_mayoria_loss",    # issue #24 (majority loss)
)


def run_scenario(name: str) -> None:
    """Start a clean cluster with Stage C.1 timing, run one scenario, then archive."""
    print(f"\n=== {name}: clean three-replica cluster (Stage C.1 timing) ===", flush=True)
    env = {**os.environ, **_STAGE_C1_ENV}
    subprocess.run(
        [sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
        cwd=ROOT,
        check=True,
        env=env,
    )
    try:
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
    parser = argparse.ArgumentParser(
        description="Run Stage C.1 scenarios against clean local clusters."
    )
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        action="append",
        help="Run only this scenario (repeatable); default: all.",
    )
    arguments = parser.parse_args()
    targets = arguments.scenario or list(SCENARIOS)

    # Raise early for unimplemented stubs so the caller knows what is pending.
    _STUBS = {"stage_c_lider"}  # issue #23 still pending
    for target in targets:
        if target in _STUBS:
            raise NotImplementedError(
                f"{target} is not yet implemented — see issue #23."
            )

    for scenario in targets:
        run_scenario(scenario)


if __name__ == "__main__":
    main()
