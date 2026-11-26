"""Run each Stage E.A scenario on a fresh local three-replica cluster.

Usage: python3 scenarios/run_stage_ea.py --scenario stage_e_normal
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = (
    "stage_e_normal", "stage_e_rotation", "stage_e_failover",
    "stage_e_lost_login", "stage_e_partition",
)


def archive_evidence(name: str) -> Path:
    """Preserve the actual safe Raft disks and scenario output before down removes run/."""
    run = ROOT / "run"
    destination_root = ROOT / "historiales"
    destination_root.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    suffix = 0
    while True:
        destination = destination_root / f"{stamp}-{name}{f'-{suffix}' if suffix else ''}"
        try:
            destination.mkdir()
            break
        except FileExistsError:
            suffix += 1
    for source in run.glob("*.jsonl"):
        shutil.copy2(source, destination / source.name)
    for node in ("n1", "n2", "n3"):
        target = destination / node
        target.mkdir()
        for filename in ("disk.json", "roles.log", "events.log"):
            source = run / node / filename
            if source.exists():
                shutil.copy2(source, target / filename)
    return destination


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
        print(f"Stage E.A evidence archived at {archive_evidence(name).relative_to(ROOT)}", flush=True)
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
