"""Run each Stage F scenario on a fresh local three-replica cluster.

Usage: python3 scenarios/run_stage_f.py --scenario stage_f_sharing

Every replica runs the same Stage F binary, so the runner marks all voters
as coordinated (``STAGE_F_ALL_VOTERS_READY=1``) and the leader commits the v3
activation fence. The legacy scenario instead starts from the completed
Stage E release (``LEGACY_BASELINE``) and performs the rolling upgrade itself,
so the cluster starts without the coordination flag.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

SCENARIOS = (
    "stage_f_sharing", "stage_f_partition", "stage_f_failover", "stage_f_legacy",
)
UPGRADE_SCENARIOS = {"stage_f_legacy"}
# Completed Stage E release on master (PR #52); the Stage F specification baseline.
LEGACY_BASELINE = "3ecced2"


def archive_evidence(name: str) -> Path:
    from run_stage_ea import archive_evidence as archive
    import run_stage_ea

    run_stage_ea.ROOT = ROOT
    return archive(name)


def prepare_legacy_binaries() -> Path:
    """Extract the Stage E release tree for the rolling-upgrade scenario."""
    destination = Path(tempfile.mkdtemp(prefix="stage-f-legacy-binaries-"))
    archive = subprocess.run(["git", "archive", LEGACY_BASELINE], cwd=ROOT, check=True,
                             capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", str(destination)], input=archive, check=True)
    return destination


def run_scenario(name: str) -> None:
    if name not in SCENARIOS:
        raise ValueError(name)
    if (ROOT / "run").exists():
        raise RuntimeError("run/ already exists; preserve or archive it before starting a clean scenario")
    env = {key: value for key, value in os.environ.items() if key != "STAGE_F_ALL_VOTERS_READY"}
    legacy = None
    if name in UPGRADE_SCENARIOS:
        legacy = prepare_legacy_binaries()
        env["STAGE_F_LEGACY_BINARIES"] = str(legacy)
    else:
        env["STAGE_F_ALL_VOTERS_READY"] = "1"
    started = False
    try:
        started = True
        subprocess.run([sys.executable, str(ROOT / "cluster.py"), "up", "--local"],
                       cwd=ROOT, check=True, env=env)
        subprocess.run([sys.executable, str(ROOT / "scenarios" / f"{name}.py")],
                       cwd=ROOT, check=True, env=env)
        print(f"Stage F evidence archived at {archive_evidence(name)}", flush=True)
    finally:
        if started:
            subprocess.run([sys.executable, str(ROOT / "cluster.py"), "down"],
                           cwd=ROOT, check=True, env=env)
        if legacy is not None:
            shutil.rmtree(legacy, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    for name in args.scenario or SCENARIOS:
        run_scenario(name)


if __name__ == "__main__":
    main()
