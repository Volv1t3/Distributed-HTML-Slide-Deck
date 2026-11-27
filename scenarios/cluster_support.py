"""Stage-neutral helpers for local three-replica scenario runs.

Every stage's scenarios and runners read replica durable state and archive
run evidence the same way. Keeping those helpers here lets an earlier stage's
runner use them without depending on a later stage's support module.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import shutil
import time
from typing import Iterable

SLOTS = ("n1", "n2", "n3")


def raft_state(run_dir: Path, slot: str) -> dict:
    """Return one replica's durable ``raft_state`` from ``<run>/<slot>/disk.json``."""
    return json.loads((Path(run_dir) / slot / "disk.json").read_text())["raft_state"]


def wait_for_activation(run_dir: Path, *, slots: Iterable[str] = SLOTS, timeout: float = 20.0) -> int:
    """Wait until each replica durably applied the committed v3 activation fence."""
    deadline = time.monotonic() + timeout
    fences: dict[str, int | None] = {}
    while time.monotonic() < deadline:
        for slot in slots:
            try:
                policy = raft_state(run_dir, slot)["fsm"].get("presentation_schema_policy") or {}
            except (OSError, ValueError, KeyError):
                policy = {}
            fences[slot] = policy.get("v3_enforced_from_index")
        if all(fences.values()) and len(set(fences.values())) == 1:
            return next(iter(fences.values()))
        time.sleep(0.2)
    raise RuntimeError(f"v3 activation fence not applied on every replica: {fences}")


def archive_evidence(name: str, *, root: Path) -> Path:
    """Copy ``<root>/run`` evidence to ``<root>/historiales`` before ``down`` removes it.

    Keeps the scenario JSONL files plus each replica's ``disk.json``,
    ``roles.log`` and ``events.log``.
    """
    run = Path(root) / "run"
    destination_root = Path(root) / "historiales"
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
    for node in SLOTS:
        target = destination / node
        target.mkdir()
        for filename in ("disk.json", "roles.log", "events.log"):
            source = run / node / filename
            if source.exists():
                shutil.copy2(source, target / filename)
    return destination
