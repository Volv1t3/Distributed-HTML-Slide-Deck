"""Shared helpers for Stage C.1 distributed scenarios.

Extends stage_b_support with Stage C.1 timing constants and lock-state
wait helpers. Import via ``from stage_c_support import *`` to get all
Stage B helpers as well.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Re-export everything from Stage B support
from stage_b_support import (  # noqa: F401, E402
    SCENARIO_EMAIL,
    SCENARIO_PASSWORD,
    _post,
    append_evidence,
    finalize_checked_scenario,
    last_history_entry,
    login_scenario_user,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_event,
    wait_for_presentation_catch_up,
    write_checker_evidence,
    write_checker_projection,
    write_latency_summary,
    write_manifest,
)

# Stage C.1 timing profile (spec §Timing and configuration)
RENEWAL_INTERVAL: float = 0.20   # seconds
INACTIVITY_TIMEOUT: float = 0.80  # seconds
STAGE_C1_ENV: dict[str, str] = {
    "LOCK_INACTIVITY_TIMEOUT": str(INACTIVITY_TIMEOUT),
    "LOCK_RENEWAL_INTERVAL": str(RENEWAL_INTERVAL),
}


def wait_for_lease_epoch(
    deck_id: str,
    element_id: str,
    min_epoch: int,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Poll any node until the committed lease_epoch >= min_epoch."""
    import cluster

    nodes = cluster.load()["host_nodes"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node_url in nodes.values():
            try:
                response = _post(
                    node_url,
                    {
                        "id": f"wait-epoch-{element_id[:8]}-{int(time.time()*1000)}",
                        "op": "read_lock_status",
                        "args": {"deck_id": deck_id, "element_id": element_id},
                        "client": "_wait_helper",
                    },
                    2.0,
                )
                if response.get("ok"):
                    lock = (response.get("result") or {}).get("lock")
                    if lock is not None and lock.get("lease_epoch", -1) >= min_epoch:
                        return response["result"]
            except Exception:
                pass
        time.sleep(0.1)
    raise RuntimeError(
        f"lease_epoch for {element_id} did not reach {min_epoch} within {timeout}s"
    )


def wait_for_lock_expiration(
    deck_id: str,
    element_id: str,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Poll any node until no active lock is reported for the element."""
    import cluster

    nodes = cluster.load()["host_nodes"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node_url in nodes.values():
            try:
                response = _post(
                    node_url,
                    {
                        "id": f"wait-exp-{element_id[:8]}-{int(time.time()*1000)}",
                        "op": "read_lock_status",
                        "args": {"deck_id": deck_id, "element_id": element_id},
                        "client": "_wait_helper",
                    },
                    2.0,
                )
                if response.get("ok"):
                    result = response.get("result") or {}
                    if result.get("lock") is None:
                        return result
            except Exception:
                pass
        time.sleep(0.2)
    raise RuntimeError(
        f"lock on {element_id} did not expire within {timeout}s"
    )


def assert_all_replicas_lock_state(
    nodes_map: dict[str, str],
    deck_id: str,
    element_id: str,
    *,
    expected_epoch: int | None = None,
    expected_fencing: int | None = None,
    expect_no_lock: bool = False,
    run_dir: Path | None = None,
) -> None:
    """Assert lock convergence on every replica's durable snapshot.

    Stage F refuses follower-local ``read_lock_status``; each replica's applied
    ``disk.json`` is the per-replica evidence instead.
    """
    results: dict[str, Any] = {}
    for slot in nodes_map:
        try:
            disk = json.loads(((run_dir or ROOT / "run") / slot / "disk.json").read_text())
            deck = disk["raft_state"]["fsm"]["decks"][deck_id]
            results[slot] = {"deck_id": deck_id, "element_id": element_id,
                             "lock": deck.get("locks", {}).get(element_id)}
        except Exception as exc:
            results[slot] = {"error": str(exc)}

    for slot, result in results.items():
        lock = result.get("lock")
        if expect_no_lock:
            if lock is not None:
                raise AssertionError(
                    f"replica {slot} still holds a lock: {lock}\nall results: {results}"
                )
        else:
            if lock is None:
                raise AssertionError(
                    f"replica {slot} has no lock (expected one)\nall results: {results}"
                )
            if expected_epoch is not None and lock.get("lease_epoch") != expected_epoch:
                raise AssertionError(
                    f"replica {slot} epoch={lock.get('lease_epoch')} != {expected_epoch}\nall: {results}"
                )
            if expected_fencing is not None and lock.get("fencing_token") != expected_fencing:
                raise AssertionError(
                    f"replica {slot} fencing={lock.get('fencing_token')} != {expected_fencing}\nall: {results}"
                )


class StageC1Metrics:
    """Collects per-scenario coordination measurements for the Stage C.1 report.

    Each scenario appends its measurements; the orchestrator serialises the
    aggregate via ``report()``.  All latencies are in seconds unless noted.
    """

    def __init__(self) -> None:
        self.renewal_latencies: list[float] = []
        self.local_read_latencies: list[float] = []
        self.expiration_reacquisition_time: float | None = None
        self.election_unavailability_ms: float | None = None
        self.majority_loss_unavailability_ms: float | None = None
        self.log_entry_count: int | None = None

    def _percentile(self, data: list[float], p: float) -> float | None:
        if not data:
            return None
        s = sorted(data)
        idx = int(len(s) * p)
        return s[min(idx, len(s) - 1)]

    def report(self) -> dict:
        return {
            "renewal_p50_s": self._percentile(self.renewal_latencies, 0.50),
            "renewal_p95_s": self._percentile(self.renewal_latencies, 0.95),
            "local_read_p50_s": self._percentile(self.local_read_latencies, 0.50),
            "local_read_p95_s": self._percentile(self.local_read_latencies, 0.95),
            "expiration_reacquisition_s": self.expiration_reacquisition_time,
            "election_unavailability_ms": self.election_unavailability_ms,
            "majority_loss_unavailability_ms": self.majority_loss_unavailability_ms,
            "log_entry_count": self.log_entry_count,
            "renewal_sample_count": len(self.renewal_latencies),
            "local_read_sample_count": len(self.local_read_latencies),
        }


def extract_internal_expiration_events(log_path: str | Path) -> list[dict]:
    """Extract expire_lock and expiration-coordinator events from replica log(s).

    Accepts either a single ``events.log`` file or a directory; in the directory
    case every ``*.log`` file found recursively is scanned.
    """
    log_path = Path(log_path)
    _interesting = {
        "expire_lock_proposed",
        "expire_lock_committed_expired",
        "expire_lock_committed_noop",
        "lock_expiration_coordinator_leader_init",
        "lock_expiration_coordinator_stepdown",
        "lock_expiration_renewal_observed",
    }
    files = sorted(log_path.glob("**/*.log")) if log_path.is_dir() else [log_path]
    events: list[dict] = []
    for log_file in files:
        if not log_file.exists():
            continue
        for line in log_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if record.get("event") in _interesting:
                    events.append(record)
            except (json.JSONDecodeError, ValueError):
                pass
    return events


def build_expiration_checker_records(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate committed internal expirations into checker history records.

    The projection uses only terminal ``expire_lock_committed_*`` events. Each
    record keeps the server request identity and result shape so the unchanged
    reference model can replay the transition, while ``origin`` labels it as
    server-originated rather than a fabricated client command.
    """
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in sorted(events, key=lambda item: item.get("timestamp", 0.0)):
        name = event.get("event")
        if name not in {"expire_lock_committed_expired", "expire_lock_committed_noop"}:
            continue
        request_id = event.get("request_id")
        if not isinstance(request_id, str) or request_id in seen:
            continue
        seen.add(request_id)

        status = "EXPIRED" if name == "expire_lock_committed_expired" else "NO_OP_STALE_EXPIRY"
        deck_id = event.get("deck_id")
        element_id = event.get("element_id")
        fencing_token = event.get("fencing_token")
        lease_epoch = event.get("lease_epoch")
        element_version = event.get("element_version")
        timestamp = event.get("timestamp")

        result: dict[str, Any] = {
            "status": status,
            "value": (
                {
                    "deck_id": deck_id,
                    "element_id": element_id,
                    "fencing_token": fencing_token,
                }
                if status == "EXPIRED"
                else None
            ),
            "version": element_version if status == "EXPIRED" else None,
            "details": {},
        }
        records.append({
            "id": request_id,
            "client": "_lock_expiration",
            "op": "expire_lock",
            "args": {
                "deck_id": deck_id,
                "element_id": element_id,
                "expected_token": fencing_token,
                "expected_lease_epoch": lease_epoch,
            },
            "invoke": timestamp,
            "complete": timestamp,
            "ok": True,
            "result": result,
            "error": None,
            "node": event.get("node_slot"),
            "origin": "lock_expiration_coordinator",
            "internal_transition": True,
        })
    return records


def renew_lock_request(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    deck_id: str,
    session_id: str,
    element_id: str,
    fencing_token: int,
    *,
    _auth: dict[str, Any] | None = None,
    node: str | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Record a renew_lock request with an exact ID (for retry evidence)."""
    return recorded_request(
        client_name,
        history_path,
        request_id,
        "renew_lock",
        {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": fencing_token,
        },
        _auth=_auth,
        node=node,
        timeout=timeout,
    )
