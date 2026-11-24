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
    _post,
    append_evidence,
    finalize_checked_scenario,
    last_history_entry,
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
) -> None:
    """Read lock status from every replica and assert convergence."""
    results: dict[str, Any] = {}
    for slot, url in nodes_map.items():
        try:
            response = _post(
                url,
                {
                    "id": f"assert-lock-{slot}-{element_id[:8]}",
                    "op": "read_lock_status",
                    "args": {"deck_id": deck_id, "element_id": element_id, "local": True},
                    "client": "_assert_helper",
                },
                4.0,
            )
            results[slot] = response.get("result") or {}
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


def renew_lock_request(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    deck_id: str,
    session_id: str,
    element_id: str,
    fencing_token: int,
    *,
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
        node=node,
        timeout=timeout,
    )
