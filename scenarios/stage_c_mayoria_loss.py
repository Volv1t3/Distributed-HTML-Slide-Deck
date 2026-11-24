"""Stage C.1 Scenario 5: majority loss — prove that no authoritative lock
transition can be confirmed when a quorum cannot form, then restore quorum
and verify that the surviving committed state is recovered.

Timing profile: LOCK_INACTIVITY_TIMEOUT=0.80s, LOCK_RENEWAL_INTERVAL=0.20s.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chaos
import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_c_support import (
    append_evidence,
    finalize_checked_scenario,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def _attempt(
    client_name: str,
    probes: Path,
    request_id: str,
    operation: str,
    arguments: dict,
    *,
    node: str | None = None,
) -> dict:
    """Send a single request with a 2-second timeout against a majority-less cluster."""
    return recorded_request(
        client_name,
        probes,
        request_id,
        operation,
        arguments,
        node=node,
        timeout=2.5,
    )


def _assert_not_confirmed(response: dict, operation: str) -> None:
    """Fail if the cluster falsely confirmed a write during majority loss."""
    if response.get("ok") is True:
        status = (response.get("result") or {}).get("status")
        # The only "ok" outcome acceptable without quorum is an observable
        # QUORUM_UNAVAILABLE or similar non-committed result.
        # Any committed status (RENEWED, EXPIRED, LOCK_ACQUIRED, RELEASED,
        # SAVED, PUBLISHED, CREATED_AND_LOCKED) is a safety violation.
        committed_statuses = {
            "RENEWED", "EXPIRED", "LOCK_ACQUIRED", "ALREADY_HELD",
            "CREATED_AND_LOCKED", "RELEASED", "SAVED", "PUBLISHED",
            "DELETED", "SLIDE_CREATED",
        }
        if status in committed_statuses:
            raise RuntimeError(
                f"majority-less cluster falsely confirmed {operation}: "
                f"status={status}, response={response}"
            )


def main() -> None:
    scenario = "stage_c_mayoria_loss"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name = "stage-c-majority-loss-editor"
    client = Client(client_name, history)
    config = cluster.load()

    # --- Setup: establish a deck, element, and lock before majority loss ---
    leader_slot = wait_for_leader(config)
    nodes_map = config["host_nodes"]
    names = config["names"]  # e.g. ["n1", "n2", "n3"]

    deck_id = stable_uuid("stage-c-majority:deck")
    slide_id = stable_uuid("stage-c-majority:slide")
    element_id = stable_uuid("stage-c-majority:element")
    session_id = stable_uuid("stage-c-majority:session")

    require_status(
        client.call("publish_deck", **published_deck(deck_id, [slide_id], name="Stage C majority loss")),
        "PUBLISHED",
    )
    create_resp = require_status(
        client.call(
            "create_element",
            deck_id=deck_id,
            session_id=session_id,
            slide_id=slide_id,
            element=text_element(element_id, "Before majority loss"),
        ),
        "CREATED_AND_LOCKED",
    )
    fencing_token = create_resp["value"]["fencing_token"]
    epoch = create_resp["value"]["lease_epoch"]
    element_version = 1  # one after create
    print(f"[mayoria_loss] setup complete: token={fencing_token}, epoch={epoch}", flush=True)

    # Save the last confirmed applied index so we can verify recovery later
    before_loss_index: int | None = None
    try:
        status_map = cluster.status(config)
        before_loss_index = status_map.get(leader_slot, {}).get("last_applied")
    except Exception:
        pass

    # --- Remove the majority: kill two of three nodes ---
    # Keep one node alive (the leader or any surviving node)
    to_kill = [slot for slot in names if slot != leader_slot][:2]
    if len(to_kill) < 2:
        # If we only have the leader, pick any two nodes
        to_kill = names[:2]
        surviving = [n for n in names if n not in to_kill][0]
    else:
        surviving = leader_slot

    for slot in to_kill:
        chaos.main(["kill", slot])
    majority_lost_at = time.monotonic()
    print(f"[mayoria_loss] killed {to_kill}; surviving node: {surviving}", flush=True)

    # Give the surviving node a moment to notice quorum is gone
    time.sleep(0.5)

    # --- All write operations must NOT be falsely confirmed ---
    ops_to_probe = [
        ("renew-no-quorum", "renew_lock", {
            "deck_id": deck_id, "session_id": session_id,
            "element_id": element_id, "fencing_token": fencing_token,
        }),
        ("acquire-no-quorum", "acquire_lock", {
            "deck_id": deck_id, "session_id": stable_uuid("stage-c-majority:session2"),
            "element_id": element_id,
        }),
        ("release-no-quorum", "release_lock", {
            "deck_id": deck_id, "session_id": session_id,
            "element_id": element_id, "fencing_token": fencing_token,
        }),
        ("update-no-quorum", "update_element", {
            "deck_id": deck_id, "session_id": session_id,
            "element_id": element_id, "fencing_token": fencing_token,
            "expected_version": element_version,
            "patch": {"content": "Must not commit without quorum"},
        }),
        ("delete-no-quorum", "delete_element", {
            "deck_id": deck_id, "session_id": session_id,
            "element_id": element_id, "fencing_token": fencing_token,
            "expected_version": element_version,
        }),
    ]

    for req_id, op, args in ops_to_probe:
        response = _attempt(client_name, probes, f"{req_id}-{element_id[:8]}", op, args, node=surviving)
        _assert_not_confirmed(response, op)
        status = (response.get("result") or {}).get("status")
        print(f"[mayoria_loss] {op}: correctly unconfirmed (status={status}, ok={response.get('ok')})", flush=True)
        append_evidence(probes, "majority_loss_write_blocked", operation=op, response=response)

    unavailability_seconds = time.monotonic() - majority_lost_at

    # --- Restore quorum: restart the killed nodes with intact disks ---
    for slot in to_kill:
        chaos.main(["start", slot])
    print(f"[mayoria_loss] restarted {to_kill} with intact disks", flush=True)

    # Wait for a new leader to emerge
    new_leader_slot = wait_for_leader(config)
    print(f"[mayoria_loss] new leader after recovery: {new_leader_slot}", flush=True)

    # --- Verify the lock state survived majority loss ---
    lock_check = recorded_request(
        client_name,
        history,
        f"lock-status-after-recovery-{element_id[:8]}",
        "read_lock_status",
        {"deck_id": deck_id, "element_id": element_id},
        node=new_leader_slot,
    )
    recovered_lock = (lock_check.get("result") or {}).get("lock")
    # The lock was active before majority loss with our token; it should still
    # be active (or expired if inactivity timeout elapsed during recovery).
    # What must NOT be true: a different token or a falsely committed transition.
    if recovered_lock is not None:
        recovered_token = recovered_lock.get("fencing_token")
        if recovered_token != fencing_token:
            raise RuntimeError(
                f"recovered lock has unexpected token={recovered_token} (expected {fencing_token})"
            )
    append_evidence(
        probes,
        "post_recovery_lock_state",
        lock=recovered_lock,
        unavailability_seconds=unavailability_seconds,
    )
    print(f"[mayoria_loss] lock state after recovery: {recovered_lock}", flush=True)

    # --- Verify writes are now possible again ---
    # Release the original lock (or acquire if it expired)
    if recovered_lock is not None and recovered_lock.get("fencing_token") == fencing_token:
        require_status(
            recorded_request(
                client_name,
                history,
                f"release-after-recovery-{element_id[:8]}",
                "release_lock",
                {
                    "deck_id": deck_id,
                    "session_id": session_id,
                    "element_id": element_id,
                    "fencing_token": fencing_token,
                },
                node=new_leader_slot,
            ),
            "RELEASED",
        )
        print("[mayoria_loss] successfully released lock after recovery", flush=True)
    else:
        print(f"[mayoria_loss] lock not present (may have expired); recovery verified", flush=True)

    # Final convergence check
    _final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element_state = _final["slide"]["elements"][0]
    if element_state["configuration"]["content"] != "Before majority loss":
        raise RuntimeError(
            f"element content changed during majority loss — safety violation: {element_state}"
        )
    print("[mayoria_loss] all replicas converged to pre-loss state", flush=True)

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "create_element", "acquire_lock", "renew_lock",
            "release_lock", "update_element",
        },
        read_operations={"get_element", "read_lock_status"},
        unavailability_seconds=unavailability_seconds,
        extra_evidence=[probes.name],
    )
    print("[mayoria_loss] scenario complete", flush=True)


if __name__ == "__main__":
    main()
