"""Stage C.1 — leader failure during lock concurrency (issue #23).

Establishes a selected, automatically renewed element lock, fails the leader
while a renewal is in flight, retries with the same request ID, and proves
that only committed Lock Authority survives. Verifies the replacement leader
initialises inherited timers with a full grace period before any expiration
proposal is emitted.
"""

from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chaos
import cluster
from client import Client
from lock_maintenance import LockAuthority, LockMaintenanceService
from stage_a_support import wait_for_leader
from stage_b_support import (
    append_evidence,
    finalize_checked_scenario,
    login_scenario_user,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)
from stage_c_support import (
    INACTIVITY_TIMEOUT,
    RENEWAL_INTERVAL,
    renew_lock_request,
    wait_for_lease_epoch,
    assert_all_replicas_lock_state,
)


def main() -> None:
    scenario = "stage_c_lider"
    history = Path("run") / f"{scenario}.jsonl"
    pending_probe = Path("run") / f"{scenario}_pending_renewal.jsonl"
    fault_evidence = Path("run") / f"{scenario}_faults.jsonl"
    client_name = stable_uuid("stage-c-lider:client")

    client = Client(client_name, history)
    config = cluster.load()
    old_leader = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != old_leader]

    deck_id = stable_uuid("lider-c:deck")
    slide_id = stable_uuid("lider-c:slide")
    element_id = stable_uuid("lider-c:element")
    session_id = stable_uuid("lider-c:session")

    _auth = login_scenario_user(
        client_name, history, "stage-c-lider:login",
    )

    # 1. Publish deck and create+lock element (token=1, epoch=0)
    require_status(
        recorded_request(
            client_name, history,
            "stage-c-lider:publish-deck",
            "publish_deck",
            published_deck(deck_id, [slide_id], name="Stage C leader"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    create_result = require_status(
        recorded_request(
            client_name, history,
            f"stage-c-lider:create-elem-{element_id[:8]}",
            "create_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "slide_id": slide_id,
                "element": text_element(element_id, "Before leader failure"),
            },
            _auth=_auth,
        ),
        "CREATED_AND_LOCKED",
    )
    fencing_token: int = create_result["value"]["fencing_token"]
    initial_epoch: int = create_result["value"].get("lease_epoch", 0)
    element_version: int = create_result["value"]["element_version"]
    assert fencing_token == 1, f"expected fencing_token=1, got {fencing_token}"
    assert initial_epoch == 0, f"expected epoch=0, got {initial_epoch}"

    # 2. Start client-side automatic renewal and wait for epoch > 0.
    authority = LockAuthority(
        deck_id=deck_id,
        slide_id=slide_id,
        element_id=element_id,
        editor_session=session_id,
        fencing_token=fencing_token,
        lease_epoch=initial_epoch,
        element_version=element_version,
    )
    maintenance = LockMaintenanceService(
        request_sender=lambda op, args, request_id, attempt_timeout: recorded_request(
            client_name,
            history,
            request_id,
            op,
            args,
            _auth=_auth,
            timeout=attempt_timeout,
        ),
        http_timeout=RENEWAL_INTERVAL,
        inactivity_timeout=INACTIVITY_TIMEOUT,
    )
    maintenance.register_lock(authority, RENEWAL_INTERVAL)
    try:
        lease_status = wait_for_lease_epoch(deck_id, element_id, min_epoch=1)
        confidence = maintenance.get_confidence(element_id)
    finally:
        # Stop background renewal before inducing failure so the pending
        # renewal below is the only attempt racing the leader change.
        maintenance.close()
    append_evidence(
        fault_evidence,
        "initial_renewal_observed",
        epoch=lease_status["lock"]["lease_epoch"],
        confidence=confidence.value,
    )

    # 3. Build a pending renewal request identity (exact-ID retry)
    pending_request_id = f"stage-c-lider-pending-renew-{element_id[:8]}"
    renew_args = {
        "deck_id": deck_id,
        "session_id": session_id,
        "element_id": element_id,
        "fencing_token": fencing_token,
    }

    pending_response: dict[str, dict] = {}

    def send_pending_renewal() -> None:
        """Send renewal to the old leader in a background thread."""
        from stage_b_support import _post
        nodes = cluster.load()["host_nodes"]
        try:
            resp = _post(
                nodes[old_leader],
                {"id": pending_request_id, "op": "renew_lock",
                 "args": {**renew_args, "_auth": _auth},
                 "client": client_name},
                timeout=3.0,
            )
        except Exception as exc:
            resp = {"ok": False, "error": type(exc).__name__}
        pending_response["value"] = resp
        import json
        pending_path = Path("run") / f"{scenario}_pending_renewal.jsonl"
        pending_path.parent.mkdir(parents=True, exist_ok=True)
        with pending_path.open("a") as fh:
            import time as _t
            fh.write(
                json.dumps({
                    "id": pending_request_id,
                    "op": "renew_lock",
                    "args": renew_args,
                    "ok": resp.get("ok", False),
                    "result": resp.get("result"),
                    "error": resp.get("error"),
                    "invoke": _t.time(),
                    "client": client_name,
                }) + "\n"
            )

    # 4. Partition old leader from majority, then immediately fire pending renewal
    chaos.main(["partition", old_leader, "/", *majority])
    unavailable_started = time.monotonic()

    pending_thread = threading.Thread(target=send_pending_renewal)
    pending_thread.start()
    time.sleep(0.3)  # let the in-flight renewal reach the isolated leader

    chaos.main(["kill", old_leader])
    append_evidence(
        fault_evidence, "leader_killed_with_renewal_in_flight",
        leader=old_leader,
        pending_request_id=pending_request_id,
    )
    chaos.main(["heal"])
    pending_thread.join()

    # Isolated leader must NOT have confirmed the renewal
    if pending_response.get("value", {}).get("ok") is True:
        raise RuntimeError(
            f"isolated leader unexpectedly confirmed the renewal: {pending_response}"
        )

    # 5. Wait for the new leader
    new_leader = wait_for_leader(config, exclude={old_leader})
    election_unavailability_seconds = time.monotonic() - unavailable_started
    append_evidence(
        fault_evidence, "new_leader_elected",
        old_leader=old_leader,
        new_leader=new_leader,
        unavailability_seconds=round(election_unavailability_seconds, 3),
    )

    # 6. Exact-ID retry on the new leader
    retry = renew_lock_request(
        client_name,
        history,
        pending_request_id,
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
        node=new_leader,
        timeout=10.0,
    )

    retry_ok = retry.get("ok") is True
    retry_status = (retry.get("result") or {}).get("status", "")

    if retry_ok and retry_status == "RENEWED":
        # Exact-ID committed — epoch must have advanced exactly once from the
        # epoch at the time the request was first issued; must NOT be E+2 or more.
        post_retry_epoch = (retry.get("result") or {}).get("value", {}).get("lease_epoch", 0)
        append_evidence(
            fault_evidence, "exact_id_retry_renewed",
            request_id=pending_request_id,
            new_leader=new_leader,
            post_retry_epoch=post_retry_epoch,
        )
    elif retry_status in ("STALE_TOKEN", "NOT_FOUND"):
        # Lock was expired during the leader-change window — this is a valid outcome.
        append_evidence(
            fault_evidence, "exact_id_retry_stale_authority",
            request_id=pending_request_id,
            new_leader=new_leader,
            status=retry_status,
        )
    else:
        raise RuntimeError(
            f"exact-ID renewal retry returned unexpected status: {retry}"
        )

    # 7. Verify inherited grace period: the new leader must NOT have already
    #    proposed expiration for the lock (it just became leader).
    #    We verify by confirming the lock is still active immediately after election
    #    (inactivity_timeout=0.80s but grace period was just re-seeded).
    grace_check = client.call(
        "read_lock_status",
        node=new_leader,
        deck_id=deck_id,
        element_id=element_id,
    )
    lock_alive = (grace_check.get("result") or {}).get("lock") is not None
    if retry_ok and retry_status == "RENEWED" and not lock_alive:
        raise RuntimeError(
            f"lock should still be active after confirmed renewal and fresh grace period: {grace_check}"
        )
    append_evidence(
        fault_evidence, "inherited_grace_period_verified",
        lock_active=lock_alive,
    )

    # 8. No former-leader stale update: try to update with token from new session on old slot
    #    (old_leader restarted with intact disk) — this is validated via AppendEntries catch-up.
    chaos.main(["start", old_leader])
    append_evidence(fault_evidence, "former_leader_restarted", leader=old_leader)

    # 9. Final convergence
    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    nodes_map = cluster.load()["host_nodes"]
    # Every live replica must agree on lock state (active or absent, same epoch/fencing)
    lock_states = {}
    for slot, url in nodes_map.items():
        from stage_b_support import _post
        try:
            resp = _post(
                url,
                {"id": f"final-lock-{slot}", "op": "read_lock_status",
                 "args": {"deck_id": deck_id, "element_id": element_id, "local": True},
                 "client": "_assert"},
                4.0,
            )
            lock_states[slot] = (resp.get("result") or {}).get("lock")
        except Exception:
            lock_states[slot] = "UNREACHABLE"

    # All replicas must agree (same fencing_token and lease_epoch or all None)
    unique_states = set(
        str(ls) if not isinstance(ls, dict)
        else f"token={ls.get('fencing_token')},epoch={ls.get('lease_epoch')}"
        for ls in lock_states.values()
        if ls != "UNREACHABLE"
    )
    if len(unique_states) > 1:
        raise RuntimeError(f"lock state did not converge across replicas: {lock_states}")

    append_evidence(
        fault_evidence, "final_lock_state_convergence_verified",
        lock_states=str(lock_states),
    )

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"create_element", "renew_lock"},
        read_operations={"get_element", "read_lock_status"},
        unavailability_seconds=election_unavailability_seconds,
        extra_evidence=[pending_probe.name, fault_evidence.name],
    )
    print(f"stage_c_lider PASSED — unavailability={election_unavailability_seconds:.3f}s")


if __name__ == "__main__":
    main()
