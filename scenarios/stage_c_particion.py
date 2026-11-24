"""Stage C.1 Scenario 4: partition the lock owner in a minority, let the majority
expire and reassign the lock, heal the partition, then verify that the former
owner's stale authority is rejected and that every replica converges.

Timing profile: LOCK_INACTIVITY_TIMEOUT=0.80s, LOCK_RENEWAL_INTERVAL=0.20s.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chaos
import cluster
from client import Client
from lock_maintenance import LockAuthority, LockMaintenanceService
from stage_a_support import wait_for_leader
from stage_c_support import (
    INACTIVITY_TIMEOUT,
    RENEWAL_INTERVAL,
    append_evidence,
    assert_all_replicas_lock_state,
    build_expiration_checker_records,
    extract_internal_expiration_events,
    finalize_checked_scenario,
    published_deck,
    recorded_request,
    renew_lock_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_lock_expiration,
    wait_for_presentation_catch_up,
    write_checker_projection,
)


def main() -> None:
    scenario = "stage_c_particion"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name = "stage-c-partition-editor"
    client = Client(client_name, history)
    config = cluster.load()

    # --- Setup: start on the current leader ---
    leader_slot = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != leader_slot]
    nodes_map = config["host_nodes"]

    deck_id = stable_uuid("stage-c-partition:deck")
    slide_id = stable_uuid("stage-c-partition:slide")
    element_id = stable_uuid("stage-c-partition:element")
    old_session = stable_uuid("stage-c-partition:session1")

    # 1. Publish deck, create element+lock on the leader (token=1, epoch=0)
    require_status(
        client.call("publish_deck", **published_deck(deck_id, [slide_id], name="Stage C partition")),
        "PUBLISHED",
    )
    create_resp = require_status(
        client.call(
            "create_element",
            deck_id=deck_id,
            session_id=old_session,
            slide_id=slide_id,
            element=text_element(element_id, "Before partition"),
        ),
        "CREATED_AND_LOCKED",
    )
    old_token = create_resp["value"]["fencing_token"]
    initial_epoch = create_resp["value"]["lease_epoch"]
    print(f"[particion] lock acquired: token={old_token}, epoch={initial_epoch}", flush=True)

    # 2. Wait for at least one automatic renewal on the full cluster
    renew1 = renew_lock_request(
        client_name, history, f"renew-pre-partition-{element_id[:8]}",
        deck_id, old_session, element_id, old_token,
    )
    require_status(renew1, "RENEWED")
    epoch_after_renew = renew1["result"]["value"]["lease_epoch"]
    print(f"[particion] pre-partition renewal: epoch={epoch_after_renew}", flush=True)

    # 3. Isolate the leader (lock owner) in the minority
    chaos.main(["partition", leader_slot, "/", *majority])
    partition_started = time.monotonic()
    print(f"[particion] partitioned: {leader_slot} isolated from {majority}", flush=True)

    # 4. Minority attempt: renewal from isolated node must NOT be confirmed
    minority_renew = recorded_request(
        client_name,
        probes,
        f"minority-renew-{element_id[:8]}",
        "renew_lock",
        {
            "deck_id": deck_id,
            "session_id": old_session,
            "element_id": element_id,
            "fencing_token": old_token,
        },
        node=leader_slot,
        timeout=2.0,
    )
    if minority_renew.get("ok") is True and minority_renew.get("result", {}).get("status") == "RENEWED":
        raise RuntimeError(f"minority confirmed renewal — safety violation: {minority_renew}")
    unavailability_seconds = time.monotonic() - partition_started
    append_evidence(
        probes,
        "minority_renewal_rejected",
        isolated=leader_slot,
        response=minority_renew,
        unavailability_seconds=unavailability_seconds,
    )
    print(f"[particion] minority renewal correctly failed: {minority_renew.get('error') or minority_renew.get('result', {}).get('status')}", flush=True)

    # 5. Majority side: wait for the lock to expire (inactivity timeout × 1.5)
    new_leader_slot = wait_for_leader(config, exclude={leader_slot})
    print(f"[particion] majority leader: {new_leader_slot}", flush=True)

    expiration_wait = INACTIVITY_TIMEOUT * 1.5 + 0.5  # extra scheduling slack
    print(f"[particion] waiting {expiration_wait:.1f}s for expiration on majority...", flush=True)
    wait_for_lock_expiration(deck_id, element_id, timeout=expiration_wait + 5.0)
    print("[particion] lock expired on majority", flush=True)

    # 6. New session acquires on the majority → must get a larger token
    new_session = stable_uuid("stage-c-partition:session2")
    acquire_resp = require_status(
        recorded_request(
            client_name,
            history,
            f"acquire-after-expiry-{element_id[:8]}",
            "acquire_lock",
            {"deck_id": deck_id, "session_id": new_session, "element_id": element_id},
            node=new_leader_slot,
        ),
        "LOCK_ACQUIRED",
    )
    new_token = acquire_resp["value"]["fencing_token"]
    new_epoch = acquire_resp["value"]["lease_epoch"]
    if new_token <= old_token:
        raise RuntimeError(
            f"new token {new_token} is not strictly larger than old token {old_token}"
        )
    print(f"[particion] reacquired: token={new_token} (> {old_token}), epoch={new_epoch}", flush=True)
    append_evidence(
        probes,
        "lock_reassigned",
        old_token=old_token,
        new_token=new_token,
        new_epoch=new_epoch,
        new_leader=new_leader_slot,
    )

    # Maintain the reassigned lock while healing and probing stale authority.
    new_authority = LockAuthority(
        deck_id=deck_id,
        slide_id=slide_id,
        element_id=element_id,
        editor_session=new_session,
        fencing_token=new_token,
        lease_epoch=new_epoch,
        element_version=acquire_resp["value"]["element_version"],
    )
    maintenance = LockMaintenanceService(
        request_sender=lambda op, args, request_id, attempt_timeout: recorded_request(
            client_name,
            history,
            request_id,
            op,
            args,
            node=new_leader_slot,
            timeout=attempt_timeout,
        ),
        http_timeout=RENEWAL_INTERVAL,
        inactivity_timeout=INACTIVITY_TIMEOUT,
    )
    maintenance.register_lock(new_authority, RENEWAL_INTERVAL)
    print("[particion] automatic renewal started for reassigned lock", flush=True)

    # 7. Protected edit with new token succeeds on majority
    require_status(
        recorded_request(
            client_name,
            history,
            f"edit-after-reassign-{element_id[:8]}",
            "update_element",
            {
                "deck_id": deck_id,
                "session_id": new_session,
                "element_id": element_id,
                "fencing_token": new_token,
                "expected_version": 1,
                "patch": {"content": "Written after reassignment"},
            },
            node=new_leader_slot,
        ),
        "SAVED",
    )
    print("[particion] protected edit succeeded with new token", flush=True)

    # 8. Extract internal expiration evidence from operational logs
    expiration_trace_path = Path("run") / f"{scenario}_expiration_trace.jsonl"
    all_expiration_events: list[dict] = []
    for slot in majority:
        log_path = ROOT / "run" / slot / "events.log"
        events = extract_internal_expiration_events(log_path)
        for ev in events:
            ev["_source_slot"] = slot
        all_expiration_events.extend(events)
    expiration_trace_path.parent.mkdir(parents=True, exist_ok=True)
    expiration_trace_path.write_text(
        "".join(json.dumps(ev, sort_keys=True) + "\n" for ev in all_expiration_events),
        encoding="utf-8",
    )
    print(f"[particion] expiration events found: {len(all_expiration_events)}", flush=True)
    append_evidence(probes, "internal_expiration_evidence", event_count=len(all_expiration_events))

    # 9. Heal partition
    chaos.main(["heal"])
    print("[particion] partition healed", flush=True)
    time.sleep(0.5)  # allow healed node to reconnect

    # 10. Former owner's stale attempts must be deterministically rejected
    stale_renew = recorded_request(
        client_name,
        history,
        f"stale-renew-{element_id[:8]}",
        "renew_lock",
        {
            "deck_id": deck_id,
            "session_id": old_session,
            "element_id": element_id,
            "fencing_token": old_token,
        },
        timeout=10.0,
    )
    stale_renew_status = (stale_renew.get("result") or {}).get("status")
    if stale_renew_status not in ("STALE_TOKEN", "NOT_FOUND"):
        raise RuntimeError(
            f"former owner's stale renewal was not rejected: status={stale_renew_status}, response={stale_renew}"
        )
    append_evidence(probes, "stale_renewal_rejected", status=stale_renew_status)
    print(f"[particion] stale renewal rejected: {stale_renew_status}", flush=True)

    stale_release = recorded_request(
        client_name,
        history,
        f"stale-release-{element_id[:8]}",
        "release_lock",
        {
            "deck_id": deck_id,
            "session_id": old_session,
            "element_id": element_id,
            "fencing_token": old_token,
        },
        timeout=10.0,
    )
    stale_release_status = (stale_release.get("result") or {}).get("status")
    if stale_release_status not in ("STALE_TOKEN", "NOT_FOUND", "NOT_OWNER"):
        raise RuntimeError(
            f"former owner's stale release was not rejected: status={stale_release_status}"
        )
    append_evidence(probes, "stale_release_rejected", status=stale_release_status)
    print(f"[particion] stale release rejected: {stale_release_status}", flush=True)

    # Stop background renewal before the final reads, then make one explicit
    # renewal so the reassigned lock has a fresh inactivity window while every
    # replica is checked.
    confidence = maintenance.get_confidence(element_id)
    maintenance.close()
    final_renewal = renew_lock_request(
        client_name,
        history,
        f"renew-before-final-{element_id[:8]}",
        deck_id,
        new_session,
        element_id,
        new_token,
        node=new_leader_slot,
    )
    require_status(final_renewal, "RENEWED")
    append_evidence(
        probes,
        "reassigned_lock_maintained",
        confidence=confidence.value,
        final_epoch=final_renewal["result"]["value"]["lease_epoch"],
    )

    # 11. All replicas converge
    _final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element_state = _final["slide"]["elements"][0]
    if element_state["configuration"]["content"] != "Written after reassignment":
        raise RuntimeError(f"converged content is wrong: {element_state}")
    if element_state["version"] != 2:
        raise RuntimeError(f"expected version=2 after one edit, got {element_state['version']}")

    # Verify lock convergence across all replicas
    assert_all_replicas_lock_state(
        nodes_map, deck_id, element_id,
        expected_fencing=new_token,
    )
    print("[particion] all replicas converged", flush=True)

    # Merge the committed server-originated expiration into the checker input.
    internal_records = build_expiration_checker_records(all_expiration_events)
    if not internal_records:
        raise RuntimeError("no committed expiration transitions available for checker projection")
    projection_history_path = Path("run") / f"{scenario}_checker_history.jsonl"
    projection_audit_path = Path("run") / f"{scenario}_checker_projection.jsonl"
    projection_summary = write_checker_projection(
        history,
        projection_history_path,
        projection_audit_path,
        internal_records=internal_records,
    )
    append_evidence(
        probes,
        "checker_projection",
        history=projection_history_path.name,
        audit=projection_audit_path.name,
        **projection_summary,
    )
    print(f"[particion] checker projection: {projection_summary}", flush=True)

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"create_element", "acquire_lock", "renew_lock", "update_element", "release_lock"},
        read_operations={"get_element", "read_lock_status"},
        unavailability_seconds=unavailability_seconds,
        extra_evidence=[
            probes.name,
            expiration_trace_path.name,
            projection_history_path.name,
            projection_audit_path.name,
        ],
        checker_history_path=projection_history_path,
    )
    print("[particion] scenario complete", flush=True)


if __name__ == "__main__":
    main()
