"""Stage C.1 preserved-disk follower catch-up scenario.

Three-replica cluster. Acquire lock, renew on majority while one follower is
offline, let the lock expire (or be released) and reassign, then restart the
follower with intact disk and verify full AppendEntries catch-up of content,
element version, lock state, Lease Epoch, and fencing counter.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chaos
import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_c_support import (
    INACTIVITY_TIMEOUT,
    append_evidence,
    assert_all_replicas_lock_state,
    finalize_checked_scenario,
    login_scenario_user,
    published_deck,
    recorded_request,
    renew_lock_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_lock_expiration,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_c_replica"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client = Client(stable_uuid("stage-c-replica:client"), history)
    config = cluster.load()
    leader = wait_for_leader(config)
    follower = next(slot for slot in config["names"] if slot != leader)

    deck_id = stable_uuid("stage-c-replica:deck")
    slide_id = stable_uuid("stage-c-replica:slide")
    element_id = stable_uuid("stage-c-replica:element")
    session_id = stable_uuid("stage-c-replica:session")

    _auth = login_scenario_user(
        client.name, history, "stage-c-replica:login",
    )

    # 1. Publish deck and create+lock element (majority commits this)
    require_status(
        recorded_request(
            client.name, history,
            "stage-c-replica:publish-deck",
            "publish_deck",
            published_deck(deck_id, [slide_id], name="Stage C follower recovery"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    create_resp = require_status(
        recorded_request(
            client.name, history,
            f"stage-c-replica:create-elem-{element_id[:8]}",
            "create_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "slide_id": slide_id,
                "element": text_element(element_id, "Before follower failure"),
            },
            _auth=_auth,
        ),
        "CREATED_AND_LOCKED",
    )
    fencing_token: int = create_resp["value"]["fencing_token"]
    initial_epoch: int = create_resp["value"]["lease_epoch"]
    element_version: int = create_resp["value"]["element_version"]

    # 2. Renew once on the full cluster (follower still online)
    r1 = renew_lock_request(
        client.name,
        history,
        f"stage-c-replica-renew-pre-{element_id[:8]}",
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
    )
    require_status(r1, "RENEWED")
    epoch_before_offline = r1["result"]["value"]["lease_epoch"]
    append_evidence(probes, "initial_renewal_committed", epoch=epoch_before_offline, leader=leader)

    # 3. Stop one follower (keep disk intact)
    chaos.main(["kill", follower])
    append_evidence(probes, "follower_killed", leader=leader, follower=follower)

    # 4. Commit renewals and content update on the majority
    r2 = renew_lock_request(
        client.name,
        history,
        f"stage-c-replica-renew-offline-1-{element_id[:8]}",
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
        node=leader,
    )
    require_status(r2, "RENEWED")
    append_evidence(probes, "majority_renewal_committed", epoch=r2["result"]["value"]["lease_epoch"])

    require_status(
        recorded_request(
            client.name, history,
            f"stage-c-replica:update-elem-{element_id[:8]}",
            "update_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "element_id": element_id,
                "fencing_token": fencing_token,
                "expected_version": element_version,
                "patch": {"content": "Committed with follower down"},
            },
            node=leader,
            _auth=_auth,
        ),
        "SAVED",
    )
    append_evidence(probes, "majority_edit_committed")

    # 5. Release lock (while follower is still offline) so the lock is gone
    require_status(
        recorded_request(
            client.name, history,
            f"stage-c-replica:release-{element_id[:8]}",
            "release_lock",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "element_id": element_id,
                "fencing_token": fencing_token,
            },
            node=leader,
            _auth=_auth,
        ),
        "RELEASED",
    )
    append_evidence(probes, "lock_released_on_majority")

    # 6. New session reacquires (larger fencing token)
    session2 = stable_uuid("stage-c-replica:session2")
    acquire2 = require_status(
        recorded_request(
            client.name, history,
            f"stage-c-replica:acquire2-{element_id[:8]}",
            "acquire_lock",
            {
                "deck_id": deck_id,
                "session_id": session2,
                "element_id": element_id,
            },
            node=leader,
            _auth=_auth,
        ),
        "LOCK_ACQUIRED",
    )
    fencing_token2: int = acquire2["value"]["fencing_token"]
    epoch2: int = acquire2["value"]["lease_epoch"]
    assert fencing_token2 > fencing_token, (
        f"new fencing token {fencing_token2} must be > old {fencing_token}"
    )
    append_evidence(
        probes,
        "reassignment_committed",
        new_fencing_token=fencing_token2,
        new_epoch=epoch2,
    )

    # 7. Restart follower with intact disk and wait for catch-up
    chaos.main(["start", follower])
    append_evidence(probes, "follower_restarted_with_preserved_disk", follower=follower)

    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element = final["slide"]["elements"][0]
    if element["configuration"]["content"] != "Committed with follower down":
        raise RuntimeError(f"follower did not catch up content: {final}")
    if element["version"] != element_version + 1:
        raise RuntimeError(
            f"follower version mismatch: expected {element_version + 1}, got {element['version']}"
        )
    append_evidence(probes, "follower_caught_up_presentation")

    # 8. Verify lock state convergence across ALL replicas (new owner, larger token)
    assert_all_replicas_lock_state(
        config["host_nodes"],
        deck_id,
        element_id,
        expected_fencing=fencing_token2,
        expected_epoch=epoch2,
    )
    append_evidence(
        probes,
        "lock_state_converged",
        expected_fencing=fencing_token2,
        expected_epoch=epoch2,
    )

    # 9. Verify old fencing token is rejected on restarted follower
    stale_renew = renew_lock_request(
        client.name,
        history,
        f"stage-c-replica-stale-renew-{element_id[:8]}",
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
        node=follower,
    )
    stale_status = (stale_renew.get("result") or {}).get("status")
    if stale_status not in {"STALE_TOKEN", "NOT_OWNER"}:
        raise RuntimeError(
            f"old token was not rejected after catch-up: {stale_renew}"
        )
    append_evidence(probes, "stale_token_rejected_after_catchup", status=stale_status)

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "create_element", "update_element", "renew_lock",
            "release_lock", "acquire_lock",
        },
        read_operations={"get_element"},
        extra_evidence=[probes.name],
    )
    print(
        f"[{scenario}] PASSED — follower caught up: "
        f"content='{element['configuration']['content']}', "
        f"version={element['version']}, "
        f"new_token={fencing_token2}, epoch={epoch2}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
