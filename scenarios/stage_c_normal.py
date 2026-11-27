"""Stage C.1 normal renewal and convergence scenario.

Three-replica cluster. Acquire lock, observe two manual renewals (advancing
Lease Epoch), verify element version is unchanged, edit, release, verify
convergence of content/version/lock/fencing across all replicas.

Stage C.1 timing profile: renewal_interval=0.20s, inactivity_timeout=0.80s.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import cluster
from stage_a_support import wait_for_leader
from stage_c_support import (
    ScenarioClient,
    assert_all_replicas_lock_state,
    finalize_checked_scenario,
    login_scenario_user,
    owned_deck,
    recorded_request,
    renew_lock_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_c_normal"
    history = Path("run") / f"{scenario}.jsonl"
    client = ScenarioClient(stable_uuid("stage-c-normal:client"), history)
    config = cluster.load()
    leader = wait_for_leader(config)

    deck_id = stable_uuid("stage-c-normal:deck")
    slide_id = stable_uuid("stage-c-normal:slide")
    element_id = stable_uuid("stage-c-normal:element")
    session_id = stable_uuid("stage-c-normal:session")

    _auth = login_scenario_user(
        client.name, history, "stage-c-normal:login",
    )

    # 1. Publish deck and create+lock element (token=1, epoch=0)
    require_status(
        recorded_request(
            client.name, history,
            "stage-c-normal:publish-deck",
            "publish_deck",
            owned_deck(deck_id, [slide_id], name="Stage C normal"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    create_resp = require_status(
        recorded_request(
            client.name, history,
            f"stage-c-normal:create-elem-{element_id[:8]}",
            "create_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "slide_id": slide_id,
                "element": text_element(element_id, "Initial content"),
            },
            _auth=_auth,
        ),
        "CREATED_AND_LOCKED",
    )
    fencing_token: int = create_resp["value"]["fencing_token"]
    initial_epoch: int = create_resp["value"]["lease_epoch"]
    element_version: int = create_resp["value"]["element_version"]
    assert initial_epoch == 0, f"expected initial epoch=0, got {initial_epoch}"
    assert fencing_token == 1, f"expected initial fencing_token=1, got {fencing_token}"
    assert element_version == 1, f"expected element_version=1, got {element_version}"

    # 2. Renew lock (epoch 0 → 1)
    r1 = renew_lock_request(
        client.name,
        history,
        f"stage-c-normal-renew-1-{element_id[:8]}",
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
    )
    require_status(r1, "RENEWED")
    assert r1["result"]["value"]["lease_epoch"] == 1, (
        f"expected epoch=1 after first renewal, got {r1}"
    )

    # 3. Renew again (epoch 1 → 2)
    r2 = renew_lock_request(
        client.name,
        history,
        f"stage-c-normal-renew-2-{element_id[:8]}",
        deck_id,
        session_id,
        element_id,
        fencing_token,
        _auth=_auth,
    )
    require_status(r2, "RENEWED")
    assert r2["result"]["value"]["lease_epoch"] == 2, (
        f"expected epoch=2 after second renewal, got {r2}"
    )

    # 4. Verify element version is unchanged by renewals
    observed = client.call("get_element", node=leader, deck_id=deck_id, element_id=element_id)
    if not observed.get("ok"):
        raise RuntimeError(f"element read failed: {observed}")
    obs_version = observed["result"]["element"]["version"]
    if obs_version != element_version:
        raise RuntimeError(
            f"element version changed during renewals: expected {element_version}, got {obs_version}"
        )

    # 5. Update element content (edit with correct fencing token and version)
    require_status(
        recorded_request(
            client.name, history,
            f"stage-c-normal:update-elem-{element_id[:8]}",
            "update_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "element_id": element_id,
                "fencing_token": fencing_token,
                "expected_version": element_version,
                "patch": {"content": "After renewal"},
            },
            node=leader,
            _auth=_auth,
        ),
        "SAVED",
    )

    # 6. Release lock
    require_status(
        recorded_request(
            client.name, history,
            f"stage-c-normal:release-{element_id[:8]}",
            "release_lock",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "element_id": element_id,
                "fencing_token": fencing_token,
            },
            _auth=_auth,
        ),
        "RELEASED",
    )

    # 7. Wait for all replicas to converge and verify lock state
    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    elements = final.get("slide", {}).get("elements", [])
    if not elements:
        raise RuntimeError(f"no elements in converged state: {final}")
    final_element = elements[0]
    if final_element["configuration"]["content"] != "After renewal":
        raise RuntimeError(f"unexpected converged content: {final_element}")
    if final_element["version"] != element_version + 1:
        raise RuntimeError(
            f"unexpected converged version: expected {element_version + 1}, got {final_element['version']}"
        )

    # Verify lock is gone on all replicas
    assert_all_replicas_lock_state(
        config["host_nodes"],
        deck_id,
        element_id,
        expect_no_lock=True,
    )

    # 8. Finalize with checker, latency, and manifest evidence
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"create_element", "update_element", "renew_lock", "release_lock"},
        read_operations={"get_element"},
    )
    print(f"[{scenario}] PASSED — epoch advanced to 2, element_version unchanged at {element_version}, converged.", flush=True)


if __name__ == "__main__":
    main()
