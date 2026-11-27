"""Stage D.1 — Follower-recovery scenario (Scenario 3).

Three-replica cluster. Stop one follower, commit Deck Settings,
Slide Settings, and a structural order change (create/reorder/delete)
on the remaining majority, then restart the stopped follower with its
disk preserved. Verify the follower catches up via ordinary
AppendEntries and matches every replica on:
  - every active deck setting and deck_settings_version
  - every slide setting and slide_settings_version
  - structural deck_version and slide sequence (Derived Slide Ordinals)
  - every element and lock state

The scenario starts from a clean three-replica cluster.
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
from stage_a_support import wait_for_leader
from stage_d_support import (
    ScenarioClient,
    append_evidence,
    assert_all_replicas_deck_settings,
    assert_all_replicas_lock_state,
    assert_all_replicas_slide_settings,
    finalize_checked_scenario,
    login_scenario_user,
    owned_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_all_replicas_applied_equal,
    wait_for_presentation_catch_up,
    write_checker_projection,
)


def main() -> None:
    scenario = "stage_d_replica"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name = stable_uuid("stage-d-replica:client")
    client = ScenarioClient(client_name, history)
    config = cluster.load()
    leader = wait_for_leader(config)
    follower = next(slot for slot in config["names"] if slot != leader)
    nodes_map = config["host_nodes"]

    deck_id = stable_uuid("stage-d-replica:deck")
    slide1_id = stable_uuid("stage-d-replica:slide1")
    slide2_id = stable_uuid("stage-d-replica:slide2")
    element_id = stable_uuid("stage-d-replica:element")
    session_id = stable_uuid("stage-d-replica:session")

    _auth = login_scenario_user(
        client_name, history, "stage-d-replica:login",
    )

    # 1. Publish a schema v2 deck with two slides on the full cluster
    require_status(
        recorded_request(
            client_name, history,
            "stage-d-replica:publish-deck",
            "publish_deck",
            owned_deck(deck_id, [slide1_id, slide2_id], name="Stage D follower recovery"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    append_evidence(probes, "deck_published", deck_id=deck_id, leader=leader)

    # 2. Create a text element on slide1 (locked) on the full cluster
    create_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-replica:create-elem-{element_id[:8]}",
            "create_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "slide_id": slide1_id,
                "element": text_element(element_id, "Before follower failure"),
            },
            _auth=_auth,
        ),
        "CREATED_AND_LOCKED",
    )
    fencing_token: int = create_resp["value"]["fencing_token"]
    element_version: int = create_resp["value"]["element_version"]
    append_evidence(probes, "element_created_and_locked", element_id=element_id,
                    fencing_token=fencing_token)

    # 3. Stop one follower (keep disk intact)
    chaos.main(["kill", follower])
    append_evidence(probes, "follower_killed", follower=follower, leader=leader)

    # 4. Commit Deck Settings on the majority (rename + resize)
    deck_settings_resp = require_status(
        recorded_request(
            client_name,
            history,
            f"stage-d-replica-deck-settings-{deck_id[:8]}",
            "update_deck_settings",
            {
                "deck_id": deck_id,
                "expected_deck_settings_version": 0,
                "patch": {"name": "Renamed While Follower Down", "width_px": 1920, "height_px": 1080},
            },
            _auth=_auth,
            node=leader,
        ),
        "DECK_SETTINGS_UPDATED",
    )
    new_deck_settings_version: int = deck_settings_resp["value"]["deck_settings_version"]
    assert new_deck_settings_version == 1, (
        f"expected deck_settings_version=1, got {new_deck_settings_version}"
    )
    append_evidence(
        probes, "deck_settings_committed_on_majority",
        deck_settings_version=new_deck_settings_version,
        name="Renamed While Follower Down", width_px=1920, height_px=1080,
    )

    # 5. Commit Slide Settings on the majority (change slide1 background)
    slide1_settings_resp = require_status(
        recorded_request(
            client_name,
            history,
            f"stage-d-replica-slide1-settings-{slide1_id[:8]}",
            "update_slide_settings",
            {
                "deck_id": deck_id,
                "slide_id": slide1_id,
                "expected_slide_settings_version": 0,
                "patch": {"background": "#FFEEDDCC"},
            },
            _auth=_auth,
            node=leader,
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    new_slide1_ssv: int = slide1_settings_resp["value"]["slide_settings_version"]
    assert new_slide1_ssv == 1, f"expected slide1_settings_version=1, got {new_slide1_ssv}"
    append_evidence(
        probes, "slide1_settings_committed_on_majority",
        slide_settings_version=new_slide1_ssv, background="#FFEEDDCC",
    )

    # 6. Structural order change: create a third slide on the majority
    slide3_id = stable_uuid("stage-d-replica:slide3")
    create_slide_resp = require_status(
        recorded_request(
            client_name,
            history,
            f"stage-d-replica-create-slide3-{slide3_id[:8]}",
            "create_slide",
            {
                "deck_id": deck_id,
                "expected_deck_version": 0,
                "slide": {
                    "metadata": {
                        "slide_id": slide3_id,
                        "background": "#AABBCCDD",
                    },
                    "elements": [],
                },
            },
            _auth=_auth,
            node=leader,
        ),
        "CREATED",
    )
    new_deck_version_after_create: int = create_slide_resp["value"]["deck_version"]
    assert new_deck_version_after_create == 1, (
        f"expected deck_version=1 after create_slide, got {new_deck_version_after_create}"
    )
    append_evidence(
        probes, "slide3_created_on_majority",
        deck_version=new_deck_version_after_create, slide3_id=slide3_id,
    )

    # 7. Update element content on majority while follower is offline
    require_status(
        recorded_request(
            client_name, history,
            f"stage-d-replica:update-elem-{element_id[:8]}",
            "update_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "element_id": element_id,
                "fencing_token": fencing_token,
                "expected_version": element_version,
                "patch": {"content": "Updated with follower down"},
            },
            node=leader,
            _auth=_auth,
        ),
        "SAVED",
    )
    append_evidence(probes, "element_updated_on_majority")

    # 8. Restart the follower with intact disk
    chaos.main(["start", follower])
    append_evidence(probes, "follower_restarted_with_preserved_disk", follower=follower)

    # 9. Wait for all replicas to converge on the same applied index
    applied = wait_for_all_replicas_applied_equal(timeout=30.0)
    append_evidence(probes, "all_replicas_converged", applied_per_slot=applied)

    # 10. Wait for presentation catch-up (slide1 serves as the reference slide)
    final = wait_for_presentation_catch_up(client, deck_id, slide1_id)
    slide1_elements = final["slide"]["elements"]
    if not slide1_elements:
        raise RuntimeError(f"slide1 has no elements after catch-up: {final}")
    element = slide1_elements[0]
    if element["configuration"]["content"] != "Updated with follower down":
        raise RuntimeError(f"follower did not catch up element content: {final}")
    if element["version"] != element_version + 1:
        raise RuntimeError(
            f"follower element version mismatch: expected {element_version + 1}, got {element['version']}"
        )
    append_evidence(probes, "follower_caught_up_presentation")

    # 11. Assert Deck Settings convergence on ALL replicas
    deck_obs = assert_all_replicas_deck_settings(
        nodes_map,
        deck_id,
        expected_deck_settings_version=1,
        expected_name="Renamed While Follower Down",
        expected_width_px=1920,
        expected_height_px=1080,
        expected_deck_version=1,  # +1 for created slide3
    )
    append_evidence(
        probes, "deck_settings_converged_on_all_replicas",
        expected_deck_settings_version=1,
        expected_deck_version=1,
    )

    # 12. Assert Slide Settings convergence on ALL replicas for slide1
    slide_obs = assert_all_replicas_slide_settings(
        nodes_map,
        deck_id,
        slide1_id,
        expected_slide_settings_version=1,
        expected_background="#FFEEDDCC",
        expected_ordinal=0,
    )
    append_evidence(
        probes, "slide_settings_converged_on_all_replicas",
        expected_slide_settings_version=1, expected_background="#FFEEDDCC",
    )

    # 13. Assert deck preview shows correct slide sequence (slide1, slide2, slide3)
    #     on all replicas (structural order convergence)
    #     Stage F: each replica's applied durable slide order, then one leader read.
    from stage_d_support import durable_deck
    leader_preview = require_status(client.call("get_deck_preview", deck_id=deck_id), "OK")
    if leader_preview["slide_ids"] != [slide1_id, slide2_id, slide3_id]:
        raise RuntimeError(f"leader preview slide sequence mismatch: {leader_preview['slide_ids']}")
    for slot in nodes_map:
        slide_ids = [slide["metadata"]["slide_id"] for slide in durable_deck(slot, deck_id)["slides"]]
        if slide_ids != [slide1_id, slide2_id, slide3_id]:
            raise RuntimeError(
                f"replica {slot}: slide sequence mismatch: got {slide_ids}, "
                f"expected [{slide1_id}, {slide2_id}, {slide3_id}]"
            )
    append_evidence(
        probes, "slide_sequence_converged_on_all_replicas",
        expected=[slide1_id, slide2_id, slide3_id],
    )

    # 14. Assert lock state convergence (element should still be locked)
    assert_all_replicas_lock_state(
        nodes_map,
        deck_id,
        element_id,
        expected_fencing=fencing_token,
    )
    append_evidence(
        probes, "lock_state_converged_on_all_replicas",
        expected_fencing=fencing_token,
    )

    # 15. Verify Derived Slide Ordinals are consistent (0-based position)
    #     Stage F: each replica's applied durable slide order, not a follower read.
    from stage_d_support import durable_deck
    for slot in nodes_map:
        order = [slide["metadata"]["slide_id"] for slide in durable_deck(slot, deck_id)["slides"]]
        if order != [slide1_id, slide2_id, slide3_id]:
            raise RuntimeError(f"replica {slot} derived slide ordinals differ: {order}")
    append_evidence(probes, "derived_slide_ordinals_verified")

    # Project checker history: strip Raft-assigned committed_index from settings
    # results so the reference model can verify the history.
    checker_history = Path("run") / f"{scenario}_checker_input.jsonl"
    checker_audit = Path("run") / f"{scenario}_checker_audit.jsonl"
    write_checker_projection(history, checker_history, checker_audit)

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "publish_deck",
            "create_element",
            "update_element",
            "update_deck_settings",
            "update_slide_settings",
            "create_slide",
        },
        read_operations={"get_slide", "get_deck_preview"},
        extra_evidence=[probes.name, checker_audit.name],
        checker_history_path=checker_history,
    )
    print(
        f"[{scenario}] PASSED — follower caught up: "
        f"deck_settings_version={new_deck_settings_version}, "
        f"slide1_settings_version={new_slide1_ssv}, "
        f"deck_version={new_deck_version_after_create}, "
        f"element_content='{element['configuration']['content']}'.",
        flush=True,
    )


if __name__ == "__main__":
    main()
