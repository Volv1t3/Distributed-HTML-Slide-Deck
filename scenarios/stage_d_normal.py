"""Stage D.1 — Normal settings and ordering scenario (Scenario 1).

Three-replica cluster. Publish a schema-v2 deck, rename it, resize the
canvas, change multiple slide backgrounds, create/reorder/delete slides.
Proves:
  - consolidated Deck Settings and Slide Settings visible on all replicas
  - Canvas Resize leaves every element's geometry, version, lock owner,
    fencing token, and Lease Epoch byte-identical (Canvas Resize Invariant)
  - every reported Derived Slide Ordinal equals its zero-based array position
    after every structural transition

The scenario starts from a clean three-replica cluster.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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
    update_deck_settings_request,
    update_slide_settings_request,
    wait_for_all_replicas_applied_equal,
    wait_for_deck_settings_version,
    write_checker_projection,
)


def _assert_ordinals(client, deck_id: str, expected_slide_ids: list[str]) -> None:
    """Read the deck preview and verify every ordinal equals array position.

    Stage F: protected reads go through the leader with the scenario bearer.
    """
    data = client.call("get_deck_preview", deck_id=deck_id)
    assert data.get("ok"), f"get_deck_preview failed: {data}"
    result = data["result"]
    slide_ids = result.get("slide_ids") or []
    assert slide_ids == expected_slide_ids, (
        f"ordinal mismatch: got {slide_ids}, expected {expected_slide_ids}"
    )

    for expected_ordinal, slide_id in enumerate(expected_slide_ids):
        sd = client.call("get_slide", deck_id=deck_id, slide_id=slide_id)
        assert sd.get("ok"), f"get_slide ordinal check failed for {slide_id}: {sd}"
        ordinal = sd["result"]["slide"]["metadata"].get("ordinal")
        assert ordinal == expected_ordinal, (
            f"slide {slide_id} ordinal {ordinal} != expected {expected_ordinal}"
        )


def main() -> None:
    scenario = "stage_d_normal"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name = stable_uuid("stage-d-normal:client")

    client = ScenarioClient(client_name, history)
    config = cluster.load()
    leader = wait_for_leader(config)
    nodes_map = config["host_nodes"]

    deck_id    = stable_uuid("stage-d-normal:deck")
    slide1_id  = stable_uuid("stage-d-normal:slide1")
    slide2_id  = stable_uuid("stage-d-normal:slide2")
    slide3_id  = stable_uuid("stage-d-normal:slide3")
    element_id = stable_uuid("stage-d-normal:element")
    session_id = stable_uuid("stage-d-normal:session")

    _auth = login_scenario_user(
        client_name, history, "stage-d-normal:login",
    )

    # 1. Publish schema-v2 deck with two slides
    require_status(
        recorded_request(
            client_name, history,
            "stage-d-normal:publish-deck",
            "publish_deck",
            owned_deck(deck_id, [slide1_id, slide2_id], name="Stage D normal initial"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    append_evidence(probes, "deck_published", deck_id=deck_id, leader=leader)

    # 2. Create a text element on slide1 and lock it; record pre-resize state
    create_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-normal:create-elem-{element_id[:8]}",
            "create_element",
            {
                "deck_id": deck_id,
                "session_id": session_id,
                "slide_id": slide1_id,
                "element": text_element(element_id, "Pre-resize content", x=100),
            },
            _auth=_auth,
        ),
        "CREATED_AND_LOCKED",
    )
    pre_resize_fencing_token: int = create_resp["value"]["fencing_token"]
    pre_resize_element_version: int = create_resp["value"]["element_version"]
    pre_resize_lease_epoch: int = create_resp["value"]["lease_epoch"]
    pre_resize_lock_owner: str = session_id
    append_evidence(
        probes, "element_locked_pre_resize",
        element_id=element_id,
        fencing_token=pre_resize_fencing_token,
        element_version=pre_resize_element_version,
        lease_epoch=pre_resize_lease_epoch,
    )

    # Snapshot all element fields through an authenticated leader read before
    # any deck-settings change (Stage F: protected reads need the bearer).
    _snap_data = client.call("get_slide", deck_id=deck_id, slide_id=slide1_id)
    assert _snap_data.get("ok"), f"pre-resize get_slide failed: {_snap_data}"
    _snap_els = _snap_data["result"]["slide"]["elements"]
    assert len(_snap_els) == 1, f"expected 1 element before resize: {_snap_els}"
    pre_resize_element_full: dict = _snap_els[0]

    # 3. Rename the deck (deck_settings_version: 0 → 1)
    rename_resp = require_status(
        update_deck_settings_request(
            client_name, history,
            f"stage-d-normal-rename-{deck_id[:8]}",
            deck_id, 0,
            {"name": "Stage D Normal — Renamed"},
            _auth=_auth,
        ),
        "DECK_SETTINGS_UPDATED",
    )
    assert rename_resp["value"]["deck_settings_version"] == 1, rename_resp
    assert rename_resp["value"]["deck_version"] == 0  # structural version unchanged
    append_evidence(probes, "deck_renamed", new_name="Stage D Normal — Renamed",
                    deck_settings_version=1)

    # 4. Resize canvas (deck_settings_version: 1 → 2)
    #    Canvas Resize must NOT touch any element field.
    resize_resp = require_status(
        update_deck_settings_request(
            client_name, history,
            f"stage-d-normal-resize-{deck_id[:8]}",
            deck_id, 1,
            {"width_px": 1920, "height_px": 1080},
            _auth=_auth,
        ),
        "DECK_SETTINGS_UPDATED",
    )
    assert resize_resp["value"]["deck_settings_version"] == 2, resize_resp
    assert resize_resp["value"]["deck_version"] == 0  # structural version unchanged
    append_evidence(probes, "canvas_resized", width_px=1920, height_px=1080,
                    deck_settings_version=2)

    # 5. Assert Canvas Resize Invariant across all replicas:
    #    every element field must be byte-identical to its pre-resize snapshot.
    wait_for_all_replicas_applied_equal()
    #    Stage F: compare each replica's applied durable element, not a follower read.
    from stage_d_support import durable_element
    for slot in nodes_map:
        el = durable_element(slot, deck_id, element_id)
        assert el is not None, f"element {element_id} missing on {slot}"
        # Every element field must be byte-identical to the pre-resize snapshot.
        assert el == pre_resize_element_full, (
            f"Canvas Resize Invariant: element changed on {slot}:\n"
            f"  before: {pre_resize_element_full}\n"
            f"  after:  {el}"
        )
    append_evidence(probes, "canvas_resize_invariant_verified",
                    element_id=element_id, pre_x=100, pre_version=pre_resize_element_version)

    # 6. Assert lock state unchanged on all replicas (fencing token and epoch preserved)
    assert_all_replicas_lock_state(
        nodes_map, deck_id, element_id,
        expected_fencing=pre_resize_fencing_token,
        expected_epoch=pre_resize_lease_epoch,
    )
    append_evidence(probes, "lock_state_verified_post_resize",
                    expected_fencing_token=pre_resize_fencing_token,
                    expected_lease_epoch=pre_resize_lease_epoch)

    # 7. Change slide1 background (slide_settings_version: 0 → 1)
    slide1_bg_resp = require_status(
        update_slide_settings_request(
            client_name, history,
            f"stage-d-normal-bg1-{slide1_id[:8]}",
            deck_id, slide1_id, 0,
            {"background": "#FF0000FF"},
            _auth=_auth,
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    assert slide1_bg_resp["value"]["slide_settings_version"] == 1, slide1_bg_resp
    assert slide1_bg_resp["value"]["ordinal"] == 0, slide1_bg_resp
    append_evidence(probes, "slide1_background_updated", background="#FF0000FF")

    # 8. Change slide2 background independently (slide2_settings_version: 0 → 1)
    slide2_bg_resp = require_status(
        update_slide_settings_request(
            client_name, history,
            f"stage-d-normal-bg2-{slide2_id[:8]}",
            deck_id, slide2_id, 0,
            {"background": "#00FF00FF"},
            _auth=_auth,
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    assert slide2_bg_resp["value"]["slide_settings_version"] == 1, slide2_bg_resp
    assert slide2_bg_resp["value"]["ordinal"] == 1, slide2_bg_resp
    append_evidence(probes, "slide2_background_updated", background="#00FF00FF")

    # 9. Verify deck_settings_version did NOT advance during slide settings changes
    wait_for_deck_settings_version(deck_id, 2)
    assert_all_replicas_deck_settings(
        nodes_map, deck_id,
        expected_deck_settings_version=2,
        expected_name="Stage D Normal — Renamed",
        expected_width_px=1920,
        expected_height_px=1080,
    )
    append_evidence(probes, "deck_settings_stable_during_slide_updates",
                    deck_settings_version=2)

    # 10. Create a third slide (structural deck_version advances)
    create_slide_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-normal:create-slide3-{slide3_id[:8]}",
            "create_slide",
            {
                "deck_id": deck_id,
                "expected_deck_version": 0,
                "slide": {"metadata": {"slide_id": slide3_id, "background": "#0000FFFF"}, "elements": []},
            },
            _auth=_auth,
        ),
        "CREATED",
    )
    post_create_dv: int = create_slide_resp["value"]["deck_version"]
    assert post_create_dv >= 1, create_slide_resp
    append_evidence(probes, "slide3_created", deck_version=post_create_dv,
                    slide3_id=slide3_id)

    # 11. Assert Derived Slide Ordinals after create: [slide1, slide2, slide3]
    wait_for_all_replicas_applied_equal()
    _assert_ordinals(client, deck_id, [slide1_id, slide2_id, slide3_id])
    append_evidence(probes, "ordinals_verified_after_create",
                    expected=[slide1_id, slide2_id, slide3_id])

    # 12. Reorder slides: move slide3 to position 0 → [slide3, slide1, slide2]
    reorder_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-normal:reorder-{deck_id[:8]}",
            "reorder_slides",
            {
                "deck_id": deck_id,
                "expected_deck_version": post_create_dv,
                "slide_ids": [slide3_id, slide1_id, slide2_id],
            },
            _auth=_auth,
        ),
        "REORDERED",
    )
    post_reorder_dv: int = reorder_resp["value"]["deck_version"]
    wait_for_all_replicas_applied_equal()
    _assert_ordinals(client, deck_id, [slide3_id, slide1_id, slide2_id])
    append_evidence(probes, "ordinals_verified_after_reorder",
                    expected=[slide3_id, slide1_id, slide2_id],
                    deck_version=post_reorder_dv)

    # 13. Delete slide3 → [slide1, slide2]; ordinals reset
    delete_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-normal:delete-slide3-{slide3_id[:8]}",
            "delete_slide",
            {
                "deck_id": deck_id,
                "expected_deck_version": post_reorder_dv,
                "slide_id": slide3_id,
            },
            _auth=_auth,
        ),
        "DELETED",
    )
    post_delete_dv: int = delete_resp["value"]["deck_version"]
    wait_for_all_replicas_applied_equal()
    _assert_ordinals(client, deck_id, [slide1_id, slide2_id])
    append_evidence(probes, "ordinals_verified_after_delete",
                    expected=[slide1_id, slide2_id],
                    deck_version=post_delete_dv)

    # 14. Final convergence: all replicas agree on all settings
    wait_for_all_replicas_applied_equal()
    assert_all_replicas_deck_settings(
        nodes_map, deck_id,
        expected_deck_settings_version=2,
        expected_name="Stage D Normal — Renamed",
        expected_width_px=1920,
        expected_height_px=1080,
        expected_deck_version=post_delete_dv,
    )
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide1_id,
        expected_slide_settings_version=1,
        expected_background="#FF0000FF",
        expected_ordinal=0,
    )
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide2_id,
        expected_slide_settings_version=1,
        expected_background="#00FF00FF",
        expected_ordinal=1,
    )
    append_evidence(probes, "final_convergence_verified")

    # 15. Write projection + finalize evidence
    checker_history = Path("run") / f"{scenario}_checker.jsonl"
    write_checker_projection(history, checker_history,
                             Path("run") / f"{scenario}_projection_audit.jsonl")

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "publish_deck", "create_element", "update_deck_settings",
            "update_slide_settings", "create_slide", "reorder_slides", "delete_slide",
        },
        read_operations={"get_deck_preview", "get_slide"},
        checker_history_path=checker_history,
        extra_evidence=[str(probes)],
    )
    print(f"[{scenario}] PASSED", flush=True)


if __name__ == "__main__":
    main()
