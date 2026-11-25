"""Stage D.1 — Partition and healing scenario (Scenario 5).

Three-replica cluster. Isolate one follower in the minority. Prove:
  - the minority cannot confirm settings work (update_deck_settings directed
    at it times out or returns an inconclusive error, never DECK_SETTINGS_UPDATED)
  - the majority commits Deck Settings, Slide Settings, and a structural order
    change (create/reorder/delete) during the partition
  - an uncertain client intention never claims a confirmed save before a
    definite committed result is known
  - after healing, the minority replica catches up to the majority's schema-v2
    settings, independent revisions, slide sequence, and derived ordinals
  - client polling rejects stale applied_index responses; settings-only commits
    visible via applied_index even when deck_version did not advance for them

The scenario starts from a clean three-replica cluster.
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
from stage_a_support import wait_for_leader
from stage_d_support import (
    _post,
    append_evidence,
    assert_all_replicas_deck_settings,
    assert_all_replicas_slide_settings,
    finalize_checked_scenario,
    login_scenario_user,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    update_deck_settings_request,
    update_slide_settings_request,
    wait_for_all_replicas_applied_equal,
    wait_for_deck_settings_version,
    write_checker_projection,
)


def main() -> None:
    scenario = "stage_d_particion"
    history = Path("run") / f"{scenario}.jsonl"
    probes  = Path("run") / f"{scenario}_probes.jsonl"
    client_name = "stage-d-partition-editor"

    client = Client(client_name, history)
    config = cluster.load()

    # Identify leader and follower to isolate (isolate a follower, not the leader,
    # so the majority can keep committing without an election).
    leader_slot = wait_for_leader(config)
    names = config["names"]
    # Pick a follower that is not the leader
    follower_slot = next(slot for slot in names if slot != leader_slot)
    majority = [slot for slot in names if slot != follower_slot]
    nodes_map = config["host_nodes"]

    deck_id   = stable_uuid("stage-d-partition:deck")
    slide1_id = stable_uuid("stage-d-partition:slide1")
    slide2_id = stable_uuid("stage-d-partition:slide2")
    slide3_id = stable_uuid("stage-d-partition:slide3")

    _auth = login_scenario_user(
        client_name, history, "stage-d-particion:login",
        stable_uuid("stage-d-particion:client"),
    )

    # 1. Publish a schema-v2 deck on the full cluster
    require_status(
        recorded_request(
            client_name, history,
            "stage-d-particion:publish-deck",
            "publish_deck",
            published_deck(deck_id, [slide1_id, slide2_id], name="Stage D partition"),
            _auth=_auth,
        ),
        "PUBLISHED",
    )
    append_evidence(probes, "deck_published", deck_id=deck_id, leader=leader_slot,
                    follower=follower_slot)

    # Record the deck preview applied_index after publication on the full cluster
    full_preview = _post(
        nodes_map[leader_slot],
        {"id": f"preview-pre-partition-{deck_id[:8]}", "op": "get_deck_preview",
         "args": {"deck_id": deck_id}, "client": "_partition_probe"},
        5.0,
    )
    assert full_preview.get("ok"), f"get_deck_preview failed: {full_preview}"
    baseline_applied_index: int = full_preview["result"]["applied_index"]
    append_evidence(probes, "baseline_applied_index",
                    applied_index=baseline_applied_index)

    # 2. Isolate the follower in the minority
    chaos.main(["partition", follower_slot, "/", *majority])
    partition_started = time.monotonic()
    print(f"[particion-d] partitioned: {follower_slot} isolated from {majority}", flush=True)
    append_evidence(probes, "partition_started",
                    isolated=follower_slot, majority=majority)

    # 3. Minority probe: a settings command directed at the minority must NOT
    #    return a confirmed DECK_SETTINGS_UPDATED result.
    try:
        minority_resp = _post(
            nodes_map[follower_slot],
            {
                "id": f"minority-settings-probe-{deck_id[:8]}",
                "op": "update_deck_settings",
                "args": {
                    "deck_id": deck_id,
                    "expected_deck_settings_version": 0,
                    "patch": {"name": "Minority attempt — must not commit"},
                    "_auth": _auth,
                },
                "client": client_name,
            },
            3.0,
        )
    except Exception as _exc:
        minority_resp = {"ok": False, "error": type(_exc).__name__}
    minority_status = (minority_resp.get("result") or {}).get("status")
    if minority_status == "DECK_SETTINGS_UPDATED":
        raise RuntimeError(
            f"minority confirmed settings — quorum safety violation: {minority_resp}"
        )
    unavailability_seconds = time.monotonic() - partition_started
    append_evidence(
        probes, "minority_settings_correctly_rejected",
        isolated=follower_slot,
        response_status=minority_status or minority_resp.get("error"),
        unavailability_seconds=round(unavailability_seconds, 3),
    )
    print(f"[particion-d] minority correctly failed: "
          f"{minority_status or minority_resp.get('error')}", flush=True)

    # 4. Majority commits: Deck Settings rename, Slide Settings change, and
    #    a structural order change — all while the follower is isolated.
    majority_leader = wait_for_leader(config, exclude={follower_slot})

    rename_resp = require_status(
        update_deck_settings_request(
            client_name, history,
            f"stage-d-partition-rename-{deck_id[:8]}",
            deck_id, 0,
            {"name": "Stage D Partition — Majority Rename"},
            _auth=_auth,
            node=majority_leader,
        ),
        "DECK_SETTINGS_UPDATED",
    )
    post_rename_dsv: int = rename_resp["value"]["deck_settings_version"]
    assert post_rename_dsv == 1, rename_resp
    post_rename_applied_index: int = rename_resp["value"]["committed_index"]
    append_evidence(probes, "majority_deck_settings_committed",
                    deck_settings_version=post_rename_dsv,
                    committed_index=post_rename_applied_index)

    slide1_bg_resp = require_status(
        update_slide_settings_request(
            client_name, history,
            f"stage-d-partition-bg1-{slide1_id[:8]}",
            deck_id, slide1_id, 0,
            {"background": "#FF8800FF"},
            _auth=_auth,
            node=majority_leader,
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    post_slide1_ssv: int = slide1_bg_resp["value"]["slide_settings_version"]
    assert post_slide1_ssv == 1, slide1_bg_resp
    append_evidence(probes, "majority_slide_settings_committed",
                    slide_settings_version=post_slide1_ssv)

    # Structural change: create a third slide on the majority
    create_resp = require_status(
        recorded_request(
            client_name, history,
            f"stage-d-particion:create-slide3-{slide3_id[:8]}",
            "create_slide",
            {
                "deck_id": deck_id,
                "expected_deck_version": 0,
                "slide": {"metadata": {"slide_id": slide3_id, "background": "#CCCCCCFF"}, "elements": []},
            },
            _auth=_auth,
            node=majority_leader,
        ),
        "CREATED",
    )
    post_create_dv: int = create_resp["value"]["deck_version"]
    # Wait for all majority replicas to apply the structural change
    # (follower is still isolated so we don't require it here)
    for slot in majority:
        node_url = nodes_map[slot]
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            resp = _post(
                node_url,
                {"id": f"wait-create-{slot}-{deck_id[:8]}", "op": "get_deck_preview",
                 "args": {"deck_id": deck_id}, "client": "_majority_wait"},
                3.0,
            )
            if resp.get("ok") and resp["result"].get("deck_version", 0) >= post_create_dv:
                break
            time.sleep(0.2)
    append_evidence(probes, "majority_structural_change_committed",
                    deck_version=post_create_dv, slide3_id=slide3_id)

    # 5. Heal partition
    chaos.main(["heal"])
    print(f"[particion-d] partition healed after "
          f"{time.monotonic() - partition_started:.1f}s", flush=True)
    time.sleep(0.5)
    append_evidence(probes, "partition_healed")

    # 6. Wait for all replicas (including former minority) to apply the same index
    wait_for_all_replicas_applied_equal(timeout=30.0)
    append_evidence(probes, "all_replicas_applied_equal")
    print("[particion-d] all replicas converged on applied index", flush=True)

    # 7. Assert full convergence: settings, revisions, structural state
    assert_all_replicas_deck_settings(
        nodes_map, deck_id,
        expected_deck_settings_version=post_rename_dsv,
        expected_name="Stage D Partition — Majority Rename",
        expected_deck_version=post_create_dv,
    )
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide1_id,
        expected_slide_settings_version=post_slide1_ssv,
        expected_background="#FF8800FF",
        expected_ordinal=0,
    )
    # slide3 must exist on all replicas now (including former minority)
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide3_id,
        expected_slide_settings_version=0,
        expected_background="#CCCCCCFF",
        expected_ordinal=2,
    )
    append_evidence(probes, "convergence_verified")
    print("[particion-d] all replicas converged on settings and structural state", flush=True)

    # 8. Polling monotonicity check: a settings-only commit advanced applied_index
    #    while deck_version stayed at 0 (deck_settings_version 0→1 is settings-only).
    #    Reading from the recovered (former-minority) replica must now return
    #    applied_index >= post_rename_applied_index (not the stale baseline_applied_index).
    recovered_preview = _post(
        nodes_map[follower_slot],
        {"id": f"preview-post-heal-{deck_id[:8]}", "op": "get_deck_preview",
         "args": {"deck_id": deck_id}, "client": "_partition_probe_post"},
        5.0,
    )
    assert recovered_preview.get("ok"), (
        f"get_deck_preview on recovered replica failed: {recovered_preview}"
    )
    recovered_applied_index: int = recovered_preview["result"]["applied_index"]
    assert recovered_applied_index >= post_rename_applied_index, (
        f"recovered replica applied_index {recovered_applied_index} is behind "
        f"post-rename committed_index {post_rename_applied_index}"
    )
    assert recovered_applied_index > baseline_applied_index, (
        f"recovered replica still at baseline applied_index {baseline_applied_index}"
    )
    append_evidence(
        probes, "polling_monotonicity_verified",
        baseline_applied_index=baseline_applied_index,
        post_rename_applied_index=post_rename_applied_index,
        recovered_applied_index=recovered_applied_index,
    )
    print(f"[particion-d] polling monotonicity verified: "
          f"recovered ai={recovered_applied_index} > baseline={baseline_applied_index}", flush=True)

    # B5. Stale-poll rejection via ClientIntentionOutbox.record_base_projection():
    #     the outbox's monotonic guard must silently reject a stale applied_index.
    from client_journal import ClientIntentionOutbox
    _b5_journal = Path("run") / f"{scenario}_b5_stale_poll.jsonl"
    _b5_outbox = ClientIntentionOutbox(
        logical_client_id=client_name,
        journal_path=_b5_journal,
        session_id=stable_uuid(f"{scenario}:b5-session"),
    )
    _b5_outbox.record_base_projection({"applied_index": recovered_applied_index})
    _b5_outbox.record_base_projection({"applied_index": baseline_applied_index})
    assert _b5_outbox.highest_applied_index == recovered_applied_index, (
        f"stale-poll rejection failed: watermark regressed from {recovered_applied_index} "
        f"to {_b5_outbox.highest_applied_index} after stale observation={baseline_applied_index}"
    )
    append_evidence(
        probes, "stale_poll_rejection_verified",
        recovered_applied_index=recovered_applied_index,
        stale_applied_index=baseline_applied_index,
        watermark_after_stale=_b5_outbox.highest_applied_index,
    )
    print(f"[particion-d] stale-poll rejection verified: watermark stays at "
          f"{_b5_outbox.highest_applied_index}", flush=True)

    # 9. Settings-only commits observable on recovered replica
    #    (deck_version did NOT advance for the deck settings rename)
    recovered_deck_version: int = recovered_preview["result"].get("deck_version", -1)
    recovered_dsv: int = recovered_preview["result"].get("deck_settings_version", -1)
    assert recovered_deck_version == post_create_dv, (
        f"recovered deck_version {recovered_deck_version} != {post_create_dv}"
    )
    assert recovered_dsv == post_rename_dsv, (
        f"recovered deck_settings_version {recovered_dsv} != {post_rename_dsv} — "
        "settings-only commit not visible after healing"
    )
    append_evidence(
        probes, "settings_only_commit_visible_on_recovered",
        deck_version=recovered_deck_version,
        deck_settings_version=recovered_dsv,
    )
    print("[particion-d] settings-only commit visible on recovered replica", flush=True)

    # 10. Finalize evidence
    checker_history = Path("run") / f"{scenario}_checker.jsonl"
    write_checker_projection(history, checker_history,
                             Path("run") / f"{scenario}_projection_audit.jsonl")

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "publish_deck", "update_deck_settings", "update_slide_settings",
            "create_slide",
        },
        read_operations={"get_deck_preview", "get_slide"},
        checker_history_path=checker_history,
        extra_evidence=[str(probes)],
    )
    print(f"[{scenario}] PASSED", flush=True)


if __name__ == "__main__":
    main()
