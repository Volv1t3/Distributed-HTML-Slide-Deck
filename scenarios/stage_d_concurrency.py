"""Stage D.1 — Concurrent settings scenario (Scenario 2).

Three-replica cluster. Proves two properties:
  1. Changes to INDEPENDENT settings resources (different slides, deck vs.
     slide) can both commit without false conflicts.
  2. Two concurrent commands using the SAME expected revision against the
     SAME settings resource produce exactly one SETTINGS_UPDATED success and
     one reconcilable SETTINGS_VERSION_CONFLICT.
  3. After the conflict, the client can discard the stale intention OR submit
     a corrected new intention using the returned revision and a fresh request
     ID — both paths are exercised.

The scenario starts from a clean three-replica cluster.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence,
    assert_all_replicas_deck_settings,
    assert_all_replicas_slide_settings,
    finalize_checked_scenario,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    update_deck_settings_request,
    update_slide_settings_request,
    wait_for_all_replicas_applied_equal,
    write_checker_projection,
)


def main() -> None:
    scenario = "stage_d_concurrency"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name_a = "stage-d-concur-clientA"
    client_name_b = "stage-d-concur-clientB"

    client_a = Client(client_name_a, history)
    client_b = Client(client_name_b, history)
    config = cluster.load()
    leader = wait_for_leader(config)
    nodes_map = config["host_nodes"]

    deck_id   = stable_uuid("stage-d-concur:deck")
    slide1_id = stable_uuid("stage-d-concur:slide1")
    slide2_id = stable_uuid("stage-d-concur:slide2")

    # 1. Publish deck with two slides
    require_status(
        client_a.call(
            "publish_deck",
            **published_deck(deck_id, [slide1_id, slide2_id], name="Stage D concurrency"),
        ),
        "PUBLISHED",
    )
    append_evidence(probes, "deck_published", deck_id=deck_id, leader=leader)

    # ─────────────────────────────────────────────────────────────────────
    # Phase A: Independent-resource concurrency — deck settings and slide2
    #           settings must both succeed without false conflicts.
    # ─────────────────────────────────────────────────────────────────────
    results: dict[str, dict] = {}
    errors: dict[str, Exception] = {}

    def rename_deck() -> None:
        try:
            results["rename"] = update_deck_settings_request(
                client_name_a, history,
                f"stage-d-concur-rename-{deck_id[:8]}",
                deck_id, 0,
                {"name": "Stage D Concurrent — Renamed"},
            )
        except Exception as exc:
            errors["rename"] = exc

    def change_slide2() -> None:
        try:
            results["slide2_bg"] = update_slide_settings_request(
                client_name_b, history,
                f"stage-d-concur-bg2-{slide2_id[:8]}",
                deck_id, slide2_id, 0,
                {"background": "#AABB00FF"},
            )
        except Exception as exc:
            errors["slide2_bg"] = exc

    barrier = threading.Barrier(2)

    def _wrap_rename() -> None:
        barrier.wait()
        rename_deck()

    def _wrap_slide2() -> None:
        barrier.wait()
        change_slide2()

    t1 = threading.Thread(target=_wrap_rename)
    t2 = threading.Thread(target=_wrap_slide2)
    t1.start(); t2.start()
    t1.join(); t2.join()

    if errors:
        raise RuntimeError(f"independent-resource concurrency errors: {errors}")

    rename_result = results["rename"]
    slide2_result = results["slide2_bg"]

    require_status(rename_result, "DECK_SETTINGS_UPDATED")
    require_status(slide2_result, "SLIDE_SETTINGS_UPDATED")
    assert rename_result["result"]["value"]["deck_settings_version"] == 1, rename_result
    assert slide2_result["result"]["value"]["slide_settings_version"] == 1, slide2_result
    append_evidence(probes, "independent_concurrency_both_succeeded",
                    deck_settings_version=1, slide2_settings_version=1)

    # ─────────────────────────────────────────────────────────────────────
    # Phase B: Same-resource conflict — race two updates against slide1
    #           both using expected_slide_settings_version=0.
    #           Exactly one must succeed; the other must get CONFLICT.
    # ─────────────────────────────────────────────────────────────────────
    race_results: dict[str, dict] = {}
    race_errors: dict[str, Exception] = {}

    def update_slide1_a() -> None:
        try:
            race_results["a"] = update_slide_settings_request(
                client_name_a, history,
                f"stage-d-concur-race-a-{slide1_id[:8]}",
                deck_id, slide1_id, 0,
                {"background": "#FF0000FF"},
            )
        except Exception as exc:
            race_errors["a"] = exc

    def update_slide1_b() -> None:
        try:
            race_results["b"] = update_slide_settings_request(
                client_name_b, history,
                f"stage-d-concur-race-b-{slide1_id[:8]}",
                deck_id, slide1_id, 0,
                {"background": "#00FF00FF"},
            )
        except Exception as exc:
            race_errors["b"] = exc

    barrier2 = threading.Barrier(2)

    def _wrap_a() -> None:
        barrier2.wait()
        update_slide1_a()

    def _wrap_b() -> None:
        barrier2.wait()
        update_slide1_b()

    t3 = threading.Thread(target=_wrap_a)
    t4 = threading.Thread(target=_wrap_b)
    t3.start(); t4.start()
    t3.join(); t4.join()

    if race_errors:
        raise RuntimeError(f"race errors: {race_errors}")

    statuses = {k: v.get("result", {}).get("status") for k, v in race_results.items()}
    successes = [k for k, s in statuses.items() if s == "SLIDE_SETTINGS_UPDATED"]
    conflicts  = [k for k, s in statuses.items() if s == "SLIDE_SETTINGS_VERSION_CONFLICT"]

    assert len(successes) == 1 and len(conflicts) == 1, (
        f"expected exactly 1 success and 1 conflict, got statuses={statuses}"
    )
    winner_key   = successes[0]
    conflict_key = conflicts[0]
    winner_bg    = "#FF0000FF" if winner_key == "a" else "#00FF00FF"
    conflict_resp = race_results[conflict_key]

    # Conflict must include current settings and min_commit_index fence
    conflict_val = conflict_resp["result"]["value"]
    assert "min_commit_index" in conflict_val, (
        f"conflict missing min_commit_index: {conflict_val}"
    )
    assert conflict_val.get("slide_settings_version") == 1, conflict_val
    append_evidence(
        probes, "same_resource_race_result",
        winner=winner_key, conflict=conflict_key,
        winner_background=winner_bg,
        conflict_returned_ssv=conflict_val.get("slide_settings_version"),
    )

    # ─────────────────────────────────────────────────────────────────────
    # Phase C: Discard path — the conflict intention is simply dropped.
    # ─────────────────────────────────────────────────────────────────────
    append_evidence(probes, "conflict_discard_path", conflict_key=conflict_key,
                    action="intentionally_not_resubmitting_original")

    # ─────────────────────────────────────────────────────────────────────
    # Phase D: Corrected-resubmission path — fresh request ID, returned
    #          revision. Must succeed with slide_settings_version=2.
    # ─────────────────────────────────────────────────────────────────────
    corrected_bg = "#0000FFFF"
    corrected_resp = require_status(
        update_slide_settings_request(
            client_name_b, history,
            f"stage-d-concur-corrected-{slide1_id[:8]}-v2",
            deck_id, slide1_id,
            conflict_val["slide_settings_version"],  # confirmed version from conflict
            {"background": corrected_bg},
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    assert corrected_resp["value"]["slide_settings_version"] == 2, corrected_resp
    append_evidence(probes, "corrected_resubmission_succeeded",
                    background=corrected_bg, slide_settings_version=2)

    # 5. Final convergence check
    wait_for_all_replicas_applied_equal()
    assert_all_replicas_deck_settings(
        nodes_map, deck_id,
        expected_deck_settings_version=1,
        expected_name="Stage D Concurrent — Renamed",
    )
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide2_id,
        expected_slide_settings_version=1,
        expected_background="#AABB00FF",
    )
    assert_all_replicas_slide_settings(
        nodes_map, deck_id, slide1_id,
        expected_slide_settings_version=2,
        expected_background=corrected_bg,
    )
    append_evidence(probes, "final_convergence_verified")

    # 6. Finalize evidence
    checker_history = Path("run") / f"{scenario}_checker.jsonl"
    write_checker_projection(history, checker_history,
                             Path("run") / f"{scenario}_projection_audit.jsonl")

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"publish_deck", "update_deck_settings", "update_slide_settings"},
        read_operations={"get_deck_preview", "get_slide"},
        checker_history_path=checker_history,
        extra_evidence=[str(probes)],
    )
    print(f"[{scenario}] PASSED", flush=True)


if __name__ == "__main__":
    main()
