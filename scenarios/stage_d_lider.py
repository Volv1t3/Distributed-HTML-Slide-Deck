"""Stage D.1 — Leader-failure and retry scenario (Scenario 4).

Three-replica cluster. Fail the leader while a settings command
(update_deck_settings) is pending around the response/failover boundary,
then retry the EXACT same Logical Client Identity + request ID through the
new leader. Proves:
  - exactly one settings-version increment and one stored result regardless
    of whether the original response was observed by the client
  - reusing that same request identity with DIFFERENT content after failover
    is rejected as REQUEST_ID_REUSE

Also proves that a separate update_slide_settings command committed on the
majority (committed before the leader failure) is preserved after failover.

The scenario starts from a clean three-replica cluster.
"""

from __future__ import annotations

import threading
import time
import json
from pathlib import Path
import sys

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
    scenario = "stage_d_lider"
    history = Path("run") / f"{scenario}.jsonl"
    pending_probe = Path("run") / f"{scenario}_pending_settings.jsonl"
    fault_evidence = Path("run") / f"{scenario}_faults.jsonl"
    client_name = "stage-d-leader-editor"

    client = Client(client_name, history)
    config = cluster.load()
    old_leader = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != old_leader]
    nodes_map = config["host_nodes"]

    deck_id = stable_uuid("lider-d:deck")
    slide_id = stable_uuid("lider-d:slide")

    # 1. Publish deck on the full cluster
    require_status(
        client.call(
            "publish_deck",
            **published_deck(deck_id, [slide_id], name="Stage D leader failure"),
        ),
        "PUBLISHED",
    )
    append_evidence(fault_evidence, "deck_published", deck_id=deck_id, leader=old_leader)

    # 2. Commit a Slide Settings update on the full cluster (this will survive failover)
    slide_settings_resp = require_status(
        update_slide_settings_request(
            client_name,
            history,
            f"stage-d-lider-slide-settings-pre-{slide_id[:8]}",
            deck_id,
            slide_id,
            0,
            {"background": "#AABBCCDD"},
            node=old_leader,
        ),
        "SLIDE_SETTINGS_UPDATED",
    )
    slide_ssv_before_failure: int = slide_settings_resp["value"]["slide_settings_version"]
    assert slide_ssv_before_failure == 1, (
        f"expected slide_settings_version=1, got {slide_ssv_before_failure}"
    )
    append_evidence(
        fault_evidence, "slide_settings_committed_before_failure",
        slide_settings_version=slide_ssv_before_failure, background="#AABBCCDD",
    )

    # 3. Build the pending deck-settings request identity (exact-ID retry target)
    pending_request_id = f"stage-d-lider-pending-deck-settings-{deck_id[:8]}"
    settings_args = {
        "deck_id": deck_id,
        "expected_deck_settings_version": 0,
        "patch": {"name": "Renamed via Failing Leader"},
    }
    pending_response: dict = {}

    def send_pending_settings() -> None:
        """Send update_deck_settings to the old leader in a background thread."""
        try:
            resp = _post(
                nodes_map[old_leader],
                {
                    "id": pending_request_id,
                    "op": "update_deck_settings",
                    "args": settings_args,
                    "client": client_name,
                },
                timeout=3.0,
            )
        except Exception as exc:
            resp = {"ok": False, "error": type(exc).__name__}
        pending_response["value"] = resp
        import json as _json
        pending_path = Path("run") / f"{scenario}_pending_settings.jsonl"
        pending_path.parent.mkdir(parents=True, exist_ok=True)
        with pending_path.open("a") as fh:
            fh.write(
                _json.dumps({
                    "id": pending_request_id,
                    "op": "update_deck_settings",
                    "args": settings_args,
                    "ok": resp.get("ok", False),
                    "result": resp.get("result"),
                    "error": resp.get("error"),
                    "invoke": time.time(),
                    "client": client_name,
                }) + "\n"
            )

    # 4. Partition old leader from majority, then fire the pending command
    chaos.main(["partition", old_leader, "/", *majority])
    unavailability_started = time.monotonic()

    pending_thread = threading.Thread(target=send_pending_settings)
    pending_thread.start()
    time.sleep(0.3)  # let the in-flight command reach the isolated leader

    chaos.main(["kill", old_leader])
    append_evidence(
        fault_evidence, "leader_killed_with_settings_in_flight",
        leader=old_leader,
        pending_request_id=pending_request_id,
    )
    chaos.main(["heal"])
    pending_thread.join()

    # Isolated leader must NOT have confirmed the update
    if pending_response.get("value", {}).get("ok") is True:
        raise RuntimeError(
            f"isolated leader unexpectedly confirmed settings update: {pending_response}"
        )

    # 5. Wait for the new leader
    new_leader = wait_for_leader(config, exclude={old_leader})
    election_unavailability_seconds = time.monotonic() - unavailability_started
    append_evidence(
        fault_evidence, "new_leader_elected",
        old_leader=old_leader,
        new_leader=new_leader,
        unavailability_seconds=round(election_unavailability_seconds, 3),
    )

    # 6. Exact-ID retry on the new leader — same request_id, same content.
    #    Must succeed with DECK_SETTINGS_UPDATED: one increment regardless of
    #    whether the entry was already committed or is newly committed now.
    retry = recorded_request(
        client_name,
        history,
        pending_request_id,
        "update_deck_settings",
        settings_args,
        node=new_leader,
        timeout=15.0,
    )
    retry_status = (retry.get("result") or {}).get("status", "")
    if retry_status != "DECK_SETTINGS_UPDATED":
        raise RuntimeError(
            f"exact-ID deck settings retry must return DECK_SETTINGS_UPDATED, got: {retry}"
        )
    post_retry_dsv = (retry.get("result") or {}).get("value", {}).get("deck_settings_version", 0)
    assert post_retry_dsv == 1, (
        f"expected deck_settings_version=1 after retry, got {post_retry_dsv}"
    )
    append_evidence(
        fault_evidence, "exact_id_retry_deck_settings_updated",
        request_id=pending_request_id,
        new_leader=new_leader,
        deck_settings_version=post_retry_dsv,
    )

    # 6b. Stored-result dedup replay: send the identical request again.
    #     The Raft dedup store must echo DECK_SETTINGS_UPDATED with the same
    #     version — no second increment.
    dedup_replay = recorded_request(
        client_name,
        history,
        pending_request_id,
        "update_deck_settings",
        settings_args,
        node=new_leader,
        timeout=10.0,
    )
    dedup_status = (dedup_replay.get("result") or {}).get("status", "")
    dedup_dsv = (dedup_replay.get("result") or {}).get("value", {}).get("deck_settings_version", 0)
    if dedup_status != "DECK_SETTINGS_UPDATED" or dedup_dsv != post_retry_dsv:
        raise RuntimeError(
            f"stored-result replay must echo DECK_SETTINGS_UPDATED dsv={post_retry_dsv}, "
            f"got status={dedup_status} dsv={dedup_dsv}: {dedup_replay}"
        )
    append_evidence(
        fault_evidence, "exact_id_dedup_replay_confirmed",
        request_id=pending_request_id,
        new_leader=new_leader,
        deck_settings_version=dedup_dsv,
    )

    # 7. Confirm settings version advanced exactly ONCE regardless of outcome
    #    Poll until the deck shows version=1 on some live node.
    wait_for_deck_settings_version(deck_id, min_version=1, timeout=15.0)
    append_evidence(
        fault_evidence, "deck_settings_version_confirmed_exactly_one_increment",
        expected_deck_settings_version=1,
    )

    # 8. REQUEST_ID_REUSE: reuse the same request identity with DIFFERENT content
    reuse = recorded_request(
        client_name,
        history,
        pending_request_id,  # same request_id
        "update_deck_settings",
        {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,  # same version
            "patch": {"name": "DIFFERENT CONTENT — must be rejected"},  # different patch
        },
        node=new_leader,
        timeout=10.0,
    )
    reuse_status = (reuse.get("result") or {}).get("status", "")
    if reuse_status != "REQUEST_ID_REUSE":
        raise RuntimeError(
            f"expected REQUEST_ID_REUSE for reused request identity with different content, "
            f"got: {reuse}"
        )
    append_evidence(
        fault_evidence, "request_id_reuse_rejected_as_expected",
        request_id=pending_request_id,
        new_leader=new_leader,
        reuse_status=reuse_status,
    )

    # 9. Restart the old leader with intact disk and wait for full convergence
    chaos.main(["start", old_leader])
    append_evidence(fault_evidence, "former_leader_restarted", leader=old_leader)

    applied = wait_for_all_replicas_applied_equal(timeout=30.0)
    append_evidence(fault_evidence, "all_replicas_converged", applied_per_slot=applied)

    # 10. Assert Deck Settings convergence on ALL replicas
    assert_all_replicas_deck_settings(
        nodes_map,
        deck_id,
        expected_deck_settings_version=1,
        expected_name="Renamed via Failing Leader",
    )
    append_evidence(
        fault_evidence, "deck_settings_converged_on_all_replicas",
        expected_deck_settings_version=1, expected_name="Renamed via Failing Leader",
    )

    # 11. Assert Slide Settings are preserved after failover
    assert_all_replicas_slide_settings(
        nodes_map,
        deck_id,
        slide_id,
        expected_slide_settings_version=1,
        expected_background="#AABBCCDD",
        expected_ordinal=0,
    )
    append_evidence(
        fault_evidence, "slide_settings_preserved_after_failover",
        expected_slide_settings_version=1, expected_background="#AABBCCDD",
    )

    # 12. Assert deck_settings_version is exactly 1 — no double-increment
    all_node_versions = []
    for slot, url in nodes_map.items():
        resp = _post(
            url,
            {
                "id": f"final-deck-preview-{slot}",
                "op": "get_deck_preview",
                "args": {"deck_id": deck_id},
                "client": "_assert",
            },
            4.0,
        )
        actual_dsv = (resp.get("result") or {}).get("deck_settings_version", -1)
        all_node_versions.append(actual_dsv)
        if actual_dsv != 1:
            raise RuntimeError(
                f"replica {slot}: deck_settings_version={actual_dsv} != 1 "
                f"(double-increment guard failed)"
            )
    append_evidence(
        fault_evidence, "no_double_increment_verified",
        all_node_versions=all_node_versions,
    )

    # Record final reads for convergence checking
    from stage_b_support import wait_for_presentation_catch_up as _wait_catchup
    _wait_catchup(client, deck_id, slide_id)

    # Project checker history: strip Raft-assigned committed_index from settings
    # results and exclude duplicate/reuse entries so the reference model can verify.
    checker_history = Path("run") / f"{scenario}_checker_input.jsonl"
    checker_audit = Path("run") / f"{scenario}_checker_audit.jsonl"
    write_checker_projection(history, checker_history, checker_audit)

    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={
            "publish_deck",
            "update_deck_settings",
            "update_slide_settings",
        },
        read_operations={"get_deck_preview", "get_slide"},
        unavailability_seconds=election_unavailability_seconds,
        extra_evidence=[pending_probe.name, fault_evidence.name, checker_audit.name],
        checker_history_path=checker_history,
    )
    print(
        f"[{scenario}] PASSED — "
        f"unavailability={election_unavailability_seconds:.3f}s, "
        f"deck_settings_version=1 (exactly one increment), "
        f"REQUEST_ID_REUSE confirmed.",
        flush=True,
    )


if __name__ == "__main__":
    main()
