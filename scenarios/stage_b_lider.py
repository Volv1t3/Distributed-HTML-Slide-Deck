"""Leader failure with one pending edit, exact-ID retry, and preserved-disk recovery."""

from __future__ import annotations

from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chaos
import cluster
from stage_a_support import wait_for_leader
from stage_b_support import (
    ScenarioClient,
    append_evidence,
    finalize_checked_scenario,
    owned_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_event,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_b_lider"
    history = Path("run") / f"{scenario}.jsonl"
    pending_probe = Path("run") / f"{scenario}_pending_probe.jsonl"
    fault_evidence = Path("run") / f"{scenario}_faults.jsonl"
    client_name = stable_uuid("stage-b-leader-editor")
    client = ScenarioClient(client_name, history, login=True)
    config = cluster.load()
    old_leader = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != old_leader]
    deck_id = stable_uuid("leader:deck")
    slide_id = stable_uuid("leader:slide")
    element_id = stable_uuid("leader:element")
    session_id = stable_uuid("leader:session")
    pending_request_id = "stage-b-leader-pending-update"
    update_arguments = {
        "deck_id": deck_id,
        "session_id": session_id,
        "element_id": element_id,
        "fencing_token": 1,
        "expected_version": 1,
        "patch": {"content": "Committed after exact-ID retry"},
    }

    require_status(client.call(
        "publish_deck",
        **owned_deck(deck_id, [slide_id], name="Stage B leader recovery"),
    ), "PUBLISHED")
    require_status(client.call(
        "create_element",
        deck_id=deck_id,
        session_id=session_id,
        slide_id=slide_id,
        element=text_element(element_id, "Before leader failure"),
    ), "CREATED_AND_LOCKED")

    chaos.main(["partition", old_leader, "/", *majority])
    unavailable_started = time.monotonic()
    pending_response: dict[str, dict] = {}

    def send_pending() -> None:
        pending_response["value"] = recorded_request(
            client_name,
            pending_probe,
            pending_request_id,
            "update_element",
            update_arguments,
            node=old_leader,
            timeout=2.0,
        )

    pending_thread = threading.Thread(target=send_pending)
    pending_thread.start()
    # Stage E+: bearer admission confirms a fresh ReadIndex before appending,
    # so an isolated leader holds the edit at admission instead of appending it.
    wait_for_event(old_leader, "auth_admitted_read_started", request_id=pending_request_id)
    chaos.main(["kill", old_leader])
    append_evidence(fault_evidence, "leader_killed_with_pending_edit", leader=old_leader)
    chaos.main(["heal"])
    pending_thread.join()
    if pending_response["value"].get("ok") is True:
        raise RuntimeError(f"isolated pending leader unexpectedly confirmed the edit: {pending_response}")

    new_leader = wait_for_leader(config, exclude={old_leader})
    retry = recorded_request(
        client_name,
        history,
        pending_request_id,
        "update_element",
        update_arguments,
        node=new_leader,
    )
    require_status(retry, "SAVED")
    unavailable_seconds = time.monotonic() - unavailable_started
    append_evidence(
        fault_evidence,
        "replacement_committed_exact_retry",
        old_leader=old_leader,
        new_leader=new_leader,
        request_id=pending_request_id,
        unavailability_seconds=unavailable_seconds,
    )
    observed = client.call("get_element", node=new_leader, deck_id=deck_id, element_id=element_id)
    if observed.get("result", {}).get("element_version") != 2:
        raise RuntimeError(f"replacement leader did not expose the committed retry: {observed}")

    chaos.main(["start", old_leader])
    append_evidence(fault_evidence, "former_leader_restarted_with_preserved_disk", leader=old_leader)
    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element = final["slide"]["elements"][0]
    if element["version"] != 2 or element["configuration"]["content"] != update_arguments["patch"]["content"]:
        raise RuntimeError(f"leader-failure state did not converge: {final}")
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"update_element"},
        read_operations={"get_element"},
        unavailability_seconds=unavailable_seconds,
        extra_evidence=[pending_probe.name, fault_evidence.name],
    )
    print("final", final)


if __name__ == "__main__":
    main()
