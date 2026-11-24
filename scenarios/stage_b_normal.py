"""Healthy Stage B collaboration, structure, retry, reads, and convergence."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_b_support import (
    finalize_checked_scenario,
    last_history_entry,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_b_normal"
    history = Path("run") / f"{scenario}.jsonl"
    duplicate_evidence = Path("run") / f"{scenario}_duplicate_probes.jsonl"
    client = Client("stage-b-normal-editor", history)
    leader = wait_for_leader(cluster.load())
    deck_id = stable_uuid("normal:deck")
    slide_id = stable_uuid("normal:slide:primary")
    temporary_slide_id = stable_uuid("normal:slide:temporary")
    element_id = stable_uuid("normal:element")
    session_id = stable_uuid("normal:session")

    require_status(
        client.call("publish_deck", **published_deck(deck_id, [slide_id], name="Stage B normal")),
        "PUBLISHED",
    )
    require_status(client.call(
        "create_slide",
        deck_id=deck_id,
        expected_deck_version=0,
        slide={
            "metadata": {
                "slide_id": temporary_slide_id,
                "background": "#445566FF",
            },
            "elements": [],
        },
        insert_after_slide_id=slide_id,
    ), "CREATED")
    require_status(client.call(
        "reorder_slides",
        deck_id=deck_id,
        expected_deck_version=1,
        slide_ids=[temporary_slide_id, slide_id],
    ), "REORDERED")
    require_status(client.call(
        "delete_slide",
        deck_id=deck_id,
        slide_id=temporary_slide_id,
        expected_deck_version=2,
    ), "DELETED")
    require_status(client.call(
        "create_element",
        deck_id=deck_id,
        session_id=session_id,
        slide_id=slide_id,
        element=text_element(element_id, "Version 1"),
    ), "CREATED_AND_LOCKED")

    duplicate_arguments = None
    duplicate_response = None
    duplicate_request_id = None
    for expected_version in range(1, 6):
        arguments = {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": 1,
            "expected_version": expected_version,
            "patch": {"content": f"Version {expected_version + 1}", "x": expected_version * 100},
        }
        response = client.call("update_element", **arguments)
        require_status(response, "SAVED")
        if expected_version == 5:
            duplicate_arguments = arguments
            duplicate_response = response
            duplicate_request_id = last_history_entry(history)["id"]
        observed = client.call("get_element", node=leader, deck_id=deck_id, element_id=element_id)
        if observed.get("result", {}).get("status") != "OK":
            raise RuntimeError(f"local element observation failed: {observed}")

    retry = recorded_request(
        client.name,
        duplicate_evidence,
        duplicate_request_id,
        "update_element",
        duplicate_arguments,
        node=leader,
    )
    if retry != duplicate_response:
        raise RuntimeError(f"exact duplicate retry changed its result: first={duplicate_response}, retry={retry}")

    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    elements = final.get("slide", {}).get("elements", [])
    if len(elements) != 1 or elements[0]["version"] != 6:
        raise RuntimeError(f"unexpected converged normal state: {final}")
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"update_element"},
        read_operations={"get_element"},
        extra_evidence=[duplicate_evidence.name],
    )
    print("final", final)


if __name__ == "__main__":
    main()
