"""Minority write refusal, majority editing, healing, and convergence."""

from __future__ import annotations

from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chaos
import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_b_support import (
    append_evidence,
    finalize_checked_scenario,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_b_particion"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client_name = "stage-b-partition-editor"
    client = Client(client_name, history)
    config = cluster.load()
    isolated = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != isolated]
    deck_id = stable_uuid("partition:deck")
    slide_id = stable_uuid("partition:slide")
    element_id = stable_uuid("partition:element")
    session_id = stable_uuid("partition:session")

    require_status(client.call(
        "publish_deck",
        **published_deck(deck_id, [slide_id], name="Stage B partition"),
    ), "PUBLISHED")
    require_status(client.call(
        "create_element",
        deck_id=deck_id,
        session_id=session_id,
        slide_id=slide_id,
        element=text_element(element_id, "Before partition"),
    ), "CREATED_AND_LOCKED")

    chaos.main(["partition", isolated, "/", *majority])
    unavailable_started = time.monotonic()
    minority_response = recorded_request(
        client_name,
        probes,
        "stage-b-minority-update",
        "update_element",
        {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": 1,
            "expected_version": 1,
            "patch": {"content": "Must not be confirmed on minority"},
        },
        node=isolated,
        timeout=2.0,
    )
    unavailability_seconds = time.monotonic() - unavailable_started
    if minority_response.get("ok") is True:
        raise RuntimeError(f"minority confirmed a shared edit: {minority_response}")
    append_evidence(
        probes,
        "minority_write_rejected",
        isolated=isolated,
        response=minority_response,
        unavailability_seconds=unavailability_seconds,
    )

    replacement = wait_for_leader(config, exclude={isolated})
    require_status(client.call(
        "update_element",
        node=replacement,
        deck_id=deck_id,
        session_id=session_id,
        element_id=element_id,
        fencing_token=1,
        expected_version=1,
        patch={"content": "Committed by majority"},
    ), "SAVED")
    observed = client.call("get_element", node=replacement, deck_id=deck_id, element_id=element_id)
    if observed.get("result", {}).get("element", {}).get("configuration", {}).get("content") != "Committed by majority":
        raise RuntimeError(f"majority-side read did not observe progress: {observed}")

    chaos.main(["heal"])
    append_evidence(probes, "partition_healed", replacement_leader=replacement)
    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element = final["slide"]["elements"][0]
    if element["version"] != 2 or element["configuration"]["content"] != "Committed by majority":
        raise RuntimeError(f"healed cluster did not converge to majority state: {final}")
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"update_element"},
        read_operations={"get_element"},
        unavailability_seconds=unavailability_seconds,
        extra_evidence=[probes.name],
    )
    print("final", final)


if __name__ == "__main__":
    main()
