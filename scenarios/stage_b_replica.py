"""Follower failure, majority editing, preserved-disk restart, and catch-up."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chaos
import cluster
from client import Client
from stage_a_support import wait_for_leader
from stage_b_support import (
    append_evidence,
    finalize_checked_scenario,
    published_deck,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_b_replica"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    client = Client("stage-b-replica-editor", history)
    config = cluster.load()
    leader = wait_for_leader(config)
    follower = next(slot for slot in config["names"] if slot != leader)
    deck_id = stable_uuid("replica:deck")
    slide_id = stable_uuid("replica:slide")
    element_id = stable_uuid("replica:element")
    session_id = stable_uuid("replica:session")

    require_status(client.call(
        "publish_deck",
        **published_deck(deck_id, [slide_id], name="Stage B follower recovery"),
    ), "PUBLISHED")
    require_status(client.call(
        "create_element",
        deck_id=deck_id,
        session_id=session_id,
        slide_id=slide_id,
        element=text_element(element_id, "Before follower failure"),
    ), "CREATED_AND_LOCKED")

    chaos.main(["kill", follower])
    append_evidence(probes, "follower_killed", leader=leader, follower=follower)
    for expected_version in range(1, 4):
        require_status(client.call(
            "update_element",
            node=leader,
            deck_id=deck_id,
            session_id=session_id,
            element_id=element_id,
            fencing_token=1,
            expected_version=expected_version,
            patch={"content": f"Committed with follower down {expected_version}"},
        ), "SAVED")
        observed = client.call("get_element", node=leader, deck_id=deck_id, element_id=element_id)
        if observed.get("result", {}).get("status") != "OK":
            raise RuntimeError(f"majority-side observation failed: {observed}")

    chaos.main(["start", follower])
    append_evidence(probes, "follower_restarted_with_preserved_disk", follower=follower)
    final = wait_for_presentation_catch_up(client, deck_id, slide_id)
    element = final["slide"]["elements"][0]
    if element["version"] != 4 or element["configuration"]["content"] != "Committed with follower down 3":
        raise RuntimeError(f"restarted follower did not catch up: {final}")
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"update_element"},
        read_operations={"get_element"},
        extra_evidence=[probes.name],
    )
    print("final", final)


if __name__ == "__main__":
    main()
