"""Competing same-element locks and concurrent distinct-element edits."""

from __future__ import annotations

from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import cluster
from stage_a_support import wait_for_leader
from stage_b_support import (
    ScenarioClient,
    append_evidence,
    finalize_checked_scenario,
    owned_deck,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
)


def main() -> None:
    scenario = "stage_b_concurrency"
    history = Path("run") / f"{scenario}.jsonl"
    probes = Path("run") / f"{scenario}_probes.jsonl"
    clients = {
        "sofia": ScenarioClient(stable_uuid("stage-b-concurrency-sofia"), history, login=True),
        "santiago": ScenarioClient(stable_uuid("stage-b-concurrency-santiago"), history, login=True),
    }
    sessions = {
        "sofia": stable_uuid("concurrency:session:sofia"),
        "santiago": stable_uuid("concurrency:session:santiago"),
    }
    deck_id = stable_uuid("concurrency:deck")
    slide_id = stable_uuid("concurrency:slide")
    first_element = stable_uuid("concurrency:element:first")
    second_element = stable_uuid("concurrency:element:second")
    leader = wait_for_leader(cluster.load())

    require_status(
        clients["sofia"].call(
            "publish_deck",
            **owned_deck(deck_id, [slide_id], name="Stage B concurrency"),
        ),
        "PUBLISHED",
    )
    require_status(clients["sofia"].call(
        "create_element",
        deck_id=deck_id,
        session_id=sessions["sofia"],
        slide_id=slide_id,
        element=text_element(first_element, "Contended", z_index=0),
    ), "CREATED_AND_LOCKED")
    require_status(clients["sofia"].call(
        "release_lock",
        deck_id=deck_id,
        session_id=sessions["sofia"],
        element_id=first_element,
        fencing_token=1,
    ), "RELEASED")

    barrier = threading.Barrier(3)
    acquisitions: dict[str, dict] = {}

    def acquire(editor: str) -> None:
        barrier.wait()
        acquisitions[editor] = clients[editor].call(
            "acquire_lock",
            deck_id=deck_id,
            session_id=sessions[editor],
            element_id=first_element,
        )

    threads = [threading.Thread(target=acquire, args=(editor,)) for editor in clients]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    statuses = {editor: response.get("result", {}).get("status") for editor, response in acquisitions.items()}
    if sorted(statuses.values()) != ["LOCKED", "LOCK_ACQUIRED"]:
        raise RuntimeError(f"competing acquisitions did not preserve exclusion: {acquisitions}")
    winner = next(editor for editor, status in statuses.items() if status == "LOCK_ACQUIRED")
    other = next(editor for editor in clients if editor != winner)
    winner_token = acquisitions[winner]["result"]["value"]["fencing_token"]
    append_evidence(probes, "competing_lock_result", winner=winner, responses=acquisitions)

    require_status(clients[other].call(
        "create_element",
        deck_id=deck_id,
        session_id=sessions[other],
        slide_id=slide_id,
        element=text_element(second_element, "Independent", x=500, z_index=1),
    ), "CREATED_AND_LOCKED")

    barrier = threading.Barrier(3)
    updates: dict[str, dict] = {}

    def update_first() -> None:
        barrier.wait()
        updates["first"] = clients[winner].call(
            "update_element",
            deck_id=deck_id,
            session_id=sessions[winner],
            element_id=first_element,
            fencing_token=winner_token,
            expected_version=1,
            patch={"content": f"Edited by {winner}"},
        )

    def update_second() -> None:
        barrier.wait()
        updates["second"] = clients[other].call(
            "update_element",
            deck_id=deck_id,
            session_id=sessions[other],
            element_id=second_element,
            fencing_token=1,
            expected_version=1,
            patch={"content": f"Edited by {other}"},
        )

    update_threads = [threading.Thread(target=update_first), threading.Thread(target=update_second)]
    for thread in update_threads:
        thread.start()
    barrier.wait()
    for thread in update_threads:
        thread.join()
    for response in updates.values():
        require_status(response, "SAVED")
    append_evidence(probes, "distinct_element_updates", responses=updates)

    for element_id in (first_element, second_element):
        observed = clients["sofia"].call("get_element", node=leader, deck_id=deck_id, element_id=element_id)
        if observed.get("result", {}).get("status") != "OK":
            raise RuntimeError(f"element observation failed: {observed}")
    final = wait_for_presentation_catch_up(clients["sofia"], deck_id, slide_id)
    if [element["version"] for element in final["slide"]["elements"]] != [2, 2]:
        raise RuntimeError(f"distinct element edits did not converge: {final}")
    finalize_checked_scenario(
        scenario,
        history,
        edit_operations={"acquire_lock", "update_element"},
        read_operations={"get_element"},
        extra_evidence=[probes.name],
    )
    print("final", final)


if __name__ == "__main__":
    main()
