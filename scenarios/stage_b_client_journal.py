"""Live Stage B client-journal crash, offline, polling, and reconciliation evidence."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chaos
import cluster
from client import Client
from client_journal import JournaledClient, Lifecycle
from stage_a_support import wait_for_leader
from stage_b_support import (
    append_evidence,
    published_deck,
    recorded_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_presentation_catch_up,
    write_checker_evidence,
    write_checker_projection,
    write_latency_summary,
    write_manifest,
)


class SimulatedClientCrash(BaseException):
    """Abrupt process loss that bypasses normal transport error recovery."""


def main() -> None:
    scenario = "stage_b_client_journal"
    journal_path = Path("run") / f"{scenario}.jsonl"
    server_history = Path("run") / f"{scenario}_server_history.jsonl"
    checker_history = Path("run") / f"{scenario}_checker_history.jsonl"
    checker_projection = Path("run") / f"{scenario}_checker_projection.jsonl"
    checker_path = Path("run") / f"{scenario}_checker.jsonl"
    evidence_path = Path("run") / f"{scenario}_evidence.jsonl"
    metrics_path = Path("run") / f"{scenario}_metrics.jsonl"
    manifest_path = Path("run") / f"{scenario}_manifest.jsonl"
    final_history = Path("run") / f"{scenario}_final.jsonl"
    logical_client = "stage-b-journal-client"
    config = cluster.load()
    leader = wait_for_leader(config)
    lagging_follower = next(slot for slot in config["names"] if slot != leader)
    deck_id = stable_uuid("journal:deck")
    editing_slide = stable_uuid("journal:slide:editing")
    deleted_slide = stable_uuid("journal:slide:deleted")
    element_id = stable_uuid("journal:element:offline")
    superseded_element = stable_uuid("journal:element:superseded")
    session_id = stable_uuid("journal:session")
    transport = {"offline": False, "crash_after_publication_send": True}

    def request_sender(operation: str, arguments: dict, request_id: str, node: str) -> dict:
        if transport["offline"]:
            return {"ok": False, "error": "simulated_offline_client"}
        response = recorded_request(
            logical_client,
            server_history,
            request_id,
            operation,
            arguments,
            node=node,
        )
        if request_id == "journal-publication" and transport["crash_after_publication_send"]:
            transport["crash_after_publication_send"] = False
            append_evidence(
                evidence_path,
                "client_crashed_after_server_send_before_response",
                request_id=request_id,
                server_response=response,
            )
            raise SimulatedClientCrash("process terminated before response handling")
        return response

    journal = JournaledClient(
        logical_client,
        journal_path,
        session_id,
        request_sender=request_sender,
        local_replica=lagging_follower,
        leader_replica=leader,
    )
    journal.enqueue(
        "publish_deck",
        published_deck(deck_id, [editing_slide, deleted_slide], name="Stage B journal recovery"),
        request_id="journal-publication",
    )
    try:
        journal.synchronize()
    except SimulatedClientCrash:
        pass
    else:
        raise RuntimeError("scenario did not interrupt the client before response handling")
    if journal.outbox.get("journal-publication").lifecycle is not Lifecycle.IN_FLIGHT:
        raise RuntimeError("abrupt client crash did not leave a durable in-flight intention")

    restarted = JournaledClient(
        logical_client,
        journal_path,
        session_id,
        request_sender=request_sender,
        local_replica=lagging_follower,
        leader_replica=leader,
    )
    restarted.synchronize()
    if restarted.outbox.get("journal-publication").lifecycle is not Lifecycle.COMMITTED:
        raise RuntimeError("restarted client did not retry the original publication ID")
    append_evidence(
        evidence_path,
        "client_restart_exact_id_recovery",
        intention=restarted.outbox.get("journal-publication").to_json(),
    )

    transport["offline"] = True
    restarted.enqueue(
        "create_element",
        {
            "deck_id": deck_id,
            "slide_id": editing_slide,
            "element": text_element(element_id, "Offline element"),
        },
        request_id="offline-create",
    )
    restarted.enqueue(
        "update_element",
        {
            "deck_id": deck_id,
            "element_id": element_id,
            "patch": {"content": "Offline dependent draft"},
        },
        request_id="offline-dependent-update",
    )
    restarted.synchronize()
    pending_creation = restarted.outbox.speculative_projection["pending_element_creations"]
    if pending_creation[0]["element_id"] != element_id:
        raise RuntimeError("offline creation was not retained as pending local work")
    append_evidence(
        evidence_path,
        "offline_creation_pending_without_invented_token",
        creation=restarted.outbox.get("offline-create").to_json(),
        dependent_update=restarted.outbox.get("offline-dependent-update").to_json(),
        speculative_projection=restarted.outbox.speculative_projection,
    )
    transport["offline"] = False
    restarted.synchronize()
    if restarted.outbox.get("offline-dependent-update").lifecycle is not Lifecycle.COMMITTED:
        raise RuntimeError("offline dependent update did not wait for the authoritative creation token")

    transport["offline"] = True
    restarted.enqueue(
        "create_element",
        {
            "deck_id": deck_id,
            "slide_id": deleted_slide,
            "element": text_element(superseded_element, "Superseded child"),
        },
        request_id="superseded-create",
    )
    restarted.enqueue(
        "update_element",
        {
            "deck_id": deck_id,
            "element_id": superseded_element,
            "patch": {"content": "Must never be sent"},
        },
        request_id="superseded-update",
    )
    restarted.enqueue(
        "delete_slide",
        {
            "deck_id": deck_id,
            "slide_id": deleted_slide,
            "expected_deck_version": 0,
        },
        request_id="delete-with-supersession",
    )
    for request_id in ("superseded-create", "superseded-update"):
        if restarted.outbox.get(request_id).lifecycle is not Lifecycle.SUPERSEDED:
            raise RuntimeError(f"unsent descendant {request_id} was not superseded")
    append_evidence(
        evidence_path,
        "pending_slide_deletion_supersession",
        create=restarted.outbox.get("superseded-create").to_json(),
        update=restarted.outbox.get("superseded-update").to_json(),
        deletion=restarted.outbox.get("delete-with-supersession").to_json(),
    )
    transport["offline"] = False
    restarted.synchronize()
    if restarted.outbox.get("delete-with-supersession").lifecycle is not Lifecycle.COMMITTED:
        raise RuntimeError("slide deletion did not commit after superseding unsent descendants")

    remote = recorded_request(
        "stage-b-journal-remote",
        server_history,
        "journal-remote-update",
        "update_element",
        {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": 1,
            "expected_version": 2,
            "patch": {"content": "Remote version"},
        },
        node=leader,
    )
    require_status(remote, "SAVED")
    restarted.enqueue(
        "update_element",
        {
            "deck_id": deck_id,
            "element_id": element_id,
            "fencing_token": 1,
            "expected_version": 2,
            "patch": {"content": "Conflicting local draft"},
        },
        request_id="conflicting-draft",
    )
    restarted.synchronize()
    conflict = restarted.outbox.get("conflicting-draft")
    if conflict.lifecycle is not Lifecycle.NEEDS_MANUAL_RECONCILIATION:
        raise RuntimeError(f"version conflict did not require manual reconciliation: {conflict}")
    require_status(recorded_request(
        logical_client,
        server_history,
        "release-before-reconciliation",
        "release_lock",
        {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": 1,
        },
        node=leader,
    ), "RELEASED")
    reconciliation = restarted.prepare_reconciliation("conflicting-draft")
    if reconciliation["fencing_token"] != 2 or reconciliation["expected_version"] != 3:
        raise RuntimeError(f"reconciliation did not use fresh authority: {reconciliation}")
    restarted.submit_reconciled_update(
        "conflicting-draft",
        {"content": "User-corrected draft"},
        replacement_request_id="corrected-draft",
    )
    restarted.synchronize()
    if restarted.outbox.get("corrected-draft").lifecycle is not Lifecycle.COMMITTED:
        raise RuntimeError("user-corrected replacement did not commit")
    append_evidence(
        evidence_path,
        "manual_conflict_reconciliation",
        conflict=conflict.to_json(),
        reconciliation=reconciliation,
        corrected=restarted.outbox.get("corrected-draft").to_json(),
    )

    def poll_on(node: str):
        return lambda operation, arguments, request_id: recorded_request(
            logical_client,
            server_history,
            request_id,
            operation,
            arguments,
            node=node,
        )

    if not restarted.outbox.poll(deck_id, poll_on(leader), watched_element_ids=(element_id,)):
        raise RuntimeError("authoritative base poll failed before lagging-replica scenario")
    chaos.main(["partition", lagging_follower, "/", *[
        slot for slot in config["names"] if slot != lagging_follower
    ]])
    require_status(recorded_request(
        "stage-b-journal-remote",
        server_history,
        "journal-majority-update",
        "update_element",
        {
            "deck_id": deck_id,
            "session_id": session_id,
            "element_id": element_id,
            "fencing_token": 2,
            "expected_version": 4,
            "patch": {"content": "Majority version while follower lags"},
        },
        node=leader,
    ), "SAVED")
    if not restarted.outbox.poll(deck_id, poll_on(leader), watched_element_ids=(element_id,)):
        raise RuntimeError("majority projection did not advance")
    highest_before_lagging_poll = restarted.outbox.highest_applied_index
    restarted.enqueue(
        "update_element",
        {
            "deck_id": deck_id,
            "element_id": element_id,
            "fencing_token": 2,
            "expected_version": 5,
            "patch": {"content": "Pending draft preserved across lagging poll"},
        },
        request_id="pending-during-lagging-poll",
    )
    accepted_lagging = restarted.outbox.poll(
        deck_id,
        poll_on(lagging_follower),
        watched_element_ids=(element_id,),
    )
    pending = restarted.outbox.get("pending-during-lagging-poll")
    if accepted_lagging or restarted.outbox.highest_applied_index != highest_before_lagging_poll:
        raise RuntimeError("lagging polling regressed the authoritative base projection")
    if pending.lifecycle is not Lifecycle.PENDING_SYNC:
        raise RuntimeError("lagging polling replaced or merged the pending local draft")
    append_evidence(
        evidence_path,
        "lagging_poll_rejected_without_overwriting_draft",
        lagging_replica=lagging_follower,
        highest_applied_index=highest_before_lagging_poll,
        pending=pending.to_json(),
    )

    chaos.main(["heal"])
    final_client = Client("stage-b-journal-final", final_history)
    final = wait_for_presentation_catch_up(final_client, deck_id, editing_slide)
    element = final["slide"]["elements"][0]
    if element["version"] != 5 or element["configuration"]["content"] != "Majority version while follower lags":
        raise RuntimeError(f"journal scenario did not converge to committed authority: {final}")
    projection_summary = write_checker_projection(
        server_history,
        checker_history,
        checker_projection,
    )
    if (
        projection_summary["excluded_records"] != 1
        or projection_summary["normalized_records"] != 2
    ):
        raise RuntimeError(f"unexpected client-journal checker projection: {projection_summary}")
    write_checker_evidence(checker_history, checker_path)
    write_latency_summary(
        server_history,
        metrics_path,
        edit_operations={"update_element", "acquire_lock"},
        read_operations={"get_element", "get_deck_preview", "get_slide", "read_lock_status"},
    )
    write_manifest(
        manifest_path,
        scenario,
        journal=journal_path.name,
        server_history=server_history.name,
        checker_history=checker_history.name,
        checker_projection=checker_projection.name,
        checker=checker_path.name,
        final_history=final_history.name,
        client_journal_evidence=evidence_path.name,
        metrics=metrics_path.name,
    )
    print("final", final)


if __name__ == "__main__":
    main()
