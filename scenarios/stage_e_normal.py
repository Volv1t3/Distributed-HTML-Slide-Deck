"""Stage E.A normal authenticated command scenario on one clean cluster."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cluster
from authenticated_client import AuthenticatedClient
from secret_scanner import scan_artifacts
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, assert_all_replicas_deck_settings, assert_all_replicas_lock_state,
    assert_all_replicas_slide_settings, published_deck, require_status, stable_uuid,
    text_element, wait_for_all_replicas_applied_equal, write_checker_evidence,
    write_manifest,
)
from stage_e_support import write_auth_checker_projection, write_auth_latencies, write_auth_summaries


def main() -> None:
    scenario = "stage_e_normal"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    credential_trace = run / f"{scenario}_credentials.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    checker_history = run / f"{scenario}_checker_history.jsonl"
    checker_audit = run / f"{scenario}_projection_audit.jsonl"
    checker_result = run / f"{scenario}_checker.jsonl"
    metrics = run / f"{scenario}_metrics.jsonl"
    auth_summaries = run / f"{scenario}_auth_summaries.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    leader = wait_for_leader(config)
    nodes = config["host_nodes"]
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    element_id = stable_uuid(f"{run_id}:element")
    session_id = stable_uuid(f"{run_id}:session")

    with TemporaryDirectory(prefix="stage-e-normal-client-") as private_dir:
        client = AuthenticatedClient(private_dir, nodes, history)
        login_id = f"{run_id}:login"
        login = require_status(client.login(login_id, "sofia@example.test", "Sofia-Classroom-1"), "AUTHENTICATED")
        assert login["client_id"] == client.client_id, login
        assert login["user_id"] == "11111111-1111-4111-8111-111111111111", login
        assert login["credential_generation"] == 1, login
        assert login["access_token"], "login did not return a usable bearer"
        append_evidence(credential_trace, "credential_issued", client_id=client.client_id,
                        user_id=login["user_id"], credential_id=login["credential_id"],
                        credential_generation=login["credential_generation"], request_id=login_id)

        def command(name: str, op: str, args: dict, status: str) -> dict:
            request_id = f"{run_id}:{name}"
            return require_status(client.call(request_id, op, args), status)

        command("publish", "publish_deck", {
            "deck_id": deck_id,
            **published_deck(deck_id, [slide_id], name="Stage E normal"),
        }, "PUBLISHED")
        created = command("create", "create_element", {
            "deck_id": deck_id, "slide_id": slide_id, "session_id": session_id,
            "element": text_element(element_id, "Authenticated edit"),
        }, "CREATED_AND_LOCKED")
        fencing = created["value"]["fencing_token"]
        epoch = created["value"]["lease_epoch"]
        version = created["value"]["element_version"]
        command("wrong-session", "update_element", {
            "deck_id": deck_id, "element_id": element_id,
            "session_id": stable_uuid(f"{run_id}:other-session"),
            "fencing_token": fencing, "expected_version": version,
            "patch": {"content": "Wrong editor session"},
        }, "NOT_OWNER")
        updated = command("edit", "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": fencing, "expected_version": version,
            "patch": {"content": "Authenticated and fenced"},
        }, "SAVED")
        assert updated["value"]["element_version"] == version + 1, updated
        conflict = command("edit-conflict", "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": fencing, "expected_version": version,
            "patch": {"content": "Stale"},
        }, "VERSION_CONFLICT")
        assert conflict["value"]["current_version"] == version + 1, conflict

        deck = command("deck-settings", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0,
            "patch": {"name": "Stage E authenticated"},
        }, "DECK_SETTINGS_UPDATED")
        assert deck["value"]["deck_settings_version"] == 1, deck
        deck_conflict = command("deck-settings-conflict", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0,
            "patch": {"name": "Stale name"},
        }, "DECK_SETTINGS_VERSION_CONFLICT")
        assert deck_conflict["value"]["deck_settings_version"] == 1, deck_conflict
        slide = command("slide-settings", "update_slide_settings", {
            "deck_id": deck_id, "slide_id": slide_id, "expected_slide_settings_version": 0,
            "patch": {"background": "#00FF00FF"},
        }, "SLIDE_SETTINGS_UPDATED")
        assert slide["value"]["slide_settings_version"] == 1, slide
        slide_conflict = command("slide-settings-conflict", "update_slide_settings", {
            "deck_id": deck_id, "slide_id": slide_id, "expected_slide_settings_version": 0,
            "patch": {"background": "#FF0000FF"},
        }, "SLIDE_SETTINGS_VERSION_CONFLICT")
        assert slide_conflict["value"]["slide_settings_version"] == 1, slide_conflict

        applied = wait_for_all_replicas_applied_equal()
        assert_all_replicas_deck_settings(nodes, deck_id,
                                          expected_deck_settings_version=1,
                                          expected_name="Stage E authenticated")
        assert_all_replicas_slide_settings(nodes, deck_id, slide_id,
                                           expected_slide_settings_version=1,
                                           expected_background="#00FF00FF")
        assert_all_replicas_lock_state(nodes, deck_id, element_id,
                                       expected_fencing=fencing, expected_epoch=epoch)
        summaries = write_auth_summaries(run, auth_summaries, client.client_id)
        assert summaries[0]["credential_id"] == login["credential_id"]
        append_evidence(probes, "normal_auth_converged", run_id=run_id, leader=leader,
                        client_id=client.client_id, login_request_id=login_id,
                        applied=applied, deck_id=deck_id, element_id=element_id)

        write_auth_checker_projection(history, checker_history, checker_audit)
        write_checker_evidence(checker_history, checker_result, run_dir=run)
        write_auth_latencies(history, metrics, events_dir=run)
        append_evidence(faults, "fault_trace", injections=[], runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, client_id=client.client_id,
                       history=history.name, checker_history=checker_history.name,
                       checker=checker_result.name, checker_audit=checker_audit.name,
                       probes=probes.name, credential_trace=credential_trace.name,
                       auth_summaries=auth_summaries.name, metrics=metrics.name,
                       faults=faults.name, role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        leaks = scan_artifacts(run, ["Sofia-Classroom-1", login["access_token"]])
        assert not leaks, f"secret literal appeared in archived run inputs: {leaks}"

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
