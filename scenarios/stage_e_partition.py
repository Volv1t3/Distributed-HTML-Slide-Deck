"""Stage E.A quorum auth safety and both revoke/write Raft orders."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import threading
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chaos
import cluster
from cluster_support import raft_state
from authenticated_client import AuthenticatedClient
from secret_scanner import scan_artifacts
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, assert_all_replicas_deck_settings,
    assert_all_replicas_slide_settings, published_deck, require_status,
    stable_uuid, wait_for_all_replicas_applied_equal, wait_for_event,
    write_manifest,
)
from stage_e_support import (
    finalize_auth_evidence, write_auth_summaries,
    write_auth_order_reference, write_request_result_summaries,
)
from stage_b_support import SCENARIO_OWNER, owned_deck


def _entry_index(run: Path, slot: str, request_id: str) -> int:
    disk = raft_state(run, slot)
    entries = [entry for entry in disk["raft"]["log"]
               if entry["command"]["request_id"] == request_id]
    assert len(entries) == 1, (slot, request_id, entries)
    index = entries[0]["index"]
    assert disk["raft"]["commit_index"] >= index
    assert disk["raft"]["last_applied"] >= index
    return index


def _no_minority_ack(response: dict, label: str) -> None:
    assert response.get("ok") is not True, f"minority acknowledged {label}: {response}"
    assert response.get("redirect") or response.get("error"), response


def main() -> None:
    scenario = "stage_e_partition"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    credentials = run / f"{scenario}_credentials.jsonl"
    checker_history = run / f"{scenario}_checker_history.jsonl"
    checker_audit = run / f"{scenario}_projection_audit.jsonl"
    checker_result = run / f"{scenario}_checker.jsonl"
    metrics = run / f"{scenario}_metrics.jsonl"
    auth_a = run / f"{scenario}_client_a_auth_summaries.jsonl"
    auth_b = run / f"{scenario}_client_b_auth_summaries.jsonl"
    request_results = run / f"{scenario}_request_results.jsonl"
    auth_reference = run / f"{scenario}_auth_reference.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    nodes = config["host_nodes"]
    isolated = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != isolated]
    deck_a = stable_uuid(f"{run_id}:deck-a")
    slide_a = stable_uuid(f"{run_id}:slide-a")
    deck_b = stable_uuid(f"{run_id}:deck-b")
    slide_b = stable_uuid(f"{run_id}:slide-b")
    baseline_id = f"{run_id}:baseline"
    write_a_id = f"{run_id}:write-before-revoke"
    revoke_a_id = f"{run_id}:revoke-after-write"
    revoke_b_id = f"{run_id}:revoke-before-write"
    write_b_id = f"{run_id}:write-after-revoke"

    with TemporaryDirectory(prefix="stage-e-partition-clients-") as private_dir:
        client_a = AuthenticatedClient(Path(private_dir) / "a", nodes, history)
        login_a = require_status(client_a.login(
            f"{run_id}:login-a", "sofia@example.test", "Sofia-Classroom-1",
        ), "AUTHENTICATED")
        token_a = login_a["access_token"]
        assert login_a["credential_generation"] == 1
        append_evidence(credentials, "client_a_issued", client_id=client_a.client_id,
                        credential_id=login_a["credential_id"], generation=1)
        require_status(client_a.call(f"{run_id}:publish-a", "publish_deck", {
            "deck_id": deck_a, **owned_deck(deck_a, [slide_a], name="Stage E partition A", owner_user_id=SCENARIO_OWNER),
        }, node=isolated), "PUBLISHED")
        baseline_args = {
            "deck_id": deck_a, "expected_deck_settings_version": 0,
            "patch": {"name": "Before partition"},
        }
        require_status(client_a.call(baseline_id, "update_deck_settings", baseline_args,
                                     node=isolated), "DECK_SETTINGS_UPDATED")
        wait_for_all_replicas_applied_equal()

        chaos.main(["partition", isolated, "/", *majority])
        partition_started = time.monotonic()
        append_evidence(faults, "leader_isolated_as_minority", isolated=isolated,
                        majority=majority)
        majority_leader = wait_for_leader(config, exclude={isolated})
        election_s = time.monotonic() - partition_started
        append_evidence(faults, "majority_leader_elected", leader=majority_leader,
                        unavailability_seconds=round(election_s, 3))

        # Direct requests remain on the isolated transport slot. No redirect is
        # followed and no stale stored result may be disclosed there.
        minority_client = AuthenticatedClient(Path(private_dir) / "a",
                                              {isolated: nodes[isolated]}, history,
                                              timeout=3.0)
        minority_login = minority_client.login(
            f"{run_id}:minority-login", "sofia@example.test", "Sofia-Classroom-1",
        )
        _no_minority_ack(minority_login, "token issuance")
        append_evidence(probes, "minority_issuance_denied", response=minority_login)
        minority_revoke = minority_client.call(f"{run_id}:minority-revoke", "revoke_access_token", {
            "expected_credential_id": login_a["credential_id"],
            "expected_generation": 1,
        }, node=isolated, timeout=3.0)
        _no_minority_ack(minority_revoke, "token revocation")
        append_evidence(probes, "minority_revoke_denied", response=minority_revoke)
        minority_dedup = minority_client.call(baseline_id, "update_deck_settings",
                                            baseline_args, node=isolated, timeout=3.0)
        _no_minority_ack(minority_dedup, "authenticated stored result")
        append_evidence(probes, "minority_dedup_disclosure_denied", response=minority_dedup,
                        request_id=baseline_id)
        minority_write = minority_client.call(f"{run_id}:minority-write", "update_deck_settings", {
            "deck_id": deck_a, "expected_deck_settings_version": 1,
            "patch": {"name": "Minority cannot write"},
        }, node=isolated, timeout=3.0)
        _no_minority_ack(minority_write, "protected write")
        append_evidence(probes, "minority_protected_write_denied", response=minority_write)

        # Both requests overlap in flight. Waiting for the write's admission
        # read before starting revoke makes the write win the Raft log order.
        write_a_args = {
            "deck_id": deck_a, "expected_deck_settings_version": 1,
            "patch": {"name": "Write before revoke"},
        }
        revoke_a_args = {
            "expected_credential_id": login_a["credential_id"],
            "expected_generation": 1,
        }
        chaos.main(["delay", majority_leader, "200"])
        winning: dict[str, dict] = {}

        def send_winning_write() -> None:
            winning["write"] = client_a.call(write_a_id, "update_deck_settings",
                                             write_a_args, node=majority_leader, timeout=8.0)

        def send_later_revoke() -> None:
            winning["revoke"] = client_a.call(revoke_a_id, "revoke_access_token",
                                              revoke_a_args, node=majority_leader, timeout=8.0)

        write_a_thread = threading.Thread(target=send_winning_write)
        revoke_a_thread = threading.Thread(target=send_later_revoke)
        write_a_thread.start()
        wait_for_event(majority_leader, "auth_admitted_read_started",
                       request_id=write_a_id, timeout=2.0)
        revoke_a_thread.start()
        wait_for_event(majority_leader, "auth_admitted_read_started",
                       request_id=revoke_a_id, timeout=2.0)
        write_a_thread.join(12.0)
        revoke_a_thread.join(12.0)
        assert not write_a_thread.is_alive() and not revoke_a_thread.is_alive(), winning
        chaos.main(["delay", majority_leader, "0"])
        write_a = require_status(winning["write"], "DECK_SETTINGS_UPDATED")
        assert write_a["value"]["deck_settings_version"] == 2
        revoke_a = require_status(winning["revoke"], "TOKEN_REVOKED")
        assert revoke_a["value"]["credential_generation"] == 2
        first_write_index = _entry_index(run, majority_leader, write_a_id)
        first_revoke_index = _entry_index(run, majority_leader, revoke_a_id)
        assert first_write_index < first_revoke_index
        append_evidence(probes, "write_before_revoke_committed",
                        write_index=first_write_index, revoke_index=first_revoke_index,
                        deck_settings_version=2, requests_overlapped=True)

        # The majority can also issue a new credential while the old leader is
        # isolated. This independent client supplies the second ordering.
        client_b = AuthenticatedClient(Path(private_dir) / "b",
                                       {majority_leader: nodes[majority_leader]}, history)
        login_b = require_status(client_b.login(
            f"{run_id}:login-b", "sofia@example.test", "Sofia-Classroom-1",
        ), "AUTHENTICATED")
        token_b = login_b["access_token"]
        assert login_b["credential_generation"] == 1
        append_evidence(credentials, "majority_client_b_issued", client_id=client_b.client_id,
                        credential_id=login_b["credential_id"], generation=1)
        require_status(client_b.call(f"{run_id}:publish-b", "publish_deck", {
            "deck_id": deck_b, **owned_deck(deck_b, [slide_b], name="Stage E partition B", owner_user_id=SCENARIO_OWNER),
        }, node=majority_leader), "PUBLISHED")

        # Delay leader outbound Raft messages so both fresh Auth Admission
        # Reads can finish before either proposed entry commits. The leader's
        # serialized waiter order appends revoke first, then the protected write.
        chaos.main(["delay", majority_leader, "200"])
        concurrent: dict[str, dict] = {}
        revoke_b_args = {
            "expected_credential_id": login_b["credential_id"],
            "expected_generation": 1,
        }
        write_b_args = {
            "deck_id": deck_b, "expected_deck_settings_version": 0,
            "patch": {"name": "Must remain unchanged"},
        }

        def send_revoke() -> None:
            concurrent["revoke"] = client_b.call(
                revoke_b_id, "revoke_access_token", revoke_b_args,
                node=majority_leader, timeout=8.0,
            )

        def send_write() -> None:
            concurrent["write"] = client_b.call(
                write_b_id, "update_deck_settings", write_b_args,
                node=majority_leader, timeout=8.0,
            )

        revoke_thread = threading.Thread(target=send_revoke)
        write_thread = threading.Thread(target=send_write)
        revoke_thread.start()
        wait_for_event(majority_leader, "auth_admitted_read_started",
                       request_id=revoke_b_id, timeout=2.0)
        write_thread.start()
        wait_for_event(majority_leader, "auth_admitted_read_started",
                       request_id=write_b_id, timeout=2.0)
        revoke_thread.join(12.0)
        write_thread.join(12.0)
        assert not revoke_thread.is_alive() and not write_thread.is_alive(), concurrent
        chaos.main(["delay", majority_leader, "0"])
        revoke_b = require_status(concurrent["revoke"], "TOKEN_REVOKED")
        assert revoke_b["value"]["credential_generation"] == 2
        committed_denial = require_status(concurrent["write"], "UNAUTHENTICATED")
        assert committed_denial["status"] == "UNAUTHENTICATED"
        second_revoke_index = _entry_index(run, majority_leader, revoke_b_id)
        second_write_index = _entry_index(run, majority_leader, write_b_id)
        assert second_revoke_index < second_write_index
        append_evidence(probes, "revoke_before_write_committed_denial",
                        revoke_index=second_revoke_index, write_index=second_write_index,
                        status="UNAUTHENTICATED", deck_settings_version=0)

        chaos.main(["heal"])
        append_evidence(faults, "partition_healed", isolated=isolated,
                        majority=majority, runtime_faults=json.loads((run / "faults.json").read_text()))
        applied = wait_for_all_replicas_applied_equal()
        assert_all_replicas_deck_settings(nodes, deck_a,
                                          expected_deck_settings_version=2,
                                          expected_name="Write before revoke",
                                          expected_deck_version=0)
        assert_all_replicas_deck_settings(nodes, deck_b,
                                          expected_deck_settings_version=0,
                                          expected_name="Stage E partition B",
                                          expected_deck_version=0)
        assert_all_replicas_slide_settings(nodes, deck_a, slide_a,
                                           expected_slide_settings_version=0,
                                           expected_background="#112233FF")
        assert_all_replicas_slide_settings(nodes, deck_b, slide_b,
                                           expected_slide_settings_version=0,
                                           expected_background="#112233FF")
        summary_a = write_auth_summaries(run, auth_a, client_a.client_id)
        summary_b = write_auth_summaries(run, auth_b, client_b.client_id)
        assert all(item["credential_generation"] == 2 and item["revoked"] for item in summary_a)
        assert all(item["credential_generation"] == 2 and item["revoked"] for item in summary_b)
        results = write_request_result_summaries(run, request_results, [
            (client_a.client_id, baseline_id),
            (client_a.client_id, write_a_id),
            (client_a.client_id, revoke_a_id),
            (client_b.client_id, revoke_b_id),
            (client_b.client_id, write_b_id),
        ])
        denied_results = [item for item in results if item["request_id"] == write_b_id]
        assert len(denied_results) == 3
        assert all(item["result"]["status"] == "UNAUTHENTICATED" for item in denied_results)
        assert all(item["committed_index"] == second_write_index for item in denied_results)
        reference = write_auth_order_reference(run, auth_reference, majority_leader)
        by_request = {item["request_id"]: item for item in reference}
        assert by_request[write_a_id]["predicted_status"] == "DECK_SETTINGS_UPDATED"
        assert by_request[revoke_a_id]["predicted_status"] == "TOKEN_REVOKED"
        assert by_request[revoke_b_id]["predicted_status"] == "TOKEN_REVOKED"
        assert by_request[write_b_id]["predicted_status"] == "UNAUTHENTICATED"
        assert by_request[write_b_id]["deck_settings_version"] == 0
        append_evidence(probes, "healed_auth_results_and_presentation_converged",
                        applied=applied, request_result_count=len(results),
                        client_a_generation=2, client_b_generation=2,
                        deck_a_settings_version=2, deck_b_settings_version=0)

        finalize_auth_evidence(history, checker_history, checker_audit,
                               checker_result, metrics, run_dir=run)
        append_evidence(metrics, "majority_election_unavailability", duration_seconds=round(election_s, 3))
        write_manifest(manifest, scenario, run_id=run_id,
                       client_ids=[client_a.client_id, client_b.client_id],
                       history=history.name, checker_history=checker_history.name,
                       checker=checker_result.name, checker_audit=checker_audit.name,
                       probes=probes.name, credential_trace=credentials.name,
                       auth_summaries=[auth_a.name, auth_b.name],
                       request_results=request_results.name, auth_reference=auth_reference.name,
                       metrics=metrics.name,
                       faults=faults.name,
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        leaks = scan_artifacts(run, ["Sofia-Classroom-1", token_a, token_b])
        assert not leaks, f"literal secret appeared in durable or archived run inputs: {leaks}"
        append_evidence(probes, "literal_secret_scan_clean", scanned_tree="run", findings=0)

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
