"""Stage E.A committed login response loss and same-client reauthentication."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import chaos
import cluster
from authenticated_client import AuthenticatedClient
from client_journal import ClientIntentionOutbox, Lifecycle
from secret_scanner import scan_artifacts
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, assert_all_replicas_deck_settings,
    assert_all_replicas_slide_settings, published_deck, stable_uuid,
    wait_for_all_replicas_applied_equal, write_checker_evidence, write_manifest,
)
from stage_e_support import (
    durable_issuance_evidence, lose_committed_login_response,
    write_auth_checker_projection, write_auth_latencies, write_auth_summaries,
)


def _auth(run: Path, slot: str) -> dict:
    return json.loads((run / slot / "disk.json").read_text())["raft_state"]["fsm"]["auth"]


def main() -> None:
    scenario = "stage_e_lost_login"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    credentials = run / f"{scenario}_credentials.jsonl"
    durable_proof = run / f"{scenario}_durable_issuance.jsonl"
    archived_journal = run / f"{scenario}_intentions.jsonl"
    checker_history = run / f"{scenario}_checker_history.jsonl"
    checker_audit = run / f"{scenario}_projection_audit.jsonl"
    checker_result = run / f"{scenario}_checker.jsonl"
    metrics = run / f"{scenario}_metrics.jsonl"
    auth_summaries = run / f"{scenario}_auth_summaries.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    nodes = config["host_nodes"]
    old_leader = wait_for_leader(config)
    followers = [slot for slot in config["names"] if slot != old_leader]
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    business_id = f"{run_id}:publish-intention"
    lost_login_id = f"{run_id}:lost-login"
    recovery_login_id = f"{run_id}:recovery-login"

    with TemporaryDirectory(prefix="stage-e-lost-login-client-") as private_dir:
        client_dir = Path(private_dir) / "installation"
        journal_path = Path(private_dir) / "intentions.jsonl"
        first_client = AuthenticatedClient(client_dir, {old_leader: nodes[old_leader]}, history)
        original_client_id = first_client.client_id
        outbox = ClientIntentionOutbox(original_client_id, journal_path,
                                      stable_uuid(f"{run_id}:session"))
        outbox.enqueue("publish_deck", {
            "deck_id": deck_id,
            **published_deck(deck_id, [slide_id], name="Stage E lost login"),
        }, request_id=business_id)
        assert outbox.get(business_id).lifecycle is Lifecycle.PENDING_SYNC
        outbox.synchronize(lambda intention: first_client.call(
            intention.request_id, intention.operation, intention.arguments,
            node=old_leader,
        ))
        assert outbox.get(business_id).lifecycle is Lifecycle.PENDING_SYNC
        append_evidence(probes, "business_intention_waiting_for_auth",
                        client_id=original_client_id, request_id=business_id)

        committed: dict = {}

        def on_committed(credential_id: str, generation: int, digest: str) -> None:
            assert generation == 1
            deadline = time.monotonic() + 15.0
            while True:
                for follower in followers:
                    try:
                        proof = durable_issuance_evidence(
                            run, (old_leader, follower), original_client_id,
                            lost_login_id, credential_id,
                        )
                    except (AssertionError, OSError, ValueError):
                        continue
                    assert proof[0]["token_digest"] == digest
                    committed.update(credential_id=credential_id, digest=digest,
                                     index=proof[0]["committed_index"], voter=follower)
                    for item in proof:
                        append_evidence(durable_proof, "lost_response_issuance_durable", **item)
                    append_evidence(faults, "response_dropped_after_durable_commit",
                                    leader=old_leader, surviving_voter=follower,
                                    issuance_index=committed["index"])
                    chaos.main(["kill", old_leader])
                    append_evidence(faults, "issuing_leader_stopped_before_client_install",
                                    leader=old_leader, issuance_index=committed["index"])
                    return
                if time.monotonic() >= deadline:
                    raise AssertionError("issuance was not durably applied on a majority")
                time.sleep(0.1)

        lost_response, lost_secret = lose_committed_login_response(
            first_client, lost_login_id, "sofia@example.test", "Sofia-Classroom-1",
            on_committed,
        )
        assert lost_response == {"ok": False, "error": "ConnectionError"}
        assert not (client_dir / "credential.json").exists(), "lost bearer reached client credential storage"
        assert committed, "response-loss boundary did not observe committed issuance"
        append_evidence(credentials, "issued_but_secret_lost", client_id=original_client_id,
                        request_id=lost_login_id, credential_id=committed["credential_id"],
                        generation=1, token_digest=committed["digest"])

        election_started = time.monotonic()
        new_leader = wait_for_leader(config, exclude={old_leader})
        election_s = time.monotonic() - election_started
        append_evidence(faults, "replacement_leader_elected", old_leader=old_leader,
                        new_leader=new_leader, unavailability_seconds=round(election_s, 3))
        recovered_client = AuthenticatedClient(client_dir, {new_leader: nodes[new_leader]}, history)
        assert recovered_client.client_id == original_client_id
        assert not (client_dir / "credential.json").exists()
        old_attempt = recovered_client.login(
            lost_login_id, "sofia@example.test", "Sofia-Classroom-1",
        )
        assert old_attempt.get("ok") is False
        assert old_attempt.get("error") in {"LOGIN_SECRET_UNAVAILABLE", "REAUTH_REQUIRED"}, old_attempt
        assert not (client_dir / "credential.json").exists()
        append_evidence(probes, "old_login_secret_unavailable", request_id=lost_login_id,
                        error=old_attempt["error"], client_id=original_client_id)

        recovery = recovered_client.login(
            recovery_login_id, "sofia@example.test", "Sofia-Classroom-1",
        )
        result = recovery.get("result") if isinstance(recovery, dict) else None
        assert recovery.get("ok") is True and isinstance(result, dict)
        assert result.get("status") == "AUTHENTICATED", recovery
        assert result["client_id"] == original_client_id
        assert result["credential_generation"] == 2
        assert result["credential_id"] != committed["credential_id"]
        fresh_secret = result["access_token"]
        assert fresh_secret != lost_secret
        append_evidence(credentials, "same_client_reauthenticated", client_id=original_client_id,
                        request_id=recovery_login_id, credential_id=result["credential_id"],
                        generation=2)

        replayed = ClientIntentionOutbox(original_client_id, journal_path,
                                         stable_uuid(f"{run_id}:session"))
        assert replayed.get(business_id).request_id == business_id
        assert replayed.get(business_id).lifecycle is Lifecycle.PENDING_SYNC
        replayed.synchronize(lambda intention: recovered_client.call(
            intention.request_id, intention.operation, intention.arguments,
            node=new_leader,
        ))
        assert replayed.get(business_id).lifecycle is Lifecycle.COMMITTED
        assert replayed.get(business_id).request_id == business_id
        shutil.copy2(journal_path, archived_journal)
        append_evidence(probes, "original_business_id_committed_after_reauth",
                        request_id=business_id, client_id=original_client_id,
                        lifecycle=replayed.get(business_id).lifecycle.value)

        chaos.main(["start", old_leader])
        append_evidence(faults, "former_leader_restarted_with_preserved_disk", slot=old_leader)
        applied = wait_for_all_replicas_applied_equal()
        assert_all_replicas_deck_settings(nodes, deck_id,
                                          expected_deck_settings_version=0,
                                          expected_name="Stage E lost login")
        assert_all_replicas_slide_settings(nodes, deck_id, slide_id,
                                           expected_slide_settings_version=0,
                                           expected_background="#112233FF")
        summaries = write_auth_summaries(run, auth_summaries, original_client_id)
        assert all(item["credential_generation"] == 2 and not item["revoked"] for item in summaries)
        assert all(item["credential_id"] == result["credential_id"] for item in summaries)
        assert all(item["bound_user_id"] == result["user_id"] for item in summaries)
        for slot in config["names"]:
            retired = _auth(run, slot)["retired_credentials"][committed["credential_id"]]
            assert retired["token_digest"] == committed["digest"]
            assert retired["credential_generation"] == 1
            assert retired["retirement_reason"] == "ROTATED"
        append_evidence(probes, "old_credential_retired_and_replicas_converged",
                        old_credential_id=committed["credential_id"],
                        new_credential_id=result["credential_id"],
                        generation=2, applied=applied)

        write_auth_checker_projection(history, checker_history, checker_audit)
        write_checker_evidence(checker_history, checker_result, run_dir=run)
        write_auth_latencies(history, metrics, events_dir=run)
        append_evidence(metrics, "election_unavailability", duration_seconds=round(election_s, 3))
        write_manifest(manifest, scenario, run_id=run_id, client_id=original_client_id,
                       history=history.name, intention_journal=archived_journal.name,
                       checker_history=checker_history.name, checker=checker_result.name,
                       checker_audit=checker_audit.name, probes=probes.name,
                       credential_trace=credentials.name, durable_issuance=durable_proof.name,
                       auth_summaries=auth_summaries.name, metrics=metrics.name,
                       faults=faults.name, role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        leaks = scan_artifacts(run, ["Sofia-Classroom-1", lost_secret, fresh_secret])
        assert not leaks, f"literal secret appeared in durable or archived run inputs: {leaks}"
        append_evidence(probes, "literal_secret_scan_clean", scanned_tree="run", findings=0)

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
