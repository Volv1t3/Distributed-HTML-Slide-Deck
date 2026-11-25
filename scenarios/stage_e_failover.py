"""Stage E.A credential durability across follower lag and leader failover."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
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
from secret_scanner import scan_artifacts
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, assert_all_replicas_deck_settings,
    assert_all_replicas_slide_settings, published_deck, require_status,
    stable_uuid, wait_for_all_replicas_applied_equal, write_checker_evidence,
    write_manifest,
)
from stage_e_support import (
    durable_issuance_evidence, write_auth_checker_projection,
    write_auth_latencies, write_auth_summaries,
)


def _disk(run: Path, slot: str) -> dict:
    return json.loads((run / slot / "disk.json").read_text())["raft_state"]


def main() -> None:
    scenario = "stage_e_failover"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    credentials = run / f"{scenario}_credentials.jsonl"
    durable_proof = run / f"{scenario}_durable_issuance.jsonl"
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
    lagging = "n2" if "n2" in followers else followers[0]
    survivor = next(slot for slot in followers if slot != lagging)
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")

    # Preserve the follower's disk and prove it has not seen this client binding.
    chaos.main(["kill", lagging])
    lagging_before = _disk(run, lagging)
    lagging_uuid = lagging_before["node_uuid"]
    lagging_index = lagging_before["raft"]["last_applied"]
    append_evidence(faults, "follower_stopped_before_issuance", slot=lagging,
                    node_uuid=lagging_uuid, last_applied=lagging_index)

    with TemporaryDirectory(prefix="stage-e-failover-client-") as private_dir:
        client = AuthenticatedClient(private_dir, {
            old_leader: nodes[old_leader], survivor: nodes[survivor],
        }, history)
        assert client.client_id not in lagging_before["fsm"]["auth"]["active_credentials"]
        login_id = f"{run_id}:login"
        issued = require_status(client.login(login_id, "sofia@example.test", "Sofia-Classroom-1"), "AUTHENTICATED")
        token = issued["access_token"]
        assert issued["client_id"] == client.client_id
        assert issued["credential_generation"] == 1
        append_evidence(credentials, "credential_issued", client_id=client.client_id,
                        request_id=login_id, credential_id=issued["credential_id"], generation=1)

        deadline = time.monotonic() + 15.0
        while True:
            try:
                proof = durable_issuance_evidence(
                    run, (old_leader, survivor), client.client_id, login_id,
                    issued["credential_id"],
                )
                break
            except (AssertionError, OSError, ValueError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        assert all(item["credential_generation"] == 1 and not item["revoked"] for item in proof)
        assert proof[0]["token_digest"] == hashlib.sha256(token.encode()).hexdigest()
        for item in proof:
            append_evidence(durable_proof, "durable_issuance", **item)
        assert client.client_id not in _disk(run, lagging)["fsm"]["auth"]["active_credentials"]
        append_evidence(probes, "lagging_follower_missing_issuance", slot=lagging,
                        last_applied=lagging_index, issuance_index=proof[0]["committed_index"])

        election_started = time.monotonic()
        chaos.main(["kill", old_leader])
        append_evidence(faults, "issuing_leader_stopped", slot=old_leader,
                        issuance_index=proof[0]["committed_index"])
        chaos.main(["start", lagging])
        recovered = _disk(run, lagging)
        assert recovered["node_uuid"] == lagging_uuid, "lagging follower did not retain its disk identity"
        assert recovered["incarnation"] > lagging_before["incarnation"]
        append_evidence(faults, "lagging_follower_restarted_with_preserved_disk",
                        slot=lagging, node_uuid=lagging_uuid,
                        old_incarnation=lagging_before["incarnation"],
                        new_incarnation=recovered["incarnation"])
        new_leader = wait_for_leader(config, exclude={old_leader})
        assert new_leader == survivor, (new_leader, survivor)
        unavailable_s = time.monotonic() - election_started
        append_evidence(faults, "up_to_date_voter_elected", slot=new_leader,
                        unavailability_seconds=round(unavailable_s, 3))

        # A fresh admission read on the replacement leader validates the
        # credential that was already durably applied before the old leader died.
        publication = require_status(client.call(f"{run_id}:publish", "publish_deck", {
            "deck_id": deck_id,
            **published_deck(deck_id, [slide_id], name="Stage E failover"),
        }, node=new_leader), "PUBLISHED")
        assert publication["status"] == "PUBLISHED"
        settings = require_status(client.call(f"{run_id}:settings", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0,
            "patch": {"name": "Credential survived failover"},
        }, node=new_leader), "DECK_SETTINGS_UPDATED")
        assert settings["value"]["deck_settings_version"] == 1
        append_evidence(probes, "replacement_authenticated_protected_write",
                        old_leader=old_leader, new_leader=new_leader,
                        client_id=client.client_id, credential_id=issued["credential_id"],
                        deck_id=deck_id)

        chaos.main(["start", old_leader])
        append_evidence(faults, "issuing_leader_restarted_with_preserved_disk", slot=old_leader)
        applied = wait_for_all_replicas_applied_equal()
        observations = assert_all_replicas_deck_settings(
            nodes, deck_id, expected_deck_settings_version=1,
            expected_name="Credential survived failover",
        )
        assert_all_replicas_slide_settings(
            nodes, deck_id, slide_id, expected_slide_settings_version=0,
            expected_background="#112233FF",
        )
        summaries = write_auth_summaries(run, auth_summaries, client.client_id)
        assert all(item["bound_user_id"] == issued["user_id"] for item in summaries)
        assert all(item["credential_user_id"] == issued["user_id"] for item in summaries)
        assert all(item["credential_id"] == issued["credential_id"] for item in summaries)
        assert all(item["credential_generation"] == 1 and not item["revoked"] for item in summaries)
        assert all(item["token_digest"] == proof[0]["token_digest"] for item in summaries)
        assert applied[lagging] > lagging_index, (applied, lagging_index)
        append_evidence(probes, "all_replicas_converged", applied=applied,
                        lagging_follower=lagging, deck_id=deck_id,
                        deck_settings_version=1, replica_count=len(observations))

        write_auth_checker_projection(history, checker_history, checker_audit)
        write_checker_evidence(checker_history, checker_result, run_dir=run)
        write_auth_latencies(history, metrics, events_dir=run)
        append_evidence(metrics, "election_unavailability", duration_seconds=round(unavailable_s, 3))
        write_manifest(manifest, scenario, run_id=run_id, client_id=client.client_id,
                       history=history.name, checker_history=checker_history.name,
                       checker=checker_result.name, checker_audit=checker_audit.name,
                       probes=probes.name, credential_trace=credentials.name,
                       durable_issuance=durable_proof.name, auth_summaries=auth_summaries.name,
                       metrics=metrics.name, faults=faults.name,
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        leaks = scan_artifacts(run, ["Sofia-Classroom-1", token])
        assert not leaks, f"secret literal appeared in archived run inputs: {leaks}"

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
