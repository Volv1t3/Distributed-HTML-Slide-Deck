"""Stage E.A sequential credential rotation, fenced revoke, and exact-ID retries."""

from __future__ import annotations

import hashlib
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
    append_evidence, assert_all_replicas_deck_settings, published_deck,
    require_status, stable_uuid, wait_for_all_replicas_applied_equal,
    write_checker_evidence, write_manifest,
)
from stage_e_support import (
    recorded_bearer_probe, write_auth_checker_projection, write_auth_latencies,
    write_auth_summaries,
)


def main() -> None:
    scenario = "stage_e_rotation"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    credential_trace = run / f"{scenario}_credentials.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    checker_history = run / f"{scenario}_checker_history.jsonl"
    checker_audit = run / f"{scenario}_projection_audit.jsonl"
    checker_result = run / f"{scenario}_checker.jsonl"
    metrics = run / f"{scenario}_metrics.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    leader = wait_for_leader(config)
    nodes = config["host_nodes"]
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    summary_files: list[str] = []

    with TemporaryDirectory(prefix="stage-e-rotation-client-") as private_dir:
        client = AuthenticatedClient(private_dir, nodes, history)
        login1_id = f"{run_id}:login-1"
        login2_id = f"{run_id}:login-2"
        first = require_status(client.login(login1_id, "sofia@example.test", "Sofia-Classroom-1"), "AUTHENTICATED")
        old_token = first["access_token"]
        assert first["client_id"] == client.client_id and first["credential_generation"] == 1, first
        append_evidence(credential_trace, "issued", client_id=client.client_id,
                        request_id=login1_id, credential_id=first["credential_id"], generation=1)

        def snapshot(phase: str) -> dict:
            wait_for_all_replicas_applied_equal()
            path = run / f"{scenario}_{phase}_auth_summaries.jsonl"
            summary_files.append(path.name)
            return write_auth_summaries(run, path, client.client_id)[0]

        before_retry = snapshot("first_issue")
        retry = client.login(login1_id, "sofia@example.test", "Sofia-Classroom-1")
        assert retry == {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"}, retry
        after_retry = snapshot("issue_retry")
        assert after_retry["credential_generation"] == 1
        assert after_retry["credential_id"] == before_retry["credential_id"]
        append_evidence(probes, "exact_login_retry_no_rotation", request_id=login1_id,
                        credential_generation=1)

        require_status(client.call(f"{run_id}:publish", "publish_deck", {
            "deck_id": deck_id, **published_deck(deck_id, [slide_id], name="Stage E rotation"),
        }), "PUBLISHED")

        second = require_status(client.login(login2_id, "sofia@example.test", "Sofia-Classroom-1"), "AUTHENTICATED")
        new_token = second["access_token"]
        assert second["client_id"] == client.client_id
        assert second["credential_generation"] == 2 and new_token != old_token
        assert second["credential_id"] != first["credential_id"]
        append_evidence(credential_trace, "rotated", client_id=client.client_id,
                        request_id=login2_id, previous_credential_id=first["credential_id"],
                        credential_id=second["credential_id"], generation=2)
        rotated = snapshot("rotated")
        assert rotated["credential_id"] == second["credential_id"]
        assert rotated["token_digest"] == hashlib.sha256(new_token.encode()).hexdigest()

        old_attempt = recorded_bearer_probe(
            nodes[leader], history, client.client_id, old_token,
            f"{run_id}:old-bearer-edit", "update_deck_settings", {
                "deck_id": deck_id, "expected_deck_settings_version": 0,
                "patch": {"name": "Old bearer cannot edit"},
            },
        )
        assert old_attempt == {"ok": False, "error": "REAUTH_REQUIRED"}, old_attempt
        append_evidence(probes, "old_bearer_rejected", credential_id=first["credential_id"])

        stale = require_status(client.call(f"{run_id}:stale-revoke", "revoke_access_token", {
            "expected_credential_id": first["credential_id"], "expected_generation": 1,
        }), "STALE_CREDENTIAL")
        assert stale["status"] == "STALE_CREDENTIAL"
        after_stale = snapshot("stale_revoke")
        assert after_stale["credential_id"] == second["credential_id"]
        assert after_stale["credential_generation"] == 2 and after_stale["revoked"] is False
        append_evidence(probes, "stale_revoke_fenced", credential_id=first["credential_id"],
                        active_credential_id=second["credential_id"])

        edit = require_status(client.call(f"{run_id}:new-bearer-edit", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0,
            "patch": {"name": "New bearer edits"},
        }), "DECK_SETTINGS_UPDATED")
        assert edit["value"]["deck_settings_version"] == 1, edit

        revoke_id = f"{run_id}:revoke-current"
        revoke_args = {
            "expected_credential_id": second["credential_id"],
            "expected_generation": 2,
        }
        revoked = require_status(client.call(revoke_id, "revoke_access_token", revoke_args), "TOKEN_REVOKED")
        assert revoked["value"]["credential_generation"] == 3, revoked
        append_evidence(credential_trace, "revoked", client_id=client.client_id,
                        request_id=revoke_id, credential_id=second["credential_id"], generation=3)
        final_before_retry = snapshot("revoked")
        assert final_before_retry["credential_generation"] == 3 and final_before_retry["revoked"] is True

        revoke_retry = client.call(revoke_id, "revoke_access_token", revoke_args)
        assert revoke_retry == {"ok": False, "error": "REAUTH_REQUIRED"}, revoke_retry
        final_after_retry = snapshot("revoke_retry")
        assert final_after_retry["credential_generation"] == 3
        assert final_after_retry["credential_id"] == second["credential_id"]
        append_evidence(probes, "exact_revoke_retry_no_second_revoke", request_id=revoke_id,
                        credential_generation=3)

        denied = client.call(f"{run_id}:post-revoke-edit", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 1,
            "patch": {"name": "Revoked bearer cannot edit"},
        })
        assert denied == {"ok": False, "error": "REAUTH_REQUIRED"}, denied
        wait_for_all_replicas_applied_equal()
        assert_all_replicas_deck_settings(nodes, deck_id,
                                          expected_deck_settings_version=1,
                                          expected_name="New bearer edits")
        append_evidence(probes, "revoked_bearer_rejected", run_id=run_id,
                        client_id=client.client_id, deck_id=deck_id,
                        credential_generation=3)

        write_auth_checker_projection(history, checker_history, checker_audit)
        write_checker_evidence(checker_history, checker_result, run_dir=run)
        write_auth_latencies(history, metrics, events_dir=run)
        append_evidence(faults, "fault_trace", injections=[], runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, client_id=client.client_id,
                       history=history.name, checker_history=checker_history.name,
                       checker=checker_result.name, checker_audit=checker_audit.name,
                       probes=probes.name, credential_trace=credential_trace.name,
                       auth_summaries=summary_files, metrics=metrics.name, faults=faults.name,
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        leaks = scan_artifacts(run, ["Sofia-Classroom-1", old_token, new_token])
        assert not leaks, f"secret literal appeared in archived run inputs: {leaks}"

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
