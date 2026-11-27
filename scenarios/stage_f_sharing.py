"""Stage F publication, sharing, downgrade, revocation and receipts on one clean cluster."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

import cluster
from authenticated_client import AuthenticatedClient
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, published_deck, require_status, stable_uuid, text_element,
    wait_for_all_replicas_applied_equal, write_manifest,
)
from stage_e_support import recorded_bearer_probe
from stage_f_support import (
    ACCOUNTS, LEADER_READS, assert_no_leaks, finalize_access_evidence, owned_deck,
    wait_for_activation,
)


def main() -> None:
    scenario = "stage_f_sharing"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    nodes = config["host_nodes"]
    leader = wait_for_leader(config)
    fence = wait_for_activation(run)
    append_evidence(probes, "v3_activation_applied", v3_enforced_from_index=fence, leader=leader)
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    element_id = stable_uuid(f"{run_id}:element")
    unknown_deck = stable_uuid(f"{run_id}:unknown-deck")
    session_id = stable_uuid(f"{run_id}:mateo-session")
    owner_id, mateo_id, elena_id = (ACCOUNTS[name][2] for name in ("sofia", "mateo", "elena"))

    with TemporaryDirectory(prefix="stage-f-sharing-clients-") as private_dir:
        clients = {name: AuthenticatedClient(Path(private_dir) / name, nodes, history)
                   for name in ACCOUNTS}
        tokens = []
        for name, (email, password, user_id) in ACCOUNTS.items():
            login = require_status(clients[name].login(f"{run_id}:login-{name}", email, password),
                                   "AUTHENTICATED")
            assert login["user_id"] == user_id, login
            tokens.append(login["access_token"])
        actors = {clients[name].client_id: ACCOUNTS[name][2] for name in ACCOUNTS}
        sofia, mateo, elena = clients["sofia"], clients["mateo"], clients["elena"]
        ids: list[tuple[str, str]] = []

        def call(client, name: str, op: str, args: dict, status: str | None = None, **kwargs) -> dict:
            request_id = f"{run_id}:{name}"
            response = client.call(request_id, op, args, **kwargs)
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            # Durable dedup outcomes exist only for committed mutations, not for
            # reads or the adapter's pre-proposal v2 publication rejection.
            if op not in LEADER_READS | {"query_operation_receipt"} and \
                    result.get("status") != "UNSUPPORTED_PRESENTATION_SCHEMA":
                ids.append((client.client_id, request_id))
            return require_status(response, status) if status else response

        # Publication: owner binding, owner mismatch and post-fence v2 denial.
        call(sofia, "publish", "publish_deck", {
            "deck_id": deck_id, **owned_deck(deck_id, [slide_id], name="Stage F shared", owner_user_id=owner_id),
        }, "PUBLISHED")
        mismatch = call(sofia, "publish-owner-mismatch", "publish_deck", {
            "deck_id": unknown_deck,
            **owned_deck(unknown_deck, [slide_id], name="Claimed", owner_user_id=mateo_id),
        })
        assert mismatch["result"]["status"] != "PUBLISHED", mismatch
        v2 = call(sofia, "publish-v2-after-fence", "publish_deck", {
            "deck_id": unknown_deck, **published_deck(unknown_deck, [slide_id], name="Legacy shape"),
        }, "UNSUPPORTED_PRESENTATION_SCHEMA")
        append_evidence(probes, "publication_boundary", owner_mismatch=mismatch["result"]["status"],
                        v2_after_fence=v2["status"])

        # A new deck is private: nonparticipant and unknown deck are indistinguishable.
        owner_role = call(sofia, "owner-grants", "query_grants", {"deck_id": deck_id}, "OK")
        assert owner_role["effective_access"] == "OWNER" and owner_role["access_version"] == 0, owner_role
        private = call(mateo, "private-grants", "query_grants", {"deck_id": deck_id}, "OK")
        missing = call(mateo, "unknown-grants", "query_grants", {"deck_id": unknown_deck}, "OK")
        assert private == missing == {"status": "OK", "effective_access": "NONE"}, (private, missing)
        masked = call(mateo, "private-preview", "get_deck_preview", {"deck_id": deck_id}, "ACCESS_DENIED")
        masked_unknown = call(mateo, "unknown-preview", "get_deck_preview", {"deck_id": unknown_deck},
                              "ACCESS_DENIED")
        assert masked == masked_unknown == {"status": "ACCESS_DENIED"}, (masked, masked_unknown)
        call(mateo, "private-edit", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "Intruder"},
        }, "ACCESS_DENIED")

        # Followers never serve protected content: a direct follower read only redirects.
        follower = next(slot for slot in config["names"] if slot != wait_for_leader(config))
        probe = recorded_bearer_probe(nodes[follower], history, sofia.client_id, tokens[0],
                                      f"{run_id}:follower-read", "get_deck_preview", {"deck_id": deck_id})
        assert probe.get("ok") is not True and (probe.get("redirect") or probe.get("error")), probe
        append_evidence(probes, "follower_protected_read", follower=follower,
                        redirect=probe.get("redirect"), error=probe.get("error"))

        # VIEW: reads succeed, every mutation and sharing change is denied.
        granted = call(sofia, "grant-view", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "VIEW",
            "expected_access_version": 0,
        }, "ACCESS_GRANTED")
        assert granted["value"]["access_version"] == 1, granted
        view = call(mateo, "view-preview", "get_deck_preview", {"deck_id": deck_id}, "OK")
        assert view["effective_access"] == "VIEW" and view["access_version"] == 1, view
        call(mateo, "view-slide", "get_slide", {"deck_id": deck_id, "slide_id": slide_id}, "OK")
        call(mateo, "view-edit", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "Viewer"},
        }, "ACCESS_DENIED")
        call(mateo, "view-shares", "grant_access", {
            "deck_id": deck_id, "target_user_id": elena_id, "access_level": "VIEW",
            "expected_access_version": 1,
        }, "ACCESS_DENIED")

        # Grant ordering: stale revision conflicts, upgrade, then unchanged.
        call(sofia, "grant-stale", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 0,
        }, "ACCESS_VERSION_CONFLICT")
        call(sofia, "grant-edit", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 1,
        }, "ACCESS_UPDATED")
        call(sofia, "grant-edit-again", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 2,
        }, "ACCESS_UNCHANGED")
        call(sofia, "grant-self", "grant_access", {
            "deck_id": deck_id, "target_user_id": owner_id, "access_level": "VIEW",
            "expected_access_version": 2,
        }, "INVALID_ACCESS_TARGET")
        call(sofia, "grant-unregistered", "grant_access", {
            "deck_id": deck_id, "target_user_id": str(uuid4()), "access_level": "VIEW",
            "expected_access_version": 2,
        }, "INVALID_TARGET_USER")

        # EDIT: element, lock and settings mutations succeed under ordinary preconditions.
        created = call(mateo, "edit-create", "create_element", {
            "deck_id": deck_id, "slide_id": slide_id, "session_id": session_id,
            "element": text_element(element_id, "Shared edit"),
        }, "CREATED_AND_LOCKED")
        fencing = created["value"]["fencing_token"]
        saved_id = f"{run_id}:edit-save"
        call(mateo, "edit-save", "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": fencing, "expected_version": 1, "patch": {"content": "Editor change"},
        }, "SAVED")
        call(mateo, "edit-settings", "update_deck_settings", {
            "deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "Edited by Mateo"},
        }, "DECK_SETTINGS_UPDATED")

        # Downgrade: a still-valid lock never overrides the reduced role.
        call(sofia, "downgrade", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "VIEW",
            "expected_access_version": 2,
        }, "ACCESS_UPDATED")
        denied_id = f"{run_id}:downgraded-save"
        call(mateo, "downgraded-save", "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": fencing, "expected_version": 2, "patch": {"content": "After downgrade"},
        }, "ACCESS_DENIED")
        lock = call(mateo, "downgraded-lock-read", "read_lock_status",
                    {"deck_id": deck_id, "element_id": element_id}, "OK")
        append_evidence(probes, "lock_after_downgrade", lock_present=lock["lock"] is not None,
                        fencing_token=(lock["lock"] or {}).get("fencing_token"))

        # Revocation: content and roster vanish; receipts stay minimal.
        call(sofia, "revoke", "revoke_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 3,
        }, "ACCESS_REVOKED")
        call(sofia, "revoke-again", "revoke_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 4,
        }, "ACCESS_NOT_FOUND")
        revoked_read = call(mateo, "revoked-preview", "get_deck_preview", {"deck_id": deck_id}, "ACCESS_DENIED")
        assert revoked_read == masked_unknown, revoked_read
        assert call(mateo, "revoked-grants", "query_grants", {"deck_id": deck_id}, "OK") == missing
        replay = mateo.call(saved_id, "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": fencing, "expected_version": 1, "patch": {"content": "Editor change"},
        })
        assert replay == {"ok": True, "result": {"status": "ACCESS_DENIED"}}, replay
        receipts = {
            label: call(mateo, f"receipt-{label}", "query_operation_receipt",
                        {"original_request_id": original}, None)["result"]["status"]
            for label, original in (("saved", saved_id), ("denied", denied_id),
                                    ("unknown", f"{run_id}:never-sent"))
        }
        assert receipts == {"saved": "COMMITTED_SUCCESS", "denied": "COMMITTED_REJECTED",
                            "unknown": "OUTCOME_UNKNOWN"}, receipts
        foreign = call(elena, "receipt-foreign", "query_operation_receipt",
                       {"original_request_id": saved_id}, "OUTCOME_UNKNOWN")
        append_evidence(probes, "revocation_privacy", replay_status=replay["result"]["status"],
                        receipts=receipts, foreign_receipt=foreign["status"])
        owner_view = call(sofia, "owner-final", "get_deck_preview", {"deck_id": deck_id}, "OK")
        assert owner_view["access_version"] == 4 and owner_view["deck_settings_version"] == 1, owner_view

        applied = wait_for_all_replicas_applied_equal()
        append_evidence(probes, "sharing_converged", run_id=run_id, applied=applied, deck_id=deck_id)
        paths = finalize_access_evidence(scenario, run, history, actors=actors, deck_ids=[deck_id],
                                         request_ids=ids)
        append_evidence(faults, "fault_trace", injections=[],
                        runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, history=history.name, probes=probes.name,
                       faults=faults.name, **{name: path.name for name, path in paths.items()},
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        append_evidence(probes, "secret_scan", **assert_no_leaks(run, tokens))

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
