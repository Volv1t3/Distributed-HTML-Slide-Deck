"""Stage F leader failover, client restart recovery and preserved-disk replay."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

import chaos
import cluster
from authenticated_client import AuthenticatedClient
from client_journal import JournaledClient, Lifecycle
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, require_status, stable_uuid, wait_for_all_replicas_applied_equal,
    write_manifest,
)
from stage_f_support import (
    ACCOUNTS, LEADER_READS, SLOTS, access_summary, assert_no_leaks, finalize_access_evidence,
    owned_deck, wait_for_activation,
)


def main() -> None:
    scenario = "stage_f_failover"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    journal_trace = run / f"{scenario}_journal.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())

    config = cluster.load()
    assert len(config["names"]) == 3, config
    nodes = config["host_nodes"]
    leader = wait_for_leader(config)
    fence = wait_for_activation(run)
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    owner_id, mateo_id = ACCOUNTS["sofia"][2], ACCOUNTS["mateo"][2]
    injections: list[dict] = []

    with TemporaryDirectory(prefix="stage-f-failover-clients-") as private_dir:
        private = Path(private_dir)
        sofia = AuthenticatedClient(private / "sofia", nodes, history)
        mateo = AuthenticatedClient(private / "mateo", nodes, history)
        tokens = {}
        for name, account in (("sofia", sofia), ("mateo", mateo)):
            email, password, _ = ACCOUNTS[name]
            tokens[name] = require_status(account.login(f"{run_id}:login-{name}", email, password),
                                          "AUTHENTICATED")["access_token"]
        actors = {sofia.client_id: owner_id, mateo.client_id: mateo_id}
        ids: list[tuple[str, str]] = []

        def call(account, name, op, args, status=None, **kwargs):
            request_id = f"{run_id}:{name}"
            response = account.call(request_id, op, args, **kwargs)
            if op not in LEADER_READS | {"query_operation_receipt"} and response.get("ok") is True:
                ids.append((account.client_id, request_id))
            return require_status(response, status) if status else response

        def editor(current_leader: str) -> tuple[AuthenticatedClient, JournaledClient]:
            """(Re)start Mateo's installation from its private durable directory."""
            installation = AuthenticatedClient(private / "mateo", nodes, history)
            assert installation.client_id == mateo.client_id
            journal = JournaledClient(
                installation.client_id, private / "mateo" / "journal" / f"{deck_id}.jsonl",
                stable_uuid(f"{run_id}:mateo-session"), request_sender=installation.journal_sender,
                local_replica=current_leader, leader_replica=current_leader,
                user_id=mateo_id, deck_id=deck_id, poll_interval_seconds=0.01,
            )
            return installation, journal

        def trace(label: str, journal: JournaledClient, *request_ids: str) -> dict:
            record = {"label": label, "gate": journal.outbox.gate, "intentions": {
                request_id: {"lifecycle": journal.outbox.get(request_id).lifecycle.value,
                             "status": (journal.outbox.get(request_id).result or {}).get("status")}
                for request_id in request_ids}}
            append_evidence(journal_trace, "journal_state", **record)
            return record

        def settings(journal: JournaledClient, request_id: str, name: str, expected: int):
            ids.append((mateo.client_id, request_id))
            return journal.enqueue("update_deck_settings", {
                "deck_id": deck_id, "expected_deck_settings_version": expected, "patch": {"name": name},
            }, request_id=request_id)

        call(sofia, "publish", "publish_deck", {"deck_id": deck_id, **owned_deck(
            deck_id, [slide_id], name="Stage F failover", owner_user_id=owner_id)}, "PUBLISHED")
        call(sofia, "grant-edit", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 0,
        }, "ACCESS_GRANTED")

        # Journaled edit before failover: fresh role first, then a leader poll.
        _, journal = editor(leader)
        assert journal.outbox.gate["suspended"], journal.outbox.gate
        assert journal.refresh_permission() and journal.poll_if_due(deck_id)
        first = f"{run_id}:journal-before-failover"
        settings(journal, first, "Before failover", 0)
        journal.synchronize()
        assert journal.outbox.get(first).lifecycle is Lifecycle.COMMITTED, journal.outbox.get(first)
        trace("before_failover", journal, first)

        # Leader failure: the grant must survive on the elected replacement.
        wait_for_all_replicas_applied_equal()
        chaos.main(["kill", leader])
        killed_at = time.monotonic()
        injections.append({"fault": "kill", "slot": leader, "at": time.time()})
        survivors = [slot for slot in SLOTS if slot != leader]
        new_leader = wait_for_leader(config, exclude={leader})
        append_evidence(faults, "replacement_leader_elected", leader=new_leader,
                        unavailability_seconds=round(time.monotonic() - killed_at, 3))
        # Client restart on the replacement leader: a positive role must be re-verified.
        mateo, journal = editor(new_leader)
        assert journal.outbox.gate["suspended"], journal.outbox.gate
        assert journal.refresh_permission() and journal.poll_if_due(deck_id)
        second = f"{run_id}:journal-after-failover"
        settings(journal, second, "After failover", 1)
        journal.synchronize()
        assert journal.outbox.get(second).lifecycle is Lifecycle.COMMITTED, journal.outbox.get(second)
        trace("after_failover_restart", journal, first, second)

        # Revocation while the editor keeps working locally.
        sofia_live = AuthenticatedClient(private / "sofia", {slot: nodes[slot] for slot in survivors}, history)
        call(sofia_live, "revoke", "revoke_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 1,
        }, "ACCESS_REVOKED")
        denied = f"{run_id}:journal-denied"
        settings(journal, denied, "Denied after revoke", 2)
        journal.synchronize()
        assert journal.outbox.get(denied).lifecycle is Lifecycle.REJECTED_FINAL, journal.outbox.get(denied)
        assert journal.outbox.gate["suspended"] and journal.outbox.gate["last_observed_access"] == "NONE"
        retained = f"{run_id}:journal-retained"
        settings(journal, retained, "Retained offline work", 2)
        assert journal.synchronize() == []
        trace("after_revocation", journal, denied, retained)

        # Client restart after access loss, then regrant: no automatic resend.
        mateo, journal = editor(new_leader)
        assert journal.outbox.gate["requires_explicit_reconciliation"], journal.outbox.gate
        call(sofia_live, "regrant", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 2,
        }, "ACCESS_GRANTED")
        assert journal.refresh_permission()
        assert journal.outbox.gate["last_observed_access"] == "EDIT"
        assert journal.synchronize() == []
        kept = journal.outbox.get(retained)
        assert kept.lifecycle is Lifecycle.PENDING_SYNC and kept.arguments["patch"] == {
            "name": "Retained offline work"}, kept
        recovery = [item.request_id for item in journal.denied_recovery_view()]
        assert recovery == [denied], recovery
        receipts = {}
        for label, original in (("first", first), ("denied", denied), ("retained", retained)):
            receipts[label] = call(mateo, f"receipt-{label}", "query_operation_receipt",
                                   {"original_request_id": original})["result"]["status"]
        assert receipts == {"first": "COMMITTED_SUCCESS", "denied": "COMMITTED_REJECTED",
                            "retained": "OUTCOME_UNKNOWN"}, receipts
        trace("after_restart_and_regrant", journal, first, second, denied, retained)
        ids.remove((mateo.client_id, retained))
        append_evidence(probes, "client_recovery", receipts=receipts, recovery_view=recovery,
                        retained_request_id=retained, auto_resend=False)

        # Preserved-disk replay: restart the old leader, then the whole cluster.
        chaos.main(["start", leader])
        injections.append({"fault": "start_preserved_disk", "slot": leader, "at": time.time()})
        before = wait_for_all_replicas_applied_equal(timeout=30)
        summary_before = [access_summary(run, slot, [deck_id]) for slot in SLOTS]
        for slot in SLOTS:
            chaos.main(["kill", slot])
        injections.append({"fault": "kill_all", "at": time.time()})
        for slot in SLOTS:
            chaos.main(["start", slot])
        injections.append({"fault": "start_all_preserved_disks", "at": time.time()})
        replay_leader = wait_for_leader(config, timeout=30)
        after = wait_for_all_replicas_applied_equal(timeout=30)
        assert min(after.values()) >= max(before.values()), (before, after)
        summary_after = [access_summary(run, slot, [deck_id]) for slot in SLOTS]
        for old, new in zip(summary_before, summary_after):
            assert old["decks"] == new["decks"] and old["v3_enforced_from_index"] == new["v3_enforced_from_index"], (old, new)
        owner_view = call(sofia, "owner-after-replay", "get_deck_preview", {"deck_id": deck_id}, "OK")
        assert owner_view["access_version"] == 3 and owner_view["deck_settings_version"] == 2, owner_view
        editor_role = call(mateo, "editor-after-replay", "query_grants", {"deck_id": deck_id}, "OK")
        assert editor_role["effective_access"] == "EDIT", editor_role
        append_evidence(probes, "preserved_disk_replay", applied_before=before, applied_after=after,
                        leader_after=replay_leader, fence=fence, run_id=run_id)

        applied = wait_for_all_replicas_applied_equal()
        paths = finalize_access_evidence(scenario, run, history, actors=actors, deck_ids=[deck_id],
                                         request_ids=ids)
        append_evidence(faults, "fault_trace", injections=injections,
                        runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, history=history.name, probes=probes.name,
                       faults=faults.name, journal_trace=journal_trace.name, applied=applied,
                       **{name: path.name for name, path in paths.items()},
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        append_evidence(probes, "secret_scan", **assert_no_leaks(run, list(tokens.values())))
        journal_leaks = [path for path in private.rglob("*.jsonl")
                         if any(token.encode() in path.read_bytes() for token in tokens.values())]
        assert not journal_leaks, journal_leaks

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
