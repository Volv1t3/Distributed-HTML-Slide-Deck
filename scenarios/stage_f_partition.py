"""Stage F stale followers, minority reads and both revoke/edit commit orders."""

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
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

import chaos
import cluster
from authenticated_client import AuthenticatedClient
from cluster_support import raft_state
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, require_status, stable_uuid, wait_for_all_replicas_applied_equal,
    wait_for_event, write_manifest,
)
from stage_e_support import recorded_bearer_probe
from stage_f_support import (
    ACCOUNTS, LEADER_READS, access_summary, assert_no_leaks, finalize_access_evidence,
    owned_deck, wait_for_activation,
)


def _no_protected_success(response: dict, label: str) -> None:
    assert response.get("ok") is not True, f"{label} served without a quorum leader: {response}"
    assert response.get("redirect") or response.get("error"), response


def _entry_index(run: Path, slot: str, request_id: str) -> int:
    raft = raft_state(run, slot)["raft"]
    entries = [entry["index"] for entry in raft["log"] if entry["command"]["request_id"] == request_id]
    assert len(entries) == 1, (slot, request_id, entries)
    return entries[0]


def main() -> None:
    scenario = "stage_f_partition"
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
    deck_id = stable_uuid(f"{run_id}:deck")
    slide_id = stable_uuid(f"{run_id}:slide")
    owner_id, mateo_id = ACCOUNTS["sofia"][2], ACCOUNTS["mateo"][2]
    injections: list[dict] = []

    with TemporaryDirectory(prefix="stage-f-partition-clients-") as private_dir:
        def client(name: str, slots) -> AuthenticatedClient:
            return AuthenticatedClient(Path(private_dir) / name, {slot: nodes[slot] for slot in slots},
                                       history, timeout=4.0)

        sofia, mateo = client("sofia", nodes), client("mateo", nodes)
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
            if op not in LEADER_READS and response.get("ok") is True:
                ids.append((account.client_id, request_id))
            return require_status(response, status) if status else response

        call(sofia, "publish", "publish_deck", {"deck_id": deck_id, **owned_deck(
            deck_id, [slide_id], name="Stage F partition", owner_user_id=owner_id)}, "PUBLISHED")
        call(sofia, "grant-edit", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 0,
        }, "ACCESS_GRANTED")
        wait_for_all_replicas_applied_equal()

        # 1. Stale follower: revoke commits on the majority while one follower
        # still holds the old grant on disk. It must not serve that grant.
        stale = next(slot for slot in config["names"] if slot != leader)
        majority = [slot for slot in config["names"] if slot != stale]
        chaos.main(["partition", stale, "/", *majority])
        injections.append({"fault": "partition", "isolated": stale, "majority": majority,
                           "role": "follower", "at": time.time()})
        sofia_major = client("sofia", majority)
        mateo_major = client("mateo", majority)
        call(sofia_major, "revoke-while-follower-stale", "revoke_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 1,
        }, "ACCESS_REVOKED")
        time.sleep(0.5)
        stale_view = access_summary(run, stale, [deck_id])["decks"][deck_id]
        current_view = access_summary(run, leader, [deck_id])["decks"][deck_id]
        assert stale_view["grants"] == [[mateo_id, "EDIT"]] and stale_view["access_version"] == 1, stale_view
        assert current_view["grants"] == [] and current_view["access_version"] == 2, current_view
        stale_read = recorded_bearer_probe(nodes[stale], history, mateo.client_id, tokens["mateo"],
                                           f"{run_id}:stale-follower-read", "get_deck_preview",
                                           {"deck_id": deck_id})
        _no_protected_success(stale_read, "stale follower read")
        stale_grants = recorded_bearer_probe(nodes[stale], history, mateo.client_id, tokens["mateo"],
                                             f"{run_id}:stale-follower-grants", "query_grants",
                                             {"deck_id": deck_id})
        _no_protected_success(stale_grants, "stale follower permission query")
        call(mateo_major, "majority-read-after-revoke", "get_deck_preview", {"deck_id": deck_id},
             "ACCESS_DENIED")
        append_evidence(probes, "stale_follower_masked", stale=stale, stale_access_version=1,
                        committed_access_version=2, stale_read=stale_read.get("redirect") or stale_read.get("error"),
                        stale_grants=stale_grants.get("redirect") or stale_grants.get("error"))
        chaos.main(["heal"])
        injections.append({"fault": "heal", "at": time.time()})
        wait_for_all_replicas_applied_equal()
        assert access_summary(run, stale, [deck_id])["decks"][deck_id]["access_version"] == 2

        # 2. Minority leader: an isolated leader cannot confirm a fresh quorum,
        # so it serves neither content nor a false NONE.
        isolated = wait_for_leader(config)
        majority = [slot for slot in config["names"] if slot != isolated]
        chaos.main(["partition", isolated, "/", *majority])
        started = time.monotonic()
        injections.append({"fault": "partition", "isolated": isolated, "majority": majority,
                           "role": "leader", "at": time.time()})
        for label, token, client_id, op in (
            ("minority-owner-read", tokens["sofia"], sofia.client_id, "get_deck_preview"),
            ("minority-owner-slide", tokens["sofia"], sofia.client_id, "get_slide"),
            ("minority-revoked-grants", tokens["mateo"], mateo.client_id, "query_grants"),
        ):
            args = {"deck_id": deck_id, **({"slide_id": slide_id} if op == "get_slide" else {})}
            response = recorded_bearer_probe(nodes[isolated], history, client_id, token,
                                             f"{run_id}:{label}", op, args)
            _no_protected_success(response, label)
            append_evidence(probes, "minority_read_refused", label=label, isolated=isolated,
                            error=response.get("error"), redirect=response.get("redirect"))
        new_leader = wait_for_leader(config, exclude={isolated})
        append_evidence(faults, "majority_leader_elected", leader=new_leader,
                        # Upper bound: measured after the three minority probes finished.
                        observed_after_seconds=round(time.monotonic() - started, 3))
        sofia_major = client("sofia", majority)
        mateo_major = client("mateo", majority)
        owner_view = call(sofia_major, "majority-owner-read", "get_deck_preview", {"deck_id": deck_id}, "OK")
        assert owner_view["effective_access"] == "OWNER" and owner_view["access_version"] == 2, owner_view

        # 3. Both revoke/edit commit orders, with overlapping in-flight requests.
        def race(first, second, label: str) -> dict:
            chaos.main(["delay", new_leader, "200"])
            outcome: dict[str, dict] = {}
            threads = []
            for key, account, request_id, op, args in (first, second):
                thread = threading.Thread(target=lambda key=key, account=account, request_id=request_id,
                                          op=op, args=args: outcome.__setitem__(
                                              key, account.call(request_id, op, args, node=new_leader,
                                                                timeout=8.0)))
                thread.start()
                threads.append(thread)
                wait_for_event(new_leader, "auth_admitted_read_started", request_id=request_id, timeout=3.0)
            for thread in threads:
                thread.join(12.0)
            chaos.main(["delay", new_leader, "0"])
            injections.append({"fault": "delay_ms", "slot": new_leader, "value": 200, "race": label})
            for key, account, request_id, _, _ in (first, second):
                if outcome[key].get("ok") is True:
                    ids.append((account.client_id, request_id))
            return outcome

        call(sofia_major, "regrant-edit", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 2,
        }, "ACCESS_GRANTED")
        edit_first, revoke_second = f"{run_id}:edit-before-revoke", f"{run_id}:revoke-after-edit"
        first = race(
            ("edit", mateo_major, edit_first, "update_deck_settings", {
                "deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "Edit wins"}}),
            ("revoke", sofia_major, revoke_second, "revoke_access", {
                "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 3}),
            "edit_then_revoke",
        )
        require_status(first["edit"], "DECK_SETTINGS_UPDATED")
        require_status(first["revoke"], "ACCESS_REVOKED")
        assert _entry_index(run, new_leader, edit_first) < _entry_index(run, new_leader, revoke_second)
        call(sofia_major, "regrant-edit-2", "grant_access", {
            "deck_id": deck_id, "target_user_id": mateo_id, "access_level": "EDIT",
            "expected_access_version": 4,
        }, "ACCESS_GRANTED")
        revoke_first, edit_second = f"{run_id}:revoke-before-edit", f"{run_id}:edit-after-revoke"
        second = race(
            ("revoke", sofia_major, revoke_first, "revoke_access", {
                "deck_id": deck_id, "target_user_id": mateo_id, "expected_access_version": 5}),
            ("edit", mateo_major, edit_second, "update_deck_settings", {
                "deck_id": deck_id, "expected_deck_settings_version": 1, "patch": {"name": "Revoke wins"}}),
            "revoke_then_edit",
        )
        require_status(second["revoke"], "ACCESS_REVOKED")
        require_status(second["edit"], "ACCESS_DENIED")
        assert _entry_index(run, new_leader, revoke_first) < _entry_index(run, new_leader, edit_second)
        append_evidence(probes, "revoke_edit_orders", edit_then_revoke=["DECK_SETTINGS_UPDATED", "ACCESS_REVOKED"],
                        revoke_then_edit=["ACCESS_REVOKED", "ACCESS_DENIED"], leader=new_leader)

        chaos.main(["heal"])
        injections.append({"fault": "heal", "at": time.time()})
        applied = wait_for_all_replicas_applied_equal()
        final = call(sofia, "final-owner-read", "get_deck_preview", {"deck_id": deck_id}, "OK")
        assert final["deck_settings_version"] == 1 and final["access_version"] == 6, final
        append_evidence(probes, "partition_converged", applied=applied, fence=fence, run_id=run_id)
        paths = finalize_access_evidence(scenario, run, history, actors=actors, deck_ids=[deck_id],
                                         request_ids=ids)
        append_evidence(faults, "fault_trace", injections=injections,
                        runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, history=history.name, probes=probes.name,
                       faults=faults.name, **{name: path.name for name, path in paths.items()},
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        append_evidence(probes, "secret_scan", **assert_no_leaks(run, list(tokens.values())))

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
