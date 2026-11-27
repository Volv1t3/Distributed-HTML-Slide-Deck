"""Stage F rolling upgrade from a Stage E cluster: v2 replay, activation fence, quarantine.

The runner extracts the completed Stage E release and passes its path in
``STAGE_F_LEGACY_BINARIES``. This scenario replaces the freshly started
Stage F processes with Stage E processes on empty disks, creates an
authenticated v2 deck there, upgrades each voter to the Stage F binary on its
preserved disk, then marks all voters coordinated so the leader commits the
v3 activation fence. The quarantined deck is then migrated through the
leader's local operator spool (``<run>/<slot>/operator``, ADR-018 §4): every
voter re-derives the provenance from its own retained committed history.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scenarios") not in sys.path:
    sys.path.insert(0, str(ROOT / "scenarios"))

import cluster
from authenticated_client import AuthenticatedClient
from dto_base import sha256_json
from stage_a_support import wait_for_leader
from stage_d_support import (
    append_evidence, published_deck, require_status, stable_uuid, text_element,
    wait_for_all_replicas_applied_equal, write_manifest,
)
from stage_f_support import (
    ACCOUNTS, LEADER_READS, SLOTS, assert_no_leaks, finalize_access_evidence, owned_deck,
    wait_for_activation,
)


def start_legacy_local(config: dict, slot: str, legacy_root: Path) -> None:
    """Start one Stage E replica process on this cluster's run directory."""
    env = {key: value for key, value in os.environ.items() if key != "STAGE_F_ALL_VOTERS_READY"}
    env.update(NODE_ID=slot, NODES=config["local_nodes"], RUN_DIR=str(cluster.RUN))
    out = open(cluster.RUN / slot / "stdout.log", "a")
    process = subprocess.Popen([sys.executable, "-u", str(legacy_root / "node.py"), slot], env=env,
                               stdout=out, stderr=out, cwd=legacy_root, start_new_session=True)
    config["pids"][slot] = process.pid


def restart(config: dict, slot: str, *, legacy_root: Path | None = None) -> None:
    cluster.stop_local(config, slot)
    if legacy_root is None:
        cluster.start_local(config, slot)
    else:
        start_legacy_local(config, slot, legacy_root)
    cluster.save(config)
    assert cluster.wait_up(config, [slot], 30), f"{slot} did not restart"


def deck_state(run: Path, slot: str, deck_id: str) -> dict | None:
    fsm = json.loads((run / slot / "disk.json").read_text())["raft_state"]["fsm"]
    return (fsm.get("decks") or {}).get(deck_id)


def provenance(run: Path, slot: str, deck_id: str) -> dict:
    """Locate the committed authenticated v2 publication a migration would verify."""
    state = json.loads((run / slot / "disk.json").read_text())["raft_state"]
    raft = state["raft"]
    for entry in raft["log"][: raft["last_applied"]]:
        command = entry["command"]
        if command["operation"] != "publish_deck" or command.get("deck_id") != deck_id:
            continue
        record = state["request_results"].get(sha256_json([command["client_id"], command["request_id"]]))
        return {
            "slot": slot, "log_index": entry["index"], "request_id": command["request_id"],
            "client_id": command["client_id"],
            "actor_user_id": command["payload"].get("authenticated_actor_user_id"),
            "result_status": record["result"]["status"] if record else None,
            "result_committed_index": record["committed_index"] if record else None,
            "schema_version": command["payload"]["presentation"]["schema_version"],
        }
    raise AssertionError(f"{slot}: no retained publication entry for {deck_id}")


def request_operator_migration(run: Path, slot: str, deck_id: str, name: str,
                               timeout: float = 20.0) -> dict:
    """Drop one explicit migration request into a replica's operator spool."""
    spool = run / slot / "operator"
    spool.mkdir(parents=True, exist_ok=True)
    staged = spool / f".{name}.tmp"
    staged.write_text(json.dumps({"operation": "migrate_legacy_deck", "deck_id": deck_id}))
    os.replace(staged, spool / f"{name}.json")
    result = spool / f"{name}.result.json"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result.exists():
            return json.loads(result.read_text())
        time.sleep(0.1)
    raise AssertionError(f"{slot}: operator request {name} was not answered")


def stored_migration_result(run: Path, slot: str, deck_id: str) -> tuple[dict, dict]:
    """Return the committed migration entry and its durable dedup result."""
    state = json.loads((run / slot / "disk.json").read_text())["raft_state"]
    raft = state["raft"]
    for entry in raft["log"][: raft["last_applied"]]:
        command = entry["command"]
        if command["operation"] == "migrate_legacy_deck" and command.get("deck_id") == deck_id:
            record = state["request_results"][sha256_json([command["client_id"], command["request_id"]])]
            return entry, record
    raise AssertionError(f"{slot}: no committed migration for {deck_id}")


def main() -> None:
    scenario = "stage_f_legacy"
    run = ROOT / "run"
    history = run / f"{scenario}.jsonl"
    probes = run / f"{scenario}_probes.jsonl"
    faults = run / f"{scenario}_faults.jsonl"
    manifest = run / f"{scenario}_manifest.jsonl"
    run_id = str(uuid4())
    legacy_root = Path(os.environ["STAGE_F_LEGACY_BINARIES"])
    assert os.environ.get("STAGE_F_ALL_VOTERS_READY") is None

    config = cluster.load()
    assert len(config["names"]) == 3, config
    nodes = config["host_nodes"]
    injections: list[dict] = []
    legacy_deck = stable_uuid(f"{run_id}:legacy-deck")
    legacy_slide = stable_uuid(f"{run_id}:legacy-slide")
    legacy_element = stable_uuid(f"{run_id}:legacy-element")
    new_deck = stable_uuid(f"{run_id}:v3-deck")
    unknown_deck = stable_uuid(f"{run_id}:unknown-deck")
    owner_id = ACCOUNTS["sofia"][2]

    # Phase 0: a clean Stage E cluster (the release before Stage F).
    for slot in SLOTS:
        cluster.stop_local(config, slot)
        (run / slot / "disk.json").unlink(missing_ok=True)
    for slot in SLOTS:
        start_legacy_local(config, slot, legacy_root)
    cluster.save(config)
    assert cluster.wait_up(config, list(SLOTS), 30)
    injections.append({"fault": "start_stage_e_binaries", "release": legacy_root.name, "at": time.time()})
    wait_for_leader(config, timeout=30)

    with TemporaryDirectory(prefix="stage-f-legacy-clients-") as private_dir:
        sofia = AuthenticatedClient(Path(private_dir) / "sofia", nodes, history)
        email, password, _ = ACCOUNTS["sofia"]
        tokens = [require_status(sofia.login(f"{run_id}:login-stage-e", email, password),
                                 "AUTHENTICATED")["access_token"]]
        actors = {sofia.client_id: owner_id}
        ids: list[tuple[str, str]] = []

        def call(name, op, args, status=None):
            request_id = f"{run_id}:{name}"
            response = sofia.call(request_id, op, args)
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            if (op not in LEADER_READS | {"query_operation_receipt"} and response.get("ok") is True
                    and result.get("status") != "UNSUPPORTED_PRESENTATION_SCHEMA"):
                ids.append((sofia.client_id, request_id))
            return require_status(response, status) if status else response

        # Phase 1: authenticated v2 history written by Stage E.
        call("legacy-publish", "publish_deck", {"deck_id": legacy_deck, **published_deck(
            legacy_deck, [legacy_slide], name="Stage E legacy")}, "PUBLISHED")
        call("legacy-settings", "update_deck_settings", {
            "deck_id": legacy_deck, "expected_deck_settings_version": 0, "patch": {"name": "Legacy edit"},
        }, "DECK_SETTINGS_UPDATED")
        call("legacy-element", "create_element", {
            "deck_id": legacy_deck, "slide_id": legacy_slide,
            "session_id": stable_uuid(f"{run_id}:legacy-session"),
            "element": text_element(legacy_element, "Legacy content"),
        }, "CREATED_AND_LOCKED")
        legacy_applied = wait_for_all_replicas_applied_equal()
        before = {slot: deck_state(run, slot, legacy_deck) for slot in SLOTS}
        assert before["n1"] is not None and all(state == before["n1"] for state in before.values())
        assert before["n1"].get("schema_version", 2) == 2
        append_evidence(probes, "stage_e_history_written", applied=legacy_applied,
                        deck_settings_version=before["n1"]["deck_settings_version"])

        # Phase 2: rolling binary upgrade on preserved disks; followers first.
        leader = wait_for_leader(config)
        for slot in [slot for slot in SLOTS if slot != leader] + [leader]:
            restart(config, slot)
            injections.append({"fault": "upgrade_preserved_disk", "slot": slot, "at": time.time()})
            wait_for_leader(config, timeout=30)
            wait_for_all_replicas_applied_equal(timeout=30)
        mixed = json.loads((run / leader / "disk.json").read_text())["raft_state"]["fsm"]
        assert mixed.get("presentation_schema_policy") is None, "activation before coordination"
        pending = call("publish-while-pending", "publish_deck", {"deck_id": new_deck, **owned_deck(
            new_deck, [legacy_slide], name="Too early", owner_user_id=owner_id)})
        assert pending == {"ok": False, "error": "PRESENTATION_SCHEMA_UNAVAILABLE"}, pending
        append_evidence(probes, "uncoordinated_upgrade", activation_policy=None,
                        publish_while_pending=pending["error"])

        # Phase 3: every voter runs Stage F; mark coordination and restart.
        os.environ["STAGE_F_ALL_VOTERS_READY"] = "1"
        for slot in SLOTS:
            restart(config, slot)
        injections.append({"fault": "coordinated_restart_preserved_disks", "at": time.time()})
        wait_for_leader(config, timeout=30)
        fence = wait_for_activation(run, timeout=30)
        legacy_index = provenance(run, "n1", legacy_deck)["log_index"]
        assert fence > legacy_index, (fence, legacy_index)
        after = {slot: deck_state(run, slot, legacy_deck) for slot in SLOTS}
        assert after == before, "historical replay or activation rewrote the legacy deck"

        # Phase 4: quarantine is indistinguishable from an unknown deck.
        grants = call("legacy-grants", "query_grants", {"deck_id": legacy_deck}, "OK")
        unknown = call("unknown-grants", "query_grants", {"deck_id": unknown_deck}, "OK")
        assert grants == unknown == {"status": "OK", "effective_access": "NONE"}, (grants, unknown)
        masked = call("legacy-preview", "get_deck_preview", {"deck_id": legacy_deck}, "ACCESS_DENIED")
        masked_unknown = call("unknown-preview", "get_deck_preview", {"deck_id": unknown_deck}, "ACCESS_DENIED")
        assert masked == masked_unknown, (masked, masked_unknown)
        call("legacy-edit-after-fence", "update_deck_settings", {
            "deck_id": legacy_deck, "expected_deck_settings_version": 1, "patch": {"name": "Denied"},
        }, "ACCESS_DENIED")
        call("legacy-grant-after-fence", "grant_access", {
            "deck_id": legacy_deck, "target_user_id": ACCOUNTS["mateo"][2], "access_level": "VIEW",
            "expected_access_version": 0,
        }, "ACCESS_DENIED")
        call("v2-after-fence", "publish_deck", {"deck_id": unknown_deck, **published_deck(
            unknown_deck, [legacy_slide], name="v2 after fence")}, "UNSUPPORTED_PRESENTATION_SCHEMA")
        call("v3-after-fence", "publish_deck", {"deck_id": new_deck, **owned_deck(
            new_deck, [stable_uuid(f"{run_id}:v3-slide")], name="Stage F deck", owner_user_id=owner_id,
        )}, "PUBLISHED")
        append_evidence(probes, "legacy_quarantine", fence=fence, legacy_publication_index=legacy_index,
                        query_grants=grants, preview=masked, deck_state_unchanged=True)

        # Phase 5: provenance is retained identically on all voters, then an
        # explicit operator request on the leader proposes the migration.
        proofs = [provenance(run, slot, legacy_deck) for slot in SLOTS]
        assert all({**proof, "slot": "-"} == {**proofs[0], "slot": "-"} for proof in proofs), proofs
        assert proofs[0]["actor_user_id"] == owner_id and proofs[0]["result_status"] == "PUBLISHED"
        assert proofs[0]["result_committed_index"] == proofs[0]["log_index"]
        for proof in proofs:
            append_evidence(probes, "legacy_migration_provenance", **proof)

        follower = next(slot for slot in SLOTS if slot != wait_for_leader(config))
        refused = request_operator_migration(run, follower, legacy_deck, "migrate-on-follower")
        assert refused == {"status": "NOT_LEADER"}, refused
        leader = wait_for_leader(config)
        invoke = time.time()
        proposal = request_operator_migration(run, leader, legacy_deck, "migrate-legacy")
        assert proposal["status"] == "PROPOSED", proposal
        migrated_applied = wait_for_all_replicas_applied_equal(timeout=30)
        complete = time.time()
        migrated = {slot: deck_state(run, slot, legacy_deck) for slot in SLOTS}
        assert all(state == migrated["n1"] for state in migrated.values()), migrated
        assert migrated["n1"]["schema_version"] == 3
        assert migrated["n1"]["metadata"]["owner_user_id"] == owner_id
        assert migrated["n1"]["access"] == {"schema_version": 1, "access_version": 0, "grants": {}}
        preserved = {key: value for key, value in migrated["n1"].items() if key not in {"schema_version", "access"}}
        expected = {key: value for key, value in before["n1"].items() if key != "schema_version"}
        expected["metadata"] = {**expected["metadata"], "owner_user_id": owner_id}
        assert preserved == expected, "migration changed content, settings, locks or revisions"
        entry, stored = stored_migration_result(run, leader, legacy_deck)
        assert stored["result"]["status"] == "LEGACY_DECK_MIGRATED", stored
        # The operator is a client of the system: record its migration for check.py.
        with history.open("a") as stream:
            stream.write(json.dumps({
                "id": entry["command"]["request_id"], "client": "operator", "op": "migrate_legacy_deck",
                "args": {"deck_id": legacy_deck, "owner_user_id": owner_id},
                "invoke": invoke, "complete": complete, "ok": True,
                "result": stored["result"], "error": None,
            }) + "\n")
        retry = request_operator_migration(run, leader, legacy_deck, "migrate-legacy-retry")
        assert retry["status"] in {"LEGACY_DECK_MIGRATED", "LEGACY_DECK_ALREADY_MIGRATED"}, retry
        owner_preview = call("legacy-preview-after-migration", "get_deck_preview",
                             {"deck_id": legacy_deck}, "OK")
        assert owner_preview["effective_access"] == "OWNER", owner_preview
        assert owner_preview["metadata"]["name"] == "Legacy edit", owner_preview
        roster = call("legacy-roster-after-migration", "query_access_state", {"deck_id": legacy_deck}, "OK")
        assert roster["grants"] == [] and roster["access_version"] == 0, roster
        append_evidence(probes, "legacy_migration_applied", proposal=proposal, retry=retry,
                        follower_request=refused, log_index=entry["index"],
                        applied=migrated_applied, owner_user_id=owner_id,
                        owner_preview_access=owner_preview["effective_access"])
        before = migrated

        # Phase 6: preserved-disk replay of the whole upgraded cluster.
        before_replay = wait_for_all_replicas_applied_equal(timeout=30)
        for slot in SLOTS:
            restart(config, slot)
        injections.append({"fault": "restart_all_preserved_disks", "at": time.time()})
        wait_for_leader(config, timeout=30)
        assert wait_for_activation(run, timeout=30) == fence
        after_replay = wait_for_all_replicas_applied_equal(timeout=30)
        assert min(after_replay.values()) >= max(before_replay.values())
        assert {slot: deck_state(run, slot, legacy_deck) for slot in SLOTS} == before
        replayed = call("legacy-preview-after-replay", "get_deck_preview", {"deck_id": legacy_deck}, "OK")
        assert replayed["effective_access"] == "OWNER", replayed
        owner = call("v3-owner-after-replay", "query_grants", {"deck_id": new_deck}, "OK")
        assert owner["effective_access"] == "OWNER", owner
        append_evidence(probes, "preserved_disk_replay", before=before_replay, after=after_replay)

        wait_for_all_replicas_applied_equal()
        paths = finalize_access_evidence(scenario, run, history, actors=actors,
                                         deck_ids=[legacy_deck, new_deck], request_ids=ids)
        append_evidence(faults, "fault_trace", injections=injections,
                        runtime_faults=json.loads((run / "faults.json").read_text()))
        write_manifest(manifest, scenario, run_id=run_id, history=history.name, probes=probes.name,
                       faults=faults.name, legacy_release=legacy_root.name,
                       **{name: path.name for name, path in paths.items()},
                       role_traces=[f"{slot}/roles.log" for slot in nodes],
                       operational_logs=[f"{slot}/events.log" for slot in nodes])
        append_evidence(probes, "secret_scan", **assert_no_leaks(run, tokens))

    print(f"[{scenario}] PASSED run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
