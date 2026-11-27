"""Stage F issue #60: provenance-verified v2 -> v3 legacy deck migration.

Seams under test:
  1. ``RaftEngine`` committed apply (follower AppendEntries catch-up). The
     migration's provenance bridge is derived from the replica's own retained
     committed log entry and request-result record, so every test drives a real
     committed history through the engine instead of handing the FSM a context.
  2. ``KeyValueStateMachine`` public read projections (``query_grants`` /
     ``protected_read``) on the resulting applied state, to observe quarantine.
  3. ``App`` leader-only internal proposal and the public request adapter.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app import App

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import (
    CommandEnvelopeDTO,
    PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
    legacy_deck_migration_command,
    presentation_schema_activation_request_id,
    validate_committed_command,
)
from dto_base import sha256_json
from node import Request
from persistence import InMemoryPersistence
from raft_core import RaftEngine
from stage_e_raft_harness import SimulatedRaftHarnessMixin, actor_stamp, issue_token_command
from state_machine import KeyValueStateMachine

CATALOG = load_account_catalog()
OWNER = str(CATALOG.users[0].profile.user_id)
OTHER = str(CATALOG.users[1].profile.user_id)
LEADER_UUID = uuid4()


def _genesis(enforced_from_index: int = 1) -> dict[str, Any]:
    return {"kv": {}, "auth": initial_auth_state(CATALOG, enforced_from_index=enforced_from_index)}


def _key(command: CommandEnvelopeDTO) -> str:
    return sha256_json([command.client_id, command.request_id])


def _activation() -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
        "request_id": presentation_schema_activation_request_id(), "origin_node_uuid": str(uuid4()),
        "operation": "activate_presentation_schema_v3", "payload": {},
    })


def _v2_publication(deck_id: str, *, client_id: str, request_id: str,
                    actor: dict[str, object] | None) -> CommandEnvelopeDTO:
    element = {
        "element_id": str(uuid4()), "kind": "rectangle",
        "configuration": {
            "x": 1, "y": 2, "width": 30, "height": 40, "scale_basis_points": 10000,
            "rotation_centidegrees": 0, "fill": "#112233FF", "border_color": "#000000FF",
            "border_style": "solid", "border_thickness": 1,
        },
    }
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id, "request_id": request_id,
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id, "name": "Legacy", "owner_session_id": str(uuid4()),
                    "width_px": 1280, "height_px": 720,
                },
                "slides": [{
                    "metadata": {"slide_id": str(uuid4()), "background": "#FFFFFFFF"},
                    "elements": [element],
                }],
            },
            **(actor or {}),
        },
    })


def _acquire_lock(deck_id: str, element_id: str, *, client_id: str,
                  actor: dict[str, object]) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id, "request_id": f"lock-{uuid4()}",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "session_id": str(uuid4()),
        "operation": "acquire_lock", "payload": {"element_id": element_id, **actor},
    })


class _History:
    """A committed history containing an authenticated historical v2 publication."""

    def __init__(self, *, enforced_from_index: int = 1, authenticated: bool = True) -> None:
        self.genesis = _genesis(enforced_from_index)
        self.deck_id = str(uuid4())
        self.client_id, credential_id = str(uuid4()), str(uuid4())
        self.actor = actor_stamp(OWNER, credential_id)
        self.publication = _v2_publication(
            self.deck_id, client_id=self.client_id, request_id="legacy-publish",
            actor=self.actor if authenticated else None,
        )
        element_id = self.publication.payload["presentation"]["slides"][0]["elements"][0]["element_id"]
        self.commands: list[CommandEnvelopeDTO] = [
            issue_token_command(
                OWNER, self.client_id, credential_id, hashlib.sha256(b"legacy-owner").hexdigest(),
            ),
            self.publication,
        ]
        if authenticated:
            self.commands.append(_acquire_lock(
                self.deck_id, element_id, client_id=self.client_id, actor=self.actor,
            ))
        self.commands.append(_activation())

    def migration(self, *, owner: str = OWNER, original_client_id: str | None = None,
                  original_request_id: str | None = None, ordinal: int = 0) -> CommandEnvelopeDTO:
        return legacy_deck_migration_command(
            deck_id=self.deck_id,
            original_client_id=original_client_id or self.publication.client_id,
            original_request_id=original_request_id or self.publication.request_id,
            owner_user_id=owner,
            origin_node_uuid=LEADER_UUID,
            ordinal=ordinal,
        )


FOLLOWER_UUID = uuid4()


def _follower(commands: list[CommandEnvelopeDTO], *, genesis: dict[str, Any],
              storage: InMemoryPersistence | None = None,
              sent: list[tuple[str, dict[str, Any]]] | None = None) -> RaftEngine:
    """Replay a committed history through a follower's AppendEntries path."""
    leader_uuid = LEADER_UUID
    engine = RaftEngine(
        node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=FOLLOWER_UUID,
        storage=storage or InMemoryPersistence(),
        send=(lambda slot, message: sent.append((slot, message))) if sent is not None else (lambda *_: None),
        clock=lambda: 0.0, election_timeout=lambda: 1000.0, genesis_fsm=genesis,
    )
    engine.handle_message("n2", {
        "kind": "register_node", "protocol_version": 1, "rpc_id": str(uuid4()),
        "sender_slot": "n2", "sender_uuid": str(leader_uuid), "term": 0, "incarnation": 1,
        "membership_fingerprint": engine.membership_fingerprint(),
    })
    if commands:
        engine.handle_message("n2", {
            "kind": "append_entries", "protocol_version": 1, "rpc_id": str(uuid4()),
            "sender_slot": "n2", "sender_uuid": str(leader_uuid), "term": 1,
            "prev_log_index": 0, "prev_log_term": 0,
            "entries": [
                {"index": index, "term": 1, "command": command.model_dump(mode="json")}
                for index, command in enumerate(commands, start=1)
            ],
            "leader_commit": len(commands),
        })
    return engine


def _result(engine: RaftEngine, command: CommandEnvelopeDTO) -> str:
    return engine.state.request_results[_key(command)].result.status


def _deck(engine: RaftEngine, deck_id: str) -> dict[str, Any]:
    return deepcopy(engine.state.fsm["decks"][deck_id])


def _role(engine: RaftEngine, deck_id: str, user_id: str) -> str:
    return KeyValueStateMachine(engine.state.fsm).query_grants(
        deck_id, actor_user_id=user_id, applied_index=engine.state.raft.last_applied,
    )["effective_access"]


# ---------------------------------------------------------------------------
# Eligible migration
# ---------------------------------------------------------------------------

def test_authenticated_v2_publication_migrates_with_owner_and_preserved_state():
    history = _History()
    before = _deck(_follower(history.commands, genesis=history.genesis), history.deck_id)
    assert before["schema_version"] == 2 and before["locks"]

    migration = history.migration()
    engine = _follower([*history.commands, migration], genesis=history.genesis)

    assert _result(engine, migration) == "LEGACY_DECK_MIGRATED"
    after = _deck(engine, history.deck_id)
    expected = deepcopy(before)
    expected["schema_version"] = 3
    expected["metadata"]["owner_user_id"] = OWNER
    expected["access"] = {"schema_version": 1, "access_version": 0, "grants": {}}
    assert after == expected
    assert _role(engine, history.deck_id, OWNER) == "OWNER"
    assert _role(engine, history.deck_id, OTHER) == "NONE"


# ---------------------------------------------------------------------------
# Unprovable provenance never assigns ownership
# ---------------------------------------------------------------------------

def _assert_quarantined(engine: RaftEngine, deck_id: str, before: dict[str, Any]) -> None:
    assert _deck(engine, deck_id) == before
    machine = KeyValueStateMachine(engine.state.fsm)
    unknown = str(uuid4())
    for user in (OWNER, OTHER):
        assert machine.query_grants(deck_id, actor_user_id=user, applied_index=1) == {
            "status": "OK", "effective_access": "NONE",
        }
        assert machine.protected_read(
            "get_deck_preview", {"deck_id": deck_id}, actor_user_id=user, applied_index=1,
        ) == machine.protected_read(
            "get_deck_preview", {"deck_id": unknown}, actor_user_id=user, applied_index=1,
        ) == {"status": "ACCESS_DENIED"}


def test_pre_stage_e_actorless_publication_stays_quarantined():
    history = _History(enforced_from_index=1000, authenticated=False)
    before = _deck(_follower(history.commands, genesis=history.genesis), history.deck_id)
    migration = history.migration()
    engine = _follower([*history.commands, migration], genesis=history.genesis)
    assert _result(engine, migration) == "LEGACY_PROVENANCE_REJECTED"
    _assert_quarantined(engine, history.deck_id, before)


def test_claim_for_a_different_registered_owner_is_rejected():
    history = _History()
    before = _deck(_follower(history.commands, genesis=history.genesis), history.deck_id)
    migration = history.migration(owner=OTHER)
    engine = _follower([*history.commands, migration], genesis=history.genesis)
    assert _result(engine, migration) == "LEGACY_PROVENANCE_REJECTED"
    _assert_quarantined(engine, history.deck_id, before)


def test_rejected_publication_of_the_same_deck_is_not_provenance():
    # An actorless pre-Stage-E publication installs the deck; a later
    # authenticated attempt for the same deck commits only a rejection.
    history = _History(enforced_from_index=4, authenticated=False)
    late_client, late_credential = str(uuid4()), str(uuid4())
    rejected = _v2_publication(
        history.deck_id, client_id=late_client, request_id="late-publish",
        actor=actor_stamp(OTHER, late_credential),
    )
    commands = [
        history.commands[0],
        history.commands[1],
        issue_token_command(OTHER, late_client, late_credential, hashlib.sha256(b"late").hexdigest()),
        rejected,
        _activation(),
    ]
    before = _deck(_follower(commands, genesis=history.genesis), history.deck_id)
    migration = history.migration(
        owner=OTHER, original_client_id=late_client, original_request_id="late-publish",
    )
    engine = _follower([*commands, migration], genesis=history.genesis)
    assert _result(engine, rejected) == "DECK_ALREADY_PUBLISHED"
    assert _result(engine, migration) == "LEGACY_PROVENANCE_REJECTED"
    _assert_quarantined(engine, history.deck_id, before)


def test_unauthenticated_actor_stamp_publication_is_not_provenance():
    # A client-asserted actor whose credential was never issued is rejected at
    # original apply, so no deck and no provenance exist.
    deck_id, client_id = str(uuid4()), str(uuid4())
    forged = _v2_publication(
        deck_id, client_id=client_id, request_id="forged", actor=actor_stamp(OWNER, str(uuid4())),
    )
    migration = legacy_deck_migration_command(
        deck_id=deck_id, original_client_id=client_id, original_request_id="forged",
        owner_user_id=OWNER, origin_node_uuid=LEADER_UUID,
    )
    engine = _follower([forged, _activation(), migration], genesis=_genesis())
    assert _result(engine, forged) == "UNAUTHENTICATED"
    assert _result(engine, migration) == "LEGACY_DECK_NOT_FOUND"
    assert deck_id not in engine.state.fsm.get("decks", {})


def test_reference_to_a_non_publication_or_other_deck_is_rejected():
    history = _History()
    other = _History()
    lock = history.commands[2]
    commands = [*history.commands[:-1], *other.commands[:-1], _activation()]
    before = _deck(_follower(commands, genesis=history.genesis), history.deck_id)
    by_lock = history.migration(original_client_id=lock.client_id, original_request_id=lock.request_id)
    by_other_deck = history.migration(
        original_client_id=other.publication.client_id,
        original_request_id=other.publication.request_id, ordinal=1,
    )
    engine = _follower([*commands, by_lock, by_other_deck], genesis=history.genesis)
    assert _result(engine, by_lock) == "LEGACY_PROVENANCE_REJECTED"
    assert _result(engine, by_other_deck) == "LEGACY_PROVENANCE_REJECTED"
    _assert_quarantined(engine, history.deck_id, before)


def test_missing_original_request_is_unavailable():
    history = _History()
    before = _deck(_follower(history.commands, genesis=history.genesis), history.deck_id)
    migration = history.migration(original_request_id="never-committed")
    engine = _follower([*history.commands, migration], genesis=history.genesis)
    assert _result(engine, migration) == "LEGACY_PROVENANCE_UNAVAILABLE"
    _assert_quarantined(engine, history.deck_id, before)


def test_snapshot_without_retained_history_fails_closed():
    # A replica whose v2 deck arrived only as FSM state (no retained committed
    # publication or request-result record) cannot verify provenance.
    history = _History()
    source = _follower(history.commands, genesis=history.genesis)
    snapshot = deepcopy(source.state.fsm)
    before = _deck(source, history.deck_id)
    migration = history.migration()
    # Unrelated entries move this replica's log past the snapshot's v3 fence.
    padding = [
        CommandEnvelopeDTO(
            command_schema_version=1, request_id=f"pad-{index}", client_id="padding",
            origin_node_uuid=LEADER_UUID, operation="kv_put",
            payload={"key": "k", "value": str(index)},
        )
        for index in range(len(history.commands))
    ]
    engine = _follower([*padding, migration], genesis=snapshot)
    assert _result(engine, migration) == "LEGACY_PROVENANCE_UNAVAILABLE"
    _assert_quarantined(engine, history.deck_id, before)


def test_migration_before_the_v3_fence_is_unavailable():
    history = _History()
    without_activation = history.commands[:-1]
    before = _deck(_follower(without_activation, genesis=history.genesis), history.deck_id)
    migration = history.migration()
    engine = _follower([*without_activation, migration], genesis=history.genesis)
    assert _result(engine, migration) == "PRESENTATION_SCHEMA_UNAVAILABLE"
    assert _deck(engine, history.deck_id) == before


# ---------------------------------------------------------------------------
# Idempotence, conflicts and convergence
# ---------------------------------------------------------------------------

def test_exact_retry_is_idempotent_and_conflicting_claims_fail():
    history = _History()
    migration = history.migration()
    second_claim = history.migration(ordinal=1)
    conflicting = history.migration(owner=OTHER, ordinal=2)
    once = _follower([*history.commands, migration], genesis=history.genesis)
    engine = _follower(
        [*history.commands, migration, migration, second_claim, conflicting],
        genesis=history.genesis,
    )
    assert _result(engine, migration) == "LEGACY_DECK_MIGRATED"
    assert engine.state.request_results[_key(migration)].committed_index == len(history.commands) + 1
    assert _result(engine, second_claim) == "LEGACY_DECK_ALREADY_MIGRATED"
    assert _result(engine, conflicting) == "LEGACY_MIGRATION_CONFLICT"
    assert _deck(engine, history.deck_id) == _deck(once, history.deck_id)


def test_reused_migration_identity_with_a_different_claim_is_request_id_reuse():
    history = _History()
    migration = history.migration()
    reused = migration.model_copy(update={"payload": {**migration.payload, "owner_user_id": OTHER}})
    engine = _follower([*history.commands, migration, reused], genesis=history.genesis)
    assert _result(engine, migration) == "LEGACY_DECK_MIGRATED"
    assert _deck(engine, history.deck_id)["metadata"]["owner_user_id"] == OWNER


def test_restart_and_independent_replay_converge_without_startup_rewrite():
    history = _History()
    migration = history.migration()
    storage = InMemoryPersistence()
    _follower(history.commands, genesis=history.genesis, storage=storage)
    restarted = _follower([], genesis=history.genesis, storage=storage)
    # Restart alone never migrates an eligible deck.
    assert _deck(restarted, history.deck_id)["schema_version"] == 2
    assert "owner_user_id" not in _deck(restarted, history.deck_id)["metadata"]

    migrated_storage = InMemoryPersistence()
    first = _follower([*history.commands, migration], genesis=history.genesis, storage=migrated_storage)
    after_restart = _follower([], genesis=history.genesis, storage=migrated_storage)
    replayed = _follower([*history.commands, migration], genesis=history.genesis)
    for engine in (after_restart, replayed):
        assert engine.state.fsm == first.state.fsm
        assert engine.state.request_results == first.state.request_results


def test_replacement_leader_returns_the_committed_migration_outcome():
    history = _History()
    migration = history.migration()
    sent: list[tuple[str, dict[str, Any]]] = []
    engine = _follower([*history.commands, migration], genesis=history.genesis, sent=sent)
    engine.start_election()
    vote = next(message for slot, message in sent if slot == "n2" and message["kind"] == "request_vote")
    engine.handle_message("n2", {
        "kind": "request_vote_response", "protocol_version": 1, "rpc_id": str(uuid4()),
        "sender_slot": "n2", "sender_uuid": str(LEADER_UUID), "term": vote["term"],
        "reply_to": vote["rpc_id"], "vote_granted": True,
    })
    assert engine.role == "leader"
    log_length = len(engine.state.raft.log)
    retried = legacy_deck_migration_command(
        deck_id=history.deck_id, original_client_id=history.publication.client_id,
        original_request_id=history.publication.request_id, owner_user_id=OWNER,
        origin_node_uuid=engine.node_uuid,
    )
    outcome = engine.propose(retried)
    assert getattr(outcome, "status", None) == "LEGACY_DECK_MIGRATED"
    assert len(engine.state.raft.log) == log_length


def test_migration_envelope_is_internal_only():
    history = _History()
    validate_committed_command(history.migration())
    forged = history.migration().model_dump(mode="json")
    forged["client_id"] = history.client_id
    with pytest.raises(ValueError):
        validate_committed_command(CommandEnvelopeDTO.model_validate(forged))
    wrong_id = history.migration().model_dump(mode="json")
    wrong_id["request_id"] = "migrate-legacy-deck:" + str(uuid4())
    with pytest.raises(ValueError):
        validate_committed_command(CommandEnvelopeDTO.model_validate(wrong_id))


# ---------------------------------------------------------------------------
# App: leader-only internal proposal, public adapter, protected reads
# ---------------------------------------------------------------------------

class LegacyMigrationAppTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replicas: list[App] = []
        self.tokens: dict[str, str] = {}

    def tearDown(self) -> None:
        for replica in self.replicas:
            replica._password_verifier.shutdown()
            replica._events.close()
            replica._log.close()
        self.workspace.cleanup()

    def _leader(self) -> App:
        replica = App("n1", self.nodes, self.run_dir)
        replica.start()
        self.replicas.append(replica)
        self._elect_and_commit_noop(replica)
        return replica

    def _login(self, replica: App, user_id: str) -> tuple[str, dict[str, object]]:
        client_id, credential_id = str(uuid4()), str(uuid4())
        token = "migration-bearer-" + str(uuid4())
        self._commit_command(replica, issue_token_command(
            user_id, client_id, credential_id, hashlib.sha256(token.encode("ascii")).hexdigest(),
            request_id="migration-issue-" + client_id,
        ))
        self.tokens[client_id] = token
        return client_id, actor_stamp(user_id, credential_id)

    def _read(self, replica: App, op: str, args: dict, client: str) -> dict:
        request = Request("read-" + str(uuid4()), op, {**args, "_auth": {"access_token": self.tokens[client]}}, client)
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        immediate = replica.on_request(request)
        if immediate is not None:
            return immediate
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        self.assertTrue(done.is_set(), "protected read produced no reply")
        return response

    def _acknowledge_append(self, replica: App) -> None:
        append = next(
            message for destination, message in self._messages(replica)
            if destination == "n2" and message.get("kind") == "append_entries" and message.get("entries")
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": append["term"], "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self._messages(replica)

    def test_public_adapter_never_accepts_a_migration_request(self) -> None:
        replica = self._leader()
        log_length = len(replica.engine.state.raft.log)
        response = replica.on_request(Request("forged", "migrate_legacy_deck", {
            "deck_id": str(uuid4()), "original_client_id": "c", "original_request_id": "r",
            "owner_user_id": OWNER,
        }, "client"))
        self.assertEqual(response["result"]["status"], "INVALID_REQUEST")
        self.assertEqual(len(replica.engine.state.raft.log), log_length)

    def test_leader_proposes_verified_migration_and_quarantine_lifts_only_for_owner(self) -> None:
        replica = self._leader()
        owner, owner_actor = self._login(replica, OWNER)
        other, _ = self._login(replica, OTHER)
        deck_id = str(uuid4())
        publication = _v2_publication(deck_id, client_id=owner, request_id="historic", actor=owner_actor)
        self._commit_command(replica, publication)
        self._commit_command(replica, _activation())
        self.assertEqual(replica.engine.state.fsm["decks"][deck_id]["schema_version"], 2)

        for client in (owner, other):
            self.assertEqual(
                self._read(replica, "get_deck_preview", {"deck_id": deck_id}, client)["result"],
                self._read(replica, "get_deck_preview", {"deck_id": str(uuid4())}, client)["result"],
            )
            self.assertEqual(
                self._read(replica, "query_grants", {"deck_id": deck_id}, client)["result"],
                {"status": "OK", "effective_access": "NONE"},
            )
        # Leadership, ticks and reads never migrate on their own.
        replica.on_tick()
        self.assertEqual(replica.engine.state.fsm["decks"][deck_id]["schema_version"], 2)

        proposal = replica.propose_legacy_deck_migration(deck_id)
        self.assertEqual(proposal["status"], "PROPOSED")
        self._acknowledge_append(replica)
        self.assertEqual(replica.engine.state.fsm["decks"][deck_id]["metadata"]["owner_user_id"], OWNER)

        preview = self._read(replica, "get_deck_preview", {"deck_id": deck_id}, owner)["result"]
        self.assertEqual((preview["status"], preview["effective_access"]), ("OK", "OWNER"))
        self.assertEqual(
            self._read(replica, "get_deck_preview", {"deck_id": deck_id}, other)["result"],
            {"status": "ACCESS_DENIED"},
        )

        log_length = len(replica.engine.state.raft.log)
        retry = replica.propose_legacy_deck_migration(deck_id)
        self.assertEqual(retry["status"], "LEGACY_DECK_MIGRATED")
        self.assertEqual(len(replica.engine.state.raft.log), log_length)

    def test_unprovable_deck_and_follower_do_not_propose(self) -> None:
        replica = self._leader()
        log_length = len(replica.engine.state.raft.log)
        self.assertEqual(
            replica.propose_legacy_deck_migration(str(uuid4()))["status"],
            "LEGACY_PROVENANCE_UNAVAILABLE",
        )
        self.assertEqual(len(replica.engine.state.raft.log), log_length)
        replica.engine.role = "follower"
        self.assertEqual(replica.propose_legacy_deck_migration(str(uuid4()))["status"], "NOT_LEADER")

