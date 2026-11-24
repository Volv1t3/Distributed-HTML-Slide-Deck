"""Stage E.0 authentication catalog and activation acceptance tests."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from pydantic import ValidationError

from auth_catalog import load_account_catalog
from auth_dto import (
    AccountCatalogDTO,
    AuthStateDTO,
    PasswordVerifierDTO,
    catalog_fingerprint,
    initial_auth_state,
    verify_password,
)
from command_dto import ClientResultDTO, CommandEnvelopeDTO, validate_committed_command
from app import App, AuthCatalogStatus
from dto_base import sha256_json
from node import Request
from persistence import InMemoryPersistence, new_persisted_state
from raft_core import RaftEngine
from raft_dto import DedupRecordDTO, LogEntryDTO, MAX_ENTRY_BYTES
from state_machine import KeyValueStateMachine


class AuthCatalogTests(unittest.TestCase):
    def test_frozen_catalog_is_canonical_deterministic_and_verifiable(self) -> None:
        first = load_account_catalog()
        second = load_account_catalog()

        self.assertEqual(first.model_dump(mode="json"), second.model_dump(mode="json"))
        self.assertEqual(catalog_fingerprint(first), catalog_fingerprint(second))
        self.assertEqual(len(catalog_fingerprint(first)), 64)
        self.assertEqual(
            [user.profile.email for user in first.users],
            ["elena@example.test", "mateo@example.test", "sofia@example.test"],
        )
        sofia = next(user for user in first.users if user.profile.email == "sofia@example.test")
        self.assertTrue(verify_password("Sofia-Classroom-1", sofia.verifier))
        self.assertFalse(verify_password("not-the-password", sofia.verifier))
        serialized = repr(initial_auth_state(first, enforced_from_index=1)).lower()
        self.assertNotIn("sofia-classroom-1", serialized)

    def test_catalog_and_auth_dtos_reject_malformed_identity_data(self) -> None:
        catalog = load_account_catalog()
        raw = catalog.model_dump(mode="json")

        extra = deepcopy(raw)
        extra["users"][0]["profile"]["unknown"] = True
        with self.assertRaises(ValidationError):
            AccountCatalogDTO.model_validate(extra)

        duplicate_id = deepcopy(raw)
        duplicate_id["users"][1]["profile"]["user_id"] = duplicate_id["users"][0]["profile"]["user_id"]
        with self.assertRaises(ValidationError):
            AccountCatalogDTO.model_validate(duplicate_id)

        duplicate_email = deepcopy(raw)
        duplicate_email["users"][1]["profile"]["email"] = f"  {duplicate_email['users'][0]['profile']['email'].upper()}  "
        with self.assertRaises(ValidationError):
            AccountCatalogDTO.model_validate(duplicate_email)

        verifier = raw["users"][0]["verifier"]
        for field, value in (
            ("iterations", True),
            ("iterations", 599_999),
            ("salt_hex", "A" * 32),
            ("hash_hex", "g" * 64),
        ):
            malformed = {**verifier, field: value}
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                PasswordVerifierDTO.model_validate(malformed)

        auth = initial_auth_state(catalog, enforced_from_index=1)
        malformed_auth = deepcopy(auth)
        malformed_auth["enforced_from_index"] = True
        with self.assertRaises(ValidationError):
            AuthStateDTO.model_validate(malformed_auth)

    def test_catalog_activation_command_fits_real_raft_entry_limit(self) -> None:
        catalog = load_account_catalog()
        command = self.activation_command(catalog)
        validate_committed_command(command)
        entry = LogEntryDTO(index=1, term=1, command=command)
        self.assertLess(len(entry.model_dump_json().encode()), MAX_ENTRY_BYTES)

        public_spoof = command.model_copy(update={"client_id": "external-client"})
        with self.assertRaises(ValueError):
            validate_committed_command(public_spoof)
        wrong_identity = command.model_copy(update={"request_id": "nondeterministic"})
        with self.assertRaises(ValueError):
            validate_committed_command(wrong_identity)
        validate_committed_command(
            command.model_copy(update={"request_id": f"{command.request_id}:1"})
        )
        for suffix in ("", "0", "01", "-1", "collision"):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                validate_committed_command(
                    command.model_copy(
                        update={"request_id": f"{command.request_id}:{suffix}"}
                    )
                )

    def test_fresh_genesis_uses_auth_only_when_explicitly_configured(self) -> None:
        catalog = load_account_catalog()
        genesis = {"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)}
        state = new_persisted_state(uuid4(), "a" * 64, initial_fsm=genesis)
        genesis["auth"]["client_bindings"][str(uuid4())] = str(uuid4())

        self.assertEqual(state.fsm["auth"]["enforced_from_index"], 1)
        self.assertEqual(state.fsm["auth"]["client_bindings"], {})
        self.assertEqual(new_persisted_state(uuid4(), "b" * 64).fsm, {"kv": {}})

        storage = InMemoryPersistence()
        node_uuid = uuid4()
        engine = RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=node_uuid,
            storage=storage,
            send=lambda _destination, _message: None,
            clock=lambda: 0.0,
            genesis_fsm={"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)},
        )
        first_auth = deepcopy(engine.state.fsm["auth"])
        restarted = RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=node_uuid,
            storage=storage,
            send=lambda _destination, _message: None,
            clock=lambda: 0.0,
            genesis_fsm={"kv": {"must": "not merge"}},
        )
        self.assertEqual(restarted.state.fsm["auth"], first_auth)
        self.assertNotIn("must", restarted.state.fsm["kv"])

    def test_state_machine_activation_is_indexed_idempotent_and_preserving(self) -> None:
        catalog = load_account_catalog()
        command = self.activation_command(catalog)
        before = {
            "kv": {"legacy": "preserved"},
            "decks": {
                "deck": {
                    "schema_version": 2,
                    "metadata": {"name": "Existing", "width_px": 1280, "height_px": 720},
                    "slides": [],
                    "deck_version": 4,
                    "deck_settings_version": 2,
                    "slide_tombstones": {"old-slide": {"deleted": True}},
                    "element_tombstones": {"old-element": {"deleted": True}},
                    "locks": {"locked": {"session_id": "s", "fencing_token": 7, "lease_epoch": 3}},
                    "fencing_counters": {"locked": 7},
                    "next_creation_sequence": 9,
                }
            },
        }
        machine = KeyValueStateMachine(deepcopy(before))

        with self.assertRaises(ValueError):
            machine.apply(command)
        applied = machine.apply(command, committed_index=12)
        after = machine.snapshot()
        self.assertEqual(applied.status, "AUTH_CATALOG_INITIALIZED")
        self.assertEqual(after["auth"]["enforced_from_index"], 13)
        self.assertEqual({key: value for key, value in after.items() if key != "auth"}, before)

        stable = deepcopy(after)
        repeated = machine.apply(command, committed_index=99)
        self.assertEqual(repeated.status, "AUTH_CATALOG_ALREADY_INITIALIZED")
        self.assertEqual(machine.snapshot(), stable)

        changed = catalog.model_dump(mode="json")
        changed["users"][0]["profile"]["email"] = "different@example.test"
        other_catalog = AccountCatalogDTO.model_validate(changed)
        mismatch = machine.apply(self.activation_command(other_catalog), committed_index=100)
        self.assertEqual(mismatch.status, "AUTH_CATALOG_MISMATCH")
        self.assertEqual(machine.snapshot(), stable)

    def test_unrelated_commits_retain_validated_auth_state(self) -> None:
        catalog = load_account_catalog()
        auth = initial_auth_state(catalog, enforced_from_index=1)
        machine = KeyValueStateMachine({"kv": {}, "auth": auth})
        put = CommandEnvelopeDTO(
            command_schema_version=1,
            request_id="put-after-auth",
            client_id="legacy-client",
            origin_node_uuid=uuid4(),
            operation="kv_put",
            payload={"key": "deck", "value": "value"},
        )
        self.assertEqual(machine.apply(put, committed_index=1).status, "OK")
        self.assertEqual(machine.snapshot()["auth"], auth)

    def test_new_leader_reuses_inherited_uncommitted_initializer_and_replicas_converge(self) -> None:
        catalog = load_account_catalog()
        inherited = self.activation_command(catalog)
        node_ids = {slot: uuid4() for slot in ("n1", "n2", "n3")}
        voters = set(node_ids)

        def inherited_storage(slot: str) -> InMemoryPersistence:
            storage = InMemoryPersistence()
            state = new_persisted_state(node_ids[slot], sha256_json(sorted(voters)))
            state.raft.current_term = 1
            state.raft.log = [LogEntryDTO(index=1, term=1, command=inherited)]
            state.peer_bindings = {
                peer: node_id for peer, node_id in node_ids.items() if peer != slot
            }
            storage.save(state)
            return storage

        leader_sent: list[tuple[str, dict[str, object]]] = []
        follower_sent: list[tuple[str, dict[str, object]]] = []
        leader = RaftEngine(
            node_slot="n1",
            voters=voters,
            node_uuid=node_ids["n1"],
            storage=inherited_storage("n1"),
            send=lambda destination, message: leader_sent.append((destination, message)),
            clock=lambda: 0.0,
            election_timeout=lambda: 1.0,
        )
        follower = RaftEngine(
            node_slot="n2",
            voters=voters,
            node_uuid=node_ids["n2"],
            storage=inherited_storage("n2"),
            send=lambda destination, message: follower_sent.append((destination, message)),
            clock=lambda: 0.0,
            election_timeout=lambda: 1.0,
        )

        leader.start_election()
        vote = next(
            message for destination, message in leader_sent
            if destination == "n2" and message["kind"] == "request_vote"
        )
        leader.handle_message("n2", {
            "kind": "request_vote_response",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n2",
            "sender_uuid": str(node_ids["n2"]),
            "term": 2,
            "reply_to": vote["rpc_id"],
            "vote_granted": True,
        })

        retried = inherited.model_copy(update={"origin_node_uuid": node_ids["n1"]})
        self.assertEqual(leader.propose(retried).index, 1)
        self.assertEqual(
            [entry.command.operation for entry in leader.state.raft.log],
            ["initialize_auth_catalog", "raft.noop"],
        )
        self.assertEqual(leader.state.raft.log[-1].term, 2)

        append = next(
            message for destination, message in reversed(leader_sent)
            if destination == "n2" and message["kind"] == "append_entries"
        )
        follower.handle_message("n1", append)
        acknowledgment = follower_sent[-1][1]
        leader.handle_message("n2", acknowledgment)
        commit = next(
            message for destination, message in reversed(leader_sent)
            if destination == "n2" and message["kind"] == "append_entries"
            and message["leader_commit"] == 2
        )
        follower.handle_message("n1", commit)

        self.assertEqual(leader.state.raft.commit_index, 2)
        self.assertEqual(follower.state.raft.commit_index, 2)
        self.assertEqual(leader.state.fsm["auth"], follower.state.fsm["auth"])
        self.assertEqual(leader.state.fsm["auth"]["enforced_from_index"], 2)
        self.assertEqual(
            sum(
                entry.command.operation == "initialize_auth_catalog"
                for entry in leader.state.raft.log
            ),
            1,
        )

    @staticmethod
    def activation_command(catalog: AccountCatalogDTO, *, request_id: str | None = None) -> CommandEnvelopeDTO:
        fingerprint = catalog_fingerprint(catalog)
        return CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=request_id or f"initialize-auth-catalog-v1:{fingerprint}",
            client_id="_auth_catalog",
            origin_node_uuid=uuid4(),
            operation="initialize_auth_catalog",
            payload={
                "schema_version": 1,
                "catalog_fingerprint": fingerprint,
                "users": [user.model_dump(mode="json") for user in catalog.users],
            },
        )


class AppAuthCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.apps: list[App] = []

    def tearDown(self) -> None:
        for app in self.apps:
            app._events.close()
            app._log.close()
        self.workspace.cleanup()

    def start_app(self) -> App:
        app = App("n1", self.nodes, self.run_dir)
        app.start()
        self.apps.append(app)
        return app

    def write_stage_d_disk(self, *, auth: dict[str, object] | None = None) -> None:
        fsm: dict[str, object] = {"kv": {"legacy": "preserved"}}
        if auth is not None:
            fsm["auth"] = auth
        state = new_persisted_state(
            uuid4(),
            sha256_json(sorted(self.nodes)),
            initial_fsm=fsm,
        )
        path = self.run_dir / "n1" / "disk.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"raft_state": state.model_dump(mode="json")}), encoding="utf-8")

    def test_fresh_app_is_ready_and_internal_initializer_is_not_public(self) -> None:
        app = self.start_app()
        self.assertIs(app.auth_catalog_status, AuthCatalogStatus.READY)
        self.assertTrue(app.auth_catalog_ready)
        self.assertEqual(app.status()["auth_catalog_status"], "READY")
        self.assertTrue(app.status()["auth_catalog_ready"])
        self.assertEqual(app.engine.state.fsm["auth"]["enforced_from_index"], 1)
        response = app.on_request(Request("spoof", "initialize_auth_catalog", {}, "external"))
        self.assertEqual(response["result"]["status"], "INVALID_REQUEST")

    def test_existing_state_activates_once_through_raft_and_then_becomes_ready(self) -> None:
        self.write_stage_d_disk()
        app = self.start_app()
        self.assertIs(app.auth_catalog_status, AuthCatalogStatus.PENDING)
        self.assertFalse(app.auth_catalog_ready)

        app.engine.role = "leader"
        app.engine.state.raft.current_term = 1
        app.engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
        app.engine.next_index = {"n2": 1, "n3": 1}
        app._ensure_auth_catalog_activation()
        app._ensure_auth_catalog_activation()
        initializers = [
            entry for entry in app.engine.state.raft.log
            if entry.command.operation == "initialize_auth_catalog"
        ]
        self.assertEqual(len(initializers), 1)
        self.assertEqual(app.engine.state.fsm["kv"], {"legacy": "preserved"})
        events = (self.run_dir / "n1" / "events.log").read_text(encoding="utf-8")
        self.assertNotIn("hash_hex", events)
        self.assertNotIn("sofia@example.test", events)

        app.engine.state.raft.commit_index = initializers[0].index
        app.engine._apply_committed()
        app._refresh_auth_catalog_status()
        self.assertIs(app.auth_catalog_status, AuthCatalogStatus.READY)
        self.assertEqual(app.engine.state.fsm["auth"]["enforced_from_index"], 2)
        self.assertEqual(app.engine.state.fsm["kv"], {"legacy": "preserved"})

    def test_legacy_activation_identity_collision_is_preserved_and_bypassed(self) -> None:
        catalog = load_account_catalog()
        base_command = AuthCatalogTests.activation_command(catalog)
        legacy_command = CommandEnvelopeDTO(
            command_schema_version=1,
            request_id="legacy-write",
            client_id="legacy-client",
            origin_node_uuid=uuid4(),
            operation="kv_put",
            payload={"key": "legacy", "value": "preserved"},
        )
        state = new_persisted_state(
            uuid4(),
            sha256_json(sorted(self.nodes)),
            initial_fsm={"kv": {"legacy": "preserved"}},
        )
        state.raft.current_term = 1
        state.raft.log = [LogEntryDTO(index=1, term=1, command=legacy_command)]
        state.raft.commit_index = 1
        state.raft.last_applied = 1
        collision_key = sha256_json([base_command.client_id, base_command.request_id])
        state.request_results[collision_key] = DedupRecordDTO(
            fingerprint="0" * 64,
            result=ClientResultDTO(status="LEGACY_RESULT", value="unchanged"),
            committed_index=1,
        )
        preserved_collision = state.request_results[collision_key].model_dump(mode="json")
        path = self.run_dir / "n1" / "disk.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps({"raft_state": state.model_dump(mode="json")}),
            encoding="utf-8",
        )

        app = self.start_app()
        app.engine.role = "leader"
        app.engine.state.raft.current_term = 2
        app.engine.match_index = {"n1": 1, "n2": 0, "n3": 0}
        app.engine.next_index = {"n2": 2, "n3": 2}
        app._ensure_auth_catalog_activation()

        initializer = next(
            entry for entry in app.engine.state.raft.log
            if entry.command.operation == "initialize_auth_catalog"
        )
        self.assertEqual(initializer.command.request_id, f"{base_command.request_id}:1")
        validate_committed_command(initializer.command)
        self.assertEqual(
            app.engine.state.request_results[collision_key].model_dump(mode="json"),
            preserved_collision,
        )

        app.engine.state.raft.commit_index = initializer.index
        app.engine._apply_committed()
        app._refresh_auth_catalog_status()
        self.assertTrue(app.auth_catalog_ready)
        self.assertEqual(app.engine.state.fsm["kv"], {"legacy": "preserved"})
        self.assertEqual(
            app.engine.state.request_results[collision_key].model_dump(mode="json"),
            preserved_collision,
        )

    def test_malformed_persisted_auth_preserves_state_and_refuses_readiness(self) -> None:
        self.write_stage_d_disk(
            auth={
                "schema_version": 1,
                "catalog_fingerprint": "a" * 64,
                "enforced_from_index": 1,
                "users": {},
            }
        )
        stored_raw = json.loads(
            (self.run_dir / "n1" / "disk.json").read_text(encoding="utf-8")
        )["raft_state"]["fsm"]["auth"]

        app = self.start_app()

        self.assertIs(app.auth_catalog_status, AuthCatalogStatus.FINGERPRINT_MISMATCH)
        self.assertFalse(app.auth_catalog_ready)
        self.assertEqual(app.engine.state.fsm["auth"], stored_raw)
        self.assertEqual(app.engine.state.fsm["kv"], {"legacy": "preserved"})

        catalog = load_account_catalog()
        command = AuthCatalogTests.activation_command(catalog)
        machine = KeyValueStateMachine(app.engine.state.fsm)
        result = machine.apply(command, committed_index=1)
        self.assertEqual(result.status, "AUTH_CATALOG_MISMATCH")
        self.assertEqual(machine.snapshot()["auth"], stored_raw)

    def test_configured_fingerprint_mismatch_preserves_state_and_refuses_readiness(self) -> None:
        catalog = load_account_catalog().model_dump(mode="json")
        catalog["users"][0]["profile"]["email"] = "configured-elsewhere@example.test"
        other = AccountCatalogDTO.model_validate(catalog)
        stored_auth = initial_auth_state(other, enforced_from_index=7)
        self.write_stage_d_disk(auth=stored_auth)

        app = self.start_app()
        self.assertIs(app.auth_catalog_status, AuthCatalogStatus.FINGERPRINT_MISMATCH)
        self.assertFalse(app.auth_catalog_ready)
        self.assertEqual(app.engine.state.fsm["auth"], stored_auth)
        self.assertEqual(app.engine.state.fsm["kv"], {"legacy": "preserved"})


if __name__ == "__main__":
    unittest.main()
