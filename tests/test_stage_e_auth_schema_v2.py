"""Auth schema v2: permanent retirement history and committed v1 migration.

Issue #39 follow-up. These tests deliberately exercise the Raft-neutral FSM,
the independent sequential model, the strict DTO boundary, and the real
leader adapter's internal upgrade path.
"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from pydantic import ValidationError

from app import App, AuthCatalogStatus
from auth_catalog import load_account_catalog
from auth_dto import (
    AuthStateDTO,
    AuthStateV1DTO,
    RetiredCredentialDTO,
    auth_schema_upgrade_request_id,
    initial_auth_state,
    upgrade_auth_state_v2,
    validate_auth_state,
)
from command_dto import CommandEnvelopeDTO, validate_committed_command
from dto_base import sha256_json
from node import Request
from persistence import new_persisted_state
from state_machine import KeyValueStateMachine
import spec


class CredentialHistoryV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user = self.catalog.users[0].profile.user_id
        self.client = uuid4()
        self.auth = initial_auth_state(self.catalog, enforced_from_index=1)

    def issue(self, *, client=None, user=None, credential=None, digest="a" * 64, request="login-1"):
        client = client or self.client
        return CommandEnvelopeDTO(
            command_schema_version=1,
            client_id=str(client),
            request_id=request,
            origin_node_uuid=uuid4(),
            operation="issue_access_token",
            payload={
                "user_id": str(user or self.user),
                "client_id": str(client),
                "credential_id": str(credential or uuid4()),
                "token_digest": digest,
            },
        )

    @staticmethod
    def upgrade(fingerprint: str, *, request_id=None):
        return CommandEnvelopeDTO(
            command_schema_version=1,
            client_id="_auth_schema",
            request_id=request_id or auth_schema_upgrade_request_id(fingerprint),
            origin_node_uuid=uuid4(),
            operation="upgrade_auth_schema_v2",
            payload={"schema_version": 2, "catalog_fingerprint": fingerprint},
        )

    def test_v2_requires_explicit_history_and_v1_is_not_silently_mutated(self):
        self.assertEqual(self.auth["schema_version"], 2)
        self.assertEqual(self.auth["retired_credentials"], {})
        legacy = deepcopy(self.auth)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        self.assertEqual(validate_auth_state(legacy), legacy)
        self.assertEqual(AuthStateV1DTO.model_validate(legacy).schema_version, 1)
        with self.assertRaises(ValidationError):
            AuthStateDTO.model_validate(legacy)
        malformed = deepcopy(self.auth)
        del malformed["retired_credentials"]
        with self.assertRaises(ValidationError):
            validate_auth_state(malformed)
        self.assertEqual(KeyValueStateMachine({"kv": {}, "auth": legacy}).snapshot()["auth"], legacy)

    def test_rotation_retains_tombstone_and_rejects_historical_id_and_digest(self):
        machine = KeyValueStateMachine({"kv": {}, "auth": self.auth})
        first = self.issue(digest="a" * 64)
        second = self.issue(digest="b" * 64, request="login-2")
        self.assertEqual(machine.apply(first, committed_index=1).status, "TOKEN_ISSUED")
        self.assertEqual(machine.apply(second, committed_index=2).status, "TOKEN_ISSUED")
        auth = machine.snapshot()["auth"]
        retired = auth["retired_credentials"][first.payload["credential_id"]]
        self.assertEqual(retired["retirement_reason"], "ROTATED")
        self.assertEqual(retired["credential_generation"], 1)
        self.assertEqual(retired["token_digest"], "a" * 64)
        self.assertEqual(auth["active_credentials"][str(self.client)]["credential_generation"], 2)
        self.assertEqual(len(auth["active_credentials"]), 1)

        same_id = self.issue(credential=first.payload["credential_id"], digest="c" * 64, request="login-3")
        same_digest = self.issue(digest="a" * 64, request="login-4")
        self.assertEqual(machine.apply(same_id, committed_index=3).status, "CREDENTIAL_ID_REUSE")
        self.assertEqual(machine.apply(same_digest, committed_index=4).status, "TOKEN_DIGEST_REUSE")
        self.assertEqual(machine.snapshot()["auth"], auth)
        AuthStateDTO.model_validate(auth)

    def test_reused_digest_across_clients_and_error_precedence_match_reference_model(self):
        machine = KeyValueStateMachine({"kv": {}, "auth": self.auth})
        reference = None
        first = self.issue(digest="a" * 64)
        other_client = uuid4()
        second = self.issue(client=other_client, digest="b" * 64, request="login-2")
        both_conflicts = self.issue(
            client=uuid4(), credential=second.payload["credential_id"],
            digest="a" * 64, request="login-3",
        )
        for index, command in enumerate((first, second, both_conflicts), start=1):
            result = machine.apply(command, committed_index=index)
            reference, expected = spec.apply(
                reference, "issue_access_token", {**command.payload, "request_id": command.request_id}
            )
            self.assertEqual(result.model_dump(mode="json"), expected)
        self.assertEqual(result.status, "CREDENTIAL_ID_REUSE")
        self.assertEqual(
            machine.snapshot()["auth"]["active_credentials"],
            reference["active_credentials"],
        )
        self.assertEqual(
            machine.snapshot()["auth"]["retired_credentials"],
            reference["retired_credentials"],
        )

    def test_revoked_current_record_is_retired_as_revoked_on_next_rotation(self):
        machine = KeyValueStateMachine({"kv": {}, "auth": self.auth})
        first = self.issue()
        self.assertEqual(machine.apply(first, committed_index=1).status, "TOKEN_ISSUED")
        snapshot = machine.snapshot()
        snapshot["auth"]["active_credentials"][str(self.client)]["revoked"] = True
        machine = KeyValueStateMachine(snapshot)
        second = self.issue(digest="b" * 64, request="login-2")
        self.assertEqual(machine.apply(second, committed_index=2).status, "TOKEN_ISSUED")
        retired = machine.snapshot()["auth"]["retired_credentials"][first.payload["credential_id"]]
        self.assertEqual(retired["retirement_reason"], "REVOKED")
        self.assertFalse(machine.snapshot()["auth"]["active_credentials"][str(self.client)]["revoked"])

    def test_malformed_retirement_history_is_rejected(self):
        machine = KeyValueStateMachine({"kv": {}, "auth": self.auth})
        first = self.issue()
        second = self.issue(digest="b" * 64, request="login-2")
        machine.apply(first, committed_index=1)
        machine.apply(second, committed_index=2)
        raw = machine.snapshot()["auth"]
        malformed_cases = []
        case = deepcopy(raw)
        case["retired_credentials"][first.payload["credential_id"]]["token_digest"] = "b" * 64
        malformed_cases.append(case)
        case = deepcopy(raw)
        case["retired_credentials"][first.payload["credential_id"]]["credential_id"] = str(uuid4())
        malformed_cases.append(case)
        case = deepcopy(raw)
        case["retired_credentials"][first.payload["credential_id"]]["credential_generation"] = 2
        malformed_cases.append(case)
        case = deepcopy(raw)
        case["retired_credentials"][first.payload["credential_id"]]["unexpected"] = "secret"
        malformed_cases.append(case)
        for candidate in malformed_cases:
            with self.subTest(candidate=candidate), self.assertRaises(ValidationError):
                AuthStateDTO.model_validate(candidate)

        with self.assertRaises(ValidationError):
            RetiredCredentialDTO.model_validate({**next(iter(raw["retired_credentials"].values())), "retirement_reason": "EXPIRED"})

    def test_committed_migration_preserves_legacy_current_credentials_and_other_state(self):
        legacy = deepcopy(self.auth)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        client = str(self.client)
        cred = str(uuid4())
        legacy["client_bindings"][client] = str(self.user)
        legacy["active_credentials"][client] = {
            "user_id": str(self.user), "client_id": client,
            "credential_id": cred, "token_digest": "a" * 64,
            "credential_generation": 5, "revoked": True,
        }
        original = {"kv": {"legacy": "unchanged"}, "auth": legacy}
        machine = KeyValueStateMachine(original)
        self.assertEqual(machine.snapshot(), original)
        command = self.upgrade(legacy["catalog_fingerprint"])
        validate_committed_command(command)
        self.assertEqual(machine.apply(command, committed_index=8).status, "AUTH_SCHEMA_UPGRADED")
        upgraded = machine.snapshot()
        self.assertEqual(upgraded["kv"], original["kv"])
        self.assertEqual(upgraded["auth"]["schema_version"], 2)
        self.assertEqual(upgraded["auth"]["retired_credentials"], {})
        self.assertEqual(upgraded["auth"]["active_credentials"], legacy["active_credentials"])
        self.assertEqual(upgraded["auth"]["enforced_from_index"], 1)
        self.assertEqual(machine.apply(command, committed_index=9).status, "AUTH_SCHEMA_ALREADY_V2")
        newer = self.issue(digest="b" * 64, request="new-login")
        self.assertEqual(machine.apply(newer, committed_index=10).value["credential_generation"], 6)
        self.assertEqual(machine.snapshot()["auth"]["retired_credentials"][cred]["retirement_reason"], "REVOKED")

    def test_colliding_v1_records_are_quarantined_by_committed_upgrade(self):
        legacy = deepcopy(self.auth)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        c1, c2 = str(uuid4()), str(uuid4())
        for client in (c1, c2):
            legacy["client_bindings"][client] = str(self.user)
            legacy["active_credentials"][client] = {
                "user_id": str(self.user), "client_id": client,
                "credential_id": str(uuid4()), "token_digest": "f" * 64,
                "credential_generation": 1, "revoked": False,
            }
        machine = KeyValueStateMachine({"kv": {}, "auth": legacy})
        command = self.upgrade(legacy["catalog_fingerprint"])
        outcome = machine.apply(command, committed_index=3)
        self.assertEqual(outcome.status, "AUTH_SCHEMA_UPGRADED")
        self.assertEqual(outcome.details["quarantined_clients"], 2)
        upgraded = machine.snapshot()["auth"]
        self.assertEqual(upgraded["schema_version"], 2)
        self.assertEqual(upgraded["active_credentials"], {})
        self.assertEqual(upgraded["legacy_quarantined_credentials"], legacy["active_credentials"])
        self.assertEqual(upgraded["client_bindings"], legacy["client_bindings"])
        mismatch = self.upgrade("0" * 64)
        self.assertEqual(machine.apply(mismatch, committed_index=4).status, "AUTH_CATALOG_MISMATCH")
        self.assertEqual(machine.snapshot()["auth"], upgraded)
        first_client = c1
        original = legacy["active_credentials"][first_client]
        self.assertEqual(
            machine.apply(self.issue(client=first_client, digest=original["token_digest"], request="reused"), committed_index=5).status,
            "TOKEN_DIGEST_REUSE",
        )
        rotated = self.issue(client=first_client, digest="b" * 64, request="fresh-login")
        self.assertEqual(machine.apply(rotated, committed_index=6).value["credential_generation"], 2)
        self.assertEqual(machine.snapshot()["auth"]["legacy_quarantined_credentials"], legacy["active_credentials"])
        self.assertEqual(machine.snapshot()["auth"]["active_credentials"][first_client]["token_digest"], "b" * 64)
        AuthStateDTO.model_validate(machine.snapshot()["auth"])

    def test_migration_quarantines_id_collision_even_when_digest_is_distinct(self):
        legacy = deepcopy(self.auth)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        same_id = str(uuid4())
        clients = [str(uuid4()) for _ in range(3)]
        for i, client in enumerate(clients):
            legacy["client_bindings"][client] = str(self.user)
            legacy["active_credentials"][client] = {
                "user_id": str(self.user), "client_id": client,
                "credential_id": same_id if i < 2 else str(uuid4()),
                "token_digest": str(i + 1) * 64,
                "credential_generation": 3, "revoked": False,
            }
        machine = KeyValueStateMachine({"kv": {"untouched": "yes"}, "auth": legacy})
        result = machine.apply(self.upgrade(legacy["catalog_fingerprint"]), committed_index=1)
        self.assertEqual(result.status, "AUTH_SCHEMA_UPGRADED")
        self.assertEqual(result.details["quarantined_clients"], 2)
        upgraded = machine.snapshot()["auth"]
        self.assertEqual(set(upgraded["legacy_quarantined_credentials"]), set(clients[:2]))
        self.assertEqual(set(upgraded["active_credentials"]), {clients[2]})
        self.assertEqual(machine.snapshot()["kv"], {"untouched": "yes"})
        self.assertEqual(
            machine.apply(self.issue(client=clients[0], credential=same_id, digest="9" * 64), committed_index=2).status,
            "CREDENTIAL_ID_REUSE",
        )
        self.assertEqual(
            machine.apply(self.issue(client=clients[0], digest="1" * 64, request="retry"), committed_index=3).status,
            "TOKEN_DIGEST_REUSE",
        )
        fresh = self.issue(client=clients[0], digest="a" * 64, request="valid-login")
        self.assertEqual(machine.apply(fresh, committed_index=4).value["credential_generation"], 4)
        self.assertEqual(machine.snapshot()["auth"]["active_credentials"][clients[2]],
                         upgraded["active_credentials"][clients[2]])

    def test_internal_upgrade_command_rejects_malformed_and_public_spoof(self):
        command = self.upgrade(self.auth["catalog_fingerprint"])
        validate_committed_command(command)
        for invalid in (
            command.model_copy(update={"client_id": "public-client"}),
            command.model_copy(update={"request_id": "non-canonical"}),
            command.model_copy(update={"payload": {**command.payload, "schema_version": True}}),
            command.model_copy(update={"payload": {**command.payload, "access_token": "raw-secret"}}),
        ):
            with self.assertRaises((ValueError, ValidationError)):
                validate_committed_command(invalid)
        self.assertEqual(auth_schema_upgrade_request_id(self.auth["catalog_fingerprint"]), command.request_id)


class UpgradeAdmissionTests(unittest.TestCase):
    def test_conflicting_v1_migration_automatically_quarantines_and_becomes_ready(self):
        catalog = load_account_catalog()
        legacy = initial_auth_state(catalog, enforced_from_index=1)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        user_id = str(catalog.users[0].profile.user_id)
        for client in (str(uuid4()), str(uuid4())):
            legacy["client_bindings"][client] = user_id
            legacy["active_credentials"][client] = {
                "user_id": user_id, "client_id": client, "credential_id": str(uuid4()),
                "token_digest": "e" * 64, "credential_generation": 1, "revoked": False,
            }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
            state = new_persisted_state(uuid4(), sha256_json(sorted(nodes)),
                                        initial_fsm={"kv": {}, "auth": legacy})
            folder = root / "n1"
            folder.mkdir(parents=True)
            (folder / "disk.json").write_text(json.dumps({"raft_state": state.model_dump(mode="json")}))
            app = App("n1", nodes, root)
            try:
                app.start()
                app.engine.role = "leader"
                app.engine.state.raft.current_term = 1
                app.engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
                app.engine.next_index = {"n2": 1, "n3": 1}
                app._ensure_auth_schema_upgrade()
                self.assertEqual(len(app.engine.state.raft.log), 1)
                app.engine.state.raft.commit_index = 1
                app.engine._apply_committed()
                app._refresh_auth_catalog_status()
                self.assertEqual(app.auth_catalog_status, AuthCatalogStatus.READY)
                self.assertTrue(app.auth_catalog_ready)
                upgraded = app.engine.state.fsm["auth"]
                self.assertEqual(upgraded["schema_version"], 2)
                self.assertEqual(upgraded["active_credentials"], {})
                self.assertEqual(upgraded["legacy_quarantined_credentials"], legacy["active_credentials"])
                for _ in range(3):
                    app._ensure_auth_schema_upgrade()
                    app._refresh_auth_catalog_status()
                self.assertEqual(len(app.engine.state.raft.log), 1)
            finally:
                app._events.close()
                app._log.close()

    def test_app_requires_committed_v2_upgrade_before_readiness(self):
        catalog = load_account_catalog()
        v2 = initial_auth_state(catalog, enforced_from_index=7)
        legacy = deepcopy(v2)
        legacy["schema_version"] = 1
        del legacy["retired_credentials"]
        legacy.pop("legacy_quarantined_credentials", None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
            persisted = new_persisted_state(
                uuid4(), sha256_json(sorted(nodes)), initial_fsm={"kv": {"preserved": "yes"}, "auth": legacy}
            )
            folder = root / "n1"
            folder.mkdir(parents=True)
            (folder / "disk.json").write_text(json.dumps({"raft_state": persisted.model_dump(mode="json")}))
            app = App("n1", nodes, root)
            try:
                app.start()
                self.assertEqual(app.auth_catalog_status, AuthCatalogStatus.UPGRADE_PENDING)
                self.assertFalse(app.auth_catalog_ready)
                self.assertEqual(app.engine.state.fsm["auth"], legacy)
                for op in ("upgrade_auth_schema_v2", "issue_access_token"):
                    response = app.on_request(Request("unauthorized", op, {}, "external"))
                    self.assertEqual(response["result"]["status"], "INVALID_REQUEST")

                app.engine.role = "leader"
                app.engine.state.raft.current_term = 1
                app.engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
                app.engine.next_index = {"n2": 1, "n3": 1}
                app._ensure_auth_schema_upgrade()
                app._ensure_auth_schema_upgrade()
                upgrades = [entry for entry in app.engine.state.raft.log
                            if entry.command.operation == "upgrade_auth_schema_v2"]
                self.assertEqual(len(upgrades), 1)
                self.assertEqual(app.engine.state.fsm["auth"], legacy)
                app.engine.state.raft.commit_index = upgrades[0].index
                app.engine._apply_committed()
                app._refresh_auth_catalog_status()
                self.assertTrue(app.auth_catalog_ready)
                self.assertEqual(app.engine.state.fsm["auth"]["schema_version"], 2)
                self.assertEqual(app.engine.state.fsm["kv"], {"preserved": "yes"})
                app.engine.storage.save(app.engine.state)
                self.assertEqual(
                    json.loads((folder / "disk.json").read_text())["raft_state"]["fsm"]["auth"]["schema_version"],
                    2,
                )
            finally:
                app._events.close()
                app._log.close()


if __name__ == "__main__":
    unittest.main()
