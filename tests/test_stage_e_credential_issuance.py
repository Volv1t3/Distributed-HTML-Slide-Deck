"""Stage E issue #39: deterministic issuance, rotation and Raft deduplication."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import unittest
from uuid import uuid4

from pydantic import ValidationError

from auth_catalog import load_account_catalog
from auth_dto import (
    AuthStateDTO,
    IssueAccessTokenPayloadDTO,
    initial_auth_state,
    issue_access_token_command_fingerprint,
)
from command_dto import CommandEnvelopeDTO, command_fingerprint, validate_committed_command
from dto_base import sha256_json
from persistence import InMemoryPersistence
from raft_core import RaftEngine
from state_machine import KeyValueStateMachine
import spec


class IssueAccessTokenTests(unittest.TestCase):
    def setUp(self):
        self.catalog = load_account_catalog()
        self.users = [user.profile.user_id for user in self.catalog.users]
        self.auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.client_id = uuid4()
        self.credential_id = uuid4()
        self.digest = hashlib.sha256(b"unpredictable-test-token-A").hexdigest()

    def command(self, *, user=None, client=None, credential=None, digest=None, request_id="login-1"):
        client = client or self.client_id
        return CommandEnvelopeDTO(
            command_schema_version=1,
            client_id=str(client),
            request_id=request_id,
            origin_node_uuid=uuid4(),
            operation="issue_access_token",
            payload={
                "user_id": str(user or self.users[0]),
                "client_id": str(client),
                "credential_id": str(credential or self.credential_id),
                "token_digest": digest or self.digest,
            },
        )

    def machine(self):
        return KeyValueStateMachine({"kv": {}, "auth": deepcopy(self.auth)})

    def test_strict_issuance_dto_and_internal_shape(self):
        command = self.command()
        validate_committed_command(command)
        for field, value in (("token_digest", "A" * 64), ("token_digest", "g" * 64), ("client_id", "not-a-uuid")):
            altered = {**command.payload, field: value}
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                IssueAccessTokenPayloadDTO.model_validate(altered)
        with self.assertRaises(ValidationError):
            IssueAccessTokenPayloadDTO.model_validate({**command.payload, "access_token": "secret"})
        for altered in (
            command.model_copy(update={"client_id": str(uuid4())}),
            command.model_copy(update={"deck_id": uuid4()}),
            command.model_copy(update={"session_id": uuid4()}),
            command.model_copy(update={"payload": {**command.payload, "password": "secret"}}),
        ):
            with self.subTest(command=altered), self.assertRaises((ValueError, ValidationError)):
                validate_committed_command(altered)

    def test_issuance_fingerprint_uses_one_canonical_server_material_projection(self):
        command = self.command()
        canonical = issue_access_token_command_fingerprint(
            client_id=command.client_id,
            payload=command.payload,
        )
        self.assertEqual(command_fingerprint(command), canonical)

        changed_digest = self.command(digest="f" * 64)
        changed_credential = self.command(credential=uuid4())
        self.assertNotEqual(command_fingerprint(changed_digest), canonical)
        self.assertNotEqual(command_fingerprint(changed_credential), canonical)

        # Raw bearer material is outside the DTO and therefore cannot become
        # part of the canonical issuance fingerprint.
        with self.assertRaises(ValidationError):
            issue_access_token_command_fingerprint(
                client_id=command.client_id,
                payload={**command.payload, "access_token": "raw-secret"},
            )

    def test_first_issuance_rotation_and_binding(self):
        machine = self.machine()
        first = self.command()
        result = machine.apply(first, committed_index=1)
        self.assertEqual(result.status, "TOKEN_ISSUED")
        self.assertEqual(result.value["credential_generation"], 1)
        self.assertEqual(machine.snapshot()["auth"]["client_bindings"][str(self.client_id)], str(self.users[0]))
        self.assertNotIn("access_token", json.dumps(machine.snapshot()))
        second = self.command(credential=uuid4(), digest=hashlib.sha256(b"unpredictable-test-token-B").hexdigest(), request_id="login-2")
        result = machine.apply(second, committed_index=2)
        self.assertEqual(result.value["credential_generation"], 2)
        active = machine.snapshot()["auth"]["active_credentials"][str(self.client_id)]
        self.assertEqual(active["token_digest"], second.payload["token_digest"])
        self.assertFalse(active["revoked"])
        self.assertEqual(len(machine.snapshot()["auth"]["active_credentials"]), 1)
        retired = machine.snapshot()["auth"]["retired_credentials"][str(self.credential_id)]
        self.assertEqual(retired["token_digest"], self.digest)
        self.assertEqual(retired["retirement_reason"], "ROTATED")
        before = machine.snapshot()
        denied = machine.apply(self.command(user=self.users[1], credential=uuid4(), digest="1" * 64, request_id="login-3"), committed_index=3)
        self.assertEqual(denied.status, "CLIENT_ALREADY_BOUND")
        self.assertEqual(before, machine.snapshot())
        AuthStateDTO.model_validate(machine.snapshot()["auth"])

    def test_unknown_user_catalog_unavailable_and_reuse(self):
        machine = self.machine()
        unknown = machine.apply(self.command(user=uuid4()), committed_index=1)
        self.assertEqual(unknown.status, "USER_NOT_FOUND")
        self.assertEqual(machine.snapshot()["auth"], self.auth)
        no_catalog = KeyValueStateMachine({"kv": {}})
        self.assertEqual(no_catalog.apply(self.command(), committed_index=1).status, "AUTH_CATALOG_NOT_READY")
        first = self.command()
        machine.apply(first, committed_index=1)
        saved = machine.snapshot()
        for command, status in (
            (self.command(credential=self.credential_id, digest="2" * 64, request_id="next"), "CREDENTIAL_ID_REUSE"),
            (self.command(credential=uuid4(), digest=self.digest, request_id="next2"), "TOKEN_DIGEST_REUSE"),
        ):
            self.assertEqual(machine.apply(command, committed_index=2).status, status)
            self.assertEqual(machine.snapshot(), saved)

    def test_reference_model_matches_independent_fsm_and_exact_replay(self):
        machine = self.machine()
        reference = None
        commands = [
            self.command(),
            self.command(request_id="login-1"),
            self.command(credential=uuid4(), digest="a" * 64, request_id="login-1"),
            self.command(credential=uuid4(), digest="b" * 64, request_id="login-2"),
            self.command(user=self.users[1], credential=uuid4(), digest="c" * 64, request_id="login-3"),
            self.command(user=uuid4(), client=uuid4(), credential=uuid4(), digest="d" * 64, request_id="login-4"),
        ]
        seen = {}
        for i, command in enumerate(commands, 1):
            key = sha256_json([command.client_id, command.request_id])
            fingerprint = command_fingerprint(command)
            if key in seen:
                old_fp, old_result = seen[key]
                result = old_result if old_fp == fingerprint else type(old_result)(status="REQUEST_ID_REUSE")
            else:
                result = machine.apply(command, committed_index=i)
                seen[key] = (fingerprint, result)
            args = {**command.payload, "request_id": command.request_id}
            reference, expected = spec.apply(reference, "issue_access_token", args)
            self.assertEqual(result.model_dump(mode="json"), expected)
            if result.status == "TOKEN_ISSUED":
                record = machine.snapshot()["auth"]["active_credentials"][command.client_id]
                self.assertEqual(record, reference["active_credentials"][command.client_id])
                self.assertEqual(
                    machine.snapshot()["auth"]["retired_credentials"],
                    reference["retired_credentials"],
                )
        self.assertEqual(spec.key("issue_access_token", commands[0].payload), "_auth_credentials")

    def test_real_raft_dedup_persists_safe_result(self):
        storage = InMemoryPersistence()
        node_uuid = uuid4()
        engine = RaftEngine(
            node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=node_uuid,
            storage=storage, send=lambda *_: None, clock=lambda: 0.0,
            genesis_fsm={"kv": {}, "auth": deepcopy(self.auth)},
        )
        engine.role = "leader"
        engine.state.raft.current_term = 1
        engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
        engine.next_index = {"n2": 1, "n3": 1}
        command = self.command()
        appended = engine.propose(command)
        engine.state.raft.commit_index = appended.index
        engine._apply_committed()
        storage.save(engine.state)
        first_record = engine.state.request_results[sha256_json([command.client_id, command.request_id])]
        self.assertEqual(first_record.result.status, "TOKEN_ISSUED")
        self.assertNotIn("access_token", json.dumps(first_record.model_dump(mode="json")))
        self.assertNotIn("password", json.dumps(engine.state.model_dump(mode="json")))
        original = deepcopy(engine.state.fsm)
        exact = command.model_copy(update={"origin_node_uuid": uuid4()})
        self.assertEqual(engine.propose(exact), first_record.result)
        self.assertEqual(engine.state.fsm, original)
        self.assertEqual(engine.propose(self.command(digest="e" * 64)).status, "REQUEST_ID_REUSE")
        self.assertEqual(engine.state.fsm, original)
        restored = RaftEngine(
            node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=node_uuid,
            storage=storage, send=lambda *_: None, clock=lambda: 0.0,
        )
        self.assertEqual(restored.state.fsm, original)
        self.assertEqual(restored.state.request_results, engine.state.request_results)


if __name__ == "__main__":
    unittest.main()
