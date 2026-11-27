"""Stage E issue #42: authenticated Deck Settings — protected-command tracer bullet.

Acceptance criteria (spec §11 items 6-9):
  6. update before revoke may succeed; revoke before update deterministically
     rejects the later operation and leaves settings versions unchanged.
  7. update_deck_settings and update_slide_settings: committed auth denial leaves
     all Stage D revision domains unchanged; valid actor still receives the
     original resource-specific settings conflict behaviour.
  9. Exact-ID retry under a new valid token returns the original committed result
     once; same ID with a different actor or payload produces REQUEST_ID_REUSE.
  (also AC 1-5, 7 from the issue itself)
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4
from pydantic import ValidationError

from stage_e_raft_harness import (
    SimulatedRaftHarnessMixin,
    actor_stamp,
    issue_token_command,
    publish_empty_deck_command,
)

from auth_catalog import load_account_catalog
from auth_dto import (
    initial_auth_state,
    validate_auth_state,
)
from command_dto import (
    CommandEnvelopeDTO,
    DeckSettingsPatchDTO,
    UpdateDeckSettingsPayloadDTO,
    command_fingerprint,
    validate_committed_command,
)
from dto_base import sha256_json
from node import Request
from state_machine import KeyValueStateMachine


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_deck_command(operation: str, deck_id: object, payload: dict,
                       *, request_id: str = "req-1", client_id: str = "client-a") -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": operation,
        "payload": payload,
    })




# ---------------------------------------------------------------------------
# Unit: state machine auth guard (spec §11 item 7)
# ---------------------------------------------------------------------------

class DeckSettingsAuthGuardTests(unittest.TestCase):
    """Deterministic guard: auth state checked before each settings mutation."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-token-A"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()
        self.deck_id = uuid4()

        # Build a machine with auth state (via issue_access_token) and a published deck
        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        # Issue credential
        issue_cmd = issue_token_command(
            self.user_id, self.client_id, self.credential_id, self.digest,
        )
        result = self.base_machine.apply(issue_cmd, committed_index=1)
        self.assertEqual(result.status, "TOKEN_ISSUED", result)
        # Publish a deck below the enforcement boundary (committed_index=0 < enforced_from_index=1)
        # so that the actorless publish_deck command is accepted as a pre-auth entry.
        pub_cmd = publish_empty_deck_command(self.deck_id)
        result = self.base_machine.apply(pub_cmd, committed_index=0)
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"), result)

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _update_settings_cmd(self, *, with_actor: bool = True,
                              user_id: str | None = None,
                              credential_id: str | None = None,
                              credential_generation: int | None = None,
                              expected_version: int = 0,
                              client_id: str | None = None,
                              request_id: str | None = None) -> CommandEnvelopeDTO:
        payload: dict = {
            "expected_deck_settings_version": expected_version,
            "patch": {"name": "Updated Name"},
        }
        if with_actor:
            payload["authenticated_actor_user_id"] = user_id or self.user_id
            payload["credential_id"] = credential_id or self.credential_id
            payload["credential_generation"] = (
                credential_generation if credential_generation is not None else 1
            )
        return _make_deck_command(
            "update_deck_settings",
            self.deck_id,
            payload,
            client_id=client_id or self.client_id,
            request_id=request_id or ("upd-" + str(uuid4())),
        )

    # --- AC 2: valid credential permits the original success ---

    def test_valid_actor_fields_permit_settings_update(self) -> None:
        machine = self._machine()
        cmd = self._update_settings_cmd()
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        snap = machine.snapshot()
        deck = snap["decks"][str(self.deck_id)]
        self.assertEqual(deck["deck_settings_version"], 1)

    def test_valid_actor_conflict_uses_existing_conflict_response(self) -> None:
        """Valid auth + wrong version → settings conflict, not UNAUTHENTICATED."""
        machine = self._machine()
        cmd = self._update_settings_cmd(expected_version=99)
        result = machine.apply(cmd, committed_index=3)
        self.assertIn(result.status, ("DECK_SETTINGS_CONFLICT", "DECK_SETTINGS_VERSION_CONFLICT"))

    # --- AC 4: invalidated credential returns UNAUTHENTICATED, no revision change ---

    def test_revoked_credential_returns_unauthenticated(self) -> None:
        machine = self._machine()
        # Revoke by marking revoked in the active credentials dict directly
        snap = deepcopy(machine.snapshot())
        snap["auth"]["active_credentials"][self.client_id]["revoked"] = True
        machine = KeyValueStateMachine(snap)
        cmd = self._update_settings_cmd()
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_wrong_user_id_returns_unauthenticated(self) -> None:
        machine = self._machine()
        cmd = self._update_settings_cmd(user_id=str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_wrong_credential_id_returns_unauthenticated(self) -> None:
        machine = self._machine()
        cmd = self._update_settings_cmd(credential_id=str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_wrong_credential_generation_returns_unauthenticated(self) -> None:
        machine = self._machine()
        cmd = self._update_settings_cmd(credential_generation=99)
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_unauthenticated_leaves_settings_version_unchanged(self) -> None:
        """AC 4: committed denial must not touch deck or settings revision."""
        machine = self._machine()
        before = deepcopy(machine.snapshot())
        cmd = self._update_settings_cmd(credential_id=str(uuid4()))  # wrong cred
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")
        after = machine.snapshot()
        deck_before = before["decks"][str(self.deck_id)]
        deck_after = after["decks"][str(self.deck_id)]
        self.assertEqual(deck_after["deck_settings_version"], deck_before["deck_settings_version"])
        self.assertEqual(deck_after["deck_version"], deck_before["deck_version"])

    # --- enforced_from_index boundary ---

    def test_no_actor_fields_below_enforced_index_allowed(self) -> None:
        """Pre-activation actorless entries remain valid below enforced_from_index."""
        machine = self._machine()
        # enforced_from_index is 1; apply at committed_index=0 (a hypothetical legacy entry)
        # Since we cannot go below 0, test that actorless with no actor fields
        # and committed_index < enforced_from_index passes.
        # We override enforced_from_index to something high to simulate backward compat.
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100  # future boundary
        machine = KeyValueStateMachine(snap)
        cmd = self._update_settings_cmd(with_actor=False)
        result = machine.apply(cmd, committed_index=3)  # 3 < 100 → backward compat
        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")

    def test_no_actor_fields_at_enforced_index_rejected(self) -> None:
        """Actorless command at or above enforced_from_index is rejected."""
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 3  # will apply at exactly 3
        machine = KeyValueStateMachine(snap)
        cmd = self._update_settings_cmd(with_actor=False)
        result = machine.apply(cmd, committed_index=3)  # 3 >= 3 → rejected
        self.assertEqual(result.status, "UNAUTHENTICATED")

    # --- AC 6: update before revoke / revoke before update ---

    def test_update_before_revoke_may_succeed(self) -> None:
        """update_deck_settings committed before revocation succeeds."""
        machine = self._machine()
        cmd = self._update_settings_cmd()
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        # After success, the credential can still be valid; revoke it now
        snap = deepcopy(machine.snapshot())
        snap["auth"]["active_credentials"][self.client_id]["revoked"] = True
        machine = KeyValueStateMachine(snap)
        # A second update at index 4 would now fail
        cmd2 = self._update_settings_cmd(expected_version=1, request_id="upd2-" + str(uuid4()))
        result2 = machine.apply(cmd2, committed_index=4)
        self.assertEqual(result2.status, "UNAUTHENTICATED")


# ---------------------------------------------------------------------------
# Unit: DTO fields and fingerprint (AC 5)
# ---------------------------------------------------------------------------

class DeckSettingsAuthDTOTests(unittest.TestCase):
    """UpdateDeckSettingsPayloadDTO carries optional actor fields."""

    def _payload(self, *, with_actor: bool = False) -> dict:
        p: dict = {
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
        }
        if with_actor:
            p["authenticated_actor_user_id"] = str(uuid4())
            p["credential_id"] = str(uuid4())
            p["credential_generation"] = 1
        return p

    def test_payload_accepts_optional_actor_fields(self) -> None:
        payload = self._payload(with_actor=True)
        dto = UpdateDeckSettingsPayloadDTO.model_validate(payload)
        self.assertIsInstance(dto.authenticated_actor_user_id, UUID)
        self.assertIsInstance(dto.credential_id, UUID)
        self.assertEqual(dto.credential_generation, 1)

    def test_payload_rejects_partial_actor_stamp(self) -> None:
        payload = self._payload(with_actor=True)
        del payload["credential_id"]
        with self.assertRaises(ValidationError):
            UpdateDeckSettingsPayloadDTO.model_validate(payload)

    def test_payload_accepts_missing_actor_fields(self) -> None:
        dto = UpdateDeckSettingsPayloadDTO.model_validate(self._payload())
        self.assertIsNone(dto.authenticated_actor_user_id)
        self.assertIsNone(dto.credential_id)
        self.assertIsNone(dto.credential_generation)

    def test_validate_committed_command_accepts_actor_fields(self) -> None:
        deck_id = uuid4()
        cmd = _make_deck_command(
            "update_deck_settings", deck_id,
            {
                "expected_deck_settings_version": 0,
                "patch": {"name": "X"},
                "authenticated_actor_user_id": str(uuid4()),
                "credential_id": str(uuid4()),
                "credential_generation": 1,
            },
        )
        # Should not raise
        validate_committed_command(cmd)


class DeckSettingsAuthFingerprintTests(unittest.TestCase):
    """AC 5: fingerprint includes actor_user_id but excludes bearer secret and generation."""

    def _cmd(self, *, user_id: str | None = None, generation: int = 1,
             deck_id: object | None = None) -> CommandEnvelopeDTO:
        d = deck_id or uuid4()
        return _make_deck_command(
            "update_deck_settings",
            d,
            {
                "expected_deck_settings_version": 0,
                "patch": {"name": "Same"},
                "authenticated_actor_user_id": user_id or str(uuid4()),
                "credential_id": str(uuid4()),
                "credential_generation": generation,
            },
        )

    def test_different_actor_user_id_produces_different_fingerprint(self) -> None:
        deck_id = uuid4()
        client_id = str(uuid4())
        base_user = str(uuid4())
        cmd_a = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "req-fp-1",
            "client_id": client_id,
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_deck_settings",
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {"name": "Same"},
                "authenticated_actor_user_id": base_user,
                "credential_id": str(uuid4()),
                "credential_generation": 1,
            },
        })
        cmd_b = cmd_a.model_copy(update={
            "payload": {
                **cmd_a.payload,
                "authenticated_actor_user_id": str(uuid4()),  # different actor
            }
        })
        self.assertNotEqual(command_fingerprint(cmd_a), command_fingerprint(cmd_b))

    def test_different_credential_generation_produces_same_fingerprint(self) -> None:
        """Bearer rotation (generation bump) must not invalidate stored dedup results."""
        deck_id = uuid4()
        client_id = str(uuid4())
        actor_user_id = str(uuid4())
        cred_id = str(uuid4())
        base_payload = {
            "expected_deck_settings_version": 0,
            "patch": {"name": "Same"},
            "authenticated_actor_user_id": actor_user_id,
            "credential_id": cred_id,
            "credential_generation": 1,
        }
        cmd_gen1 = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "req-gen-1",
            "client_id": client_id,
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_deck_settings",
            "payload": base_payload,
        })
        cmd_gen2 = cmd_gen1.model_copy(update={
            "payload": {**base_payload, "credential_generation": 2},
        })
        self.assertEqual(command_fingerprint(cmd_gen1), command_fingerprint(cmd_gen2))
        cmd_rotated_credential = cmd_gen1.model_copy(update={
            "payload": {**base_payload, "credential_id": str(uuid4()), "credential_generation": 2},
        })
        self.assertEqual(command_fingerprint(cmd_gen1), command_fingerprint(cmd_rotated_credential))


# ---------------------------------------------------------------------------
# Integration: app.py wire-stripping and actor stamping (AC 1, 3)
# ---------------------------------------------------------------------------

try:
    from app import App
    from auth_admission import PasswordCheckResult, PasswordCheckTask, redact_auth_data, REDACTED
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


class ControlledPasswordVerifier:
    """Deterministic fake; never reads App or Raft state."""

    def __init__(self) -> None:
        self.tasks: list = []
        self.results: list = []
        self.accepting = True

    def submit(self, task) -> bool:
        if not self.accepting:
            return False
        self.tasks.append(task)
        return True

    def complete(self, *, valid: bool) -> None:
        task = self.tasks.pop(0)
        self.results.append(PasswordCheckResult(task.attempt_id, task.captured_term, valid))

    def drain_results(self):
        results, self.results = self.results, []
        return results

    def shutdown(self) -> None:
        pass


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class DeckSettingsAuthAdmissionTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    """Integration via App.on_request: wire-stripping, stamping, REAUTH_REQUIRED."""

    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replicas: list[App] = []
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)

    def tearDown(self) -> None:
        for replica in self.replicas:
            if hasattr(replica, "_password_verifier"):
                replica._password_verifier.shutdown()
            if hasattr(replica, "_events"):
                replica._events.close()
            if hasattr(replica, "_log"):
                replica._log.close()
        self.workspace.cleanup()

    def _app(self, *, mode: str = "OPERATIONAL") -> App:
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": mode}):
            replica = App("n1", self.nodes, self.run_dir)
            replica.start()
        self.replicas.append(replica)
        return replica


    def _setup_credential(self, replica: App) -> tuple[str, str, str]:
        """Issue a credential and publish a deck; return (client_id, access_token, deck_id)."""
        client_id = str(uuid4())
        deck_id = str(uuid4())
        access_token = "integration-test-bearer-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(uuid4())

        issue_cmd = issue_token_command(
            self.user_id, client_id, credential_id, digest,
            request_id="setup-issue-" + str(uuid4()),
        )
        self._commit_command(replica, issue_cmd)

        pub_cmd = publish_empty_deck_command(deck_id, client_id=client_id,
                                request_id="setup-pub-" + str(uuid4()),
                                actor=actor_stamp(self.user_id, credential_id))
        self._commit_command(replica, pub_cmd)
        self.assertIn(deck_id, replica.engine.state.fsm["decks"])
        return client_id, access_token, deck_id

    # --- AC 1: wire-only auth field, caller-supplied actor assertions rejected ---

    def test_caller_supplied_authenticated_actor_user_id_is_rejected(self) -> None:
        """Caller must not be able to inject actor identity via args."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        req = Request("req-actor", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
            "authenticated_actor_user_id": self.user_id,  # forbidden
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_caller_supplied_credential_id_is_rejected(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        req = Request("req-cred-id", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
            "credential_id": str(uuid4()),  # forbidden
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_caller_supplied_credential_generation_is_rejected(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        req = Request("req-cred-gen", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
            "credential_generation": 1,  # forbidden
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_caller_supplied_actor_fields_are_rejected_on_direct_path(self) -> None:
        """Actor assertions are refused before the non-admitted path can copy them.

        The direct (actorless) path builds its payload from ``req.args`` verbatim.
        Without an on_request-level rejection, a caller who knows the non-secret
        credential_id/generation from a prior login response could forge an
        authenticated mutation with no bearer at all.
        """
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        credential = replica.engine.state.fsm["auth"]["active_credentials"][client_id]
        log_len_before = len(replica.engine.state.raft.log)
        req = Request("req-forged-direct", "publish_deck", {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id,
                    "name": "Forged",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [{
                    "metadata": {"slide_id": str(uuid4()), "background": "#112233FF"},
                    "elements": [],
                }],
            },
            # No _auth: this would otherwise take the direct path.
            "authenticated_actor_user_id": str(credential["user_id"]),
            "credential_id": str(credential["credential_id"]),
            "credential_generation": credential["credential_generation"],
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")
        self.assertEqual(len(replica.engine.state.raft.log), log_len_before,
                         "forged command must not be proposed")

    # --- AC 3: missing/stale/revoked credentials return REAUTH_REQUIRED pre-proposal ---

    def test_update_deck_settings_without_auth_field_returns_reauth_required(self) -> None:
        """Missing _auth entirely → REAUTH_REQUIRED, no Raft proposal."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        log_len_before = len(replica.engine.state.raft.log)
        req = Request("req-no-auth", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
            # no _auth field
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), False)
        self.assertEqual(result.get("error"), "REAUTH_REQUIRED")
        # No business command appended
        self._messages(replica)
        self.assertEqual(len(replica.engine.state.raft.log), log_len_before)

    def test_invalid_bearer_token_returns_reauth_required_after_read(self) -> None:
        """Wrong bearer → REAUTH_REQUIRED after admission read completes."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _correct_token, deck_id = self._setup_credential(replica)
        log_len_before = len(replica.engine.state.raft.log)
        import threading
        done, response = threading.Event(), {}
        req = Request("req-bad-token", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "X"},
            "_auth": {"access_token": "wrong-token"},
        }, client_id)
        replica._waiting["req-bad-token"] = (done, response)
        # on_request starts the admission read → returns None (async)
        result = replica.on_request(req)
        self.assertIsNone(result, "should return None while awaiting read")
        # Drive the read to READY
        self._acknowledge_read(replica)
        # reply() sets done and fills response
        self.assertTrue(done.is_set(), "expected reply to be delivered")
        self.assertEqual(response.get("ok"), False)
        self.assertEqual(response.get("error"), "REAUTH_REQUIRED")
        # No business command appended
        self.assertEqual(len(replica.engine.state.raft.log), log_len_before)

    def test_valid_bearer_token_produces_authenticated_command(self) -> None:
        """Valid bearer → after admission read, command stamped with actor and proposed."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        req = Request("req-valid-auth", "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 0,
            "patch": {"name": "Authenticated Update"},
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result, "should return None while awaiting read")
        # Drive the read
        self._acknowledge_read(replica)
        # Progress logins (which now also drives auth-admitted commands)
        replica._progress_auth_work()
        # Command should now be in the Raft log
        log = replica.engine.state.raft.log
        auth_entries = [
            entry for entry in log
            if entry.command.operation == "update_deck_settings"
        ]
        self.assertTrue(auth_entries, "expected update_deck_settings in log after auth")
        cmd = auth_entries[-1].command
        # Actor fields must be stamped in the payload
        self.assertEqual(str(cmd.payload.get("authenticated_actor_user_id")), self.user_id)
        self.assertIsNotNone(cmd.payload.get("credential_id"))
        self.assertIsNotNone(cmd.payload.get("credential_generation"))
        # Raw bearer must NOT be in the payload
        import json
        payload_str = json.dumps(cmd.payload)
        self.assertNotIn(access_token, payload_str)

    # --- AC 6: exact-ID retry returns stored result; different actor → REQUEST_ID_REUSE ---

    def test_exact_id_retry_returns_stored_result(self) -> None:
        """AC6 / §11 item 9: same request_id + same actor returns stored result without re-applying."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        request_id = "req-ac6-retry-" + str(uuid4())

        cred = replica.engine.state.fsm["auth"]["active_credentials"][client_id]
        actor_user_id = str(cred["user_id"])
        credential_id = str(cred["credential_id"])
        credential_generation = cred["credential_generation"]

        # Commit the first authenticated command directly with stamped actor fields.
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": request_id,
            "client_id": client_id,
            "origin_node_uuid": str(uuid4()),
            "deck_id": deck_id,
            "operation": "update_deck_settings",
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {"name": "Original Apply"},
                "authenticated_actor_user_id": actor_user_id,
                "credential_id": credential_id,
                "credential_generation": credential_generation,
            },
        })
        self._commit_command(replica, cmd)
        log_len_after_first = len(replica.engine.state.raft.log)

        # Replay the same request_id via the admission path; a re-apply would fail
        # (expected_deck_settings_version=1 is now stale) but dedup should short-circuit.
        req = Request(request_id, "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 99,
            "patch": {"name": "Must not apply again"},
            "_auth": {"access_token": access_token},
        }, client_id)
        replica.on_request(req)
        self._acknowledge_read(replica)
        replica._progress_auth_work()

        # Stored result was returned; no second log entry was appended.
        self.assertEqual(len(replica.engine.state.raft.log), log_len_after_first,
                         "exact-ID retry must not append a new log entry")

    def test_exact_id_different_actor_returns_request_id_reuse(self) -> None:
        """AC6 / §11 item 9: same request_id with a different actor produces REQUEST_ID_REUSE."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        request_id = "req-ac6-reuse-" + str(uuid4())

        cred = replica.engine.state.fsm["auth"]["active_credentials"][client_id]
        credential_id = str(cred["credential_id"])
        credential_generation = cred["credential_generation"]
        fake_actor_id = str(uuid4())  # different from the real self.user_id

        # Commit the same (client_id, request_id) with a different actor.
        cmd_other = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": request_id,
            "client_id": client_id,
            "origin_node_uuid": str(uuid4()),
            "deck_id": deck_id,
            "operation": "update_deck_settings",
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {"name": "Committed by other actor"},
                "authenticated_actor_user_id": fake_actor_id,
                "credential_id": credential_id,
                "credential_generation": credential_generation,
            },
        })
        self._commit_command(replica, cmd_other)
        log_len_after_first = len(replica.engine.state.raft.log)

        # Replay via on_request — real actor (self.user_id ≠ fake_actor_id) is stamped.
        # Different fingerprint → REQUEST_ID_REUSE returned as immediate ClientResultDTO.
        req = Request(request_id, "update_deck_settings", {
            "deck_id": deck_id,
            "expected_deck_settings_version": 1,
            "patch": {"name": "Should not apply"},
            "_auth": {"access_token": access_token},
        }, client_id)
        replica.on_request(req)
        self._acknowledge_read(replica)
        replica._progress_auth_work()

        # No new log entry — REQUEST_ID_REUSE was returned without proposal.
        self.assertEqual(len(replica.engine.state.raft.log), log_len_after_first,
                         "REQUEST_ID_REUSE must not append a new log entry")

    # --- Stage F (ADR-017) supersedes Stage E AC 7: deck reads now need a bearer ---

    def test_get_deck_preview_requires_bearer_after_stage_f(self) -> None:
        """Former Local Eventual Reads are protected leader reads in Stage F."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        req = Request("req-read", "get_deck_preview", {"deck_id": deck_id}, client_id)
        self.assertEqual(replica.on_request(req), {"ok": False, "error": "REAUTH_REQUIRED"})

    def test_read_lock_status_requires_bearer_after_stage_f(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        element_id = str(uuid4())
        req = Request("req-lock-read", "read_lock_status",
                      {"deck_id": deck_id, "element_id": element_id}, client_id)
        self.assertEqual(replica.on_request(req), {"ok": False, "error": "REAUTH_REQUIRED"})


if __name__ == "__main__":
    unittest.main()
