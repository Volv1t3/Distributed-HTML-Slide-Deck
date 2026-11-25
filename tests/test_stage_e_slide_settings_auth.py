"""Stage E issue #43: bearer auth guard for update_slide_settings."""

from __future__ import annotations

import hashlib
import os
import queue
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO
from node import Request
from state_machine import KeyValueStateMachine


def _make_command(operation: str, deck_id: object, payload: dict,
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


def _publish_deck(deck_id: object, *, client_id: str = "client-a") -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": "pub-1",
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": str(deck_id),
                    "name": "Test Deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [{
                    "metadata": {"slide_id": str(uuid4()), "background": "#112233FF"},
                    "elements": [],
                }],
            }
        },
    })


def _issue_token(user_id: str, client_id: str, credential_id: str,
                 digest: str) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": "login-1",
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "operation": "issue_access_token",
        "payload": {
            "user_id": user_id,
            "client_id": client_id,
            "credential_id": credential_id,
            "token_digest": digest,
        },
    })


class SlideSettingsAuthGuardTests(unittest.TestCase):
    """Unit: state machine auth guard for update_slide_settings."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-slide-settings"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()
        self.deck_id = uuid4()

        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        result = self.base_machine.apply(
            _issue_token(self.user_id, self.client_id, self.credential_id, self.digest),
            committed_index=1,
        )
        self.assertEqual(result.status, "TOKEN_ISSUED", result)
        result = self.base_machine.apply(_publish_deck(self.deck_id, client_id=self.client_id), committed_index=0)
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"), result)
        snap = self.base_machine.snapshot()
        self.slide_id = snap["decks"][str(self.deck_id)]["slides"][0]["metadata"]["slide_id"]

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _actor(self) -> dict:
        return {
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }

    def test_valid_actor_permits_update_slide_settings(self) -> None:
        machine = self._machine()
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
            **self._actor(),
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "SLIDE_SETTINGS_UPDATED")

    def test_wrong_actor_rejects_update_slide_settings(self) -> None:
        machine = self._machine()
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
            "authenticated_actor_user_id": str(uuid4()),
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_update_slide_settings(self) -> None:
        machine = self._machine()
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_update_slide_settings(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "SLIDE_SETTINGS_UPDATED")

    def test_revoked_credential_rejects_update_slide_settings(self) -> None:
        machine = self._machine()
        # Revoke the credential in the auth state
        snap = deepcopy(machine.snapshot())
        snap["auth"]["active_credentials"][self.client_id]["revoked"] = True
        machine = KeyValueStateMachine(snap)
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
            **self._actor(),
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_wrong_credential_generation_rejects_update_slide_settings(self) -> None:
        machine = self._machine()
        payload = {
            "expected_slide_settings_version": 0,
            "slide_id": self.slide_id,
            "patch": {"background": "#AABBCCFF"},
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 999,  # wrong generation
        }
        cmd = _make_command("update_slide_settings", self.deck_id, payload,
                            client_id=self.client_id, request_id="uss-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

try:
    from app import App
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class SlideSettingsAuthAdmissionTests(unittest.TestCase):
    """Integration via App.on_request for update_slide_settings."""

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

    @staticmethod
    def _messages(replica: App) -> list:
        sent = []
        while True:
            try:
                sent.append(replica._out.get_nowait())
            except queue.Empty:
                return sent

    def _elect_and_commit_noop(self, replica: App) -> None:
        self._messages(replica)
        for slot in ("n2", "n3"):
            replica.on_message(slot, {
                "kind": "register_node", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": slot,
                "sender_uuid": str(uuid4()), "term": 0, "incarnation": 1,
                "membership_fingerprint": replica.engine.membership_fingerprint(),
            })
        self._messages(replica)
        replica.engine.start_election()
        vote = next(
            m for d, m in self._messages(replica)
            if d == "n2" and m["kind"] == "request_vote"
        )
        replica.on_message("n2", {
            "kind": "request_vote_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": vote["rpc_id"], "vote_granted": True,
        })
        append = next(
            m for d, m in self._messages(replica)
            if d == "n2" and m.get("kind") == "append_entries" and m.get("entries")
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self._messages(replica)

    def _commit_command(self, replica: App, command: CommandEnvelopeDTO) -> None:
        self._messages(replica)
        outcome = replica.engine.propose(command)
        if hasattr(outcome, "index"):
            append = next(
                m for d, m in self._messages(replica)
                if d == "n2" and m.get("kind") == "append_entries" and m.get("entries")
            )
            replica.on_message("n2", {
                "kind": "append_entries_response", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1, "reply_to": append["rpc_id"], "success": True,
                "match_index": append["entries"][-1]["index"],
            })
            self._messages(replica)

    def _acknowledge_read(self, replica: App) -> None:
        probe = next(
            m for d, m in self._messages(replica)
            if d == "n2" and m["kind"] == "append_entries"
            and m.get("read_context") is not None
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": probe["rpc_id"], "success": True,
            "match_index": probe["prev_log_index"],
            "read_context": probe["read_context"],
        })

    def _setup(self, replica: App) -> tuple[str, str, str, str]:
        client_id = str(uuid4())
        deck_id = str(uuid4())
        access_token = "bearer-ss-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(uuid4())
        self._commit_command(replica, _issue_token(self.user_id, client_id, credential_id, digest))
        self._commit_command(replica, _publish_deck(deck_id, client_id=client_id))
        deck = replica.engine.state.fsm["decks"][deck_id]
        slide_id = deck["slides"][0]["metadata"]["slide_id"]
        return client_id, access_token, deck_id, slide_id

    def test_caller_supplied_actor_rejected_for_update_slide_settings(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id, slide_id = self._setup(replica)
        req = Request("req-uss-actor", "update_slide_settings", {
            "deck_id": deck_id,
            "expected_slide_settings_version": 0,
            "slide_id": slide_id,
            "patch": {"background": "#AABBCCFF"},
            "authenticated_actor_user_id": self.user_id,
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_valid_bearer_produces_authenticated_update_slide_settings_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, slide_id = self._setup(replica)
        req = Request("req-uss-valid", "update_slide_settings", {
            "deck_id": deck_id,
            "expected_slide_settings_version": 0,
            "slide_id": slide_id,
            "patch": {"background": "#AABBCCFF"},
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "update_slide_settings"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))


if __name__ == "__main__":
    unittest.main()
