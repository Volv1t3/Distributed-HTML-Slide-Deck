"""Stage E issue #43: bearer auth guard for publish_deck."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from stage_e_raft_harness import SimulatedRaftHarnessMixin, issue_token_command

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO
from node import Request
from state_machine import KeyValueStateMachine


def _publish_cmd(deck_id: object, *, client_id: str = "client-a",
                 request_id: str = "pub-1",
                 actor: dict | None = None) -> CommandEnvelopeDTO:
    payload: dict = {
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
    }
    if actor:
        payload.update(actor)
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": "publish_deck",
        "payload": payload,
    })


class PublishDeckAuthGuardTests(unittest.TestCase):
    """Unit: state machine auth guard for publish_deck."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-publish"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()

        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        result = self.base_machine.apply(
            issue_token_command(self.user_id, self.client_id, self.credential_id, self.digest),
            committed_index=1,
        )
        self.assertEqual(result.status, "TOKEN_ISSUED", result)

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _actor(self) -> dict:
        return {
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }

    def test_valid_actor_permits_publish_deck(self) -> None:
        machine = self._machine()
        deck_id = uuid4()
        cmd = _publish_cmd(deck_id, client_id=self.client_id,
                           request_id="pub-" + str(uuid4()), actor=self._actor())
        result = machine.apply(cmd, committed_index=3)
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"))

    def test_wrong_actor_rejects_publish_deck(self) -> None:
        machine = self._machine()
        deck_id = uuid4()
        cmd = _publish_cmd(deck_id, client_id=self.client_id,
                           request_id="pub-" + str(uuid4()), actor={
                               "authenticated_actor_user_id": str(uuid4()),
                               "credential_id": self.credential_id,
                               "credential_generation": 1,
                           })
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_publish_deck(self) -> None:
        machine = self._machine()
        deck_id = uuid4()
        cmd = _publish_cmd(deck_id, client_id=self.client_id,
                           request_id="pub-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_publish_deck(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        deck_id = uuid4()
        cmd = _publish_cmd(deck_id, client_id=self.client_id,
                           request_id="pub-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)  # 3 < 100 → allowed
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"))

    def test_no_auth_state_actorless_publish_permitted(self) -> None:
        """When auth is None (pre-auth-init), actorless commands pass through."""
        machine = KeyValueStateMachine({"kv": {}})
        deck_id = uuid4()
        cmd = _publish_cmd(deck_id, client_id=self.client_id,
                           request_id="pub-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=1)
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"))

    def test_already_published_deck_returns_error_not_unauthenticated(self) -> None:
        """Guard fires before the idempotency check; second publish with valid actor should DECK_ALREADY_PUBLISHED."""
        machine = self._machine()
        deck_id = uuid4()
        # First publish (with actor)
        cmd1 = _publish_cmd(deck_id, client_id=self.client_id,
                            request_id="pub1-" + str(uuid4()), actor=self._actor())
        r1 = machine.apply(cmd1, committed_index=3)
        self.assertIn(r1.status, ("PUBLISHED", "DECK_PUBLISHED"))
        # Second publish (same deck, with actor)
        cmd2 = _publish_cmd(deck_id, client_id=self.client_id,
                            request_id="pub2-" + str(uuid4()), actor=self._actor())
        r2 = machine.apply(cmd2, committed_index=4)
        self.assertEqual(r2.status, "DECK_ALREADY_PUBLISHED")


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

try:
    from app import App
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class PublishDeckAuthAdmissionTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    """Integration via App.on_request for publish_deck."""

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


    def _setup_credential(self, replica: App) -> tuple[str, str]:
        client_id = str(uuid4())
        access_token = "bearer-pub-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(uuid4())
        self._commit_command(replica, issue_token_command(self.user_id, client_id, credential_id, digest))
        return client_id, access_token

    def _make_presentation(self, deck_id: str) -> dict:
        return {
            "schema_version": 2,
            "metadata": {
                "deck_id": deck_id,
                "name": "Test",
                "owner_session_id": str(uuid4()),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [{
                "metadata": {"slide_id": str(uuid4()), "background": "#112233FF"},
                "elements": [],
            }],
        }

    def test_caller_supplied_actor_rejected_for_publish_deck(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token = self._setup_credential(replica)
        deck_id = str(uuid4())
        req = Request("req-pub-actor", "publish_deck", {
            "deck_id": deck_id,
            "presentation": self._make_presentation(deck_id),
            "authenticated_actor_user_id": self.user_id,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_valid_bearer_produces_authenticated_publish_deck_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token = self._setup_credential(replica)
        deck_id = str(uuid4())
        # Real clients carry deck_id only inside presentation.metadata.
        request = Request("req-pub-valid", "publish_deck", {
            "presentation": self._make_presentation(deck_id),
        }, client_id)
        response = self._commit_admitted_request(replica, request, access_token)
        self.assertEqual(response["result"]["status"], "PUBLISHED", response)
        self.assertIn(deck_id, replica.engine.state.fsm["decks"])
        entries = [e for e in replica.engine.state.raft.log if e.command.operation == "publish_deck"]
        self.assertEqual(len(entries), 1)
        cmd = entries[-1].command
        self.assertEqual(str(cmd.deck_id), deck_id)
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))

    def test_publish_deck_without_auth_returns_reauth_required(self) -> None:
        """Missing _auth on publish_deck → REAUTH_REQUIRED, no Raft proposal."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        deck_id = str(uuid4())
        log_len_before = len(replica.engine.state.raft.log)
        result = replica.on_request(Request("req-pub-no-auth", "publish_deck", {
            "presentation": self._make_presentation(deck_id),
        }, str(uuid4())))
        self.assertEqual(result, {"ok": False, "error": "REAUTH_REQUIRED"})
        self._messages(replica)
        self.assertEqual(len(replica.engine.state.raft.log), log_len_before)


if __name__ == "__main__":
    unittest.main()
