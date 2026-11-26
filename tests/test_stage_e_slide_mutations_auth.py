"""Stage E issue #43: bearer auth guard for create_slide, delete_slide, reorder_slides."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from stage_e_raft_harness import (
    SimulatedRaftHarnessMixin,
    actor_stamp,
    issue_token_command,
    publish_empty_deck_command,
)

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import (
    CommandEnvelopeDTO,
    CreateSlidePayloadDTO,
    DeleteSlidePayloadDTO,
    ReorderSlidesPayloadDTO,
)
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
# Unit: state machine auth guard for create_slide, delete_slide, reorder_slides
# ---------------------------------------------------------------------------

class SlideMutationsAuthGuardTests(unittest.TestCase):
    """Deterministic guard: auth state checked before each slide structure mutation."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-slide-A"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()
        self.deck_id = uuid4()
        self.slide_id = uuid4()

        # Build a machine with auth state and a published deck
        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        # Issue credential at index 1
        issue_cmd = issue_token_command(
            self.user_id, self.client_id, self.credential_id, self.digest,
        )
        result = self.base_machine.apply(issue_cmd, committed_index=1)
        self.assertEqual(result.status, "TOKEN_ISSUED", result)
        # Publish a deck below the enforcement boundary (committed_index=0 < enforced_from_index=1)
        pub_cmd = publish_empty_deck_command(self.deck_id)
        result = self.base_machine.apply(pub_cmd, committed_index=0)
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"), result)
        # Store the initial slide_id from the published deck
        self.initial_slide_id = self.base_machine.snapshot()["decks"][str(self.deck_id)]["slides"][0]["metadata"]["slide_id"]

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _actor_payload(self) -> dict:
        return {
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }

    # --- create_slide ---

    def test_valid_actor_permits_create_slide(self) -> None:
        machine = self._machine()
        new_slide_id = uuid4()
        payload = {
            "expected_deck_version": 0,
            "slide": {
                "metadata": {"slide_id": str(new_slide_id), "background": "#AABBCCFF"},
                "elements": [],
            },
            **self._actor_payload(),
        }
        cmd = _make_deck_command("create_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="cs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "CREATED")

    def test_wrong_actor_rejects_create_slide(self) -> None:
        machine = self._machine()
        new_slide_id = uuid4()
        payload = {
            "expected_deck_version": 0,
            "slide": {
                "metadata": {"slide_id": str(new_slide_id), "background": "#AABBCCFF"},
                "elements": [],
            },
            "authenticated_actor_user_id": str(uuid4()),  # wrong user
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_deck_command("create_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="cs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_create_slide(self) -> None:
        machine = self._machine()
        new_slide_id = uuid4()
        payload = {
            "expected_deck_version": 0,
            "slide": {
                "metadata": {"slide_id": str(new_slide_id), "background": "#AABBCCFF"},
                "elements": [],
            },
        }
        cmd = _make_deck_command("create_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="cs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_create_slide(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100  # future boundary
        machine = KeyValueStateMachine(snap)
        new_slide_id = uuid4()
        payload = {
            "expected_deck_version": 0,
            "slide": {
                "metadata": {"slide_id": str(new_slide_id), "background": "#AABBCCFF"},
                "elements": [],
            },
        }
        cmd = _make_deck_command("create_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="cs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)  # 3 < 100 → allowed
        self.assertEqual(result.status, "CREATED")

    # --- delete_slide ---

    def test_valid_actor_permits_delete_slide(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_id": self.initial_slide_id,
            **self._actor_payload(),
        }
        cmd = _make_deck_command("delete_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="ds-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "DELETED")

    def test_wrong_actor_rejects_delete_slide(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_id": self.initial_slide_id,
            "authenticated_actor_user_id": str(uuid4()),  # wrong user
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_deck_command("delete_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="ds-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_delete_slide(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_id": self.initial_slide_id,
        }
        cmd = _make_deck_command("delete_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="ds-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_delete_slide(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        payload = {
            "expected_deck_version": 0,
            "slide_id": self.initial_slide_id,
        }
        cmd = _make_deck_command("delete_slide", self.deck_id, payload,
                                 client_id=self.client_id, request_id="ds-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)  # 3 < 100 → allowed
        self.assertEqual(result.status, "DELETED")

    # --- reorder_slides ---

    def test_valid_actor_permits_reorder_slides(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_ids": [self.initial_slide_id],
            **self._actor_payload(),
        }
        cmd = _make_deck_command("reorder_slides", self.deck_id, payload,
                                 client_id=self.client_id, request_id="rs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "REORDERED")

    def test_wrong_actor_rejects_reorder_slides(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_ids": [self.initial_slide_id],
            "authenticated_actor_user_id": str(uuid4()),  # wrong
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_deck_command("reorder_slides", self.deck_id, payload,
                                 client_id=self.client_id, request_id="rs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_reorder_slides(self) -> None:
        machine = self._machine()
        payload = {
            "expected_deck_version": 0,
            "slide_ids": [self.initial_slide_id],
        }
        cmd = _make_deck_command("reorder_slides", self.deck_id, payload,
                                 client_id=self.client_id, request_id="rs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_reorder_slides(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        payload = {
            "expected_deck_version": 0,
            "slide_ids": [self.initial_slide_id],
        }
        cmd = _make_deck_command("reorder_slides", self.deck_id, payload,
                                 client_id=self.client_id, request_id="rs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)  # 3 < 100 → allowed
        self.assertEqual(result.status, "REORDERED")

    def test_unauthenticated_leaves_deck_version_unchanged(self) -> None:
        machine = self._machine()
        before = deepcopy(machine.snapshot())
        payload = {
            "expected_deck_version": 0,
            "slide_ids": [self.initial_slide_id],
            "authenticated_actor_user_id": str(uuid4()),  # wrong
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_deck_command("reorder_slides", self.deck_id, payload,
                                 client_id=self.client_id, request_id="rs-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")
        after = machine.snapshot()
        deck_before = before["decks"][str(self.deck_id)]
        deck_after = after["decks"][str(self.deck_id)]
        self.assertEqual(deck_after["deck_version"], deck_before["deck_version"])


# ---------------------------------------------------------------------------
# Integration: app.py admission for slide mutations
# ---------------------------------------------------------------------------

try:
    from app import App
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class SlideMutationsAuthAdmissionTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    """Integration via App.on_request: wire-stripping, stamping for slide mutations."""

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

    # --- Caller-supplied actor field rejection ---

    def test_caller_supplied_authenticated_actor_user_id_rejected_for_create_slide(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id = self._setup_credential(replica)
        req = Request("req-actor-cs", "create_slide", {
            "deck_id": deck_id,
            "expected_deck_version": 0,
            "slide": {"metadata": {"slide_id": str(uuid4()), "background": "#AABBCCFF"}, "elements": []},
            "authenticated_actor_user_id": self.user_id,  # forbidden
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    # --- Valid bearer → command stamped with actor fields ---

    def test_valid_bearer_produces_authenticated_create_slide_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        new_slide_id = str(uuid4())
        req = Request("req-valid-cs", "create_slide", {
            "deck_id": deck_id,
            "expected_deck_version": 0,
            "slide": {"metadata": {"slide_id": new_slide_id, "background": "#AABBCCFF"}, "elements": []},
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result, "should return None while awaiting read")
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        log = replica.engine.state.raft.log
        auth_entries = [
            entry for entry in log
            if entry.command.operation == "create_slide"
        ]
        self.assertTrue(auth_entries, "expected create_slide in log after auth")
        cmd = auth_entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        self.assertIsNotNone(cmd.payload.get("credential_id"))
        self.assertIsNotNone(cmd.payload.get("credential_generation"))
        import json
        payload_str = json.dumps(cmd.payload)
        self.assertNotIn(access_token, payload_str)

    def test_valid_bearer_produces_authenticated_delete_slide_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        # Get the initial slide_id from the published deck
        initial_slide_id = replica.engine.state.fsm["decks"][deck_id]["slides"][0]["metadata"]["slide_id"]
        req = Request("req-valid-del-slide", "delete_slide", {
            "deck_id": deck_id,
            "expected_deck_version": 0,
            "slide_id": initial_slide_id,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result, "should return None while awaiting read")
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        log = replica.engine.state.raft.log
        auth_entries = [
            entry for entry in log
            if entry.command.operation == "delete_slide"
        ]
        self.assertTrue(auth_entries, "expected delete_slide in log after auth")
        cmd = auth_entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))

    def test_valid_bearer_produces_authenticated_reorder_slides_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id = self._setup_credential(replica)
        initial_slide_id = replica.engine.state.fsm["decks"][deck_id]["slides"][0]["metadata"]["slide_id"]
        req = Request("req-valid-reorder", "reorder_slides", {
            "deck_id": deck_id,
            "expected_deck_version": 0,
            "slide_ids": [initial_slide_id],
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result, "should return None while awaiting read")
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        log = replica.engine.state.raft.log
        auth_entries = [
            entry for entry in log
            if entry.command.operation == "reorder_slides"
        ]
        self.assertTrue(auth_entries, "expected reorder_slides in log after auth")
        cmd = auth_entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))


if __name__ == "__main__":
    unittest.main()
