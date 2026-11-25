"""Stage E issue #43: bearer auth guard for create_element, update_element, delete_element."""

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

# Minimal valid shape element configuration (rectangle)
_SHAPE_CONF = {
    "x": 0, "y": 0, "width": 100, "height": 60,
    "scale_basis_points": 10000,
    "rotation_centidegrees": 0,
    "fill": "#112233FF", "border_color": "#000000FF",
    "border_style": "solid", "border_thickness": 0,
    "z_index": 1,
}


def _make_command(operation: str, deck_id: object, payload: dict,
                  *, request_id: str = "req-1", client_id: str = "client-a",
                  session_id: str | None = None) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "session_id": session_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": operation,
        "payload": payload,
    })


def _publish_deck_no_elements(deck_id: object, slide_id: object,
                              *, client_id: str = "client-a") -> CommandEnvelopeDTO:
    """Publish a deck with one empty slide (no elements)."""
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
                    "metadata": {"slide_id": str(slide_id), "background": "#112233FF"},
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


class ElementMutationsAuthGuardTests(unittest.TestCase):
    """Unit: state machine auth guard for create_element, update_element, delete_element."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.session_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-elem"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()
        self.deck_id = uuid4()
        self.slide_id = uuid4()

        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        # Issue credential
        result = self.base_machine.apply(
            _issue_token(self.user_id, self.client_id, self.credential_id, self.digest),
            committed_index=1,
        )
        self.assertEqual(result.status, "TOKEN_ISSUED", result)
        # Publish deck below enforcement boundary
        result = self.base_machine.apply(
            _publish_deck_no_elements(self.deck_id, self.slide_id, client_id=self.client_id),
            committed_index=0,
        )
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"), result)

        # Create an element below the boundary to have something for update/delete tests.
        self.element_id = uuid4()
        create_result = self.base_machine.apply(
            _make_command("create_element", self.deck_id, {
                "slide_id": str(self.slide_id),
                "element": {
                    "element_id": str(self.element_id),
                    "kind": "rectangle",
                    "version": 1,
                    "configuration": _SHAPE_CONF,
                },
            }, client_id=self.client_id, session_id=self.session_id,
                request_id="setup-create-elem"),
            committed_index=0,
        )
        self.assertEqual(create_result.status, "CREATED_AND_LOCKED")
        self.fencing_token: int = create_result.value["fencing_token"]

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _actor(self) -> dict:
        return {
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }

    # --- create_element ---

    def test_valid_actor_permits_create_element(self) -> None:
        machine = self._machine()
        new_elem_id = uuid4()
        payload = {
            "slide_id": str(self.slide_id),
            "element": {
                "element_id": str(new_elem_id),
                "kind": "rectangle",
                "version": 1,
                "configuration": _SHAPE_CONF,
            },
            **self._actor(),
        }
        cmd = _make_command("create_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=str(uuid4()),
                            request_id="ce-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "CREATED_AND_LOCKED")

    def test_wrong_actor_rejects_create_element(self) -> None:
        machine = self._machine()
        new_elem_id = uuid4()
        payload = {
            "slide_id": str(self.slide_id),
            "element": {
                "element_id": str(new_elem_id),
                "kind": "rectangle",
                "version": 1,
                "configuration": _SHAPE_CONF,
            },
            "authenticated_actor_user_id": str(uuid4()),  # wrong
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("create_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=str(uuid4()),
                            request_id="ce-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_create_element(self) -> None:
        machine = self._machine()
        new_elem_id = uuid4()
        payload = {
            "slide_id": str(self.slide_id),
            "element": {
                "element_id": str(new_elem_id),
                "kind": "rectangle",
                "version": 1,
                "configuration": _SHAPE_CONF,
            },
        }
        cmd = _make_command("create_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=str(uuid4()),
                            request_id="ce-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_create_element(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        new_elem_id = uuid4()
        payload = {
            "slide_id": str(self.slide_id),
            "element": {
                "element_id": str(new_elem_id),
                "kind": "rectangle",
                "version": 1,
                "configuration": _SHAPE_CONF,
            },
        }
        cmd = _make_command("create_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=str(uuid4()),
                            request_id="ce-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "CREATED_AND_LOCKED")

    # --- update_element ---

    def test_valid_actor_permits_update_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            "patch": {"fill": "#AABBCCFF"},
            **self._actor(),
        }
        cmd = _make_command("update_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="ue-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "SAVED")

    def test_wrong_actor_rejects_update_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            "patch": {"fill": "#AABBCCFF"},
            "authenticated_actor_user_id": str(uuid4()),
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("update_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="ue-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_update_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            "patch": {"fill": "#AABBCCFF"},
        }
        cmd = _make_command("update_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="ue-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_update_element(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            "patch": {"fill": "#AABBCCFF"},
        }
        cmd = _make_command("update_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="ue-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "SAVED")

    # --- delete_element ---

    def test_valid_actor_permits_delete_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            **self._actor(),
        }
        cmd = _make_command("delete_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="de-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "DELETED")

    def test_wrong_actor_rejects_delete_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
            "authenticated_actor_user_id": str(uuid4()),
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("delete_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="de-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_delete_element(self) -> None:
        machine = self._machine()
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
        }
        cmd = _make_command("delete_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="de-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_delete_element(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": self.fencing_token,
            "expected_version": 1,
        }
        cmd = _make_command("delete_element", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="de-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=3)
        self.assertEqual(result.status, "DELETED")


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

try:
    from app import App
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class ElementMutationsAuthAdmissionTests(unittest.TestCase):
    """Integration via App.on_request for element mutations."""

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
        """Returns (client_id, access_token, deck_id, slide_id)."""
        client_id = str(uuid4())
        deck_id = str(uuid4())
        slide_id = str(uuid4())
        access_token = "bearer-elem-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(uuid4())
        self._commit_command(replica, _issue_token(self.user_id, client_id, credential_id, digest))
        self._commit_command(replica, _publish_deck_no_elements(deck_id, slide_id, client_id=client_id))
        return client_id, access_token, deck_id, slide_id

    def test_caller_supplied_actor_rejected_for_create_element(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id, slide_id = self._setup(replica)
        new_elem_id = str(uuid4())
        req = Request("req-ce-actor", "create_element", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "slide_id": slide_id,
            "element": {
                "element_id": new_elem_id,
                "kind": "rectangle", "version": 1,
                "configuration": _SHAPE_CONF,
            },
            "authenticated_actor_user_id": self.user_id,
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_valid_bearer_produces_authenticated_create_element_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, slide_id = self._setup(replica)
        new_elem_id = str(uuid4())
        req = Request("req-ce-valid", "create_element", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "slide_id": slide_id,
            "element": {
                "element_id": new_elem_id,
                "kind": "rectangle", "version": 1,
                "configuration": _SHAPE_CONF,
            },
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "create_element"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))

    def test_valid_bearer_produces_authenticated_update_element_command(self) -> None:
        """update_element admission: actor stamped, bearer not persisted."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, slide_id = self._setup(replica)
        # We can test the admission flow without caring about fencing_token validity
        # because the guard fires before the business logic.
        req = Request("req-ue-valid", "update_element", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": str(uuid4()),  # doesn't exist → NOT_FOUND after guard
            "fencing_token": 1,
            "expected_version": 1,
            "patch": {"fill": "#AABBCCFF"},
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "update_element"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))

    def test_valid_bearer_produces_authenticated_delete_element_command(self) -> None:
        """delete_element admission: actor stamped, bearer not persisted."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, slide_id = self._setup(replica)
        req = Request("req-dele-valid", "delete_element", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": str(uuid4()),  # doesn't exist → NOT_FOUND after guard
            "fencing_token": 1,
            "expected_version": 1,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "delete_element"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))


if __name__ == "__main__":
    unittest.main()
