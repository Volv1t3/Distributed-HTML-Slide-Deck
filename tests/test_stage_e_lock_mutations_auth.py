"""Stage E issue #43: bearer auth guard for acquire_lock, release_lock, renew_lock."""

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


def _publish_deck(deck_id: object, slide_id: object,
                  element_id: object, *, client_id: str = "client-a") -> CommandEnvelopeDTO:
    """Publish a deck with one element so we can test lock operations."""
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
                    "elements": [{
                        "element_id": str(element_id),
                        "kind": "rectangle",
                        "version": 1,
                        "configuration": _SHAPE_CONF,
                    }],
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


class LockMutationsAuthGuardTests(unittest.TestCase):
    """Unit: state machine auth guard for acquire_lock, release_lock, renew_lock."""

    def setUp(self) -> None:
        self.catalog = load_account_catalog()
        self.user_id = str(self.catalog.users[0].profile.user_id)
        self.client_id = str(uuid4())
        self.session_id = str(uuid4())
        self.credential_id = str(uuid4())
        self.access_token = "test-bearer-lock"
        self.digest = hashlib.sha256(self.access_token.encode("ascii")).hexdigest()
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.element_id = uuid4()

        auth = initial_auth_state(self.catalog, enforced_from_index=1)
        self.base_machine = KeyValueStateMachine({"kv": {}, "auth": deepcopy(auth)})
        result = self.base_machine.apply(
            _issue_token(self.user_id, self.client_id, self.credential_id, self.digest),
            committed_index=1,
        )
        self.assertEqual(result.status, "TOKEN_ISSUED", result)
        result = self.base_machine.apply(
            _publish_deck(self.deck_id, self.slide_id, self.element_id,
                          client_id=self.client_id),
            committed_index=0,
        )
        self.assertIn(result.status, ("PUBLISHED", "DECK_PUBLISHED"), result)

    def _machine(self) -> KeyValueStateMachine:
        return KeyValueStateMachine(deepcopy(self.base_machine.snapshot()))

    def _actor(self) -> dict:
        return {
            "authenticated_actor_user_id": self.user_id,
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }

    def _do_acquire(self, machine: KeyValueStateMachine, *,
                    actor: bool = True,
                    wrong_actor: bool = False,
                    committed_index: int = 3) -> object:
        payload: dict = {"element_id": str(self.element_id)}
        if actor:
            payload.update(self._actor())
        elif wrong_actor:
            payload.update({
                "authenticated_actor_user_id": str(uuid4()),
                "credential_id": self.credential_id,
                "credential_generation": 1,
            })
        return machine.apply(
            _make_command("acquire_lock", self.deck_id, payload,
                          client_id=self.client_id,
                          session_id=self.session_id,
                          request_id="acq-" + str(uuid4())),
            committed_index=committed_index,
        )

    # --- acquire_lock ---

    def test_valid_actor_permits_acquire_lock(self) -> None:
        machine = self._machine()
        result = self._do_acquire(machine, actor=True)
        self.assertEqual(result.status, "LOCK_ACQUIRED")

    def test_wrong_actor_rejects_acquire_lock(self) -> None:
        machine = self._machine()
        result = self._do_acquire(machine, actor=False, wrong_actor=True)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_acquire_lock(self) -> None:
        machine = self._machine()
        result = self._do_acquire(machine, actor=False, committed_index=3)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_acquire_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        result = self._do_acquire(machine, actor=False, committed_index=3)
        self.assertEqual(result.status, "LOCK_ACQUIRED")

    # --- release_lock ---

    def test_valid_actor_permits_release_lock(self) -> None:
        machine = self._machine()
        # First acquire below boundary (no actor, enforced_from_index=100 temporarily)
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        self.assertEqual(acq.status, "LOCK_ACQUIRED")
        fencing_token = acq.value["fencing_token"]
        # Now restore the machine snapshot from low_machine (which has the lock)
        machine = low_machine
        snap2 = deepcopy(machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1  # re-enforce
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
            **self._actor(),
        }
        cmd = _make_command("release_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="rel-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "RELEASED")

    def test_wrong_actor_rejects_release_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        snap2 = deepcopy(low_machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
            "authenticated_actor_user_id": str(uuid4()),
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("release_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="rel-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_release_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        snap2 = deepcopy(low_machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
        }
        cmd = _make_command("release_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="rel-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_release_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
        }
        cmd = _make_command("release_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="rel-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "RELEASED")

    # --- renew_lock ---

    def test_valid_actor_permits_renew_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        snap2 = deepcopy(low_machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
            **self._actor(),
        }
        cmd = _make_command("renew_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="renew-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "RENEWED")

    def test_wrong_actor_rejects_renew_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        snap2 = deepcopy(low_machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
            "authenticated_actor_user_id": str(uuid4()),
            "credential_id": self.credential_id,
            "credential_generation": 1,
        }
        cmd = _make_command("renew_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="renew-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_at_boundary_rejects_renew_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        low_machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(low_machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        snap2 = deepcopy(low_machine.snapshot())
        snap2["auth"]["enforced_from_index"] = 1
        machine = KeyValueStateMachine(snap2)

        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
        }
        cmd = _make_command("renew_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="renew-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "UNAUTHENTICATED")

    def test_missing_actor_below_boundary_allowed_for_renew_lock(self) -> None:
        machine = self._machine()
        snap = deepcopy(machine.snapshot())
        snap["auth"]["enforced_from_index"] = 100
        machine = KeyValueStateMachine(snap)
        acq = self._do_acquire(machine, actor=False, committed_index=3)
        fencing_token = acq.value["fencing_token"]
        payload = {
            "element_id": str(self.element_id),
            "fencing_token": fencing_token,
        }
        cmd = _make_command("renew_lock", self.deck_id, payload,
                            client_id=self.client_id,
                            session_id=self.session_id,
                            request_id="renew-" + str(uuid4()))
        result = machine.apply(cmd, committed_index=4)
        self.assertEqual(result.status, "RENEWED")


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

try:
    from app import App
    _APP_AVAILABLE = True
except ImportError:
    _APP_AVAILABLE = False


@unittest.skipUnless(_APP_AVAILABLE, "app.py not importable")
class LockMutationsAuthAdmissionTests(unittest.TestCase):
    """Integration via App.on_request for lock mutations."""

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
        """Returns (client_id, access_token, deck_id, element_id)."""
        client_id = str(uuid4())
        deck_id = str(uuid4())
        slide_id = str(uuid4())
        element_id = str(uuid4())
        access_token = "bearer-lock-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(uuid4())
        self._commit_command(replica, _issue_token(self.user_id, client_id, credential_id, digest))
        self._commit_command(replica, _publish_deck(deck_id, slide_id, element_id, client_id=client_id))
        return client_id, access_token, deck_id, element_id

    def test_caller_supplied_actor_rejected_for_acquire_lock(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, _, deck_id, element_id = self._setup(replica)
        req = Request("req-acq-actor", "acquire_lock", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": element_id,
            "authenticated_actor_user_id": self.user_id,
            "_auth": {"access_token": "some-token"},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNotNone(result)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(result.get("result", {}).get("status"), "INVALID_REQUEST")

    def test_valid_bearer_produces_authenticated_acquire_lock_command(self) -> None:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, element_id = self._setup(replica)
        req = Request("req-acq-valid", "acquire_lock", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": element_id,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "acquire_lock"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))

    def test_valid_bearer_produces_authenticated_release_lock_command(self) -> None:
        """release_lock admission: actor stamped, bearer not persisted."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, element_id = self._setup(replica)
        # fencing_token 1 is unknown to the guard (guard fires first)
        req = Request("req-rel-valid", "release_lock", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": element_id,
            "fencing_token": 1,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "release_lock"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))

    def test_valid_bearer_produces_authenticated_renew_lock_command(self) -> None:
        """renew_lock admission: actor stamped, bearer not persisted."""
        replica = self._app()
        self._elect_and_commit_noop(replica)
        client_id, access_token, deck_id, element_id = self._setup(replica)
        req = Request("req-renew-valid", "renew_lock", {
            "deck_id": deck_id,
            "session_id": str(uuid4()),
            "element_id": element_id,
            "fencing_token": 1,
            "_auth": {"access_token": access_token},
        }, client_id)
        result = replica.on_request(req)
        self.assertIsNone(result)
        self._acknowledge_read(replica)
        replica._progress_logins()
        log = replica.engine.state.raft.log
        entries = [e for e in log if e.command.operation == "renew_lock"]
        self.assertTrue(entries)
        cmd = entries[-1].command
        self.assertIsNotNone(cmd.payload.get("authenticated_actor_user_id"))
        import json
        self.assertNotIn(access_token, json.dumps(cmd.payload))


if __name__ == "__main__":
    unittest.main()
