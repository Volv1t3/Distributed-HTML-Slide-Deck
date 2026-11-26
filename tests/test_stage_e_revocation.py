"""Fenced, committed credential self-revocation."""

import hashlib
import json
import os
import queue
from copy import deepcopy
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from app import App
from authenticated_client import AuthenticatedClient
from auth_catalog import load_account_catalog
from auth_admission import PasswordCheckResult
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO, validate_committed_command
from dto_base import sha256_json
from persistence import InMemoryPersistence
from raft_core import RaftEngine
from state_machine import KeyValueStateMachine
from secret_scanner import scan_artifacts
from node import Request
from client_journal import ClientIntentionOutbox
import cluster


class RevocationTests(unittest.TestCase):
    def setUp(self):
        self.user = str(load_account_catalog().users[0].profile.user_id)
        self.client = str(uuid4())
        self.credential = str(uuid4())
        self.machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(load_account_catalog(), enforced_from_index=1)})
        self.machine.apply(self.command("issue_access_token", "login-1", {
            "user_id": self.user, "client_id": self.client,
            "credential_id": self.credential, "token_digest": hashlib.sha256(b"token-one").hexdigest(),
        }), committed_index=1)

    def command(self, operation, request_id, payload):
        return CommandEnvelopeDTO(command_schema_version=1, request_id=request_id,
                                  client_id=self.client, origin_node_uuid=uuid4(),
                                  operation=operation, payload=payload)

    def revoke(self, request_id="revoke-1"):
        return self.command("revoke_access_token", request_id, {
            "client_id": self.client, "authenticated_actor_user_id": self.user,
            "credential_id": self.credential, "credential_generation": 1,
            "expected_credential_id": self.credential, "expected_generation": 1,
        })

    def settings_machine(self, deck_id=None):
        """Create a published deck and current credential without another TestCase fixture."""
        deck_id = deck_id or uuid4()
        machine = KeyValueStateMachine(self.machine.snapshot())
        published = CommandEnvelopeDTO(
            command_schema_version=1, request_id="publish-before-auth",
            client_id=self.client, origin_node_uuid=uuid4(), deck_id=deck_id,
            operation="publish_deck", payload={"presentation": {
                "schema_version": 2,
                "metadata": {"deck_id": str(deck_id), "name": "Original",
                             "owner_session_id": str(uuid4()), "width_px": 1280,
                             "height_px": 720},
                "slides": [{"metadata": {"slide_id": str(uuid4()),
                                         "background": "#112233FF"}, "elements": []}],
            }},
        )
        self.assertEqual(machine.apply(published, committed_index=0).status, "PUBLISHED")
        update = CommandEnvelopeDTO(
            command_schema_version=1, request_id="settings-update",
            client_id=self.client, origin_node_uuid=uuid4(), deck_id=deck_id,
            operation="update_deck_settings", payload={
                "expected_deck_settings_version": 0,
                "patch": {"name": "Updated"},
                "authenticated_actor_user_id": self.user,
                "credential_id": self.credential,
                "credential_generation": 1,
            },
        )
        return machine, deck_id, update

    def test_current_revoke_commits_once_and_stale_revoke_cannot_cancel_new_login(self):
        revoke = self.revoke()
        validate_committed_command(revoke)
        self.assertEqual(self.machine.apply(revoke, committed_index=2).status, "TOKEN_REVOKED")
        active = self.machine.snapshot()["auth"]["active_credentials"][self.client]
        self.assertTrue(active["revoked"])
        self.assertEqual(active["credential_generation"], 2)
        replacement = str(uuid4())
        self.machine.apply(self.command("issue_access_token", "login-2", {
            "user_id": self.user, "client_id": self.client, "credential_id": replacement,
            "token_digest": hashlib.sha256(b"token-two").hexdigest(),
        }), committed_index=3)
        before = self.machine.snapshot()
        self.assertEqual(self.machine.apply(self.revoke("revoke-late"), committed_index=4).status, "STALE_CREDENTIAL")
        self.assertEqual(self.machine.snapshot(), before)

    def test_artifact_scanner_finds_literals_in_nested_debug_and_history_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "n1").mkdir()
            (root / "n1" / "events.log").write_text('{"payload":"literal-password"}')
            (root / "archive.jsonl").write_text('{"result":"literal-bearer"}')
            (root / "safe.json").write_text('{"token_digest":"abc123"}')
            self.assertEqual(set(scan_artifacts(root, ["literal-password", "literal-bearer"])), {
                root / "n1" / "events.log", root / "archive.jsonl",
            })

    def test_raft_exact_revoke_replay_and_preserved_state_restart(self):
        storage = InMemoryPersistence()
        node_uuid = uuid4()
        engine = RaftEngine(node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=node_uuid,
                            storage=storage, send=lambda *_: None, clock=lambda: 0.0,
                            genesis_fsm=self.machine.snapshot())
        engine.role = "leader"
        engine.state.raft.current_term = 1
        engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
        engine.next_index = {"n2": 1, "n3": 1}
        command = self.revoke()
        entry = engine.propose(command)
        engine.state.raft.commit_index = entry.index
        engine._apply_committed()
        storage.save(engine.state)
        key = sha256_json([command.client_id, command.request_id])
        self.assertEqual(engine.state.request_results[key].result.status, "TOKEN_REVOKED")
        after = deepcopy(engine.state.fsm)
        self.assertEqual(engine.propose(command).status, "TOKEN_REVOKED")
        self.assertEqual(engine.state.fsm, after)
        restored = RaftEngine(node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=node_uuid,
                              storage=storage, send=lambda *_: None, clock=lambda: 0.0)
        self.assertEqual(restored.state.fsm, after)
        self.assertTrue(restored.state.fsm["auth"]["active_credentials"][self.client]["revoked"])

    def test_revoke_exact_id_replay_after_rotation_returns_token_revoked(self):
        """A revoke retried with a reauthenticated stamp replays its stored result.

        The stamped credential_id/generation are transient proof material and are
        excluded from the fingerprint, so an exact revoke retry after a bearer
        rotation must return TOKEN_REVOKED rather than REQUEST_ID_REUSE. The
        expected fence stays in the fingerprint, so a different fence is reuse.
        """
        storage = InMemoryPersistence()
        engine = RaftEngine(node_slot="n1", voters={"n1", "n2", "n3"}, node_uuid=uuid4(),
                            storage=storage, send=lambda *_: None, clock=lambda: 0.0,
                            genesis_fsm=self.machine.snapshot())
        engine.role = "leader"
        engine.state.raft.current_term = 1
        engine.match_index = {"n1": 0, "n2": 0, "n3": 0}
        engine.next_index = {"n2": 1, "n3": 1}
        entry = engine.propose(self.revoke("revoke-replay"))
        engine.state.raft.commit_index = entry.index
        engine._apply_committed()
        after_first = deepcopy(engine.state.fsm)

        rotated_stamp = self.command("revoke_access_token", "revoke-replay", {
            "client_id": self.client, "authenticated_actor_user_id": self.user,
            "credential_id": str(uuid4()), "credential_generation": 2,
            "expected_credential_id": self.credential, "expected_generation": 1,
        })
        self.assertEqual(engine.propose(rotated_stamp).status, "TOKEN_REVOKED")
        self.assertEqual(engine.state.fsm, after_first)

        different_fence = self.command("revoke_access_token", "revoke-replay", {
            "client_id": self.client, "authenticated_actor_user_id": self.user,
            "credential_id": self.credential, "credential_generation": 1,
            "expected_credential_id": self.credential, "expected_generation": 7,
        })
        self.assertEqual(engine.propose(different_fence).status, "REQUEST_ID_REUSE")

    def test_settings_update_and_revoke_follow_committed_log_order(self):
        first, deck_id, update = self.settings_machine()
        revoke = self.revoke("revoke-order")
        self.assertEqual(first.apply(update, committed_index=3).status, "DECK_SETTINGS_UPDATED")
        self.assertEqual(first.apply(revoke, committed_index=4).status, "TOKEN_REVOKED")
        self.assertEqual(first.snapshot()["decks"][str(deck_id)]["deck_settings_version"], 1)
        second, _, _ = self.settings_machine(deck_id)
        self.assertEqual(second.apply(revoke, committed_index=3).status, "TOKEN_REVOKED")
        self.assertEqual(second.apply(update, committed_index=4).status, "UNAUTHENTICATED")
        deck = second.snapshot()["decks"][str(deck_id)]
        self.assertEqual((deck["deck_version"], deck["deck_settings_version"]), (0, 0))

    def test_rotation_before_old_token_update_preserves_settings_revisions(self):
        machine, deck_id, update = self.settings_machine()
        replacement = str(uuid4())
        issue = self.command("issue_access_token", "login-2", {
            "user_id": self.user, "client_id": self.client,
            "credential_id": replacement,
            "token_digest": hashlib.sha256(b"replacement").hexdigest(),
        })
        self.assertEqual(machine.apply(issue, committed_index=3).status, "TOKEN_ISSUED")
        self.assertEqual(machine.apply(update, committed_index=4).status, "UNAUTHENTICATED")
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual((deck["deck_version"], deck["deck_settings_version"]), (0, 0))


class RevocationAdmissionTests(unittest.TestCase):
    """Exercise the node request boundary with a correlated quorum response."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": "DEBUG"}):
            self.app = App("n1", nodes, self.root)
            self.app.start()
        self.addCleanup(self.app._log.close)
        self.addCleanup(self.app._events.close)
        self.addCleanup(self.app._password_verifier.shutdown)
        self.client = str(uuid4())
        self.user = str(load_account_catalog().users[0].profile.user_id)
        self.credential = str(uuid4())
        self.token = "literal-stage-e-bearer-for-scanning"

    def messages(self):
        sent = []
        while True:
            try:
                sent.append(self.app._out.get_nowait())
            except queue.Empty:
                return sent

    def peer_reply(self, request, *, match_index=None):
        response = {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(self.app.engine.state.peer_bindings["n2"]),
            "term": self.app.engine.state.raft.current_term,
            "reply_to": request["rpc_id"], "success": True,
            "match_index": request["prev_log_index"] if match_index is None else match_index,
        }
        if request.get("read_context") is not None:
            response["read_context"] = request["read_context"]
        self.app.on_message("n2", response)

    def elect(self):
        self.messages()
        for slot in ("n2", "n3"):
            self.app.on_message(slot, {
                "kind": "register_node", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": slot,
                "sender_uuid": str(uuid4()), "term": 0, "incarnation": 1,
                "membership_fingerprint": self.app.engine.membership_fingerprint(),
            })
        self.messages()
        self.app.engine.start_election()
        vote = next(msg for dst, msg in self.messages()
                    if dst == "n2" and msg["kind"] == "request_vote")
        self.app.on_message("n2", {
            "kind": "request_vote_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(self.app.engine.state.peer_bindings["n2"]),
            "term": vote["term"], "reply_to": vote["rpc_id"], "vote_granted": True,
        })
        append = next(msg for dst, msg in self.messages()
                      if dst == "n2" and msg["kind"] == "append_entries" and msg["entries"])
        self.peer_reply(append, match_index=append["entries"][-1]["index"])
        self.messages()

    def issue(self):
        command = CommandEnvelopeDTO(
            command_schema_version=1, request_id="issued", client_id=self.client,
            origin_node_uuid=self.app.engine.node_uuid,
            operation="issue_access_token", payload={
                "user_id": self.user, "client_id": self.client,
                "credential_id": self.credential,
                "token_digest": hashlib.sha256(self.token.encode()).hexdigest(),
            },
        )
        outcome = self.app.engine.propose(command)
        self.assertEqual(outcome.index, len(self.app.engine.state.raft.log))
        append = next(msg for dst, msg in self.messages()
                      if dst == "n2" and msg["kind"] == "append_entries"
                      and any(entry["command"]["operation"] == "issue_access_token"
                              for entry in msg["entries"]))
        self.peer_reply(append, match_index=append["entries"][-1]["index"])
        self.messages()

    def revoke_request(self, request_id="revoke"):
        return Request(request_id, "revoke_access_token", {
            "expected_credential_id": self.credential, "expected_generation": 1,
            "_auth": {"access_token": self.token},
        }, self.client)

    def commit_revoke(self):
        request = self.revoke_request()
        done, response = threading.Event(), {}
        self.app._waiting[request.id] = (done, response)
        self.assertIsNone(self.app.on_request(request))
        probe = next(msg for dst, msg in self.messages()
                     if dst == "n2" and msg["kind"] == "append_entries"
                     and msg.get("read_context") is not None)
        self.peer_reply(probe)
        append = next(msg for dst, msg in self.messages()
                      if dst == "n2" and msg["kind"] == "append_entries"
                      and any(entry["command"]["operation"] == "revoke_access_token"
                              for entry in msg["entries"]))
        self.peer_reply(append, match_index=append["entries"][-1]["index"])
        self.assertTrue(done.is_set())
        self.assertEqual(response["result"]["status"], "TOKEN_REVOKED")
        self.messages()

    def test_no_quorum_revoke_and_isolated_leader_reveal_no_stored_result(self):
        self.elect()
        self.issue()
        pending = self.revoke_request()
        done, response = threading.Event(), {}
        self.app._waiting[pending.id] = (done, response)
        self.assertIsNone(self.app.on_request(pending))
        self.assertFalse(done.is_set())
        self.assertFalse(any(entry.command.operation == "revoke_access_token"
                             for entry in self.app.engine.state.raft.log))
        self.messages()  # drop both quorum probes
        self.app.engine.clock = lambda: 10 ** 9
        self.app.on_tick()
        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
        self.assertFalse(self.app.engine.state.fsm["auth"]["active_credentials"][self.client]["revoked"])
        self.assertFalse(any(entry.command.operation == "revoke_access_token"
                             for entry in self.app.engine.state.raft.log))
        replay_done, replay_response = threading.Event(), {}
        self.app._waiting["issued"] = (replay_done, replay_response)
        self.assertIsNone(self.app.on_request(self.revoke_request("issued")))
        self.assertFalse(replay_done.is_set())
        self.assertFalse(any(entry.command.operation == "revoke_access_token"
                             for entry in self.app.engine.state.raft.log))
        self.messages()
        self.app.engine.clock = lambda: 2 * 10 ** 9
        self.app.on_tick()
        self.assertTrue(replay_done.is_set())
        self.assertEqual(replay_response, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
        login = Request("isolated-login", "login", {
            "email": "sofia@example.test", "password": "Sofia-Classroom-1",
            "client_id": self.client,
        }, self.client)
        login_done, login_response = threading.Event(), {}
        self.app._waiting[login.id] = (login_done, login_response)
        self.assertIsNone(self.app.on_request(login))
        self.messages()
        self.app.engine.clock = lambda: 3 * 10 ** 9
        self.app.on_tick()
        self.assertTrue(login_done.is_set())
        self.assertEqual(login_response, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
        self.assertFalse(any(entry.command.operation == "issue_access_token"
                             and entry.command.request_id == login.id
                             for entry in self.app.engine.state.raft.log))

    def test_preserved_disk_restart_rejects_revoked_bearer_before_dedup_disclosure(self):
        self.elect()
        self.issue()
        self.commit_revoke()
        self.app._password_verifier.shutdown()
        self.app._events.close()
        self.app._log.close()
        self.app = App("n1", self.app.nodes, self.root)
        self.app.start()
        self.addCleanup(self.app._log.close)
        self.addCleanup(self.app._events.close)
        self.addCleanup(self.app._password_verifier.shutdown)
        self.assertEqual(self.app.engine.state.incarnation, 2)
        self.assertTrue(self.app.engine.state.fsm["auth"]["active_credentials"][self.client]["revoked"])
        self.elect()
        for request_id in ("revoke", "new-revoke"):
            done, response = threading.Event(), {}
            self.app._waiting[request_id] = (done, response)
            self.assertIsNone(self.app.on_request(self.revoke_request(request_id)))
            probe = next(msg for dst, msg in self.messages()
                         if dst == "n2" and msg["kind"] == "append_entries"
                         and msg.get("read_context") is not None)
            self.peer_reply(probe)
            self.assertTrue(done.is_set())
            self.assertEqual(response, {"ok": False, "error": "REAUTH_REQUIRED"})
        self.assertEqual(sum(entry.command.operation == "revoke_access_token"
                             for entry in self.app.engine.state.raft.log), 1)

    def test_blocked_password_worker_does_not_stop_tick_or_message_and_stale_result_is_discarded(self):
        self.elect()
        started, release = threading.Event(), threading.Event()

        def slow_verification(task):
            started.set()
            release.wait(2)
            return PasswordCheckResult(task.attempt_id, task.captured_term, True)

        request = Request("slow-login", "login", {
            "email": "sofia@example.test", "password": "Sofia-Classroom-1",
            "client_id": self.client,
        }, self.client)
        done, response = threading.Event(), {}
        self.app._waiting[request.id] = (done, response)
        with patch("auth_admission._verify_task", side_effect=slow_verification):
            self.assertIsNone(self.app.on_request(request))
            probe = next(msg for dst, msg in self.messages()
                         if dst == "n2" and msg["kind"] == "append_entries"
                         and msg.get("read_context") is not None)
            self.peer_reply(probe)
            self.assertTrue(started.wait(1))
            self.messages()
            self.app.engine.clock = lambda: 10 ** 9
            tick = threading.Thread(target=self.app.on_tick)
            tick.start()
            tick.join(0.2)
            try:
                self.assertFalse(tick.is_alive(), "Raft tick waited for password work")
                self.assertTrue(any(message["kind"] == "append_entries"
                                    for _, message in self.messages()),
                                "leader did not send heartbeats while password work was blocked")
                self.app.on_message("n2", {
                    "kind": "append_entries", "protocol_version": 1,
                    "rpc_id": str(uuid4()), "sender_slot": "n2",
                    "sender_uuid": str(self.app.engine.state.peer_bindings["n2"]),
                    "term": 2, "prev_log_index": 0, "prev_log_term": 0,
                    "leader_commit": 0, "entries": [],
                })
                self.assertEqual(self.app.engine.state.raft.current_term, 2)
                self.assertEqual(self.app.engine.role, "follower")
                self.app.engine.clock = lambda: 2 * 10 ** 9
                self.app.on_tick()
                self.assertEqual(self.app.engine.role, "candidate")
                self.assertEqual(self.app.engine.state.raft.current_term, 3)
            finally:
                release.set()
                tick.join(1)
            for _ in range(100):
                self.app.on_tick()
                if done.is_set():
                    break
                threading.Event().wait(0.005)
        self.assertTrue(done.is_set())
        self.assertNotEqual(response.get("result", {}).get("status"), "AUTHENTICATED")
        self.assertFalse(any(entry.command.operation == "issue_access_token"
                             for entry in self.app.engine.state.raft.log))

    def test_actual_debug_and_persisted_raft_artifacts_contain_no_bearer(self):
        self.elect()
        self.issue()
        password = "Sofia-Classroom-1"
        self.assertIsNone(self.app.on_request(Request("scan-login", "login", {
            "email": "sofia@example.test", "password": password,
            "client_id": self.client,
        }, self.client)))
        self.messages()  # no quorum response for this password admission
        self.commit_revoke()
        self.assertTrue((self.root / "n1" / "disk.json").exists())
        self.assertTrue((self.root / "n1" / "events.log").exists())
        with tempfile.TemporaryDirectory() as credential_directory:
            transport = AuthenticatedClient(Path(credential_directory) / "client", [],
                                            self.root / "client-history.jsonl")
            with patch.object(transport, "_try_nodes", return_value={
                "ok": False, "error": "REAUTH_REQUIRED",
            }):
                transport._in_memory_credential = {"access_token": self.token}
                transport.call("history-revoke", "revoke_access_token", {
                    "expected_credential_id": self.credential,
                    "expected_generation": 1,
                })
                journal = ClientIntentionOutbox(self.client, self.root / "intentions.jsonl", str(uuid4()))
                deck_id = str(uuid4())
                journal.record_base_projection({"deck_id": deck_id, "applied_index": 1})
                journal.enqueue("update_deck_settings", {
                    "deck_id": deck_id, "expected_deck_settings_version": 0,
                    "patch": {"name": "Safe edit"},
                }, request_id="journal-edit")
                journal.synchronize(lambda intent: transport.call(
                    intent.request_id, intent.operation, intent.arguments))
            with patch.object(cluster, "RUN", self.root), patch.object(cluster, "HERE", self.root):
                archived = cluster.keep_histories()
            self.assertIsNotNone(archived)
            self.assertTrue((archived / "n1" / "events.log").is_file())
            self.assertEqual(scan_artifacts(self.root, [self.token, password]), [])
        durable = json.loads((self.root / "n1" / "disk.json").read_text())["raft_state"]
        self.assertTrue(durable["request_results"])
        self.assertTrue(durable["fsm"]["auth"]["active_credentials"][self.client]["revoked"])


class RevocationFailoverTests(unittest.TestCase):
    def test_new_leader_retains_replicated_revocation_after_old_leader_isolated(self):
        slots = ("n1", "n2", "n3")
        ids = {slot: uuid4() for slot in slots}
        storage = {slot: InMemoryPersistence() for slot in slots}
        network = []
        auth = initial_auth_state(load_account_catalog(), enforced_from_index=1)
        engines = {
            slot: RaftEngine(
                node_slot=slot, voters=slots, node_uuid=ids[slot],
                storage=storage[slot],
                send=lambda destination, message, source=slot:
                    network.append((source, destination, message)),
                clock=lambda: 0.0,
                genesis_fsm={"kv": {}, "auth": deepcopy(auth)},
            ) for slot in slots
        }
        for source in slots:
            for destination in slots:
                if source != destination:
                    engines[destination].handle_message(source, {
                        "kind": "register_node", "protocol_version": 1,
                        "rpc_id": str(uuid4()), "sender_slot": source,
                        "sender_uuid": str(ids[source]), "term": 0,
                        "incarnation": 1,
                        "membership_fingerprint": engines[source].membership_fingerprint(),
                    })

        def deliver(*, isolated=()):
            steps = 0
            while network:
                source, destination, message = network.pop(0)
                if source in isolated or destination in isolated:
                    continue
                engines[destination].handle_message(source, message)
                steps += 1
                self.assertLess(steps, 500, "replication did not settle")

        network.clear()
        engines["n1"].start_election()
        deliver()
        self.assertEqual(engines["n1"].role, "leader")
        client, credential = str(uuid4()), str(uuid4())
        user = str(load_account_catalog().users[0].profile.user_id)
        issue = CommandEnvelopeDTO(
            command_schema_version=1, request_id="replicated-login",
            client_id=client, origin_node_uuid=ids["n1"],
            operation="issue_access_token", payload={
                "user_id": user, "client_id": client,
                "credential_id": credential,
                "token_digest": hashlib.sha256(b"failover-token").hexdigest(),
            },
        )
        engines["n1"].propose(issue)
        deliver()
        revoke = CommandEnvelopeDTO(
            command_schema_version=1, request_id="replicated-revoke",
            client_id=client, origin_node_uuid=ids["n1"],
            operation="revoke_access_token", payload={
                "client_id": client, "authenticated_actor_user_id": user,
                "credential_id": credential, "credential_generation": 1,
                "expected_credential_id": credential, "expected_generation": 1,
            },
        )
        engines["n1"].propose(revoke)
        deliver()
        self.assertTrue(all(engine.state.fsm["auth"]["active_credentials"][client]["revoked"]
                            for engine in engines.values()))
        network.clear()
        engines["n2"].start_election()
        deliver(isolated={"n1"})
        self.assertEqual(engines["n2"].role, "leader")
        self.assertTrue(engines["n2"].state.fsm["auth"]["active_credentials"][client]["revoked"])
        self.assertTrue(storage["n2"].load().fsm["auth"]["active_credentials"][client]["revoked"])


if __name__ == "__main__":
    unittest.main()
