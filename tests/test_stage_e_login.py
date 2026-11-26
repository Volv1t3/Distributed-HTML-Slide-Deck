"""Stage E issue #40: quorum-confirmed password login admission."""

from __future__ import annotations

import json
import os
import queue
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from app import App
from auth_admission import (
    BoundedPasswordVerifier,
    PasswordCheckResult,
    PasswordCheckTask,
    REDACTED,
    redact_auth_data,
)
from auth_catalog import load_account_catalog
from command_dto import CommandEnvelopeDTO
from dto_base import sha256_json
from node import Request


class ControlledPasswordVerifier:
    """Deterministic system-boundary fake; it never inspects App or Raft state."""

    def __init__(self) -> None:
        self.tasks = []
        self.results = []
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


class LoginAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replicas: list[App] = []

    def tearDown(self) -> None:
        for replica in self.replicas:
            replica._password_verifier.shutdown()
            replica._events.close()
            replica._log.close()
        self.workspace.cleanup()

    def app(self, *, mode: str = "OPERATIONAL") -> App:
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": mode}, clear=False):
            replica = App("n1", self.nodes, self.run_dir)
            replica.start()
        self.replicas.append(replica)
        return replica

    @staticmethod
    def request(request_id: str = "login-1", *, email: str = "sofia@example.test",
                password: str = "Sofia-Classroom-1", client_id: str | None = None) -> Request:
        client = client_id or str(uuid4())
        return Request(request_id, "login", {
            "email": email, "password": password, "client_id": client,
        }, client)

    @staticmethod
    def messages(replica: App) -> list[tuple[str, dict[str, object]]]:
        sent = []
        while True:
            try:
                sent.append(replica._out.get_nowait())
            except queue.Empty:
                return sent

    def register(self, replica: App, slot: str) -> None:
        replica.on_message(slot, {
            "kind": "register_node", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": slot,
            "sender_uuid": str(uuid4()), "term": 0, "incarnation": 1,
            "membership_fingerprint": replica.engine.membership_fingerprint(),
        })

    def elect_and_commit_noop(self, replica: App) -> None:
        self.messages(replica)
        self.register(replica, "n2")
        self.register(replica, "n3")
        self.messages(replica)
        replica.engine.start_election()
        vote = next(message for destination, message in self.messages(replica)
                    if destination == "n2" and message["kind"] == "request_vote")
        replica.on_message("n2", {
            "kind": "request_vote_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": vote["rpc_id"], "vote_granted": True,
        })
        append = next(message for destination, message in self.messages(replica)
                      if destination == "n2" and message["kind"] == "append_entries"
                      and message["entries"])
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self.messages(replica)

    def acknowledge_read(self, replica: App) -> None:
        probe = next(message for destination, message in self.messages(replica)
                     if destination == "n2" and message["kind"] == "append_entries"
                     and message.get("read_context") is not None)
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": probe["rpc_id"], "success": True,
            "match_index": probe["prev_log_index"],
            "read_context": probe["read_context"],
        })

    def install_controlled_verifier(self, replica: App) -> ControlledPasswordVerifier:
        replica._password_verifier.shutdown()
        verifier = ControlledPasswordVerifier()
        replica._password_verifier = verifier
        return verifier

    def test_redaction_is_recursive_case_insensitive_and_pre_serialization_safe(self) -> None:
        raw = {"password": "literal-password", "Nested": [{"Access_Token": "literal-token"}],
               "_AUTH": {"access_token": "nested-token"}, "safe": "visible"}
        redacted = redact_auth_data(raw)
        serialized = json.dumps(redacted)
        self.assertEqual(redacted["password"], REDACTED)
        self.assertEqual(redacted["safe"], "visible")
        self.assertNotIn("literal-password", serialized)
        self.assertNotIn("literal-token", serialized)
        self.assertNotIn("nested-token", serialized)

    def test_password_executor_has_an_explicit_pending_cap(self) -> None:
        started, release = threading.Event(), threading.Event()

        def blocked(task):
            started.set()
            release.wait(1)
            return PasswordCheckResult(task.attempt_id, task.captured_term, False)

        verifier_record = load_account_catalog().users[0].verifier
        verifier = BoundedPasswordVerifier(max_workers=1, max_pending=1)
        first = PasswordCheckTask("first", 1, None, verifier_record, b"candidate")
        second = PasswordCheckTask("second", 1, None, verifier_record, b"candidate")
        try:
            with patch("auth_admission._verify_task", side_effect=blocked):
                self.assertTrue(verifier.submit(first))
                self.assertTrue(started.wait(1))
                self.assertFalse(verifier.submit(second))
                release.set()
                for _ in range(100):
                    results = verifier.drain_results()
                    if results:
                        break
                    threading.Event().wait(0.005)
                self.assertEqual(results, [PasswordCheckResult("first", 1, False)])
                self.assertTrue(verifier.submit(second))
        finally:
            release.set()
            verifier.shutdown()

    def test_follower_redirects_before_password_work_and_logs_only_redacted_payload(self) -> None:
        replica = self.app(mode="DEBUG")
        request = self.request(password="literal-password")
        replica.engine.leader_slot = "n2"

        self.assertEqual(replica.on_request(request), {"redirect": "n2"})
        self.assertEqual(replica._password_checks, {})
        events = (self.run_dir / "n1" / "events.log").read_text()
        self.assertNotIn("literal-password", events)
        self.assertIn(REDACTED, events)

    def test_malformed_scalar_login_payload_is_rejected_without_diagnostic_leakage(self) -> None:
        replica = self.app(mode="DEBUG")
        self.elect_and_commit_noop(replica)
        literal_secret = "literal-scalar-password"
        request = Request("malformed-login", "login", literal_secret, str(uuid4()))

        response = replica.on_request(request)

        self.assertEqual(response, {"ok": True, "result": {"status": "INVALID_REQUEST"}})
        events = (self.run_dir / "n1" / "events.log").read_text()
        self.assertNotIn(literal_secret, events)
        self.assertIn(REDACTED, events)

    def test_same_request_id_is_namespaced_per_client_across_pending_login_stages(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request_id = "shared-login-id"
        clients = (str(uuid4()), str(uuid4()))
        requests = tuple(self.request(request_id, client_id=client_id) for client_id in clients)

        self.assertIsNone(replica.on_request(requests[0]))
        self.assertIsNone(replica.on_request(requests[1]))
        self.assertEqual(len(replica._login_read_waiters), 2)
        self.assertEqual(
            replica.on_request(requests[0]),
            {"ok": False, "error": "AUTH_BUSY"},
        )

        read_probes = {
            message["read_context"]: message
            for destination, message in self.messages(replica)
            if destination == "n2" and message["kind"] == "append_entries"
            and message.get("read_context") is not None
        }
        self.assertEqual(len(read_probes), 2)
        for probe in read_probes.values():
            replica.on_message("n2", {
                "kind": "append_entries_response", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1, "reply_to": probe["rpc_id"], "success": True,
                "match_index": probe["prev_log_index"],
                "read_context": probe["read_context"],
            })
        self.assertEqual(len(verifier.tasks), 2)
        self.assertEqual(len(replica._password_checks), 2)

        verifier.complete(valid=True)
        verifier.complete(valid=True)
        replica.on_tick()

        expected_keys = {sha256_json([client_id, request_id]) for client_id in clients}
        self.assertEqual(set(replica._pending_login_proposals), expected_keys)
        self.assertEqual(
            {proposal.client_id for proposal in replica._pending_login_proposals.values()},
            set(clients),
        )

    def test_valid_login_waits_for_read_password_and_durable_issuance_commit(self) -> None:
        replica = self.app(mode="DEBUG")
        self.elect_and_commit_noop(replica)
        client_id = str(uuid4())
        request = self.request(client_id=client_id)
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)

        self.assertIsNone(replica.on_request(request))
        self.assertFalse(done.is_set())
        self.assertFalse(any(entry.command.operation == "issue_access_token"
                             for entry in replica.engine.state.raft.log))
        self.acknowledge_read(replica)

        for _ in range(200):
            threading.Event().wait(0.005)
            replica.on_tick()
            issuance = [message for destination, message in self.messages(replica)
                        if destination == "n2" and message["kind"] == "append_entries"
                        and any(entry["command"]["operation"] == "issue_access_token"
                                for entry in message["entries"])]
            if issuance:
                break
        else:
            self.fail("password verification did not produce an issuance proposal")
        self.assertFalse(done.is_set(), "the raw token must not be returned before commit")
        append = issuance[-1]
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })

        self.assertTrue(done.is_set())
        self.assertEqual(response["result"]["status"], "AUTHENTICATED")
        self.assertEqual(response["result"]["client_id"], client_id)
        self.assertEqual(response["result"]["credential_generation"], 1)
        token = response["result"]["access_token"]
        self.assertGreaterEqual(len(token), 40)
        UUID(response["result"]["credential_id"])
        durable = json.dumps(replica.disk["raft_state"])
        self.assertNotIn("Sofia-Classroom-1", durable)
        self.assertNotIn(token, durable)
        self.assertNotIn(token, json.dumps([
            entry.model_dump(mode="json") for entry in replica.engine.state.raft.log
        ]))
        self.assertNotIn(token, json.dumps({
            key: value.model_dump(mode="json")
            for key, value in replica.engine.state.request_results.items()
        }))
        events = (self.run_dir / "n1" / "events.log").read_text()
        self.assertNotIn("Sofia-Classroom-1", events)
        self.assertNotIn(token, events)
        self.assertEqual(replica._pending_login_proposals, {})

    def test_unknown_email_and_wrong_password_have_identical_password_gated_outcomes(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        responses = []
        for ordinal, (email, valid) in enumerate((
            ("unknown@example.test", True),
            ("sofia@example.test", False),
        )):
            request = self.request(f"invalid-{ordinal}", email=email)
            done, response = threading.Event(), {}
            replica._waiting[request.id] = (done, response)
            self.assertIsNone(replica.on_request(request))
            self.acknowledge_read(replica)
            self.assertEqual(len(verifier.tasks), 1, "both paths must pay one PBKDF2 cost")
            verifier.complete(valid=valid)
            replica.on_tick()
            self.assertTrue(done.is_set())
            self.assertFalse(any(entry.command.operation == "issue_access_token"
                                 for entry in replica.engine.state.raft.log))
            responses.append(response)
        self.assertEqual(responses[0], responses[1])
        self.assertEqual(responses[0], self._invalid_response())

    @staticmethod
    def _invalid_response() -> dict[str, object]:
        return {"ok": True, "result": {"status": "INVALID_CREDENTIALS"}}

    def test_step_down_discards_password_result_and_erases_pending_login_state(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("stale-password")
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        self.assertEqual(len(verifier.tasks), 1)

        last_index = len(replica.engine.state.raft.log)
        last_term = replica.engine.state.raft.log[-1].term
        replica.on_message("n2", {
            "kind": "append_entries", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 2, "prev_log_index": last_index, "prev_log_term": last_term,
            "entries": [], "leader_commit": replica.engine.state.raft.commit_index,
        })

        self.assertTrue(done.is_set())
        self.assertNotEqual(response.get("result", {}).get("status"), "AUTHENTICATED")
        self.assertEqual(replica._password_checks, {})
        self.assertEqual(replica._pending_login_proposals, {})
        verifier.complete(valid=True)
        replica.on_tick()
        self.assertFalse(any(entry.command.operation == "issue_access_token"
                             for entry in replica.engine.state.raft.log))

    def test_isolated_leader_times_out_before_password_or_dedup_disclosure(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("isolated")
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        self.assertIsNone(replica.on_request(request))
        waiter = replica._login_read_waiters[sha256_json([request.client, request.id])]
        replica.engine.pending_read_indices[waiter.read_context].deadline = replica.now() - 1

        replica.on_tick()

        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
        self.assertEqual(verifier.tasks, [])
        self.assertFalse(any(entry.command.operation == "issue_access_token"
                             for entry in replica.engine.state.raft.log))

    def test_internal_issuance_operation_is_not_publicly_reachable(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        response = replica.on_request(Request(
            "spoof-issuance", "issue_access_token", {
                "user_id": str(uuid4()), "client_id": str(uuid4()),
                "credential_id": str(uuid4()), "token_digest": "a" * 64,
            }, str(uuid4()),
        ))
        self.assertEqual(response["result"]["status"], "INVALID_REQUEST")

    def test_exact_committed_retry_without_volatile_secret_requires_new_login_id(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("lost-secret")
        first_done, first_response = threading.Event(), {}
        replica._waiting[request.id] = (first_done, first_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        replica.on_tick()
        append = next(message for destination, message in self.messages(replica)
                      if destination == "n2" and message["kind"] == "append_entries"
                      and any(entry["command"]["operation"] == "issue_access_token"
                              for entry in message["entries"]))
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self.assertTrue(first_done.is_set())

        retry_done, retry_response = threading.Event(), {}
        replica._waiting[request.id] = (retry_done, retry_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        with patch.object(replica, "_new_access_token_bytes", side_effect=AssertionError):
            replica.on_tick()

        self.assertTrue(retry_done.is_set())
        self.assertEqual(retry_response, {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"})
        self.assertEqual(
            sum(entry.command.operation == "issue_access_token"
                for entry in replica.engine.state.raft.log),
            1,
        )

    def test_inflight_non_login_request_identity_collision_is_request_id_reuse(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        client_id = str(uuid4())
        request = self.request("inflight-non-login", client_id=client_id)
        replica.engine.propose(CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=request.id,
            client_id=client_id,
            origin_node_uuid=replica.engine.node_uuid,
            operation="kv_put",
            payload={"key": "collision", "value": "uncommitted"},
        ))
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)

        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        replica.on_tick()

        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": True, "result": {"status": "REQUEST_ID_REUSE"}})

    def test_inflight_login_without_volatile_secret_requires_new_login_id(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        client_id = str(uuid4())
        request = self.request("inflight-login", client_id=client_id)
        user_id = replica._account_catalog.users[0].profile.user_id
        replica.engine.propose(CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=request.id,
            client_id=client_id,
            origin_node_uuid=replica.engine.node_uuid,
            operation="issue_access_token",
            payload={
                "user_id": str(user_id),
                "client_id": client_id,
                "credential_id": str(uuid4()),
                "token_digest": "a" * 64,
            },
        ))
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)

        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        replica.on_tick()

        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"})

    def test_uncommitted_login_proposal_expires_and_later_commit_cannot_restore_secret(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("proposal-timeout")
        first_done, first_response = threading.Event(), {}
        replica._waiting[request.id] = (first_done, first_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        replica.on_tick()
        append = next(message for destination, message in self.messages(replica)
                      if destination == "n2" and message["kind"] == "append_entries"
                      and any(entry["command"]["operation"] == "issue_access_token"
                              for entry in message["entries"]))
        pending = next(iter(replica._pending_login_proposals.values()))
        self.assertFalse(first_done.is_set())
        self.assertEqual(replica.on_request(request), {"ok": False, "error": "AUTH_BUSY"})

        with patch.object(replica, "now", return_value=pending.deadline):
            replica.on_tick()

        self.assertTrue(first_done.is_set())
        self.assertEqual(first_response, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
        self.assertEqual(replica._pending_login_proposals, {})
        self.assertNotEqual(first_response.get("result", {}).get("status"), "AUTHENTICATED")

        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        retry_done, retry_response = threading.Event(), {}
        replica._waiting[request.id] = (retry_done, retry_response)
        self.assertIsNone(replica.on_request(request), "timeout cleanup must remove AUTH_BUSY")
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        with patch.object(replica, "_new_access_token_bytes", side_effect=AssertionError):
            replica.on_tick()

        self.assertTrue(retry_done.is_set())
        self.assertEqual(retry_response, {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"})
        self.assertNotEqual(retry_response.get("result", {}).get("status"), "AUTHENTICATED")

    def test_persistence_failure_erases_login_secret_and_returns_only_safe_error(self) -> None:
        replica = self.app(mode="DEBUG")
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("persistence-failure", password="literal-password")
        first_done, first_response = threading.Event(), {}
        replica._waiting[request.id] = (first_done, first_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)

        with patch.object(replica.engine.storage, "save",
                          side_effect=OSError("literal-persistence-secret")):
            replica.on_tick()

        self.assertTrue(first_done.is_set())
        self.assertEqual(first_response, {"ok": False, "error": "AUTH_UNAVAILABLE"})
        self.assertEqual(replica._pending_login_proposals, {})
        self.assertNotEqual(first_response.get("result", {}).get("status"), "AUTHENTICATED")
        events = (self.run_dir / "n1" / "events.log").read_text()
        self.assertNotIn("literal-password", events)
        self.assertNotIn("literal-persistence-secret", events)

        retry_done, retry_response = threading.Event(), {}
        replica._waiting[request.id] = (retry_done, retry_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        with patch.object(replica, "_new_access_token_bytes", side_effect=AssertionError):
            replica.on_tick()

        self.assertTrue(retry_done.is_set())
        self.assertEqual(retry_response, {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"})

    def test_proposal_failure_before_append_forbids_new_secret_for_exact_retry(self) -> None:
        replica = self.app()
        self.elect_and_commit_noop(replica)
        verifier = self.install_controlled_verifier(replica)
        request = self.request("proposal-failure")
        first_done, first_response = threading.Event(), {}
        replica._waiting[request.id] = (first_done, first_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)

        with patch.object(replica.engine, "propose", side_effect=OSError("storage unavailable")):
            replica.on_tick()

        self.assertTrue(first_done.is_set())
        self.assertEqual(first_response, {"ok": False, "error": "AUTH_UNAVAILABLE"})
        self.assertEqual(replica._pending_login_proposals, {})

        retry_done, retry_response = threading.Event(), {}
        replica._waiting[request.id] = (retry_done, retry_response)
        self.assertIsNone(replica.on_request(request))
        self.acknowledge_read(replica)
        verifier.complete(valid=True)
        with patch.object(replica, "_new_access_token_bytes", side_effect=AssertionError):
            replica.on_tick()

        self.assertTrue(retry_done.is_set())
        self.assertEqual(retry_response, {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"})


if __name__ == "__main__":
    unittest.main()
