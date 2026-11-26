"""Authentication behavior at the durable intention and transport boundaries."""

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from authenticated_client import AuthenticatedClient
from client_journal import ClientIntentionOutbox, JournaledClient, Lifecycle, LocalIntentionRejected
from lock_maintenance import LockAuthority, MaintainedLockConfidence, RenewalWorker


class JournalAuthenticationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "intentions.jsonl"
        self.outbox = ClientIntentionOutbox("client-1", self.path, "session-1")
        self.outbox.record_base_projection({"deck_id": "deck-1", "applied_index": 1})

    def test_nested_authentication_fields_cannot_enter_journal(self):
        for secret in ("_auth", "password", "access_token", "token_digest"):
            with self.subTest(secret=secret), self.assertRaises(LocalIntentionRejected):
                self.outbox.enqueue("update_deck_settings", {
                    "deck_id": "deck-1", "expected_deck_settings_version": 0,
                    "patch": {"name": "Safe", "nested": [{secret: "SECRET"}]},
                })
        self.assertNotIn("SECRET", self.path.read_text() if self.path.exists() else "")

    def test_reauthentication_retries_original_request_after_restart(self):
        self.outbox.enqueue("update_deck_settings", {
            "deck_id": "deck-1", "expected_deck_settings_version": 0,
            "patch": {"name": "A"},
        }, request_id="original")
        self.outbox.synchronize(lambda _: {"ok": False, "error": "REAUTH_REQUIRED"})
        restarted = ClientIntentionOutbox("client-1", self.path, "session-1")
        sent = []
        restarted.synchronize(lambda intent: sent.append(intent.request_id) or {
            "ok": True, "result": {"status": "DECK_SETTINGS_UPDATED", "value": {"deck_settings_version": 1}},
        })
        self.assertEqual(sent, ["original"])
        self.assertEqual(restarted.get("original").lifecycle, Lifecycle.COMMITTED)

    def test_denial_is_terminal_and_next_settings_uses_last_success(self):
        for request_id, version in (("success", 0), ("denied", 1)):
            self.outbox.enqueue("update_deck_settings", {
                "deck_id": "deck-1", "expected_deck_settings_version": version,
                "patch": {"name": request_id},
            }, request_id=request_id)
        observed = []
        def send(intent):
            observed.append((intent.request_id, intent.arguments["expected_deck_settings_version"]))
            if intent.request_id == "success":
                return {"ok": True, "result": {"status": "DECK_SETTINGS_UPDATED", "value": {"deck_settings_version": 1}}}
            if intent.request_id == "denied":
                return {"ok": True, "result": {"status": "UNAUTHENTICATED"}}
            return {"ok": True, "result": {"status": "DECK_SETTINGS_UPDATED", "value": {"deck_settings_version": 2}}}
        self.outbox.synchronize(send)
        self.outbox.enqueue("update_deck_settings", {
            "deck_id": "deck-1", "expected_deck_settings_version": 2,
            "patch": {"name": "later"},
        }, request_id="later")
        self.outbox.synchronize(send)
        self.assertEqual(observed, [("success", 0), ("denied", 1), ("later", 1)])
        restarted = ClientIntentionOutbox("client-1", self.path, "session-1")
        self.assertEqual(restarted.get("denied").lifecycle, Lifecycle.REJECTED_FINAL)
        self.assertEqual(restarted.get("later").arguments["expected_deck_settings_version"], 1)

    def test_transport_uses_current_credential_on_same_id_retry(self):
        transport = AuthenticatedClient(Path(self.temp.name) / "client", [], Path(self.temp.name) / "history.jsonl")
        captured = []
        def post(payload, timeout=None):
            captured.append(payload)
            return {"ok": False, "error": "REAUTH_REQUIRED"} if len(captured) == 1 else {"ok": True, "result": {"status": "DECK_SETTINGS_UPDATED"}}
        with patch.object(transport, "_try_nodes", side_effect=post):
            transport._in_memory_credential = {"access_token": "old"}
            transport.call("same-id", "update_deck_settings", {"deck_id": "deck-1"})
            transport._in_memory_credential = {"access_token": "new"}
            transport.call("same-id", "update_deck_settings", {"deck_id": "deck-1"})
        self.assertEqual([item["id"] for item in captured], ["same-id", "same-id"])
        self.assertEqual([item["args"]["_auth"]["access_token"] for item in captured], ["old", "new"])
        self.assertNotIn("old", transport._history_path.read_text())
        self.assertNotIn("new", transport._history_path.read_text())

    def test_journal_and_lock_senders_share_authenticated_transport(self):
        transport = AuthenticatedClient(Path(self.temp.name) / "client", {"leader": "http://leader"},
                                        Path(self.temp.name) / "history.jsonl")
        transport._in_memory_credential = {"access_token": "secret", "user_id": "user-1"}
        captured = []
        def post(url, payload, timeout=None):
            captured.append((url, payload, timeout))
            return {"ok": True, "result": {"status": "DECK_SETTINGS_UPDATED", "value": {"deck_settings_version": 1}}}
        journaled = JournaledClient(transport.client_id, self.path, "session-1",
                                    request_sender=transport.journal_sender,
                                    local_replica="leader", leader_replica="leader",
                                    user_id="user-1", deck_id="deck-1")
        journaled.outbox.record_base_projection({"deck_id": "deck-1", "applied_index": 1})
        journaled.outbox.observe_permission({"status": "OK", "effective_access": "EDIT",
            "access_version": 1, "applied_index": 1}, epoch=journaled.outbox.begin_permission_observation())
        journaled.enqueue("update_deck_settings", {"deck_id": "deck-1",
            "expected_deck_settings_version": 0, "patch": {"name": "A"}}, request_id="intent-1")
        with patch.object(transport, "_raw_post", side_effect=post):
            journaled.synchronize()
            transport.renewal_sender("renew_lock", {"session_id": "session-1"}, "renew-1", 2.5)
        self.assertEqual([call[1]["id"] for call in captured], ["intent-1", "renew-1"])
        self.assertEqual(captured[0][0], "http://leader")
        self.assertEqual(captured[1][2], 2.5)
        self.assertTrue(all(call[1]["args"]["_auth"]["access_token"] == "secret" for call in captured))
        self.assertNotIn("secret", self.path.read_text())
        self.assertNotIn("secret", transport._history_path.read_text())

    def test_journal_rejects_transport_for_different_client_identity(self):
        transport = AuthenticatedClient(Path(self.temp.name) / "other-client", [],
                                        Path(self.temp.name) / "history.jsonl")
        with self.assertRaises(ValueError):
            JournaledClient("different-client-id", self.path, "session-1",
                            request_sender=transport.journal_sender,
                            local_replica="leader", leader_replica="leader",
                            user_id="user-1", deck_id="deck-1")

    def test_history_redacts_reflected_secrets_across_entire_entry(self):
        transport = AuthenticatedClient(Path(self.temp.name) / "client", [],
                                        Path(self.temp.name) / "history.jsonl")
        password = "distinct-password-secret"
        token = "distinct-token-secret"
        with patch.object(transport, "_try_nodes", return_value={
            "ok": False, "error": f"bad password {password}",
            "result": {"nested": [{"message": f"password was {password}"}]},
        }):
            transport.login("login-1", "u@example.test", password)
        transport._in_memory_credential = {"access_token": token}
        with patch.object(transport, "_try_nodes", return_value={
            "ok": False, "error": f"invalid token {token}",
            "result": {"detail": f"reflected {token}", "token_digest": "digest-secret"},
        }):
            transport.call("edit-1", "update_deck_settings", {"deck_id": "deck-1"})
        with patch.object(transport, "_try_nodes", side_effect=RuntimeError(f"failed with {token}")):
            transport.call("edit-2", "update_deck_settings", {"deck_id": "deck-1"})
        history = transport._history_path.read_text()
        for secret in (password, token, "digest-secret"):
            self.assertNotIn(secret, history)
        self.assertEqual(len([json.loads(line) for line in history.splitlines()]), 3)

    def test_committed_renewal_denial_uses_fresh_id_and_reauth_required_retries_it(self):
        seen = []
        finished = threading.Event()
        responses = [
            {"ok": True, "result": {"status": "UNAUTHENTICATED"}},
            {"ok": False, "error": "REAUTH_REQUIRED"},
            {"ok": True, "result": {"status": "RENEWED", "lease_epoch": 1}},
        ]
        def send(op, args, request_id, timeout):
            seen.append(request_id)
            if len(seen) == 3:
                finished.set()
            return responses[len(seen) - 1]
        authority = LockAuthority("deck-1", "slide-1", "element-1", "session-1", 1, 0, 1)
        worker = RenewalWorker(authority, 0.01, send)
        worker.start()
        self.assertTrue(finished.wait(2.0))
        worker.stop()
        worker.join()
        self.assertNotEqual(seen[0], seen[1])
        self.assertEqual(seen[1], seen[2])
        self.assertNotEqual(worker.confidence, MaintainedLockConfidence.LOST)


if __name__ == "__main__":
    unittest.main()
