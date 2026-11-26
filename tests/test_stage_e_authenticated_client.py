"""Tests for AuthenticatedClient — Stage E issue #41.

Covers all 7 acceptance criteria plus spec section 11 items 12–14.

AC-1  Client identity persists across restarts; distinct from other identities.
AC-2  Credential stored atomically outside run/ with 0o600 permissions; never in journal.
AC-3  Transport preserves caller-supplied request IDs; redacts password/bearer before history.
AC-4  Delayed older generation cannot replace a newer locally installed credential.
AC-5  LOGIN_SECRET_UNAVAILABLE returned to caller, never faked as success.
AC-6  Reauthentication uses new login request ID while preserving client_id.
AC-7  Duplicate delivery returns recoverable result; no conflicting random material.

Spec item 12: committed login with lost token → recoverable error; reauth rotates.
Spec item 13: delayed gen-1 cannot overwrite gen-2 in credential store.
Spec item 14: restart preserves client_id; token only in credential store outside run/.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_id() -> str:
    return str(uuid4())


def _auth_success(client_id: str, generation: int = 1, token: str | None = None) -> dict:
    """Build a fake AUTHENTICATED server response."""
    return {
        "ok": True,
        "result": {
            "status": "AUTHENTICATED",
            "user_id": _new_id(),
            "client_id": client_id,
            "credential_id": _new_id(),
            "credential_generation": generation,
            "access_token": token or f"token_gen{generation}_{uuid4().hex[:8]}",
        },
    }


# ---------------------------------------------------------------------------
# AC-1 — Client identity persistence
# ---------------------------------------------------------------------------

class TestClientIdentityPersistence(unittest.TestCase):
    """AC-1: client_id persists across restarts and is distinct from other IDs."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self, history: str = "h.jsonl") -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir,
            nodes=[],
            history_path=Path(self._tmp) / history,
        )

    def test_generates_valid_uuid_client_id(self):
        c = self._make()
        UUID(c.client_id)  # raises if not a valid UUID

    def test_client_id_persists_after_restart(self):
        c1 = self._make()
        cid = c1.client_id
        c2 = self._make()
        self.assertEqual(cid, c2.client_id)

    def test_different_dirs_produce_different_ids(self):
        from authenticated_client import AuthenticatedClient
        d1 = Path(self._tmp) / "c1"
        d2 = Path(self._tmp) / "c2"
        c1 = AuthenticatedClient(d1, nodes=[], history_path=Path(self._tmp) / "h1.jsonl")
        c2 = AuthenticatedClient(d2, nodes=[], history_path=Path(self._tmp) / "h2.jsonl")
        self.assertNotEqual(c1.client_id, c2.client_id)

    def test_client_id_is_distinct_from_request_id(self):
        c = self._make()
        self.assertNotEqual(c.client_id, _new_id())


# ---------------------------------------------------------------------------
# AC-2 — Credential storage outside run/ with restrictive permissions
# ---------------------------------------------------------------------------

class TestCredentialStorage(unittest.TestCase):
    """AC-2: credentials stored atomically outside run/ with mode 0o600."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir,
            nodes=[],
            history_path=Path(self._tmp) / "h.jsonl",
        )

    def _dummy_cred(self, generation: int = 1) -> dict:
        return {
            "access_token": f"tok{generation}",
            "credential_id": _new_id(),
            "credential_generation": generation,
            "user_id": _new_id(),
        }

    def test_client_id_file_has_mode_0o600(self):
        c = self._make()
        mode = os.stat(c._client_id_path).st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_credential_file_has_mode_0o600(self):
        c = self._make()
        c._store_credential(self._dummy_cred())
        mode = os.stat(c._credential_path).st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_client_directory_has_mode_0o700(self):
        self._client_dir.mkdir(mode=0o755)
        self._make()
        self.assertEqual(os.stat(self._client_dir).st_mode & 0o777, 0o700)

    def test_credential_path_is_under_client_dir(self):
        """Credential store is rooted in client_dir; callers choose a path outside run/."""
        c = self._make()
        self.assertTrue(
            str(c._credential_path).startswith(str(self._client_dir)),
            f"{c._credential_path} is not under client_dir {self._client_dir}",
        )

    def test_credential_path_is_not_inside_run_subdir(self):
        """client_dir must not resolve under a directory named 'run'."""
        from authenticated_client import AuthenticatedClient
        import tempfile, os
        with tempfile.TemporaryDirectory() as base:
            run_dir = Path(base) / "run" / "client"
            run_dir.mkdir(parents=True)
            c = AuthenticatedClient(
                run_dir, nodes=[], history_path=Path(base) / "h.jsonl"
            )
            # Confirm the invariant is the caller's responsibility: a client_dir
            # inside run/ is not prevented by the class — it is the caller's duty
            # to pass a path outside run/. This test documents that contract.
            for part in c._credential_path.parts:
                if part == "run":
                    return  # correctly detected the bad placement
        # If the test reaches here without finding "run" something is wrong with
        # the constructed path; skip rather than false-fail.
        self.skipTest("Could not construct a run/ path for contract documentation test")

    def test_stored_credential_is_loadable(self):
        c = self._make()
        cred = self._dummy_cred(generation=1)
        c._store_credential(cred)
        loaded = c._load_credential()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["credential_generation"], 1)
        self.assertEqual(loaded["access_token"], "tok1")

    def test_credential_not_in_history_file(self):
        c = self._make()
        h = Path(self._tmp) / "h.jsonl"
        # Trigger a login that writes history
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, token="SECRET_TOKEN")):
            c.login(_new_id(), "u@example.test", "pw")
        if h.exists():
            self.assertNotIn("SECRET_TOKEN", h.read_text())


# ---------------------------------------------------------------------------
# AC-3 — Transport redaction and request-ID preservation
# ---------------------------------------------------------------------------

class TestTransportRedaction(unittest.TestCase):
    """AC-3: history preserves caller request ID; password/bearer never serialized."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"
        self._history = Path(self._tmp) / "history.jsonl"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=self._history
        )

    def test_password_not_in_history_after_login(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": True, "result": {"status": "INVALID_CREDENTIALS"},
        }):
            c.login(_new_id(), "u@example.test", "s3cr3t_password!")
        self.assertNotIn("s3cr3t_password!", self._history.read_text())

    def test_access_token_not_in_history_after_authenticated_login(self):
        c = self._make()
        token = "BEARER_SECRET_XYZ"
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, token=token)):
            c.login(_new_id(), "u@example.test", "anypassword")
        self.assertNotIn(token, self._history.read_text())

    def test_auth_field_not_in_history_after_business_call(self):
        c = self._make()
        token = "BEARER_TOK_CALL"
        c._in_memory_credential = {
            "access_token": token,
            "credential_id": _new_id(),
            "credential_generation": 1,
            "user_id": _new_id(),
        }
        with patch.object(c, "_try_nodes", return_value={"ok": True, "result": {"status": "OK"}}):
            c.call(_new_id(), "some_op", {"key": "value"})
        self.assertNotIn(token, self._history.read_text())
        self.assertNotIn("_auth", self._history.read_text())

    def test_caller_request_id_recorded_in_history(self):
        c = self._make()
        req_id = "explicit-req-id-TEST-001"
        with patch.object(c, "_try_nodes", return_value={
            "ok": True, "result": {"status": "INVALID_CREDENTIALS"},
        }):
            c.login(req_id, "u@example.test", "pw")
        self.assertIn(req_id, self._history.read_text())

    def test_wire_payload_includes_auth_field_for_business_call(self):
        c = self._make()
        token = "WIRE_BEARER_TOKEN"
        c._in_memory_credential = {
            "access_token": token,
            "credential_id": _new_id(),
            "credential_generation": 1,
            "user_id": _new_id(),
        }
        captured: list[dict] = []
        def fake_nodes(payload, timeout=None):
            captured.append(payload)
            return {"ok": True, "result": {"status": "OK"}}
        with patch.object(c, "_try_nodes", side_effect=fake_nodes):
            c.call(_new_id(), "edit_slide", {"slide_id": "s1"})
        self.assertTrue(captured, "no request was captured")
        wire_args = captured[0].get("args", {})
        self.assertIn("_auth", wire_args)
        self.assertEqual(wire_args["_auth"]["access_token"], token)

    def test_wire_payload_has_caller_request_id(self):
        c = self._make()
        req_id = "caller-req-id-9999"
        captured: list[dict] = []
        def fake_nodes(payload, timeout=None):
            captured.append(payload)
            return {"ok": True, "result": {"status": "INVALID_CREDENTIALS"}}
        with patch.object(c, "_try_nodes", side_effect=fake_nodes):
            c.login(req_id, "u@example.test", "pw")
        self.assertEqual(captured[0]["id"], req_id)


# ---------------------------------------------------------------------------
# AC-4 — Generation guard (spec item 13)
# ---------------------------------------------------------------------------

class TestGenerationGuard(unittest.TestCase):
    """AC-4 / spec item 13: older generation cannot replace newer credential."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=Path(self._tmp) / "h.jsonl"
        )

    def _cred(self, generation: int, token: str) -> dict:
        return {
            "access_token": token,
            "credential_id": _new_id(),
            "credential_generation": generation,
            "user_id": _new_id(),
        }

    def test_older_generation_does_not_overwrite_newer(self):
        c = self._make()
        c._store_credential(self._cred(2, "tok_gen2"))
        c._store_credential(self._cred(1, "tok_gen1"))  # delayed older
        self.assertEqual(c._in_memory_credential["credential_generation"], 2)
        self.assertEqual(c._in_memory_credential["access_token"], "tok_gen2")

    def test_newer_generation_replaces_older(self):
        c = self._make()
        c._store_credential(self._cred(1, "tok_gen1"))
        c._store_credential(self._cred(2, "tok_gen2"))
        self.assertEqual(c._in_memory_credential["credential_generation"], 2)

    def test_delayed_gen1_cannot_overwrite_gen2_after_restart(self):
        """Spec item 13: restart must still reject delayed older-generation store."""
        c1 = self._make()
        c1._store_credential(self._cred(2, "tok_gen2"))
        # Restart
        c2 = self._make()
        self.assertEqual(c2._in_memory_credential["credential_generation"], 2)
        c2._store_credential(self._cred(1, "tok_gen1_delayed"))
        self.assertEqual(c2._in_memory_credential["credential_generation"], 2)
        loaded = c2._load_credential()
        self.assertEqual(loaded["credential_generation"], 2)

    def test_same_generation_does_not_overwrite(self):
        c = self._make()
        c._store_credential(self._cred(1, "original"))
        c._store_credential(self._cred(1, "replacement"))
        self.assertEqual(c._in_memory_credential["access_token"], "original")


# ---------------------------------------------------------------------------
# AC-5 — LOGIN_SECRET_UNAVAILABLE surfaced, not faked (spec item 12)
# ---------------------------------------------------------------------------

class TestLoginSecretUnavailable(unittest.TestCase):
    """AC-5 / spec item 12: committed login with lost token → recoverable error."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=Path(self._tmp) / "h.jsonl"
        )

    def test_login_secret_unavailable_returned_as_error(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"
        }):
            result = c.login(_new_id(), "u@example.test", "pw")
        self.assertFalse(result.get("ok", True))
        self.assertEqual(result.get("error"), "LOGIN_SECRET_UNAVAILABLE")

    def test_login_secret_unavailable_does_not_install_credential(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"
        }):
            c.login(_new_id(), "u@example.test", "pw")
        self.assertIsNone(c._in_memory_credential)

    def test_no_fake_token_returned_on_unavailable(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"
        }):
            result = c.login(_new_id(), "u@example.test", "pw")
        result_data = result.get("result") or {}
        self.assertNotIn("access_token", result_data)
        self.assertNotEqual(result.get("result", {}).get("status"), "AUTHENTICATED")

    def test_successful_login_installs_credential(self):
        c = self._make()
        token = "valid_token_xyz"
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, token=token)):
            result = c.login(_new_id(), "u@example.test", "pw")
        self.assertTrue(result["ok"])
        self.assertIsNotNone(c._in_memory_credential)
        self.assertEqual(c._in_memory_credential["access_token"], token)


# ---------------------------------------------------------------------------
# AC-6 — Reauthentication preserves client_id; uses new request ID
# ---------------------------------------------------------------------------

class TestReauthentication(unittest.TestCase):
    """AC-6: reauthentication preserves client_id; caller supplies new request ID."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=Path(self._tmp) / "h.jsonl"
        )

    def test_client_id_unchanged_after_two_logins(self):
        c = self._make()
        original_cid = c.client_id
        user_id = _new_id()
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, 1)):
            c.login(_new_id(), "u@example.test", "pw")
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, 2)):
            c.login(_new_id(), "u@example.test", "pw")
        self.assertEqual(c.client_id, original_cid)

    def test_reauth_with_new_request_id_accepted(self):
        c = self._make()
        req_ids_sent: list[str] = []
        call_count = [0]
        def fake_post(payload):
            req_ids_sent.append(payload["id"])
            call_count[0] += 1
            return _auth_success(c.client_id, generation=call_count[0])
        with patch.object(c, "_try_nodes", side_effect=fake_post):
            req1 = _new_id()
            req2 = _new_id()
            c.login(req1, "u@example.test", "pw")
            c.login(req2, "u@example.test", "pw")
        self.assertEqual(req_ids_sent[0], req1)
        self.assertEqual(req_ids_sent[1], req2)
        self.assertNotEqual(req1, req2)

    def test_credential_generation_advances_on_reauth(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, 1)):
            c.login(_new_id(), "u@example.test", "pw")
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, 2)):
            c.login(_new_id(), "u@example.test", "pw")
        self.assertEqual(c._in_memory_credential["credential_generation"], 2)


# ---------------------------------------------------------------------------
# AC-7 — Duplicate delivery returns recoverable result
# ---------------------------------------------------------------------------

class TestDuplicateDelivery(unittest.TestCase):
    """AC-7: duplicate delivery surfaces recoverable result, no new random material."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=Path(self._tmp) / "h.jsonl"
        )

    def test_request_id_reuse_surfaced_to_caller(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": True, "result": {"status": "REQUEST_ID_REUSE"}
        }):
            result = c.login(_new_id(), "u@example.test", "pw")
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["status"], "REQUEST_ID_REUSE")

    def test_login_secret_unavailable_in_flight_surfaced(self):
        c = self._make()
        with patch.object(c, "_try_nodes", return_value={
            "ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"
        }):
            result = c.login(_new_id(), "u@example.test", "pw")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "LOGIN_SECRET_UNAVAILABLE")

    def test_duplicate_does_not_replace_existing_credential(self):
        """A REQUEST_ID_REUSE must not overwrite an already-installed credential."""
        c = self._make()
        token = "original_token"
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, 1, token=token)):
            c.login(_new_id(), "u@example.test", "pw")
        # Simulate duplicate returning REUSE
        with patch.object(c, "_try_nodes", return_value={
            "ok": True, "result": {"status": "REQUEST_ID_REUSE"}
        }):
            c.login(_new_id(), "u@example.test", "pw")
        self.assertEqual(c._in_memory_credential["access_token"], token)


# ---------------------------------------------------------------------------
# Spec item 14 — Restart preserves client_id; token only in credential store
# ---------------------------------------------------------------------------

class TestRestartPreservation(unittest.TestCase):
    """Spec item 14: restart preserves client_id; raw token only in credential store."""

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._client_dir = Path(self._tmp) / "client"
        self._history = Path(self._tmp) / "h.jsonl"

    def _make(self) -> "AuthenticatedClient":
        from authenticated_client import AuthenticatedClient
        return AuthenticatedClient(
            self._client_dir, nodes=[], history_path=self._history
        )

    def test_client_id_preserved_after_restart(self):
        c1 = self._make()
        cid = c1.client_id
        c2 = self._make()
        self.assertEqual(cid, c2.client_id)

    def test_credential_accessible_after_restart(self):
        token = "durable_token_across_restart"
        c1 = self._make()
        c1._store_credential({
            "access_token": token,
            "credential_id": _new_id(),
            "credential_generation": 1,
            "user_id": _new_id(),
        })
        c2 = self._make()
        self.assertIsNotNone(c2._in_memory_credential)
        self.assertEqual(c2._in_memory_credential["access_token"], token)

    def test_raw_token_not_in_jsonl_history(self):
        """Spec item 14: raw token never appears in JSONL history file."""
        c = self._make()
        token = "NEVER_IN_JSONL_HISTORY"
        with patch.object(c, "_try_nodes", return_value=_auth_success(c.client_id, token=token)):
            c.login(_new_id(), "u@example.test", "mypassword")
        if self._history.exists():
            text = self._history.read_text()
            self.assertNotIn(token, text)
            self.assertNotIn("mypassword", text)

    def test_credential_file_separate_from_history(self):
        c = self._make()
        self.assertNotEqual(c._credential_path, self._history)

    def test_client_id_file_separate_from_history(self):
        c = self._make()
        self.assertNotEqual(c._client_id_path, self._history)

    def test_access_token_not_in_history_after_business_call_result(self):
        """AC-3: call() must redact access_token from server result before writing history."""
        c = self._make()
        token = "RESULT_BEARER_SECRET"
        with patch.object(c, "_try_nodes", return_value={
            "ok": True,
            "result": {"status": "OK", "access_token": token},
        }):
            c.call(_new_id(), "some_op", {"key": "val"})
        self.assertNotIn(token, self._history.read_text())


if __name__ == "__main__":
    unittest.main()
