"""Acceptance checks for the Stage E.A normal scenario evidence boundary."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from authenticated_client import AuthenticatedClient
from scenarios import run_stage_ea, stage_e_support
from auth_catalog import load_account_catalog
from dto_base import sha256_json
from scenarios.stage_d_support import published_deck, stable_uuid
import spec


class StageEANormalEvidenceTests(unittest.TestCase):
    def test_reference_rejects_a_write_claimed_after_committed_revocation(self):
        user_id = str(load_account_catalog().users[0].profile.user_id)
        client_id = stable_uuid("stage-ea-reference-client")
        credential_id = stable_uuid("stage-ea-reference-credential")
        deck_id = stable_uuid("stage-ea-reference-deck")
        slide_id = stable_uuid("stage-ea-reference-slide")
        commands = [
            ("issue_access_token", "issue", {"user_id": user_id, "client_id": client_id,
                "credential_id": credential_id, "token_digest": "a" * 64}, "TOKEN_ISSUED"),
            ("publish_deck", "publish", {
                **published_deck(deck_id, [slide_id], name="Reference"),
                "authenticated_actor_user_id": user_id, "credential_id": credential_id,
                "credential_generation": 1}, "PUBLISHED"),
            ("revoke_access_token", "revoke", {"client_id": client_id,
                "authenticated_actor_user_id": user_id, "credential_id": credential_id,
                "credential_generation": 1, "expected_credential_id": credential_id,
                "expected_generation": 1}, "TOKEN_REVOKED"),
            ("update_deck_settings", "write", {
                "expected_deck_settings_version": 0, "patch": {"name": "Denied"},
                "authenticated_actor_user_id": user_id, "credential_id": credential_id,
                "credential_generation": 1}, "UNAUTHENTICATED"),
        ]
        entries = [{"index": index, "command": {"client_id": client_id,
                    "deck_id": deck_id if operation in {"publish_deck", "update_deck_settings"} else None,
                    "request_id": request_id, "operation": operation, "payload": payload}}
                   for index, (operation, request_id, payload, _) in enumerate(commands, 1)]
        results = {sha256_json([client_id, request_id]): {"committed_index": index,
                   "result": {"status": status}}
                   for index, (_, request_id, _, status) in enumerate(commands, 1)}
        reference = spec.check_committed_auth_order(entries, results)
        self.assertEqual(reference[-1]["predicted_status"], "UNAUTHENTICATED")
        self.assertEqual(reference[-1]["deck_settings_version"], 0)
        results[sha256_json([client_id, "write"])]["result"]["status"] = "DECK_SETTINGS_UPDATED"
        with self.assertRaisesRegex(AssertionError, "expected UNAUTHENTICATED"):
            spec.check_committed_auth_order(entries, results)

    def test_committed_login_response_can_be_lost_before_client_stores_token(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            client = AuthenticatedClient(root / "client", {"n1": "http://n1"}, root / "history.jsonl")
            observed = []
            response = {"ok": True, "result": {
                "status": "AUTHENTICATED", "client_id": client.client_id,
                "user_id": "user-id", "credential_id": "credential-id",
                "credential_generation": 1, "access_token": "lost-bearer",
            }}
            with patch.object(client, "_raw_post", return_value=response):
                outcome, lost_secret = stage_e_support.lose_committed_login_response(
                    client, "login-id", "sofia@example.test", "test-password",
                    lambda credential_id, generation, digest: observed.append((credential_id, generation, digest)),
                )
            self.assertEqual(outcome, {"ok": False, "error": "ConnectionError"})
            self.assertEqual(lost_secret, "lost-bearer")
            self.assertEqual(observed[0][:2], ("credential-id", 1))
            self.assertFalse((root / "client" / "credential.json").exists())
            self.assertNotIn("lost-bearer", (root / "history.jsonl").read_text())

    def test_bearer_probe_records_identity_and_result_without_the_secret(self):
        with TemporaryDirectory() as directory:
            history = Path(directory) / "history.jsonl"
            class Response:
                def __enter__(self):
                    return self
                def __exit__(self, *_):
                    return None
                def read(self):
                    return b'{"ok":false,"error":"REAUTH_REQUIRED"}'
            with patch.object(stage_e_support.urllib.request, "urlopen", return_value=Response()) as post:
                response = stage_e_support.recorded_bearer_probe(
                    "http://127.0.0.1:8001", history, "client-uuid", "old-token",
                    "old-credential-denied", "update_element", {"deck_id": "deck"},
                )
            self.assertEqual(response["error"], "REAUTH_REQUIRED")
            request = json.loads(post.call_args.args[0].data)
            self.assertEqual(request["id"], "old-credential-denied")
            self.assertEqual(request["client"], "client-uuid")
            self.assertEqual(request["args"]["_auth"]["access_token"], "old-token")
            record = json.loads(history.read_text())
            self.assertEqual(record["id"], "old-credential-denied")
            self.assertNotIn("old-token", history.read_text())

    def test_runner_refuses_existing_cluster_data(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run").mkdir()
            with patch.object(run_stage_ea, "ROOT", root), patch.object(run_stage_ea.subprocess, "run") as command:
                with self.assertRaisesRegex(RuntimeError, "run/"):
                    run_stage_ea.run_scenario("stage_e_normal")
                command.assert_not_called()

    def test_archive_does_not_overwrite_a_run_from_the_same_second(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run"
            run.mkdir()
            (run / "history.jsonl").write_text("new run\n")
            (run / "n1").mkdir()
            (run / "n1" / "disk.json").write_text('{"raft_state":{"raft":{"log":[]}}}')
            existing = root / "historiales" / "same-second-stage_e_normal"
            existing.mkdir(parents=True)
            (existing / "history.jsonl").write_text("old run\n")
            with patch.object(run_stage_ea, "ROOT", root), \
                 patch.object(run_stage_ea, "datetime") as clock:
                clock.now.return_value.strftime.return_value = "same-second"
                archived = run_stage_ea.archive_evidence("stage_e_normal")
            self.assertNotEqual(archived, existing)
            self.assertEqual((existing / "history.jsonl").read_text(), "old run\n")
            self.assertEqual((archived / "history.jsonl").read_text(), "new run\n")
            self.assertEqual((archived / "n1" / "disk.json").read_text(),
                             (run / "n1" / "disk.json").read_text())

    def test_projection_keeps_business_ids_and_excludes_secretless_login(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "raw.jsonl"
            projected = root / "checker.jsonl"
            audit = root / "audit.jsonl"
            records = [
                {"id": "login-1", "client": "stable-uuid", "op": "login", "args": {"email": "sofia@example.test"}, "ok": True, "result": {"status": "AUTHENTICATED"}},
                {"id": "edit-1", "client": "stable-uuid", "op": "update_element", "args": {"deck_id": "deck"}, "ok": True, "result": {"status": "UPDATED"}},
            ]
            history.write_text("".join(json.dumps(record) + "\n" for record in records))
            stage_e_support.write_auth_checker_projection(history, projected, audit)
            self.assertEqual([json.loads(line)["id"] for line in projected.read_text().splitlines()], ["edit-1"])
            self.assertIn("login-1", audit.read_text())

    def test_projection_excludes_revocation_and_explicit_auth_denials(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "raw.jsonl"
            checker = root / "checker.jsonl"
            audit = root / "audit.jsonl"
            records = [
                {"id": "publish", "client": "client", "op": "publish_deck", "args": {}, "ok": True, "result": {"status": "PUBLISHED"}},
                {"id": "revoke", "client": "client", "op": "revoke_access_token", "args": {"expected_generation": 2}, "ok": True, "result": {"status": "TOKEN_REVOKED"}},
                {"id": "denied", "client": "client", "op": "update_element", "args": {}, "ok": False, "error": "REAUTH_REQUIRED"},
            ]
            history.write_text("".join(json.dumps(record) + "\n" for record in records))
            stage_e_support.write_auth_checker_projection(history, checker, audit)
            self.assertEqual([json.loads(line)["id"] for line in checker.read_text().splitlines()], ["publish"])
            self.assertIn("revoke", audit.read_text())
            self.assertIn("denied", audit.read_text())

    def test_projection_audits_committed_auth_denial_outside_presentation_model(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "raw.jsonl"
            checker = root / "checker.jsonl"
            audit = root / "audit.jsonl"
            history.write_text(json.dumps({
                "id": "revoked-write", "client": "client", "op": "update_deck_settings",
                "args": {"deck_id": "deck"}, "ok": True,
                "result": {"status": "UNAUTHENTICATED"},
            }) + "\n")
            stage_e_support.write_auth_checker_projection(history, checker, audit)
            self.assertEqual(checker.read_text(), "")
            self.assertIn("revoked-write", audit.read_text())
            self.assertIn("UNAUTHENTICATED", audit.read_text())

    def test_request_result_evidence_detects_replica_divergence(self):
        from dto_base import sha256_json
        with TemporaryDirectory() as directory:
            root = Path(directory)
            key = sha256_json(["client", "write-id"])
            for slot in ("n1", "n2", "n3"):
                (root / slot).mkdir()
                (root / slot / "disk.json").write_text(json.dumps({"raft_state": {
                    "request_results": {key: {"committed_index": 4,
                        "result": {"status": "UNAUTHENTICATED" if slot != "n3" else "SAVED"}}},
                }}))
            with self.assertRaises(AssertionError):
                stage_e_support.write_request_result_summaries(
                    root, root / "results.jsonl", [("client", "write-id")],
                )

    def test_latency_summary_separates_login_issuance_and_edits(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "login", "invoke": 1.0, "complete": 1.2, "ok": True, "result": {"status": "AUTHENTICATED"}},
                {"op": "publish_deck", "invoke": 2.0, "complete": 2.1, "ok": True,
                 "result": {"status": "PUBLISHED"}},
                {"op": "revoke_access_token", "invoke": 3.0, "complete": 3.05, "ok": True, "result": {"status": "TOKEN_REVOKED"}},
            ]))
            (root / "n1").mkdir()
            (root / "n1" / "events.log").write_text("".join(json.dumps(record) + "\n" for record in [
                {"event": "login_issuance_proposed", "request_id": "login-1", "timestamp": 1.1},
                {"event": "login_completed", "request_id": "login-1", "timestamp": 1.19},
            ]))
            summary = stage_e_support.write_auth_latencies(history, root / "metrics.jsonl", events_dir=root)
            self.assertEqual(summary["login"]["count"], 1)
            self.assertEqual(summary["token_issue"]["count"], 1)
            self.assertEqual(summary["authenticated_edit"]["count"], 1)
            self.assertEqual(summary["token_revoke"]["count"], 1)
            self.assertEqual(summary["login"]["p50_ms"], 200.0)
            comparison = summary["stage_d_comparison"]
            self.assertIsNone(comparison["login"]["stage_d"])
            self.assertIsNone(comparison["token_issue"]["stage_d"])
            self.assertIsNone(comparison["token_revoke"]["stage_d"])
            self.assertEqual(comparison["authenticated_edit"]["stage_d"]["count"], 9)

    def test_even_sample_latency_p50_uses_median(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "login", "invoke": 1.0, "complete": 1.1, "ok": True,
                 "result": {"status": "AUTHENTICATED"}},
                {"op": "login", "invoke": 2.0, "complete": 2.2, "ok": True,
                 "result": {"status": "AUTHENTICATED"}},
            ]))
            summary = stage_e_support.write_auth_latencies(history, root / "metrics.jsonl", events_dir=root)
            self.assertEqual(summary["login"], {
                "count": 2, "p50_ms": 150.0, "p95_ms": 200.0,
            })

    def test_committed_auth_denial_is_not_counted_as_successful_edit_latency(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "update_deck_settings", "invoke": 1.0, "complete": 1.1,
                 "ok": True, "result": {"status": "DECK_SETTINGS_UPDATED"}},
                {"op": "update_deck_settings", "invoke": 2.0, "complete": 2.5,
                 "ok": True, "result": {"status": "UNAUTHENTICATED"}},
            ]))
            summary = stage_e_support.write_auth_latencies(history, root / "metrics.jsonl", events_dir=root)
            self.assertEqual(summary["authenticated_edit"]["count"], 1)
            self.assertEqual(summary["committed_auth_denial"]["count"], 1)

    def test_durable_issue_evidence_requires_committed_applied_entry_on_both_voters(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for slot in ("n1", "n3"):
                (root / slot).mkdir()
                (root / slot / "disk.json").write_text(json.dumps({"raft_state": {
                    "raft": {"log": [{"index": 1, "command": {
                        "operation": "issue_access_token", "request_id": "login-id",
                        "client_id": "client-id", "payload": {"credential_id": "credential-id", "token_digest": "digest"},
                    }}], "commit_index": 1, "last_applied": 1},
                    "fsm": {"auth": {"active_credentials": {"client-id": {
                        "credential_id": "credential-id", "credential_generation": 1,
                        "token_digest": "digest", "revoked": False,
                    }}}},
                }}))
            proof = stage_e_support.durable_issuance_evidence(
                root, ("n1", "n3"), "client-id", "login-id", "credential-id",
            )
            self.assertEqual({entry["slot"] for entry in proof}, {"n1", "n3"})
            self.assertTrue(all(entry["committed_index"] == 1 for entry in proof))
            disk = root / "n3" / "disk.json"
            data = json.loads(disk.read_text())
            data["raft_state"]["raft"]["commit_index"] = 0
            data["raft_state"]["raft"]["last_applied"] = 0
            disk.write_text(json.dumps(data))
            with self.assertRaisesRegex(AssertionError, "not committed"):
                stage_e_support.durable_issuance_evidence(
                    root, ("n1", "n3"), "client-id", "login-id", "credential-id",
                )

    def test_final_auth_summaries_detect_client_binding_divergence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for slot in ("n1", "n2", "n3"):
                (root / slot).mkdir()
                (root / slot / "disk.json").write_text(json.dumps({"raft_state": {
                    "raft": {"last_applied": 3},
                    "fsm": {"auth": {
                        "schema_version": 2, "catalog_fingerprint": "catalog",
                        "client_bindings": {"client-id": "user-a" if slot != "n3" else "user-b"},
                        "active_credentials": {"client-id": {
                            "user_id": "user-a", "credential_id": "credential-id",
                            "credential_generation": 1, "token_digest": "digest", "revoked": False,
                        }},
                    }},
                }}))
            with self.assertRaises(AssertionError):
                stage_e_support.write_auth_summaries(root, root / "summaries.jsonl", "client-id")


if __name__ == "__main__":
    unittest.main()
