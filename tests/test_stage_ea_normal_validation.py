"""Acceptance checks for the Stage E.A normal scenario evidence boundary."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import cluster
from scenarios import run_stage_ea, stage_e_support


class StageEANormalEvidenceTests(unittest.TestCase):
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
            existing = root / "historiales" / "same-second"
            existing.mkdir(parents=True)
            (existing / "history.jsonl").write_text("old run\n")
            with patch.object(cluster, "HERE", root), patch.object(cluster, "RUN", run), \
                 patch.object(cluster.time, "strftime", return_value="same-second"):
                archived = cluster.keep_histories()
            self.assertNotEqual(archived, existing)
            self.assertEqual((existing / "history.jsonl").read_text(), "old run\n")
            self.assertEqual((archived / "history.jsonl").read_text(), "new run\n")

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

    def test_latency_summary_separates_login_issuance_and_edits(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "login", "invoke": 1.0, "complete": 1.2, "ok": True, "result": {"status": "AUTHENTICATED"}},
                {"op": "publish_deck", "invoke": 2.0, "complete": 2.1, "ok": True},
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

    def test_even_sample_latency_p50_uses_median(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "login", "invoke": 1.0, "complete": 1.1, "ok": True},
                {"op": "login", "invoke": 2.0, "complete": 2.2, "ok": True},
            ]))
            summary = stage_e_support.write_auth_latencies(history, root / "metrics.jsonl", events_dir=root)
            self.assertEqual(summary["login"], {
                "count": 2, "p50_ms": 150.0, "p95_ms": 200.0,
            })

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
