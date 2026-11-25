"""Acceptance checks for the Stage E.A normal scenario evidence boundary."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import cluster
from scenarios import run_stage_ea, stage_e_support


class StageEANormalEvidenceTests(unittest.TestCase):
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

    def test_latency_summary_separates_login_issuance_and_edits(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "history.jsonl"
            history.write_text("".join(json.dumps(record) + "\n" for record in [
                {"op": "login", "invoke": 1.0, "complete": 1.2, "ok": True, "result": {"status": "AUTHENTICATED"}},
                {"op": "publish_deck", "invoke": 2.0, "complete": 2.1, "ok": True},
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
            self.assertEqual(summary["login"]["p50_ms"], 200.0)


if __name__ == "__main__":
    unittest.main()
