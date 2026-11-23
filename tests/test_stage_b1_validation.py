"""Acceptance contracts for Stage B.1 histories, evidence, and live scenarios."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import cluster
import spec


class StageBReferenceModelTests(unittest.TestCase):
    def test_presentation_observations_are_checker_visible_eventual_reads(self) -> None:
        for operation in ("get_deck_preview", "get_slide", "get_element", "read_lock_status"):
            with self.subTest(operation=operation):
                self.assertEqual(spec.GUARANTEE[operation], "eventual")
                self.assertIn(operation, spec.READS)

    def test_reference_model_reconstructs_final_element_content_version_and_lock(self) -> None:
        from scenarios.stage_b_support import published_deck, text_element

        deck_id, slide_id, element_id = (str(uuid4()) for _ in range(3))
        session_id = str(uuid4())
        state, _ = spec.apply(None, "publish_deck", published_deck(deck_id, [slide_id], name="Model"))
        state, _ = spec.apply(state, "create_element", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "session_id": session_id,
            "element": text_element(element_id, "Before", z_index=3),
        })
        state, _ = spec.apply(state, "update_element", {
            "deck_id": deck_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": 1,
            "expected_version": 1,
            "patch": {"content": "After", "x": 125},
        })

        _, observation = spec.apply(state, "get_element", {"deck_id": deck_id, "element_id": element_id})

        self.assertEqual(observation["status"], "OK")
        self.assertEqual(observation["element"]["configuration"]["content"], "After")
        self.assertEqual(observation["element"]["version"], 2)
        self.assertEqual(observation["lock"]["session_id"], session_id)

    def test_convergence_hook_ignores_only_replica_progress_metadata(self) -> None:
        from scenarios.stage_b_support import published_deck

        deck_id, slide_id = str(uuid4()), str(uuid4())
        arguments = published_deck(deck_id, [slide_id], name="Convergence")
        applied = [{"op": "publish_deck", "args": arguments}]
        final = {
            "status": "OK",
            "applied_index": 9,
            "deck_id": deck_id,
            "deck_version": 0,
            "slide": {
                "metadata": {"slide_id": slide_id, "background": "#112233", "ordinal": 0},
                "elements": [],
            },
        }

        self.assertTrue(spec.converged_ok(applied, final))
        self.assertFalse(spec.converged_ok(applied, {**final, "deck_version": 1}))
        self.assertFalse(spec.converged_ok(applied, {**final, "applied_index": -1}))

    def test_convergence_hook_respects_raft_order_for_overlapping_lock_attempts(self) -> None:
        from scenarios.stage_b_support import published_deck, text_element

        deck_id, slide_id, element_id = (str(uuid4()) for _ in range(3))
        creator, winner, loser = (str(uuid4()) for _ in range(3))
        publication = published_deck(deck_id, [slide_id], name="Concurrent convergence")
        create_arguments = {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "session_id": creator,
            "element": text_element(element_id, "Contended"),
        }
        release_arguments = {
            "deck_id": deck_id,
            "element_id": element_id,
            "session_id": creator,
            "fencing_token": 1,
        }
        winner_arguments = {"deck_id": deck_id, "element_id": element_id, "session_id": winner}
        loser_arguments = {"deck_id": deck_id, "element_id": element_id, "session_id": loser}
        state, published = spec.apply(None, "publish_deck", publication)
        state, created = spec.apply(state, "create_element", create_arguments)
        state, released = spec.apply(state, "release_lock", release_arguments)
        state, won = spec.apply(state, "acquire_lock", winner_arguments)
        state, lost = spec.apply(state, "acquire_lock", loser_arguments)
        _, expected = spec.apply(state, "get_element", {"deck_id": deck_id, "element_id": element_id})
        observed = {**expected, "applied_index": 9}
        applied = [
            {"op": "publish_deck", "args": publication, "invoke": 1.0, "complete": 2.0, "ok": True, "result": published},
            {"op": "create_element", "args": create_arguments, "invoke": 3.0, "complete": 4.0, "ok": True, "result": created},
            {"op": "release_lock", "args": release_arguments, "invoke": 5.0, "complete": 6.0, "ok": True, "result": released},
            # The loser invoked first, but the overlapping winner committed first.
            {"op": "acquire_lock", "args": loser_arguments, "invoke": 7.0, "complete": 9.0, "ok": True, "result": lost},
            {"op": "acquire_lock", "args": winner_arguments, "invoke": 7.1, "complete": 8.0, "ok": True, "result": won},
        ]

        self.assertTrue(spec.converged_ok(applied, observed))


class StageBHistoryClientTests(unittest.TestCase):
    def test_exact_request_id_is_sent_and_recorded_for_retry_evidence(self) -> None:
        from scenarios.stage_b_support import recorded_request

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            history = root / "history.jsonl"
            response = {"ok": True, "result": {"status": "PUBLISHED"}}

            with patch("scenarios.stage_b_support.cluster.load", return_value={
                "host_nodes": {"n1": "http://n1"},
            }), patch("scenarios.stage_b_support._post", return_value=response) as post:
                self.assertEqual(
                    recorded_request(
                        "logical-client",
                        history,
                        "stable-request-id",
                        "publish_deck",
                        {"presentation": {}},
                        node="n1",
                    ),
                    response,
                )

            self.assertEqual(post.call_args.args[1]["id"], "stable-request-id")
            recorded = json.loads(history.read_text(encoding="utf-8"))
            self.assertEqual(recorded["id"], "stable-request-id")


class StageBArchiveTests(unittest.TestCase):
    def test_archive_keeps_checker_manifest_metrics_journal_and_node_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            run = root / "run"
            project = root / "project"
            (run / "n1").mkdir(parents=True)
            project.mkdir()
            (run / "stage_b_history.jsonl").write_text("{}\n", encoding="utf-8")
            (run / "stage_b_client_journal.jsonl").write_text("{}\n", encoding="utf-8")
            (run / "stage_b_checker.jsonl").write_text("{}\n", encoding="utf-8")
            (run / "stage_b_metrics.jsonl").write_text("{}\n", encoding="utf-8")
            (run / "stage_b_manifest.jsonl").write_text("{}\n", encoding="utf-8")
            (run / "n1" / "roles.log").write_text("{}\n", encoding="utf-8")
            (run / "n1" / "events.log").write_text("{}\n", encoding="utf-8")
            original_run, original_here = cluster.RUN, cluster.HERE
            try:
                cluster.RUN, cluster.HERE = run, project
                archived = cluster.keep_histories()
            finally:
                cluster.RUN, cluster.HERE = original_run, original_here

            self.assertIsNotNone(archived)
            for filename in (
                "stage_b_history.jsonl",
                "stage_b_client_journal.jsonl",
                "stage_b_checker.jsonl",
                "stage_b_metrics.jsonl",
                "stage_b_manifest.jsonl",
            ):
                with self.subTest(filename=filename):
                    self.assertTrue((archived / filename).is_file())
            self.assertTrue((archived / "n1" / "roles.log").is_file())
            self.assertTrue((archived / "n1" / "events.log").is_file())


class StageBEvidenceTests(unittest.TestCase):
    def test_checker_projection_keeps_audit_history_but_excludes_unmodelled_identity_and_index_data(self) -> None:
        from scenarios.stage_b_support import write_checker_projection

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            raw = root / "raw.jsonl"
            projected = root / "checker-history.jsonl"
            exclusions = root / "projection.jsonl"
            records = [
                {"id": "publish", "op": "publish_deck", "result": {"status": "PUBLISHED"}},
                {"id": "publish", "op": "publish_deck", "result": {"status": "PUBLISHED"}},
                {"id": "conflict", "op": "update_element", "result": {
                    "status": "VERSION_CONFLICT",
                    "value": {"current_version": 2, "min_commit_index": 7},
                }},
                {"id": "refresh", "op": "authoritative_refresh_element", "result": {
                    "status": "OK", "applied_index": 8,
                }},
                {"id": "save", "op": "update_element", "result": {"status": "SAVED"}},
            ]
            raw.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )

            summary = write_checker_projection(raw, projected, exclusions)

            retained = [json.loads(line) for line in projected.read_text(encoding="utf-8").splitlines()]
            omitted = [json.loads(line) for line in exclusions.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([record["id"] for record in retained], ["publish", "save"])
            self.assertEqual(summary, {"source_records": 5, "checker_records": 2, "excluded_records": 3})
            self.assertEqual(
                {record["reason"] for record in omitted},
                {
                    "duplicate_request_identity",
                    "raft_assigned_conflict_index",
                    "raft_assigned_authoritative_read_index",
                },
            )
            self.assertEqual(
                [json.loads(line) for line in raw.read_text(encoding="utf-8").splitlines()],
                records,
                "building checker evidence must not rewrite the complete server audit",
            )

    def test_latency_summary_separates_quorum_edits_local_reads_and_unavailability(self) -> None:
        from scenarios.stage_b_support import write_latency_summary

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            history = root / "history.jsonl"
            records = [
                {"op": "update_element", "ok": True, "invoke": 1.0, "complete": 1.010},
                {"op": "update_element", "ok": True, "invoke": 2.0, "complete": 2.030},
                {"op": "get_element", "ok": True, "invoke": 3.0, "complete": 3.002},
                {"op": "get_element", "ok": True, "invoke": 4.0, "complete": 4.004},
            ]
            history.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
            output = root / "metrics.jsonl"

            write_latency_summary(
                history,
                output,
                edit_operations={"update_element"},
                read_operations={"get_element"},
                unavailability_seconds=1.25,
            )

            metrics = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(metrics["quorum_edit"]["count"], 2)
            self.assertEqual(metrics["quorum_edit"]["p50_ms"], 30.0)
            self.assertEqual(metrics["local_eventual_read"]["p95_ms"], 4.0)
            self.assertEqual(metrics["observed_unavailability_seconds"], 1.25)

    def test_checker_output_is_wrapped_as_archive_compatible_jsonl(self) -> None:
        from scenarios.stage_b_support import write_checker_evidence

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            history = root / "history.jsonl"
            history.write_text("", encoding="utf-8")
            output = root / "checker.jsonl"
            completed = SimpleNamespace(returncode=0, stdout="Resultado: sin violaciones\n", stderr="")

            with patch("scenarios.stage_b_support.subprocess.run", return_value=completed) as run:
                write_checker_evidence(history, output, run_dir=root)

            record = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(record["event"], "checker_result")
            self.assertEqual(record["returncode"], 0)
            self.assertIn("sin violaciones", record["stdout"])
            self.assertIn("check.py", run.call_args.args[0][1])

    def test_clean_cluster_suite_registers_every_stage_b1_live_scenario(self) -> None:
        from scenarios.run_stage_b1 import SCENARIOS

        self.assertEqual(SCENARIOS, (
            "stage_b_normal",
            "stage_b_concurrency",
            "stage_b_replica",
            "stage_b_lider",
            "stage_b_particion",
            "stage_b_client_journal",
        ))


if __name__ == "__main__":
    unittest.main()
