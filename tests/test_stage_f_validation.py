"""Stage F #61: deterministic contracts for the clean-cluster evidence boundary.

Live scenarios themselves run under the ``integration`` marker. These tests pin
the evidence helpers they rely on: replica convergence projections, the
committed access-order reference, checker projection, latency buckets,
secret/digest scanning and the clean-run runner.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dto_base import sha256_json
from scenarios import stage_f_support

OWNER = "11111111-1111-4111-8111-111111111111"
MATEO = "22222222-2222-4222-8222-222222222222"
DECK = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SLIDE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def _disk(root: Path, slot: str, *, grants: dict, access_version: int,
          last_applied: int = 9, digest: str = "d" * 64) -> None:
    (root / slot).mkdir(parents=True, exist_ok=True)
    (root / slot / "disk.json").write_text(json.dumps({"raft_state": {
        "raft": {"last_applied": last_applied, "commit_index": last_applied, "log": []},
        "request_results": {},
        "fsm": {
            "presentation_schema_policy": {"schema_version": 1, "v3_enforced_from_index": 3},
            "auth": {"active_credentials": {"client": {"token_digest": digest}}},
            "decks": {DECK: {
                "schema_version": 3, "deck_version": 0, "deck_settings_version": 1,
                "metadata": {"deck_id": DECK, "owner_user_id": OWNER},
                "access": {"schema_version": 1, "access_version": access_version, "grants": grants},
            }},
        },
    }}))


class AccessSummaryTests(unittest.TestCase):
    def test_converged_replicas_report_owner_grants_and_policy_without_digests(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for slot in ("n1", "n2", "n3"):
                _disk(root, slot, grants={MATEO: "VIEW"}, access_version=2)
            output = root / "access.jsonl"
            summaries = stage_f_support.write_access_summaries(root, output, [DECK])
            self.assertEqual(len(summaries), 3)
            self.assertEqual(summaries[0]["decks"][DECK], {
                "schema_version": 3, "owner_user_id": OWNER, "access_version": 2,
                "grants": [[MATEO, "VIEW"]], "deck_version": 0, "deck_settings_version": 1,
            })
            self.assertEqual(summaries[0]["v3_enforced_from_index"], 3)
            self.assertNotIn("d" * 64, output.read_text())

    def test_a_stale_follower_grant_roster_is_divergence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            _disk(root, "n1", grants={}, access_version=3)
            _disk(root, "n2", grants={}, access_version=3)
            _disk(root, "n3", grants={MATEO: "EDIT"}, access_version=2)
            with self.assertRaises(AssertionError):
                stage_f_support.write_access_summaries(root, root / "access.jsonl", [DECK])


def _committed(commands):
    """Build (entries, request_results) for a committed log of (client, op, payload, status)."""
    entries, results = [], {}
    for index, (client_id, operation, payload, status) in enumerate(commands, 1):
        request_id = f"r{index}"
        deck_id = None
        if operation == "publish_deck":
            deck_id = payload["presentation"]["metadata"]["deck_id"]
        elif operation != "activate_presentation_schema_v3":
            deck_id = DECK
        entries.append({"index": index, "command": {
            "client_id": client_id, "request_id": request_id, "deck_id": deck_id,
            "operation": operation, "payload": payload,
        }})
        results[sha256_json([client_id, request_id])] = {
            "committed_index": index, "result": {"status": status},
        }
    return entries, results


def _v3_publication():
    return {
        "presentation": {"schema_version": 3, "metadata": {
            "deck_id": DECK, "owner_user_id": OWNER, "name": "Ref",
        }, "slides": []},
        "authenticated_actor_user_id": OWNER,
    }


def _settings(actor):
    return {"expected_deck_settings_version": 0, "patch": {"name": "x"},
            "authenticated_actor_user_id": actor}


def _grant(level, version):
    return {"target_user_id": MATEO, "access_level": level, "expected_access_version": version,
            "authenticated_actor_user_id": OWNER}


def _revoke(version):
    return {"target_user_id": MATEO, "expected_access_version": version,
            "authenticated_actor_user_id": OWNER}


class CommittedAccessOrderReferenceTests(unittest.TestCase):
    def _log(self, final_edit_status="ACCESS_DENIED"):
        return _committed([
            ("reserved", "activate_presentation_schema_v3", {}, "PRESENTATION_SCHEMA_ACTIVATED"),
            ("owner", "publish_deck", _v3_publication(), "PUBLISHED"),
            ("mateo", "update_deck_settings", _settings(MATEO), "ACCESS_DENIED"),
            ("owner", "grant_access", _grant("EDIT", 0), "ACCESS_GRANTED"),
            ("owner", "grant_access", _grant("EDIT", 1), "ACCESS_UNCHANGED"),
            ("owner", "grant_access", _grant("VIEW", 0), "ACCESS_VERSION_CONFLICT"),
            ("mateo", "update_deck_settings", _settings(MATEO), "DECK_SETTINGS_UPDATED"),
            ("mateo", "grant_access", {**_grant("EDIT", 1), "authenticated_actor_user_id": MATEO},
             "ACCESS_DENIED"),
            ("owner", "revoke_access", _revoke(1), "ACCESS_REVOKED"),
            ("owner", "revoke_access", _revoke(2), "ACCESS_NOT_FOUND"),
            ("mateo", "update_deck_settings", _settings(MATEO), final_edit_status),
        ])

    def test_edit_before_and_after_revocation_follow_committed_order(self):
        import spec
        entries, results = self._log()
        reference = spec.check_committed_access_order(entries, results)
        by_index = {item["index"]: item for item in reference}
        self.assertEqual(by_index[7]["role"], "EDIT")
        self.assertEqual(by_index[7]["predicted_status"], "AUTHORIZED")
        self.assertEqual(by_index[11]["role"], "NONE")
        self.assertEqual(by_index[11]["predicted_status"], "ACCESS_DENIED")
        self.assertEqual(by_index[9]["access_version"], 2)

    def test_mutation_stored_as_authorized_after_committed_revoke_is_rejected(self):
        import spec
        entries, results = self._log(final_edit_status="DECK_SETTINGS_UPDATED")
        with self.assertRaisesRegex(AssertionError, "expected ACCESS_DENIED"):
            spec.check_committed_access_order(entries, results)


class CheckerProjectionTests(unittest.TestCase):
    def test_projection_stamps_actor_and_audits_what_the_reference_cannot_model(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history, checker, audit = root / "h.jsonl", root / "c.jsonl", root / "a.jsonl"
            records = [
                {"id": "login", "client": "c-owner", "op": "login", "args": {}, "ok": True,
                 "result": {"status": "AUTHENTICATED"}, "invoke": 1.0, "complete": 1.1},
                {"id": "read", "client": "c-mateo", "op": "query_grants", "args": {"deck_id": DECK},
                 "ok": True, "result": {"status": "OK", "effective_access": "VIEW", "deck_id": DECK,
                                        "access_version": 1, "applied_index": 7},
                 "invoke": 2.0, "complete": 2.1},
                {"id": "denied", "client": "c-mateo", "op": "update_deck_settings",
                 "args": {"deck_id": DECK}, "ok": True, "result": {"status": "ACCESS_DENIED"},
                 "invoke": 3.0, "complete": 3.1},
                {"id": "receipt", "client": "c-mateo", "op": "query_operation_receipt",
                 "args": {"original_request_id": "edit"}, "ok": True,
                 "result": {"status": "COMMITTED_SUCCESS"}, "invoke": 4.0, "complete": 4.1},
                {"id": "grant", "client": "c-owner", "op": "grant_access", "args": {"deck_id": DECK},
                 "ok": True, "result": {"status": "ACCESS_DENIED"}, "invoke": 5.0, "complete": 5.1},
            ]
            history.write_text("".join(json.dumps(record) + "\n" for record in records))
            stage_f_support.write_access_checker_projection(
                history, checker, audit, actors={"c-owner": OWNER, "c-mateo": MATEO},
            )
            projected = [json.loads(line) for line in checker.read_text().splitlines()]
            self.assertEqual([record["id"] for record in projected], ["read", "grant"])
            self.assertEqual(projected[0]["args"]["actor_user_id"], MATEO)
            self.assertNotIn("applied_index", projected[0]["result"])
            self.assertEqual(projected[1]["args"]["actor_user_id"], OWNER)
            reasons = {json.loads(line).get("request_id"): json.loads(line)["reason"]
                       for line in audit.read_text().splitlines()}
            self.assertEqual(reasons["denied"], "role_denial_checked_by_committed_access_reference")
            self.assertEqual(reasons["receipt"], "receipt_requires_retained_raft_log")
            self.assertEqual(reasons["login"], "authentication_admission_or_lifecycle")
            self.assertEqual(reasons["read"], "raft_assigned_read_index")

    def test_publication_boundary_rejections_are_audited_not_modelled(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history, checker, audit = root / "h.jsonl", root / "c.jsonl", root / "a.jsonl"
            history.write_text("".join(json.dumps({
                "id": request_id, "client": "c-owner", "op": "publish_deck",
                "args": {"deck_id": DECK}, "ok": True, "result": {"status": status},
                "invoke": 1.0, "complete": 1.1,
            }) + "\n" for request_id, status in (
                ("mismatch", "OWNER_MISMATCH"), ("v2", "UNSUPPORTED_PRESENTATION_SCHEMA"),
            )))
            stage_f_support.write_access_checker_projection(
                history, checker, audit, actors={"c-owner": OWNER},
            )
            self.assertEqual(checker.read_text(), "")
            self.assertEqual(audit.read_text().count("publication_boundary_checked_by_scenario"), 2)


class LatencyBucketTests(unittest.TestCase):
    def test_reads_denials_grants_and_edits_are_measured_in_separate_buckets(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            history = root / "h.jsonl"

            def record(op, status, duration, ok=True):
                return {"op": op, "invoke": 10.0, "complete": 10.0 + duration if ok else None,
                        "ok": ok, "result": {"status": status} if ok else None}
            history.write_text("".join(json.dumps(item) + "\n" for item in [
                record("login", "AUTHENTICATED", 0.5),
                record("get_deck_preview", "OK", 0.010),
                record("get_slide", "OK", 0.030),
                record("get_element", "ACCESS_DENIED", 0.020),
                record("query_grants", "OK", 0.005),
                record("query_operation_receipt", "COMMITTED_SUCCESS", 0.006),
                record("grant_access", "ACCESS_GRANTED", 0.040),
                record("revoke_access", "ACCESS_REVOKED", 0.050),
                record("grant_access", "ACCESS_VERSION_CONFLICT", 0.045),
                record("update_deck_settings", "DECK_SETTINGS_UPDATED", 0.060),
                record("update_deck_settings", "ACCESS_DENIED", 0.070),
                record("get_slide", None, 0, ok=False),
            ]))
            summary = stage_f_support.write_access_latencies(history, root / "m.jsonl")
            self.assertEqual(summary["protected_read"], {"count": 2, "p50_ms": 20.0, "p95_ms": 30.0})
            self.assertEqual(summary["protected_read_denied"]["count"], 1)
            self.assertEqual(summary["permission_query"]["count"], 1)
            self.assertEqual(summary["operation_receipt"]["count"], 1)
            self.assertEqual(summary["grant_change"]["count"], 2)
            self.assertEqual(summary["grant_rejected"]["count"], 1)
            self.assertEqual(summary["authorized_edit"]["count"], 1)
            self.assertEqual(summary["access_denied_mutation"]["count"], 1)
            self.assertEqual(summary["login"]["count"], 1)
            self.assertEqual(summary["unavailable"]["count"], 1)


class SecretScanTests(unittest.TestCase):
    def test_digest_may_live_in_raft_disk_but_never_in_public_projections(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            _disk(root, "n1", grants={}, access_version=0, digest="e" * 64)
            (root / "n1" / "events.log").write_text('{"event":"x"}\n')
            (root / "scenario.jsonl").write_text('{"ok":true}\n')
            self.assertEqual(stage_f_support.digests_in_raft_disks(root), {"e" * 64})
            self.assertEqual(stage_f_support.scan_public_projections(root, {"e" * 64}), [])
            (root / "scenario_access.jsonl").write_text(json.dumps({"token_digest": "e" * 64}) + "\n")
            self.assertEqual(stage_f_support.scan_public_projections(root, {"e" * 64}),
                             [root / "scenario_access.jsonl"])


class DurableReplicaAssertionTests(unittest.TestCase):
    """Stage F refuses follower-local deck reads, so older scenarios compare snapshots."""

    def _replicas(self, root: Path, *, n3_name: str = "Shared", n3_epoch: int = 2) -> dict:
        for slot in ("n1", "n2", "n3"):
            (root / slot).mkdir(parents=True)
            (root / slot / "disk.json").write_text(json.dumps({"raft_state": {"fsm": {"decks": {DECK: {
                "schema_version": 3, "deck_version": 1, "deck_settings_version": 2,
                "metadata": {"deck_id": DECK, "name": n3_name if slot == "n3" else "Shared",
                             "width_px": 1280, "height_px": 720, "owner_user_id": OWNER},
                "slides": [{"metadata": {"slide_id": SLIDE, "background": "#000000FF",
                                         "slide_settings_version": 1}, "elements": []}],
                "locks": {"element": {"session_id": "s", "fencing_token": 3,
                                      "lease_epoch": n3_epoch if slot == "n3" else 2}},
            }}}}}))
        return {slot: f"http://{slot}" for slot in ("n1", "n2", "n3")}

    def test_converged_snapshots_satisfy_the_unchanged_helper_contracts(self):
        from scenarios import stage_d_support
        with TemporaryDirectory() as directory:
            root = Path(directory)
            nodes = self._replicas(root)
            observed = stage_d_support.assert_all_replicas_deck_settings(
                nodes, DECK, expected_deck_settings_version=2, expected_name="Shared",
                expected_width_px=1280, expected_deck_version=1, run_dir=root)
            self.assertEqual(set(observed), {"n1", "n2", "n3"})
            stage_d_support.assert_all_replicas_slide_settings(
                nodes, DECK, SLIDE, expected_slide_settings_version=1,
                expected_background="#000000FF", expected_ordinal=0, run_dir=root)
            stage_d_support.assert_all_replicas_lock_state(
                nodes, DECK, "element", expected_epoch=2, expected_fencing=3, run_dir=root)

    def test_a_divergent_snapshot_fails_the_helper(self):
        from scenarios import stage_d_support
        with TemporaryDirectory() as directory:
            root = Path(directory)
            nodes = self._replicas(root, n3_name="Stale", n3_epoch=1)
            with self.assertRaisesRegex(AssertionError, "n3"):
                stage_d_support.assert_all_replicas_deck_settings(
                    nodes, DECK, expected_deck_settings_version=2, expected_name="Shared", run_dir=root)
            with self.assertRaisesRegex(AssertionError, "n3"):
                stage_d_support.assert_all_replicas_lock_state(
                    nodes, DECK, "element", expected_epoch=2, run_dir=root)


class StageERegressionRunnerTests(unittest.TestCase):
    def test_stage_e_runner_coordinates_voters_and_waits_for_the_v3_fence(self):
        from unittest.mock import patch
        from scenarios import run_stage_ea
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(run_stage_ea, "ROOT", root), \
                 patch.object(run_stage_ea, "archive_evidence", return_value=root), \
                 patch.object(run_stage_ea, "wait_for_activation", return_value=3) as fence, \
                 patch.object(run_stage_ea.subprocess, "run") as command:
                run_stage_ea.run_scenario("stage_e_normal")
            up, scenario = command.call_args_list[0], command.call_args_list[1]
            self.assertEqual(up.kwargs["env"]["STAGE_F_ALL_VOTERS_READY"], "1")
            self.assertEqual(scenario.kwargs["env"]["STAGE_F_ALL_VOTERS_READY"], "1")
            fence.assert_called_once_with(root / "run")


class RunnerTests(unittest.TestCase):
    def test_runner_refuses_existing_cluster_data(self):
        from unittest.mock import patch
        from scenarios import run_stage_f
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run").mkdir()
            with patch.object(run_stage_f, "ROOT", root), \
                 patch.object(run_stage_f.subprocess, "run") as command:
                with self.assertRaisesRegex(RuntimeError, "run/"):
                    run_stage_f.run_scenario("stage_f_sharing")
                command.assert_not_called()

    def test_coordinated_voters_activate_except_for_the_legacy_upgrade_scenario(self):
        from unittest.mock import patch
        from scenarios import run_stage_f
        for name, expected in (("stage_f_sharing", "1"), ("stage_f_legacy", None)):
            with TemporaryDirectory() as directory, self.subTest(name=name):
                root = Path(directory)
                with patch.object(run_stage_f, "ROOT", root), \
                     patch.object(run_stage_f, "archive_evidence", return_value=root), \
                     patch.object(run_stage_f, "prepare_legacy_binaries", return_value=root / "legacy"), \
                     patch.object(run_stage_f.subprocess, "run") as command:
                    run_stage_f.run_scenario(name)
                up = command.call_args_list[0]
                self.assertIn("up", up.args[0])
                self.assertEqual(up.kwargs["env"].get("STAGE_F_ALL_VOTERS_READY"), expected)
                self.assertIn("down", command.call_args_list[-1].args[0])


import pytest  # noqa: E402


@pytest.mark.integration
@pytest.mark.parametrize("scenario", [
    "stage_f_sharing", "stage_f_partition", "stage_f_failover", "stage_f_legacy",
])
def test_stage_f_live_scenario_passes_on_a_clean_cluster(scenario):
    """Start three local replicas, run one Stage F scenario, archive and stop."""
    import subprocess
    import sys
    root = Path(__file__).resolve().parent.parent
    completed = subprocess.run([sys.executable, str(root / "scenarios" / "run_stage_f.py"),
                                "--scenario", scenario], cwd=root, capture_output=True, text=True,
                               timeout=600)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert f"[{scenario}] PASSED" in completed.stdout


if __name__ == "__main__":
    unittest.main()
