"""Behavioral contracts shared by the executable Stage A.1 fault scenarios."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import spec


class FaultScenarioSupportTests(unittest.TestCase):
    def test_checker_models_local_get_as_eventual(self) -> None:
        self.assertEqual(spec.GUARANTEE["get"], "eventual")

    def test_wait_for_leader_uses_the_starter_status_seam(self) -> None:
        from scenarios.stage_a_support import wait_for_leader

        statuses = [
            {"n1": {"role": "follower", "term": 1}, "n2": {"role": "candidate", "term": 1}},
            {"n1": {"role": "follower", "term": 2}, "n2": {"role": "leader", "term": 2}},
        ]
        with patch("scenarios.stage_a_support.cluster.status", side_effect=statuses), patch(
            "scenarios.stage_a_support.time.sleep"
        ):
            self.assertEqual(wait_for_leader({"names": ["n1", "n2"]}, timeout=1), "n2")

    def test_minority_probe_rejects_only_an_acknowledged_write(self) -> None:
        from scenarios.stage_a_support import require_no_acknowledged_write

        require_no_acknowledged_write({"ok": False, "error": "QUORUM_UNAVAILABLE"})
        require_no_acknowledged_write({"redirect": "n2"})
        with self.assertRaisesRegex(RuntimeError, "minority"):
            require_no_acknowledged_write({"ok": True, "result": "ok"})

    def test_convergence_requires_equal_successful_final_reads(self) -> None:
        from scenarios.stage_a_support import require_converged

        self.assertEqual(
            require_converged({"n1": {"ok": True, "result": "v1"}, "n2": {"ok": True, "result": "v1"}}),
            "v1",
        )
        with self.assertRaisesRegex(RuntimeError, "did not converge"):
            require_converged({"n1": {"ok": True, "result": "v1"}, "n2": {"ok": True, "result": "v2"}})


if __name__ == "__main__":
    unittest.main()
