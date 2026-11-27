"""Stage C.1 acceptance contracts for reference model and integration scenarios.

Unit tests run without a cluster. Integration tests (marked @pytest.mark.integration)
start real local clusters and require the Stage C.1 environment to be available.
Skip integration tests with: pytest -m "not integration"
"""

from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

import pytest


# ---------------------------------------------------------------------------
# Reference-model unit tests (no cluster required)
# ---------------------------------------------------------------------------

class StageCReferenceModelTests(unittest.TestCase):
    """Verify the spec.py reference model understands renew_lock and expire_lock."""

    def _deck_with_locked_element(self) -> tuple:
        """Return (state, deck_id, slide_id, element_id, session_id) with a locked element."""
        import spec
        from scenarios.stage_b_support import published_deck, text_element

        deck_id, slide_id, element_id = (str(uuid4()) for _ in range(3))
        session_id = str(uuid4())
        state, _ = spec.apply(None, "publish_deck", published_deck(deck_id, [slide_id], name="C1 test"))
        state, _ = spec.apply(state, "create_element", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "session_id": session_id,
            "element": text_element(element_id, "initial"),
        })
        return state, deck_id, slide_id, element_id, session_id

    def test_renew_lock_advances_epoch_in_reference_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        state, result = spec.apply(state, "renew_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": 1,
        })
        self.assertEqual(result["status"], "RENEWED")
        self.assertEqual(result["value"]["lease_epoch"], 1)

    def test_renew_lock_does_not_change_element_version_in_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        _, read_before = spec.apply(state, "get_element", {"deck_id": deck_id, "element_id": element_id})
        version_before = read_before["element"]["version"]

        state, _ = spec.apply(state, "renew_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": 1,
        })
        _, read_after = spec.apply(state, "get_element", {"deck_id": deck_id, "element_id": element_id})
        self.assertEqual(read_after["element"]["version"], version_before)

    def test_renew_lock_stale_token_in_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        _, result = spec.apply(state, "renew_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": 999,
        })
        self.assertEqual(result["status"], "STALE_TOKEN")

    def test_expire_lock_removes_lock_in_reference_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        state, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "expected_token": 1,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(result["status"], "EXPIRED")

        _, lock_obs = spec.apply(state, "read_lock_status", {"deck_id": deck_id, "element_id": element_id})
        self.assertIsNone(lock_obs.get("lock"))

    def test_expire_lock_retains_fencing_counter_in_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        # Expire the lock
        state, _ = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "expected_token": 1,
            "expected_lease_epoch": 0,
        })

        # New acquisition should get fencing_token = 2 (counter retained)
        session2 = str(uuid4())
        _, result2 = spec.apply(state, "acquire_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "session_id": session2,
        })
        self.assertEqual(result2["status"], "LOCK_ACQUIRED")
        self.assertGreater(result2["value"]["fencing_token"], 1)

    def test_expire_lock_stale_epoch_is_noop_in_model(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()

        # Renew once so epoch=1
        state, _ = spec.apply(state, "renew_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": 1,
        })

        # Try to expire with stale epoch=0
        _, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "element_id": element_id,
            "expected_token": 1,
            "expected_lease_epoch": 0,  # stale — current is 1
        })
        self.assertEqual(result["status"], "NO_OP_STALE_EXPIRY")

    def test_renew_lock_is_in_spec_guarantee(self) -> None:
        import spec
        self.assertIn("renew_lock", spec.GUARANTEE)

    def test_acquire_lock_returns_complete_lock_authority(self) -> None:
        import spec
        state, deck_id, slide_id, element_id, session_id = self._deck_with_locked_element()
        # create_element already gave us a lock; re-read it via acquire on a fresh element
        element_id2 = str(uuid4())
        from scenarios.stage_b_support import text_element
        state, result = spec.apply(state, "create_element", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "session_id": session_id,
            "element": text_element(element_id2, "second"),
        })
        self.assertEqual(result["status"], "CREATED_AND_LOCKED")
        self.assertIn("fencing_token", result["value"])
        self.assertIn("lease_epoch", result["value"])
        self.assertIn("element_version", result["value"])


# ---------------------------------------------------------------------------
# Integration scenario tests (require a running cluster)
# ---------------------------------------------------------------------------

def _run_live_stage_c_scenario(testcase: unittest.TestCase, name: str) -> None:
    """Start a clean cluster exactly as ``run_stage_c1`` does, run one scenario, stop.

    Uses the runner's environment (Stage C.1 timing plus the Stage F all-voters
    flag) and waits for the v3 activation fence before the scenario publishes.
    """
    import os
    import subprocess
    import sys
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root / "scenarios"))
    from cluster_support import wait_for_activation
    from run_stage_c1 import _STAGE_C1_ENV

    env = {**os.environ, **_STAGE_C1_ENV}
    subprocess.run(
        [sys.executable, str(root / "cluster.py"), "up", "--local"],
        cwd=root, check=True, env=env,
    )
    try:
        wait_for_activation(root / "run")
        result = subprocess.run(
            [sys.executable, str(root / "scenarios" / f"{name}.py")],
            cwd=root, env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            testcase.fail(f"{name} failed:\n{result.stdout}\n{result.stderr}")
    finally:
        subprocess.run(
            [sys.executable, str(root / "cluster.py"), "down"],
            cwd=root, check=True,
        )


@pytest.mark.integration
class StageCNormalScenarioTest(unittest.TestCase):
    """Integration test: Stage C.1 normal renewal scenario (issue #22)."""

    def test_stage_c_normal_scenario_passes(self) -> None:
        """Run the normal renewal scenario against a live cluster."""
        _run_live_stage_c_scenario(self, "stage_c_normal")


@pytest.mark.integration
class StageCReplicaScenarioTest(unittest.TestCase):
    """Integration test: Stage C.1 follower catch-up scenario (issue #22)."""

    def test_stage_c_replica_scenario_passes(self) -> None:
        """Run the follower catch-up scenario against a live cluster."""
        _run_live_stage_c_scenario(self, "stage_c_replica")


@pytest.mark.integration
class StageCParticionScenarioTest(unittest.TestCase):
    """Integration test: Stage C.1 partition/heal scenario (issue #22)."""

    def test_stage_c_particion_scenario_passes(self) -> None:
        """Run the partition/heal scenario against a live cluster."""
        _run_live_stage_c_scenario(self, "stage_c_particion")


@pytest.mark.integration
class StageCMayoriaLossScenarioTest(unittest.TestCase):
    """Integration test: Stage C.1 majority-loss/recovery scenario (issue #22)."""

    def test_stage_c_mayoria_loss_scenario_passes(self) -> None:
        """Run the majority-loss/recovery scenario against a live cluster."""
        _run_live_stage_c_scenario(self, "stage_c_mayoria_loss")
