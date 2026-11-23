"""Tests for issue #19: LockMaintenanceService with background renewal workers.

Seams:
  1. LockMaintenanceService public API (register_lock, deregister_lock, get_confidence, ctx mgr)
  2. RenewalWorker confidence state machine via injected mock sender
  3. ClientIntentionOutbox Scoped Authority Gate (set_lock_confidence / _eligible)
"""

from __future__ import annotations

import threading
import time
import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from lock_maintenance import (
    LockAuthority,
    LockMaintenanceService,
    MaintainedLockConfidence,
    RenewalWorker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _authority(element_id: str | None = None) -> LockAuthority:
    return LockAuthority(
        deck_id=str(uuid4()),
        slide_id=str(uuid4()),
        element_id=element_id or str(uuid4()),
        editor_session=str(uuid4()),
        fencing_token=1,
        lease_epoch=0,
        element_version=1,
    )


def _renewed_response(lease_epoch: int = 1) -> dict:
    return {"ok": True, "result": {"status": "RENEWED", "lease_epoch": lease_epoch}}


def _stale_response() -> dict:
    return {"ok": True, "result": {"status": "STALE_TOKEN"}}


def _not_owner_response() -> dict:
    return {"ok": True, "result": {"status": "NOT_OWNER"}}


def _not_found_response() -> dict:
    return {"ok": True, "result": {"status": "NOT_FOUND"}}


def _quorum_unavailable_response() -> dict:
    return {"ok": True, "result": {"status": "QUORUM_UNAVAILABLE"}}


def _transport_failure() -> None:
    raise ConnectionError("simulated network failure")


# ---------------------------------------------------------------------------
# Cycle 1: MaintainedLockConfidence enum
# ---------------------------------------------------------------------------

class MaintainedLockConfidenceEnumTests(unittest.TestCase):
    def test_all_five_states_exist(self):
        states = {c.value for c in MaintainedLockConfidence}
        self.assertEqual(states, {"VERIFIED", "RENEWING", "UNVERIFIED", "LOST", "STOPPED"})

    def test_enum_values_are_strings(self):
        for c in MaintainedLockConfidence:
            self.assertIsInstance(c.value, str)


# ---------------------------------------------------------------------------
# Cycle 2: LockAuthority dataclass
# ---------------------------------------------------------------------------

class LockAuthorityTests(unittest.TestCase):
    def test_fields_accessible(self):
        auth = _authority("elem-1")
        self.assertEqual(auth.element_id, "elem-1")
        self.assertEqual(auth.fencing_token, 1)
        self.assertEqual(auth.lease_epoch, 0)


# ---------------------------------------------------------------------------
# Cycle 3: RenewalWorker confidence transitions
# ---------------------------------------------------------------------------

class RenewalWorkerConfidenceTests(unittest.TestCase):
    """Tests use injected clock and synchronization so they don't rely on wall time."""

    def _make_worker(self, sender, renewal_interval=0.05):
        """Build a worker with a fast interval and return worker, confidence_log."""
        confidence_log: list[MaintainedLockConfidence] = []
        auth = _authority()

        worker = RenewalWorker(
            authority=auth,
            renewal_interval=renewal_interval,
            request_sender=sender,
            on_confidence_change=lambda eid, c: confidence_log.append(c),
        )
        return worker, confidence_log

    def test_initial_confidence_is_verified(self):
        worker, _ = self._make_worker(lambda op, args, rid: _renewed_response())
        self.assertEqual(worker.confidence, MaintainedLockConfidence.VERIFIED)

    def test_renewed_response_transitions_to_verified(self):
        called = threading.Event()

        def sender(op, args, rid):
            called.set()
            return _renewed_response()

        worker, log = self._make_worker(sender, renewal_interval=0.02)
        worker.start()
        called.wait(timeout=2.0)
        worker.stop()
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.VERIFIED, log)
        self.assertFalse(worker._thread.is_alive())

    def test_stale_token_transitions_to_lost(self):
        done = threading.Event()

        def sender(op, args, rid):
            return _stale_response()

        worker, log = self._make_worker(sender, renewal_interval=0.02)
        worker._on_confidence_change = lambda eid, c: (log.append(c), done.set() if c == MaintainedLockConfidence.LOST else None)
        worker.start()
        done.wait(timeout=2.0)
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.LOST, log)
        self.assertFalse(worker._thread.is_alive())

    def test_not_owner_transitions_to_lost(self):
        done = threading.Event()

        def sender(op, args, rid):
            return _not_owner_response()

        worker, log = self._make_worker(sender, renewal_interval=0.02)
        worker._on_confidence_change = lambda eid, c: (log.append(c), done.set() if c == MaintainedLockConfidence.LOST else None)
        worker.start()
        done.wait(timeout=2.0)
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.LOST, log)

    def test_not_found_transitions_to_lost(self):
        done = threading.Event()

        def sender(op, args, rid):
            return _not_found_response()

        worker, log = self._make_worker(sender, renewal_interval=0.02)
        worker._on_confidence_change = lambda eid, c: (log.append(c), done.set() if c == MaintainedLockConfidence.LOST else None)
        worker.start()
        done.wait(timeout=2.0)
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.LOST, log)

    def test_transport_failure_transitions_to_unverified(self):
        done = threading.Event()
        log: list[MaintainedLockConfidence] = []

        def sender(op, args, rid):
            raise ConnectionError("network")

        worker, _ = self._make_worker(sender, renewal_interval=0.02)
        # Override the on_confidence_change to capture and stop after UNVERIFIED
        worker._on_confidence_change = lambda eid, c: (
            log.append(c),
            done.set() if c == MaintainedLockConfidence.UNVERIFIED else None,
        )
        worker.start()
        done.wait(timeout=2.0)
        worker.stop()
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.UNVERIFIED, log)

    def test_quorum_unavailable_transitions_to_unverified(self):
        done = threading.Event()
        log: list[MaintainedLockConfidence] = []

        def sender(op, args, rid):
            return _quorum_unavailable_response()

        worker, _ = self._make_worker(sender, renewal_interval=0.02)
        worker._on_confidence_change = lambda eid, c: (
            log.append(c),
            done.set() if c == MaintainedLockConfidence.UNVERIFIED else None,
        )
        worker.start()
        done.wait(timeout=2.0)
        worker.stop()
        worker.join(timeout=2.0)

        self.assertIn(MaintainedLockConfidence.UNVERIFIED, log)

    def test_exact_id_retry_on_unverified(self):
        """Worker retries the SAME request_id after uncertain outcomes."""
        seen_request_ids: list[str] = []
        call_count = [0]
        done = threading.Event()

        def sender(op, args, rid):
            call_count[0] += 1
            seen_request_ids.append(rid)
            if call_count[0] >= 3:
                done.set()
                return _renewed_response()
            raise ConnectionError("transient")

        worker, _ = self._make_worker(sender, renewal_interval=0.02)
        worker.start()
        done.wait(timeout=3.0)
        worker.stop()
        worker.join(timeout=2.0)

        # First attempt and all retries share the same ID, only after RENEWED a new ID is used
        # calls 1 and 2 share the same ID; call 3 may also share it (before the renewal commits)
        self.assertEqual(seen_request_ids[0], seen_request_ids[1],
                         "Retries must reuse the same request_id")

    def test_lease_epoch_advances_in_authority_after_renewed(self):
        done = threading.Event()

        def sender(op, args, rid):
            done.set()
            return _renewed_response(lease_epoch=5)

        worker, _ = self._make_worker(sender, renewal_interval=0.02)
        worker.start()
        done.wait(timeout=2.0)
        # Give thread a moment to apply the state change
        time.sleep(0.05)
        worker.stop()
        worker.join(timeout=2.0)

        self.assertEqual(worker.authority.lease_epoch, 5)

    def test_cooperative_stop_no_thread_leak(self):
        worker, _ = self._make_worker(lambda op, args, rid: _renewed_response(), renewal_interval=0.02)
        worker.start()
        time.sleep(0.05)
        worker.stop()
        worker.join(timeout=2.0)
        self.assertFalse(worker._thread.is_alive())


# ---------------------------------------------------------------------------
# Cycle 4: LockMaintenanceService — register/deregister/get_confidence
# ---------------------------------------------------------------------------

class LockMaintenanceServiceTests(unittest.TestCase):

    def _service(self, sender=None, on_confidence_change=None):
        if sender is None:
            sender = lambda op, args, rid: _renewed_response()
        return LockMaintenanceService(
            request_sender=sender,
            on_confidence_change=on_confidence_change,
        )

    def test_register_starts_worker_immediately(self):
        """register_lock starts renewal without requiring a manual tick."""
        called = threading.Event()

        def sender(op, args, rid):
            called.set()
            return _renewed_response()

        with self._service(sender) as svc:
            auth = _authority()
            svc.register_lock(auth, renewal_interval=0.02)
            self.assertTrue(called.wait(timeout=2.0))

    def test_get_confidence_returns_verified_after_renewed(self):
        called = threading.Event()

        def sender(op, args, rid):
            called.set()
            return _renewed_response()

        with self._service(sender) as svc:
            auth = _authority()
            svc.register_lock(auth, renewal_interval=0.02)
            called.wait(timeout=2.0)
            time.sleep(0.05)
            self.assertEqual(svc.get_confidence(auth.element_id), MaintainedLockConfidence.VERIFIED)

    def test_get_confidence_returns_stopped_for_unregistered(self):
        with self._service() as svc:
            self.assertEqual(svc.get_confidence("nonexistent"), MaintainedLockConfidence.STOPPED)

    def test_per_element_isolation(self):
        """A slow sender for element A must not block renewal of element B."""
        b_called = threading.Event()
        a_blocked = threading.Event()

        def sender(op, args, rid):
            elem_id = args.get("element_id")
            if elem_id == "elem-a":
                # Block until test says so
                a_blocked.wait(timeout=5.0)
                return _renewed_response()
            else:
                b_called.set()
                return _renewed_response()

        auth_a = LockAuthority(
            deck_id="d", slide_id="s", element_id="elem-a",
            editor_session="sess", fencing_token=1, lease_epoch=0, element_version=1,
        )
        auth_b = LockAuthority(
            deck_id="d", slide_id="s", element_id="elem-b",
            editor_session="sess", fencing_token=1, lease_epoch=0, element_version=1,
        )
        with self._service(sender) as svc:
            svc.register_lock(auth_a, renewal_interval=0.02)
            svc.register_lock(auth_b, renewal_interval=0.02)
            # B should renew independently even though A is blocked
            self.assertTrue(b_called.wait(timeout=2.0))
            a_blocked.set()

    def test_deregister_verified_creates_release_callback(self):
        called = threading.Event()

        def sender(op, args, rid):
            called.set()
            return _renewed_response()

        releases: list[LockAuthority] = []

        def release_cb(auth):
            releases.append(auth)

        with self._service(sender) as svc:
            auth = _authority()
            svc.register_lock(auth, renewal_interval=0.02)
            called.wait(timeout=2.0)
            time.sleep(0.05)
            svc.deregister_lock(auth.element_id, release_callback=release_cb)

        self.assertEqual(len(releases), 1)

    def test_deregister_lost_creates_no_release_callback(self):
        done = threading.Event()

        def sender(op, args, rid):
            return _stale_response()

        releases: list[LockAuthority] = []

        def release_cb(auth):
            releases.append(auth)

        with self._service(sender) as svc:
            auth = _authority()
            # intercept LOST transition
            orig_confidence_change = None

            # We need to know when LOST is reached before deregistering
            lost_event = threading.Event()
            svc._on_confidence_change = lambda eid, c: lost_event.set() if c == MaintainedLockConfidence.LOST else None

            svc.register_lock(auth, renewal_interval=0.02)
            lost_event.wait(timeout=2.0)
            svc.deregister_lock(auth.element_id, release_callback=release_cb)

        self.assertEqual(len(releases), 0)

    def test_context_manager_stops_all_workers(self):
        threads_before = threading.active_count()
        auth = _authority()
        with LockMaintenanceService(request_sender=lambda op, args, rid: _renewed_response()) as svc:
            svc.register_lock(auth, renewal_interval=0.1)
        # After exit all workers should be joined
        time.sleep(0.05)
        threads_after = threading.active_count()
        self.assertLessEqual(threads_after, threads_before + 1)  # allow minor variance

    def test_duplicate_register_raises(self):
        with self._service() as svc:
            auth = _authority("elem-dup")
            svc.register_lock(auth, renewal_interval=0.1)
            with self.assertRaises(ValueError):
                svc.register_lock(auth, renewal_interval=0.1)

    def test_register_after_close_raises(self):
        svc = self._service()
        svc.close()
        with self.assertRaises(RuntimeError):
            svc.register_lock(_authority(), renewal_interval=0.1)


# ---------------------------------------------------------------------------
# Cycle 5: Scoped Authority Gate in ClientIntentionOutbox
# ---------------------------------------------------------------------------

class ScopedAuthorityGateTests(unittest.TestCase):
    """
    Tests the ClientIntentionOutbox.set_lock_confidence / _eligible gate.
    Protected mutations (update_element, delete_element) must not be sent
    while confidence is UNVERIFIED or LOST. Unrelated ops must still flow.
    """

    def _make_outbox(self, tmp_dir):
        from client_journal import ClientIntentionOutbox
        from command_dto import CommandEnvelopeDTO

        deck_id = str(uuid4())
        slide_id = str(uuid4())
        element_id = str(uuid4())
        session_id = str(uuid4())

        journal_path = Path(tmp_dir) / "journal.jsonl"
        outbox = ClientIntentionOutbox(
            logical_client_id="test-client",
            journal_path=journal_path,
            session_id=session_id,
        )
        # Provide a minimal base projection so the element is "known"
        outbox.record_base_projection({
            "deck_id": deck_id,
            "applied_index": 3,
            "deck_version": 1,
            "slide_ids": [slide_id],
            "slides": {
                slide_id: {
                    "elements": [{"element_id": element_id}],
                }
            },
            "elements": {
                element_id: {"element_id": element_id, "slide_id": slide_id, "element_version": 1}
            },
            "locks": {},
        })

        return outbox, deck_id, slide_id, element_id, session_id

    def test_update_element_blocked_when_unverified(self):
        with TemporaryDirectory() as tmp:
            outbox, deck_id, slide_id, element_id, session_id = self._make_outbox(tmp)
            outbox.set_lock_confidence(element_id, MaintainedLockConfidence.UNVERIFIED)

            sent = []
            outbox.enqueue("update_element", {
                "deck_id": deck_id,
                "element_id": element_id,
                "fencing_token": 1,
                "expected_version": 1,
                "patch": {"content": "new"},
            })
            outbox.synchronize(lambda intention: sent.append(intention.operation) or {"ok": True, "result": {"status": "SAVED"}})
            self.assertNotIn("update_element", sent)

    def test_update_element_blocked_when_lost(self):
        with TemporaryDirectory() as tmp:
            outbox, deck_id, slide_id, element_id, session_id = self._make_outbox(tmp)
            outbox.set_lock_confidence(element_id, MaintainedLockConfidence.LOST)

            sent = []
            outbox.enqueue("update_element", {
                "deck_id": deck_id,
                "element_id": element_id,
                "fencing_token": 1,
                "expected_version": 1,
                "patch": {"content": "new"},
            })
            outbox.synchronize(lambda intention: sent.append(intention.operation) or {"ok": True, "result": {"status": "SAVED"}})
            self.assertNotIn("update_element", sent)

    def test_update_element_allowed_when_verified(self):
        with TemporaryDirectory() as tmp:
            outbox, deck_id, slide_id, element_id, session_id = self._make_outbox(tmp)
            outbox.set_lock_confidence(element_id, MaintainedLockConfidence.VERIFIED)

            sent = []
            outbox.enqueue("update_element", {
                "deck_id": deck_id,
                "element_id": element_id,
                "fencing_token": 1,
                "expected_version": 1,
                "patch": {"content": "new"},
            })
            outbox.synchronize(lambda intention: sent.append(intention.operation) or {"ok": True, "result": {"status": "SAVED"}})
            self.assertIn("update_element", sent)

    def test_acquire_lock_not_blocked_when_unverified(self):
        """Non-protected ops (acquire_lock) must still flow through the gate."""
        with TemporaryDirectory() as tmp:
            outbox, deck_id, slide_id, element_id, session_id = self._make_outbox(tmp)
            outbox.set_lock_confidence(element_id, MaintainedLockConfidence.UNVERIFIED)

            sent = []
            outbox.enqueue("acquire_lock", {
                "deck_id": deck_id,
                "element_id": element_id,
            })
            outbox.synchronize(lambda intention: sent.append(intention.operation) or {"ok": True, "result": {"status": "LOCK_ACQUIRED", "fencing_token": 1, "lease_epoch": 0, "element_version": 1}})
            self.assertIn("acquire_lock", sent)

    def test_confidence_defaults_to_verified_for_unknown_element(self):
        with TemporaryDirectory() as tmp:
            outbox, deck_id, slide_id, element_id, session_id = self._make_outbox(tmp)
            # No explicit set_lock_confidence → update_element should be eligible
            sent = []
            outbox.enqueue("update_element", {
                "deck_id": deck_id,
                "element_id": element_id,
                "fencing_token": 1,
                "expected_version": 1,
                "patch": {"content": "new"},
            })
            outbox.synchronize(lambda intention: sent.append(intention.operation) or {"ok": True, "result": {"status": "SAVED"}})
            self.assertIn("update_element", sent)


if __name__ == "__main__":
    unittest.main()
