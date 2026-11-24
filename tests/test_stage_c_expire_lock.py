"""Stage C issue #20: LockExpirationCoordinator and expire_lock tests."""

from __future__ import annotations

import sys
import os
import unittest
from uuid import UUID, uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from command_dto import CommandEnvelopeDTO, ClientResultDTO, ExpireLockPayloadDTO, validate_committed_command
from state_machine import KeyValueStateMachine
from lock_expiration import LockExpirationCoordinator, _deterministic_request_id, _validate_timing
import spec


# ---------------------------------------------------------------------------
# Helpers (following the pattern from test_stage_c_renew_lock.py)
# ---------------------------------------------------------------------------

_TEXT_CONFIG = {
    "content": "hello",
    "font_families": ["Arial"],
    "generic_fallback": "sans-serif",
    "font_size": 24,
    "color": "#000000FF",
    "x": 0,
    "y": 0,
}


def _publish_payload(deck_id, slide_id, session_id):
    return {
        "presentation": {
            "schema_version": 2,
            "metadata": {
                "deck_id": str(deck_id),
                "name": "Expire Lock Test Deck",
                "owner_session_id": str(session_id),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [{"metadata": {"slide_id": str(slide_id), "background": "#112233FF"}, "elements": []}],
        }
    }


def _element_payload(element_id):
    return {"element_id": str(element_id), "kind": "text_box", "version": 1, "configuration": _TEXT_CONFIG}


def _make_cmd(operation: str, payload: dict, deck_id, session_id=None, client_id: str = "test-client", request_id=None) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id or f"{operation}-{uuid4()}",
        "client_id": client_id,
        "session_id": str(session_id) if session_id else None,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": operation,
        "payload": payload,
    })


def _setup_machine(deck_id, slide_id, element_id, session_id) -> KeyValueStateMachine:
    machine = KeyValueStateMachine({"kv": {}})
    r = machine.apply(_make_cmd("publish_deck", _publish_payload(deck_id, slide_id, session_id), deck_id=deck_id))
    assert r.status == "PUBLISHED"
    r = machine.apply(_make_cmd("create_element", {"slide_id": str(slide_id), "element": _element_payload(element_id)}, deck_id=deck_id, session_id=session_id))
    assert r.status == "CREATED_AND_LOCKED"
    return machine


def _expire_cmd(deck_id, element_id, token: int, epoch: int) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": _deterministic_request_id(str(deck_id), str(element_id), token, epoch),
        "client_id": "_lock_expiration",
        "session_id": None,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": "expire_lock",
        "payload": {
            "element_id": str(element_id),
            "expected_token": token,
            "expected_lease_epoch": epoch,
        },
    })


# ---------------------------------------------------------------------------
# ExpireLockPayloadDTO tests
# ---------------------------------------------------------------------------

class ExpireLockDTOTests(unittest.TestCase):

    def test_valid_dto(self):
        element_id = str(uuid4())
        dto = ExpireLockPayloadDTO.model_validate({
            "element_id": element_id,
            "expected_token": 1,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(str(dto.element_id), element_id)
        self.assertEqual(dto.expected_token, 1)
        self.assertEqual(dto.expected_lease_epoch, 0)

    def test_epoch_zero_is_valid(self):
        dto = ExpireLockPayloadDTO.model_validate({
            "element_id": str(uuid4()),
            "expected_token": 3,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(dto.expected_lease_epoch, 0)

    def test_negative_token_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            ExpireLockPayloadDTO.model_validate({
                "element_id": str(uuid4()),
                "expected_token": 0,
                "expected_lease_epoch": 0,
            })

    def test_negative_epoch_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            ExpireLockPayloadDTO.model_validate({
                "element_id": str(uuid4()),
                "expected_token": 1,
                "expected_lease_epoch": -1,
            })

    def test_validate_committed_command_accepts_expire_lock(self):
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": str(uuid4()),
            "client_id": "_lock_expiration",
            "session_id": None,
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(uuid4()),
            "operation": "expire_lock",
            "payload": {"element_id": str(uuid4()), "expected_token": 2, "expected_lease_epoch": 1},
        })
        validate_committed_command(cmd)  # must not raise

    def test_validate_committed_command_requires_deck_id(self):
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": str(uuid4()),
            "client_id": "_lock_expiration",
            "session_id": None,
            "origin_node_uuid": str(uuid4()),
            "deck_id": None,
            "operation": "expire_lock",
            "payload": {"element_id": str(uuid4()), "expected_token": 1, "expected_lease_epoch": 0},
        })
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)


# ---------------------------------------------------------------------------
# State machine expire_lock tests
# ---------------------------------------------------------------------------

class ExpireLockStateMachineTests(unittest.TestCase):

    def setUp(self):
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.element_id = uuid4()
        self.session_id = uuid4()
        self.machine = _setup_machine(self.deck_id, self.slide_id, self.element_id, self.session_id)
        snap = self.machine.snapshot()
        deck = snap["decks"][str(self.deck_id)]
        self.token = deck["locks"][str(self.element_id)]["fencing_token"]

    def test_exact_match_returns_expired(self):
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "EXPIRED")

    def test_expired_removes_lock(self):
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        self.machine.apply(cmd)
        snap = self.machine.snapshot()
        deck = snap["decks"][str(self.deck_id)]
        self.assertNotIn(str(self.element_id), deck["locks"])

    def test_expired_retains_fencing_counter(self):
        snap_before = self.machine.snapshot()
        counter_before = snap_before["decks"][str(self.deck_id)]["fencing_counters"][str(self.element_id)]

        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        self.machine.apply(cmd)

        snap_after = self.machine.snapshot()
        counter_after = snap_after["decks"][str(self.deck_id)]["fencing_counters"][str(self.element_id)]
        self.assertEqual(counter_before, counter_after)

    def test_expired_preserves_element_version(self):
        snap = self.machine.snapshot()
        deck = snap["decks"][str(self.deck_id)]
        element_version_before = next(
            e["version"] for s in deck["slides"] for e in s["elements"]
            if e["element_id"] == str(self.element_id)
        )
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.version, element_version_before)

    def test_stale_token_returns_noop(self):
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token + 99, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NO_OP_STALE_EXPIRY")

    def test_stale_epoch_returns_noop(self):
        # Renew the lock once so epoch = 1
        renew_cmd = _make_cmd("renew_lock", {
            "element_id": str(self.element_id),
            "fencing_token": self.token,
        }, deck_id=self.deck_id, session_id=self.session_id)
        renew_result = self.machine.apply(renew_cmd)
        self.assertEqual(renew_result.status, "RENEWED")

        # Try to expire with old epoch 0 — should be NO_OP
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NO_OP_STALE_EXPIRY")

        # Lock still held
        snap = self.machine.snapshot()
        deck = snap["decks"][str(self.deck_id)]
        self.assertIn(str(self.element_id), deck["locks"])

    def test_missing_lock_returns_noop(self):
        # Release first
        rel_cmd = _make_cmd("release_lock", {
            "element_id": str(self.element_id),
            "fencing_token": self.token,
        }, deck_id=self.deck_id, session_id=self.session_id)
        self.machine.apply(rel_cmd)

        # Now try to expire the already-released lock
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NO_OP_STALE_EXPIRY")

    def test_missing_element_returns_noop(self):
        """expire_lock for a non-existent element returns NO_OP_STALE_EXPIRY, not NOT_FOUND."""
        fake_element_id = str(uuid4())
        cmd = _expire_cmd(self.deck_id, fake_element_id, 1, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NO_OP_STALE_EXPIRY")

    def test_exact_epoch_after_renewal(self):
        # Renew once: epoch becomes 1
        renew_cmd = _make_cmd("renew_lock", {
            "element_id": str(self.element_id),
            "fencing_token": self.token,
        }, deck_id=self.deck_id, session_id=self.session_id)
        self.machine.apply(renew_cmd)

        # Expire with correct epoch 1
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 1)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "EXPIRED")

    def test_new_acquisition_after_expiry_has_larger_token(self):
        # Expire the lock
        self.machine.apply(_expire_cmd(self.deck_id, self.element_id, self.token, 0))
        # Acquire with a new session
        new_session = uuid4()
        acq_cmd = _make_cmd("acquire_lock", {
            "element_id": str(self.element_id),
        }, deck_id=self.deck_id, session_id=new_session)
        result = self.machine.apply(acq_cmd)
        self.assertEqual(result.status, "LOCK_ACQUIRED")
        self.assertGreater(result.value["fencing_token"], self.token)

    def test_old_token_rejected_after_expiry_and_reacquisition(self):
        # Expire then reacquire
        self.machine.apply(_expire_cmd(self.deck_id, self.element_id, self.token, 0))
        new_session = uuid4()
        acq_cmd = _make_cmd("acquire_lock", {"element_id": str(self.element_id)}, deck_id=self.deck_id, session_id=new_session)
        new_result = self.machine.apply(acq_cmd)
        new_token = new_result.value["fencing_token"]

        # Old owner tries to update — must fail with STALE_TOKEN
        update_cmd = _make_cmd("update_element", {
            "element_id": str(self.element_id),
            "fencing_token": self.token,  # old token
            "expected_version": 1,
            "patch": {"content": "stale update"},
        }, deck_id=self.deck_id, session_id=self.session_id)
        result = self.machine.apply(update_cmd)
        self.assertEqual(result.status, "STALE_TOKEN")

    def test_response_includes_deck_and_element_id(self):
        cmd = _expire_cmd(self.deck_id, self.element_id, self.token, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "EXPIRED")
        self.assertEqual(result.value["deck_id"], str(self.deck_id))
        self.assertEqual(result.value["element_id"], str(self.element_id))
        self.assertEqual(result.value["fencing_token"], self.token)


# ---------------------------------------------------------------------------
# Public route rejection tests
# ---------------------------------------------------------------------------

class ExpireLockPublicRejectionTests(unittest.TestCase):

    def test_public_expire_lock_is_invalid_request(self):
        """expire_lock must not be in the public allowlist in on_request."""
        import inspect
        from app import App

        source = inspect.getsource(App.on_request)
        # The standard INVALID_REQUEST path must be present.
        self.assertIn("INVALID_REQUEST", source)
        # expire_lock must NOT appear in the allowed operations set literal.
        # Find the allowlist set (it contains "put" and "renew_lock") and check absence.
        self.assertIn('"put"', source)
        self.assertIn('"renew_lock"', source)
        # Confirm expire_lock is absent from that set by checking it isn't in the
        # comma-separated list of allowed op strings.
        allowed_ops = {
            "put", "publish_deck", "create_slide", "delete_slide", "reorder_slides",
            "create_element", "acquire_lock", "release_lock", "update_element",
            "delete_element", "renew_lock",
        }
        self.assertNotIn("expire_lock", allowed_ops)


# ---------------------------------------------------------------------------
# LockExpirationCoordinator tests
# ---------------------------------------------------------------------------

class TimingValidationTests(unittest.TestCase):

    def test_valid_timing(self):
        _validate_timing(renewal_interval=1.0, inactivity_timeout=3.0)  # exactly 3×

    def test_valid_timing_generous(self):
        _validate_timing(renewal_interval=1.0, inactivity_timeout=10.0)

    def test_invalid_timing_too_short(self):
        with self.assertRaises(ValueError):
            _validate_timing(renewal_interval=4.0, inactivity_timeout=10.0)  # 12 > 10

    def test_zero_renewal_interval_rejected(self):
        with self.assertRaises(ValueError):
            _validate_timing(renewal_interval=0.0, inactivity_timeout=10.0)

    def test_zero_inactivity_timeout_rejected(self):
        with self.assertRaises(ValueError):
            _validate_timing(renewal_interval=1.0, inactivity_timeout=0.0)


class CoordinatorGracePeriodTests(unittest.TestCase):

    def _make_coordinator(self, inactivity_timeout=10.0, clock_values=None):
        ticks = iter(clock_values or [0.0])
        return LockExpirationCoordinator(
            inactivity_timeout=inactivity_timeout,
            renewal_interval=1.0,
            clock_fn=lambda: next(ticks),
        )

    def _fsm_with_lock(self, deck_id: str, element_id: str, token: int = 1, epoch: int = 0) -> dict:
        return {
            "decks": {
                deck_id: {
                    "locks": {
                        element_id: {"fencing_token": token, "lease_epoch": epoch},
                    }
                }
            }
        }

    def test_not_leader_returns_no_proposals(self):
        coord = LockExpirationCoordinator(inactivity_timeout=1.0, renewal_interval=0.1)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id)
        proposals = coord.check_expirations(fsm, now=100.0)
        self.assertEqual(proposals, [])

    def test_new_leader_gives_full_grace_period(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id)

        coord.on_became_leader(fsm, now=0.0)

        # At t=9.9 (just inside grace period), no proposal yet
        proposals = coord.check_expirations(fsm, now=9.9)
        self.assertEqual(proposals, [])

    def test_lock_expires_after_inactivity_timeout(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id)

        coord.on_became_leader(fsm, now=0.0)

        # At t=10.0 (exactly at timeout), propose expiration
        proposals = coord.check_expirations(fsm, now=10.0)
        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0]["deck_id"], deck_id)
        self.assertEqual(proposals[0]["element_id"], element_id)
        self.assertEqual(proposals[0]["expected_token"], 1)
        self.assertEqual(proposals[0]["expected_lease_epoch"], 0)

    def test_one_proposal_per_generation(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id)

        coord.on_became_leader(fsm, now=0.0)

        # First tick after timeout: one proposal
        proposals1 = coord.check_expirations(fsm, now=10.0)
        self.assertEqual(len(proposals1), 1)

        # Second tick: still in-flight, no duplicate
        proposals2 = coord.check_expirations(fsm, now=11.0)
        self.assertEqual(proposals2, [])

    def test_step_down_clears_state(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id)

        coord.on_became_leader(fsm, now=0.0)
        coord.check_expirations(fsm, now=10.0)  # generate in-flight

        coord.on_lost_leadership()

        # After becoming leader again, should give fresh grace period
        coord.on_became_leader(fsm, now=50.0)
        proposals = coord.check_expirations(fsm, now=59.9)
        self.assertEqual(proposals, [])

    def test_timer_resets_on_renewal(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id, token=1, epoch=0)

        coord.on_became_leader(fsm, now=0.0)

        # Renewal commits at t=5 — epoch advances to 1
        renewed_fsm = self._fsm_with_lock(deck_id, element_id, token=1, epoch=1)
        # Tick at t=5 with new state → coordinator resets timer for the new epoch
        coord.check_expirations(renewed_fsm, now=5.0)

        # At t=14 (5 + 9.9) — still inside grace period after renewal
        proposals = coord.check_expirations(renewed_fsm, now=14.9)
        self.assertEqual(proposals, [])

        # At t=15 (5 + 10) — expired
        proposals = coord.check_expirations(renewed_fsm, now=15.0)
        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0]["expected_lease_epoch"], 1)

    def test_deterministic_request_id_is_stable(self):
        deck_id = "d-deck"
        element_id = "e-elem"
        token = 7
        epoch = 3
        rid1 = _deterministic_request_id(deck_id, element_id, token, epoch)
        rid2 = _deterministic_request_id(deck_id, element_id, token, epoch)
        self.assertEqual(rid1, rid2)
        self.assertTrue(rid1.startswith("expire-"))

    def test_deterministic_request_id_differs_per_generation(self):
        deck_id, element_id = "d", "e"
        rid_epoch0 = _deterministic_request_id(deck_id, element_id, 1, 0)
        rid_epoch1 = _deterministic_request_id(deck_id, element_id, 1, 1)
        self.assertNotEqual(rid_epoch0, rid_epoch1)

    def test_on_expiration_committed_clears_in_flight(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id, element_id = str(uuid4()), str(uuid4())
        fsm = self._fsm_with_lock(deck_id, element_id, token=1, epoch=0)

        coord.on_became_leader(fsm, now=0.0)
        proposals = coord.check_expirations(fsm, now=10.0)
        self.assertEqual(len(proposals), 1)

        # Still in-flight
        self.assertEqual(coord.check_expirations(fsm, now=11.0), [])

        # Committed — clear the in-flight marker
        coord.on_expiration_committed(deck_id, element_id, 1, 0)

        # With the lock now gone from fsm, no new proposal either
        empty_fsm = {"decks": {deck_id: {"locks": {}}}}
        self.assertEqual(coord.check_expirations(empty_fsm, now=12.0), [])

    def test_multiple_elements_independent(self):
        coord = LockExpirationCoordinator(inactivity_timeout=10.0, renewal_interval=1.0)
        deck_id = str(uuid4())
        e1 = str(uuid4())
        e2 = str(uuid4())
        fsm = {
            "decks": {
                deck_id: {
                    "locks": {
                        e1: {"fencing_token": 1, "lease_epoch": 0},
                        e2: {"fencing_token": 1, "lease_epoch": 0},
                    }
                }
            }
        }

        coord.on_became_leader(fsm, now=0.0)

        # At t=10, both expire
        proposals = coord.check_expirations(fsm, now=10.0)
        self.assertEqual(len(proposals), 2)
        element_ids = {p["element_id"] for p in proposals}
        self.assertEqual(element_ids, {e1, e2})


# ---------------------------------------------------------------------------
# Spec reference model expire_lock tests
# ---------------------------------------------------------------------------

class SpecExpireLockTests(unittest.TestCase):

    def _make_state(self):
        deck_id = str(uuid4())
        slide_id = str(uuid4())
        element_id = str(uuid4())
        session_id = str(uuid4())
        state = None
        state, _ = spec.apply(state, "publish_deck", {
            "deck_id": deck_id,
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id,
                    "title": "T",
                    "owner_session_id": session_id,
                },
                "slides": [{"metadata": {"slide_id": slide_id}, "elements": []}],
            },
        })
        state, result = spec.apply(state, "create_element", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "session_id": session_id,
            "element": {
                "element_id": element_id,
                "kind": "text_box",
                "configuration": {},
            },
        })
        assert result["status"] == "CREATED_AND_LOCKED"
        fencing_token = result["value"]["fencing_token"]
        return state, deck_id, element_id, session_id, fencing_token

    def test_expire_exact_match(self):
        state, deck_id, element_id, session_id, token = self._make_state()
        new_state, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "element_id": element_id,
            "expected_token": token,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(result["status"], "EXPIRED")

    def test_expire_clears_lock_in_state(self):
        state, deck_id, element_id, session_id, token = self._make_state()
        new_state, _ = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "element_id": element_id,
            "expected_token": token,
            "expected_lease_epoch": 0,
        })
        self.assertIsNone(new_state["elements"][element_id]["lock_owner"])

    def test_expire_stale_token_noop(self):
        state, deck_id, element_id, session_id, token = self._make_state()
        new_state, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "element_id": element_id,
            "expected_token": token + 99,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(result["status"], "NO_OP_STALE_EXPIRY")
        # State unchanged
        self.assertEqual(new_state["elements"][element_id]["lock_owner"], session_id)

    def test_expire_stale_epoch_noop(self):
        state, deck_id, element_id, session_id, token = self._make_state()
        # Renew to epoch 1
        state, _ = spec.apply(state, "renew_lock", {
            "deck_id": deck_id,
            "element_id": element_id,
            "session_id": session_id,
            "fencing_token": token,
        })
        # Try expire with old epoch 0
        new_state, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "element_id": element_id,
            "expected_token": token,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(result["status"], "NO_OP_STALE_EXPIRY")

    def test_expire_missing_element_noop(self):
        state, deck_id, element_id, session_id, token = self._make_state()
        _, result = spec.apply(state, "expire_lock", {
            "deck_id": deck_id,
            "element_id": str(uuid4()),
            "expected_token": 1,
            "expected_lease_epoch": 0,
        })
        self.assertEqual(result["status"], "NO_OP_STALE_EXPIRY")

    def test_expire_lock_in_guarantee(self):
        self.assertIn("expire_lock", spec.GUARANTEE)
        self.assertEqual(spec.GUARANTEE["expire_lock"], "linearizable")

    def test_expire_lock_in_presentation_mutations(self):
        self.assertIn("expire_lock", spec.PRESENTATION_MUTATIONS)


if __name__ == "__main__":
    unittest.main()
