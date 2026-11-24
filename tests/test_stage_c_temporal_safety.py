"""Deterministic temporal safety tests for Stage C (issue #21).

Covers four race conditions using the state machine and LockExpirationCoordinator
directly with injected clocks — no live HTTP cluster, no wall-clock sleeps.

Race 1: renewal committed before an older expiration proposal → NO_OP_STALE_EXPIRY
Race 2: expiration committed before renewal → STALE_TOKEN; new owner gets larger token
Race 3: delayed old-token release after reassignment → rejected, new owner unaffected
Race 4: leader change after renewal → full grace period re-seeded, committed authority preserved
Extra: one proposal per generation (coordinator dedup)
"""
import unittest
from uuid import uuid4

from command_dto import CommandEnvelopeDTO
from lock_expiration import LockExpirationCoordinator
from state_machine import KeyValueStateMachine

# ---------------------------------------------------------------------------
# Shared helpers
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


def _publish_payload(deck_id, slide_id):
    return {
        "presentation": {
            "schema_version": 1,
            "metadata": {
                "deck_id": str(deck_id),
                "name": "Temporal Safety Deck",
                "owner_session_id": str(uuid4()),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [
                {
                    "metadata": {
                        "slide_id": str(slide_id),
                        "background": "#000000",
                        "ordinal": 0,
                    },
                    "elements": [],
                }
            ],
        }
    }


def _element_payload(element_id):
    return {
        "element_id": str(element_id),
        "kind": "text_box",
        "version": 1,
        "configuration": _TEXT_CONFIG,
    }


def _cmd(operation, payload, *, deck_id, client_id="tester", session_id=None, request_id=None):
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id or f"{operation}-{uuid4()}",
        "client_id": client_id,
        "session_id": str(session_id) if session_id is not None else None,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": operation,
        "payload": payload,
    })


def _expire_cmd(deck_id, element_id, expected_token, expected_epoch, *, request_id=None):
    return _cmd(
        "expire_lock",
        {
            "element_id": str(element_id),
            "expected_token": expected_token,
            "expected_lease_epoch": expected_epoch,
        },
        deck_id=deck_id,
        client_id="_lock_expiration",
        session_id=None,
        request_id=request_id or f"expire-{uuid4()}",
    )


def _build_machine_with_lock(deck_id, slide_id, element_id, owner_session):
    """Return (machine, fencing_token) for a fresh deck with one locked element."""
    m = KeyValueStateMachine({"kv": {}})
    r = m.apply(_cmd("publish_deck", _publish_payload(deck_id, slide_id), deck_id=deck_id))
    assert r.status == "PUBLISHED"
    r = m.apply(_cmd(
        "create_element",
        {"slide_id": str(slide_id), "element": _element_payload(element_id)},
        deck_id=deck_id,
        session_id=owner_session,
    ))
    assert r.status == "CREATED_AND_LOCKED"
    return m, r.value["fencing_token"]


# ---------------------------------------------------------------------------
# Base test class with common setup
# ---------------------------------------------------------------------------

class TemporalSafetyBase(unittest.TestCase):
    def setUp(self):
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.element_id = uuid4()
        self.owner = uuid4()
        self.other = uuid4()
        self.machine, self.token = _build_machine_with_lock(
            self.deck_id, self.slide_id, self.element_id, self.owner,
        )

    # Convenience wrappers

    def renew(self, session, token, *, request_id=None):
        return self.machine.apply(_cmd(
            "renew_lock",
            {"element_id": str(self.element_id), "fencing_token": token},
            deck_id=self.deck_id,
            session_id=session,
            request_id=request_id,
        ))

    def expire(self, token, epoch, *, request_id=None):
        return self.machine.apply(_expire_cmd(
            self.deck_id, self.element_id, token, epoch, request_id=request_id,
        ))

    def release(self, session, token, *, request_id=None):
        return self.machine.apply(_cmd(
            "release_lock",
            {"element_id": str(self.element_id), "fencing_token": token},
            deck_id=self.deck_id,
            session_id=session,
            request_id=request_id,
        ))

    def acquire(self, session, *, request_id=None):
        return self.machine.apply(_cmd(
            "acquire_lock",
            {"element_id": str(self.element_id)},
            deck_id=self.deck_id,
            session_id=session,
            request_id=request_id,
        ))

    def _lock_state(self):
        """Return the raw lock dict for the element, or None if not held."""
        snapshot = self.machine.snapshot()
        deck = snapshot.get("decks", {}).get(str(self.deck_id), {})
        return deck.get("locks", {}).get(str(self.element_id))

    def _fencing_counter(self):
        snapshot = self.machine.snapshot()
        deck = snapshot.get("decks", {}).get(str(self.deck_id), {})
        return deck.get("fencing_counters", {}).get(str(self.element_id))

    def _element_version(self):
        snapshot = self.machine.snapshot()
        deck = snapshot.get("decks", {}).get(str(self.deck_id), {})
        for slide in deck.get("slides", []):
            for el in slide.get("elements", []):
                if el["element_id"] == str(self.element_id):
                    return el["version"]
        return None


# ---------------------------------------------------------------------------
# Race 1: renewal committed before an older expiration proposal
# ---------------------------------------------------------------------------

class Race1RenewalBeforeExpiration(TemporalSafetyBase):
    """Renewal advances epoch; an older expiration proposal targeting the original epoch is a no-op."""

    def test_renewal_advances_epoch(self):
        r = self.renew(self.owner, self.token)
        self.assertEqual(r.status, "RENEWED")
        self.assertEqual(r.value["lease_epoch"], 1)

    def test_stale_expiration_after_renewal_is_noop(self):
        # Renew first (epoch 0 → 1)
        self.renew(self.owner, self.token)
        # Expiration proposal targeting old epoch 0 arrives late
        r = self.expire(self.token, 0)
        self.assertEqual(r.status, "NO_OP_STALE_EXPIRY")

    def test_lock_still_active_after_stale_expiration(self):
        self.renew(self.owner, self.token)
        self.expire(self.token, 0)
        lock = self._lock_state()
        self.assertIsNotNone(lock)
        self.assertEqual(lock["lease_epoch"], 1)

    def test_element_version_unchanged_after_renewal_and_stale_expiry(self):
        v_before = self._element_version()
        self.renew(self.owner, self.token)
        self.expire(self.token, 0)
        self.assertEqual(self._element_version(), v_before)

    def test_multiple_renewals_then_stale_expiry(self):
        # Renew twice: epoch goes 0 → 1 → 2
        r1 = self.renew(self.owner, self.token)
        r2 = self.renew(self.owner, self.token)
        self.assertEqual(r1.value["lease_epoch"], 1)
        self.assertEqual(r2.value["lease_epoch"], 2)
        # Expiration at epoch 0 is a no-op
        r = self.expire(self.token, 0)
        self.assertEqual(r.status, "NO_OP_STALE_EXPIRY")
        # Expiration at epoch 1 (also stale) is a no-op
        r = self.expire(self.token, 1)
        self.assertEqual(r.status, "NO_OP_STALE_EXPIRY")
        # Only current epoch 2 would expire the lock
        lock = self._lock_state()
        self.assertIsNotNone(lock)
        self.assertEqual(lock["lease_epoch"], 2)

    def test_current_epoch_expiration_succeeds(self):
        self.renew(self.owner, self.token)
        # Expiration at current epoch 1 should succeed
        r = self.expire(self.token, 1)
        self.assertEqual(r.status, "EXPIRED")
        self.assertIsNone(self._lock_state())


# ---------------------------------------------------------------------------
# Race 2: expiration committed before renewal
# ---------------------------------------------------------------------------

class Race2ExpirationBeforeRenewal(TemporalSafetyBase):
    """Expiration first removes the lock; subsequent renewal returns STALE_TOKEN."""

    def test_expiration_at_initial_epoch_succeeds(self):
        r = self.expire(self.token, 0)
        self.assertEqual(r.status, "EXPIRED")
        self.assertIsNone(self._lock_state())

    def test_expired_lock_fencing_counter_retained(self):
        counter_before = self._fencing_counter()
        self.expire(self.token, 0)
        # Counter must not have decreased
        self.assertEqual(self._fencing_counter(), counter_before)

    def test_renewal_after_expiration_returns_stale_token(self):
        self.expire(self.token, 0)
        r = self.renew(self.owner, self.token)
        self.assertEqual(r.status, "STALE_TOKEN")

    def test_new_acquisition_after_expiration_gets_larger_token(self):
        old_token = self.token
        self.expire(old_token, 0)
        r = self.acquire(self.other)
        self.assertEqual(r.status, "LOCK_ACQUIRED")
        new_token = r.value["fencing_token"]
        self.assertGreater(new_token, old_token)

    def test_new_acquisition_starts_at_epoch_zero(self):
        self.expire(self.token, 0)
        r = self.acquire(self.other)
        self.assertEqual(r.value["lease_epoch"], 0)

    def test_element_version_unchanged_through_expiration_and_reacquire(self):
        v_before = self._element_version()
        self.expire(self.token, 0)
        self.acquire(self.other)
        self.assertEqual(self._element_version(), v_before)

    def test_former_owner_renewal_after_new_acquisition_rejected(self):
        # Expire, then other editor acquires
        self.expire(self.token, 0)
        r2 = self.acquire(self.other)
        new_token = r2.value["fencing_token"]
        # Former owner tries to renew with old token — STALE_TOKEN
        r = self.renew(self.owner, self.token)
        self.assertEqual(r.status, "STALE_TOKEN")
        # New owner's lock is unaffected
        lock = self._lock_state()
        self.assertIsNotNone(lock)
        self.assertEqual(lock["fencing_token"], new_token)


# ---------------------------------------------------------------------------
# Race 3: delayed old-token release after reassignment
# ---------------------------------------------------------------------------

class Race3DelayedOldTokenRelease(TemporalSafetyBase):
    """An old release (or renewal) with the former token cannot affect the new owner."""

    def _setup_reassigned(self):
        """Expire original lock, new editor acquires; return new token."""
        self.expire(self.token, 0)
        r = self.acquire(self.other)
        self.assertEqual(r.status, "LOCK_ACQUIRED")
        return r.value["fencing_token"]

    def test_delayed_release_with_old_token_is_stale(self):
        new_token = self._setup_reassigned()
        r = self.release(self.owner, self.token)
        self.assertIn(r.status, {"STALE_TOKEN", "NOT_OWNER"})

    def test_new_owner_lock_unaffected_by_delayed_release(self):
        new_token = self._setup_reassigned()
        self.release(self.owner, self.token)
        lock = self._lock_state()
        self.assertIsNotNone(lock)
        self.assertEqual(lock["fencing_token"], new_token)
        self.assertEqual(lock["session_id"], str(self.other))

    def test_delayed_renewal_with_old_token_is_stale(self):
        self._setup_reassigned()
        r = self.renew(self.owner, self.token)
        self.assertEqual(r.status, "STALE_TOKEN")

    def test_new_owner_can_still_renew_after_delayed_message(self):
        new_token = self._setup_reassigned()
        # Delayed old-owner release arrives
        self.release(self.owner, self.token)
        # New owner renews successfully
        r = self.renew(self.other, new_token)
        self.assertEqual(r.status, "RENEWED")
        self.assertEqual(r.value["lease_epoch"], 1)

    def test_second_expiration_with_old_token_is_noop(self):
        self._setup_reassigned()
        # Another expiration proposal for the original (token, epoch=0) arrives
        r = self.expire(self.token, 0)
        self.assertEqual(r.status, "NO_OP_STALE_EXPIRY")

    def test_new_owner_element_version_unchanged_by_delayed_messages(self):
        v_before = self._element_version()
        new_token = self._setup_reassigned()
        self.release(self.owner, self.token)
        self.renew(self.owner, self.token)
        self.assertEqual(self._element_version(), v_before)


# ---------------------------------------------------------------------------
# Race 4: leader change after renewal — grace period and committed authority
# ---------------------------------------------------------------------------

class Race4LeaderChangeAfterRenewal(unittest.TestCase):
    """LockExpirationCoordinator grants full grace period to inherited locks on leader election."""

    def setUp(self):
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.element_id = uuid4()
        self.owner = uuid4()
        self.machine, self.token = _build_machine_with_lock(
            self.deck_id, self.slide_id, self.element_id, self.owner,
        )
        # Injected clock starts at t=0
        self._now = 0.0

    def _clock(self):
        return self._now

    def _advance(self, seconds):
        self._now += seconds

    def _coordinator(self, inactivity_timeout=10.0):
        return LockExpirationCoordinator(
            inactivity_timeout=inactivity_timeout,
            renewal_interval=inactivity_timeout / 4,
            clock_fn=self._clock,
        )

    def _fsm(self):
        return self.machine.snapshot()

    def _renew(self):
        r = self.machine.apply(_cmd(
            "renew_lock",
            {"element_id": str(self.element_id), "fencing_token": self.token},
            deck_id=self.deck_id,
            session_id=self.owner,
        ))
        return r

    def test_new_leader_gives_inherited_lock_full_grace_period(self):
        coord = self._coordinator(inactivity_timeout=10.0)
        # Become leader at t=0
        coord.on_became_leader(self._fsm(), now=self._now)
        # Advance to just before timeout
        self._advance(9.99)
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(proposals, [], "No expiration should fire within grace period")

    def test_expiration_fires_after_full_grace_period(self):
        coord = self._coordinator(inactivity_timeout=10.0)
        coord.on_became_leader(self._fsm(), now=self._now)
        # Advance past inactivity_timeout
        self._advance(10.01)
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(len(proposals), 1)
        p = proposals[0]
        self.assertEqual(p["element_id"], str(self.element_id))
        self.assertEqual(p["expected_token"], self.token)
        self.assertEqual(p["expected_lease_epoch"], 0)

    def test_step_down_clears_all_timers(self):
        coord = self._coordinator(inactivity_timeout=10.0)
        coord.on_became_leader(self._fsm(), now=self._now)
        self._advance(10.01)
        coord.on_lost_leadership()
        # Even past the timeout, no proposals after step-down
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(proposals, [])

    def test_new_leader_after_stepdown_gets_fresh_grace_period(self):
        coord = self._coordinator(inactivity_timeout=10.0)
        # First leader at t=0
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(5.0)
        # Step down at t=5
        coord.on_lost_leadership()
        # New leader at t=5 — full grace period starts now
        self._advance(0.0)  # still t=5
        coord.on_became_leader(self._fsm(), now=self._now)
        # Advance only 7 more seconds (total 12, but grace period reset at t=5)
        self._advance(7.0)
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(proposals, [], "Lock still within fresh grace period")

    def test_renewal_resets_timer_so_new_leader_grace_period_covers_renewed_epoch(self):
        coord = self._coordinator(inactivity_timeout=10.0)
        # Renew the lock (epoch 0 → 1)
        r = self._renew()
        self.assertEqual(r.status, "RENEWED")
        new_epoch = r.value["lease_epoch"]
        # New leader becomes leader at t=0 with the renewed lock in FSM
        coord.on_became_leader(self._fsm(), now=self._now)
        # Advance within grace period
        self._advance(9.0)
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(proposals, [], "Still within grace period after renewal")

    def test_renewal_committed_authority_preserved_through_leader_change(self):
        """Committed renew_lock result is preserved; the state machine already holds it."""
        r = self._renew()
        self.assertEqual(r.status, "RENEWED")
        lock = self.machine.snapshot()["decks"][str(self.deck_id)]["locks"][str(self.element_id)]
        self.assertEqual(lock["lease_epoch"], 1)
        # Coordinator step-down and new leader do not undo committed state
        coord = self._coordinator()
        coord.on_became_leader(self._fsm(), now=self._now)
        coord.on_lost_leadership()
        coord.on_became_leader(self._fsm(), now=self._now)
        lock_after = self.machine.snapshot()["decks"][str(self.deck_id)]["locks"][str(self.element_id)]
        self.assertEqual(lock_after["lease_epoch"], 1)


# ---------------------------------------------------------------------------
# Extra: one proposal per generation (coordinator dedup)
# ---------------------------------------------------------------------------

class CoordinatorDeduplication(unittest.TestCase):
    """check_expirations emits at most one proposal per (token, epoch) generation."""

    def setUp(self):
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.element_id = uuid4()
        self.owner = uuid4()
        self.machine, self.token = _build_machine_with_lock(
            self.deck_id, self.slide_id, self.element_id, self.owner,
        )
        self._now = 0.0

    def _clock(self):
        return self._now

    def _advance(self, s):
        self._now += s

    def _coord(self):
        return LockExpirationCoordinator(
            inactivity_timeout=10.0,
            renewal_interval=2.5,
            clock_fn=self._clock,
        )

    def _fsm(self):
        return self.machine.snapshot()

    def test_only_one_proposal_emitted_per_generation(self):
        coord = self._coord()
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(10.01)
        p1 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(len(p1), 1)
        # Second call: same generation still in-flight → no new proposal
        p2 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(p2, [])

    def test_renewal_clears_in_flight_marker_and_resets_timer(self):
        coord = self._coord()
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(10.01)
        coord.check_expirations(self._fsm(), now=self._now)

        # Renewal commits: apply to state machine
        r = self.machine.apply(_cmd(
            "renew_lock",
            {"element_id": str(self.element_id), "fencing_token": self.token},
            deck_id=self.deck_id,
            session_id=self.owner,
        ))
        self.assertEqual(r.status, "RENEWED")

        # check_expirations immediately after renewal → no proposal (timer reset)
        p = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(p, [])

    def test_expiration_committed_removes_timer(self):
        coord = self._coord()
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(10.01)
        proposals = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(len(proposals), 1)

        # Apply expiration to state machine
        p = proposals[0]
        r = self.machine.apply(_expire_cmd(
            self.deck_id, self.element_id,
            p["expected_token"], p["expected_lease_epoch"],
            request_id=p["request_id"],
        ))
        self.assertEqual(r.status, "EXPIRED")

        # Coordinator notified of committed expiration
        coord.on_expiration_committed(
            str(self.deck_id), str(self.element_id),
            p["expected_token"], p["expected_lease_epoch"],
        )

        # FSM now has no lock; check_expirations returns empty
        p2 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(p2, [])

    def test_step_down_clears_in_flight_markers(self):
        coord = self._coord()
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(10.01)
        p1 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(len(p1), 1)

        # Step down and re-elect
        coord.on_lost_leadership()
        coord.on_became_leader(self._fsm(), now=self._now)

        # Immediately after new leadership: within grace period (timer reset)
        p2 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(p2, [], "New leader resets timers; no proposal within grace period")

    def test_deterministic_request_id_stable_across_calls(self):
        coord = self._coord()
        coord.on_became_leader(self._fsm(), now=0.0)
        self._advance(10.01)
        p1 = coord.check_expirations(self._fsm(), now=self._now)
        # Clear in-flight marker manually to test second call returns same id
        coord._in_flight.clear()
        p2 = coord.check_expirations(self._fsm(), now=self._now)
        self.assertEqual(len(p2), 1)
        self.assertEqual(p1[0]["request_id"], p2[0]["request_id"])


if __name__ == "__main__":
    unittest.main()
