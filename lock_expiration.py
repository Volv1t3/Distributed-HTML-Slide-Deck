"""Leader-local lock expiration coordinator for Stage C."""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any
from uuid import UUID


_DEFAULT_INACTIVITY_TIMEOUT = float(os.environ.get("LOCK_INACTIVITY_TIMEOUT", "10.0"))
_DEFAULT_RENEWAL_INTERVAL = float(os.environ.get("LOCK_RENEWAL_INTERVAL", "3.0"))


def _validate_timing(renewal_interval: float, inactivity_timeout: float) -> None:
    """Raise ValueError if the timing constraint 3 × renewal_interval <= inactivity_timeout is violated."""
    if not (renewal_interval > 0):
        raise ValueError("renewal_interval must be positive")
    if not (inactivity_timeout > 0):
        raise ValueError("inactivity_timeout must be positive")
    if 3 * renewal_interval > inactivity_timeout:
        raise ValueError(
            f"timing constraint violated: 3 × renewal_interval ({3 * renewal_interval}) "
            f"> inactivity_timeout ({inactivity_timeout})"
        )


def _deterministic_request_id(deck_id: str, element_id: str, token: int, epoch: int) -> str:
    """Stable request ID for an expiration proposal; survives leader failover."""
    raw = f"expire|{deck_id}|{element_id}|{token}|{epoch}"
    return "expire-" + hashlib.sha256(raw.encode()).hexdigest()[:32]


class LockExpirationCoordinator:
    """Leader-local inactivity tracker.

    Proposes one ``expire_lock`` command per inactive (token, epoch) generation
    through the Raft proposal path.  Never persisted; rebuilt on every new leadership.
    """

    def __init__(
        self,
        inactivity_timeout: float = _DEFAULT_INACTIVITY_TIMEOUT,
        renewal_interval: float = _DEFAULT_RENEWAL_INTERVAL,
        clock_fn: Any = None,
    ) -> None:
        _validate_timing(renewal_interval, inactivity_timeout)
        self.inactivity_timeout = inactivity_timeout
        self.renewal_interval = renewal_interval
        self._clock_fn = clock_fn if clock_fn is not None else time.monotonic

        # Key: (deck_id, element_id) → (token, epoch, last_seen_monotonic)
        self._activity: dict[tuple[str, str], tuple[int, int, float]] = {}
        # Proposals in flight: (deck_id, element_id, token, epoch)
        self._in_flight: set[tuple[str, str, int, int]] = set()
        self._is_leader: bool = False

    # ------------------------------------------------------------------
    # Leadership callbacks
    # ------------------------------------------------------------------

    def on_became_leader(self, fsm: dict[str, Any], now: float | None = None) -> None:
        """Seed inactivity timers from committed state; every inherited lock gets a full grace period."""
        if now is None:
            now = self._clock_fn()
        self._is_leader = True
        self._activity = {}
        self._in_flight = set()
        for deck_id, deck in fsm.get("decks", {}).items():
            for element_id, lock in deck.get("locks", {}).items():
                self._activity[(deck_id, element_id)] = (
                    lock["fencing_token"],
                    lock["lease_epoch"],
                    now,
                )

    def on_lost_leadership(self) -> None:
        """Discard all ephemeral expiration state on step-down."""
        self._is_leader = False
        self._activity = {}
        self._in_flight = set()

    # ------------------------------------------------------------------
    # Per-tick expiration check
    # ------------------------------------------------------------------

    def check_expirations(
        self, fsm: dict[str, Any], now: float | None = None
    ) -> list[dict[str, Any]]:
        """Return payload dicts for every generation that is now due for expiration.

        Each returned dict is ``{"deck_id": ..., "element_id": ..., "expected_token": ...,
        "expected_lease_epoch": ..., "request_id": ...}``.  The caller builds the full
        ``CommandEnvelopeDTO`` so this module stays free of that dependency.
        """
        if not self._is_leader:
            return []
        if now is None:
            now = self._clock_fn()

        proposals: list[dict[str, Any]] = []
        for deck_id, deck in fsm.get("decks", {}).items():
            for element_id, lock in deck.get("locks", {}).items():
                token: int = lock["fencing_token"]
                epoch: int = lock["lease_epoch"]
                in_flight_key = (deck_id, element_id, token, epoch)
                if in_flight_key in self._in_flight:
                    continue

                activity_key = (deck_id, element_id)
                observed = self._activity.get(activity_key)
                if observed is None or observed[0] != token or observed[1] != epoch:
                    # New or renewed lock — reset the timer, don't expire yet.
                    self._activity[activity_key] = (token, epoch, now)
                    continue

                _, _, last_seen = observed
                if now - last_seen < self.inactivity_timeout:
                    continue

                # Generation is due: emit one proposal and mark in-flight.
                self._in_flight.add(in_flight_key)
                proposals.append({
                    "deck_id": deck_id,
                    "element_id": element_id,
                    "expected_token": token,
                    "expected_lease_epoch": epoch,
                    "request_id": _deterministic_request_id(deck_id, element_id, token, epoch),
                })
        return proposals

    def on_expiration_committed(
        self, deck_id: str, element_id: str, token: int, epoch: int
    ) -> None:
        """Remove the in-flight marker after a committed EXPIRED or NO_OP_STALE_EXPIRY."""
        self._in_flight.discard((deck_id, element_id, token, epoch))
        self._activity.pop((deck_id, element_id), None)
