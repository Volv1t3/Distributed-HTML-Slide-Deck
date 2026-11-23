"""Client-side Lock Maintenance Service for Stage C.

Maintains one cooperative RenewalWorker per selected element.  Neither clocks
nor threads ever change replicated state; the service only calls the existing
public ``renew_lock`` HTTP endpoint.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class MaintainedLockConfidence(str, Enum):
    """Client-local confidence that a selected element lock is still valid."""

    VERIFIED = "VERIFIED"
    RENEWING = "RENEWING"
    UNVERIFIED = "UNVERIFIED"
    LOST = "LOST"
    STOPPED = "STOPPED"


_DETERMINISTIC_REJECTIONS = frozenset({"STALE_TOKEN", "NOT_OWNER", "NOT_FOUND"})

RequestSender = Callable[[str, dict[str, Any], str], dict[str, Any] | None]
ConfidenceChangeCallback = Callable[[str, MaintainedLockConfidence], None]


@dataclass
class LockAuthority:
    """The client's latest committed view of its lock on one element."""

    deck_id: str
    slide_id: str
    element_id: str
    editor_session: str
    fencing_token: int
    lease_epoch: int
    element_version: int


class RenewalWorker:
    """Background thread that renews one selected element's Lock Authority.

    Uses an injected clock and optional on_confidence_change callback so tests
    can observe state without exposing a manual-tick API.
    """

    def __init__(
        self,
        authority: LockAuthority,
        renewal_interval: float,
        request_sender: RequestSender,
        *,
        clock: Callable[[], float] | None = None,
        on_confidence_change: ConfidenceChangeCallback | None = None,
        http_timeout: float = 5.0,
    ) -> None:
        self.authority = authority
        self.renewal_interval = renewal_interval
        self._request_sender = request_sender
        self._clock = clock or time.monotonic
        self._on_confidence_change = on_confidence_change
        self._http_timeout = http_timeout

        self._stop_event = threading.Event()
        self._confidence = MaintainedLockConfidence.VERIFIED
        # Transient Renewal Attempt: one request_id per outstanding attempt.
        self._transient_request_id: str | None = None
        self._last_renewal_time = self._clock()

        self._thread = threading.Thread(target=self._run, daemon=True)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def confidence(self) -> MaintainedLockConfidence:
        return self._confidence

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def join(self, timeout: float = 10.0) -> None:
        self._thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _set_confidence(self, c: MaintainedLockConfidence) -> None:
        self._confidence = c
        if self._on_confidence_change:
            self._on_confidence_change(self.authority.element_id, c)

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                due_in = self._due_in()
                if due_in > 0:
                    self._stop_event.wait(timeout=due_in)
                    if self._stop_event.is_set():
                        break
                self._attempt_renewal()
                if self._confidence is MaintainedLockConfidence.LOST:
                    break
        finally:
            self._set_confidence(MaintainedLockConfidence.STOPPED)

    def _due_in(self) -> float:
        return max(0.0, self._last_renewal_time + self.renewal_interval - self._clock())

    def _attempt_renewal(self) -> None:
        if self._transient_request_id is None:
            self._transient_request_id = uuid.uuid4().hex

        request_id = self._transient_request_id
        self._set_confidence(MaintainedLockConfidence.RENEWING)

        try:
            response = self._request_sender(
                "renew_lock",
                {
                    "deck_id": self.authority.deck_id,
                    "element_id": self.authority.element_id,
                    "session_id": self.authority.editor_session,
                    "fencing_token": self.authority.fencing_token,
                },
                request_id,
            )
        except Exception:
            self._set_confidence(MaintainedLockConfidence.UNVERIFIED)
            return

        if not isinstance(response, dict) or not response.get("ok"):
            self._set_confidence(MaintainedLockConfidence.UNVERIFIED)
            return

        result = response.get("result") or {}
        status = result.get("status")

        if status == "RENEWED":
            self.authority = LockAuthority(
                deck_id=self.authority.deck_id,
                slide_id=self.authority.slide_id,
                element_id=self.authority.element_id,
                editor_session=self.authority.editor_session,
                fencing_token=self.authority.fencing_token,
                lease_epoch=result.get("lease_epoch", self.authority.lease_epoch + 1),
                element_version=self.authority.element_version,
            )
            self._transient_request_id = None  # fresh ID for the next renewal
            self._last_renewal_time = self._clock()
            self._set_confidence(MaintainedLockConfidence.VERIFIED)

        elif status in _DETERMINISTIC_REJECTIONS:
            self._set_confidence(MaintainedLockConfidence.LOST)

        else:
            # QUORUM_UNAVAILABLE, redirect, or any other uncertain outcome
            self._set_confidence(MaintainedLockConfidence.UNVERIFIED)


class LockMaintenanceService:
    """Coordinator that maintains one RenewalWorker per selected element.

    Use as a context manager to guarantee all workers are stopped on exit.
    """

    def __init__(
        self,
        request_sender: RequestSender | None = None,
        *,
        on_confidence_change: ConfidenceChangeCallback | None = None,
        clock: Callable[[], float] | None = None,
        http_timeout: float = 5.0,
    ) -> None:
        self._request_sender = request_sender
        self._on_confidence_change = on_confidence_change
        self._clock = clock or time.monotonic
        self._http_timeout = http_timeout

        self._registry: dict[str, RenewalWorker] = {}
        self._lock = threading.Lock()
        self._closed = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def register_lock(self, authority: LockAuthority, renewal_interval: float) -> None:
        """Start automatic renewal for one selected element."""
        with self._lock:
            if self._closed:
                raise RuntimeError("LockMaintenanceService is closed")
            if authority.element_id in self._registry:
                raise ValueError(
                    f"element {authority.element_id!r} is already registered"
                )
            worker = RenewalWorker(
                authority=authority,
                renewal_interval=renewal_interval,
                request_sender=self._request_sender,
                clock=self._clock,
                on_confidence_change=self._on_confidence_change,
                http_timeout=self._http_timeout,
            )
            self._registry[authority.element_id] = worker
        worker.start()

    def deregister_lock(
        self,
        element_id: str,
        *,
        release_callback: Callable[[LockAuthority], None] | None = None,
    ) -> MaintainedLockConfidence:
        """Stop renewal for a deselected element and optionally create a release."""
        with self._lock:
            worker = self._registry.pop(element_id, None)
        if worker is None:
            return MaintainedLockConfidence.STOPPED

        worker.stop()
        # Capture confidence BEFORE join — the worker transitions to STOPPED after.
        pre_stop_confidence = worker.confidence
        worker.join()

        if pre_stop_confidence not in {
            MaintainedLockConfidence.LOST,
            MaintainedLockConfidence.STOPPED,
        }:
            if release_callback is not None:
                release_callback(worker.authority)

        return pre_stop_confidence

    def get_confidence(self, element_id: str) -> MaintainedLockConfidence:
        with self._lock:
            worker = self._registry.get(element_id)
        if worker is None:
            return MaintainedLockConfidence.STOPPED
        return worker.confidence

    def close(self) -> None:
        """Stop all workers and prevent new registrations."""
        with self._lock:
            self._closed = True
            workers = list(self._registry.values())
            self._registry.clear()
        for worker in workers:
            worker.stop()
        for worker in workers:
            worker.join()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "LockMaintenanceService":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
