"""Volatile, bounded password-admission helpers for Stage E login."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import hashlib
import hmac
from queue import Empty, SimpleQueue
from threading import Lock
from typing import Any

from auth_dto import PasswordVerifierDTO


MAX_PASSWORD_BYTES = 256
MAX_PASSWORD_WORKERS = 2
MAX_PENDING_PASSWORD_CHECKS = 8
REDACTED = "<redacted>"

_SENSITIVE_KEYS = frozenset({
    "password",
    "access_token",
    "_auth",
    "verifier",
    "password_verifier",
    "salt_hex",
    "hash_hex",
    "token_digest",
})


def _is_sensitive_key(key: object) -> bool:
    normalized = str(key).lower()
    return normalized in _SENSITIVE_KEYS or normalized.endswith((
        "_verifier", "_hash", "_digest", "_salt",
    ))


def redact_auth_data(value: Any) -> Any:
    """Return a recursively sanitized copy safe to fingerprint or serialize."""

    if isinstance(value, dict):
        return {
            key: REDACTED if _is_sensitive_key(key) else redact_auth_data(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_auth_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_auth_data(item) for item in value)
    return value


@dataclass(frozen=True)
class PasswordCheckTask:
    """Secret-bearing immutable input copied for one PBKDF2 worker."""

    attempt_id: str
    captured_term: int
    user_id: str | None
    verifier: PasswordVerifierDTO = field(repr=False)
    password_bytes: bytes = field(repr=False)


@dataclass(frozen=True)
class PasswordCheckResult:
    attempt_id: str
    captured_term: int
    valid: bool
    available: bool = True


def _verify_task(task: PasswordCheckTask) -> PasswordCheckResult:
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        task.password_bytes,
        bytes.fromhex(task.verifier.salt_hex),
        task.verifier.iterations,
    )
    valid = hmac.compare_digest(derived.hex(), task.verifier.hash_hex)
    return PasswordCheckResult(task.attempt_id, task.captured_term, valid)


class BoundedPasswordVerifier:
    """Fixed PBKDF2 pool with an explicit cap on running and queued work."""

    def __init__(
        self,
        *,
        max_workers: int = MAX_PASSWORD_WORKERS,
        max_pending: int = MAX_PENDING_PASSWORD_CHECKS,
    ) -> None:
        if type(max_workers) is not int or max_workers < 1:
            raise ValueError("max_workers must be a positive integer")
        if type(max_pending) is not int or max_pending < max_workers:
            raise ValueError("max_pending must be an integer no smaller than max_workers")
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="password-check")
        self._max_pending = max_pending
        self._pending = 0
        self._lock = Lock()
        self._completed: SimpleQueue[PasswordCheckResult] = SimpleQueue()

    def submit(self, task: PasswordCheckTask) -> bool:
        """Submit without blocking; return false when the explicit queue cap is full."""

        with self._lock:
            if self._pending >= self._max_pending:
                return False
            self._pending += 1
        try:
            future = self._executor.submit(_verify_task, task)
        except RuntimeError:
            with self._lock:
                self._pending -= 1
            return False

        def completed(done) -> None:
            try:
                result = done.result()
            except Exception:
                result = PasswordCheckResult(
                    task.attempt_id,
                    task.captured_term,
                    False,
                    available=False,
                )
            self._completed.put(result)

        future.add_done_callback(completed)
        return True

    def drain_results(self) -> list[PasswordCheckResult]:
        results: list[PasswordCheckResult] = []
        while True:
            try:
                results.append(self._completed.get_nowait())
            except Empty:
                break
        if results:
            with self._lock:
                self._pending -= len(results)
        return results

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)
