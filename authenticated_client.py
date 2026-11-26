"""Durable, auth-aware client transport for Stage E.

Owns a stable client_id UUID and a credential store both in a caller-
configured directory outside the disposable run/ tree.  Writes a
checker-compatible JSONL history with password, access_token, and _auth
fields redacted before serialization.

Do NOT edit client.py (instructor-owned) to use this module.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import random
import threading
import time
import urllib.request
from uuid import uuid4

from auth_dto import AUTH_SECRET_KEYS, is_auth_secret_key
from command_dto import PresentationSnapshotV3DTO


def _secret_values(value: object) -> set[str]:
    """Collect secrets from keyed fields so reflected text can be scrubbed."""
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if is_auth_secret_key(key):
                if isinstance(item, str) and item:
                    found.add(item)
                elif isinstance(item, (dict, list, tuple)):
                    found.update(_secret_values(item))
            else:
                found.update(_secret_values(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.update(_secret_values(item))
    return found


def redact_history_data(value: object, secrets: set[str] | frozenset[str] = frozenset()) -> object:
    """Return a recursively sanitized copy safe for JSONL history output."""
    if isinstance(value, dict):
        return {
            key: redact_history_data(item, secrets)
            for key, item in value.items()
            if not is_auth_secret_key(key)
        }
    if isinstance(value, (list, tuple)):
        return [redact_history_data(item, secrets) for item in value]
    if isinstance(value, str):
        for secret in sorted(secrets, key=len, reverse=True):
            value = value.replace(secret, "[REDACTED]")
    return value


def _redact_args(args: dict) -> dict:
    """Remove secret fields at every nesting level before history serialization."""
    return redact_history_data(args)


def _redact_result(result: object) -> object:
    """Remove secret fields from a result before history serialization."""
    return redact_history_data(result)


class AuthenticatedClient:
    """Auth-aware HTTP transport with durable client identity and credential store.

    Parameters
    ----------
    client_dir:
        Directory outside run/ used to persist client_id and the latest
        credential.  Created if it does not exist.  Every file is written
        with mode 0o600.
    nodes:
        Mapping {node_name: url} or list of URL strings, as returned by
        ``cluster.cluster()``.  Shuffled before each request.
    history_path:
        JSONL file for checker-compatible call records.  Secrets are
        redacted before any write.
    timeout:
        Per-attempt HTTP timeout in seconds.
    """

    def __init__(
        self,
        client_dir: "Path | str",
        nodes: "dict[str, str] | list[str]",
        history_path: "Path | str",
        timeout: float = 10.0,
        seed_credential: "dict | None" = None,
    ) -> None:
        self._client_dir = Path(client_dir)
        self._client_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self._client_dir, 0o700)
        # Accept both {name: url} dicts (from cluster.cluster()) and plain url lists.
        if isinstance(nodes, dict):
            self._named_nodes = dict(nodes)
            self._node_urls: list[str] = list(nodes.values())
        else:
            self._named_nodes = {}
            self._node_urls = list(nodes)
        self._history_path = Path(history_path)
        self._history_path.parent.mkdir(parents=True, exist_ok=True)
        self._timeout = timeout
        self._lock = threading.Lock()
        self._in_memory_credential: dict | None = None
        # Load or generate stable client_id first.
        self._client_id: str = self._load_or_create_client_id()
        # Restore previously persisted credential (if any).
        stored = self._load_credential()
        if stored is not None:
            self._in_memory_credential = stored
        if seed_credential is not None:
            self._store_credential(seed_credential)

    # ------------------------------------------------------------------
    # Public identity
    # ------------------------------------------------------------------

    @property
    def client_id(self) -> str:
        """Stable UUID string identifying this client installation."""
        return self._client_id

    # ------------------------------------------------------------------
    # File paths
    # ------------------------------------------------------------------

    @property
    def _client_id_path(self) -> Path:
        return self._client_dir / "client_id"

    @property
    def _credential_path(self) -> Path:
        return self._client_dir / "credential.json"

    def _draft_path(self, deck_id: str) -> Path:
        """Use a UUID filename so a deck ID cannot escape the private draft directory."""
        from uuid import UUID

        return self._client_dir / "drafts" / (str(UUID(deck_id)) + ".json")

    def _current_user_id(self) -> str:
        credential = self._in_memory_credential
        if not isinstance(credential, dict) or not credential.get("user_id"):
            raise PermissionError("a signed-in account is required for a local draft")
        return str(credential["user_id"])

    def create_draft(self, presentation: dict) -> dict:
        """Create a private v3 draft bound to the currently signed-in account."""
        from copy import deepcopy

        owner = self._current_user_id()
        proposed = deepcopy(presentation)
        metadata = proposed.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("draft metadata is required")
        if "owner_user_id" in metadata and metadata["owner_user_id"] != owner:
            raise PermissionError("draft owner differs from the signed-in account")
        metadata["owner_user_id"] = owner
        validated = PresentationSnapshotV3DTO.model_validate(proposed).model_dump(mode="json")
        path = self._draft_path(validated["metadata"]["deck_id"])
        path.parent.mkdir(mode=0o700, exist_ok=True)
        if path.exists():
            raise FileExistsError("local draft already exists")
        self._atomic_write_text(path, json.dumps(validated))
        return validated

    def load_draft(self, deck_id: str) -> dict:
        """Open a draft only while its original account is selected."""
        owner = self._current_user_id()
        draft = json.loads(self._draft_path(deck_id).read_text(encoding="utf-8"))
        validated = PresentationSnapshotV3DTO.model_validate(draft).model_dump(mode="json")
        if validated["metadata"]["owner_user_id"] != owner:
            raise PermissionError("local draft belongs to another account")
        return validated

    def update_draft(self, deck_id: str, changes: dict) -> dict:
        """Persist local metadata or slide edits without requiring network access."""
        draft = self.load_draft(deck_id)
        allowed = {"name", "width_px", "height_px", "slides"}
        if not isinstance(changes, dict) or set(changes) - allowed:
            raise ValueError("unsupported local draft change")
        for key, value in changes.items():
            if key == "slides":
                draft["slides"] = value
            else:
                draft["metadata"][key] = value
        validated = PresentationSnapshotV3DTO.model_validate(draft).model_dump(mode="json")
        self._atomic_write_text(self._draft_path(deck_id), json.dumps(validated))
        return validated

    def publish_draft(self, deck_id: str, request_id: str) -> dict:
        """Submit the unchanged owner claim through authenticated publication."""
        return self.call(request_id, "publish_deck", {"presentation": self.load_draft(deck_id)})

    # ------------------------------------------------------------------
    # Durable identity and credential I/O
    # ------------------------------------------------------------------

    def _load_or_create_client_id(self) -> str:
        path = self._client_id_path
        if path.exists():
            return path.read_text().strip()
        new_id = str(uuid4())
        self._atomic_write_text(path, new_id)
        return new_id

    def _load_credential(self) -> dict | None:
        """Return the persisted credential dict or None if none exists."""
        path = self._credential_path
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def _store_credential(self, credential: dict) -> None:
        """Atomically persist credential if its generation is strictly newer.

        A delayed older-generation response (spec item 13 / AC-4) is silently
        discarded; the same-generation case is also rejected so that duplicate
        delivery cannot replace established material with conflicting random
        data (AC-7).
        """
        new_gen = credential.get("credential_generation", 0)
        with self._lock:
            current = self._in_memory_credential
            if current is not None:
                current_gen = current.get("credential_generation", 0)
                if new_gen <= current_gen:
                    return  # Reject older or equal generation.
            self._atomic_write_text(
                self._credential_path, json.dumps(credential)
            )
            self._in_memory_credential = credential

    # ------------------------------------------------------------------
    # Atomic, permission-restricted file writes
    # ------------------------------------------------------------------

    def _atomic_write_text(self, path: Path, text: str) -> None:
        """Write *text* to *path* atomically with mode 0o600."""
        tmp = path.with_suffix(".tmp")
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, text.encode("utf-8"))
        finally:
            os.close(fd)
        os.replace(str(tmp), str(path))
        # Guarantee mode on the destination (os.replace may inherit umask).
        os.chmod(str(path), 0o600)

    # ------------------------------------------------------------------
    # HTTP transport
    # ------------------------------------------------------------------

    def _raw_post(self, url: str, payload: dict, timeout: float | None = None) -> dict:
        """Single POST to *url*/client.  Raises on network error."""
        endpoint = url.rstrip("/") + "/client"
        req = urllib.request.Request(
            endpoint,
            json.dumps(payload).encode("utf-8"),
            {"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self._timeout if timeout is None else timeout) as r:
            return json.loads(r.read())

    def _try_nodes(self, payload: dict, timeout: float | None = None) -> dict:
        """Attempt POST on shuffled node list, following one redirect."""
        urls = list(self._node_urls)
        random.shuffle(urls)
        last_exc: Exception | None = None
        for url in urls:
            try:
                resp = self._raw_post(url, payload, timeout)
            except Exception as exc:
                last_exc = exc
                continue
            redirect = resp.get("redirect")
            if redirect is not None:
                # redirect is a node name; find a matching URL.
                for candidate in self._node_urls:
                    if redirect in candidate or candidate.endswith(redirect):
                        try:
                            return self._raw_post(candidate, payload, timeout)
                        except Exception as exc2:
                            last_exc = exc2
                continue
            return resp
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("no nodes available")

    # ------------------------------------------------------------------
    # History serialization (secrets always redacted)
    # ------------------------------------------------------------------

    def _write_history(self, entry: dict, secrets: set[str] | None = None) -> None:
        """Append one JSONL record; file-write is serialized under the lock."""
        sanitized = redact_history_data(entry, secrets or set())
        with self._lock:
            with open(self._history_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(sanitized) + "\n")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def login(
        self,
        request_id: str,
        email: str,
        password: str,
    ) -> dict:
        """Submit a password login request.

        The caller supplies the request ID explicitly.  This method never
        auto-generates one so the caller can correlate the request with an
        intention journal entry or a later reauthentication attempt (AC-6).

        Returns the server response dict unchanged, including error responses
        such as ``{"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"}`` (AC-5).
        Secrets are stripped from the JSONL history record.
        """
        args: dict = {
            "email": email,
            "password": password,
            "client_id": self._client_id,
        }
        payload = {
            "id": request_id,
            "op": "login",
            "client": self._client_id,
            "args": args,
        }

        t_start = time.time()
        response: dict
        error_msg: str | None = None
        try:
            response = self._try_nodes(payload)
        except Exception as exc:
            error_msg = type(exc).__name__
            response = {"ok": False, "error": error_msg}
        t_end = time.time()

        # Install credential only on AUTHENTICATED success.
        if response.get("ok") and isinstance(response.get("result"), dict):
            result = response["result"]
            if result.get("status") == "AUTHENTICATED":
                self._store_credential({
                    "access_token": result["access_token"],
                    "credential_id": result["credential_id"],
                    "credential_generation": result["credential_generation"],
                    "user_id": result["user_id"],
                })

        # Redact before writing history (AC-3, spec item 14).
        redacted_args = _redact_args(args)
        redacted_result = (
            _redact_result(response["result"])
            if isinstance(response.get("result"), dict)
            else response.get("result")
        )

        entry: dict = {
            "id": request_id,
            "client": self._client_id,
            "op": "login",
            "args": redacted_args,
            "invoke": t_start,
            "complete": t_end if response.get("ok") else None,
            "ok": bool(response.get("ok")),
            "result": redacted_result,
            "error": response.get("error"),
        }
        self._write_history(entry, _secret_values(payload) | _secret_values(response))

        return response

    def call(
        self,
        request_id: str,
        op: str,
        args: dict,
        *,
        node: str | None = None,
        timeout: float | None = None,
    ) -> dict:
        """Call a business operation, injecting _auth immediately before transport.

        ``_auth`` is added to the wire payload only at the moment of the HTTP
        call and is stripped from the JSONL history entry (AC-3).  The caller-
        supplied ``args`` dict is never mutated.
        """
        # Build wire args: inject _auth *immediately* before the raw request.
        wire_args = dict(args)
        if self._in_memory_credential is not None:
            wire_args["_auth"] = {
                "access_token": self._in_memory_credential["access_token"]
            }

        payload = {
            "id": request_id,
            "op": op,
            "client": self._client_id,
            "args": wire_args,
        }

        t_start = time.time()
        response: dict
        error_msg: str | None = None
        try:
            if node is None:
                response = self._try_nodes(payload, timeout)
            else:
                response = self._raw_post(self._named_nodes.get(node, node), payload, timeout)
        except Exception as exc:
            error_msg = type(exc).__name__
            response = {"ok": False, "error": error_msg}
        t_end = time.time()

        # Redact the original (non-wire) args for history (AC-3).
        redacted_args = _redact_args(args)

        entry: dict = {
            "id": request_id,
            "client": self._client_id,
            "op": op,
            "args": redacted_args,
            "invoke": t_start,
            "complete": t_end if response.get("ok") else None,
            "ok": bool(response.get("ok")),
            "result": _redact_result(response.get("result")),
            "error": response.get("error"),
        }
        self._write_history(entry, _secret_values(payload) | _secret_values(response))

        return response

    def logout(self, request_id: str) -> dict:
        """Revoke the locally current credential with an explicit generation fence."""
        with self._lock:
            credential = dict(self._in_memory_credential) if self._in_memory_credential else None
        if credential is None:
            return {"ok": False, "error": "REAUTH_REQUIRED"}
        response = self.call(request_id, "revoke_access_token", {
            "expected_credential_id": credential["credential_id"],
            "expected_generation": credential["credential_generation"],
        })
        if (response.get("ok") and isinstance(response.get("result"), dict)
                and response["result"].get("status") == "TOKEN_REVOKED"):
            with self._lock:
                if self._in_memory_credential == credential:
                    self._credential_path.unlink(missing_ok=True)
                    self._in_memory_credential = None
        return response

    def journal_sender(self, op: str, args: dict, request_id: str, node: str) -> dict:
        """Adapter for ``JournaledClient`` that keeps its chosen replica route."""
        return self.call(request_id, op, args, node=node)

    def renewal_sender(self, op: str, args: dict, request_id: str, timeout: float) -> dict:
        """Adapter for lock maintenance's transient exact-ID attempts."""
        return self.call(request_id, op, args, timeout=timeout)
