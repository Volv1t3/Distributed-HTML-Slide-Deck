"""Stage A.1 adapter between the unchanged starter callbacks and Raft."""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import Enum
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any, Callable
from uuid import UUID, uuid4

from pydantic import ValidationError

from auth_catalog import load_account_catalog
from auth_admission import (
    BoundedPasswordVerifier,
    MAX_PASSWORD_BYTES,
    PasswordCheckTask,
    REDACTED,
    redact_auth_data,
)
from auth_dto import (
    PasswordVerifierDTO,
    auth_catalog_activation_request_id,
    auth_schema_upgrade_request_id,
    catalog_fingerprint,
    catalog_projection,
    initial_auth_state,
    parse_exact_email_query,
    validate_auth_state,
)
from command_dto import (
    BEARER_GUARDED_OPERATIONS,
    BEARER_OPTIONAL_OPERATIONS,
    ClientResultDTO,
    CommandEnvelopeDTO,
    PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
    PROTECTED_READ_OPERATIONS,
    command_fingerprint,
    legacy_deck_migration_command,
    presentation_schema_activation_request_id,
)
from dto_base import sha256_json
from lock_expiration import LockExpirationCoordinator
from node import Node
from persistence import NodeDiskPersistence
from raft_core import RaftEngine, ReadIndexStatus
from raft_dto import LogEntryDTO, RegisterNodeDTO
from state_machine import KeyValueStateMachine


LOGIN_PROPOSAL_TIMEOUT = 3.0

# A receipt proves only the outcome of these post-publication editing commands.
# Keep the success mapping operation-specific; a generic "OK" or an unrelated
# committed command must never be promoted into an editing receipt.
RECEIPT_SUCCESS_STATUSES = {
    "create_slide": frozenset({"CREATED"}),
    "delete_slide": frozenset({"DELETED"}),
    "reorder_slides": frozenset({"REORDERED"}),
    "create_element": frozenset({"CREATED_AND_LOCKED"}),
    "acquire_lock": frozenset({"LOCK_ACQUIRED", "ALREADY_HELD"}),
    "release_lock": frozenset({"RELEASED"}),
    "update_element": frozenset({"SAVED"}),
    "delete_element": frozenset({"DELETED"}),
    "renew_lock": frozenset({"RENEWED"}),
    "update_deck_settings": frozenset({"DECK_SETTINGS_UPDATED"}),
    "update_slide_settings": frozenset({"SLIDE_SETTINGS_UPDATED"}),
}


@dataclass(frozen=True)
class ProtectedReadWaiter:
    """A bearer-checked deck read awaiting one fresh leader ReadIndex barrier.

    The raw access_token lives only in this volatile object while the read is
    pending; it never reaches Raft, persisted state, or the event log.
    """

    request_key: str
    request_id: str
    client_id: str
    access_token: str = field(repr=False)
    operation: str
    args: dict
    captured_term: int
    read_context: UUID


@dataclass(frozen=True)
class LoginReadWaiter:
    """Event-loop-owned password admission awaiting a fresh quorum read."""

    request_key: str
    request_id: str
    client_id: str
    normalized_email: str
    password_bytes: bytes = field(repr=False)
    captured_term: int
    read_context: UUID
    attempt_id: str


@dataclass(frozen=True)
class PendingPasswordCheck:
    """Non-secret metadata retained while a PBKDF2 worker is active."""

    request_key: str
    request_id: str
    client_id: str
    user_id: str | None
    account_exists: bool
    captured_term: int
    attempt_id: str


@dataclass(frozen=True)
class AuthAdmittedCommandWaiter:
    """Event-loop-owned bearer admission awaiting a fresh quorum read.

    The raw access_token is held only in this volatile in-memory object.
    It is never written to any log, Raft state, or persistent store.
    """

    request_key: str
    request_id: str
    client_id: str
    access_token: str = field(repr=False)
    operation: str
    deck_id: str | None
    # args with _auth stripped and without actor assertion fields
    stripped_args: dict = field(repr=False)
    captured_term: int
    read_context: UUID


@dataclass(frozen=True)
class PendingLoginProposal:
    """Volatile token material associated with one exact issuance command."""

    request_key: str
    request_id: str
    user_id: str
    client_id: str
    credential_id: str
    captured_term: int
    deadline: float
    command: CommandEnvelopeDTO
    command_fingerprint: str
    token_digest: str
    log_index: int | None
    access_token: str = field(repr=False)


class AuthCatalogStatus(str, Enum):
    """Local readiness view of the configured and replicated catalogs."""

    PENDING = "PENDING"
    UPGRADE_PENDING = "UPGRADE_PENDING"
    READY = "READY"
    FINGERPRINT_MISMATCH = "FINGERPRINT_MISMATCH"


class App(Node):
    def start(self):
        mode = os.environ.get("OPERATIONAL_LOG_MODE", "OPERATIONAL").upper()
        if mode not in {"OPERATIONAL", "DEBUG"}:
            raise ValueError("OPERATIONAL_LOG_MODE must be OPERATIONAL or DEBUG")
        self._log_mode = mode
        self._events = open(Path(self.run_dir) / self.id / "events.log", "a", buffering=1)
        self._storage = NodeDiskPersistence(self.disk, self.save)
        self._account_catalog = load_account_catalog()
        self._configured_catalog_fingerprint = catalog_fingerprint(self._account_catalog)
        # Operator assertion that the coordinated Stage F binary rollout is complete.
        # The committed policy, rather than this local switch, controls application.
        self._activation_proposal_enabled = os.environ.get("STAGE_F_ALL_VOTERS_READY") == "1"
        restored = self._storage.load()
        node_uuid = restored.node_uuid if restored is not None else uuid4()
        self._waiters: dict[str, list[str]] = {}
        self._protected_read_waiters: dict[str, ProtectedReadWaiter] = {}
        self._login_read_waiters: dict[str, LoginReadWaiter] = {}
        self._auth_admitted_waiters: dict[str, AuthAdmittedCommandWaiter] = {}
        self._password_checks: dict[str, PendingPasswordCheck] = {}
        self._pending_login_proposals: dict[str, PendingLoginProposal] = {}
        # Request IDs whose token material was discarded after an uncertain
        # issuance attempt must not generate a second secret in this incarnation.
        self._lost_login_secret_keys: set[str] = set()
        self._password_verifier = self._create_password_verifier()
        self._pending_expirations: dict[str, dict[str, Any]] = {}
        self.engine = RaftEngine(
            node_slot=self.id,
            voters=self.nodes,
            node_uuid=node_uuid,
            storage=self._storage,
            send=self._send_rpc,
            clock=self.now,
            genesis_fsm={
                "kv": {},
                # enforced_from_index = 1: auth is enforced from the first committed
                # entry on a fresh Stage E cluster (Spec §6).  Callers must hold a
                # valid bearer at admission for every guarded mutation.
                "auth": initial_auth_state(self._account_catalog, enforced_from_index=1),
            },
        )
        self._refresh_auth_catalog_status()
        self._event(
            "startup",
            mode=self._log_mode,
            incarnation=self.engine.state.incarnation,
            auth_catalog_status=self.auth_catalog_status.value,
            catalog_fingerprint=self._configured_catalog_fingerprint,
        )
        self._next_registration = 0.0
        self._previous_role: str = "follower"
        self._expiration_coordinator = LockExpirationCoordinator()
        self._register_unbound_peers()

    def _create_password_verifier(self) -> BoundedPasswordVerifier:
        """Factory seam for deterministic in-process admission tests."""

        return BoundedPasswordVerifier()

    @staticmethod
    def _new_access_token_bytes() -> bytes:
        return secrets.token_bytes(32)

    @staticmethod
    def _new_credential_id() -> UUID:
        return uuid4()

    def _register_unbound_peers(self) -> None:
        """Retry the bootstrap handshake until every configured peer is bound."""
        now = self.now()
        if now < self._next_registration:
            return
        sent = False
        for peer in self.peers:
            if peer in self.engine.state.peer_bindings:
                continue
            self._send_registration(peer)
            sent = True
        if sent:
            self._next_registration = now + 0.5

    def _send_registration(self, peer: str) -> None:
        registration = RegisterNodeDTO(
            kind="register_node",
            protocol_version=1,
            rpc_id=uuid4(),
            sender_slot=self.id,
            sender_uuid=self.engine.node_uuid,
            term=self.engine.state.raft.current_term,
            incarnation=self.engine.state.incarnation,
            membership_fingerprint=self.engine.membership_fingerprint(),
        )
        self._send_rpc(peer, registration.model_dump(mode="json"))

    def on_request(self, req):
        # Caller-supplied actor assertions are always forbidden: they would let a
        # client forge an authenticated identity without a valid bearer. Only the
        # server stamps these fields, at admission. They are excluded from the
        # command fingerprint, so a retried intention keeps its identity across
        # reauthentication; they are never accepted as caller input on any path.
        if isinstance(req.args, Mapping) and any(
            field in req.args
            for field in ("authenticated_actor_user_id", "credential_id", "credential_generation")
        ):
            self._event(
                "request_rejected",
                request_id=getattr(req, "id", None),
                error_code="INVALID_REQUEST",
                detail="caller may not supply server-stamped actor fields",
            )
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        safe_args = (
            REDACTED
            if req.op == "login" and not isinstance(req.args, Mapping)
            else redact_auth_data(req.args)
        )
        self._event(
            "request_received",
            request_id=req.id,
            client_id=req.client,
            operation=req.op,
            payload_fingerprint=sha256_json(safe_args),
            payload=safe_args,
        )
        if req.op == "login":
            return self._login(req)
        if req.op == "get":
            return self._get(req)
        if req.op in PROTECTED_READ_OPERATIONS:
            return self._start_protected_read(req)
        if req.op not in {
            "put", "publish_deck", "create_slide", "delete_slide", "reorder_slides", "create_element",
            "acquire_lock", "release_lock", "update_element", "delete_element",
            "renew_lock", "update_deck_settings", "update_slide_settings",
            "revoke_access_token", "grant_access", "revoke_access",
        }:
            return {"ok": True, "result": {"status": "INVALID_REQUEST", "details": {"operation": req.op}}}
        if self.engine.role != "leader":
            if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id:
                return {"redirect": self.engine.leader_slot}
            return {"ok": False, "error": "NO_LEADER"}
        # Auth-admitted commands: bearer stripped, quorum read started, actor stamped later.
        # All 12 guarded operations require a bearer at admission (Spec §3, ADR-010 §11/§12).
        # Missing _auth → REAUTH_REQUIRED pre-proposal; never falls through to engine.propose().
        if req.op in BEARER_GUARDED_OPERATIONS:
            return self._start_auth_admitted_command(req)
        try:
            if req.op == "put":
                operation = "kv_put"
                deck_id = None
                payload = {"key": req.args["key"], "value": req.args["value"]}
            elif req.op == "publish_deck":
                operation = "publish_deck"
                deck_id = req.args["presentation"]["metadata"]["deck_id"]
                payload = req.args
            else:
                operation = req.op
                deck_id = req.args["deck_id"]
                payload = {
                    key: value for key, value in req.args.items()
                    if key not in {"deck_id", "session_id"}
                }
            command = CommandEnvelopeDTO(
                command_schema_version=1,
                request_id=req.id,
                client_id=req.client,
                session_id=req.args.get("session_id") if req.op in {
                    "create_element", "acquire_lock", "release_lock", "update_element", "delete_element",
                    "renew_lock",
                } else None,
                origin_node_uuid=self.engine.node_uuid,
                deck_id=deck_id,
                operation=operation,
                payload=payload,
            )
            outcome = self.engine.propose(command)
        except (KeyError, TypeError, ValueError) as error:
            self._event("request_rejected", request_id=req.id, error_code="INVALID_REQUEST", detail=str(error))
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        if isinstance(outcome, ClientResultDTO):
            return self._immediate_result_response(command, outcome)
        if not isinstance(outcome, LogEntryDTO):
            raise RuntimeError("Raft proposal returned an unsupported result")
        key = self._request_key(command)
        self._waiters.setdefault(key, []).append(req.id)
        self._event("proposal_appended", request_id=req.id, log_index=outcome.index)
        return None

    def on_message(self, src, msg):
        self._event("rpc_received", peer_slot=src, rpc_kind=msg.get("kind") if isinstance(msg, dict) else None)
        unbound_registration = (
            isinstance(msg, dict)
            and msg.get("kind") == "register_node"
            and src not in self.engine.state.peer_bindings
        )
        try:
            self.engine.handle_message(src, msg)
        except (TypeError, ValueError) as error:
            self._event("rpc_rejected", peer_slot=src, error_code="INVALID_RPC", detail=str(error))
            return
        self._progress_auth_work()
        self._refresh_auth_catalog_status()
        self._ensure_auth_catalog_activation()
        self._ensure_auth_schema_upgrade()
        self._ensure_presentation_schema_activation()
        if unbound_registration:
            self._send_registration(src)
        self._reply_committed()
        self._progress_auth_work()

    def on_tick(self):
        previous_role = self._previous_role
        self.engine.on_tick()
        self._progress_auth_work()
        current_role = self.engine.role
        if current_role == "leader" and previous_role != "leader":
            self._event("lock_expiration_coordinator_leader_init",
                        term=self.engine.state.raft.current_term)
            self._expiration_coordinator.on_became_leader(
                fsm=self.engine.state.fsm,
                now=self.now(),
            )
        elif current_role != "leader" and previous_role == "leader":
            self._event("lock_expiration_coordinator_stepdown",
                        term=self.engine.state.raft.current_term)
            self._expiration_coordinator.on_lost_leadership()
        self._previous_role = current_role
        self._refresh_auth_catalog_status()
        self._ensure_auth_catalog_activation()
        self._ensure_auth_schema_upgrade()
        self._ensure_presentation_schema_activation()
        self._register_unbound_peers()
        self._reply_committed()
        self._reply_expiration_commits()
        self._progress_auth_work()
        if current_role == "leader":
            self._check_lock_expirations()
        self._process_operator_requests()

    # ------------------------------------------------------------------
    # Stage F local operator spool (ADR-018 §4): explicit migration trigger
    # ------------------------------------------------------------------

    OPERATOR_SPOOL_DIRECTORY = "operator"
    OPERATOR_OPERATIONS = frozenset({"migrate_legacy_deck"})

    def _process_operator_requests(self) -> None:
        """Serve explicit operator requests dropped into this replica's run directory.

        The spool is a local filesystem channel, not a public route: only the
        operator of this replica's process can write ``<run>/<slot>/operator``.
        Each ``<name>.json`` request is consumed once and answered with
        ``<name>.result.json``. Nothing here runs unless such a file exists, so
        startup, election and ticks never migrate a deck on their own.
        """
        spool = Path(self.run_dir) / self.id / self.OPERATOR_SPOOL_DIRECTORY
        if not spool.is_dir():
            return
        for request_path in sorted(spool.glob("*.json")):
            if request_path.name.endswith(".result.json"):
                continue
            name = request_path.name[: -len(".json")]
            try:
                request = json.loads(request_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                request = None
            outcome = self._operator_outcome(request)
            result_path = spool / f"{name}.result.json"
            temporary = spool / f".{name}.result.tmp"
            temporary.write_text(json.dumps(outcome, sort_keys=True), encoding="utf-8")
            os.replace(temporary, result_path)
            request_path.unlink(missing_ok=True)
            self._event("operator_request_completed", request=name, outcome=outcome.get("status"))

    def _operator_outcome(self, request: object) -> dict[str, Any]:
        if (
            not isinstance(request, Mapping)
            or set(request) != {"operation", "deck_id"}
            or request.get("operation") not in self.OPERATOR_OPERATIONS
        ):
            return {"status": "INVALID_REQUEST"}
        try:
            deck_id = self._canonical_uuid(request["deck_id"])
        except ValueError:
            return {"status": "INVALID_REQUEST"}
        return self.propose_legacy_deck_migration(deck_id)

    @property
    def auth_catalog_ready(self) -> bool:
        return self.auth_catalog_status is AuthCatalogStatus.READY

    def _refresh_auth_catalog_status(self) -> AuthCatalogStatus:
        raw = self.engine.state.fsm.get("auth")
        if raw is None:
            self.auth_catalog_status = AuthCatalogStatus.PENDING
            return self.auth_catalog_status
        try:
            stored = validate_auth_state(raw)
        except ValidationError:
            # An unparseable persisted auth blob is preserved as-is and never
            # overwritten; authentication stays refused, not crashed.
            self.auth_catalog_status = AuthCatalogStatus.FINGERPRINT_MISMATCH
            return self.auth_catalog_status
        if stored.catalog_fingerprint != self._configured_catalog_fingerprint:
            self.auth_catalog_status = AuthCatalogStatus.FINGERPRINT_MISMATCH
        elif stored.schema_version == 1:
            self.auth_catalog_status = AuthCatalogStatus.UPGRADE_PENDING
        else:
            self.auth_catalog_status = AuthCatalogStatus.READY
        return self.auth_catalog_status

    def _ensure_auth_catalog_activation(self) -> None:
        """Have only the leader replicate the deterministic Stage D migration."""
        self._ensure_auth_internal_command(
            AuthCatalogStatus.PENDING,
            self._auth_catalog_activation_command,
            "auth_catalog_activation_proposed",
            {"catalog_fingerprint": self._configured_catalog_fingerprint},
        )

    def _ensure_auth_schema_upgrade(self) -> None:
        """Commit v1-to-v2 upgrade before enabling password or bearer admission."""
        self._ensure_auth_internal_command(
            AuthCatalogStatus.UPGRADE_PENDING,
            self._auth_schema_upgrade_command,
            "auth_schema_upgrade_proposed",
        )

    def _ensure_presentation_schema_activation(self) -> None:
        if (
            not self._activation_proposal_enabled
            or self.engine.role != "leader"
            or self.auth_catalog_status is not AuthCatalogStatus.READY
            or self.engine.state.fsm.get("presentation_schema_policy") is not None
        ):
            return
        command = self._build_auth_migration_command(
            client_id=PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
            operation="activate_presentation_schema_v3",
            request_id_for_ordinal=presentation_schema_activation_request_id,
            payload={},
            failure_message="could not allocate a presentation activation identity",
        )
        key = self._request_key(command)
        was_inflight = key in self.engine.inflight_requests
        try:
            outcome = self.engine.propose(command)
        except RuntimeError:
            return
        if isinstance(outcome, LogEntryDTO) and not was_inflight:
            self._event("presentation_schema_activation_proposed", log_index=outcome.index)

    def propose_legacy_deck_migration(self, deck_id: str) -> dict[str, Any]:
        """Operator-triggered internal v2 -> v3 migration proposal (ADR-018 §4).

        Never reachable from ``on_request`` and never run at startup, election
        or tick. The leader only *names* the committed authenticated v2
        publication it found; every replica re-derives and verifies provenance
        from its own committed history at apply, so this choice is not proof.
        """
        if self.engine.role != "leader":
            return {"status": "NOT_LEADER"}
        original = self._committed_authenticated_publication(deck_id)
        if original is None:
            return {"status": "LEGACY_PROVENANCE_UNAVAILABLE"}
        command = legacy_deck_migration_command(
            deck_id=deck_id,
            original_client_id=original.client_id,
            original_request_id=original.request_id,
            owner_user_id=original.payload["authenticated_actor_user_id"],
            origin_node_uuid=self.engine.node_uuid,
        )
        try:
            outcome = self.engine.propose(command)
        except (RuntimeError, ValueError):
            return {"status": "NOT_LEADER"}
        if isinstance(outcome, ClientResultDTO):
            return {"status": outcome.status}
        self._event("legacy_deck_migration_proposed", deck_id=deck_id, log_index=outcome.index)
        return {"status": "PROPOSED", "log_index": outcome.index}

    def _committed_authenticated_publication(self, deck_id: str) -> CommandEnvelopeDTO | None:
        raft = self.engine.state.raft
        for entry in raft.log[: raft.last_applied]:
            command = entry.command
            if command.operation != "publish_deck" or str(command.deck_id) != deck_id:
                continue
            if command.payload.get("authenticated_actor_user_id") is None:
                continue
            record = self.engine.state.request_results.get(self._request_key(command))
            if (
                record is not None
                and record.committed_index == entry.index
                and record.result.status == "PUBLISHED"
            ):
                return command
        return None

    def _ensure_auth_internal_command(
        self,
        required_status: AuthCatalogStatus,
        build_command: Callable[[], CommandEnvelopeDTO],
        event_name: str,
        event_fields: dict[str, Any] | None = None,
    ) -> None:
        """Propose one idempotent auth migration command on the current leader."""
        if self.engine.role != "leader" or self.auth_catalog_status is not required_status:
            return
        command = build_command()
        request_key = self._request_key(command)
        was_inflight = request_key in self.engine.inflight_requests
        try:
            outcome = self.engine.propose(command)
        except RuntimeError:
            return
        if isinstance(outcome, LogEntryDTO) and not was_inflight:
            self._event(
                event_name,
                request_id=command.request_id,
                log_index=outcome.index,
                **(event_fields or {}),
            )
        elif isinstance(outcome, ClientResultDTO):
            self._refresh_auth_catalog_status()

    def _auth_schema_upgrade_command(self) -> CommandEnvelopeDTO:
        """Build the collision-safe v1-to-v2 migration command."""
        fingerprint = self._configured_catalog_fingerprint
        return self._build_auth_migration_command(
            client_id="_auth_schema",
            operation="upgrade_auth_schema_v2",
            request_id_for_ordinal=lambda ordinal: auth_schema_upgrade_request_id(fingerprint, ordinal),
            payload={"schema_version": 2, "catalog_fingerprint": fingerprint},
            failure_message="could not allocate an internal auth schema upgrade identity",
        )

    def _auth_catalog_activation_command(self) -> CommandEnvelopeDTO:
        """Build the collision-safe initial catalog command."""
        fingerprint = self._configured_catalog_fingerprint
        projection = catalog_projection(self._account_catalog)
        return self._build_auth_migration_command(
            client_id="_auth_catalog",
            operation="initialize_auth_catalog",
            request_id_for_ordinal=lambda ordinal: auth_catalog_activation_request_id(fingerprint, ordinal),
            payload={
                "schema_version": projection["schema_version"],
                "catalog_fingerprint": fingerprint,
                "users": projection["users"],
            },
            failure_message="could not allocate an internal catalog activation identity",
        )

    def _build_auth_migration_command(
        self,
        *,
        client_id: str,
        operation: str,
        request_id_for_ordinal: Callable[[int], str],
        payload: dict[str, Any],
        failure_message: str,
    ) -> CommandEnvelopeDTO:
        """Choose a deterministic request ID not occupied by preserved history."""
        occupied: dict[str, LogEntryDTO] = {}
        for entry in self.engine.state.raft.log:
            occupied.setdefault(self._request_key(entry.command), entry)
        candidate_count = len(self.engine.state.request_results) + len(occupied) + 1
        for ordinal in range(candidate_count):
            command = CommandEnvelopeDTO(
                command_schema_version=1,
                request_id=request_id_for_ordinal(ordinal),
                client_id=client_id,
                origin_node_uuid=self.engine.node_uuid,
                operation=operation,
                payload=payload,
            )
            key = self._request_key(command)
            if key in self.engine.state.request_results:
                continue
            previous = occupied.get(key)
            if previous is None or command_fingerprint(previous.command) == command_fingerprint(command):
                return command
        raise RuntimeError(failure_message)

    def _reply_expiration_commits(self) -> None:
        """Record the committed outcome of each internally proposed expiration."""
        for key, pending in list(self._pending_expirations.items()):
            record = self.engine.state.request_results.get(key)
            if record is None:
                continue
            self._log_expiration_committed(
                pending,
                result=record.result,
                committed_index=record.committed_index,
            )
            del self._pending_expirations[key]

    def _log_expiration_committed(
        self,
        pending: dict[str, Any],
        *,
        result: ClientResultDTO,
        committed_index: int | None = None,
    ) -> None:
        """Emit the terminal result of one internal expiration proposal."""
        self._event(
            "expire_lock_committed_expired"
            if result.status == "EXPIRED"
            else "expire_lock_committed_noop",
            deck_id=pending["deck_id"],
            element_id=pending["element_id"],
            fencing_token=pending["fencing_token"],
            lease_epoch=pending["lease_epoch"],
            request_id=pending["request_id"],
            result=result.status,
            committed_index=committed_index,
            element_version=result.version,
        )
        self._expiration_coordinator.on_expiration_committed(
            pending["deck_id"],
            pending["element_id"],
            pending["fencing_token"],
            pending["lease_epoch"],
        )

    def _check_lock_expirations(self) -> None:
        """Propose expire_lock for every element the leader observes as inactive."""
        fsm = self.engine.state.fsm
        now = self.now()
        proposals = self._expiration_coordinator.check_expirations(fsm, now)
        for proposal in proposals:
            deck_id = proposal["deck_id"]
            element_id = proposal["element_id"]
            token = proposal["expected_token"]
            epoch = proposal["expected_lease_epoch"]
            request_id = proposal["request_id"]
            self._event(
                "expire_lock_proposed",
                deck_id=deck_id,
                element_id=element_id,
                fencing_token=token,
                lease_epoch=epoch,
                request_id=request_id,
                term=self.engine.state.raft.current_term,
            )
            command = CommandEnvelopeDTO(
                command_schema_version=1,
                request_id=request_id,
                client_id="_lock_expiration",
                session_id=None,
                origin_node_uuid=self.engine.node_uuid,
                deck_id=deck_id,
                operation="expire_lock",
                payload={
                    "element_id": element_id,
                    "expected_token": token,
                    "expected_lease_epoch": epoch,
                },
            )
            pending = {
                "deck_id": deck_id,
                "element_id": element_id,
                "fencing_token": token,
                "lease_epoch": epoch,
                "request_id": request_id,
            }
            try:
                outcome = self.engine.propose(command)
                if isinstance(outcome, ClientResultDTO):
                    self._log_expiration_committed(pending, result=outcome)
                elif isinstance(outcome, LogEntryDTO):
                    self._pending_expirations[self._request_key(command)] = pending
            except (RuntimeError, ValueError):
                # Lost leadership between check and proposal; coordinator will reset on next tick.
                pass

    @staticmethod
    def _invalid_credentials_response() -> dict[str, Any]:
        return {"ok": True, "result": {"status": "INVALID_CREDENTIALS"}}

    def _leader_unavailable_response(self) -> dict[str, Any]:
        if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id:
            return {"redirect": self.engine.leader_slot}
        return {"ok": False, "error": "NO_LEADER"}

    def _login(self, req) -> dict[str, Any] | None:
        """Begin a password login without consulting stale local auth state."""

        if self.engine.role != "leader":
            return self._leader_unavailable_response()
        try:
            if not isinstance(req.args, Mapping):
                raise TypeError("invalid login payload")
            if not isinstance(req.id, str) or not 1 <= len(req.id) <= 256:
                raise ValueError("invalid request identity")
            if not isinstance(req.client, str):
                raise ValueError("invalid client identity")
            client_id = str(UUID(req.client))
            if client_id != req.client or req.args.get("client_id") != client_id:
                raise ValueError("invalid client identity")
        except (TypeError, ValueError):
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        request_key = sha256_json([client_id, req.id])
        if (request_key in self._login_read_waiters
                or any(item.request_key == request_key for item in self._password_checks.values())
                or request_key in self._pending_login_proposals):
            return {"ok": False, "error": "AUTH_BUSY"}

        email = req.args.get("email")
        password = req.args.get("password")
        if not isinstance(password, str):
            return self._invalid_credentials_response()
        try:
            password_bytes = password.encode("utf-8")
        except UnicodeError:
            return self._invalid_credentials_response()
        if len(password_bytes) > MAX_PASSWORD_BYTES:
            return self._invalid_credentials_response()
        normalized_email = email.strip().lower() if isinstance(email, str) else ""
        try:
            context = self.engine.begin_read_index(min_commit_index=0)
        except RuntimeError:
            return self._leader_unavailable_response()
        attempt_id = str(uuid4())
        captured_term = self.engine.state.raft.current_term
        self._login_read_waiters[request_key] = LoginReadWaiter(
            request_key=request_key,
            request_id=req.id,
            client_id=client_id,
            normalized_email=normalized_email,
            password_bytes=password_bytes,
            captured_term=captured_term,
            read_context=context,
            attempt_id=attempt_id,
        )
        self._event(
            "login_read_started",
            request_id=req.id,
            client_id=client_id,
            read_context=str(context),
        )
        return None

    # ------------------------------------------------------------------
    # Auth-admitted commands (update_deck_settings with bearer)
    # ------------------------------------------------------------------

    def _start_auth_admitted_command(self, req) -> dict[str, Any] | None:
        """Extract bearer, reject forbidden caller-supplied actor fields, start quorum read.

        Returns immediately with REAUTH_REQUIRED or INVALID_REQUEST when the
        pre-admission checks fail.  Returns None when a quorum read has started
        and the reply will arrive asynchronously via _progress_auth_admitted_commands.
        """
        try:
            if not isinstance(req.args, Mapping):
                raise TypeError("invalid args")
            if not isinstance(req.id, str) or not 1 <= len(req.id) <= 256:
                raise ValueError("invalid request identity")
            client_id = req.client
            if not isinstance(client_id, str):
                raise ValueError("invalid client identity")
        except (TypeError, ValueError):
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}

        # The forbidden-actor-field rejection runs once in on_request, before any
        # path (admitted or direct) can construct a command payload.

        # Extract and strip the wire-only _auth field.
        auth_field = req.args.get("_auth") if isinstance(req.args, Mapping) else None
        if not isinstance(auth_field, Mapping) or "access_token" not in auth_field:
            self._event(
                "request_rejected",
                request_id=req.id,
                error_code="REAUTH_REQUIRED",
                detail="missing or malformed _auth bearer",
            )
            return {"ok": False, "error": "REAUTH_REQUIRED"}
        access_token = auth_field["access_token"]
        if not isinstance(access_token, str) or not access_token:
            return {"ok": False, "error": "REAUTH_REQUIRED"}

        # Strip wire-only and forbidden fields from the args that will become the payload.
        stripped_args = {
            k: v for k, v in req.args.items()
            if k not in {"_auth", "authenticated_actor_user_id", "credential_id", "credential_generation"}
        }

        request_key = sha256_json([client_id, req.id])
        if request_key in self._auth_admitted_waiters:
            return {"ok": False, "error": "AUTH_BUSY"}

        try:
            context = self.engine.begin_read_index(min_commit_index=0)
        except RuntimeError:
            return self._leader_unavailable_response()

        captured_term = self.engine.state.raft.current_term
        if req.op == "publish_deck":
            presentation = req.args.get("presentation")
            metadata = presentation.get("metadata") if isinstance(presentation, Mapping) else None
            deck_id = metadata.get("deck_id") if isinstance(metadata, Mapping) else None
        else:
            deck_id = req.args.get("deck_id")
        self._auth_admitted_waiters[request_key] = AuthAdmittedCommandWaiter(
            request_key=request_key,
            request_id=req.id,
            client_id=client_id,
            access_token=access_token,
            operation=req.op,
            deck_id=str(deck_id) if deck_id is not None else None,
            stripped_args=stripped_args,
            captured_term=captured_term,
            read_context=context,
        )
        self._event(
            "auth_admitted_read_started",
            request_id=req.id,
            client_id=client_id,
            operation=req.op,
            read_context=str(context),
        )
        return None

    def _replay_disclosure_denied(
        self, waiter: AuthAdmittedCommandWaiter, actor_user_id: str, outcome: ClientResultDTO,
    ) -> bool:
        """Whether an exact-ID stored deck result must be masked for this caller now."""
        if (
            outcome.status == "REQUEST_ID_REUSE"
            or waiter.deck_id is None
            or self.engine.state.fsm.get("presentation_schema_policy") is None
            or waiter.operation not in RECEIPT_SUCCESS_STATUSES.keys() | {"grant_access", "revoke_access"}
            or self._request_key_for(waiter) not in self.engine.state.request_results
        ):
            return False
        role = KeyValueStateMachine(self.engine.state.fsm).effective_access(waiter.deck_id, actor_user_id)
        if waiter.operation in {"grant_access", "revoke_access"}:
            return role != "OWNER"
        return role not in {"OWNER", "EDIT"}

    @staticmethod
    def _request_key_for(waiter: AuthAdmittedCommandWaiter) -> str:
        return sha256_json([waiter.client_id, waiter.request_id])

    def _abort_auth_waiter(
        self, key: str, waiter: "AuthAdmittedCommandWaiter", response: dict[str, Any]
    ) -> None:
        """Clean up one admitted-command waiter and send its terminal response."""
        self.engine.finish_read_index(waiter.read_context)
        del self._auth_admitted_waiters[key]
        self.reply(waiter.request_id, response)

    def _progress_auth_admitted_commands(self) -> None:
        """Advance admitted-command state only from the serialized event-loop thread."""
        current_term = self.engine.state.raft.current_term
        for key, waiter in list(self._auth_admitted_waiters.items()):
            if not self._is_current_leader_term(waiter.captured_term):
                self._abort_auth_waiter(key, waiter, self._leader_unavailable_response())
                self._event("auth_admitted_stale", request_id=waiter.request_id)
                continue

            read = self.engine.read_index_status(waiter.read_context)
            if read.status is ReadIndexStatus.PENDING:
                continue
            if read.status is ReadIndexStatus.NOT_LEADER:
                self._abort_auth_waiter(key, waiter, self._leader_unavailable_response())
                continue
            if read.status is ReadIndexStatus.QUORUM_UNAVAILABLE:
                self._abort_auth_waiter(key, waiter, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
                continue
            if read.status is not ReadIndexStatus.READY:
                self._abort_auth_waiter(key, waiter, {"ok": False, "error": "AUTH_UNAVAILABLE"})
                continue

            # Read is READY — verify token against fresh auth state.
            self.engine.finish_read_index(waiter.read_context)
            del self._auth_admitted_waiters[key]

            self._refresh_auth_catalog_status()
            if not self.auth_catalog_ready:
                self.reply(waiter.request_id, {"ok": False, "error": "AUTH_UNAVAILABLE"})
                continue

            record = self._verified_credential(waiter.client_id, waiter.access_token)
            if record is None:
                self.reply(waiter.request_id, {"ok": False, "error": "REAUTH_REQUIRED"})
                self._event(
                    "auth_admitted_bearer_rejected",
                    request_id=waiter.request_id,
                    client_id=waiter.client_id,
                )
                continue

            if waiter.operation == "publish_deck":
                policy = self.engine.state.fsm.get("presentation_schema_policy")
                if policy is None:
                    self.reply(waiter.request_id, {"ok": False, "error": "PRESENTATION_SCHEMA_UNAVAILABLE"})
                    continue
                presentation = waiter.stripped_args.get("presentation")
                version = presentation.get("schema_version") if isinstance(presentation, Mapping) else None
                if type(version) is int and version == 2:
                    self.reply(waiter.request_id, {"ok": True, "result": {"status": "UNSUPPORTED_PRESENTATION_SCHEMA"}})
                    continue

            # Stamp non-secret actor metadata onto the command payload.
            actor_user_id = str(record.user_id)
            credential_id = str(record.credential_id)
            credential_generation = record.credential_generation

            payload = {
                k: v for k, v in waiter.stripped_args.items()
                if k not in {"deck_id", "session_id"}
            }
            payload["authenticated_actor_user_id"] = actor_user_id
            payload["credential_id"] = credential_id
            payload["credential_generation"] = credential_generation
            if waiter.operation == "revoke_access_token":
                payload["client_id"] = waiter.client_id

            try:
                command = CommandEnvelopeDTO(
                    command_schema_version=1,
                    request_id=waiter.request_id,
                    client_id=waiter.client_id,
                    session_id=waiter.stripped_args.get("session_id"),
                    origin_node_uuid=self.engine.node_uuid,
                    deck_id=waiter.deck_id,
                    operation=waiter.operation,
                    payload=payload,
                )
                outcome = self.engine.propose(command)
            except (KeyError, TypeError, ValueError, ValidationError) as error:
                self._event(
                    "request_rejected",
                    request_id=waiter.request_id,
                    error_code="INVALID_REQUEST",
                    detail=str(error),
                )
                self.reply(waiter.request_id, {"ok": True, "result": {"status": "INVALID_REQUEST"}})
                continue

            if isinstance(outcome, ClientResultDTO) and self._replay_disclosure_denied(
                waiter, actor_user_id, outcome,
            ):
                # ADR-014/spec §9: a stored deck result is disclosed only to a
                # caller whose current committed role still permits the command.
                self.reply(waiter.request_id, {"ok": True, "result": {"status": "ACCESS_DENIED"}})
                self._event("auth_admitted_replay_masked", request_id=waiter.request_id)
                continue
            if isinstance(outcome, ClientResultDTO):
                self.reply(waiter.request_id, self._immediate_result_response(command, outcome))
                self._event(
                    "auth_admitted_immediate_result",
                    request_id=waiter.request_id,
                    result=outcome.status,
                )
                continue
            if isinstance(outcome, LogEntryDTO):
                self._waiters.setdefault(self._request_key(command), []).append(waiter.request_id)
                self._event(
                    "auth_admitted_proposed",
                    request_id=waiter.request_id,
                    log_index=outcome.index,
                )
                continue
            self.reply(waiter.request_id, {"ok": False, "error": "AUTH_UNAVAILABLE"})

    def _reply_login(self, request_id: str, response: dict[str, Any], outcome: str) -> None:
        self.reply(request_id, response)
        self._event("login_completed", request_id=request_id, outcome=outcome)

    def _submit_password_check(self, waiter: LoginReadWaiter) -> None:
        auth = validate_auth_state(self.engine.state.fsm["auth"])
        user_id_value = auth.email_index.get(waiter.normalized_email)
        user_id = str(user_id_value) if user_id_value is not None else None
        stored = auth.users.get(user_id) if user_id is not None else None
        account_exists = stored is not None
        if stored is None:
            stored = self._account_catalog.users[0]
        verifier = stored.verifier.model_copy(deep=True)
        task = PasswordCheckTask(
            attempt_id=waiter.attempt_id,
            captured_term=waiter.captured_term,
            user_id=user_id if account_exists else None,
            verifier=verifier,
            password_bytes=waiter.password_bytes,
        )
        pending = PendingPasswordCheck(
            request_key=waiter.request_key,
            request_id=waiter.request_id,
            client_id=waiter.client_id,
            user_id=user_id if account_exists else None,
            account_exists=account_exists,
            captured_term=waiter.captured_term,
            attempt_id=waiter.attempt_id,
        )
        if not self._password_verifier.submit(task):
            self._reply_login(waiter.request_id, {"ok": False, "error": "AUTH_BUSY"}, "AUTH_BUSY")
            return
        self._password_checks[waiter.attempt_id] = pending
        self._event("password_check_submitted", request_id=waiter.request_id, client_id=waiter.client_id)

    def _is_current_leader_term(self, captured_term: int) -> bool:
        return (
            self.engine.role == "leader"
            and self.engine.state.raft.current_term == captured_term
        )

    def _progress_login_reads(self) -> None:
        for request_key, waiter in list(self._login_read_waiters.items()):
            read = self.engine.read_index_status(waiter.read_context)
            response: dict[str, Any] | None = None
            outcome = ""
            if (
                not self._is_current_leader_term(waiter.captured_term)
                or read.status is ReadIndexStatus.NOT_LEADER
            ):
                response, outcome = self._leader_unavailable_response(), "NOT_LEADER"
            elif read.status is ReadIndexStatus.QUORUM_UNAVAILABLE:
                response, outcome = {"ok": False, "error": "QUORUM_UNAVAILABLE"}, "QUORUM_UNAVAILABLE"
            elif read.status is ReadIndexStatus.READY:
                self.engine.finish_read_index(waiter.read_context)
                del self._login_read_waiters[request_key]
                self._refresh_auth_catalog_status()
                if not self.auth_catalog_ready:
                    self._reply_login(
                        waiter.request_id,
                        {"ok": False, "error": "AUTH_UNAVAILABLE"},
                        "AUTH_UNAVAILABLE",
                    )
                else:
                    try:
                        self._submit_password_check(waiter)
                    except (KeyError, TypeError, ValueError, ValidationError):
                        self._reply_login(
                            waiter.request_id,
                            {"ok": False, "error": "AUTH_UNAVAILABLE"},
                            "AUTH_UNAVAILABLE",
                        )
                continue
            elif read.status is ReadIndexStatus.PENDING:
                continue
            else:
                response, outcome = {"ok": False, "error": "AUTH_UNAVAILABLE"}, "AUTH_UNAVAILABLE"
            self.engine.finish_read_index(waiter.read_context)
            del self._login_read_waiters[request_key]
            self._reply_login(waiter.request_id, response, outcome)

    def _start_login_issuance(self, pending: PendingPasswordCheck) -> None:
        key = pending.request_key
        committed = self.engine.state.request_results.get(key)
        if committed is not None:
            operation = None
            if 0 < committed.committed_index <= len(self.engine.state.raft.log):
                operation = self.engine.state.raft.log[committed.committed_index - 1].command.operation
            if operation == "issue_access_token":
                self._reply_login(
                    pending.request_id,
                    {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"},
                    "LOGIN_SECRET_UNAVAILABLE",
                )
            else:
                self._reply_login(
                    pending.request_id,
                    {"ok": True, "result": {"status": "REQUEST_ID_REUSE"}},
                    "REQUEST_ID_REUSE",
                )
            return
        in_flight = self.engine.inflight_requests.get(key)
        if in_flight is not None:
            log_index = in_flight[1]
            operation = (
                self.engine.state.raft.log[log_index - 1].command.operation
                if 0 < log_index <= len(self.engine.state.raft.log)
                else None
            )
            if operation == "issue_access_token":
                response = {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"}
                outcome = "LOGIN_SECRET_UNAVAILABLE"
            else:
                response = {"ok": True, "result": {"status": "REQUEST_ID_REUSE"}}
                outcome = "REQUEST_ID_REUSE"
            self._reply_login(pending.request_id, response, outcome)
            return
        if key in self._lost_login_secret_keys:
            self._reply_login(
                pending.request_id,
                {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"},
                "LOGIN_SECRET_UNAVAILABLE",
            )
            return

        auth = validate_auth_state(self.engine.state.fsm["auth"])

        bound_user_id = auth.client_bindings.get(pending.client_id)
        if bound_user_id is not None and str(bound_user_id) != pending.user_id:
            self._reply_login(pending.request_id, self._invalid_credentials_response(), "INVALID_CREDENTIALS")
            return
        token_bytes = self._new_access_token_bytes()
        access_token = base64.urlsafe_b64encode(token_bytes).rstrip(b"=").decode("ascii")
        token_digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        credential_id = str(self._new_credential_id())
        command = CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=pending.request_id,
            client_id=pending.client_id,
            origin_node_uuid=self.engine.node_uuid,
            operation="issue_access_token",
            payload={
                "user_id": pending.user_id,
                "client_id": pending.client_id,
                "credential_id": credential_id,
                "token_digest": token_digest,
            },
        )
        proposal = PendingLoginProposal(
            request_key=key,
            request_id=pending.request_id,
            user_id=str(pending.user_id),
            client_id=pending.client_id,
            credential_id=credential_id,
            captured_term=pending.captured_term,
            deadline=self.now() + LOGIN_PROPOSAL_TIMEOUT,
            command=command,
            command_fingerprint=command_fingerprint(command),
            token_digest=token_digest,
            log_index=None,
            access_token=access_token,
        )
        self._pending_login_proposals[key] = proposal
        try:
            outcome = self.engine.propose(command)
        except (OSError, RuntimeError, TypeError, ValueError, ValidationError):
            del self._pending_login_proposals[key]
            self._lost_login_secret_keys.add(key)
            response = (
                self._leader_unavailable_response()
                if self.engine.role != "leader"
                else {"ok": False, "error": "AUTH_UNAVAILABLE"}
            )
            self._reply_login(pending.request_id, response, "PROPOSAL_UNAVAILABLE")
            return
        if isinstance(outcome, LogEntryDTO):
            self._pending_login_proposals[key] = replace(proposal, log_index=outcome.index)
            self._event("login_issuance_proposed", request_id=pending.request_id, log_index=outcome.index)
            return
        del self._pending_login_proposals[key]
        self._lost_login_secret_keys.add(key)
        self._reply_login(
            pending.request_id,
            {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"}
            if outcome.status == "TOKEN_ISSUED"
            else self._client_response(outcome),
            "LOGIN_SECRET_UNAVAILABLE" if outcome.status == "TOKEN_ISSUED" else outcome.status,
        )

    def _progress_password_checks(self) -> None:
        for result in self._password_verifier.drain_results():
            pending = self._password_checks.pop(result.attempt_id, None)
            if pending is None:
                continue
            if (
                result.captured_term != pending.captured_term
                or not self._is_current_leader_term(pending.captured_term)
            ):
                self._reply_login(pending.request_id, self._leader_unavailable_response(), "STALE_PASSWORD_CHECK")
                continue
            if not result.available:
                self._reply_login(
                    pending.request_id,
                    {"ok": False, "error": "AUTH_UNAVAILABLE"},
                    "AUTH_UNAVAILABLE",
                )
                continue
            if not result.valid or not pending.account_exists or pending.user_id is None:
                self._reply_login(pending.request_id, self._invalid_credentials_response(), "INVALID_CREDENTIALS")
                continue
            self._start_login_issuance(pending)

    def _progress_login_proposals(self) -> None:
        for key, pending in list(self._pending_login_proposals.items()):
            if not self._is_current_leader_term(pending.captured_term):
                del self._pending_login_proposals[key]
                self._lost_login_secret_keys.add(key)
                self._reply_login(pending.request_id, self._leader_unavailable_response(), "STALE_LOGIN_PROPOSAL")
                continue
            if self.now() >= pending.deadline:
                del self._pending_login_proposals[key]
                self._lost_login_secret_keys.add(key)
                self._reply_login(
                    pending.request_id,
                    {"ok": False, "error": "QUORUM_UNAVAILABLE"},
                    "LOGIN_PROPOSAL_TIMEOUT",
                )
                continue
            record = self.engine.state.request_results.get(key)
            if record is None:
                continue
            del self._pending_login_proposals[key]
            committed_command = (
                self.engine.state.raft.log[record.committed_index - 1].command
                if 0 < record.committed_index <= len(self.engine.state.raft.log)
                else None
            )
            if (pending.log_index is None
                    or record.committed_index != pending.log_index
                    or committed_command is None
                    or committed_command != pending.command
                    or command_fingerprint(committed_command) != pending.command_fingerprint):
                self._reply_login(
                    pending.request_id,
                    {"ok": False, "error": "LOGIN_SECRET_UNAVAILABLE"},
                    "LOGIN_SECRET_UNAVAILABLE",
                )
                continue
            if record.result.status != "TOKEN_ISSUED":
                self._reply_login(pending.request_id, self._client_response(record.result), record.result.status)
                continue
            safe = record.result.value or {}
            auth = validate_auth_state(self.engine.state.fsm["auth"])
            active = auth.active_credentials.get(pending.client_id)
            valid = (
                safe.get("user_id") == pending.user_id
                and safe.get("client_id") == pending.client_id
                and safe.get("credential_id") == pending.credential_id
                and active is not None
                and str(active.user_id) == pending.user_id
                and str(active.credential_id) == pending.credential_id
                and active.token_digest == pending.token_digest
                and active.credential_generation == safe.get("credential_generation")
                and active.revoked is False
            )
            if not valid:
                self._reply_login(
                    pending.request_id,
                    {"ok": False, "error": "REAUTH_REQUIRED"},
                    "REAUTH_REQUIRED",
                )
                continue
            self._reply_login(pending.request_id, {"ok": True, "result": {
                "status": "AUTHENTICATED",
                "user_id": pending.user_id,
                "client_id": pending.client_id,
                "credential_id": pending.credential_id,
                "credential_generation": active.credential_generation,
                "access_token": pending.access_token,
            }}, "AUTHENTICATED")

    def _progress_auth_work(self) -> None:
        """Advance login and bearer-admission work on the node event-loop thread."""

        for request_key, waiter in list(self._login_read_waiters.items()):
            if self._is_current_leader_term(waiter.captured_term):
                continue
            self.engine.finish_read_index(waiter.read_context)
            del self._login_read_waiters[request_key]
            self._reply_login(waiter.request_id, self._leader_unavailable_response(), "STALE_LOGIN_READ")
        for attempt_id, pending in list(self._password_checks.items()):
            if self._is_current_leader_term(pending.captured_term):
                continue
            del self._password_checks[attempt_id]
            self._reply_login(pending.request_id, self._leader_unavailable_response(), "STALE_PASSWORD_CHECK")
        for key, pending in list(self._pending_login_proposals.items()):
            if self._is_current_leader_term(pending.captured_term):
                continue
            del self._pending_login_proposals[key]
            self._lost_login_secret_keys.add(key)
            self._reply_login(pending.request_id, self._leader_unavailable_response(), "STALE_LOGIN_PROPOSAL")
        self._progress_login_reads()
        self._progress_password_checks()
        self._progress_login_proposals()
        self._progress_auth_admitted_commands()
        self._progress_protected_reads()

    def status(self):
        engine = getattr(self, "engine", None)
        if engine is None:
            return {"role": "follower", "term": 0}
        hard_state = engine.state.raft
        return {
            "role": engine.role,
            "term": hard_state.current_term,
            "leader_id": engine.leader_slot,
            "node_uuid": str(engine.node_uuid),
            "commit_index": hard_state.commit_index,
            "last_applied": hard_state.last_applied,
            "auth_catalog_status": self.auth_catalog_status.value,
            "auth_catalog_ready": self.auth_catalog_ready,
        }

    def _send_rpc(self, peer: str, message: dict[str, Any]) -> None:
        self.send(peer, message)
        self._event("rpc_sent", peer_slot=peer, rpc_kind=message.get("kind"), rpc_id=message.get("rpc_id"))

    def _get(self, req) -> dict[str, Any]:
        try:
            key = req.args["key"]
            if not isinstance(key, str) or not key:
                raise ValueError("key must be a non-empty string")
        except (KeyError, TypeError, ValueError):
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        if not req.args.get("local") and self.engine.role != "leader":
            if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id:
                return {"redirect": self.engine.leader_slot}
            return {"ok": False, "error": "NO_LEADER"}
        value = KeyValueStateMachine(self.engine.state.fsm).get_local(key)
        self._event("local_read", request_id=req.id, operation=req.op, result="OK")
        return {"ok": True, "result": value}

    # ------------------------------------------------------------------
    # Stage F protected reads (ADR-015/017): leader-bound authorized ReadIndex
    # ------------------------------------------------------------------

    @staticmethod
    def _canonical_uuid(value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("identifier must be a UUID string")
        canonical = str(UUID(value))
        if canonical != value:
            raise ValueError("identifier must be a canonical UUID string")
        return canonical

    @classmethod
    def _parse_protected_read_args(cls, operation: str, args: Mapping[str, Any]) -> dict[str, Any]:
        """Validate typed read input; the result never contains wire auth material."""
        if operation == "query_operation_receipt":
            original = args.get("original_request_id")
            if not isinstance(original, str) or not 1 <= len(original) <= 256:
                raise ValueError("original_request_id must be a nonempty request identity")
            return {"original_request_id": original}
        parsed: dict[str, Any] = {"deck_id": cls._canonical_uuid(args.get("deck_id"))}
        if operation == "resolve_registered_user":
            parsed["email"] = parse_exact_email_query(args.get("email"))
        if operation == "get_slide":
            parsed["slide_id"] = cls._canonical_uuid(args.get("slide_id"))
        if operation in {"get_element", "read_lock_status", "authoritative_refresh_element", "refresh_element"}:
            parsed["element_id"] = cls._canonical_uuid(args.get("element_id"))
        if operation in {"authoritative_refresh_element", "refresh_element"}:
            floor = args.get("min_commit_index")
            if type(floor) is not int or floor < 0:
                raise ValueError("min_commit_index must be a nonnegative integer")
            parsed["min_commit_index"] = floor
        return parsed

    def _start_protected_read(self, req) -> dict[str, Any] | None:
        """Admit one protected read to the leader's fresh ReadIndex barrier.

        Followers and leaderless replicas never consult local deck state. The
        reply is produced by ``_progress_protected_reads`` once the barrier,
        the apply fence, the bearer and the caller's role have all been checked
        against the same applied state.
        """
        if self.engine.role != "leader":
            return self._leader_unavailable_response()
        try:
            if not isinstance(req.args, Mapping):
                raise TypeError("invalid args")
            if not isinstance(req.id, str) or not 1 <= len(req.id) <= 256:
                raise ValueError("invalid request identity")
            if not isinstance(req.client, str):
                raise ValueError("invalid client identity")
            args = self._parse_protected_read_args(req.op, req.args)
        except (TypeError, ValueError):
            self._event("request_rejected", request_id=getattr(req, "id", None),
                        error_code="INVALID_REQUEST", detail="invalid protected read arguments")
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        auth_field = req.args.get("_auth")
        access_token = auth_field.get("access_token") if isinstance(auth_field, Mapping) else None
        if not isinstance(access_token, str) or not access_token:
            self._event("request_rejected", request_id=req.id,
                        error_code="REAUTH_REQUIRED", detail="missing or malformed _auth bearer")
            return {"ok": False, "error": "REAUTH_REQUIRED"}
        request_key = sha256_json([req.client, req.id])
        if request_key in self._protected_read_waiters:
            return {"ok": False, "error": "AUTH_BUSY"}
        try:
            context = self.engine.begin_read_index(min_commit_index=args.get("min_commit_index", 0))
        except RuntimeError:
            return self._leader_unavailable_response()
        self._protected_read_waiters[request_key] = ProtectedReadWaiter(
            request_key=request_key,
            request_id=req.id,
            client_id=req.client,
            access_token=access_token,
            operation=req.op,
            args=args,
            captured_term=self.engine.state.raft.current_term,
            read_context=context,
        )
        self._event("protected_read_started", request_id=req.id, client_id=req.client,
                    operation=req.op, read_context=str(context),
                    min_commit_index=args.get("min_commit_index", 0))
        return None

    def _finish_protected_read(self, waiter: ProtectedReadWaiter, response: dict[str, Any]) -> None:
        self.engine.finish_read_index(waiter.read_context)
        del self._protected_read_waiters[waiter.request_key]
        self.reply(waiter.request_id, response)
        if "redirect" in response:
            outcome = "REDIRECT"
        elif response.get("ok"):
            outcome = response["result"]["status"]
        else:
            outcome = response.get("error")
        self._event("protected_read_completed", request_id=waiter.request_id,
                    operation=waiter.operation, read_context=str(waiter.read_context),
                    outcome=outcome)

    def _progress_protected_reads(self) -> None:
        """Complete protected reads without yielding between authorization and projection."""
        for waiter in list(self._protected_read_waiters.values()):
            if not self._is_current_leader_term(waiter.captured_term):
                self._finish_protected_read(waiter, self._leader_unavailable_response())
                continue
            read = self.engine.read_index_status(waiter.read_context)
            if read.status is ReadIndexStatus.PENDING:
                continue
            if read.status is ReadIndexStatus.NOT_LEADER:
                self._finish_protected_read(waiter, self._leader_unavailable_response())
                continue
            if read.status is ReadIndexStatus.QUORUM_UNAVAILABLE:
                self._finish_protected_read(waiter, {"ok": False, "error": "QUORUM_UNAVAILABLE"})
                continue
            applied_index = self.engine.state.raft.last_applied
            if (read.status is not ReadIndexStatus.READY or read.index is None
                    or applied_index < max(read.index, waiter.args.get("min_commit_index", 0))):
                self._finish_protected_read(waiter, {"ok": False, "error": "READ_INDEX_UNAVAILABLE"})
                continue
            # The barrier is a fence, not an authorization lease: recheck readiness,
            # bearer and role against the state applied through that fence.
            self._refresh_auth_catalog_status()
            if not self.auth_catalog_ready:
                self._finish_protected_read(waiter, {"ok": False, "error": "AUTH_UNAVAILABLE"})
                continue
            record = self._verified_credential(waiter.client_id, waiter.access_token)
            if record is None:
                self._event("protected_read_bearer_rejected", request_id=waiter.request_id,
                            client_id=waiter.client_id)
                self._finish_protected_read(waiter, {"ok": False, "error": "REAUTH_REQUIRED"})
                continue
            machine = KeyValueStateMachine(self.engine.state.fsm)
            actor_user_id = str(record.user_id)
            if waiter.operation == "query_operation_receipt":
                result = self._operation_receipt(waiter.client_id, actor_user_id,
                                                 waiter.args["original_request_id"])
            elif waiter.operation == "query_grants":
                result = machine.query_grants(
                    waiter.args["deck_id"], actor_user_id=actor_user_id, applied_index=applied_index,
                )
            elif waiter.operation == "query_access_state":
                result = machine.query_access_state(
                    waiter.args["deck_id"], actor_user_id=actor_user_id, applied_index=applied_index,
                )
            elif waiter.operation == "resolve_registered_user":
                result = machine.resolve_registered_user(
                    waiter.args["deck_id"], waiter.args["email"],
                    actor_user_id=actor_user_id, applied_index=applied_index,
                )
            else:
                result = machine.protected_read(
                    waiter.operation, waiter.args, actor_user_id=actor_user_id, applied_index=applied_index,
                )
            self._finish_protected_read(waiter, {"ok": True, "result": result})

    def _operation_receipt(self, client_id: str, actor_user_id: str,
                           original_request_id: str) -> dict[str, str]:
        """Inspect retained committed evidence without projecting its private result."""
        unknown = {"status": "OUTCOME_UNKNOWN"}
        record = self.engine.state.request_results.get(sha256_json([client_id, original_request_id]))
        if record is None or not 0 < record.committed_index <= self.engine.state.raft.commit_index:
            return unknown
        if record.committed_index > len(self.engine.state.raft.log):
            return unknown
        command = self.engine.state.raft.log[record.committed_index - 1].command
        if (command.client_id != client_id or command.request_id != original_request_id
                or command.operation not in RECEIPT_SUCCESS_STATUSES
                or command.payload.get("authenticated_actor_user_id") != actor_user_id
                or record.fingerprint != command_fingerprint(command)):
            return unknown
        return {"status": (
            "COMMITTED_SUCCESS" if record.result.status in RECEIPT_SUCCESS_STATUSES[command.operation]
            else "COMMITTED_REJECTED"
        )}

    def _verified_credential(self, client_id: str, access_token: str):
        """Return the current replicated credential bound to this bearer, or None."""
        try:
            auth = validate_auth_state(self.engine.state.fsm["auth"])
            token_digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        except (KeyError, TypeError, ValueError, ValidationError, UnicodeError):
            return None
        record = auth.active_credentials.get(client_id)
        if (
            record is None
            or record.revoked
            or not hmac.compare_digest(record.token_digest, token_digest)
        ):
            return None
        return record

    def _reply_committed(self) -> None:
        for key, request_ids in list(self._waiters.items()):
            record = self.engine.state.request_results.get(key)
            if record is None:
                continue
            committed_operation = self.engine.state.raft.log[record.committed_index - 1].command.operation
            response = self._client_response(
                record.result,
                committed_index=record.committed_index if committed_operation == "acquire_lock" else None,
            )
            if committed_operation == "release_lock" and record.result.status == "RELEASED":
                command = self.engine.state.raft.log[record.committed_index - 1].command
                if command.deck_id and command.payload.get("element_id"):
                    self._expiration_coordinator.on_lock_released(
                        command.deck_id, command.payload["element_id"]
                    )
            for request_id in request_ids:
                self.reply(request_id, response)
                self._event("client_replied", request_id=request_id, result=record.result.status)
            del self._waiters[key]

    @staticmethod
    def _request_key(command: CommandEnvelopeDTO) -> str:
        return sha256_json([command.client_id, command.request_id])

    def _immediate_result_response(
        self, command: CommandEnvelopeDTO, outcome: ClientResultDTO,
    ) -> dict[str, Any]:
        """Reply for a proposal answered without a new entry, e.g. a stored-result replay."""
        record = self.engine.state.request_results.get(self._request_key(command))
        return self._client_response(
            outcome,
            committed_index=(
                record.committed_index
                if record is not None and command.operation == "acquire_lock"
                else None
            ),
        )

    @staticmethod
    def _client_response(
        result: ClientResultDTO, *, committed_index: int | None = None,
    ) -> dict[str, Any]:
        if result.status == "OK":
            response = {"ok": True, "result": result.value}
        else:
            response = {"ok": True, "result": result.model_dump(mode="json")}
        if committed_index is not None:
            response["committed_index"] = committed_index
        return response

    def _event(self, event: str, **context: Any) -> None:
        record: dict[str, Any] = {
            "timestamp": time.time(),
            "event": event,
            "node_slot": self.id,
        }
        engine = getattr(self, "engine", None)
        if engine is not None:
            record.update(node_uuid=str(engine.node_uuid), term=engine.state.raft.current_term, role=engine.role)
        for key, value in context.items():
            if value is None or (key == "payload" and self._log_mode != "DEBUG"):
                continue
            record[key] = value
        safe_record = redact_auth_data(record)
        self._events.write(json.dumps(safe_record, sort_keys=True, separators=(",", ":"), default=str) + "\n")
