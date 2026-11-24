"""Stage A.1 adapter between the unchanged starter callbacks and Raft."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import time
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from command_dto import ClientResultDTO, CommandEnvelopeDTO
from dto_base import sha256_json
from lock_expiration import LockExpirationCoordinator
from node import Node
from persistence import NodeDiskPersistence
from raft_core import RaftEngine, ReadIndexStatus
from raft_dto import LogEntryDTO, RegisterNodeDTO
from state_machine import KeyValueStateMachine


@dataclass(frozen=True)
class AuthoritativeRefreshWaiter:
    """A leader read fenced by one quorum-confirmed Raft ReadIndex round."""

    deck_id: str
    element_id: str
    min_commit_index: int
    term: int
    read_context: UUID


class App(Node):
    def start(self):
        mode = os.environ.get("OPERATIONAL_LOG_MODE", "OPERATIONAL").upper()
        if mode not in {"OPERATIONAL", "DEBUG"}:
            raise ValueError("OPERATIONAL_LOG_MODE must be OPERATIONAL or DEBUG")
        self._log_mode = mode
        self._events = open(Path(self.run_dir) / self.id / "events.log", "a", buffering=1)
        self._storage = NodeDiskPersistence(self.disk, self.save)
        restored = self._storage.load()
        node_uuid = restored.node_uuid if restored is not None else uuid4()
        self._waiters: dict[str, list[str]] = {}
        self._authoritative_refresh_waiters: dict[str, AuthoritativeRefreshWaiter] = {}
        self._pending_expirations: dict[str, dict[str, Any]] = {}
        self.engine = RaftEngine(
            node_slot=self.id,
            voters=self.nodes,
            node_uuid=node_uuid,
            storage=self._storage,
            send=self._send_rpc,
            clock=self.now,
        )
        self._event("startup", mode=self._log_mode, incarnation=self.engine.state.incarnation)
        self._next_registration = 0.0
        self._previous_role: str = "follower"
        self._expiration_coordinator = LockExpirationCoordinator()
        self._register_unbound_peers()

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
        self._event(
            "request_received",
            request_id=req.id,
            client_id=req.client,
            operation=req.op,
            payload_fingerprint=sha256_json(req.args),
            payload=req.args,
        )
        if req.op == "get":
            return self._get(req)
        if req.op in {"get_deck_preview", "get_slide", "get_element", "read_lock_status"}:
            return self._presentation_read(req)
        if req.op in {"authoritative_refresh_element", "refresh_element"}:
            return self._authoritative_element_refresh(req)
        if req.op not in {
            "put", "publish_deck", "create_slide", "delete_slide", "reorder_slides", "create_element",
            "acquire_lock", "release_lock", "update_element", "delete_element",
            "renew_lock",
        }:
            return {"ok": True, "result": {"status": "INVALID_REQUEST", "details": {"operation": req.op}}}
        if self.engine.role != "leader":
            if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id:
                return {"redirect": self.engine.leader_slot}
            return {"ok": False, "error": "NO_LEADER"}
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
            record = self.engine.state.request_results.get(self._request_key(command))
            return self._client_response(
                outcome,
                committed_index=(
                    record.committed_index
                    if record is not None and command.operation == "acquire_lock"
                    else None
                ),
            )
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
        if unbound_registration:
            self._send_registration(src)
        self._reply_committed()
        self._reply_authoritative_refreshes()

    def on_tick(self):
        previous_role = self._previous_role
        self.engine.on_tick()
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
        self._register_unbound_peers()
        self._reply_committed()
        self._reply_expiration_commits()
        self._reply_authoritative_refreshes()
        if current_role == "leader":
            self._check_lock_expirations()

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

    def _presentation_read(self, req) -> dict[str, Any]:
        """Serve replica-local observations without a leader round trip."""
        try:
            deck_id = req.args["deck_id"]
            if not isinstance(deck_id, str):
                raise ValueError("deck_id must be a string UUID")
            machine = KeyValueStateMachine(self.engine.state.fsm)
            applied_index = self.engine.state.raft.last_applied
            if req.op == "get_deck_preview":
                result = machine.get_deck_preview(deck_id, applied_index=applied_index)
            elif req.op == "get_slide":
                result = machine.get_slide(deck_id, req.args["slide_id"], applied_index=applied_index)
            elif req.op == "get_element":
                result = machine.get_element(deck_id, req.args["element_id"], applied_index=applied_index)
            else:
                result = machine.read_lock_status(deck_id, req.args["element_id"], applied_index=applied_index)
        except (KeyError, TypeError, ValueError):
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        self._event("local_read", request_id=req.id, operation=req.op, result=result["status"])
        return {"ok": True, "result": result}

    def _authoritative_element_refresh(self, req) -> dict[str, Any] | None:
        """Begin a ReadIndex quorum barrier; a local leader hint is insufficient."""
        if self.engine.role != "leader":
            if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id:
                return {"redirect": self.engine.leader_slot}
            return {"ok": False, "error": "NO_LEADER"}
        try:
            deck_id, element_id = req.args["deck_id"], req.args["element_id"]
            min_commit_index = req.args["min_commit_index"]
            if (not isinstance(deck_id, str) or not isinstance(element_id, str)
                    or type(min_commit_index) is not int or min_commit_index < 0):
                raise ValueError("invalid authoritative refresh arguments")
        except (KeyError, TypeError, ValueError):
            return {"ok": True, "result": {"status": "INVALID_REQUEST"}}
        context = self.engine.begin_read_index(min_commit_index=min_commit_index)
        self._authoritative_refresh_waiters[req.id] = AuthoritativeRefreshWaiter(
            deck_id=deck_id, element_id=element_id,
            min_commit_index=min_commit_index,
            term=self.engine.state.raft.current_term, read_context=context,
        )
        self._event("read_index_started", request_id=req.id, read_context=str(context),
                    min_commit_index=min_commit_index)
        return None

    def _authoritative_refresh_response(
        self, request_id: str, operation: str, deck_id: str, element_id: str,
        *, read_index: int,
    ) -> dict[str, Any]:
        applied_index = self.engine.state.raft.last_applied
        if applied_index < read_index:
            raise RuntimeError("ReadIndex response attempted before state-machine application")
        result = KeyValueStateMachine(self.engine.state.fsm).get_element(
            deck_id, element_id, applied_index=applied_index,
        )
        self._event("authoritative_refresh", request_id=request_id, operation=operation,
                    read_index=read_index, result=result["status"])
        return {"ok": True, "result": result}

    def _reply_authoritative_refreshes(self) -> None:
        """Reply only after a current-term quorum heartbeat and apply fence."""
        for request_id, waiter in list(self._authoritative_refresh_waiters.items()):
            read = self.engine.read_index_status(waiter.read_context)
            if (self.engine.role != "leader"
                    or self.engine.state.raft.current_term != waiter.term
                    or read.status is ReadIndexStatus.NOT_LEADER):
                response = (
                    {"redirect": self.engine.leader_slot}
                    if self.engine.leader_slot in self.nodes and self.engine.leader_slot != self.id
                    else {"ok": False, "error": "NO_LEADER"}
                )
            elif read.status is ReadIndexStatus.QUORUM_UNAVAILABLE:
                response = {"ok": False, "error": "QUORUM_UNAVAILABLE"}
            elif read.status is ReadIndexStatus.READY and read.index is not None:
                response = self._authoritative_refresh_response(
                    request_id, "authoritative_refresh_element",
                    waiter.deck_id, waiter.element_id,
                    read_index=max(read.index, waiter.min_commit_index),
                )
            elif read.status is ReadIndexStatus.PENDING:
                continue
            else:
                response = {"ok": False, "error": "READ_INDEX_UNAVAILABLE"}
            self.reply(request_id, response)
            self._event("read_index_completed", request_id=request_id, read_context=str(waiter.read_context),
                        outcome="OK" if response.get("ok") else response.get("error"))
            self.engine.finish_read_index(waiter.read_context)
            del self._authoritative_refresh_waiters[request_id]

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
        self._events.write(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str) + "\n")
