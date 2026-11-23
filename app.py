"""Stage A.1 adapter between the unchanged starter callbacks and Raft."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from command_dto import ClientResultDTO, CommandEnvelopeDTO
from dto_base import sha256_json
from node import Node
from persistence import NodeDiskPersistence
from raft_core import RaftEngine
from raft_dto import LogEntryDTO, RegisterNodeDTO
from state_machine import KeyValueStateMachine


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
        if req.op not in {"put", "publish_deck", "create_slide", "delete_slide", "reorder_slides"}:
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
                payload = {key: value for key, value in req.args.items() if key != "deck_id"}
            command = CommandEnvelopeDTO(
                command_schema_version=1,
                request_id=req.id,
                client_id=req.client,
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
            return self._client_response(outcome)
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

    def on_tick(self):
        self.engine.on_tick()
        self._register_unbound_peers()
        self._reply_committed()

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

    def _reply_committed(self) -> None:
        for key, request_ids in list(self._waiters.items()):
            record = self.engine.state.request_results.get(key)
            if record is None:
                continue
            response = self._client_response(record.result)
            for request_id in request_ids:
                self.reply(request_id, response)
                self._event("client_replied", request_id=request_id, result=record.result.status)
            del self._waiters[key]

    @staticmethod
    def _request_key(command: CommandEnvelopeDTO) -> str:
        return sha256_json([command.client_id, command.request_id])

    @staticmethod
    def _client_response(result: ClientResultDTO) -> dict[str, Any]:
        if result.status == "OK":
            return {"ok": True, "result": result.value}
        return {"ok": True, "result": result.model_dump(mode="json")}

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
