"""Shared in-process Raft helpers for Stage E admission tests."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Mapping
from queue import Empty
from uuid import uuid4

from auth_catalog import load_account_catalog
from command_dto import CommandEnvelopeDTO
from node import Request


def actor_stamp(user_id: str, credential_id: str, generation: int = 1) -> dict[str, object]:
    """Server-stamped actor fields that admission adds to an authenticated command."""
    return {
        "authenticated_actor_user_id": user_id,
        "credential_id": credential_id,
        "credential_generation": generation,
    }


def issue_token_command(
    user_id: str,
    client_id: str,
    credential_id: str,
    digest: str,
    *,
    request_id: str = "login-1",
) -> CommandEnvelopeDTO:
    """Build the common internal issue-token command used by FSM tests."""
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "operation": "issue_access_token",
        "payload": {
            "user_id": user_id,
            "client_id": client_id,
            "credential_id": credential_id,
            "token_digest": digest,
        },
    })


def publish_empty_deck_command(
    deck_id: object,
    slide_id: object | None = None,
    *,
    client_id: str = "client-a",
    request_id: str = "pub-1",
    actor: Mapping[str, object] | None = None,
) -> CommandEnvelopeDTO:
    """Build the shared one-slide, zero-element publication fixture."""
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": str(deck_id),
                    "name": "Test Deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [{
                    "metadata": {
                        "slide_id": str(slide_id or uuid4()),
                        "background": "#112233FF",
                    },
                    "elements": [],
                }],
            },
            **(actor or {}),
        },
    })


class SimulatedRaftHarnessMixin:
    """Drive a single App replica with simulated n2 quorum responses."""

    @staticmethod
    def _messages(replica) -> list[tuple[str, dict]]:
        sent = []
        while True:
            try:
                sent.append(replica._out.get_nowait())
            except Empty:
                return sent

    def _elect_and_commit_noop(self, replica) -> None:
        self._messages(replica)
        for slot in ("n2", "n3"):
            replica.on_message(slot, {
                "kind": "register_node", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": slot,
                "sender_uuid": str(uuid4()), "term": 0, "incarnation": 1,
                "membership_fingerprint": replica.engine.membership_fingerprint(),
            })
        self._messages(replica)
        replica.engine.start_election()
        vote = next(
            message for destination, message in self._messages(replica)
            if destination == "n2" and message["kind"] == "request_vote"
        )
        replica.on_message("n2", {
            "kind": "request_vote_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": vote["rpc_id"], "vote_granted": True,
        })
        append = next(
            message for destination, message in self._messages(replica)
            if destination == "n2" and message["kind"] == "append_entries"
            and message["entries"]
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self._messages(replica)

    def _commit_command(self, replica, command) -> None:
        self._messages(replica)
        outcome = replica.engine.propose(command)
        if not hasattr(outcome, "index"):
            return
        append = next(
            message for destination, message in self._messages(replica)
            if destination == "n2" and message.get("kind") == "append_entries"
            and message.get("entries")
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": append["rpc_id"], "success": True,
            "match_index": append["entries"][-1]["index"],
        })
        self._messages(replica)

    def _acknowledge_read(self, replica) -> None:
        probe = next(
            message for destination, message in self._messages(replica)
            if destination == "n2" and message["kind"] == "append_entries"
            and message.get("read_context") is not None
        )
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1, "reply_to": probe["rpc_id"], "success": True,
            "match_index": probe["prev_log_index"],
            "read_context": probe["read_context"],
        })

    def _provision_credential(self, replica) -> tuple[str, str]:
        """Commit a fresh client credential for the first catalog user; return (client_id, token)."""
        user_id = str(load_account_catalog().users[0].profile.user_id)
        client_id, credential_id = str(uuid4()), str(uuid4())
        access_token = "harness-bearer-" + str(uuid4())
        digest = hashlib.sha256(access_token.encode("ascii")).hexdigest()
        self._commit_command(replica, issue_token_command(
            user_id, client_id, credential_id, digest, request_id="harness-issue-" + client_id,
        ))
        return client_id, access_token

    def _commit_admitted_request(self, replica, request: Request, access_token: str) -> dict:
        """Drive one guarded request through bearer admission, the quorum read, and commit."""
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        self._messages(replica)
        immediate = replica.on_request(Request(
            request.id, request.op, {**request.args, "_auth": {"access_token": access_token}},
            request.client,
        ))
        if immediate is not None:
            return immediate
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        if not done.is_set():
            append = next(
                message for destination, message in self._messages(replica)
                if destination == "n2" and message.get("kind") == "append_entries"
                and any(entry["command"]["request_id"] == request.id for entry in message["entries"])
            )
            replica.on_message("n2", {
                "kind": "append_entries_response", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1, "reply_to": append["rpc_id"], "success": True,
                "match_index": append["entries"][-1]["index"],
            })
        if not done.is_set():
            raise AssertionError(f"admitted request {request.id} produced no reply")
        return response
