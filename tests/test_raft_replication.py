"""Public component seams for AppendEntries, commitment, and deduplication."""

from __future__ import annotations

import unittest
from typing import Any
from uuid import uuid4

from command_dto import ClientResultDTO, CommandEnvelopeDTO
from persistence import InMemoryPersistence
from raft_core import RaftEngine


class ReplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ids = {slot: uuid4() for slot in ("n1", "n2", "n3")}
        self.sent: list[tuple[str, dict[str, Any]]] = []

    def engine(self, storage: InMemoryPersistence | None = None) -> RaftEngine:
        return RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=self.ids["n1"],
            storage=storage or InMemoryPersistence(),
            send=lambda destination, message: self.sent.append((destination, message)),
            clock=lambda: 10.0,
            election_timeout=lambda: 1.0,
        )

    def register(self, engine: RaftEngine, slot: str) -> None:
        engine.handle_message(
            slot,
            {
                "kind": "register_node",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": slot,
                "sender_uuid": str(self.ids[slot]),
                "term": 0,
                "incarnation": 1,
                "membership_fingerprint": engine.membership_fingerprint(),
            },
        )

    def command(self, request_id: str, value: str) -> CommandEnvelopeDTO:
        return CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=request_id,
            client_id="client-1",
            origin_node_uuid=self.ids["n1"],
            operation="kv_put",
            payload={"key": "deck", "value": value},
        )

    def entry(self, index: int, term: int, value: str) -> dict[str, Any]:
        return {
            "index": index,
            "term": term,
            "command": self.command(f"request-{index}-{term}", value).model_dump(mode="json"),
        }

    def append(self, term: int, prev_index: int, prev_term: int, entries: list[dict[str, Any]], commit: int = 0) -> dict[str, Any]:
        return {
            "kind": "append_entries",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n2",
            "sender_uuid": str(self.ids["n2"]),
            "term": term,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": commit,
        }

    def test_follower_repairs_only_conflicting_uncommitted_suffix_and_heartbeat_keeps_extra_suffix(self) -> None:
        engine = self.engine()
        self.register(engine, "n2")
        engine.handle_message("n2", self.append(1, 0, 0, [self.entry(1, 1, "old"), self.entry(2, 1, "stale")]))
        engine.handle_message("n2", self.append(2, 1, 1, [self.entry(2, 2, "new")]))

        self.assertEqual([entry.term for entry in engine.state.raft.log], [1, 2])
        self.assertEqual(engine.state.raft.log[-1].command.payload["value"], "new")

        engine.handle_message("n2", self.append(2, 1, 1, []))
        self.assertEqual(len(engine.state.raft.log), 2)
        self.assertEqual(engine.state.raft.log[-1].command.payload["value"], "new")

    def test_follower_never_overwrites_committed_prefix(self) -> None:
        engine = self.engine()
        self.register(engine, "n2")
        engine.handle_message("n2", self.append(1, 0, 0, [self.entry(1, 1, "committed")], commit=1))

        with self.assertRaises(ValueError):
            engine.handle_message("n2", self.append(2, 0, 0, [self.entry(1, 2, "rewrite")], commit=1))
        self.assertEqual(engine.state.raft.log[0].command.payload["value"], "committed")
        self.assertEqual(engine.state.raft.commit_index, 1)

    def test_current_term_quorum_commit_applies_once_and_returns_deterministic_retries(self) -> None:
        storage = InMemoryPersistence()
        engine = self.engine(storage)
        self.register(engine, "n2")
        self.register(engine, "n3")
        engine.start_election()
        vote = next(message for destination, message in self.sent if destination == "n2" and message["kind"] == "request_vote")
        engine.handle_message(
            "n2",
            {
                "kind": "request_vote_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(self.ids["n2"]),
                "term": 1,
                "reply_to": vote["rpc_id"],
                "vote_granted": True,
            },
        )
        self.sent.clear()
        command = self.command("request-commit", "published")
        proposed = engine.propose(command)
        self.assertEqual(proposed.index, 2)
        append = next(message for destination, message in self.sent if destination == "n2" and message["kind"] == "append_entries")

        engine.handle_message(
            "n2",
            {
                "kind": "append_entries_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(self.ids["n2"]),
                "term": 1,
                "reply_to": str(uuid4()),
                "success": True,
                "match_index": 2,
            },
        )
        self.assertEqual(engine.state.raft.commit_index, 0)

        engine.handle_message(
            "n2",
            {
                "kind": "append_entries_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(self.ids["n2"]),
                "term": 1,
                "reply_to": append["rpc_id"],
                "success": True,
                "match_index": 2,
            },
        )

        self.assertEqual(engine.state.raft.commit_index, 2)
        self.assertEqual(engine.state.raft.last_applied, 2)
        self.assertEqual(engine.state.fsm, {"kv": {"deck": "published"}})
        self.assertEqual(storage.load().fsm, {"kv": {"deck": "published"}})

        restarted = self.engine(storage)
        self.assertEqual(restarted.state.fsm, {"kv": {"deck": "published"}})
        self.assertEqual(restarted.state.raft.last_applied, 2)

        self.assertIsInstance(engine.propose(command), ClientResultDTO)
        self.assertEqual(len(engine.state.raft.log), 2)
        reused = engine.propose(self.command("request-commit", "changed"))
        self.assertEqual(reused.status, "REQUEST_ID_REUSE")
        self.assertEqual(engine.state.fsm, {"kv": {"deck": "published"}})


if __name__ == "__main__":
    unittest.main()
