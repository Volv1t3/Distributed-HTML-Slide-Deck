"""Protocol-boundary tests for durable fixed-voter identity and elections."""

from __future__ import annotations

import unittest
from typing import Any
from uuid import UUID, uuid4

from persistence import InMemoryPersistence
from raft_core import RaftEngine


class ElectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.node_ids = {slot: uuid4() for slot in ("n1", "n2", "n3")}
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.storage = InMemoryPersistence()

    def engine(self) -> RaftEngine:
        return RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=self.node_ids["n1"],
            storage=self.storage,
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
                "sender_uuid": str(self.node_ids[slot]),
                "term": 0,
                "incarnation": 1,
                "membership_fingerprint": engine.membership_fingerprint(),
            },
        )

    def test_fresh_identity_is_persisted_and_ordinary_restart_keeps_vote_state(self) -> None:
        first = self.engine()
        self.assertEqual(self.storage.load().node_uuid, self.node_ids["n1"])
        self.assertEqual(first.role, "follower")

        restarted = self.engine()
        self.assertEqual(restarted.state.node_uuid, self.node_ids["n1"])
        self.assertEqual(restarted.state.incarnation, 2)
        self.assertEqual(restarted.role, "follower")

    def test_registration_binds_only_known_slot_and_rejects_changed_uuid(self) -> None:
        engine = self.engine()
        self.register(engine, "n2")
        original = engine.state.peer_bindings["n2"]

        with self.assertRaises(ValueError):
            engine.handle_message(
                "n2",
                {
                    "kind": "register_node",
                    "protocol_version": 1,
                    "rpc_id": str(uuid4()),
                    "sender_slot": "n2",
                    "sender_uuid": str(uuid4()),
                    "term": 0,
                    "incarnation": 2,
                    "membership_fingerprint": engine.membership_fingerprint(),
                },
            )
        self.assertEqual(engine.state.peer_bindings["n2"], original)
        self.assertEqual(engine.voters, frozenset({"n1", "n2", "n3"}))

    def test_vote_is_durable_once_per_term_and_requires_an_up_to_date_log(self) -> None:
        engine = self.engine()
        self.register(engine, "n2")
        self.register(engine, "n3")
        request = {
            "kind": "request_vote",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n2",
            "sender_uuid": str(self.node_ids["n2"]),
            "term": 2,
            "candidate_last_log_index": 0,
            "candidate_last_log_term": 0,
        }

        engine.handle_message("n2", request)
        self.assertEqual(engine.state.raft.voted_for, "n2")
        self.assertEqual(self.storage.load().raft.voted_for, "n2")
        self.assertTrue(self.sent[-1][1]["vote_granted"])

        request.update(
            rpc_id=str(uuid4()),
            sender_slot="n3",
            sender_uuid=str(self.node_ids["n3"]),
        )
        engine.handle_message("n3", request)
        self.assertFalse(self.sent[-1][1]["vote_granted"])

    def test_only_correlated_unique_votes_can_elect_and_leader_appends_current_term_noop(self) -> None:
        engine = self.engine()
        self.register(engine, "n2")
        self.register(engine, "n3")

        engine.start_election()
        n2_vote = next(message for destination, message in self.sent if destination == "n2")

        forged_peer = {
            "kind": "request_vote_response",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n3",
            "sender_uuid": str(self.node_ids["n3"]),
            "term": 1,
            "reply_to": n2_vote["rpc_id"],
            "vote_granted": True,
        }
        engine.handle_message("n3", forged_peer)
        self.assertEqual(engine.role, "candidate")

        valid_vote = {**forged_peer, "sender_slot": "n2", "sender_uuid": str(self.node_ids["n2"])}
        engine.handle_message("n2", valid_vote)
        self.assertEqual(engine.role, "leader")
        self.assertEqual(engine.state.raft.log[-1].command.operation, "raft.noop")
        self.assertEqual(engine.state.raft.log[-1].term, engine.state.raft.current_term)


if __name__ == "__main__":
    unittest.main()
