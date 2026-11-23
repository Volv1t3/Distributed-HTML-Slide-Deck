"""Raft ReadIndex safety/liveness: current-term proof, fresh quorum, apply fence."""

from __future__ import annotations

import unittest
from uuid import UUID, uuid4

from pydantic import ValidationError

from persistence import InMemoryPersistence
from raft_core import RaftEngine, ReadIndexResult, ReadIndexStatus
from raft_dto import AppendEntriesDTO, AppendEntriesResponseDTO


class ReadIndexProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ids = {slot: uuid4() for slot in ("n1", "n2", "n3")}
        self.now = 10.0
        self.sent = []
        self.raft = RaftEngine(
            node_slot="n1", voters=self.ids.keys(), node_uuid=self.ids["n1"],
            storage=InMemoryPersistence(),
            send=lambda dst, msg: self.sent.append((dst, msg)),
            clock=lambda: self.now, election_timeout=lambda: 1.0,
            heartbeat_interval=0.15, read_index_timeout=0.6,
        )
        for peer in ("n2", "n3"):
            self.raft.handle_message(peer, {
                "kind": "register_node", "protocol_version": 1,
                "rpc_id": str(uuid4()), "sender_slot": peer,
                "sender_uuid": str(self.ids[peer]), "term": 0,
                "incarnation": 1,
                "membership_fingerprint": self.raft.membership_fingerprint(),
            })

    def messages(self, peer="n2", kind="append_entries", *, read=None):
        messages = [msg for dst, msg in self.sent if dst == peer and msg["kind"] == kind]
        if read is not None:
            messages = [msg for msg in messages if bool(msg.get("read_context")) == read]
        return messages

    def ack(self, msg, *, peer="n2", success=True, ctx="same", term=None, reply_to=None):
        if ctx == "same":
            ctx = msg.get("read_context")
        self.raft.handle_message(peer, {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": peer,
            "sender_uuid": str(self.ids[peer]),
            "term": msg["term"] if term is None else term,
            "reply_to": msg["rpc_id"] if reply_to is None else reply_to,
            "success": success,
            "match_index": msg["prev_log_index"] + len(msg["entries"]) if success else 0,
            "read_context": ctx,
            **({"conflict_index": 1} if not success else {}),
        })

    def elected(self, *, commit_noop=True):
        self.raft.start_election()
        vote = self.messages(kind="request_vote")[-1]
        self.raft.handle_message("n2", {
            "kind": "request_vote_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2", "sender_uuid": str(self.ids["n2"]),
            "term": 1, "reply_to": vote["rpc_id"], "vote_granted": True,
        })
        self.assertEqual(self.raft.role, "leader")
        if commit_noop:
            self.ack(self.messages(read=False)[-1])
            self.assertEqual(self.raft.state.raft.commit_index, 1)
        self.sent.clear()

    def test_current_term_entry_must_commit_before_read_probe_can_start(self):
        self.elected(commit_noop=False)
        ctx = self.raft.begin_read_index()
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.PENDING))
        self.assertFalse(self.messages(read=True))
        # The uncommitted election no-op is pending. A replication ACK
        # enables the first ReadIndex probe, but cannot satisfy that probe.
        self.raft._broadcast_append_entries()
        self.ack(self.messages(read=False)[-1])
        self.assertTrue(self.messages(read=True))
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.PENDING))
        self.ack(self.messages(read=True)[-1])
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.READY, 1))
        self.assertEqual(len(self.raft.state.raft.log), 1, "ReadIndex never appends its own no-op")

    def test_old_normal_heartbeat_and_forged_context_cannot_satisfy_read(self):
        self.elected()
        ctx = self.raft.begin_read_index()
        probe = self.messages(read=True)[-1]
        normal = [m for m in self.messages(read=False)]
        if not normal:
            self.raft._broadcast_append_entries()
            normal = self.messages(read=False)
        self.ack(normal[-1])
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.PENDING)
        self.ack(probe, ctx=str(uuid4()))
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.PENDING)
        self.ack(probe, reply_to=str(uuid4()))
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.PENDING)
        self.ack(probe)
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.READY)
        self.raft.finish_read_index(ctx)
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.UNKNOWN)

    def test_log_reject_response_confirms_term_but_does_not_touch_replication_state(self):
        self.elected()
        before = self.raft.match_index.get("n2"), self.raft.next_index.get("n2")
        ctx = self.raft.begin_read_index()
        self.ack(self.messages(read=True)[-1], success=False)
        self.assertIs(self.raft.read_index_status(ctx).status, ReadIndexStatus.READY)
        self.assertEqual((self.raft.match_index.get("n2"), self.raft.next_index.get("n2")), before)

    def test_isolated_leader_cannot_read_and_timeout_is_not_success(self):
        self.elected()
        ctx = self.raft.begin_read_index(min_commit_index=1)
        self.now += 0.8
        self.raft.on_tick()
        self.assertEqual(
            self.raft.read_index_status(ctx),
            ReadIndexResult(ReadIndexStatus.QUORUM_UNAVAILABLE),
        )
        self.ack(self.messages(read=True)[-1])
        self.assertEqual(
            self.raft.read_index_status(ctx),
            ReadIndexResult(ReadIndexStatus.QUORUM_UNAVAILABLE),
        )
        self.raft.finish_read_index(ctx)

    def test_new_term_revokes_outstanding_read_even_if_old_quorum_ack_arrives(self):
        self.elected()
        ctx = self.raft.begin_read_index()
        probe = self.messages(read=True)[-1]
        self.raft.handle_message("n2", {
            "kind": "request_vote", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2", "sender_uuid": str(self.ids["n2"]),
            "term": 2, "candidate_last_log_index": 1, "candidate_last_log_term": 1,
        })
        self.assertEqual(self.raft.role, "follower")
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.NOT_LEADER))
        self.ack(probe)
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.NOT_LEADER))

    def test_min_commit_index_adds_applied_fence_beyond_quorum_ack(self):
        self.elected()
        ctx = self.raft.begin_read_index(min_commit_index=9)
        self.ack(self.messages(read=True)[-1])
        self.assertEqual(self.raft.read_index_status(ctx), ReadIndexResult(ReadIndexStatus.PENDING))

    def test_read_probe_does_not_commit_unverified_divergent_suffix(self):
        # n1 is a follower with index 1 committed and index 2 uncommitted.
        # A stale leader sending an empty heartbeat at prev=1 and reporting
        # leader_commit=2 must NOT make the unverified index 2 committed.
        from command_dto import CommandEnvelopeDTO
        from raft_dto import LogEntryDTO

        def entry(index):
            return LogEntryDTO(
                index=index, term=1,
                command=CommandEnvelopeDTO(
                    command_schema_version=1, request_id=f"r-{index}",
                    client_id="client", origin_node_uuid=self.ids["n2"],
                    operation="kv_put", payload={"key": "v", "value": str(index)},
                ),
            ).model_dump(mode="json")

        self.raft.handle_message("n2", {
            "kind": "append_entries", "protocol_version": 1, "rpc_id": str(uuid4()),
            "sender_slot": "n2", "sender_uuid": str(self.ids["n2"]), "term": 1,
            "prev_log_index": 0, "prev_log_term": 0, "entries": [entry(1), entry(2)],
            "leader_commit": 1,
        })
        self.assertEqual(self.raft.state.raft.commit_index, 1)
        context = uuid4()
        self.raft.handle_message("n2", {
            "kind": "append_entries", "protocol_version": 1, "rpc_id": str(uuid4()),
            "sender_slot": "n2", "sender_uuid": str(self.ids["n2"]), "term": 1,
            "prev_log_index": 1, "prev_log_term": 1, "entries": [],
            "leader_commit": 2, "read_context": str(context),
        })
        self.assertEqual(self.raft.state.raft.commit_index, 1)
        self.assertEqual(self.raft.state.raft.last_applied, 1)
        self.assertEqual(self.sent[-1][1]["read_context"], str(context))
        # A delayed, shorter heartbeat must not LOWER an already advanced
        # commit index, even if it reports a greater leader_commit.
        self.raft.handle_message("n2", {
            "kind": "append_entries", "protocol_version": 1, "rpc_id": str(uuid4()),
            "sender_slot": "n2", "sender_uuid": str(self.ids["n2"]), "term": 1,
            "prev_log_index": 0, "prev_log_term": 0, "entries": [],
            "leader_commit": 2, "read_context": str(uuid4()),
        })
        self.assertEqual(self.raft.state.raft.commit_index, 1)

    def test_authoritative_read_is_declared_linearizable_in_reference_contract(self):
        from spec import GUARANTEE
        self.assertEqual(GUARANTEE["authoritative_refresh_element"], "linearizable")

    def test_rpc_dtos_reject_non_empty_read_probes_and_malformed_context(self):
        from command_dto import CommandEnvelopeDTO
        command = CommandEnvelopeDTO(
            command_schema_version=1, request_id="noop", client_id="_raft",
            origin_node_uuid=self.ids["n1"], operation="raft.noop", payload={},
        )
        with self.assertRaises(ValidationError):
            AppendEntriesDTO(
                kind="append_entries", protocol_version=1, rpc_id=uuid4(),
                sender_slot="n1", sender_uuid=self.ids["n1"], term=1,
                prev_log_index=0, prev_log_term=0, leader_commit=0,
                entries=[{"index": 1, "term": 1, "command": command}],
                read_context=uuid4(),
            )
        with self.assertRaises(ValidationError):
            AppendEntriesResponseDTO(
                kind="append_entries_response", protocol_version=1, rpc_id=uuid4(),
                sender_slot="n2", sender_uuid=self.ids["n2"], term=1,
                reply_to=uuid4(), success=True, match_index=0,
                read_context="not-a-uuid",
            )


if __name__ == "__main__":
    unittest.main()
