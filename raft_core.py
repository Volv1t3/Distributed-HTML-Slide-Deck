"""Pure fixed-membership Raft core with injected infrastructure boundaries."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID, uuid4

from command_dto import CommandEnvelopeDTO
from dto_base import sha256_json
from persistence import RaftPersistence, new_persisted_state
from raft_dto import (
    LogEntryDTO,
    RegisterNodeDTO,
    RequestVoteDTO,
    RequestVoteResponseDTO,
    validate_raft_message,
)

Transport = Callable[[str, dict[str, Any]], None]
Clock = Callable[[], float]
ElectionTimeout = Callable[[], float]


class RaftEngine:
    """Deterministic Raft core; network and durable storage are injected."""

    def __init__(
        self,
        *,
        node_slot: str,
        voters: Iterable[str],
        node_uuid: UUID,
        storage: RaftPersistence,
        send: Transport,
        clock: Clock,
        election_timeout: ElectionTimeout | None = None,
    ) -> None:
        voter_set = frozenset(voters)
        if len(voter_set) < 3:
            raise ValueError("Raft requires at least three fixed voters")
        if node_slot not in voter_set:
            raise ValueError("local node slot must be in the fixed voter set")
        if any(not isinstance(slot, str) or not slot for slot in voter_set):
            raise ValueError("voter slots must be non-empty strings")
        self.node_slot = node_slot
        self.voters = voter_set
        self.quorum = len(voter_set) // 2 + 1
        self.node_uuid = node_uuid
        self.storage = storage
        self.send = send
        self.clock = clock
        self._election_timeout = election_timeout or (lambda: random.uniform(0.45, 0.9))

        fingerprint = self.membership_fingerprint()
        persisted = storage.load()
        if persisted is None:
            self.state = new_persisted_state(node_uuid, fingerprint)
            self.storage.save(self.state)
        else:
            if persisted.node_uuid != node_uuid:
                raise ValueError("erased-disk identity requires safe reintroduction")
            if persisted.membership_fingerprint != fingerprint:
                raise ValueError("persisted membership does not match configured voters")
            self.state = persisted
            self.state.incarnation += 1
            self.storage.save(self.state)

        self.role = "follower"
        self.leader_slot: str | None = None
        self.votes_received: set[str] = set()
        self.pending_vote_requests: dict[UUID, str] = {}
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._reset_election_deadline()

    def membership_fingerprint(self) -> str:
        return sha256_json(sorted(self.voters))

    def on_tick(self) -> None:
        """Advance a follower/candidate into a new election only after its deadline."""

        if self.role != "leader" and self.clock() >= self.election_deadline:
            self.start_election()

    def start_election(self) -> None:
        """Persist a self-vote, then solicit one correlated vote from every peer."""

        hard_state = self.state.raft
        hard_state.current_term += 1
        hard_state.voted_for = self.node_slot
        self.role = "candidate"
        self.leader_slot = None
        self.votes_received = {self.node_slot}
        self.pending_vote_requests = {}
        self._reset_election_deadline()
        self.storage.save(self.state)

        last_index, last_term = self._last_log_position()
        for peer in self.voters - {self.node_slot}:
            request = RequestVoteDTO(
                kind="request_vote",
                protocol_version=1,
                rpc_id=uuid4(),
                sender_slot=self.node_slot,
                sender_uuid=self.node_uuid,
                term=hard_state.current_term,
                candidate_last_log_index=last_index,
                candidate_last_log_term=last_term,
            )
            self.pending_vote_requests[request.rpc_id] = peer
            self.send(peer, request.model_dump(mode="json"))

    def handle_message(self, source_slot: str, raw_message: dict[str, Any]) -> None:
        """Validate source identity before a Raft message can affect protocol state."""

        message = validate_raft_message(raw_message)
        if source_slot not in self.voters or message.sender_slot != source_slot:
            raise ValueError("RPC transport source does not match configured sender slot")
        if isinstance(message, RegisterNodeDTO):
            self._handle_registration(message)
            return
        if self.state.peer_bindings.get(source_slot) != message.sender_uuid:
            raise ValueError("RPC sender UUID is not registered for its transport slot")
        if message.term > self.state.raft.current_term:
            self._advance_term(message.term)
        if isinstance(message, RequestVoteDTO):
            self._handle_request_vote(message)
        elif isinstance(message, RequestVoteResponseDTO):
            self._handle_vote_response(message)

    def _handle_registration(self, message: RegisterNodeDTO) -> None:
        if message.membership_fingerprint != self.membership_fingerprint():
            raise ValueError("registration membership fingerprint mismatch")
        existing = self.state.peer_bindings.get(message.sender_slot)
        if existing is not None and existing != message.sender_uuid:
            raise ValueError("registered slot changed UUID and is quarantined")
        if existing is None:
            self.state.peer_bindings[message.sender_slot] = message.sender_uuid
            self.storage.save(self.state)

    def _handle_request_vote(self, request: RequestVoteDTO) -> None:
        hard_state = self.state.raft
        granted = False
        changed = False
        if request.term >= hard_state.current_term:
            local_index, local_term = self._last_log_position()
            up_to_date = (request.candidate_last_log_term, request.candidate_last_log_index) >= (local_term, local_index)
            if up_to_date and hard_state.voted_for in (None, request.sender_slot):
                if hard_state.voted_for != request.sender_slot:
                    hard_state.voted_for = request.sender_slot
                    changed = True
                granted = True
        if changed:
            self.storage.save(self.state)
        response = RequestVoteResponseDTO(
            kind="request_vote_response",
            protocol_version=1,
            rpc_id=uuid4(),
            sender_slot=self.node_slot,
            sender_uuid=self.node_uuid,
            term=hard_state.current_term,
            reply_to=request.rpc_id,
            vote_granted=granted,
        )
        self.send(request.sender_slot, response.model_dump(mode="json"))

    def _handle_vote_response(self, response: RequestVoteResponseDTO) -> None:
        if response.term != self.state.raft.current_term or self.role != "candidate":
            return
        expected_slot = self.pending_vote_requests.get(response.reply_to)
        if expected_slot != response.sender_slot or not response.vote_granted:
            return
        self.pending_vote_requests.pop(response.reply_to)
        self.votes_received.add(response.sender_slot)
        if len(self.votes_received) >= self.quorum:
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = "leader"
        self.leader_slot = self.node_slot
        last_index, _ = self._last_log_position()
        self.match_index = {self.node_slot: last_index}
        self.match_index.update({peer: 0 for peer in self.voters - {self.node_slot}})
        self.next_index = {peer: last_index + 1 for peer in self.voters - {self.node_slot}}
        noop = CommandEnvelopeDTO(
            command_schema_version=1,
            request_id=f"raft-noop-{self.state.raft.current_term}",
            client_id="_raft",
            origin_node_uuid=self.node_uuid,
            operation="raft.noop",
            payload={},
        )
        self.state.raft.log.append(LogEntryDTO(index=last_index + 1, term=self.state.raft.current_term, command=noop))
        self.match_index[self.node_slot] = last_index + 1
        self.storage.save(self.state)

    def _advance_term(self, term: int) -> None:
        self.state.raft.current_term = term
        self.state.raft.voted_for = None
        self.role = "follower"
        self.leader_slot = None
        self.votes_received.clear()
        self.pending_vote_requests.clear()
        self._reset_election_deadline()
        self.storage.save(self.state)

    def _last_log_position(self) -> tuple[int, int]:
        if not self.state.raft.log:
            return 0, 0
        last = self.state.raft.log[-1]
        return last.index, last.term

    def _reset_election_deadline(self) -> None:
        timeout = self._election_timeout()
        if timeout <= 0:
            raise ValueError("election timeout must be positive")
        self.election_deadline = self.clock() + timeout
