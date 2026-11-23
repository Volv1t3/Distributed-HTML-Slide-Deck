"""Pure fixed-membership Raft core with injected infrastructure boundaries."""

from __future__ import annotations

import random
from dataclasses import dataclass
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID, uuid4

from command_dto import ClientResultDTO, CommandEnvelopeDTO, command_fingerprint, validate_committed_command
from dto_base import canonical_bytes, sha256_json
from persistence import RaftPersistence, new_persisted_state
from raft_dto import (
    LogEntryDTO,
    AppendEntriesDTO,
    AppendEntriesResponseDTO,
    RegisterNodeDTO,
    RequestVoteDTO,
    RequestVoteResponseDTO,
    validate_raft_message,
)
from state_machine import KeyValueStateMachine

Transport = Callable[[str, dict[str, Any]], None]
Clock = Callable[[], float]
ElectionTimeout = Callable[[], float]


@dataclass(frozen=True)
class PendingAppend:
    peer: str
    term: int
    last_index: int


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
        self.pending_append_requests: dict[UUID, PendingAppend] = {}
        self.inflight_requests: dict[str, tuple[str, int]] = {}
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
        elif isinstance(message, AppendEntriesDTO):
            self._handle_append_entries(message)
        elif isinstance(message, AppendEntriesResponseDTO):
            self._handle_append_response(message)

    def propose(self, command: CommandEnvelopeDTO) -> LogEntryDTO | ClientResultDTO:
        """Durably append a validated client command on the elected leader only."""

        if self.role != "leader":
            raise RuntimeError("only the current leader can propose a command")
        validate_committed_command(command)
        key = self._request_key(command)
        fingerprint = command_fingerprint(command)
        committed = self.state.request_results.get(key)
        if committed is not None:
            if committed.fingerprint == fingerprint:
                return committed.result
            return ClientResultDTO(status="REQUEST_ID_REUSE")
        in_flight = self.inflight_requests.get(key)
        if in_flight is not None:
            if in_flight[0] != fingerprint:
                return ClientResultDTO(status="REQUEST_ID_REUSE")
            return self.state.raft.log[in_flight[1] - 1]
        entry = LogEntryDTO(
            index=len(self.state.raft.log) + 1,
            term=self.state.raft.current_term,
            command=command,
        )
        self.state.raft.log.append(entry)
        self.match_index[self.node_slot] = entry.index
        self.inflight_requests[key] = (fingerprint, entry.index)
        self.storage.save(self.state)
        self._broadcast_append_entries()
        return entry

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

    def _handle_append_entries(self, request: AppendEntriesDTO) -> None:
        hard_state = self.state.raft
        if request.term < hard_state.current_term:
            self._send_append_response(request, success=False, match_index=0)
            return
        self.role = "follower"
        self.leader_slot = request.sender_slot
        self._reset_election_deadline()

        if request.prev_log_index > len(hard_state.log):
            self._send_append_response(
                request, success=False, match_index=0, conflict_index=len(hard_state.log) + 1
            )
            return
        if request.prev_log_index:
            previous = hard_state.log[request.prev_log_index - 1]
            if previous.term != request.prev_log_term:
                conflict_term = previous.term
                conflict_index = request.prev_log_index
                while conflict_index > 1 and hard_state.log[conflict_index - 2].term == conflict_term:
                    conflict_index -= 1
                self._send_append_response(
                    request,
                    success=False,
                    match_index=0,
                    conflict_index=conflict_index,
                    conflict_term=conflict_term,
                )
                return

        for incoming in request.entries:
            offset = incoming.index - 1
            if offset >= len(hard_state.log):
                hard_state.log.append(incoming)
                continue
            existing = hard_state.log[offset]
            if existing.term == incoming.term:
                if canonical_bytes(existing.command.model_dump(mode="json")) != canonical_bytes(
                    incoming.command.model_dump(mode="json")
                ):
                    raise ValueError("same Raft index and term has divergent command data")
                continue
            if incoming.index <= hard_state.commit_index:
                raise ValueError("AppendEntries attempted to overwrite committed history")
            del hard_state.log[offset:]
            hard_state.log.extend(entry for entry in request.entries if entry.index >= incoming.index)
            break

        if request.leader_commit > hard_state.commit_index:
            hard_state.commit_index = min(request.leader_commit, len(hard_state.log))
        self._apply_committed()
        self.storage.save(self.state)
        self._send_append_response(
            request,
            success=True,
            match_index=request.prev_log_index + len(request.entries),
        )

    def _handle_append_response(self, response: AppendEntriesResponseDTO) -> None:
        if self.role != "leader" or response.term != self.state.raft.current_term:
            return
        pending = self.pending_append_requests.get(response.reply_to)
        if pending is None or pending.peer != response.sender_slot or pending.term != response.term:
            return
        self.pending_append_requests.pop(response.reply_to)
        if response.success:
            if response.match_index != pending.last_index:
                return
            self.match_index[response.sender_slot] = max(
                self.match_index.get(response.sender_slot, 0), response.match_index
            )
            self.next_index[response.sender_slot] = self.match_index[response.sender_slot] + 1
            self._advance_commit()
            return
        current = self.next_index.get(response.sender_slot, len(self.state.raft.log) + 1)
        minimum = self.match_index.get(response.sender_slot, 0) + 1
        hinted = response.conflict_index if response.conflict_index is not None else current - 1
        self.next_index[response.sender_slot] = max(minimum, min(current - 1, hinted))
        self._send_append_entries(response.sender_slot)

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
        self._broadcast_append_entries()

    def _advance_term(self, term: int) -> None:
        self.state.raft.current_term = term
        self.state.raft.voted_for = None
        self.role = "follower"
        self.leader_slot = None
        self.votes_received.clear()
        self.pending_vote_requests.clear()
        self.pending_append_requests.clear()
        self._reset_election_deadline()
        self.storage.save(self.state)

    def _last_log_position(self) -> tuple[int, int]:
        if not self.state.raft.log:
            return 0, 0
        last = self.state.raft.log[-1]
        return last.index, last.term

    def _broadcast_append_entries(self) -> None:
        for peer in self.voters - {self.node_slot}:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        next_index = self.next_index.get(peer, len(self.state.raft.log) + 1)
        prev_index = next_index - 1
        prev_term = self.state.raft.log[prev_index - 1].term if prev_index else 0
        entries = self.state.raft.log[next_index - 1 : next_index - 1 + 32]
        request = AppendEntriesDTO(
            kind="append_entries",
            protocol_version=1,
            rpc_id=uuid4(),
            sender_slot=self.node_slot,
            sender_uuid=self.node_uuid,
            term=self.state.raft.current_term,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.state.raft.commit_index,
        )
        last_index = prev_index + len(entries)
        self.pending_append_requests[request.rpc_id] = PendingAppend(
            peer=peer,
            term=request.term,
            last_index=last_index,
        )
        self.send(peer, request.model_dump(mode="json"))

    def _send_append_response(
        self,
        request: AppendEntriesDTO,
        *,
        success: bool,
        match_index: int,
        conflict_index: int | None = None,
        conflict_term: int | None = None,
    ) -> None:
        response = AppendEntriesResponseDTO(
            kind="append_entries_response",
            protocol_version=1,
            rpc_id=uuid4(),
            sender_slot=self.node_slot,
            sender_uuid=self.node_uuid,
            term=self.state.raft.current_term,
            reply_to=request.rpc_id,
            success=success,
            match_index=match_index,
            conflict_index=conflict_index,
            conflict_term=conflict_term,
        )
        self.send(request.sender_slot, response.model_dump(mode="json"))

    def _advance_commit(self) -> None:
        hard_state = self.state.raft
        for candidate in range(len(hard_state.log), hard_state.commit_index, -1):
            if hard_state.log[candidate - 1].term != hard_state.current_term:
                continue
            replicas = sum(index >= candidate for index in self.match_index.values())
            if replicas >= self.quorum:
                hard_state.commit_index = candidate
                self._apply_committed()
                self.storage.save(self.state)
                self._broadcast_append_entries()
                return

    def _apply_committed(self) -> None:
        hard_state = self.state.raft
        machine = KeyValueStateMachine(self.state.fsm)
        while hard_state.last_applied < hard_state.commit_index:
            entry = hard_state.log[hard_state.last_applied]
            key = self._request_key(entry.command)
            fingerprint = command_fingerprint(entry.command)
            existing = self.state.request_results.get(key)
            if existing is None:
                result = machine.apply(entry.command)
                from raft_dto import DedupRecordDTO

                self.state.request_results[key] = DedupRecordDTO(
                    fingerprint=fingerprint,
                    result=result,
                    committed_index=entry.index,
                )
            elif existing.fingerprint == fingerprint:
                result = existing.result
            else:
                result = ClientResultDTO(status="REQUEST_ID_REUSE")
            hard_state.last_applied = entry.index
            self.inflight_requests.pop(key, None)
            self.state.fsm = machine.snapshot()

    @staticmethod
    def _request_key(command: CommandEnvelopeDTO) -> str:
        return sha256_json([command.client_id, command.request_id])

    def _reset_election_deadline(self) -> None:
        timeout = self._election_timeout()
        if timeout <= 0:
            raise ValueError("election timeout must be positive")
        self.election_deadline = self.clock() + timeout
