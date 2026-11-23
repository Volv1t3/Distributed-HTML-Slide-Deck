"""Strict, versioned DTOs for the pure Raft RPC and durable state contracts."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr, TypeAdapter, field_validator, model_validator

from command_dto import ClientResultDTO, CommandEnvelopeDTO
from dto_base import StrictDTO, canonical_bytes


MAX_RPC_BYTES = 512 * 1024
MAX_ENTRY_BYTES = 128 * 1024


class LogEntryDTO(StrictDTO):
    index: StrictInt = Field(ge=1)
    term: StrictInt = Field(ge=1)
    command: CommandEnvelopeDTO

    @model_validator(mode="after")
    def check_encoded_size(self) -> "LogEntryDTO":
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_ENTRY_BYTES:
            raise ValueError("log entry exceeds maximum canonical size")
        return self


class RpcHeaderDTO(StrictDTO):
    protocol_version: Literal[1]
    rpc_id: UUID
    sender_slot: StrictStr = Field(min_length=1, max_length=64)
    sender_uuid: UUID
    term: StrictInt = Field(ge=0)

    @field_validator("protocol_version", mode="before")
    @classmethod
    def require_integer_protocol_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("protocol version must be integer 1")
        return value


class RegisterNodeDTO(RpcHeaderDTO):
    kind: Literal["register_node"]
    incarnation: StrictInt = Field(ge=1)
    membership_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")


class RequestVoteDTO(RpcHeaderDTO):
    kind: Literal["request_vote"]
    candidate_last_log_index: StrictInt = Field(ge=0)
    candidate_last_log_term: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def check_genesis_pair(self) -> "RequestVoteDTO":
        if (self.candidate_last_log_index == 0) != (self.candidate_last_log_term == 0):
            raise ValueError("empty candidate log must use the 0/0 genesis pair")
        return self


class RequestVoteResponseDTO(RpcHeaderDTO):
    kind: Literal["request_vote_response"]
    reply_to: UUID
    vote_granted: StrictBool


class AppendEntriesDTO(RpcHeaderDTO):
    kind: Literal["append_entries"]
    prev_log_index: StrictInt = Field(ge=0)
    prev_log_term: StrictInt = Field(ge=0)
    leader_commit: StrictInt = Field(ge=0)
    entries: list[LogEntryDTO] = Field(default_factory=list, max_length=32)
    # Opaque per-read quorum confirmation context; never a client request ID.
    read_context: UUID | None = None

    @model_validator(mode="after")
    def check_indexes(self) -> "AppendEntriesDTO":
        if self.prev_log_index == 0 and self.prev_log_term != 0:
            raise ValueError("genesis prev_log_term must be zero")
        if self.term == 0:
            raise ValueError("AppendEntries requires a positive term")
        if self.read_context is not None and self.entries:
            raise ValueError("ReadIndex probes must be empty AppendEntries heartbeats")
        for offset, entry in enumerate(self.entries, start=1):
            if entry.index != self.prev_log_index + offset:
                raise ValueError("AppendEntries indexes must be contiguous")
        return self


class AppendEntriesResponseDTO(RpcHeaderDTO):
    kind: Literal["append_entries_response"]
    reply_to: UUID
    success: StrictBool
    match_index: StrictInt = Field(ge=0)
    conflict_index: StrictInt | None = Field(default=None, ge=1)
    conflict_term: StrictInt | None = Field(default=None, ge=0)
    read_context: UUID | None = None

    @model_validator(mode="after")
    def check_conflict_hints(self) -> "AppendEntriesResponseDTO":
        if self.success and (self.conflict_index is not None or self.conflict_term is not None):
            raise ValueError("successful AppendEntries response cannot contain conflict hints")
        if self.conflict_term is not None and self.conflict_index is None:
            raise ValueError("conflict term requires conflict index")
        return self


RaftMessage = Annotated[
    Union[
        RegisterNodeDTO,
        RequestVoteDTO,
        RequestVoteResponseDTO,
        AppendEntriesDTO,
        AppendEntriesResponseDTO,
    ],
    Field(discriminator="kind"),
]
RAFT_MESSAGE_ADAPTER = TypeAdapter(RaftMessage)


def validate_raft_message(raw_message: dict[str, Any]) -> RaftMessage:
    """Validate and bound an untrusted transport message before engine dispatch."""

    if len(canonical_bytes(raw_message)) > MAX_RPC_BYTES:
        raise ValueError("Raft RPC exceeds maximum canonical size")
    return RAFT_MESSAGE_ADAPTER.validate_python(raw_message)


class RaftHardStateDTO(StrictDTO):
    current_term: StrictInt = Field(ge=0)
    voted_for: StrictStr | None = None
    log: list[LogEntryDTO] = Field(default_factory=list)
    commit_index: StrictInt = Field(default=0, ge=0)
    last_applied: StrictInt = Field(default=0, ge=0)

    @model_validator(mode="after")
    def check_log_bounds(self) -> "RaftHardStateDTO":
        if not 0 <= self.last_applied <= self.commit_index <= len(self.log):
            raise ValueError("invalid commit/apply bounds")
        if any(entry.index != index for index, entry in enumerate(self.log, start=1)):
            raise ValueError("log indexes are not contiguous")
        return self


class DedupRecordDTO(StrictDTO):
    fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    result: ClientResultDTO
    committed_index: StrictInt = Field(ge=1)


class PersistedRaftStateDTO(StrictDTO):
    storage_version: Literal[1]
    node_uuid: UUID
    incarnation: StrictInt = Field(ge=1)
    membership_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    peer_bindings: dict[str, UUID] = Field(default_factory=dict)
    raft: RaftHardStateDTO
    fsm: dict[str, Any] = Field(default_factory=lambda: {"kv": {}})
    request_results: dict[str, DedupRecordDTO] = Field(default_factory=dict)

    @field_validator("storage_version", mode="before")
    @classmethod
    def require_integer_storage_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("storage version must be integer 1")
        return value

    @model_validator(mode="after")
    def check_snapshot_invariants(self) -> "PersistedRaftStateDTO":
        canonical_bytes(self.fsm)
        if any(
            record.committed_index > self.raft.last_applied
            for record in self.request_results.values()
        ):
            raise ValueError("dedup snapshot is ahead of applied state")
        return self
