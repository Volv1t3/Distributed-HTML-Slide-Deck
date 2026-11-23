"""Versioned committed command models and canonical client-request identity."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from dto_base import StrictDTO, canonical_bytes, sha256_json
from tlv_registry import TLVBlockDTO, TLVRegistry


class KVPutPayloadDTO(StrictDTO):
    key: StrictStr = Field(min_length=1, max_length=256)
    value: StrictStr = Field(max_length=10_000)


class ClientResultDTO(StrictDTO):
    status: StrictStr = Field(min_length=1, max_length=80)
    value: Any | None = None
    version: StrictInt | None = Field(default=None, ge=0)
    details: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_json_result(self) -> "ClientResultDTO":
        canonical_bytes({"value": self.value, "details": self.details})
        return self


class CommandEnvelopeDTO(StrictDTO):
    command_schema_version: Literal[1]
    request_id: StrictStr = Field(min_length=1, max_length=128)
    client_id: StrictStr = Field(min_length=1, max_length=128)
    session_id: UUID | None = None
    origin_node_uuid: UUID
    submitted_at_utc: datetime | None = None
    deck_id: UUID | None = None
    operation: StrictStr = Field(min_length=1, max_length=80)
    expected_version: StrictInt | None = Field(default=None, ge=0)
    payload: dict[str, Any]
    blocks: list[TLVBlockDTO] = Field(default_factory=list, max_length=64)

    @field_validator("command_schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("command schema must be integer 1")
        return value

    @field_validator("submitted_at_utc")
    @classmethod
    def require_utc_audit_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != timedelta(0)
        ):
            raise ValueError("submitted_at_utc must be UTC")
        return value

    @field_validator("payload")
    @classmethod
    def require_json_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        canonical_bytes(value)
        return value

    @model_validator(mode="after")
    def validate_extensions_and_read_marker(self) -> "CommandEnvelopeDTO":
        TLVRegistry().validate_blocks(self.blocks)
        if self.operation != "kv_get" and "local" in self.payload:
            raise ValueError("local is only valid for local read metadata")
        return self


def command_fingerprint(command: CommandEnvelopeDTO | dict[str, Any]) -> str:
    """Hash the logical client request, excluding origin and audit metadata."""

    envelope = (
        command
        if isinstance(command, CommandEnvelopeDTO)
        else CommandEnvelopeDTO.model_validate(command)
    )
    return sha256_json(
        {
            "client_id": envelope.client_id,
            "operation": envelope.operation,
            "payload": envelope.payload,
            "blocks": [block.model_dump(mode="json") for block in envelope.blocks],
        }
    )


def validate_committed_command(command: CommandEnvelopeDTO) -> None:
    """Validate the Stage A command subset before admission or deterministic apply."""

    if command.operation == "kv_put":
        KVPutPayloadDTO.model_validate(command.payload)
        return
    if command.operation == "raft.noop":
        if command.payload or command.blocks:
            raise ValueError("raft.noop cannot carry payload or TLV blocks")
        return
    raise ValueError(f"unsupported Stage A committed operation: {command.operation}")
