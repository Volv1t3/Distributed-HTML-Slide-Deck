"""Extensible, validated TLV-like blocks for committed command envelopes."""

from __future__ import annotations

from typing import Any

from pydantic import Field, StrictBool, StrictInt, field_validator, model_validator

from dto_base import StrictDTO, canonical_bytes


AUDIT_METADATA_TYPE_ID = 0x0301


class TLVBlockDTO(StrictDTO):
    type_id: StrictInt = Field(ge=1, le=65535)
    schema_version: StrictInt = Field(ge=1)
    required: StrictBool
    length_bytes: StrictInt = Field(ge=0, le=65536)
    value: dict[str, Any]

    @field_validator("value")
    @classmethod
    def require_json_value(cls, value: dict[str, Any]) -> dict[str, Any]:
        canonical_bytes(value)
        return value

    @model_validator(mode="after")
    def check_declared_length(self) -> "TLVBlockDTO":
        if len(canonical_bytes(self.value)) != self.length_bytes:
            raise ValueError("TLV length_bytes does not match canonical value")
        return self


class AuditMetadataDTO(StrictDTO):
    reason: str | None = Field(default=None, max_length=512)


class TLVRegistry:
    """Validates registered blocks while permitting future optional extensions."""

    _schemas = {(AUDIT_METADATA_TYPE_ID, 1): AuditMetadataDTO}

    def validate_blocks(self, raw_blocks: list[TLVBlockDTO | dict[str, Any]]) -> list[TLVBlockDTO]:
        blocks = [
            block if isinstance(block, TLVBlockDTO) else TLVBlockDTO.model_validate(block)
            for block in raw_blocks
        ]
        seen: set[int] = set()
        for block in blocks:
            if block.type_id in seen:
                raise ValueError(f"duplicate TLV type_id {block.type_id}")
            seen.add(block.type_id)
            schema = self._schemas.get((block.type_id, block.schema_version))
            if schema is None:
                if block.required:
                    raise ValueError(
                        f"unknown required TLV {block.type_id}/{block.schema_version}"
                    )
                continue
            schema.model_validate(block.value)
        return blocks
