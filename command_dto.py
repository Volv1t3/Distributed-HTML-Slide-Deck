"""Versioned committed command models and canonical client-request identity."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from dto_base import StrictDTO, canonical_bytes, sha256_json
from tlv_registry import TLVBlockDTO, TLVRegistry


class KVPutPayloadDTO(StrictDTO):
    key: StrictStr = Field(min_length=1, max_length=256)
    value: StrictStr = Field(max_length=10_000)


class PresentationMetadataDTO(StrictDTO):
    """Validated non-authoritative metadata captured by a first publication."""

    deck_id: UUID
    name: StrictStr = Field(min_length=1, max_length=120)
    owner_session_id: UUID
    width_px: StrictInt = Field(ge=320, le=3840)
    height_px: StrictInt = Field(ge=320, le=3840)


class SlideMetadataDTO(StrictDTO):
    slide_id: UUID
    background: StrictStr = Field(pattern=r"^#[0-9A-F]{6}$")
    ordinal: StrictInt = Field(ge=0)


class TextElementConfigurationDTO(StrictDTO):
    content: StrictStr = Field(max_length=10_000)
    font_families: list[StrictStr] = Field(default_factory=list, max_length=16)
    generic_fallback: Literal["sans-serif", "serif", "monospace"]
    font_size: StrictInt = Field(ge=0)
    color: StrictStr = Field(pattern=r"^#[0-9A-F]{8}$")
    x: StrictInt
    y: StrictInt
    z_index: StrictInt = 0
    bold: StrictBool = False
    italic: StrictBool = False
    align: Literal["left", "center", "right", "justify"] = "left"


class ShapeElementConfigurationDTO(StrictDTO):
    x: StrictInt
    y: StrictInt
    width: StrictInt = Field(gt=0)
    height: StrictInt = Field(gt=0)
    scale_basis_points: StrictInt = Field(ge=0)
    rotation_centidegrees: StrictInt = Field(ge=0, lt=36000)
    fill: StrictStr = Field(pattern=r"^#[0-9A-F]{8}$")
    border_color: StrictStr = Field(pattern=r"^#[0-9A-F]{8}$")
    border_style: Literal["solid", "dashed", "dotted"]
    border_thickness: StrictInt = Field(ge=0)
    z_index: StrictInt = 0
    sides: StrictInt | None = Field(default=None, ge=3, le=64)
    vertices: list[tuple[StrictInt, StrictInt]] | None = Field(default=None, max_length=128)


class PresentationElementDTO(StrictDTO):
    element_id: UUID
    kind: Literal[
        "heading",
        "subtitle",
        "text_box",
        "rectangle",
        "square",
        "circle",
        "triangle",
        "regular_polygon",
        "irregular_polygon",
    ]
    version: Literal[1] = 1
    configuration: TextElementConfigurationDTO | ShapeElementConfigurationDTO

    @model_validator(mode="after")
    def require_configuration_for_kind(self) -> "PresentationElementDTO":
        text_kinds = {"heading", "subtitle", "text_box"}
        if (self.kind in text_kinds) != isinstance(self.configuration, TextElementConfigurationDTO):
            raise ValueError("element kind does not match its typed configuration")
        if self.kind in {"square", "circle"} and self.configuration.width != self.configuration.height:
            raise ValueError("squares and circles require equal width and height")
        if self.kind == "triangle" and self.configuration.sides not in (None, 3):
            raise ValueError("triangles require three sides")
        if self.kind == "regular_polygon" and self.configuration.sides is None:
            raise ValueError("regular polygons require sides")
        if self.kind == "irregular_polygon" and not self.configuration.vertices:
            raise ValueError("irregular polygons require vertices")
        return self


class PublishedSlideDTO(StrictDTO):
    metadata: SlideMetadataDTO
    elements: list[PresentationElementDTO] = Field(default_factory=list, max_length=512)

    @model_validator(mode="after")
    def require_unique_element_ids(self) -> "PublishedSlideDTO":
        element_ids = [element.element_id for element in self.elements]
        if len(element_ids) != len(set(element_ids)):
            raise ValueError("duplicate element ID")
        return self


class PresentationSnapshotDTO(StrictDTO):
    schema_version: Literal[1]
    metadata: PresentationMetadataDTO
    slides: list[PublishedSlideDTO] = Field(default_factory=list, max_length=256)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("presentation schema must be integer 1")
        return value

    @model_validator(mode="after")
    def require_unique_snapshot_ids(self) -> "PresentationSnapshotDTO":
        slide_ids = [slide.metadata.slide_id for slide in self.slides]
        if len(slide_ids) != len(set(slide_ids)):
            raise ValueError("duplicate slide ID")
        element_ids = [element.element_id for slide in self.slides for element in slide.elements]
        if len(element_ids) != len(set(element_ids)):
            raise ValueError("duplicate element ID")
        if len({slide.metadata.ordinal for slide in self.slides}) != len(self.slides):
            raise ValueError("duplicate slide ordinal")
        return self


class PublishDeckPayloadDTO(StrictDTO):
    presentation: PresentationSnapshotDTO


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
    """Validate commands before Raft admission and deterministic application."""

    if command.operation == "kv_put":
        KVPutPayloadDTO.model_validate(command.payload)
        return
    if command.operation == "raft.noop":
        if command.payload or command.blocks:
            raise ValueError("raft.noop cannot carry payload or TLV blocks")
        return
    if command.operation == "publish_deck":
        payload = PublishDeckPayloadDTO.model_validate(command.payload)
        if command.deck_id != payload.presentation.metadata.deck_id:
            raise ValueError("publish_deck envelope deck_id must match presentation metadata")
        if command.blocks:
            raise ValueError("publish_deck does not accept command extension blocks")
        return
    raise ValueError(f"unsupported committed operation: {command.operation}")
