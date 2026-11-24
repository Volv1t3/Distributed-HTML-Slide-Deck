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
    background: StrictStr = Field(pattern=r"^#[0-9A-F]{8}$")


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
    underline: StrictBool = False
    strikethrough: StrictBool = False
    align: Literal["left", "center", "right", "justify"] = "left"
    line_spacing: StrictInt = Field(default=0, ge=0)
    indentation: StrictInt = Field(default=0, ge=0)

    @field_validator("font_families")
    @classmethod
    def require_bounded_font_family_names(cls, value: list[str]) -> list[str]:
        if any(not family or len(family) > 120 for family in value):
            raise ValueError("font family names must be non-empty and at most 120 characters")
        return value


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
        if self.kind in text_kinds:
            return self
        if self.kind in {"square", "circle"} and self.configuration.width != self.configuration.height:
            raise ValueError("squares and circles require equal width and height")
        if self.kind in {"rectangle", "square", "circle"} and (
            self.configuration.sides is not None or self.configuration.vertices is not None
        ):
            raise ValueError("basic shapes cannot carry polygon data")
        if self.kind == "triangle" and self.configuration.sides != 3:
            raise ValueError("triangles require three sides")
        if self.kind == "triangle" and self.configuration.vertices is not None:
            raise ValueError("triangles cannot carry irregular vertices")
        if self.kind == "regular_polygon" and (
            self.configuration.sides is None or self.configuration.vertices is not None
        ):
            raise ValueError("regular polygons require sides and cannot carry vertices")
        if self.kind == "irregular_polygon" and (
            self.configuration.sides is not None
            or self.configuration.vertices is None
            or len(self.configuration.vertices) < 3
        ):
            raise ValueError("irregular polygons require at least three vertices")
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
    schema_version: Literal[2]
    metadata: PresentationMetadataDTO
    slides: list[PublishedSlideDTO] = Field(default_factory=list, max_length=256)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 2:
            raise ValueError("presentation schema must be integer 2")
        return value

    @model_validator(mode="after")
    def require_unique_snapshot_ids(self) -> "PresentationSnapshotDTO":
        slide_ids = [slide.metadata.slide_id for slide in self.slides]
        if len(slide_ids) != len(set(slide_ids)):
            raise ValueError("duplicate slide ID")
        element_ids = [element.element_id for slide in self.slides for element in slide.elements]
        if len(element_ids) != len(set(element_ids)):
            raise ValueError("duplicate element ID")
        return self


class PublishDeckPayloadDTO(StrictDTO):
    presentation: PresentationSnapshotDTO


class StructuralCommandPayloadDTO(StrictDTO):
    """Shared optimistic-concurrency precondition for slide structure changes."""

    expected_deck_version: StrictInt = Field(ge=0)


class CreateSlidePayloadDTO(StructuralCommandPayloadDTO):
    """Create one empty slide at a deterministic position in an active deck."""

    slide: PublishedSlideDTO
    insert_after_slide_id: UUID | None = None

    @model_validator(mode="after")
    def require_empty_initial_slide(self) -> "CreateSlidePayloadDTO":
        if self.slide.elements:
            raise ValueError("create_slide cannot create descendant elements")
        return self


class DeleteSlidePayloadDTO(StructuralCommandPayloadDTO):
    slide_id: UUID


class ReorderSlidesPayloadDTO(StructuralCommandPayloadDTO):
    slide_ids: list[UUID] = Field(max_length=256)


class CreateElementPayloadDTO(StrictDTO):
    """Create one typed element and grant its first fencing lock atomically."""

    slide_id: UUID
    element: PresentationElementDTO


class ElementLockPayloadDTO(StrictDTO):
    """Identify an element and prove ownership of its current lock generation."""

    element_id: UUID
    fencing_token: StrictInt = Field(ge=1)


class RenewLockPayloadDTO(StrictDTO):
    """Prove ownership and request a lease epoch increment for one element lock."""

    element_id: UUID
    fencing_token: StrictInt = Field(ge=1)


class ExpireLockPayloadDTO(StrictDTO):
    """Internal leader command: expire a lock only when token and epoch match exactly."""

    element_id: UUID
    expected_token: StrictInt = Field(ge=1)
    expected_lease_epoch: StrictInt = Field(ge=0)


class AcquireLockPayloadDTO(StrictDTO):
    element_id: UUID


class UpdateElementPayloadDTO(ElementLockPayloadDTO):
    """One atomic partial replacement of a typed element configuration."""

    expected_version: StrictInt = Field(ge=1)
    patch: dict[str, Any] = Field(min_length=1)

    @field_validator("patch")
    @classmethod
    def require_canonical_patch(cls, value: dict[str, Any]) -> dict[str, Any]:
        canonical_bytes(value)
        return value


class DeleteElementPayloadDTO(ElementLockPayloadDTO):
    expected_version: StrictInt = Field(ge=1)


class DeckSettingsPatchDTO(StrictDTO):
    """Nonempty partial patch for Deck Settings (name, width_px, height_px)."""

    name: StrictStr | None = Field(default=None, min_length=1)
    width_px: StrictInt | None = Field(default=None, ge=320, le=3840)
    height_px: StrictInt | None = Field(default=None, ge=320, le=3840)

    @model_validator(mode="after")
    def require_at_least_one_field(self) -> "DeckSettingsPatchDTO":
        if self.name is None and self.width_px is None and self.height_px is None:
            raise ValueError("deck settings patch must contain at least one field")
        return self


class UpdateDeckSettingsPayloadDTO(StrictDTO):
    """Payload for the update_deck_settings committed command."""

    expected_deck_settings_version: StrictInt = Field(ge=0)
    patch: DeckSettingsPatchDTO


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
            "session_id": str(envelope.session_id) if envelope.session_id is not None else None,
            "deck_id": str(envelope.deck_id) if envelope.deck_id is not None else None,
            "operation": envelope.operation,
            "payload": envelope.payload,
            "blocks": [block.model_dump(mode="json") for block in envelope.blocks],
        }
    )


def validate_committed_command(command: CommandEnvelopeDTO) -> None:
    """Validate commands before Raft admission and deterministic application."""

    if len(canonical_bytes(command.model_dump(mode="json"))) > 1_000_000:
        raise ValueError("canonical command exceeds the protocol size limit")
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
    if command.operation == "create_slide":
        if command.deck_id is None:
            raise ValueError("create_slide requires envelope deck_id")
        CreateSlidePayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("create_slide does not accept command extension blocks")
        return
    if command.operation == "delete_slide":
        if command.deck_id is None:
            raise ValueError("delete_slide requires envelope deck_id")
        DeleteSlidePayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("delete_slide does not accept command extension blocks")
        return
    if command.operation == "reorder_slides":
        if command.deck_id is None:
            raise ValueError("reorder_slides requires envelope deck_id")
        ReorderSlidesPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("reorder_slides does not accept command extension blocks")
        return
    if command.operation == "create_element":
        if command.deck_id is None:
            raise ValueError("create_element requires envelope deck_id")
        if command.session_id is None:
            raise ValueError("create_element requires editor session_id")
        CreateElementPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("create_element does not accept command extension blocks")
        return
    if command.operation == "acquire_lock":
        if command.deck_id is None or command.session_id is None:
            raise ValueError("acquire_lock requires deck and editor session IDs")
        AcquireLockPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("acquire_lock does not accept command extension blocks")
        return
    if command.operation == "release_lock":
        if command.deck_id is None or command.session_id is None:
            raise ValueError("release_lock requires deck and editor session IDs")
        ElementLockPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("release_lock does not accept command extension blocks")
        return
    if command.operation == "update_element":
        if command.deck_id is None or command.session_id is None:
            raise ValueError("update_element requires deck and editor session IDs")
        UpdateElementPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("update_element does not accept command extension blocks")
        return
    if command.operation == "delete_element":
        if command.deck_id is None or command.session_id is None:
            raise ValueError("delete_element requires deck and editor session IDs")
        DeleteElementPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("delete_element does not accept command extension blocks")
        return
    if command.operation == "renew_lock":
        if command.deck_id is None or command.session_id is None:
            raise ValueError("renew_lock requires deck and editor session IDs")
        RenewLockPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("renew_lock does not accept command extension blocks")
        return
    if command.operation == "expire_lock":
        if command.deck_id is None:
            raise ValueError("expire_lock requires envelope deck_id")
        ExpireLockPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("expire_lock does not accept command extension blocks")
        return
    if command.operation == "update_deck_settings":
        if command.deck_id is None:
            raise ValueError("update_deck_settings requires envelope deck_id")
        UpdateDeckSettingsPayloadDTO.model_validate(command.payload)
        if command.blocks:
            raise ValueError("update_deck_settings does not accept command extension blocks")
        return
    raise ValueError(f"unsupported committed operation: {command.operation}")
