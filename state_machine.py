"""Deterministic committed state for Stage A compatibility and Stage B publication."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from command_dto import (
    ClientResultDTO,
    CommandEnvelopeDTO,
    CreateElementPayloadDTO,
    AcquireLockPayloadDTO,
    DeleteElementPayloadDTO,
    ElementLockPayloadDTO,
    RenewLockPayloadDTO,
    ExpireLockPayloadDTO,
    CreateSlidePayloadDTO,
    DeleteSlidePayloadDTO,
    KVPutPayloadDTO,
    PublishDeckPayloadDTO,
    ReorderSlidesPayloadDTO,
    PresentationElementDTO,
    UpdateElementPayloadDTO,
    UpdateDeckSettingsPayloadDTO,
    UpdateSlideSettingsPayloadDTO,
    PresentationMetadataDTO,
    validate_committed_command,
)
from dto_base import canonical_bytes


class KeyValueStateMachine:
    """Raft-applied state with the initial shared-deck publication transition.

    The public class name remains stable while existing Stage A KV entries are
    still accepted. A published deck is only installed by one committed
    ``publish_deck`` command; client lock or fencing state is never imported.
    """

    def __init__(self, snapshot: dict[str, Any] | None = None) -> None:
        initial = snapshot if snapshot is not None else {"kv": {}}
        canonical_bytes(initial)
        kv = initial.get("kv")
        decks = initial.get("decks", {})
        if not isinstance(kv, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in kv.items()
        ):
            raise ValueError("state-machine snapshot must contain string KV values")
        if not isinstance(decks, dict) or not all(
            isinstance(key, str) and isinstance(value, dict) for key, value in decks.items()
        ):
            raise ValueError("state-machine snapshot must contain object decks")
        self._kv = dict(kv)
        self._decks = {deck_id: dict(deck) for deck_id, deck in decks.items()}
        for deck in self._decks.values():
            deck.setdefault("slide_tombstones", {})
            deck.setdefault("element_tombstones", {})
            deck.setdefault("locks", {})
            deck.setdefault("fencing_counters", {})
            if "next_creation_sequence" not in deck:
                creation_sequence = 1
                for slide in deck.get("slides", []):
                    for element in slide.get("elements", []):
                        element.setdefault("creation_sequence", creation_sequence)
                        creation_sequence += 1
                deck["next_creation_sequence"] = creation_sequence

    def apply(self, command: CommandEnvelopeDTO, *, committed_index: int | None = None) -> ClientResultDTO:
        validate_committed_command(command)
        if command.operation == "raft.noop":
            return ClientResultDTO(status="OK")
        if command.operation == "kv_put":
            payload = KVPutPayloadDTO.model_validate(command.payload)
            self._kv[payload.key] = payload.value
            return ClientResultDTO(status="OK", value="ok")

        if command.operation == "create_slide":
            return self._create_slide(command)
        if command.operation == "delete_slide":
            return self._delete_slide(command)
        if command.operation == "reorder_slides":
            return self._reorder_slides(command)
        if command.operation == "create_element":
            return self._create_element(command)
        if command.operation == "acquire_lock":
            return self._acquire_lock(command)
        if command.operation == "release_lock":
            return self._release_lock(command)
        if command.operation == "renew_lock":
            return self._renew_lock(command)
        if command.operation == "expire_lock":
            return self._expire_lock(command)
        if command.operation == "update_element":
            return self._update_element(command, committed_index=committed_index)
        if command.operation == "delete_element":
            return self._delete_element(command, committed_index=committed_index)
        if command.operation == "update_deck_settings":
            return self._update_deck_settings(command, committed_index=committed_index)
        if command.operation == "update_slide_settings":
            return self._update_slide_settings(command, committed_index=committed_index)

        payload = PublishDeckPayloadDTO.model_validate(command.payload)
        deck_id = str(payload.presentation.metadata.deck_id)
        if deck_id in self._decks:
            return ClientResultDTO(
                status="DECK_ALREADY_PUBLISHED",
                value={"deck_id": deck_id, "deck_version": self._decks[deck_id]["deck_version"]},
                version=self._decks[deck_id]["deck_version"],
            )

        presentation = payload.presentation.model_dump(mode="json")
        metadata = presentation["metadata"]
        # A private owner/session assertion has no authority in shared state.
        metadata.pop("owner_session_id")
        creation_sequence = 1
        for slide in presentation["slides"]:
            slide["metadata"].setdefault("slide_settings_version", 0)
            for element in slide["elements"]:
                element["creation_sequence"] = creation_sequence
                creation_sequence += 1
        self._decks[deck_id] = {
            "schema_version": presentation["schema_version"],
            "metadata": metadata,
            "slides": presentation["slides"],
            "deck_version": 0,
            "deck_settings_version": 0,
            "slide_tombstones": {},
            "element_tombstones": {},
            "locks": {},
            "fencing_counters": {
                element["element_id"]: 0
                for slide in presentation["slides"]
                for element in slide["elements"]
            },
            "next_creation_sequence": creation_sequence,
        }
        return ClientResultDTO(
            status="PUBLISHED",
            value={"deck_id": deck_id, "deck_version": 0, "slide_ids": [slide["metadata"]["slide_id"] for slide in presentation["slides"]]},
            version=0,
        )

    def _create_element(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = CreateElementPayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        slide_id = str(payload.slide_id)
        if slide_id in deck["slide_tombstones"]:
            return ClientResultDTO(status="SLIDE_DELETED")
        slide = next(
            (candidate for candidate in deck["slides"] if candidate["metadata"]["slide_id"] == slide_id),
            None,
        )
        if slide is None:
            return ClientResultDTO(status="NOT_FOUND")

        element_id = str(payload.element.element_id)
        active_element_ids = {
            element["element_id"] for candidate in deck["slides"] for element in candidate["elements"]
        }
        if element_id in active_element_ids or element_id in deck["element_tombstones"]:
            return ClientResultDTO(status="INVALID_ARGUMENT")

        element = payload.element.model_dump(mode="json")
        element["creation_sequence"] = deck["next_creation_sequence"]
        deck["next_creation_sequence"] += 1
        fencing_token = deck["fencing_counters"].get(element_id, 0) + 1
        deck["fencing_counters"][element_id] = fencing_token
        deck["locks"][element_id] = {
            "session_id": str(command.session_id),
            "fencing_token": fencing_token,
            "lease_epoch": 0,
        }
        slide["elements"].append(element)
        return ClientResultDTO(
            status="CREATED_AND_LOCKED",
            version=1,
            value={
                "deck_id": deck_id,
                "slide_id": slide_id,
                "element_id": element_id,
                "element_version": 1,
                "fencing_token": fencing_token,
                "lease_epoch": 0,
            },
        )

    def _element(self, command: CommandEnvelopeDTO, element_id: str) -> tuple[str, dict[str, Any], dict[str, Any]] | ClientResultDTO:
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        for slide in deck["slides"]:
            for element in slide["elements"]:
                if element["element_id"] == element_id:
                    return deck_id, deck, element
        return ClientResultDTO(status="NOT_FOUND")

    @staticmethod
    def _lock_guard(deck: dict[str, Any], element_id: str, command: CommandEnvelopeDTO, fencing_token: int) -> ClientResultDTO | None:
        lock = deck["locks"].get(element_id)
        if lock is None or lock["fencing_token"] != fencing_token:
            return ClientResultDTO(status="STALE_TOKEN")
        if lock["session_id"] != str(command.session_id):
            return ClientResultDTO(status="NOT_OWNER")
        return None

    def _acquire_lock(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = AcquireLockPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck, element = resolved
        lock = deck["locks"].get(element_id)
        if lock is not None:
            if lock["session_id"] == str(command.session_id):
                return ClientResultDTO(status="ALREADY_HELD", version=element["version"], value={
                    "deck_id": deck_id, "element_id": element_id,
                    "fencing_token": lock["fencing_token"],
                    "lease_epoch": lock["lease_epoch"],
                })
            return ClientResultDTO(status="LOCKED", version=element["version"], value={
                "owner_session": lock["session_id"], "fencing_token": lock["fencing_token"],
            })
        fencing_token = deck["fencing_counters"].get(element_id, 0) + 1
        deck["fencing_counters"][element_id] = fencing_token
        deck["locks"][element_id] = {"session_id": str(command.session_id), "fencing_token": fencing_token, "lease_epoch": 0}
        return ClientResultDTO(status="LOCK_ACQUIRED", version=element["version"], value={
            "deck_id": deck_id, "element_id": element_id, "fencing_token": fencing_token,
            "element_version": element["version"], "lease_epoch": 0,
        })

    def _release_lock(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = ElementLockPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck, element = resolved
        rejected = self._lock_guard(deck, element_id, command, payload.fencing_token)
        if rejected is not None:
            return rejected
        del deck["locks"][element_id]
        return ClientResultDTO(status="RELEASED", version=element["version"], value={
            "deck_id": deck_id, "element_id": element_id, "fencing_token": payload.fencing_token,
        })

    def _renew_lock(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = RenewLockPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck, element = resolved
        rejected = self._lock_guard(deck, element_id, command, payload.fencing_token)
        if rejected is not None:
            return rejected
        lock = deck["locks"][element_id]
        lock["lease_epoch"] += 1
        new_epoch = lock["lease_epoch"]
        return ClientResultDTO(status="RENEWED", version=element["version"], value={
            "deck_id": deck_id, "element_id": element_id,
            "fencing_token": payload.fencing_token, "lease_epoch": new_epoch,
        })

    def _expire_lock(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = ExpireLockPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return ClientResultDTO(status="NO_OP_STALE_EXPIRY")
        deck_id, deck, element = resolved
        lock = deck["locks"].get(element_id)
        if (
            lock is None
            or lock["fencing_token"] != payload.expected_token
            or lock["lease_epoch"] != payload.expected_lease_epoch
        ):
            return ClientResultDTO(status="NO_OP_STALE_EXPIRY")
        del deck["locks"][element_id]
        return ClientResultDTO(status="EXPIRED", version=element["version"], value={
            "deck_id": deck_id, "element_id": element_id,
            "fencing_token": payload.expected_token,
        })

    def _update_element(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = UpdateElementPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck, element = resolved
        rejected = self._lock_guard(deck, element_id, command, payload.fencing_token)
        if rejected is not None:
            return rejected
        if element["version"] != payload.expected_version:
            return self._version_conflict(element["version"], committed_index)
        prospective = {**element["configuration"], **payload.patch}
        validated = PresentationElementDTO.model_validate({
            "element_id": element_id, "kind": element["kind"], "configuration": prospective,
        })
        element["configuration"] = validated.configuration.model_dump(mode="json")
        element["version"] += 1
        return ClientResultDTO(status="SAVED", version=element["version"], value={
            "deck_id": deck_id, "element_id": element_id, "element_version": element["version"],
        })

    def _delete_element(
        self,
        command: CommandEnvelopeDTO,
        *,
        committed_index: int | None,
    ) -> ClientResultDTO:
        payload = DeleteElementPayloadDTO.model_validate(command.payload)
        element_id = str(payload.element_id)
        resolved = self._element(command, element_id)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck, element = resolved
        rejected = self._lock_guard(deck, element_id, command, payload.fencing_token)
        if rejected is not None:
            return rejected
        if element["version"] != payload.expected_version:
            return self._version_conflict(element["version"], committed_index)
        for slide in deck["slides"]:
            for index, candidate in enumerate(slide["elements"]):
                if candidate["element_id"] == element_id:
                    tombstone = deepcopy(candidate)
                    tombstone["version"] += 1
                    tombstone["deleted"] = True
                    deck["element_tombstones"][element_id] = tombstone
                    slide["elements"].pop(index)
                    deck["locks"].pop(element_id, None)
                    return ClientResultDTO(status="DELETED", version=tombstone["version"], value={
                        "deck_id": deck_id, "element_id": element_id, "element_version": tombstone["version"],
                    })
        raise RuntimeError("active element disappeared during deletion")

    @staticmethod
    def _version_conflict(element_version: int, committed_index: int | None) -> ClientResultDTO:
        """Build the same conflict-refresh contract for every protected mutation."""
        conflict = {"current_version": element_version}
        if committed_index is not None:
            conflict["min_commit_index"] = committed_index
        return ClientResultDTO(status="VERSION_CONFLICT", version=element_version, value=conflict)

    def _structural_deck(self, command: CommandEnvelopeDTO) -> tuple[str, dict[str, Any]] | ClientResultDTO:
        if command.deck_id is None:
            raise ValueError("structural command requires deck_id")
        deck_id = str(command.deck_id)
        deck = self._decks.get(deck_id)
        if deck is None:
            return ClientResultDTO(status="NOT_FOUND")
        return deck_id, deck

    @staticmethod
    def _structure_conflict(deck: dict[str, Any]) -> ClientResultDTO:
        version = deck["deck_version"]
        return ClientResultDTO(
            status="STRUCTURE_VERSION_CONFLICT",
            version=version,
            value={"deck_version": version},
        )

    @staticmethod
    def _settings_conflict(deck: dict[str, Any], committed_index: int | None) -> ClientResultDTO:
        """Return a DECK_SETTINGS_VERSION_CONFLICT with current canonical settings."""
        settings_version = deck["deck_settings_version"]
        value: dict[str, Any] = {
            "deck_settings_version": settings_version,
            "name": deck["metadata"]["name"],
            "width_px": deck["metadata"]["width_px"],
            "height_px": deck["metadata"]["height_px"],
            "deck_version": deck["deck_version"],
        }
        if committed_index is not None:
            value["min_commit_index"] = committed_index
        return ClientResultDTO(
            status="DECK_SETTINGS_VERSION_CONFLICT",
            version=settings_version,
            value=value,
        )

    def _update_deck_settings(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = UpdateDeckSettingsPayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved

        if payload.expected_deck_settings_version != deck["deck_settings_version"]:
            return self._settings_conflict(deck, committed_index)

        # Build prospective complete settings and validate via PresentationMetadataDTO
        patch = payload.patch
        current_meta = deck["metadata"]
        prospective = {
            "deck_id": str(command.deck_id),
            "name": patch.name if patch.name is not None else current_meta["name"],
            "owner_session_id": "00000000-0000-0000-0000-000000000000",  # placeholder; stripped at publish
            "width_px": patch.width_px if patch.width_px is not None else current_meta["width_px"],
            "height_px": patch.height_px if patch.height_px is not None else current_meta["height_px"],
        }
        try:
            PresentationMetadataDTO.model_validate(prospective)
        except Exception:
            return ClientResultDTO(status="INVALID_ARGUMENT")

        # Apply patch to stored metadata (Canvas Resize Invariant: elements not touched)
        if patch.name is not None:
            deck["metadata"]["name"] = patch.name
        if patch.width_px is not None:
            deck["metadata"]["width_px"] = patch.width_px
        if patch.height_px is not None:
            deck["metadata"]["height_px"] = patch.height_px

        deck["deck_settings_version"] += 1
        settings_version = deck["deck_settings_version"]

        value: dict[str, Any] = {
            "deck_id": deck_id,
            "name": deck["metadata"]["name"],
            "width_px": deck["metadata"]["width_px"],
            "height_px": deck["metadata"]["height_px"],
            "deck_settings_version": settings_version,
            "deck_version": deck["deck_version"],
        }
        if committed_index is not None:
            value["committed_index"] = committed_index

        return ClientResultDTO(
            status="DECK_SETTINGS_UPDATED",
            version=settings_version,
            value=value,
        )

    def _create_slide(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = CreateSlidePayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        if payload.expected_deck_version != deck["deck_version"]:
            return self._structure_conflict(deck)

        slide = payload.slide.model_dump(mode="json")
        slide["metadata"].setdefault("slide_settings_version", 0)
        slide_id = str(payload.slide.metadata.slide_id)
        active_slide_ids = {active["metadata"]["slide_id"] for active in deck["slides"]}
        if slide_id in active_slide_ids:
            return ClientResultDTO(status="INVALID_ARGUMENT")
        if slide_id in deck["slide_tombstones"]:
            return ClientResultDTO(status="SLIDE_DELETED")

        if payload.insert_after_slide_id is None:
            insert_at = len(deck["slides"])
        else:
            predecessor_id = str(payload.insert_after_slide_id)
            if predecessor_id in deck["slide_tombstones"]:
                return ClientResultDTO(status="SLIDE_DELETED")
            insert_at = next(
                (
                    index + 1
                    for index, active in enumerate(deck["slides"])
                    if active["metadata"]["slide_id"] == predecessor_id
                ),
                None,
            )
            if insert_at is None:
                return ClientResultDTO(status="NOT_FOUND")

        deck["slides"].insert(insert_at, slide)
        deck["deck_version"] += 1
        version = deck["deck_version"]
        return ClientResultDTO(
            status="CREATED",
            version=version,
            value={"deck_id": deck_id, "slide_id": slide_id, "deck_version": version, "slide_ids": [slide["metadata"]["slide_id"] for slide in deck["slides"]]},
        )

    def _reorder_slides(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = ReorderSlidesPayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        if payload.expected_deck_version != deck["deck_version"]:
            return self._structure_conflict(deck)

        requested_ids = [str(slide_id) for slide_id in payload.slide_ids]
        active_by_id = {slide["metadata"]["slide_id"]: slide for slide in deck["slides"]}
        if (
            len(requested_ids) != len(set(requested_ids))
            or len(requested_ids) != len(active_by_id)
            or set(requested_ids) != set(active_by_id)
        ):
            return ClientResultDTO(status="INVALID_ARGUMENT")

        deck["slides"] = [active_by_id[slide_id] for slide_id in requested_ids]
        deck["deck_version"] += 1
        version = deck["deck_version"]
        return ClientResultDTO(
            status="REORDERED",
            version=version,
            value={"deck_id": deck_id, "deck_version": version, "slide_ids": requested_ids},
        )

    def _delete_slide(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = DeleteSlidePayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        if payload.expected_deck_version != deck["deck_version"]:
            return self._structure_conflict(deck)

        slide_id = str(payload.slide_id)
        matched = next(
            (
                (index, slide)
                for index, slide in enumerate(deck["slides"])
                if slide["metadata"]["slide_id"] == slide_id
            ),
            None,
        )
        if matched is None:
            if slide_id in deck["slide_tombstones"]:
                return ClientResultDTO(status="SLIDE_DELETED")
            return ClientResultDTO(status="NOT_FOUND")

        index, slide = matched
        deck["slides"].pop(index)
        deck["slide_tombstones"][slide_id] = True
        for element in slide["elements"]:
            element_id = element["element_id"]
            deck["element_tombstones"][element_id] = True
            deck["locks"].pop(element_id, None)
            deck["fencing_counters"].setdefault(element_id, 0)
        deck["deck_version"] += 1
        version = deck["deck_version"]
        return ClientResultDTO(
            status="DELETED",
            version=version,
            value={"deck_id": deck_id, "slide_id": slide_id, "deck_version": version, "slide_ids": [slide["metadata"]["slide_id"] for slide in deck["slides"]]},
        )

    def _update_slide_settings(
        self,
        command: CommandEnvelopeDTO,
        *,
        committed_index: int | None,
    ) -> ClientResultDTO:
        payload = UpdateSlideSettingsPayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved

        slide_id = str(payload.slide_id)
        matched = next(
            (
                (index, candidate)
                for index, candidate in enumerate(deck["slides"])
                if candidate["metadata"]["slide_id"] == slide_id
            ),
            None,
        )
        if matched is None:
            if slide_id in deck["slide_tombstones"]:
                return ClientResultDTO(status="SLIDE_DELETED")
            return ClientResultDTO(status="NOT_FOUND")

        ordinal, slide = matched
        current_version = slide["metadata"].get("slide_settings_version", 0)

        if payload.expected_slide_settings_version != current_version:
            conflict: dict[str, Any] = {
                "slide_settings_version": current_version,
                "background": slide["metadata"]["background"],
                "deck_version": deck["deck_version"],
                "slide_id": slide_id,
                "ordinal": ordinal,
            }
            if committed_index is not None:
                conflict["min_commit_index"] = committed_index
            return ClientResultDTO(
                status="SLIDE_SETTINGS_VERSION_CONFLICT",
                version=current_version,
                value=conflict,
            )

        patch = payload.patch.model_dump(mode="json")
        slide["metadata"].update(patch)
        new_version = current_version + 1
        slide["metadata"]["slide_settings_version"] = new_version

        return ClientResultDTO(
            status="SLIDE_SETTINGS_UPDATED",
            version=new_version,
            value={
                "deck_id": deck_id,
                "slide_id": slide_id,
                "slide_settings_version": new_version,
                "background": slide["metadata"]["background"],
                "deck_version": deck["deck_version"],
                "ordinal": ordinal,
                "committed_index": committed_index,
            },
        )

    def get_local(self, key: str) -> str | None:
        return self._kv.get(key)

    def get_deck_preview(self, deck_id: str, *, applied_index: int) -> dict[str, Any]:
        deck = self._decks.get(deck_id)
        if deck is None:
            return {"status": "NOT_FOUND", "applied_index": applied_index}
        return {
            "status": "OK",
            "applied_index": applied_index,
            "deck_id": deck_id,
            "schema_version": deck.get("schema_version", 2),
            "deck_version": deck["deck_version"],
            "deck_settings_version": deck.get("deck_settings_version", 0),
            "metadata": deepcopy(deck["metadata"]),
            "slide_ids": [slide["metadata"]["slide_id"] for slide in deck["slides"]],
        }

    def get_slide(self, deck_id: str, slide_id: str, *, applied_index: int) -> dict[str, Any]:
        deck = self._decks.get(deck_id)
        if deck is None:
            return {"status": "NOT_FOUND", "applied_index": applied_index}
        matched = next(
            ((index, candidate) for index, candidate in enumerate(deck["slides"]) if candidate["metadata"]["slide_id"] == slide_id),
            None,
        )
        if matched is None:
            return {
                "status": "SLIDE_DELETED" if slide_id in deck["slide_tombstones"] else "NOT_FOUND",
                "applied_index": applied_index,
                "deck_id": deck_id,
                "deck_version": deck["deck_version"],
            }
        ordinal, slide = matched
        rendered_slide = deepcopy(slide)
        rendered_slide["metadata"]["ordinal"] = ordinal
        rendered_slide["elements"].sort(
            key=lambda element: (element["configuration"]["z_index"], element["creation_sequence"])
        )
        return {
            "status": "OK", "applied_index": applied_index, "deck_id": deck_id,
            "deck_version": deck["deck_version"], "slide": rendered_slide,
        }

    def get_element(self, deck_id: str, element_id: str, *, applied_index: int) -> dict[str, Any]:
        deck = self._decks.get(deck_id)
        if deck is None:
            return {"status": "NOT_FOUND", "applied_index": applied_index}
        for slide in deck["slides"]:
            element = next((candidate for candidate in slide["elements"] if candidate["element_id"] == element_id), None)
            if element is not None:
                return {
                    "status": "OK", "applied_index": applied_index, "deck_id": deck_id,
                    "slide_id": slide["metadata"]["slide_id"], "element_version": element["version"],
                    "element": deepcopy(element), "lock": deepcopy(deck["locks"].get(element_id)),
                }
        return {
            "status": "NOT_FOUND", "applied_index": applied_index, "deck_id": deck_id,
            "element_tombstoned": element_id in deck["element_tombstones"],
        }

    def read_lock_status(self, deck_id: str, element_id: str, *, applied_index: int) -> dict[str, Any]:
        observation = self.get_element(deck_id, element_id, applied_index=applied_index)
        return {
            "status": observation["status"], "applied_index": applied_index, "deck_id": deck_id,
            "element_id": element_id, "element_version": observation.get("element_version"),
            "lock": observation.get("lock"),
        }

    def snapshot(self) -> dict[str, Any]:
        state: dict[str, Any] = {"kv": dict(self._kv)}
        if self._decks:
            state["decks"] = self._decks
        canonical_bytes(state)
        return state


SharedPresentationStateMachine = KeyValueStateMachine
