"""Deterministic committed state for Stage A compatibility and Stage B publication."""

from __future__ import annotations

from typing import Any

from command_dto import (
    ClientResultDTO,
    CommandEnvelopeDTO,
    CreateSlidePayloadDTO,
    DeleteSlidePayloadDTO,
    KVPutPayloadDTO,
    PublishDeckPayloadDTO,
    ReorderSlidesPayloadDTO,
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

    def apply(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
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
        self._decks[deck_id] = {
            "schema_version": presentation["schema_version"],
            "metadata": metadata,
            "slides": presentation["slides"],
            "deck_version": 0,
            "slide_tombstones": {},
            "element_tombstones": {},
            "locks": {},
            "fencing_counters": {
                element["element_id"]: 0
                for slide in presentation["slides"]
                for element in slide["elements"]
            },
        }
        return ClientResultDTO(
            status="PUBLISHED",
            value={"deck_id": deck_id, "deck_version": 0},
            version=0,
        )

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

    def _create_slide(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = CreateSlidePayloadDTO.model_validate(command.payload)
        resolved = self._structural_deck(command)
        if isinstance(resolved, ClientResultDTO):
            return resolved
        deck_id, deck = resolved
        if payload.expected_deck_version != deck["deck_version"]:
            return self._structure_conflict(deck)

        slide = payload.slide.model_dump(mode="json")
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
            value={"deck_id": deck_id, "slide_id": slide_id, "deck_version": version},
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
            value={"deck_id": deck_id, "slide_id": slide_id, "deck_version": version},
        )

    def get_local(self, key: str) -> str | None:
        return self._kv.get(key)

    def snapshot(self) -> dict[str, Any]:
        state: dict[str, Any] = {"kv": dict(self._kv)}
        if self._decks:
            state["decks"] = self._decks
        canonical_bytes(state)
        return state


SharedPresentationStateMachine = KeyValueStateMachine
