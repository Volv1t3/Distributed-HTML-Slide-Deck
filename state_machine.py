"""Deterministic committed state for Stage A compatibility and Stage B publication."""

from __future__ import annotations

from typing import Any

from command_dto import (
    ClientResultDTO,
    CommandEnvelopeDTO,
    KVPutPayloadDTO,
    PublishDeckPayloadDTO,
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

    def apply(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        validate_committed_command(command)
        if command.operation == "raft.noop":
            return ClientResultDTO(status="OK")
        if command.operation == "kv_put":
            payload = KVPutPayloadDTO.model_validate(command.payload)
            self._kv[payload.key] = payload.value
            return ClientResultDTO(status="OK", value="ok")

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
        }
        return ClientResultDTO(
            status="PUBLISHED",
            value={"deck_id": deck_id, "deck_version": 0},
            version=0,
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
