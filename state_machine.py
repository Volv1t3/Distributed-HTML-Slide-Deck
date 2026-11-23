"""Deterministic, Raft-neutral Stage A key-value state machine."""

from __future__ import annotations

from typing import Any

from command_dto import ClientResultDTO, CommandEnvelopeDTO, KVPutPayloadDTO, validate_committed_command
from dto_base import canonical_bytes


class KeyValueStateMachine:
    def __init__(self, snapshot: dict[str, Any] | None = None) -> None:
        initial = snapshot if snapshot is not None else {"kv": {}}
        canonical_bytes(initial)
        kv = initial.get("kv")
        if not isinstance(kv, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in kv.items()
        ):
            raise ValueError("Stage A state-machine snapshot must contain string KV values")
        self._kv = dict(kv)

    def apply(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        validate_committed_command(command)
        if command.operation == "raft.noop":
            return ClientResultDTO(status="OK")
        payload = KVPutPayloadDTO.model_validate(command.payload)
        self._kv[payload.key] = payload.value
        return ClientResultDTO(status="OK", value="ok")

    def get_local(self, key: str) -> str | None:
        return self._kv.get(key)

    def snapshot(self) -> dict[str, dict[str, str]]:
        return {"kv": dict(self._kv)}
