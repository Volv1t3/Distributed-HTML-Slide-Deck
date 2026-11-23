"""Injected persistence boundary for the pure Raft core."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from raft_dto import PersistedRaftStateDTO, RaftHardStateDTO


class RaftPersistence(Protocol):
    def load(self) -> PersistedRaftStateDTO | None: ...

    def save(self, state: PersistedRaftStateDTO) -> None: ...


def new_persisted_state(node_uuid: UUID, membership_fingerprint: str) -> PersistedRaftStateDTO:
    """Create the first durable snapshot for a validated fixed voter set."""

    return PersistedRaftStateDTO(
        storage_version=1,
        node_uuid=node_uuid,
        incarnation=1,
        membership_fingerprint=membership_fingerprint,
        raft=RaftHardStateDTO(current_term=0),
        fsm={"kv": {}},
    )


class InMemoryPersistence:
    """Strict test double that simulates a full atomic snapshot replacement."""

    def __init__(self) -> None:
        self._raw: dict[str, object] | None = None

    def load(self) -> PersistedRaftStateDTO | None:
        if self._raw is None:
            return None
        return PersistedRaftStateDTO.model_validate(self._raw)

    def save(self, state: PersistedRaftStateDTO) -> None:
        self._raw = state.model_dump(mode="json")


class NodeDiskPersistence:
    """Persist the core snapshot through the starter node's local disk API."""

    key = "raft_state"

    def __init__(self, disk: dict[str, Any], flush: Callable[[], None]) -> None:
        self._disk = disk
        self._flush = flush

    def load(self) -> PersistedRaftStateDTO | None:
        raw = self._disk.get(self.key)
        if raw is None:
            return None
        return PersistedRaftStateDTO.model_validate(raw)

    def save(self, state: PersistedRaftStateDTO) -> None:
        self._disk[self.key] = state.model_dump(mode="json")
        self._flush()
