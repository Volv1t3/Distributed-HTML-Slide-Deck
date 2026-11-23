"""Pure fixed-membership Raft core with injected infrastructure boundaries."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID

from persistence import RaftPersistence, new_persisted_state


Transport = Callable[[str, dict[str, Any]], None]
Clock = Callable[[], float]


class RaftEngine:
    """Protocol core shell; later transitions remain independent of the starter runtime."""

    def __init__(
        self,
        *,
        node_slot: str,
        voters: Iterable[str],
        node_uuid: UUID,
        storage: RaftPersistence,
        send: Transport,
        clock: Clock,
    ) -> None:
        voter_set = frozenset(voters)
        if len(voter_set) < 3:
            raise ValueError("Raft requires at least three fixed voters")
        if node_slot not in voter_set:
            raise ValueError("local node slot must be in the fixed voter set")
        if any(not isinstance(slot, str) or not slot for slot in voter_set):
            raise ValueError("voter slots must be non-empty strings")
        self.node_slot = node_slot
        self.voters = voter_set
        self.quorum = len(voter_set) // 2 + 1
        self.node_uuid = node_uuid
        self.storage = storage
        self.send = send
        self.clock = clock
        self.state = storage.load() or new_persisted_state(node_uuid, self.membership_fingerprint())

    def membership_fingerprint(self) -> str:
        from dto_base import sha256_json

        return sha256_json(sorted(self.voters))
