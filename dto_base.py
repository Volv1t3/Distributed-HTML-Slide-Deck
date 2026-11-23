"""Shared strict JSON DTO primitives for the Stage A protocol boundary."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict


class StrictDTO(BaseModel):
    """Pydantic base model that never silently accepts extra wire fields."""

    model_config = ConfigDict(extra="forbid", validate_default=True)


def canonical_bytes(value: Any) -> bytes:
    """Encode JSON-compatible data deterministically, rejecting NaN and infinity."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    """Return the SHA-256 digest of the canonical JSON representation."""

    return hashlib.sha256(canonical_bytes(value)).hexdigest()
