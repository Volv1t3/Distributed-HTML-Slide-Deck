"""Repository-owned deterministic account catalog loader."""

from __future__ import annotations

import json
from pathlib import Path

from auth_dto import AccountCatalogDTO


CATALOG_PATH = Path(__file__).with_name("fixtures") / "auth" / "catalog-v1.json"


def load_account_catalog(path: Path = CATALOG_PATH) -> AccountCatalogDTO:
    """Load and validate the frozen catalog independently of the process CWD."""

    raw = json.loads(path.read_text(encoding="utf-8"))
    catalog = AccountCatalogDTO.model_validate(raw)
    return catalog.model_copy(
        update={"users": sorted(catalog.users, key=lambda user: user.profile.email)}
    )
