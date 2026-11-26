"""Strict authentication catalog and replicated-state contracts for Stage E."""

from __future__ import annotations

from collections import Counter
import hmac
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from dto_base import StrictDTO, sha256_json

# Fields considered secret for redaction purposes (ADR-010 §10).
# Covers all nine explicit keys from the admission and journal layers.
AUTH_SECRET_KEYS: frozenset[str] = frozenset({
    "password", "access_token", "_auth", "token_digest",
    "verifier", "password_verifier", "salt_hex", "hash_hex",
})

_AUTH_SECRET_SUFFIXES = ("_verifier", "_hash", "_digest", "_salt")


def is_auth_secret_key(key: object) -> bool:
    """Return True when key names a field that must never enter logs or journals (ADR-010 §10)."""
    normalized = str(key).lower()
    return normalized in AUTH_SECRET_KEYS or normalized.endswith(_AUTH_SECRET_SUFFIXES)


class UserRecordDTO(StrictDTO):
    user_id: UUID
    first_name: StrictStr = Field(min_length=1, max_length=120)
    last_name: StrictStr = Field(min_length=1, max_length=120)
    email: StrictStr = Field(min_length=3, max_length=254)

    @field_validator("first_name", "last_name")
    @classmethod
    def require_trimmed_name(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("account names must be trimmed")
        return value

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized.count("@") != 1 or normalized.startswith("@") or normalized.endswith("@"):
            raise ValueError("email must contain one non-edge @")
        return normalized


class PasswordVerifierDTO(StrictDTO):
    algorithm: Literal["pbkdf2-hmac-sha256"]
    iterations: StrictInt = Field(ge=600_000)
    salt_hex: StrictStr = Field(pattern=r"^[0-9a-f]{32,64}$")
    hash_hex: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")


class StoredUserDTO(StrictDTO):
    profile: UserRecordDTO
    verifier: PasswordVerifierDTO


class AccountCatalogDTO(StrictDTO):
    schema_version: Literal[1]
    users: list[StoredUserDTO] = Field(min_length=1, max_length=32)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("catalog schema version must be integer 1")
        return value

    @model_validator(mode="after")
    def require_unique_identities(self) -> "AccountCatalogDTO":
        ids = [user.profile.user_id for user in self.users]
        emails = [user.profile.email for user in self.users]
        if len(set(ids)) != len(ids):
            raise ValueError("catalog user IDs must be unique")
        if len(set(emails)) != len(emails):
            raise ValueError("catalog emails must be unique")
        return self


class IssueAccessTokenPayloadDTO(StrictDTO):
    """Server-generated payload for a password-admitted internal issuance command.

    The external login caller never supplies credential_id or token_digest.
    After successful password admission, the current leader generates the
    credential identity and raw bearer token, hashes that token, and proposes
    only this non-secret verifier material through Raft.
    """

    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")


class ActorStampDTO(StrictDTO):
    """Server-stamped actor and transient credential reference on a command.

    All fields are absent on pre-activation legacy commands or present together
    on authenticated commands. The credential reference is proof metadata, not
    part of the logical request fingerprint.
    """

    authenticated_actor_user_id: UUID | None = None
    credential_id: UUID | None = None
    credential_generation: StrictInt | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def require_complete_stamp(self) -> "ActorStampDTO":
        fields = (
            self.authenticated_actor_user_id,
            self.credential_id,
            self.credential_generation,
        )
        if any(value is not None for value in fields) and any(value is None for value in fields):
            raise ValueError("actor stamp fields must be supplied together")
        return self


class RevokeAccessTokenPayloadDTO(ActorStampDTO):
    client_id: UUID
    expected_credential_id: UUID
    expected_generation: StrictInt = Field(ge=1)

    @model_validator(mode="after")
    def require_actor_stamp(self) -> "RevokeAccessTokenPayloadDTO":
        if self.authenticated_actor_user_id is None:
            raise ValueError("revocation requires an authenticated actor stamp")
        return self


def issue_access_token_command_fingerprint(
    *,
    client_id: str | UUID,
    payload: IssueAccessTokenPayloadDTO | dict[str, object],
) -> str:
    """Canonical identity of one internal credential-issuance command.

    Credential identity and token digest are intentionally included: they are
    server-generated replicated verifier material, and changing either creates
    a different issuance command. The raw bearer token is never an input.
    """

    issuance = (
        payload
        if isinstance(payload, IssueAccessTokenPayloadDTO)
        else IssueAccessTokenPayloadDTO.model_validate(payload)
    )
    canonical_client_id = str(issuance.client_id)
    if str(client_id) != canonical_client_id:
        raise ValueError("issuance client_id must match its payload")
    return sha256_json(
        {
            "client_id": canonical_client_id,
            "session_id": None,
            "deck_id": None,
            "operation": "issue_access_token",
            "payload": issuance.model_dump(mode="json"),
            "blocks": [],
        }
    )


class CredentialRecordDTO(StrictDTO):
    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    credential_generation: StrictInt = Field(ge=1)
    revoked: StrictBool = False


class RetiredCredentialDTO(StrictDTO):
    """Permanent tombstone for one previously issued credential."""

    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    credential_generation: StrictInt = Field(ge=1)
    retirement_reason: Literal["ROTATED", "REVOKED"]


class UpgradeAuthSchemaV2PayloadDTO(StrictDTO):
    """Internal, non-user Raft migration request."""

    schema_version: Literal[2]
    catalog_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 2:
            raise ValueError("upgrade target must be integer 2")
        return value


def auth_schema_upgrade_request_id(fingerprint: str, collision_ordinal: int = 0) -> str:
    """Stable internal command identity; suffix avoids preserved dedup collisions."""

    if type(collision_ordinal) is not int or collision_ordinal < 0:
        raise ValueError("schema upgrade collision ordinal must be a nonnegative integer")
    base = f"upgrade-auth-schema-v2:{fingerprint}"
    return base if collision_ordinal == 0 else f"{base}:{collision_ordinal}"


def catalog_projection(catalog: AccountCatalogDTO) -> dict[str, object]:
    """Return the sole canonical input to the catalog fingerprint."""

    users = sorted(catalog.users, key=lambda user: str(user.profile.user_id))
    return {
        "schema_version": catalog.schema_version,
        "users": [user.model_dump(mode="json") for user in users],
    }


def catalog_fingerprint(catalog: AccountCatalogDTO) -> str:
    return sha256_json(catalog_projection(catalog))


def auth_catalog_activation_request_id(
    catalog_fingerprint_value: str,
    collision_ordinal: int = 0,
) -> str:
    """Build one deterministic internal identity for catalog activation."""

    if type(collision_ordinal) is not int or collision_ordinal < 0:
        raise ValueError("activation collision ordinal must be a nonnegative integer")
    base = f"initialize-auth-catalog-v1:{catalog_fingerprint_value}"
    return base if collision_ordinal == 0 else f"{base}:{collision_ordinal}"


class InitializeAuthCatalogPayloadDTO(StrictDTO):
    schema_version: Literal[1]
    catalog_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    users: list[StoredUserDTO] = Field(min_length=1, max_length=32)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("initializer schema version must be integer 1")
        return value

    @model_validator(mode="after")
    def require_valid_catalog_fingerprint(self) -> "InitializeAuthCatalogPayloadDTO":
        catalog = AccountCatalogDTO(schema_version=self.schema_version, users=self.users)
        if not hmac.compare_digest(self.catalog_fingerprint, catalog_fingerprint(catalog)):
            raise ValueError("catalog fingerprint does not match users")
        return self


class AuthStateBaseDTO(StrictDTO):
    """Version-independent catalog, bindings, and active credential invariants."""

    schema_version: StrictInt
    catalog_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    enforced_from_index: StrictInt = Field(ge=1)
    users: dict[str, StoredUserDTO]
    email_index: dict[str, UUID]
    client_bindings: dict[str, UUID] = Field(default_factory=dict)
    active_credentials: dict[str, CredentialRecordDTO] = Field(default_factory=dict)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value not in (1, 2):
            raise ValueError("auth schema version must be integer 1 or 2")
        return value

    @model_validator(mode="after")
    def require_consistent_indexes(self) -> "AuthStateBaseDTO":
        parsed_users: dict[UUID, StoredUserDTO] = {}
        for key, user in self.users.items():
            try:
                parsed_key = UUID(key)
            except (TypeError, ValueError) as error:
                raise ValueError("auth user map keys must be UUIDs") from error
            if key != str(parsed_key):
                raise ValueError("auth user map keys must use canonical UUID text")
            if parsed_key != user.profile.user_id:
                raise ValueError("auth user map key does not match profile")
            parsed_users[parsed_key] = user
        expected_email_index = {
            user.profile.email: user.profile.user_id for user in parsed_users.values()
        }
        if self.email_index != expected_email_index:
            raise ValueError("auth email index does not match users")
        catalog = AccountCatalogDTO(schema_version=1, users=list(parsed_users.values()))
        if not hmac.compare_digest(self.catalog_fingerprint, catalog_fingerprint(catalog)):
            raise ValueError("auth catalog fingerprint does not match users")
        for key, user_id in self.client_bindings.items():
            try:
                client_id = UUID(key)
            except (TypeError, ValueError) as error:
                raise ValueError("client binding keys must be UUIDs") from error
            if key != str(client_id):
                raise ValueError("client binding keys must use canonical UUID text")
            if user_id not in parsed_users:
                raise ValueError("client binding references unknown user")
            credential = self.active_credentials.get(key)
            if credential is not None and (
                credential.client_id != client_id or credential.user_id != user_id
            ):
                raise ValueError("active credential does not match client binding")
        for key, credential in self.active_credentials.items():
            try:
                client_id = UUID(key)
            except (TypeError, ValueError) as error:
                raise ValueError("active credential keys must be UUIDs") from error
            if key != str(client_id):
                raise ValueError("active credential keys must use canonical UUID text")
            if credential.client_id != client_id or key not in self.client_bindings:
                raise ValueError("active credential requires matching client binding")
        return self


class AuthStateV1DTO(AuthStateBaseDTO):
    """Legacy state; validated exactly as v1, never upgraded on deserialization."""

    schema_version: Literal[1]


class AuthStateDTO(AuthStateBaseDTO):
    """Auth v2 records all retired credentials and prohibits lifetime reuse."""

    schema_version: Literal[2]
    retired_credentials: dict[str, RetiredCredentialDTO]
    # Legacy v1 collisions are retained without granting bearer authority.
    # Optional only to support already-persisted early v2 snapshots that
    # predate this backward-compatible field.
    legacy_quarantined_credentials: dict[str, CredentialRecordDTO] = Field(default_factory=dict)

    @model_validator(mode="after")
    def require_unique_lifetime_credential_history(self) -> "AuthStateDTO":
        used_ids: set[UUID] = set()
        used_digests: set[str] = set()
        for active in self.active_credentials.values():
            if active.credential_id in used_ids or active.token_digest in used_digests:
                raise ValueError("active credentials reuse an identity or digest")
            used_ids.add(active.credential_id)
            used_digests.add(active.token_digest)

        for key, retired in self.retired_credentials.items():
            try:
                credential_id = UUID(key)
            except (TypeError, ValueError) as error:
                raise ValueError("retired credential key must be a UUID") from error
            if key != str(credential_id) or credential_id != retired.credential_id:
                raise ValueError("retired credential key must match canonical credential ID")
            if self.client_bindings.get(str(retired.client_id)) != retired.user_id:
                raise ValueError("retired credential must reference its bound user")
            current = self.active_credentials.get(str(retired.client_id))
            if current is not None and retired.credential_generation >= current.credential_generation:
                raise ValueError("retired credential generation must precede current generation")
            if credential_id in used_ids or retired.token_digest in used_digests:
                raise ValueError("retired credentials reuse an identity or digest")
            used_ids.add(credential_id)
            used_digests.add(retired.token_digest)

        # V1 could have duplicate identifiers/digests. Every participant in
        # such a collision is quarantined and unusable, but the original
        # verifier, client binding, and generation must remain durable so that
        # the old secret cannot ever be reissued after reauthentication.
        for client_id, quarantined in self.legacy_quarantined_credentials.items():
            try:
                parsed_client = UUID(client_id)
            except (TypeError, ValueError) as error:
                raise ValueError("quarantined client key must be a UUID") from error
            if client_id != str(parsed_client) or parsed_client != quarantined.client_id:
                raise ValueError("quarantined client key must match its record")
            if self.client_bindings.get(client_id) != quarantined.user_id:
                raise ValueError("quarantined credential must reference its bound user")
            current = self.active_credentials.get(client_id)
            if current is not None and current.credential_generation <= quarantined.credential_generation:
                raise ValueError("current credential must advance quarantined generation")
            if quarantined.credential_id in used_ids or quarantined.token_digest in used_digests:
                raise ValueError("live or retired credential reuses quarantined material")
            # Duplicate values *within quarantine* are intentional v1 evidence:
            # they are never treated as live credentials or unique v2 issuances.
        return self


def upgrade_auth_state_v2(legacy_raw: object) -> dict[str, object]:
    """Deterministically quarantine colliding v1 records in a committed upgrade.

    This pure projection preserves every old identifier, digest and generation,
    but invalidates ALL members of any legacy collision. Safe, unique existing
    credentials remain active. Quarantined clients reauthenticate normally.
    Previously rotated-out v1 history cannot be reconstructed.
    """
    legacy = AuthStateV1DTO.model_validate(legacy_raw).model_dump(mode="json")
    active = legacy["active_credentials"]
    id_counts = Counter(record["credential_id"] for record in active.values())
    digest_counts = Counter(record["token_digest"] for record in active.values())
    conflicting = {
        client_id for client_id, record in active.items()
        if id_counts[record["credential_id"]] > 1
        or digest_counts[record["token_digest"]] > 1
    }
    migrated = {
        **legacy,
        "schema_version": 2,
        "active_credentials": {
            cid: active[cid] for cid in sorted(active) if cid not in conflicting
        },
        "retired_credentials": {},
        "legacy_quarantined_credentials": {
            cid: active[cid] for cid in sorted(conflicting)
        },
    }
    return AuthStateDTO.model_validate(migrated).model_dump(mode="json")


def initial_auth_state(
    catalog: AccountCatalogDTO,
    *,
    enforced_from_index: int,
) -> dict[str, object]:
    """Build the deterministic replicated state for one validated catalog."""

    users = sorted(catalog.users, key=lambda user: str(user.profile.user_id))
    raw = {
        "schema_version": 2,
        "catalog_fingerprint": catalog_fingerprint(catalog),
        "enforced_from_index": enforced_from_index,
        "users": {
            str(user.profile.user_id): user.model_dump(mode="json") for user in users
        },
        "email_index": {
            user.profile.email: str(user.profile.user_id)
            for user in sorted(users, key=lambda user: user.profile.email)
        },
        "client_bindings": {},
        "active_credentials": {},
        "retired_credentials": {},
        "legacy_quarantined_credentials": {},
    }
    return AuthStateDTO.model_validate(raw).model_dump(mode="json")


def validate_auth_state(raw: object) -> AuthStateV1DTO | AuthStateDTO:
    """Parse persisted auth state into its versioned DTO without upgrading it."""
    if isinstance(raw, dict) and type(raw.get("schema_version")) is int and raw["schema_version"] == 1:
        return AuthStateV1DTO.model_validate(raw)
    return AuthStateDTO.model_validate(raw)


def auth_state_to_dict(state: AuthStateV1DTO | AuthStateDTO) -> dict[str, object]:
    """Return the canonical persisted shape, preserving early-v2 omission."""
    serialized = state.model_dump(mode="json")
    if (
        isinstance(state, AuthStateDTO)
        and "legacy_quarantined_credentials" not in state.model_fields_set
    ):
        serialized.pop("legacy_quarantined_credentials", None)
    return serialized


