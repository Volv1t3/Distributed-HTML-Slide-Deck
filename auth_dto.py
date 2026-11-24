"""Strict authentication catalog and replicated-state contracts for Stage E."""

from __future__ import annotations

import hashlib
import hmac
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from dto_base import StrictDTO, sha256_json


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


class AuthStateDTO(StrictDTO):
    schema_version: Literal[1]
    catalog_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    enforced_from_index: StrictInt = Field(ge=1)
    users: dict[str, StoredUserDTO]
    email_index: dict[str, UUID]
    client_bindings: dict[str, UUID] = Field(default_factory=dict)
    active_credentials: dict[str, CredentialRecordDTO] = Field(default_factory=dict)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("auth schema version must be integer 1")
        return value

    @model_validator(mode="after")
    def require_consistent_indexes(self) -> "AuthStateDTO":
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


def initial_auth_state(
    catalog: AccountCatalogDTO,
    *,
    enforced_from_index: int,
) -> dict[str, object]:
    """Build the deterministic replicated state for one validated catalog."""

    users = sorted(catalog.users, key=lambda user: str(user.profile.user_id))
    raw = {
        "schema_version": 1,
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
    }
    return AuthStateDTO.model_validate(raw).model_dump(mode="json")


def validate_auth_state(raw: object) -> dict[str, object]:
    return AuthStateDTO.model_validate(raw).model_dump(mode="json")


def verify_password(password: str, verifier: PasswordVerifierDTO) -> bool:
    """Verify a candidate without exposing or changing the frozen verifier."""

    if not isinstance(password, str):
        return False
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(verifier.salt_hex),
        verifier.iterations,
    )
    return hmac.compare_digest(derived.hex(), verifier.hash_hex)
