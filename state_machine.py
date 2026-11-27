"""Deterministic committed state for Stage A compatibility and Stage B publication."""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

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
    GrantAccessPayloadDTO,
    RevokeAccessPayloadDTO,
    PresentationMetadataDTO,
    MigrateLegacyDeckPayloadDTO,
    command_fingerprint,
    validate_committed_command,
)
from auth_dto import (
    AccountCatalogDTO,
    ActorStampDTO,
    InitializeAuthCatalogPayloadDTO,
    IssueAccessTokenPayloadDTO,
    RevokeAccessTokenPayloadDTO,
    AuthStateDTO,
    AuthStateV1DTO,
    auth_state_to_dict,
    UpgradeAuthSchemaV2PayloadDTO,
    upgrade_auth_state_v2,
    initial_auth_state,
    validate_auth_state,
)
from dto_base import canonical_bytes


@dataclass(frozen=True)
class CommittedPublicationEvidence:
    """Immutable provenance bridge for ``migrate_legacy_deck`` (ADR-018 §4.1.6).

    Supplied only by the Raft engine at apply time, from the replica's own
    retained committed log entry and the request-result record of the named
    original request. Every voter holds the same committed prefix and the same
    deterministic request results, so every voter derives identical evidence.
    Canonical JSON bytes keep the context immutable while it crosses the seam.
    """

    committed_index: int
    command_json: bytes
    fingerprint: str
    result_json: bytes

    @classmethod
    def from_records(
        cls, *, committed_index: int, command: Mapping[str, Any], fingerprint: str,
        result: Mapping[str, Any],
    ) -> "CommittedPublicationEvidence":
        return cls(
            committed_index=committed_index,
            command_json=canonical_bytes(dict(command)),
            fingerprint=fingerprint,
            result_json=canonical_bytes(dict(result)),
        )


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
        auth = initial.get("auth")
        policy = initial.get("presentation_schema_policy")
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
        if policy is not None and (
            not isinstance(policy, dict)
            or set(policy) != {"schema_version", "v3_enforced_from_index"}
            or type(policy["schema_version"]) is not int
            or policy["schema_version"] != 1
            or type(policy["v3_enforced_from_index"]) is not int
            or policy["v3_enforced_from_index"] < 1
        ):
            raise ValueError("invalid presentation schema policy")
        self._presentation_schema_policy = deepcopy(policy)
        self._auth_raw = deepcopy(auth) if auth is not None else None
        self._auth_model: AuthStateV1DTO | AuthStateDTO | None = None
        self._auth: dict[str, Any] | None = None
        try:
            if auth is not None:
                self._auth_model = validate_auth_state(auth)
                self._auth = auth_state_to_dict(self._auth_model)
        except ValidationError:
            # Preserve an unparseable persisted auth blob without crashing the
            # deterministic apply path for unrelated Stage A-D commands.
            self._auth_model = None
            self._auth = None
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

    def _replace_auth_state(self, raw: dict[str, Any]) -> None:
        """Validate and atomically replace the typed and serialized auth views."""
        parsed = validate_auth_state(raw)
        self._auth_model = parsed
        self._auth = auth_state_to_dict(parsed)
        self._auth_raw = deepcopy(self._auth)

    def apply(
        self,
        command: CommandEnvelopeDTO,
        *,
        committed_index: int | None = None,
        provenance: CommittedPublicationEvidence | None = None,
    ) -> ClientResultDTO:
        validate_committed_command(command)
        if command.operation == "raft.noop":
            return ClientResultDTO(status="OK")
        if command.operation == "kv_put":
            payload = KVPutPayloadDTO.model_validate(command.payload)
            self._kv[payload.key] = payload.value
            return ClientResultDTO(status="OK", value="ok")
        if command.operation == "initialize_auth_catalog":
            if committed_index is None:
                raise ValueError("catalog initialization requires committed_index")
            payload = InitializeAuthCatalogPayloadDTO.model_validate(command.payload)
            if self._auth_raw is not None:
                if (
                    self._auth is not None
                    and self._auth["catalog_fingerprint"] == payload.catalog_fingerprint
                ):
                    return ClientResultDTO(status="AUTH_CATALOG_ALREADY_INITIALIZED")
                return ClientResultDTO(status="AUTH_CATALOG_MISMATCH")
            catalog = payload.model_dump(exclude={"catalog_fingerprint"})
            self._replace_auth_state(initial_auth_state(
                AccountCatalogDTO.model_validate(catalog),
                enforced_from_index=committed_index + 1,
            ))
            return ClientResultDTO(status="AUTH_CATALOG_INITIALIZED")

        if command.operation == "upgrade_auth_schema_v2":
            payload = UpgradeAuthSchemaV2PayloadDTO.model_validate(command.payload)
            if self._auth is None:
                return ClientResultDTO(status="AUTH_CATALOG_NOT_READY")
            if self._auth["catalog_fingerprint"] != payload.catalog_fingerprint:
                return ClientResultDTO(status="AUTH_CATALOG_MISMATCH")
            if self._auth["schema_version"] == 2:
                return ClientResultDTO(status="AUTH_SCHEMA_ALREADY_V2")
            upgraded = upgrade_auth_state_v2(self._auth)
            self._replace_auth_state(upgraded)
            return ClientResultDTO(
                status="AUTH_SCHEMA_UPGRADED",
                details={"quarantined_clients": len(upgraded["legacy_quarantined_credentials"])},
            )
        if command.operation == "activate_presentation_schema_v3":
            if committed_index is None:
                raise ValueError("presentation activation requires committed_index")
            if self._presentation_schema_policy is not None:
                return ClientResultDTO(status="PRESENTATION_SCHEMA_ALREADY_ACTIVE")
            if self._auth_model is None or self._auth_model.schema_version != 2:
                return ClientResultDTO(status="AUTH_CATALOG_NOT_READY")
            self._presentation_schema_policy = {
                "schema_version": 1, "v3_enforced_from_index": committed_index + 1,
            }
            return ClientResultDTO(status="PRESENTATION_SCHEMA_ACTIVATED")
        if command.operation == "migrate_legacy_deck":
            return self._migrate_legacy_deck(command, committed_index, provenance)
        if command.operation == "issue_access_token":
            return self._issue_access_token(command)
        if command.operation == "revoke_access_token":
            return self._revoke_access_token(command)
        if command.operation == "create_slide":
            return self._create_slide(command, committed_index=committed_index)
        if command.operation == "delete_slide":
            return self._delete_slide(command, committed_index=committed_index)
        if command.operation == "reorder_slides":
            return self._reorder_slides(command, committed_index=committed_index)
        if command.operation == "create_element":
            return self._create_element(command, committed_index=committed_index)
        if command.operation == "acquire_lock":
            return self._acquire_lock(command, committed_index=committed_index)
        if command.operation == "release_lock":
            return self._release_lock(command, committed_index=committed_index)
        if command.operation == "renew_lock":
            return self._renew_lock(command, committed_index=committed_index)
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
        if command.operation == "grant_access":
            return self._grant_access(command, committed_index=committed_index)
        if command.operation == "revoke_access":
            return self._revoke_access(command, committed_index=committed_index)

        payload = PublishDeckPayloadDTO.model_validate(command.payload)

        version = payload.presentation.schema_version
        fence = (self._presentation_schema_policy or {}).get("v3_enforced_from_index")
        if version == 3 and (fence is None or committed_index is None or committed_index < fence):
            return ClientResultDTO(status="PRESENTATION_SCHEMA_UNAVAILABLE")
        if version == 2 and fence is not None and (committed_index is None or committed_index >= fence):
            return ClientResultDTO(status="UNSUPPORTED_PRESENTATION_SCHEMA")

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

        if version == 3:
            owner = payload.presentation.metadata.owner_user_id
            if self._auth_model is None or str(owner) not in self._auth_model.users:
                return ClientResultDTO(status="ACCESS_DENIED")
            if payload.authenticated_actor_user_id != owner:
                return ClientResultDTO(status="OWNER_MISMATCH")

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
        if version == 3:
            self._decks[deck_id]["access"] = {
                "schema_version": 1, "access_version": 0, "grants": {},
            }
        return ClientResultDTO(
            status="PUBLISHED",
            value={"deck_id": deck_id, "deck_version": 0, "slide_ids": [slide["metadata"]["slide_id"] for slide in presentation["slides"]]},
            version=0,
        )

    def _migrate_legacy_deck(
        self,
        command: CommandEnvelopeDTO,
        committed_index: int | None,
        provenance: CommittedPublicationEvidence | None,
    ) -> ClientResultDTO:
        """Atomically add owner/access/v3 to a provably owned historical v2 deck.

        The command payload is a claim, never proof. Ownership is assigned only
        when replica-derived evidence shows a successful committed v2
        ``publish_deck`` of this deck whose server-stamped actor was validated
        at apply and is the claimed, currently registered owner. Every other
        outcome leaves the deck byte-for-byte unchanged (quarantined).
        """
        if committed_index is None:
            raise ValueError("legacy deck migration requires committed_index")
        payload = MigrateLegacyDeckPayloadDTO.model_validate(command.payload)
        fence = (self._presentation_schema_policy or {}).get("v3_enforced_from_index")
        if fence is None or committed_index < fence:
            return ClientResultDTO(status="PRESENTATION_SCHEMA_UNAVAILABLE")
        deck_id = str(command.deck_id)
        owner = str(payload.owner_user_id)
        deck = self._decks.get(deck_id)
        if deck is None:
            return ClientResultDTO(status="LEGACY_DECK_NOT_FOUND")
        if deck.get("schema_version") == 3:
            if deck.get("metadata", {}).get("owner_user_id") == owner:
                return ClientResultDTO(status="LEGACY_DECK_ALREADY_MIGRATED")
            return ClientResultDTO(status="LEGACY_MIGRATION_CONFLICT")
        if deck.get("schema_version") != 2:
            return ClientResultDTO(status="LEGACY_PROVENANCE_REJECTED")
        if provenance is None:
            return ClientResultDTO(status="LEGACY_PROVENANCE_UNAVAILABLE")
        if not self._legacy_provenance_proves_owner(
            provenance, payload, deck_id=deck_id, owner=owner, committed_index=committed_index,
        ):
            return ClientResultDTO(status="LEGACY_PROVENANCE_REJECTED")
        deck["schema_version"] = 3
        deck["metadata"]["owner_user_id"] = owner
        deck["access"] = {"schema_version": 1, "access_version": 0, "grants": {}}
        return ClientResultDTO(status="LEGACY_DECK_MIGRATED", value={"deck_id": deck_id})

    def _legacy_provenance_proves_owner(
        self,
        evidence: CommittedPublicationEvidence,
        claim: MigrateLegacyDeckPayloadDTO,
        *,
        deck_id: str,
        owner: str,
        committed_index: int,
    ) -> bool:
        """Cross-check replica evidence against the claim and current state."""
        if not 1 <= evidence.committed_index < committed_index:
            return False
        try:
            original = CommandEnvelopeDTO.model_validate(json.loads(evidence.command_json))
            result = ClientResultDTO.model_validate(json.loads(evidence.result_json))
            if command_fingerprint(original) != evidence.fingerprint:
                return False
            if (original.client_id, original.request_id) != (
                claim.original_client_id, claim.original_request_id,
            ):
                return False
            if original.operation != "publish_deck" or str(original.deck_id) != deck_id:
                return False
            publication = PublishDeckPayloadDTO.model_validate(original.payload)
        except (ValidationError, ValueError):
            return False
        if publication.presentation.schema_version != 2:
            return False
        if str(publication.presentation.metadata.deck_id) != deck_id:
            return False
        if result.status != "PUBLISHED" or not isinstance(result.value, dict):
            return False
        if result.value.get("deck_id") != deck_id:
            return False
        # A PUBLISHED result for an actor-stamped command means Stage E
        # validated that credential at original apply (_authorize_external_mutation).
        actor = publication.authenticated_actor_user_id
        if actor is None or publication.credential_id is None or str(actor) != owner:
            return False
        return self._auth_model is not None and owner in self._auth_model.users

    def _authorize_external_mutation(
        self,
        command: CommandEnvelopeDTO,
        actor: ActorStampDTO,
        committed_index: int | None,
        *,
        owner_only: bool = False,
    ) -> ClientResultDTO | None:
        """Check a server-stamped actor before any external mutation.

        Actorless commands remain replayable only below the replicated
        ``enforced_from_index`` migration boundary.
        """
        actor_user_id = actor.authenticated_actor_user_id
        auth = self._auth_model
        if actor_user_id is not None:
            if auth is None:
                return ClientResultDTO(status="UNAUTHENTICATED")
            record = auth.active_credentials.get(str(command.client_id))
            if (
                record is None
                or record.revoked
                or record.user_id != actor_user_id
                or record.credential_id != actor.credential_id
                or record.credential_generation != actor.credential_generation
            ):
                return ClientResultDTO(status="UNAUTHENTICATED")
        elif (
            auth is not None
            and committed_index is not None
            and committed_index >= auth.enforced_from_index
        ):
            return ClientResultDTO(status="UNAUTHENTICATED")
        fence = (self._presentation_schema_policy or {}).get("v3_enforced_from_index")
        if (
            command.operation != "publish_deck"
            and command.deck_id is not None
            and fence is not None
            and committed_index is not None
            and committed_index >= fence
        ):
            deck = self._decks.get(str(command.deck_id))
            if deck is None or deck.get("schema_version") != 3:
                return ClientResultDTO(status="ACCESS_DENIED")
            actor_str = str(actor_user_id)
            owner = deck.get("metadata", {}).get("owner_user_id")
            grants = deck.get("access", {}).get("grants", {})
            if owner != actor_str and (owner_only or grants.get(actor_str) != "EDIT"):
                return ClientResultDTO(status="ACCESS_DENIED")
        return None

    def _issue_access_token(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        """Atomically bind/rotate one client credential in replicated log order.

        Admission must already have verified the password. This deterministic
        transition never receives, persists, or returns the bearer secret.
        """
        payload = IssueAccessTokenPayloadDTO.model_validate(command.payload)
        if self._auth is None:
            return ClientResultDTO(status="AUTH_CATALOG_NOT_READY")

        user_id = str(payload.user_id)
        client_id = str(payload.client_id)
        if user_id not in self._auth["users"]:
            return ClientResultDTO(status="USER_NOT_FOUND")

        existing_owner = self._auth["client_bindings"].get(client_id)
        if existing_owner is not None and existing_owner != user_id:
            return ClientResultDTO(status="CLIENT_ALREADY_BOUND")

        previous = self._auth["active_credentials"].get(client_id)
        quarantined = self._auth.get("legacy_quarantined_credentials", {}).get(client_id)
        predecessor = previous if previous is not None else quarantined
        generation = predecessor["credential_generation"] + 1 if predecessor is not None else 1

        # Check the entire lifetime of known credentials. Check identifiers
        # before digests regardless of map insertion order (fixed precedence).
        known = list(self._auth["active_credentials"].values())
        if self._auth["schema_version"] == 2:
            known.extend(self._auth["retired_credentials"].values())
            known.extend(self._auth.get("legacy_quarantined_credentials", {}).values())
        if any(item["credential_id"] == str(payload.credential_id) for item in known):
            return ClientResultDTO(status="CREDENTIAL_ID_REUSE")
        if any(item["token_digest"] == payload.token_digest for item in known):
            return ClientResultDTO(status="TOKEN_DIGEST_REUSE")

        candidate = deepcopy(self._auth)
        if self._auth["schema_version"] == 2:
            candidate.setdefault("legacy_quarantined_credentials", {})
        if previous is not None and self._auth["schema_version"] == 2:
            candidate["retired_credentials"][previous["credential_id"]] = {
                "user_id": previous["user_id"],
                "client_id": previous["client_id"],
                "credential_id": previous["credential_id"],
                "token_digest": previous["token_digest"],
                "credential_generation": previous["credential_generation"],
                "retirement_reason": "REVOKED" if previous["revoked"] else "ROTATED",
            }
        candidate["client_bindings"][client_id] = user_id
        candidate["active_credentials"][client_id] = {
            "user_id": user_id,
            "client_id": client_id,
            "credential_id": str(payload.credential_id),
            "token_digest": payload.token_digest,
            "credential_generation": generation,
            "revoked": False,
        }
        # Legacy v1 entries remain replayable until the committed v2 upgrade.
        self._replace_auth_state(candidate)
        return ClientResultDTO(status="TOKEN_ISSUED", value={
            "user_id": user_id,
            "client_id": client_id,
            "credential_id": str(payload.credential_id),
            "credential_generation": generation,
        })

    def _revoke_access_token(self, command: CommandEnvelopeDTO) -> ClientResultDTO:
        payload = RevokeAccessTokenPayloadDTO.model_validate(command.payload)
        if self._auth is None:
            return ClientResultDTO(status="UNAUTHENTICATED")
        client_id = str(payload.client_id)
        record = self._auth["active_credentials"].get(client_id)
        if record is None or self._auth["client_bindings"].get(client_id) != str(payload.authenticated_actor_user_id):
            return ClientResultDTO(status="STALE_CREDENTIAL")
        if (record["credential_id"] != str(payload.expected_credential_id)
                or record["credential_generation"] != payload.expected_generation):
            return ClientResultDTO(status="STALE_CREDENTIAL")
        if (record["revoked"] or record["user_id"] != str(payload.authenticated_actor_user_id)
                or record["credential_id"] != str(payload.credential_id)
                or record["credential_generation"] != payload.credential_generation):
            return ClientResultDTO(status="STALE_CREDENTIAL")
        candidate = deepcopy(self._auth)
        current = candidate["active_credentials"][client_id]
        current["revoked"] = True
        current["credential_generation"] += 1
        self._replace_auth_state(candidate)
        return ClientResultDTO(status="TOKEN_REVOKED", value={
            "client_id": client_id, "credential_id": current["credential_id"],
            "credential_generation": current["credential_generation"],
        })

    def _create_element(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = CreateElementPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _acquire_lock(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = AcquireLockPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _release_lock(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = ElementLockPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _renew_lock(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = RenewLockPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _create_slide(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = CreateSlidePayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _reorder_slides(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = ReorderSlidesPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _delete_slide(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = DeleteSlidePayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

        rejected = self._authorize_external_mutation(command, payload, committed_index)
        if rejected is not None:
            return rejected

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

    def _resolve_grant_context(
        self,
        command: CommandEnvelopeDTO,
        payload: GrantAccessPayloadDTO | RevokeAccessPayloadDTO,
    ) -> ClientResultDTO | tuple[dict, dict, int, str]:
        deck = self._decks[str(command.deck_id)]
        owner_user_id: str = deck["metadata"]["owner_user_id"]
        access: dict = deck.get("access", {})
        current_version: int = access.get("access_version", 0)
        if payload.expected_access_version != current_version:
            return ClientResultDTO(status="ACCESS_VERSION_CONFLICT")
        target_user_id = str(payload.target_user_id)
        if target_user_id == owner_user_id:
            return ClientResultDTO(status="INVALID_ACCESS_TARGET")
        if self._auth_model is None or target_user_id not in self._auth_model.users:
            return ClientResultDTO(status="INVALID_TARGET_USER")
        return deck, access, current_version, target_user_id

    def _grant_access(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = GrantAccessPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index, owner_only=True)
        if rejected is not None:
            return rejected

        ctx = self._resolve_grant_context(command, payload)
        if isinstance(ctx, ClientResultDTO):
            return ctx
        deck, access, current_version, target_user_id = ctx

        grants = access.get("grants", {})
        new_level = payload.access_level
        old_level = grants.get(target_user_id)

        if old_level == new_level:
            return ClientResultDTO(status="ACCESS_UNCHANGED", value={"access_version": current_version})

        new_grants = {**grants, target_user_id: new_level}
        new_version = current_version + 1
        deck["access"] = {"schema_version": 1, "access_version": new_version, "grants": new_grants}

        status = "ACCESS_GRANTED" if old_level is None else "ACCESS_UPDATED"
        return ClientResultDTO(status=status, value={"access_version": new_version})

    def _revoke_access(self, command: CommandEnvelopeDTO, *, committed_index: int | None) -> ClientResultDTO:
        payload = RevokeAccessPayloadDTO.model_validate(command.payload)

        rejected = self._authorize_external_mutation(command, payload, committed_index, owner_only=True)
        if rejected is not None:
            return rejected

        ctx = self._resolve_grant_context(command, payload)
        if isinstance(ctx, ClientResultDTO):
            return ctx
        deck, access, current_version, target_user_id = ctx

        grants = access.get("grants", {})
        if target_user_id not in grants:
            return ClientResultDTO(status="ACCESS_NOT_FOUND", value={"access_version": current_version})

        new_grants = {k: v for k, v in grants.items() if k != target_user_id}
        new_version = current_version + 1
        deck["access"] = {"schema_version": 1, "access_version": new_version, "grants": new_grants}

        return ClientResultDTO(status="ACCESS_REVOKED", value={"access_version": new_version})

    def get_local(self, key: str) -> str | None:
        return self._kv.get(key)

    def effective_access(self, deck_id: str, actor_user_id: str) -> str:
        """Return the caller's current role on one deck: OWNER, EDIT, VIEW or NONE.

        Unknown decks and ownerless (v2, quarantined) decks are both NONE, so
        the result never distinguishes an inaccessible deck from a missing one.
        """
        deck = self._decks.get(deck_id)
        if deck is None or deck.get("schema_version") != 3:
            return "NONE"
        if deck.get("metadata", {}).get("owner_user_id") == actor_user_id:
            return "OWNER"
        level = deck.get("access", {}).get("grants", {}).get(actor_user_id)
        return level if level in {"VIEW", "EDIT"} else "NONE"

    def query_grants(self, deck_id: str, *, actor_user_id: str, applied_index: int) -> dict[str, Any]:
        """Self-only permission discovery; NONE carries no resource-specific field."""
        role = self.effective_access(deck_id, actor_user_id)
        if role == "NONE":
            return {"status": "OK", "effective_access": "NONE"}
        return {
            "status": "OK",
            "effective_access": role,
            "deck_id": deck_id,
            "access_version": self._decks[deck_id]["access"]["access_version"],
            "applied_index": applied_index,
        }

    def query_access_state(self, deck_id: str, *, actor_user_id: str, applied_index: int) -> dict[str, Any]:
        """OWNER-only grant roster (ADR-018 §3); everyone else gets the bare denial."""
        if self.effective_access(deck_id, actor_user_id) != "OWNER":
            return {"status": "ACCESS_DENIED"}
        access = self._decks[deck_id]["access"]
        grants = access.get("grants", {})
        return {
            "status": "OK",
            "deck_id": deck_id,
            "access_version": access["access_version"],
            "applied_index": applied_index,
            "grants": [
                {"target_user_id": user_id, "access_level": grants[user_id]}
                for user_id in sorted(grants)
            ],
        }

    def resolve_registered_user(
        self, deck_id: str, email: str, *, actor_user_id: str, applied_index: int,
    ) -> dict[str, Any]:
        """OWNER-only exact normalized-email lookup of a public profile (ADR-018 §5).

        Ownership is checked before the catalog is consulted, so a non-owner or
        unknown deck learns nothing about either decks or accounts.
        """
        if self.effective_access(deck_id, actor_user_id) != "OWNER":
            return {"status": "ACCESS_DENIED"}
        user_id = (self._auth or {}).get("email_index", {}).get(email)
        if user_id is None:
            return {"status": "USER_NOT_FOUND"}
        profile = self._auth["users"][str(user_id)]["profile"]
        return {
            "status": "OK",
            "applied_index": applied_index,
            "user": {
                "user_id": str(profile["user_id"]),
                "first_name": profile["first_name"],
                "last_name": profile["last_name"],
                "email": profile["email"],
            },
        }

    def protected_read(
        self, operation: str, args: Mapping[str, Any], *, actor_user_id: str, applied_index: int,
    ) -> dict[str, Any]:
        """Authorize VIEW-or-higher before producing any protected content projection."""
        deck_id = args["deck_id"]
        role = self.effective_access(deck_id, actor_user_id)
        if role == "NONE":
            return {"status": "ACCESS_DENIED"}
        if operation == "get_deck_preview":
            result = self.get_deck_preview(deck_id, applied_index=applied_index)
            result["effective_access"] = role
            result["access_version"] = self._decks[deck_id]["access"]["access_version"]
            return result
        if operation == "get_slide":
            return self.get_slide(deck_id, args["slide_id"], applied_index=applied_index)
        if operation == "read_lock_status":
            return self.read_lock_status(deck_id, args["element_id"], applied_index=applied_index)
        if operation in {"get_element", "authoritative_refresh_element", "refresh_element"}:
            return self.get_element(deck_id, args["element_id"], applied_index=applied_index)
        raise ValueError(f"not a protected content read: {operation}")

    def get_deck_preview(self, deck_id: str, *, applied_index: int) -> dict[str, Any]:
        deck = self._decks.get(deck_id)
        if self._presentation_schema_policy is not None and (
            deck is None or deck.get("schema_version") != 3
        ):
            return {"status": "ACCESS_DENIED", "applied_index": applied_index}
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
        if self._presentation_schema_policy is not None and (
            deck is None or deck.get("schema_version") != 3
        ):
            return {"status": "ACCESS_DENIED", "applied_index": applied_index}
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
        if self._presentation_schema_policy is not None and (
            deck is None or deck.get("schema_version") != 3
        ):
            return {"status": "ACCESS_DENIED", "applied_index": applied_index}
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
        if self._auth_raw is not None:
            state["auth"] = deepcopy(self._auth) if self._auth is not None else deepcopy(self._auth_raw)
        if self._presentation_schema_policy is not None:
            state["presentation_schema_policy"] = deepcopy(self._presentation_schema_policy)
        canonical_bytes(state)
        return state


SharedPresentationStateMachine = KeyValueStateMachine
