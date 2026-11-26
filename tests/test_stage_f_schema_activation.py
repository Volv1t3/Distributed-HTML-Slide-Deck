"""Stage F publication cutover at committed Raft application."""

from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO, PublishDeckPayloadDTO, validate_committed_command
from state_machine import KeyValueStateMachine
from stage_e_raft_harness import actor_stamp, issue_token_command
import hashlib


def publication(version: int, *, owner: str | None = None, request_id: str = "publish") -> CommandEnvelopeDTO:
    deck_id = str(uuid4())
    metadata = {
        "deck_id": deck_id, "name": "Deck", "owner_session_id": str(uuid4()),
        "width_px": 1280, "height_px": 720,
    }
    if owner is not None:
        metadata["owner_user_id"] = owner
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": "legacy-client", "request_id": request_id,
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "publish_deck",
        "payload": {"presentation": {"schema_version": version, "metadata": metadata, "slides": []}},
    })


def activation() -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": "_presentation_schema",
        "request_id": "activate-presentation-schema-v3", "origin_node_uuid": str(uuid4()),
        "operation": "activate_presentation_schema_v3", "payload": {},
    })


def test_strict_historical_and_current_publication_decoding():
    user_id = str(uuid4())
    v2 = publication(2)
    v3 = publication(3, owner=user_id)
    validate_committed_command(v2)
    validate_committed_command(v3)
    assert type(PublishDeckPayloadDTO.model_validate(v2.payload).presentation).__name__ == "PresentationSnapshotV2DTO"
    assert type(PublishDeckPayloadDTO.model_validate(v3.payload).presentation).__name__ == "PresentationSnapshotV3DTO"
    for invalid in ("2", True, 4):
        with pytest.raises((ValidationError, ValueError)):
            validate_committed_command(publication(invalid))
    with pytest.raises((ValidationError, ValueError)):
        validate_committed_command(publication(3))


def test_activation_fences_historical_replay_and_persists_across_restart():
    machine = KeyValueStateMachine({
        "kv": {}, "auth": initial_auth_state(load_account_catalog(), enforced_from_index=100),
    })
    old = publication(2)
    assert machine.apply(old, committed_index=4).status == "PUBLISHED"
    assert machine.apply(activation(), committed_index=5).status == "PRESENTATION_SCHEMA_ACTIVATED"
    snapshot = deepcopy(machine.snapshot())
    assert snapshot["presentation_schema_policy"] == {"schema_version": 1, "v3_enforced_from_index": 6}
    restored = KeyValueStateMachine(snapshot)
    assert restored.apply(activation(), committed_index=7).status == "PRESENTATION_SCHEMA_ALREADY_ACTIVE"
    assert restored.snapshot()["presentation_schema_policy"] == snapshot["presentation_schema_policy"]
    late = publication(2)
    assert restored.apply(late, committed_index=8).status == "UNSUPPORTED_PRESENTATION_SCHEMA"
    assert str(late.deck_id) not in restored.snapshot()["decks"]
    assert restored.snapshot()["decks"][str(old.deck_id)]["schema_version"] == 2


def test_v3_requires_fence_and_matching_registered_actor():
    catalog = load_account_catalog()
    user_id = str(catalog.users[0].profile.user_id)
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
    before = publication(3, owner=user_id)
    assert machine.apply(before, committed_index=3).status == "PRESENTATION_SCHEMA_UNAVAILABLE"
    assert machine.apply(activation(), committed_index=4).status == "PRESENTATION_SCHEMA_ACTIVATED"
    client_id, credential_id = str(uuid4()), str(uuid4())
    digest = hashlib.sha256(b"activation-test-token").hexdigest()
    assert machine.apply(issue_token_command(user_id, client_id, credential_id, digest), committed_index=5).status == "TOKEN_ISSUED"
    # A valid actor cannot publish another registered account's owner claim.
    other_id = str(catalog.users[1].profile.user_id)
    wrong = publication(3, owner=other_id).model_copy(update={"client_id": client_id})
    wrong.payload.update(actor_stamp(user_id, credential_id))
    assert machine.apply(wrong, committed_index=6).status == "ACCESS_DENIED"
    assert machine.snapshot().get("decks", {}) == {}


def test_activation_envelope_is_internal_and_collision_aware():
    validate_committed_command(activation())
    bad = activation().model_dump(mode="json")
    bad["client_id"] = "external"
    with pytest.raises(ValueError):
        validate_committed_command(CommandEnvelopeDTO.model_validate(bad))
