"""Stage F publication cutover at committed Raft application."""

from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO, PRESENTATION_SCHEMA_RESERVED_CLIENT_ID, PublishDeckPayloadDTO, validate_committed_command
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
        "command_schema_version": 1, "client_id": PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
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
    # AC #54/5: v3 publication is accepted after restart from an activated snapshot.
    catalog = load_account_catalog()
    owner = str(catalog.users[0].profile.user_id)
    client_id, credential_id = str(uuid4()), str(uuid4())
    assert restored.apply(issue_token_command(
        owner, client_id, credential_id, hashlib.sha256(b"restart-token").hexdigest(),
    ), committed_index=9).status == "TOKEN_ISSUED"
    v3 = publication(3, owner=owner).model_copy(update={"client_id": client_id})
    v3.payload.update(actor_stamp(owner, credential_id))
    assert restored.apply(v3, committed_index=10).status == "PUBLISHED"
    assert restored.snapshot()["decks"][str(v3.deck_id)]["schema_version"] == 3


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


def test_ownerless_v2_is_quarantined_after_activation_without_changing_its_state():
    catalog = load_account_catalog()
    owner = str(catalog.users[0].profile.user_id)
    client_id, credential_id = str(uuid4()), str(uuid4())
    machine = KeyValueStateMachine({
        "kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=100),
    })
    old = publication(2)
    assert machine.apply(old, committed_index=2).status == "PUBLISHED"
    assert machine.apply(activation(), committed_index=3).status == "PRESENTATION_SCHEMA_ACTIVATED"
    assert machine.apply(issue_token_command(
        owner, client_id, credential_id, hashlib.sha256(b"v2-token").hexdigest(),
    ), committed_index=4).status == "TOKEN_ISSUED"
    before = deepcopy(machine.snapshot()["decks"][str(old.deck_id)])
    edit = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": "v2-settings", "origin_node_uuid": str(uuid4()),
        "deck_id": str(old.deck_id), "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0, "patch": {"name": "Claimed"},
                    **actor_stamp(owner, credential_id)},
    })
    assert machine.apply(edit, committed_index=5).status == "ACCESS_DENIED"
    assert machine.snapshot()["decks"][str(old.deck_id)] == before
    assert machine.get_deck_preview(str(old.deck_id), applied_index=5)["status"] == "ACCESS_DENIED"
    assert machine.get_deck_preview(str(uuid4()), applied_index=5)["status"] == "ACCESS_DENIED"


def test_v3_owner_survives_settings_and_credential_rotation():
    catalog = load_account_catalog()
    owner = str(catalog.users[0].profile.user_id)
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
    assert machine.apply(activation(), committed_index=1).status == "PRESENTATION_SCHEMA_ACTIVATED"
    client_id, credential_id = str(uuid4()), str(uuid4())
    assert machine.apply(issue_token_command(
        owner, client_id, credential_id, hashlib.sha256(b"first-token").hexdigest(),
    ), committed_index=2).status == "TOKEN_ISSUED"
    published = publication(3, owner=owner).model_copy(update={"client_id": client_id})
    published.payload.update(actor_stamp(owner, credential_id))
    assert machine.apply(published, committed_index=3).status == "PUBLISHED"
    new_credential_id = str(uuid4())
    assert machine.apply(issue_token_command(
        owner, client_id, new_credential_id, hashlib.sha256(b"second-token").hexdigest(),
    ), committed_index=4).status == "TOKEN_ISSUED"
    edit = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": "rename", "origin_node_uuid": str(uuid4()),
        "deck_id": str(published.deck_id), "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0, "patch": {"name": "Renamed"},
                    **actor_stamp(owner, new_credential_id, generation=2)},
    })
    assert machine.apply(edit, committed_index=5).status == "DECK_SETTINGS_UPDATED"
    deck = machine.snapshot()["decks"][str(published.deck_id)]
    assert deck["metadata"]["owner_user_id"] == owner
    assert deck["metadata"]["name"] == "Renamed"
    assert deck["access"] == {"schema_version": 1, "access_version": 0, "grants": {}}


def test_settings_patch_cannot_modify_owner():
    # AC #55/4: DeckSettingsPatchDTO structurally rejects owner fields; state machine preserves them.
    from command_dto import DeckSettingsPatchDTO
    catalog = load_account_catalog()
    owner = str(catalog.users[0].profile.user_id)
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
    assert machine.apply(activation(), committed_index=1).status == "PRESENTATION_SCHEMA_ACTIVATED"
    client_id, credential_id = str(uuid4()), str(uuid4())
    assert machine.apply(issue_token_command(
        owner, client_id, credential_id, hashlib.sha256(b"owner-patch-token").hexdigest(),
    ), committed_index=2).status == "TOKEN_ISSUED"
    pub = publication(3, owner=owner).model_copy(update={"client_id": client_id})
    pub.payload.update(actor_stamp(owner, credential_id))
    assert machine.apply(pub, committed_index=3).status == "PUBLISHED"

    # The DTO itself must reject any patch that includes owner_user_id.
    with pytest.raises((ValidationError, ValueError)):
        DeckSettingsPatchDTO.model_validate({"name": "X", "owner_user_id": str(uuid4())})

    # Even if the envelope is forged with an owner field in the raw payload, the state
    # machine must not overwrite the stored owner_user_id.
    attacker_id = str(catalog.users[1].profile.user_id)
    attacker_cid, attacker_cred = str(uuid4()), str(uuid4())
    assert machine.apply(issue_token_command(
        owner, attacker_cid, attacker_cred, hashlib.sha256(b"attacker-token").hexdigest(),
    ), committed_index=4).status == "TOKEN_ISSUED"
    forgery = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": "forged-patch", "origin_node_uuid": str(uuid4()),
        "deck_id": str(pub.deck_id), "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0,
                    "patch": {"name": "Hijacked"},
                    **actor_stamp(owner, credential_id)},
    })
    result = machine.apply(forgery, committed_index=5)
    assert result.status == "DECK_SETTINGS_UPDATED"
    assert machine.snapshot()["decks"][str(pub.deck_id)]["metadata"]["owner_user_id"] == owner
