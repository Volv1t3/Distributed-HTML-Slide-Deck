"""Stage F issues #56 and #57: owner-managed grants and deck mutation permission matrix."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from uuid import uuid4

import pytest

from auth_catalog import load_account_catalog
from auth_dto import initial_auth_state
from command_dto import CommandEnvelopeDTO, validate_committed_command
from state_machine import KeyValueStateMachine
from stage_e_raft_harness import actor_stamp, issue_token_command


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _activation() -> CommandEnvelopeDTO:
    from command_dto import PRESENTATION_SCHEMA_RESERVED_CLIENT_ID, presentation_schema_activation_request_id
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
        "request_id": presentation_schema_activation_request_id(), "origin_node_uuid": str(uuid4()),
        "operation": "activate_presentation_schema_v3", "payload": {},
    })


def _publish_v3(owner_id: str, client_id: str, credential_id: str, *,
                deck_id: str | None = None) -> CommandEnvelopeDTO:
    did = deck_id or str(uuid4())
    cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": f"pub-{did}", "origin_node_uuid": str(uuid4()),
        "deck_id": did, "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 3,
                "metadata": {"deck_id": did, "name": "Deck", "owner_session_id": str(uuid4()),
                             "owner_user_id": owner_id, "width_px": 1280, "height_px": 720},
                "slides": [],
            },
            **actor_stamp(owner_id, credential_id),
        },
    })
    return cmd


def _grant_cmd(deck_id: str, actor_user: str, client_id: str, credential_id: str,
               target_user: str, level: str, expected_version: int,
               *, request_id: str | None = None) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": request_id or f"grant-{uuid4()}", "origin_node_uuid": str(uuid4()),
        "deck_id": deck_id, "operation": "grant_access",
        "payload": {
            "target_user_id": target_user,
            "access_level": level,
            "expected_access_version": expected_version,
            **actor_stamp(actor_user, credential_id),
        },
    })


def _revoke_cmd(deck_id: str, actor_user: str, client_id: str, credential_id: str,
                target_user: str, expected_version: int,
                *, request_id: str | None = None) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": request_id or f"revoke-{uuid4()}", "origin_node_uuid": str(uuid4()),
        "deck_id": deck_id, "operation": "revoke_access",
        "payload": {
            "target_user_id": target_user,
            "expected_access_version": expected_version,
            **actor_stamp(actor_user, credential_id),
        },
    })


class _GrantFixture:
    """Fully activated machine with two registered users, owner deck."""

    def __init__(self):
        catalog = load_account_catalog()
        self.catalog = catalog
        self.owner_id = str(catalog.users[0].profile.user_id)
        self.other_id = str(catalog.users[1].profile.user_id)
        self.owner_client = str(uuid4())
        self.owner_cred = str(uuid4())
        self.other_client = str(uuid4())
        self.other_cred = str(uuid4())
        self.machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
        idx = 1
        assert self.machine.apply(_activation(), committed_index=idx).status == "PRESENTATION_SCHEMA_ACTIVATED"
        idx += 1
        assert self.machine.apply(issue_token_command(
            self.owner_id, self.owner_client, self.owner_cred,
            hashlib.sha256(b"owner-token").hexdigest(),
        ), committed_index=idx).status == "TOKEN_ISSUED"
        idx += 1
        assert self.machine.apply(issue_token_command(
            self.other_id, self.other_client, self.other_cred,
            hashlib.sha256(b"other-token").hexdigest(),
        ), committed_index=idx).status == "TOKEN_ISSUED"
        idx += 1
        pub = _publish_v3(self.owner_id, self.owner_client, self.owner_cred)
        assert self.machine.apply(pub, committed_index=idx).status == "PUBLISHED"
        self.deck_id = str(pub.deck_id)
        self._idx = idx

    def apply(self, cmd: CommandEnvelopeDTO) -> "ClientResultDTO":  # type: ignore[name-defined]
        self._idx += 1
        return self.machine.apply(cmd, committed_index=self._idx)

    def snapshot_access(self) -> dict:
        return deepcopy(self.machine.snapshot()["decks"][self.deck_id]["access"])


# ---------------------------------------------------------------------------
# Issue #56: validate_committed_command for new operations
# ---------------------------------------------------------------------------

def test_grant_access_command_requires_deck_id():
    with pytest.raises(ValueError, match="deck_id"):
        validate_committed_command(CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1, "client_id": "c", "request_id": "r",
            "origin_node_uuid": str(uuid4()), "operation": "grant_access",
            "payload": {"target_user_id": str(uuid4()), "access_level": "VIEW",
                        "expected_access_version": 0, **actor_stamp(str(uuid4()), str(uuid4()))},
        }))


def test_revoke_access_command_requires_deck_id():
    with pytest.raises(ValueError, match="deck_id"):
        validate_committed_command(CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1, "client_id": "c", "request_id": "r",
            "origin_node_uuid": str(uuid4()), "operation": "revoke_access",
            "payload": {"target_user_id": str(uuid4()), "expected_access_version": 0,
                        **actor_stamp(str(uuid4()), str(uuid4()))},
        }))


def test_grant_access_rejects_invalid_access_level():
    with pytest.raises(Exception):
        validate_committed_command(CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1, "client_id": "c", "request_id": "r",
            "origin_node_uuid": str(uuid4()), "deck_id": str(uuid4()),
            "operation": "grant_access",
            "payload": {"target_user_id": str(uuid4()), "access_level": "OWNER",
                        "expected_access_version": 0, **actor_stamp(str(uuid4()), str(uuid4()))},
        }))


# ---------------------------------------------------------------------------
# Issue #56: grant_access statuses
# ---------------------------------------------------------------------------

def test_grant_view_creates_new_grant():
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.other_id, "VIEW", 0))
    assert result.status == "ACCESS_GRANTED"
    assert result.value["access_version"] == 1
    access = f.snapshot_access()
    assert access["grants"][f.other_id] == "VIEW"
    assert access["access_version"] == 1


def test_grant_edit_creates_new_grant():
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.other_id, "EDIT", 0))
    assert result.status == "ACCESS_GRANTED"
    assert f.snapshot_access()["grants"][f.other_id] == "EDIT"


def test_grant_update_changes_level():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "VIEW", 0))
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.other_id, "EDIT", 1))
    assert result.status == "ACCESS_UPDATED"
    assert result.value["access_version"] == 2
    assert f.snapshot_access()["grants"][f.other_id] == "EDIT"


def test_grant_unchanged_same_level():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.other_id, "EDIT", 1))
    assert result.status == "ACCESS_UNCHANGED"
    assert f.snapshot_access()["access_version"] == 1  # unchanged


def test_grant_access_version_conflict():
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.other_id, "VIEW", 99))
    assert result.status == "ACCESS_VERSION_CONFLICT"
    assert f.snapshot_access()["access_version"] == 0  # unchanged


def test_grant_invalid_access_target_self():
    """Owner cannot grant access to themselves."""
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                f.owner_id, "VIEW", 0))
    assert result.status == "INVALID_ACCESS_TARGET"
    assert f.snapshot_access()["grants"] == {}


def test_grant_invalid_target_user_unregistered():
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                str(uuid4()), "VIEW", 0))
    assert result.status == "INVALID_TARGET_USER"


def test_grant_access_denied_for_non_owner():
    f = _GrantFixture()
    result = f.apply(_grant_cmd(f.deck_id, f.other_id, f.other_client, f.other_cred,
                                f.owner_id, "VIEW", 0))
    assert result.status == "ACCESS_DENIED"


def test_grant_unauthenticated_bad_credential():
    f = _GrantFixture()
    bad_cred = str(uuid4())
    result = f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, bad_cred,
                                f.other_id, "VIEW", 0))
    assert result.status == "UNAUTHENTICATED"


# ---------------------------------------------------------------------------
# Issue #56: revoke_access statuses
# ---------------------------------------------------------------------------

def test_revoke_access_removes_grant():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    result = f.apply(_revoke_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                 f.other_id, 1))
    assert result.status == "ACCESS_REVOKED"
    assert result.value["access_version"] == 2
    assert f.other_id not in f.snapshot_access()["grants"]


def test_revoke_access_not_found_absent_grant():
    f = _GrantFixture()
    result = f.apply(_revoke_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                 f.other_id, 0))
    assert result.status == "ACCESS_NOT_FOUND"
    assert f.snapshot_access()["access_version"] == 0  # unchanged


def test_revoke_access_version_conflict():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    result = f.apply(_revoke_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                 f.other_id, 0))  # stale version
    assert result.status == "ACCESS_VERSION_CONFLICT"


def test_revoke_invalid_access_target_self():
    f = _GrantFixture()
    result = f.apply(_revoke_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                 f.owner_id, 0))
    assert result.status == "INVALID_ACCESS_TARGET"


def test_revoke_access_denied_for_edit_participant():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    # Other user has EDIT but not OWNER — cannot revoke
    result = f.apply(_revoke_cmd(f.deck_id, f.other_id, f.other_client, f.other_cred,
                                 f.owner_id, 1))
    assert result.status == "ACCESS_DENIED"


def test_grant_does_not_affect_deck_or_settings_versions():
    f = _GrantFixture()
    before = deepcopy(f.machine.snapshot()["decks"][f.deck_id])
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "VIEW", 0))
    after = f.machine.snapshot()["decks"][f.deck_id]
    assert after["deck_version"] == before["deck_version"]
    assert after["deck_settings_version"] == before["deck_settings_version"]
    assert after["access"]["access_version"] == 1


# ---------------------------------------------------------------------------
# Issue #56: access version is independent of other revisions
# ---------------------------------------------------------------------------

def test_access_version_survives_deck_mutation():
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    # Update deck settings — should not touch access_version
    settings_cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": f.owner_client,
        "request_id": "settings-1", "origin_node_uuid": str(uuid4()),
        "deck_id": f.deck_id, "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0, "patch": {"name": "Renamed"},
                    **actor_stamp(f.owner_id, f.owner_cred)},
    })
    r = f.apply(settings_cmd)
    assert r.status == "DECK_SETTINGS_UPDATED"
    assert f.snapshot_access()["access_version"] == 1  # unchanged by settings edit


# ---------------------------------------------------------------------------
# Issue #57: OWNER_MISMATCH for publish_deck
# ---------------------------------------------------------------------------

def test_publish_deck_v3_owner_mismatch():
    catalog = load_account_catalog()
    owner_id = str(catalog.users[0].profile.user_id)
    other_id = str(catalog.users[1].profile.user_id)
    client_id, credential_id = str(uuid4()), str(uuid4())
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
    machine.apply(_activation(), committed_index=1)
    machine.apply(issue_token_command(owner_id, client_id, credential_id,
                                     hashlib.sha256(b"tok").hexdigest()), committed_index=2)
    # Actor claims deck ownership but declares someone else as owner
    did = str(uuid4())
    cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": "mismatch-pub", "origin_node_uuid": str(uuid4()),
        "deck_id": did, "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 3,
                "metadata": {"deck_id": did, "name": "X", "owner_session_id": str(uuid4()),
                             "owner_user_id": other_id, "width_px": 1280, "height_px": 720},
                "slides": [],
            },
            **actor_stamp(owner_id, credential_id),
        },
    })
    result = machine.apply(cmd, committed_index=3)
    assert result.status == "OWNER_MISMATCH"
    assert did not in machine.snapshot().get("decks", {})


# ---------------------------------------------------------------------------
# Issue #57: EDIT participant can perform all 11 mutations
# ---------------------------------------------------------------------------

def _slide_id_for_deck(machine: KeyValueStateMachine, deck_id: str) -> str:
    return machine.snapshot()["decks"][deck_id]["slides"][0]["metadata"]["slide_id"]


def _make_deck_with_slide(catalog, *, deck_id: str, slide_id: str) -> KeyValueStateMachine:
    owner_id = str(catalog.users[0].profile.user_id)
    client_id, credential_id = str(uuid4()), str(uuid4())
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=1)})
    machine.apply(_activation(), committed_index=1)
    machine.apply(issue_token_command(owner_id, client_id, credential_id,
                                     hashlib.sha256(b"m-tok").hexdigest()), committed_index=2)
    cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id,
        "request_id": "pub-with-slide", "origin_node_uuid": str(uuid4()),
        "deck_id": deck_id, "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 3,
                "metadata": {"deck_id": deck_id, "name": "D", "owner_session_id": str(uuid4()),
                             "owner_user_id": owner_id, "width_px": 1280, "height_px": 720},
                "slides": [{"metadata": {"slide_id": slide_id, "background": "#000000FF"},
                            "elements": []}],
            },
            **actor_stamp(owner_id, credential_id),
        },
    })
    machine.apply(cmd, committed_index=3)
    return machine, owner_id, client_id, credential_id


def test_edit_participant_can_create_slide():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"e-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-edit",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "EDIT",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    new_slide_id = str(uuid4())
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "edit-create-slide",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "create_slide",
        "payload": {"expected_deck_version": 0,
                    "slide": {"metadata": {"slide_id": new_slide_id, "background": "#FFFFFFFF"},
                              "elements": []},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=6)
    assert result.status == "CREATED"


def test_view_participant_cannot_create_slide():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"v-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-view",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "VIEW",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "view-create-slide",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "create_slide",
        "payload": {"expected_deck_version": 0,
                    "slide": {"metadata": {"slide_id": str(uuid4()), "background": "#FFFFFFFF"},
                              "elements": []},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=6)
    assert result.status == "ACCESS_DENIED"


def test_nonparticipant_cannot_mutate_deck():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"np-tok").hexdigest()), committed_index=4)
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "np-delete-slide",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "delete_slide",
        "payload": {"expected_deck_version": 0, "slide_id": slide_id,
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=5)
    assert result.status == "ACCESS_DENIED"


def test_edit_only_owner_can_grant_revoke():
    """An EDIT participant cannot perform grant management."""
    f = _GrantFixture()
    f.apply(_grant_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred, f.other_id, "EDIT", 0))
    # EDIT participant tries to grant to owner — should be denied
    result = f.apply(_grant_cmd(f.deck_id, f.other_id, f.other_client, f.other_cred,
                                f.owner_id, "VIEW", 1))
    assert result.status == "ACCESS_DENIED"


def test_revoked_participant_loses_mutation_rights():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"rev-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-edit2",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "EDIT",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    # Now revoke
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "revoke-edit2",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "revoke_access",
        "payload": {"target_user_id": other_id, "expected_access_version": 1,
                    **actor_stamp(owner_id, owner_cred)},
    }), committed_index=6)
    # Revoked editor tries to create slide
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "revoked-create",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "create_slide",
        "payload": {"expected_deck_version": 0,
                    "slide": {"metadata": {"slide_id": str(uuid4()), "background": "#FFFFFFFF"},
                              "elements": []},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=7)
    assert result.status == "ACCESS_DENIED"


def test_access_not_found_returns_access_version():
    f = _GrantFixture()
    result = f.apply(_revoke_cmd(f.deck_id, f.owner_id, f.owner_client, f.owner_cred,
                                 f.other_id, 0))
    assert result.status == "ACCESS_NOT_FOUND"
    assert result.value["access_version"] == 0


def test_edit_participant_can_update_deck_settings():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"ds-edit-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-edit-ds",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "EDIT",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "edit-rename",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0, "patch": {"name": "Renamed by Editor"},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=6)
    assert result.status == "DECK_SETTINGS_UPDATED"


def test_view_participant_cannot_update_deck_settings():
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"ds-view-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-view-ds",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "VIEW",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "view-rename",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "update_deck_settings",
        "payload": {"expected_deck_settings_version": 0, "patch": {"name": "Hijack"},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=6)
    assert result.status == "ACCESS_DENIED"


def test_edit_participant_lock_guard_still_enforced():
    """EDIT role does not bypass the existing session/fencing-token lock guard."""
    catalog = load_account_catalog()
    deck_id, slide_id = str(uuid4()), str(uuid4())
    machine, owner_id, owner_client, owner_cred = _make_deck_with_slide(
        catalog, deck_id=deck_id, slide_id=slide_id)
    other_id = str(catalog.users[1].profile.user_id)
    other_client, other_cred = str(uuid4()), str(uuid4())
    other_session = str(uuid4())
    machine.apply(issue_token_command(other_id, other_client, other_cred,
                                     hashlib.sha256(b"lock-edit-tok").hexdigest()), committed_index=4)
    machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": owner_client, "request_id": "grant-edit-lock",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "grant_access",
        "payload": {"target_user_id": other_id, "access_level": "EDIT",
                    "expected_access_version": 0, **actor_stamp(owner_id, owner_cred)},
    }), committed_index=5)
    # Create an element as the EDIT participant (EDIT is allowed).
    new_elem_id = str(uuid4())
    r = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "create-elem-lock",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "session_id": other_session,
        "operation": "create_element",
        "payload": {"slide_id": slide_id, "element": {
            "element_id": new_elem_id, "kind": "text_box", "version": 1,
            "configuration": {"content": "hi", "font_families": ["Arial"],
                              "generic_fallback": "sans-serif", "font_size": 16,
                              "color": "#000000FF", "x": 0, "y": 0},
        }, **actor_stamp(other_id, other_cred)},
    }), committed_index=6)
    assert r.status == "CREATED_AND_LOCKED"
    correct_fencing_token = r.value["fencing_token"]

    # update_element with wrong fencing token must yield STALE_TOKEN, not ACCESS_DENIED —
    # the lock guard fires after the permission check and must still be respected.
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": other_client, "request_id": "bad-token-update",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "session_id": other_session,
        "operation": "update_element",
        "payload": {"element_id": new_elem_id, "fencing_token": correct_fencing_token + 999,
                    "expected_version": 1,
                    "patch": {"content": "changed"},
                    **actor_stamp(other_id, other_cred)},
    }), committed_index=7)
    assert result.status == "STALE_TOKEN"


def test_v2_deck_access_denied_for_all_mutations_after_fence():
    """Quarantined v2 decks must reject mutations after the activation fence."""
    catalog = load_account_catalog()
    from command_dto import PRESENTATION_SCHEMA_RESERVED_CLIENT_ID, presentation_schema_activation_request_id
    machine = KeyValueStateMachine({"kv": {}, "auth": initial_auth_state(catalog, enforced_from_index=100)})
    deck_id = str(uuid4())
    slide_id = str(uuid4())
    # Publish a v2 deck before activation
    v2_cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": "legacy", "request_id": "legacy-pub",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "publish_deck",
        "payload": {"presentation": {
            "schema_version": 2,
            "metadata": {"deck_id": deck_id, "name": "Old", "owner_session_id": str(uuid4()),
                         "width_px": 1280, "height_px": 720},
            "slides": [{"metadata": {"slide_id": slide_id, "background": "#000000FF"}, "elements": []}],
        }},
    })
    assert machine.apply(v2_cmd, committed_index=2).status == "PUBLISHED"
    assert machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
        "request_id": presentation_schema_activation_request_id(), "origin_node_uuid": str(uuid4()),
        "operation": "activate_presentation_schema_v3", "payload": {},
    }), committed_index=3).status == "PRESENTATION_SCHEMA_ACTIVATED"
    # Any authenticated user trying to mutate a v2 deck → ACCESS_DENIED
    owner_id = str(catalog.users[0].profile.user_id)
    client_id, cred_id = str(uuid4()), str(uuid4())
    machine.apply(issue_token_command(owner_id, client_id, cred_id,
                                     hashlib.sha256(b"v2-mut").hexdigest()), committed_index=4)
    result = machine.apply(CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "client_id": client_id, "request_id": "v2-delete",
        "origin_node_uuid": str(uuid4()), "deck_id": deck_id, "operation": "delete_slide",
        "payload": {"expected_deck_version": 0, "slide_id": slide_id,
                    **actor_stamp(owner_id, cred_id)},
    }), committed_index=5)
    assert result.status == "ACCESS_DENIED"
