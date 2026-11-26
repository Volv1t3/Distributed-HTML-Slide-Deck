"""Account-bound local drafts at the authenticated client boundary."""

from uuid import uuid4

import pytest

from authenticated_client import AuthenticatedClient


def _client(tmp_path, user_id):
    client = AuthenticatedClient(tmp_path / "client", [], tmp_path / "history.jsonl")
    client._store_credential({
        "user_id": user_id, "access_token": "local-test-token",
        "credential_id": str(uuid4()), "credential_generation": 1,
    })
    return client


def _presentation(deck_id):
    return {"schema_version": 3, "metadata": {
        "deck_id": deck_id, "name": "Private", "owner_session_id": str(uuid4()),
        "width_px": 1280, "height_px": 720,
    }, "slides": []}


def test_draft_keeps_account_across_restart_and_offline_edits(tmp_path):
    owner = str(uuid4())
    deck_id = str(uuid4())
    client = _client(tmp_path, owner)
    created = client.create_draft(_presentation(deck_id))
    assert created["metadata"]["owner_user_id"] == owner
    restarted = AuthenticatedClient(tmp_path / "client", [], tmp_path / "history.jsonl")
    edited = restarted.update_draft(deck_id, {"name": "Offline edit"})
    assert edited["metadata"]["name"] == "Offline edit"
    assert restarted.load_draft(deck_id)["metadata"]["owner_user_id"] == owner


def test_missing_or_switched_account_cannot_adopt_draft(tmp_path):
    owner = str(uuid4())
    deck_id = str(uuid4())
    client = _client(tmp_path, owner)
    client.create_draft(_presentation(deck_id))
    client._in_memory_credential = None
    with pytest.raises(PermissionError):
        client.load_draft(deck_id)
    client._in_memory_credential = {"user_id": str(uuid4())}
    with pytest.raises(PermissionError):
        client.update_draft(deck_id, {"name": "Claimed"})
    assert _client(tmp_path, owner).load_draft(deck_id)["metadata"]["name"] == "Private"
