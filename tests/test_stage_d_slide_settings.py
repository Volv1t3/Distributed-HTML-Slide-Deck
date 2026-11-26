"""Stage D — versioned Slide Settings: deterministic, adapter, and reference-model tests."""

from __future__ import annotations

import threading
import unittest
from uuid import UUID, uuid4

import spec
from app import App
from auth_catalog import load_account_catalog
from command_dto import (
    ClientResultDTO,
    CommandEnvelopeDTO,
    SlideSettingsPatchDTO,
    UpdateSlideSettingsPayloadDTO,
    validate_committed_command,
    presentation_schema_activation_request_id,
)
from node import Request
from state_machine import KeyValueStateMachine


def _activate_presentation_schema(replica: App, harness) -> None:
    harness._commit_command(replica, CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1, "request_id": presentation_schema_activation_request_id(),
        "client_id": "_presentation_schema", "origin_node_uuid": str(uuid4()),
        "operation": "activate_presentation_schema_v3", "payload": {},
    }))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _envelope(
    *,
    operation: str,
    deck_id: object,
    payload: dict,
    request_id: str = "req-1",
    client_id: str = "test-client",
) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": request_id,
        "client_id": client_id,
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": operation,
        "payload": payload,
    })


def _publish_deck(machine: KeyValueStateMachine, deck_id: UUID, slide_id: UUID) -> None:
    cmd = CommandEnvelopeDTO.model_validate({
        "command_schema_version": 1,
        "request_id": "pub-1",
        "client_id": "test-client",
        "origin_node_uuid": str(uuid4()),
        "deck_id": str(deck_id),
        "operation": "publish_deck",
        "payload": {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": str(deck_id),
                    "name": "Test Deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [
                    {
                        "metadata": {"slide_id": str(slide_id), "background": "#112233FF"},
                        "elements": [],
                    }
                ],
            }
        },
    })
    result = machine.apply(cmd)
    assert result.status == "PUBLISHED"


def _update_slide_settings_cmd(
    deck_id: UUID,
    slide_id: UUID,
    expected_version: int,
    background: str = "#AABBCCFF",
    *,
    request_id: str = "uss-1",
    client_id: str = "test-client",
) -> CommandEnvelopeDTO:
    return _envelope(
        operation="update_slide_settings",
        deck_id=deck_id,
        payload={
            "slide_id": str(slide_id),
            "expected_slide_settings_version": expected_version,
            "patch": {"background": background},
        },
        request_id=request_id,
        client_id=client_id,
    )


# ---------------------------------------------------------------------------
# DTO validation tests
# ---------------------------------------------------------------------------

class SlideSettingsDTOValidationTests(unittest.TestCase):
    def test_valid_patch_roundtrips(self) -> None:
        patch = SlideSettingsPatchDTO.model_validate({"background": "#AABBCCFF"})
        self.assertEqual(patch.background, "#AABBCCFF")

    def test_rgb_without_alpha_is_rejected(self) -> None:
        with self.assertRaises(Exception):
            SlideSettingsPatchDTO.model_validate({"background": "#AABBCC"})

    def test_lowercase_hex_is_rejected(self) -> None:
        with self.assertRaises(Exception):
            SlideSettingsPatchDTO.model_validate({"background": "#aabbccff"})

    def test_unknown_field_is_rejected(self) -> None:
        with self.assertRaises(Exception):
            SlideSettingsPatchDTO.model_validate({"background": "#AABBCCFF", "font": "Arial"})

    def test_validate_committed_command_accepts_update_slide_settings(self) -> None:
        deck_id = uuid4()
        slide_id = uuid4()
        cmd = _update_slide_settings_cmd(deck_id, slide_id, 0)
        # Must not raise
        validate_committed_command(cmd)

    def test_validate_committed_command_rejects_missing_deck_id(self) -> None:
        with self.assertRaises(ValueError):
            cmd = CommandEnvelopeDTO.model_validate({
                "command_schema_version": 1,
                "request_id": "r1",
                "client_id": "c1",
                "origin_node_uuid": str(uuid4()),
                "operation": "update_slide_settings",
                "payload": {
                    "slide_id": str(uuid4()),
                    "expected_slide_settings_version": 0,
                    "patch": {"background": "#AABBCCFF"},
                },
            })
            validate_committed_command(cmd)

    def test_validate_committed_command_rejects_tlv_blocks(self) -> None:
        from dto_base import canonical_bytes
        deck_id = uuid4()
        audit_value = {"reason": "test"}
        audit_len = len(canonical_bytes(audit_value))
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "r1",
            "client_id": "c1",
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_slide_settings",
            "payload": {
                "slide_id": str(uuid4()),
                "expected_slide_settings_version": 0,
                "patch": {"background": "#AABBCCFF"},
            },
            "blocks": [{
                "type_id": 0x0301,  # AUDIT_METADATA_TYPE_ID
                "schema_version": 1,
                "required": False,
                "length_bytes": audit_len,
                "value": audit_value,
            }],
        })
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)

    def test_payload_dto_rejects_negative_expected_version(self) -> None:
        with self.assertRaises(Exception):
            UpdateSlideSettingsPayloadDTO.model_validate({
                "slide_id": str(uuid4()),
                "expected_slide_settings_version": -1,
                "patch": {"background": "#AABBCCFF"},
            })


# ---------------------------------------------------------------------------
# State machine tests
# ---------------------------------------------------------------------------

class SlideSettingsStateMachineTests(unittest.TestCase):

    def setUp(self) -> None:
        self.deck_id = uuid4()
        self.slide_id = uuid4()
        self.machine = KeyValueStateMachine({"kv": {}})
        _publish_deck(self.machine, self.deck_id, self.slide_id)

    def test_success_returns_slide_settings_updated(self) -> None:
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 0)
        result = self.machine.apply(cmd, committed_index=5)
        self.assertEqual(result.status, "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(result.version, 1)
        self.assertEqual(result.value["slide_settings_version"], 1)
        self.assertEqual(result.value["background"], "#AABBCCFF")
        self.assertEqual(result.value["deck_id"], str(self.deck_id))
        self.assertEqual(result.value["slide_id"], str(self.slide_id))
        self.assertEqual(result.value["ordinal"], 0)
        self.assertEqual(result.value["deck_version"], 0)

    def test_slide_settings_version_increments_once(self) -> None:
        cmd1 = _update_slide_settings_cmd(self.deck_id, self.slide_id, 0, "#AABBCCFF", request_id="r1")
        result1 = self.machine.apply(cmd1)
        self.assertEqual(result1.value["slide_settings_version"], 1)

        cmd2 = _update_slide_settings_cmd(self.deck_id, self.slide_id, 1, "#DDEEFF00", request_id="r2")
        result2 = self.machine.apply(cmd2)
        self.assertEqual(result2.status, "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(result2.value["slide_settings_version"], 2)

    def test_stale_expected_version_returns_conflict(self) -> None:
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 1)  # stale: current is 0
        result = self.machine.apply(cmd, committed_index=7)
        self.assertEqual(result.status, "SLIDE_SETTINGS_VERSION_CONFLICT")
        self.assertEqual(result.version, 0)
        self.assertEqual(result.value["slide_settings_version"], 0)
        self.assertEqual(result.value["background"], "#112233FF")
        self.assertEqual(result.value["deck_version"], 0)
        self.assertEqual(result.value["ordinal"], 0)
        self.assertEqual(result.value["min_commit_index"], 7)

    def test_missing_target_returns_not_found(self) -> None:
        unknown_slide = uuid4()
        cmd = _update_slide_settings_cmd(self.deck_id, unknown_slide, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NOT_FOUND")

    def test_tombstoned_slide_returns_slide_deleted(self) -> None:
        # Delete the slide first
        delete_cmd = _envelope(
            operation="delete_slide",
            deck_id=self.deck_id,
            payload={"slide_id": str(self.slide_id), "expected_deck_version": 0},
            request_id="del-1",
        )
        del_result = self.machine.apply(delete_cmd)
        self.assertEqual(del_result.status, "DELETED")

        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "SLIDE_DELETED")

    def test_deleted_slide_cannot_be_mutated(self) -> None:
        """Tombstoned target must not recreate or mutate deleted state."""
        delete_cmd = _envelope(
            operation="delete_slide",
            deck_id=self.deck_id,
            payload={"slide_id": str(self.slide_id), "expected_deck_version": 0},
            request_id="del-1",
        )
        self.machine.apply(delete_cmd)

        # Try multiple settings updates — none should change state
        for i in range(3):
            cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, i, request_id=f"uss-del-{i}")
            result = self.machine.apply(cmd)
            self.assertEqual(result.status, "SLIDE_DELETED")

        # Active slides list should still be empty
        preview = self.machine.get_deck_preview(str(self.deck_id), applied_index=99)
        self.assertEqual(preview["slide_ids"], [])

    def test_missing_deck_returns_not_found(self) -> None:
        cmd = _update_slide_settings_cmd(uuid4(), self.slide_id, 0)
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "NOT_FOUND")

    def test_independence_no_other_versions_advance(self) -> None:
        """Updating one slide must not touch deck_version, deck_settings_version, etc."""
        second_slide_id = uuid4()
        create_cmd = _envelope(
            operation="create_slide",
            deck_id=self.deck_id,
            payload={
                "expected_deck_version": 0,
                "slide": {
                    "metadata": {"slide_id": str(second_slide_id), "background": "#CCDDEE11"},
                    "elements": [],
                },
            },
            request_id="create-slide-2",
        )
        create_result = self.machine.apply(create_cmd)
        self.assertEqual(create_result.status, "CREATED")
        self.assertEqual(create_result.value["deck_version"], 1)

        # Capture baseline state
        preview_before = self.machine.get_deck_preview(str(self.deck_id), applied_index=0)
        deck_version_before = preview_before["deck_version"]
        deck_settings_before = preview_before["deck_settings_version"]
        slide2_before = self.machine.get_slide(str(self.deck_id), str(second_slide_id), applied_index=0)
        slide2_ssv_before = slide2_before["slide"]["metadata"]["slide_settings_version"]

        # Update settings on first slide
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 0, "#FFEEDDCC", request_id="uss-indep")
        result = self.machine.apply(cmd)
        self.assertEqual(result.status, "SLIDE_SETTINGS_UPDATED")

        # Check nothing else changed
        preview_after = self.machine.get_deck_preview(str(self.deck_id), applied_index=0)
        self.assertEqual(preview_after["deck_version"], deck_version_before,
                         "deck_version must not advance on slide settings update")
        self.assertEqual(preview_after["deck_settings_version"], deck_settings_before,
                         "deck_settings_version must not advance on slide settings update")
        slide2_after = self.machine.get_slide(str(self.deck_id), str(second_slide_id), applied_index=0)
        self.assertEqual(
            slide2_after["slide"]["metadata"]["slide_settings_version"], slide2_ssv_before,
            "other slide's slide_settings_version must not advance",
        )

    def test_get_slide_exposes_slide_settings_version_and_ordinal(self) -> None:
        # Update settings to version 1
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 0)
        self.machine.apply(cmd)

        slide_obs = self.machine.get_slide(str(self.deck_id), str(self.slide_id), applied_index=42)
        self.assertEqual(slide_obs["status"], "OK")
        meta = slide_obs["slide"]["metadata"]
        self.assertEqual(meta["slide_settings_version"], 1)
        self.assertEqual(meta["background"], "#AABBCCFF")
        self.assertEqual(meta["ordinal"], 0)
        self.assertEqual(slide_obs["deck_version"], 0)
        self.assertEqual(slide_obs["applied_index"], 42)

    def test_conflict_min_commit_index_absent_when_not_provided(self) -> None:
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 99)
        result = self.machine.apply(cmd, committed_index=None)
        self.assertEqual(result.status, "SLIDE_SETTINGS_VERSION_CONFLICT")
        self.assertNotIn("min_commit_index", result.value)

    def test_conflict_min_commit_index_present_when_provided(self) -> None:
        cmd = _update_slide_settings_cmd(self.deck_id, self.slide_id, 99)
        result = self.machine.apply(cmd, committed_index=12)
        self.assertEqual(result.value["min_commit_index"], 12)


# ---------------------------------------------------------------------------
# Deduplication and replay tests (via App adapter)
# ---------------------------------------------------------------------------

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from stage_e_raft_harness import SimulatedRaftHarnessMixin


class SlideSettingsDeduplicationTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    """Test dedup/replay semantics through the full Raft proposal path."""

    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replica: App | None = None

    def tearDown(self) -> None:
        if self.replica is not None:
            self.replica._events.close()
            self.replica._log.close()
        self.workspace.cleanup()

    def _make_replica(self) -> "App":
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": "OPERATIONAL"}, clear=False):
            replica = App("n1", self.nodes, Path(self.workspace.name))
            replica.start()
        self.replica = replica
        return replica

    def _drain(self, replica: "App") -> list:
        messages = []
        while True:
            try:
                messages.append(replica._out.get_nowait())
            except Exception:
                break
        return messages

    def _elect_leader(self, replica: "App") -> None:
        for peer in ("n2", "n3"):
            replica.on_message(peer, {
                "kind": "register_node",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": peer,
                "sender_uuid": str(uuid4()),
                "term": 0,
                "incarnation": 1,
                "membership_fingerprint": replica.engine.membership_fingerprint(),
            })
        self._drain(replica)
        replica.engine.start_election()
        vote = next(
            (msg for dest, msg in self._drain(replica)
             if dest == "n2" and msg.get("kind") == "request_vote"),
            None,
        )
        self.assertIsNotNone(vote)
        replica.on_message("n2", {
            "kind": "request_vote_response",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1,
            "reply_to": vote["rpc_id"],
            "vote_granted": True,
        })
        self._drain(replica)
        self.client_id, self.access_token = self._provision_credential(replica)
        _activate_presentation_schema(replica, self)

    def _commit(self, replica: App, request: Request) -> dict:
        """Commit one guarded request through the real bearer-admission path."""
        return self._commit_admitted_request(replica, request, self.access_token)

    def _publish(self, replica: "App", deck_id: str, slide_id: str) -> None:
        result = self._commit(replica, Request(
            str(uuid4()), "publish_deck",
            {"presentation": {
                "schema_version": 3,
                "metadata": {
                    "deck_id": deck_id, "name": "Dedup Deck",
                    "owner_session_id": str(uuid4()),
                    "owner_user_id": str(load_account_catalog().users[0].profile.user_id),
                    "width_px": 1280, "height_px": 720,
                },
                "slides": [{"metadata": {"slide_id": slide_id, "background": "#112233FF"}, "elements": []}],
            }}, self.client_id,
        ))
        self.assertEqual(result.get("result", {}).get("status"), "PUBLISHED")

    def test_exact_id_retry_replays_stored_result_without_bumping_version(self) -> None:
        replica = self._make_replica()
        self._elect_leader(replica)
        deck_id, slide_id = str(uuid4()), str(uuid4())
        self._publish(replica, deck_id, slide_id)

        request = Request(
            str(uuid4()), "update_slide_settings",
            {"deck_id": deck_id, "slide_id": slide_id,
             "expected_slide_settings_version": 0, "patch": {"background": "#AABBCCFF"}},
            self.client_id,
        )
        result1 = self._commit(replica, request)
        self.assertEqual(result1.get("result", {}).get("status"), "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(result1["result"]["value"]["slide_settings_version"], 1)

        # Exact-ID retry through admission replays the stored result without a new entry.
        log_length = len(replica.engine.state.raft.log)
        self.assertEqual(self._commit(replica, request), result1)
        self.assertEqual(len(replica.engine.state.raft.log), log_length)

        # State machine must still have version 1, not 2
        machine = KeyValueStateMachine(replica.engine.state.fsm)
        obs = machine.get_slide(deck_id, slide_id, applied_index=0)
        self.assertEqual(obs["slide"]["metadata"]["slide_settings_version"], 1)

    def test_different_content_same_request_id_returns_request_id_reuse(self) -> None:
        replica = self._make_replica()
        self._elect_leader(replica)
        deck_id, slide_id = str(uuid4()), str(uuid4())
        self._publish(replica, deck_id, slide_id)

        req_id = str(uuid4())
        result1 = self._commit(replica, Request(
            req_id, "update_slide_settings",
            {"deck_id": deck_id, "slide_id": slide_id,
             "expected_slide_settings_version": 0, "patch": {"background": "#AABBCCFF"}},
            self.client_id,
        ))
        self.assertEqual(result1.get("result", {}).get("status"), "SLIDE_SETTINGS_UPDATED")

        # Same request_id with different content through admission (fingerprint mismatch).
        log_length = len(replica.engine.state.raft.log)
        reuse = self._commit(replica, Request(
            req_id, "update_slide_settings",
            {"deck_id": deck_id, "slide_id": slide_id,
             "expected_slide_settings_version": 0, "patch": {"background": "#11223344"}},
            self.client_id,
        ))
        self.assertEqual(reuse["result"]["status"], "REQUEST_ID_REUSE")
        self.assertEqual(len(replica.engine.state.raft.log), log_length)


# ---------------------------------------------------------------------------
# Adapter-level (HTTP/RPC route) test
# ---------------------------------------------------------------------------

class SlideSettingsAdapterTests(SimulatedRaftHarnessMixin, unittest.TestCase):

    def _make_replica(self) -> App:
        import os
        import tempfile
        workspace = tempfile.mkdtemp()
        nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": "OPERATIONAL"}, clear=False):
            replica = App("n1", nodes, Path(workspace))
            replica.start()
        return replica

    def _register_peers(self, replica: App) -> None:
        for peer in ("n2", "n3"):
            replica.on_message(peer, {
                "kind": "register_node",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": peer,
                "sender_uuid": str(uuid4()),
                "term": 0,
                "incarnation": 1,
                "membership_fingerprint": replica.engine.membership_fingerprint(),
            })
        # Drain outbox
        while True:
            try:
                replica._out.get_nowait()
            except Exception:
                break

    def _elect_leader(self, replica: App) -> None:
        self._register_peers(replica)
        replica.engine.start_election()
        while True:
            try:
                dest, msg = replica._out.get_nowait()
                if dest == "n2" and msg.get("kind") == "request_vote":
                    vote = msg
                    break
            except Exception:
                break
        replica.on_message("n2", {
            "kind": "request_vote_response",
            "protocol_version": 1,
            "rpc_id": str(uuid4()),
            "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": 1,
            "reply_to": vote["rpc_id"],
            "vote_granted": True,
        })
        # Drain outbox
        while True:
            try:
                replica._out.get_nowait()
            except Exception:
                break
        self.client_id, self.access_token = self._provision_credential(replica)
        _activate_presentation_schema(replica, self)

    def _commit_request(self, replica: App, request: Request) -> dict:
        """Commit one guarded request through the real bearer-admission path."""
        return self._commit_admitted_request(replica, request, self.access_token)

    def test_update_slide_settings_routes_through_raft_and_returns_updated_status(self) -> None:
        replica = self._make_replica()
        self._elect_leader(replica)

        deck_id = str(uuid4())
        slide_id = str(uuid4())

        pub_response = self._commit_request(replica, Request(
            str(uuid4()), "publish_deck",
            {"presentation": {
                "schema_version": 3,
                "metadata": {
                    "deck_id": deck_id, "name": "Route Test", "owner_session_id": str(uuid4()),
                    "owner_user_id": str(load_account_catalog().users[0].profile.user_id),
                    "width_px": 1280, "height_px": 720,
                },
                "slides": [{"metadata": {"slide_id": slide_id, "background": "#AABBCC00"}, "elements": []}],
            }}, self.client_id,
        ))
        self.assertEqual(pub_response.get("result", {}).get("status"), "PUBLISHED")

        uss_response = self._commit_request(replica, Request(
            str(uuid4()), "update_slide_settings",
            {
                "deck_id": deck_id,
                "slide_id": slide_id,
                "expected_slide_settings_version": 0,
                "patch": {"background": "#FFEEDDCC"},
            }, self.client_id,
        ))
        result = uss_response.get("result", {})
        self.assertEqual(result.get("status"), "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(result.get("value", {}).get("slide_settings_version"), 1)

    def test_unknown_operation_is_rejected_before_raft(self) -> None:
        replica = self._make_replica()
        self._elect_leader(replica)
        response = replica.on_request(Request("bad-op", "unknown_op", {}, "client"))
        self.assertIsNotNone(response)
        self.assertEqual(response.get("result", {}).get("status"), "INVALID_REQUEST")


# ---------------------------------------------------------------------------
# Reference model tests
# ---------------------------------------------------------------------------

class SlideSettingsReferenceModelTests(unittest.TestCase):

    def _publish_state(self, deck_id: str, slide_id: str) -> dict:
        pub_args = {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id,
                    "name": "Ref Model Deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [{"metadata": {"slide_id": slide_id, "background": "#112233FF"}, "elements": []}],
            }
        }
        state, result = spec.apply(None, "publish_deck", pub_args)
        self.assertEqual(result["status"], "PUBLISHED")
        return state

    def test_update_slide_settings_in_guarantee_dict(self) -> None:
        self.assertEqual(spec.GUARANTEE.get("update_slide_settings"), "linearizable")

    def test_update_slide_settings_in_presentation_mutations(self) -> None:
        self.assertIn("update_slide_settings", spec.PRESENTATION_MUTATIONS)

    def test_success_returns_slide_settings_updated(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        new_state, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": 0,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(result["status"], "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(result["version"], 1)
        self.assertEqual(result["value"]["slide_settings_version"], 1)
        self.assertEqual(result["value"]["background"], "#AABBCCFF")
        self.assertEqual(result["value"]["ordinal"], 0)

    def test_state_is_updated_after_apply(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        new_state, _ = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": 0,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(
            new_state["slide_data"][slide_id]["metadata"]["slide_settings_version"], 1
        )
        self.assertEqual(
            new_state["slide_data"][slide_id]["metadata"]["background"], "#AABBCCFF"
        )

    def test_stale_version_returns_conflict(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        new_state, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": 5,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(result["status"], "SLIDE_SETTINGS_VERSION_CONFLICT")
        self.assertEqual(result["version"], 0)
        # State must not be mutated on conflict (values must be unchanged)
        self.assertEqual(new_state, state)

    def test_missing_slide_returns_not_found(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        _, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": str(uuid4()),  # unknown
            "expected_slide_settings_version": 0,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(result["status"], "NOT_FOUND")

    def test_deleted_slide_returns_slide_deleted(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        state, _ = spec.apply(state, "delete_slide", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_deck_version": 0,
        })
        _, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": 0,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(result["status"], "SLIDE_DELETED")

    def test_deck_version_does_not_change(self) -> None:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        state = self._publish_state(deck_id, slide_id)
        new_state, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": 0,
            "patch": {"background": "#AABBCCFF"},
        })
        self.assertEqual(new_state["deck_version"], state["deck_version"],
                         "deck_version must not change on slide settings update")

    def test_other_slide_settings_version_unchanged(self) -> None:
        deck_id = str(uuid4())
        slide1_id = str(uuid4())
        slide2_id = str(uuid4())
        pub_args = {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id, "name": "Two Slides",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280, "height_px": 720,
                },
                "slides": [
                    {"metadata": {"slide_id": slide1_id, "background": "#112233FF"}, "elements": []},
                    {"metadata": {"slide_id": slide2_id, "background": "#AABBCC00"}, "elements": []},
                ],
            }
        }
        state, _ = spec.apply(None, "publish_deck", pub_args)
        new_state, result = spec.apply(state, "update_slide_settings", {
            "deck_id": deck_id,
            "slide_id": slide1_id,
            "expected_slide_settings_version": 0,
            "patch": {"background": "#FFEEDDCC"},
        })
        self.assertEqual(result["status"], "SLIDE_SETTINGS_UPDATED")
        self.assertEqual(
            new_state["slide_data"][slide2_id]["metadata"].get("slide_settings_version", 0), 0,
            "other slide's slide_settings_version must not advance",
        )

    def test_key_function_groups_by_deck_id(self) -> None:
        deck_id = str(uuid4())
        slide_id = str(uuid4())
        self.assertEqual(
            spec.key("update_slide_settings", {"deck_id": deck_id, "slide_id": slide_id}),
            deck_id,
        )


if __name__ == "__main__":
    unittest.main()
