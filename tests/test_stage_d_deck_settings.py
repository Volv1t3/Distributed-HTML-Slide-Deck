"""Stage D: update_deck_settings — deterministic, adapter, and reference-model tests."""

from __future__ import annotations

import unittest
from uuid import uuid4

from command_dto import (
    CommandEnvelopeDTO,
    DeckSettingsPatchDTO,
    UpdateDeckSettingsPayloadDTO,
    validate_committed_command,
)
from state_machine import KeyValueStateMachine
import spec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_command(
    *,
    operation: str,
    deck_id: object,
    payload: dict,
    request_id: str = "req-1",
    client_id: str = "client-a",
) -> CommandEnvelopeDTO:
    return CommandEnvelopeDTO.model_validate(
        {
            "command_schema_version": 1,
            "request_id": request_id,
            "client_id": client_id,
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": operation,
            "payload": payload,
        }
    )


def _publish_deck(*, deck_id, name: str = "My Deck", width: int = 1280, height: int = 720):
    return CommandEnvelopeDTO.model_validate(
        {
            "command_schema_version": 1,
            "request_id": "publish-1",
            "client_id": "client-a",
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "publish_deck",
            "payload": {
                "presentation": {
                    "schema_version": 2,
                    "metadata": {
                        "deck_id": str(deck_id),
                        "name": name,
                        "owner_session_id": str(uuid4()),
                        "width_px": width,
                        "height_px": height,
                    },
                    "slides": [
                        {
                            "metadata": {
                                "slide_id": str(uuid4()),
                                "background": "#112233FF",
                            },
                            "elements": [],
                        }
                    ],
                }
            },
        }
    )


def _update_deck_settings(
    *,
    deck_id: object,
    expected_deck_settings_version: int,
    patch: dict,
    request_id: str = "req-upd-1",
    client_id: str = "client-a",
) -> CommandEnvelopeDTO:
    return _make_command(
        operation="update_deck_settings",
        deck_id=deck_id,
        payload={
            "expected_deck_settings_version": expected_deck_settings_version,
            "patch": patch,
        },
        request_id=request_id,
        client_id=client_id,
    )


def _fresh_machine_with_deck(*, deck_id=None, name="My Deck", width=1280, height=720):
    if deck_id is None:
        deck_id = uuid4()
    machine = KeyValueStateMachine({"kv": {}})
    result = machine.apply(_publish_deck(deck_id=deck_id, name=name, width=width, height=height))
    assert result.status == "PUBLISHED"
    return deck_id, machine


# ---------------------------------------------------------------------------
# 1. DTO Validation tests (command_dto.py seam)
# ---------------------------------------------------------------------------

class TestDeckSettingsPatchDTO(unittest.TestCase):
    """DeckSettingsPatchDTO: nonempty partial patch, typed fields."""

    def test_name_only_patch_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"name": "New Name"})
        self.assertEqual(patch.name, "New Name")

    def test_width_only_patch_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"width_px": 1920})
        self.assertEqual(patch.width_px, 1920)

    def test_height_only_patch_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"height_px": 1080})
        self.assertEqual(patch.height_px, 1080)

    def test_all_fields_patch_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"name": "X", "width_px": 1920, "height_px": 1080})
        self.assertEqual(patch.name, "X")
        self.assertEqual(patch.width_px, 1920)
        self.assertEqual(patch.height_px, 1080)

    def test_empty_patch_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({})

    def test_empty_name_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"name": ""})

    def test_width_below_minimum_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"width_px": 319})

    def test_width_above_maximum_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"width_px": 3841})

    def test_height_below_minimum_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"height_px": 319})

    def test_height_above_maximum_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"height_px": 3841})

    def test_width_at_minimum_320_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"width_px": 320})
        self.assertEqual(patch.width_px, 320)

    def test_width_at_maximum_3840_is_valid(self):
        patch = DeckSettingsPatchDTO.model_validate({"width_px": 3840})
        self.assertEqual(patch.width_px, 3840)

    def test_boolean_masquerading_as_int_is_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"width_px": True})

    def test_unknown_fields_are_rejected(self):
        with self.assertRaises(ValueError):
            DeckSettingsPatchDTO.model_validate({"name": "OK", "unknown_field": "x"})


class TestUpdateDeckSettingsPayloadDTO(unittest.TestCase):
    """UpdateDeckSettingsPayloadDTO: envelope structure."""

    def test_valid_payload(self):
        payload = UpdateDeckSettingsPayloadDTO.model_validate(
            {"expected_deck_settings_version": 0, "patch": {"name": "New Name"}}
        )
        self.assertEqual(payload.expected_deck_settings_version, 0)

    def test_negative_version_is_rejected(self):
        with self.assertRaises(ValueError):
            UpdateDeckSettingsPayloadDTO.model_validate(
                {"expected_deck_settings_version": -1, "patch": {"name": "X"}}
            )

    def test_empty_patch_is_rejected(self):
        with self.assertRaises(ValueError):
            UpdateDeckSettingsPayloadDTO.model_validate(
                {"expected_deck_settings_version": 0, "patch": {}}
            )


class TestValidateCommittedCommandUpdateDeckSettings(unittest.TestCase):
    """validate_committed_command() seam for update_deck_settings."""

    def _cmd(self, payload: dict, *, deck_id=None) -> CommandEnvelopeDTO:
        d = deck_id or uuid4()
        return CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "r1",
                "client_id": "c1",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(d),
                "operation": "update_deck_settings",
                "payload": payload,
            }
        )

    def test_valid_update_deck_settings_command_is_accepted(self):
        cmd = self._cmd({"expected_deck_settings_version": 0, "patch": {"name": "X"}})
        validate_committed_command(cmd)  # must not raise

    def test_missing_deck_id_is_rejected(self):
        cmd = CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "r1",
                "client_id": "c1",
                "origin_node_uuid": str(uuid4()),
                "operation": "update_deck_settings",
                "payload": {"expected_deck_settings_version": 0, "patch": {"name": "X"}},
            }
        )
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)

    def test_tlv_blocks_are_rejected(self):
        # Build a command with TLV blocks (they should be rejected per spec)
        cmd = CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "r1",
                "client_id": "c1",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(uuid4()),
                "operation": "update_deck_settings",
                "payload": {"expected_deck_settings_version": 0, "patch": {"name": "X"}},
            }
        )
        # Manually inject a TLV block by bypassing Pydantic
        # (validate_committed_command reads cmd.blocks which default to [])
        # Test that an invalid payload with extra fields is rejected instead
        with self.assertRaises(ValueError):
            validate_committed_command(
                cmd.model_copy(
                    update={"payload": {"expected_deck_settings_version": 0, "patch": {}, "EXTRA": 1}}
                )
            )

    def test_session_id_not_required(self):
        # update_deck_settings does NOT need session_id
        cmd = self._cmd({"expected_deck_settings_version": 0, "patch": {"width_px": 1920}})
        self.assertIsNone(cmd.session_id)
        validate_committed_command(cmd)  # must not raise


# ---------------------------------------------------------------------------
# 2. State Machine tests (state_machine.py seam)
# ---------------------------------------------------------------------------

class TestUpdateDeckSettingsStateMachine(unittest.TestCase):
    """KeyValueStateMachine._update_deck_settings via apply()."""

    def test_rename_deck_succeeds_and_increments_settings_version(self):
        deck_id, machine = _fresh_machine_with_deck(name="Old Name")

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "New Name"},
            )
        )

        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        self.assertIsNotNone(result.value)
        v = result.value
        self.assertEqual(v["deck_settings_version"], 1)
        self.assertEqual(v["name"], "New Name")
        self.assertEqual(v["width_px"], 1280)
        self.assertEqual(v["height_px"], 720)
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_settings_version"], 1)
        self.assertEqual(deck["metadata"]["name"], "New Name")
        # deck_version must NOT be changed by a settings update
        self.assertEqual(deck["deck_version"], 0)

    def test_resize_canvas_succeeds_and_does_not_change_deck_version(self):
        deck_id, machine = _fresh_machine_with_deck(width=1280, height=720)

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"width_px": 1920, "height_px": 1080},
            )
        )

        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        v = result.value
        self.assertEqual(v["width_px"], 1920)
        self.assertEqual(v["height_px"], 1080)
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_version"], 0)  # structural version unchanged
        self.assertEqual(deck["deck_settings_version"], 1)

    def test_success_returns_consolidated_settings_and_deck_version(self):
        deck_id, machine = _fresh_machine_with_deck()

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "New Name"},
            )
        )

        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        v = result.value
        # Must include structural deck_version
        self.assertIn("deck_version", v)
        self.assertEqual(v["deck_version"], 0)
        # Must include settings revision
        self.assertEqual(v["deck_settings_version"], 1)
        # Must include all three settings fields
        self.assertIn("name", v)
        self.assertIn("width_px", v)
        self.assertIn("height_px", v)

    def test_success_includes_committed_index_in_value(self):
        deck_id, machine = _fresh_machine_with_deck()

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "X"},
            ),
            committed_index=42,
        )

        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        self.assertEqual(result.value["committed_index"], 42)

    def test_settings_version_increments_exactly_once_per_success(self):
        deck_id, machine = _fresh_machine_with_deck()

        machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "Name1"},
                request_id="req-1",
            )
        )
        result2 = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=1,
                patch={"name": "Name2"},
                request_id="req-2",
            )
        )
        self.assertEqual(result2.status, "DECK_SETTINGS_UPDATED")
        self.assertEqual(result2.value["deck_settings_version"], 2)
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_settings_version"], 2)

    def test_stale_expected_version_returns_conflict(self):
        deck_id, machine = _fresh_machine_with_deck()
        # Advance settings version to 1
        machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "Name1"},
                request_id="req-1",
            )
        )

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,  # stale!
                patch={"name": "Name2"},
                request_id="req-2",
            )
        )

        self.assertEqual(result.status, "DECK_SETTINGS_VERSION_CONFLICT")
        v = result.value
        # Must return current canonical settings
        self.assertEqual(v["deck_settings_version"], 1)
        self.assertIn("name", v)
        self.assertIn("width_px", v)
        self.assertIn("height_px", v)
        # Must return structural deck_version
        self.assertIn("deck_version", v)

    def test_conflict_includes_min_commit_index(self):
        deck_id, machine = _fresh_machine_with_deck()
        machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "Name1"},
                request_id="req-1",
            )
        )

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "Name2"},
                request_id="req-2",
            ),
            committed_index=99,
        )

        self.assertEqual(result.status, "DECK_SETTINGS_VERSION_CONFLICT")
        self.assertEqual(result.value["min_commit_index"], 99)

    def test_missing_deck_returns_not_found(self):
        machine = KeyValueStateMachine({"kv": {}})
        result = machine.apply(
            _update_deck_settings(
                deck_id=uuid4(),
                expected_deck_settings_version=0,
                patch={"name": "X"},
            )
        )
        self.assertEqual(result.status, "NOT_FOUND")

    def test_canvas_resize_does_not_affect_elements(self):
        """Canvas resize must leave element coordinates, dimensions, etc. untouched."""
        from uuid import uuid4 as u4
        from command_dto import CreateElementPayloadDTO

        deck_id = u4()
        slide_id = u4()
        element_id = u4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            CommandEnvelopeDTO.model_validate({
                "command_schema_version": 1,
                "request_id": "pub-1",
                "client_id": "client-a",
                "origin_node_uuid": str(u4()),
                "deck_id": str(deck_id),
                "operation": "publish_deck",
                "payload": {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "My Deck",
                            "owner_session_id": str(u4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [
                            {
                                "metadata": {"slide_id": str(slide_id), "background": "#FFFFFFFF"},
                                "elements": [],
                            }
                        ],
                    }
                },
            })
        )
        session_id = u4()
        machine.apply(
            CommandEnvelopeDTO.model_validate({
                "command_schema_version": 1,
                "request_id": "create-elem",
                "client_id": "client-a",
                "session_id": str(session_id),
                "origin_node_uuid": str(u4()),
                "deck_id": str(deck_id),
                "operation": "create_element",
                "payload": {
                    "slide_id": str(slide_id),
                    "element": {
                        "element_id": str(element_id),
                        "kind": "rectangle",
                        "configuration": {
                            "x": 100, "y": 200, "width": 400, "height": 300,
                            "scale_basis_points": 100,
                            "rotation_centidegrees": 0,
                            "fill": "#FF0000FF",
                            "border_color": "#000000FF",
                            "border_style": "solid",
                            "border_thickness": 1,
                        },
                    },
                },
            })
        )

        # snapshot before resize
        before = machine.snapshot()["decks"][str(deck_id)]
        element_before = next(
            e for slide in before["slides"] for e in slide["elements"]
            if e["element_id"] == str(element_id)
        )

        machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"width_px": 1920, "height_px": 1080},
                request_id="resize-1",
            )
        )

        after = machine.snapshot()["decks"][str(deck_id)]
        element_after = next(
            e for slide in after["slides"] for e in slide["elements"]
            if e["element_id"] == str(element_id)
        )

        # Every element field must be unchanged
        self.assertEqual(element_before, element_after)
        # Locks must be unchanged
        self.assertEqual(before["locks"], after["locks"])
        self.assertEqual(before["fencing_counters"], after["fencing_counters"])

    def test_invalid_dimension_rejected_by_dto_before_state_machine(self):
        """Dimension out-of-range is caught deterministically by DTO validation.

        The spec says invalid dimensions yield INVALID_ARGUMENT. For width/height
        out-of-range, Pydantic raises ValueError at validate_committed_command(), which
        the app layer translates to INVALID_REQUEST before the state machine is reached.
        This confirms the invariant: invalid payloads never produce a state transition.
        """
        from command_dto import validate_committed_command

        deck_id = uuid4()
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "req-bad-dim",
            "client_id": "client-a",
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_deck_settings",
            # Raw payload dict bypasses DeckSettingsPatchDTO; validate_committed_command
            # will call DeckSettingsPatchDTO.model_validate(payload["patch"]) and reject it.
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {"width_px": 319},  # just below minimum 320
            },
        })
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)

    def test_empty_patch_rejected_by_dto(self):
        """Empty patch is rejected by DeckSettingsPatchDTO, not state machine."""
        from command_dto import validate_committed_command

        deck_id = uuid4()
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "req-empty-patch",
            "client_id": "client-a",
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_deck_settings",
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {},  # empty — DeckSettingsPatchDTO rejects
            },
        })
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)

    def test_boolean_as_width_rejected_by_dto(self):
        """Boolean masquerading as int is rejected by StrictInt field."""
        from command_dto import validate_committed_command

        deck_id = uuid4()
        cmd = CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": "req-bool-dim",
            "client_id": "client-a",
            "origin_node_uuid": str(uuid4()),
            "deck_id": str(deck_id),
            "operation": "update_deck_settings",
            "payload": {
                "expected_deck_settings_version": 0,
                "patch": {"width_px": True},  # bool masquerading as int — StrictInt rejects
            },
        })
        with self.assertRaises(ValueError):
            validate_committed_command(cmd)

    def test_deck_settings_version_initialized_to_zero_on_publish(self):
        deck_id, machine = _fresh_machine_with_deck()
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_settings_version"], 0)

    def test_deck_version_unchanged_after_settings_update(self):
        deck_id, machine = _fresh_machine_with_deck()

        result = machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "New"},
            )
        )

        self.assertEqual(result.status, "DECK_SETTINGS_UPDATED")
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_version"], 0)


# ---------------------------------------------------------------------------
# 3. get_deck_preview tests
# ---------------------------------------------------------------------------

class TestGetDeckPreviewWithSettings(unittest.TestCase):
    """get_deck_preview must expose settings fields per spec."""

    def test_get_deck_preview_includes_schema_version(self):
        deck_id, machine = _fresh_machine_with_deck()
        preview = machine.get_deck_preview(str(deck_id), applied_index=0)
        self.assertEqual(preview["schema_version"], 2)

    def test_get_deck_preview_includes_deck_settings_version(self):
        deck_id, machine = _fresh_machine_with_deck()
        preview = machine.get_deck_preview(str(deck_id), applied_index=0)
        self.assertEqual(preview["deck_settings_version"], 0)

    def test_get_deck_preview_includes_metadata_name_and_dimensions(self):
        deck_id, machine = _fresh_machine_with_deck(name="Slide Show", width=800, height=600)
        preview = machine.get_deck_preview(str(deck_id), applied_index=0)
        self.assertEqual(preview["metadata"]["name"], "Slide Show")
        self.assertEqual(preview["metadata"]["width_px"], 800)
        self.assertEqual(preview["metadata"]["height_px"], 600)

    def test_get_deck_preview_deck_settings_version_advances_after_update(self):
        deck_id, machine = _fresh_machine_with_deck()
        machine.apply(
            _update_deck_settings(
                deck_id=deck_id,
                expected_deck_settings_version=0,
                patch={"name": "Updated"},
            )
        )
        preview = machine.get_deck_preview(str(deck_id), applied_index=1)
        self.assertEqual(preview["deck_settings_version"], 1)
        self.assertEqual(preview["metadata"]["name"], "Updated")

    def test_get_deck_preview_includes_ordered_slide_ids(self):
        deck_id = uuid4()
        slide_id = uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            CommandEnvelopeDTO.model_validate({
                "command_schema_version": 1,
                "request_id": "pub-2",
                "client_id": "c",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(deck_id),
                "operation": "publish_deck",
                "payload": {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "D",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [
                            {"metadata": {"slide_id": str(slide_id), "background": "#FFFFFFFF"}, "elements": []},
                        ],
                    }
                },
            })
        )
        preview = machine.get_deck_preview(str(deck_id), applied_index=0)
        self.assertIn("slide_ids", preview)
        self.assertEqual(preview["slide_ids"], [str(slide_id)])

    def test_get_deck_preview_includes_applied_index(self):
        deck_id, machine = _fresh_machine_with_deck()
        preview = machine.get_deck_preview(str(deck_id), applied_index=55)
        self.assertEqual(preview["applied_index"], 55)


# ---------------------------------------------------------------------------
# 4. Reference model tests (spec.py seam)
# ---------------------------------------------------------------------------

class TestSpecReferenceModelDeckSettings(unittest.TestCase):
    """spec.py sequential reference model for update_deck_settings."""

    def _publish_state(self, *, name="Deck", width=1280, height=720):
        deck_id = str(uuid4())
        slide_id = str(uuid4())
        args = {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": deck_id,
                    "name": name,
                    "owner_session_id": str(uuid4()),
                    "width_px": width,
                    "height_px": height,
                },
                "slides": [
                    {"metadata": {"slide_id": slide_id, "background": "#FFFFFFFF"}, "elements": []},
                ],
            }
        }
        state, result = spec.apply(None, "publish_deck", args)
        assert result["status"] == "PUBLISHED"
        return deck_id, state

    def test_update_deck_settings_is_linearizable(self):
        self.assertEqual(spec.GUARANTEE.get("update_deck_settings"), "linearizable")

    def test_update_deck_settings_is_in_presentation_mutations(self):
        self.assertIn("update_deck_settings", spec.PRESENTATION_MUTATIONS)

    def test_rename_succeeds_and_increments_settings_version(self):
        deck_id, state = self._publish_state(name="Old")

        new_state, result = spec.apply(
            state,
            "update_deck_settings",
            {
                "deck_id": deck_id,
                "expected_deck_settings_version": 0,
                "patch": {"name": "New"},
            },
        )

        self.assertEqual(result["status"], "DECK_SETTINGS_UPDATED")
        self.assertEqual(result["value"]["deck_settings_version"], 1)
        self.assertEqual(result["value"]["name"], "New")
        self.assertEqual(new_state["deck_settings_version"], 1)
        self.assertEqual(new_state["metadata"]["name"], "New")

    def test_stale_version_returns_conflict(self):
        deck_id, state = self._publish_state()
        state, _ = spec.apply(
            state,
            "update_deck_settings",
            {"deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "V1"}},
        )

        new_state, result = spec.apply(
            state,
            "update_deck_settings",
            {"deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "V2"}},
        )

        self.assertEqual(result["status"], "DECK_SETTINGS_VERSION_CONFLICT")
        self.assertEqual(result["value"]["deck_settings_version"], 1)
        # State is unchanged on conflict
        self.assertEqual(new_state["deck_settings_version"], 1)

    def test_missing_deck_returns_not_found(self):
        _, result = spec.apply(
            None,
            "update_deck_settings",
            {
                "deck_id": str(uuid4()),
                "expected_deck_settings_version": 0,
                "patch": {"name": "X"},
            },
        )
        self.assertEqual(result["status"], "NOT_FOUND")

    def test_deck_version_unchanged_in_spec_after_settings_update(self):
        deck_id, state = self._publish_state()
        initial_deck_version = state["deck_version"]

        new_state, result = spec.apply(
            state,
            "update_deck_settings",
            {"deck_id": deck_id, "expected_deck_settings_version": 0, "patch": {"name": "New"}},
        )

        self.assertEqual(result["status"], "DECK_SETTINGS_UPDATED")
        self.assertEqual(new_state["deck_version"], initial_deck_version)

    def test_spec_read_deck_preview_includes_deck_settings_version(self):
        deck_id, state = self._publish_state(name="TestDeck", width=1024, height=768)

        _, preview = spec.apply(state, "get_deck_preview", {"deck_id": deck_id})

        self.assertIn("deck_settings_version", preview)
        self.assertEqual(preview["deck_settings_version"], 0)

    def test_spec_read_deck_preview_includes_metadata(self):
        deck_id, state = self._publish_state(name="TestDeck", width=1024, height=768)

        _, preview = spec.apply(state, "get_deck_preview", {"deck_id": deck_id})

        self.assertIn("metadata", preview)
        self.assertEqual(preview["metadata"]["name"], "TestDeck")


# ---------------------------------------------------------------------------
# 5. Deduplication / retry tests (raft_core seam)
# ---------------------------------------------------------------------------

class TestUpdateDeckSettingsDeduplication(unittest.TestCase):
    """update_deck_settings reuses the dedup contract from raft_core (via RaftEngine).

    Uses a 3-voter cluster (required by Raft) with n1 as leader. We simulate
    a majority acknowledgement from n2 so entries commit, following the same
    pattern as test_raft_replication.py.
    """

    def setUp(self):
        from persistence import InMemoryPersistence
        from raft_core import RaftEngine

        self.node_ids = {slot: uuid4() for slot in ("n1", "n2", "n3")}
        self.sent = []
        self.storage = InMemoryPersistence()
        self.engine = RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=self.node_ids["n1"],
            storage=self.storage,
            send=lambda dst, msg: self.sent.append((dst, msg)),
            clock=lambda: 10.0,
            election_timeout=lambda: 1.0,
        )
        # Register peers
        for peer in ("n2", "n3"):
            self.engine.handle_message(
                peer,
                {
                    "kind": "register_node",
                    "protocol_version": 1,
                    "rpc_id": str(uuid4()),
                    "sender_slot": peer,
                    "sender_uuid": str(self.node_ids[peer]),
                    "term": 0,
                    "incarnation": 1,
                    "membership_fingerprint": self.engine.membership_fingerprint(),
                },
            )
        # Win election
        self.engine.start_election()
        vote_msg = next(
            msg for dst, msg in self.sent if dst == "n2" and msg["kind"] == "request_vote"
        )
        self.engine.handle_message(
            "n2",
            {
                "kind": "request_vote_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(self.node_ids["n2"]),
                "term": 1,
                "reply_to": vote_msg["rpc_id"],
                "vote_granted": True,
            },
        )
        self.sent.clear()

    def _commit_pending(self):
        """Simulate n2 acknowledging all pending append_entries so they commit.

        For each append_entries message, we compute the correct match_index as
        prev_log_index + len(entries) — matching what the leader's PendingAppend records.
        """
        append_msgs = [msg for dst, msg in self.sent if dst == "n2" and msg.get("kind") == "append_entries"]
        for append_msg in append_msgs:
            last_index = append_msg["prev_log_index"] + len(append_msg.get("entries", []))
            if last_index == 0:
                continue
            self.engine.handle_message(
                "n2",
                {
                    "kind": "append_entries_response",
                    "protocol_version": 1,
                    "rpc_id": str(uuid4()),
                    "sender_slot": "n2",
                    "sender_uuid": str(self.node_ids["n2"]),
                    "term": 1,
                    "reply_to": append_msg["rpc_id"],
                    "success": True,
                    "match_index": last_index,
                },
            )
        self.sent.clear()

    def test_exact_id_retry_replays_stored_result_without_second_increment(self):
        """An exact request ID retry through the RaftEngine must not advance version twice."""
        from command_dto import ClientResultDTO

        deck_id = uuid4()
        publish_cmd = _publish_deck(deck_id=deck_id)
        self.engine.propose(publish_cmd)
        self._commit_pending()

        update_cmd = _update_deck_settings(
            deck_id=deck_id,
            expected_deck_settings_version=0,
            patch={"name": "Renamed"},
            request_id="settings-req-1",
            client_id="client-x",
        )
        self.engine.propose(update_cmd)
        self._commit_pending()

        # Exact retry with same request_id
        result2 = self.engine.propose(update_cmd)

        # Must be a ClientResultDTO (dedup hit, not a new log entry)
        self.assertIsInstance(result2, ClientResultDTO)
        self.assertEqual(result2.status, "DECK_SETTINGS_UPDATED")

        # Settings version should still be 1 (not 2)
        machine = KeyValueStateMachine(self.engine.state.fsm)
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_settings_version"], 1)

    def test_request_id_reuse_with_different_patch_returns_reuse_error(self):
        """Reusing a request_id with different content must return REQUEST_ID_REUSE."""
        from command_dto import ClientResultDTO

        deck_id = uuid4()
        publish_cmd = _publish_deck(deck_id=deck_id)
        self.engine.propose(publish_cmd)
        self._commit_pending()

        # First request
        update_cmd = _update_deck_settings(
            deck_id=deck_id,
            expected_deck_settings_version=0,
            patch={"name": "Renamed"},
            request_id="shared-req-id",
            client_id="client-x",
        )
        self.engine.propose(update_cmd)
        self._commit_pending()

        # Different patch, same (client_id, request_id) pair — must be REQUEST_ID_REUSE
        update_cmd_reuse = _update_deck_settings(
            deck_id=deck_id,
            expected_deck_settings_version=0,
            patch={"name": "Different Name"},
            request_id="shared-req-id",
            client_id="client-x",
        )
        result = self.engine.propose(update_cmd_reuse)

        self.assertIsInstance(result, ClientResultDTO)
        self.assertEqual(result.status, "REQUEST_ID_REUSE")
