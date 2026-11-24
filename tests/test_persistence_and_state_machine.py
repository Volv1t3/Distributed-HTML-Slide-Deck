"""Public persistence and deterministic application seams for Stage A."""

from __future__ import annotations

import unittest
from uuid import uuid4

from command_dto import CommandEnvelopeDTO, PublishDeckPayloadDTO
from persistence import InMemoryPersistence, new_persisted_state
from raft_core import RaftEngine
from state_machine import KeyValueStateMachine


class PersistenceAndStateMachineTests(unittest.TestCase):
    @staticmethod
    def structural_command(
        *,
        operation: str,
        deck_id: object,
        payload: dict[str, object],
        request_id: str,
    ) -> CommandEnvelopeDTO:
        return CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": request_id,
                "client_id": "sofia",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(deck_id),
                "operation": operation,
                "payload": payload,
            }
        )

    @staticmethod
    def published_deck(*, deck_id: object, slides: list[object]) -> CommandEnvelopeDTO:
        return CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "publish-structural-deck",
                "client_id": "sofia",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(deck_id),
                "operation": "publish_deck",
                "payload": {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "Structural deck",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": slides,
                    }
                },
            }
        )

    def test_fresh_state_is_valid_and_round_trips_through_injected_storage(self) -> None:
        storage = InMemoryPersistence()
        state = new_persisted_state(uuid4(), "a" * 64)

        storage.save(state)
        restored = storage.load()

        self.assertEqual(restored, state)
        self.assertEqual(restored.raft.commit_index, 0)
        self.assertEqual(restored.fsm, {"kv": {}})

    def test_core_receives_transport_clock_and_persistence_as_dependencies(self) -> None:
        sent: list[tuple[str, dict[str, object]]] = []
        storage = InMemoryPersistence()
        engine = RaftEngine(
            node_slot="n1",
            voters={"n1", "n2", "n3"},
            node_uuid=uuid4(),
            storage=storage,
            send=lambda destination, message: sent.append((destination, message)),
            clock=lambda: 1.0,
        )

        self.assertEqual(engine.voters, frozenset({"n1", "n2", "n3"}))
        self.assertEqual(engine.quorum, 2)
        self.assertEqual(sent, [])

    def test_kv_put_and_noop_are_deterministic(self) -> None:
        machine = KeyValueStateMachine({"kv": {}})
        put = CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "request-1",
                "client_id": "client-1",
                "origin_node_uuid": str(uuid4()),
                "operation": "kv_put",
                "payload": {"key": "deck", "value": "first"},
            }
        )
        noop = put.model_copy(update={"operation": "raft.noop", "payload": {}})

        self.assertEqual(machine.apply(put).status, "OK")
        self.assertEqual(machine.snapshot(), {"kv": {"deck": "first"}})
        self.assertEqual(machine.apply(noop).status, "OK")
        self.assertEqual(machine.snapshot(), {"kv": {"deck": "first"}})

    def test_publish_deck_imports_one_validated_snapshot_once(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        command = CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "publish-1",
                "client_id": "sofia",
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(deck_id),
                "operation": "publish_deck",
                "payload": {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "Consensus slides",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [
                            {
                                "metadata": {
                                    "slide_id": str(slide_id),
                                    "background": "#112233FF",
                                },
                                "elements": [],
                            }
                        ],
                    }
                },
            }
        )

        machine = KeyValueStateMachine({"kv": {}})
        self.assertEqual(machine.apply(command).status, "PUBLISHED")
        snapshot = machine.snapshot()
        self.assertEqual(snapshot["decks"][str(deck_id)]["deck_version"], 0)
        self.assertEqual(snapshot["decks"][str(deck_id)]["slides"][0]["elements"], [])
        self.assertEqual(snapshot["decks"][str(deck_id)]["locks"], {})
        self.assertEqual(machine.apply(command).status, "DECK_ALREADY_PUBLISHED")

    def test_publish_payload_rejects_untyped_element_data_and_duplicate_slide_ids(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        invalid = {
            "presentation": {
                "schema_version": 2,
                "metadata": {
                    "deck_id": str(deck_id),
                    "name": "Unsafe deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [
                    {
                        "metadata": {"slide_id": str(slide_id), "background": "#112233FF"},
                        "elements": [{"element_id": str(uuid4()), "html": "<script>alert(1)</script>"}],
                    }
                ],
            }
        }

        with self.assertRaises(ValueError):
            PublishDeckPayloadDTO.model_validate(invalid)

    def test_create_slide_uses_deck_version_and_position_then_increments_once(self) -> None:
        deck_id, first_slide_id, inserted_slide_id = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {
                        "metadata": {
                            "slide_id": str(first_slide_id),
                            "background": "#112233FF",
                        },
                        "elements": [],
                    }
                ],
            )
        )

        created = machine.apply(
            self.structural_command(
                operation="create_slide",
                deck_id=deck_id,
                request_id="create-positioned-slide",
                payload={
                    "expected_deck_version": 0,
                    "slide": {
                        "metadata": {
                            "slide_id": str(inserted_slide_id),
                            "background": "#AABBCCFF",
                        },
                        "elements": [],
                    },
                    "insert_after_slide_id": str(first_slide_id),
                },
            )
        )

        self.assertEqual(created.status, "CREATED")
        self.assertEqual(created.version, 1)
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_version"], 1)
        self.assertEqual(
            [slide["metadata"]["slide_id"] for slide in deck["slides"]],
            [str(first_slide_id), str(inserted_slide_id)],
        )

        stale = machine.apply(
            self.structural_command(
                operation="create_slide",
                deck_id=deck_id,
                request_id="stale-structural-write",
                payload={
                    "expected_deck_version": 0,
                    "slide": {
                        "metadata": {
                            "slide_id": str(uuid4()),
                            "background": "#AABBCCFF",
                        },
                        "elements": [],
                    },
                },
            )
        )
        self.assertEqual(stale.status, "STRUCTURE_VERSION_CONFLICT")
        self.assertEqual(stale.version, 1)
        self.assertEqual(machine.snapshot()["decks"][str(deck_id)]["deck_version"], 1)

    def test_reorder_requires_complete_active_slide_permutation(self) -> None:
        deck_id, first_slide_id, second_slide_id = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {
                        "metadata": {"slide_id": str(first_slide_id), "background": "#112233FF"},
                        "elements": [],
                    },
                    {
                        "metadata": {"slide_id": str(second_slide_id), "background": "#445566FF"},
                        "elements": [],
                    },
                ],
            )
        )

        rejected = machine.apply(
            self.structural_command(
                operation="reorder_slides",
                deck_id=deck_id,
                request_id="partial-reorder",
                payload={"expected_deck_version": 0, "slide_ids": [str(second_slide_id)]},
            )
        )
        self.assertEqual(rejected.status, "INVALID_ARGUMENT")

        reordered = machine.apply(
            self.structural_command(
                operation="reorder_slides",
                deck_id=deck_id,
                request_id="complete-reorder",
                payload={
                    "expected_deck_version": 0,
                    "slide_ids": [str(second_slide_id), str(first_slide_id)],
                },
            )
        )
        self.assertEqual(reordered.status, "REORDERED")
        self.assertEqual(reordered.version, 1)
        self.assertEqual(
            [slide["metadata"]["slide_id"] for slide in machine.snapshot()["decks"][str(deck_id)]["slides"]],
            [str(second_slide_id), str(first_slide_id)],
        )

    def test_delete_slide_tombstones_descendants_and_rejects_deleted_predecessor(self) -> None:
        deck_id, slide_id, element_id = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {
                        "metadata": {"slide_id": str(slide_id), "background": "#112233FF"},
                        "elements": [
                            {
                                "element_id": str(element_id),
                                "kind": "text_box",
                                "version": 1,
                                "configuration": {
                                    "content": "Deleted with its slide",
                                    "generic_fallback": "sans-serif",
                                    "font_size": 12,
                                    "color": "#000000FF",
                                    "x": 0,
                                    "y": 0,
                                },
                            }
                        ],
                    }
                ],
            )
        )

        deleted = machine.apply(
            self.structural_command(
                operation="delete_slide",
                deck_id=deck_id,
                request_id="delete-slide",
                payload={"expected_deck_version": 0, "slide_id": str(slide_id)},
            )
        )
        self.assertEqual(deleted.status, "DELETED")
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["slides"], [])
        self.assertIn(str(slide_id), deck["slide_tombstones"])
        self.assertIn(str(element_id), deck["element_tombstones"])

        cannot_insert_after_deleted = machine.apply(
            self.structural_command(
                operation="create_slide",
                deck_id=deck_id,
                request_id="deleted-predecessor",
                payload={
                    "expected_deck_version": 1,
                    "slide": {
                        "metadata": {
                            "slide_id": str(uuid4()),
                            "background": "#AABBCCFF",
                        },
                        "elements": [],
                    },
                    "insert_after_slide_id": str(slide_id),
                },
            )
        )
        self.assertEqual(cannot_insert_after_deleted.status, "SLIDE_DELETED")

    def test_schema_v2_required_and_v1_rejected(self) -> None:
        deck_id = uuid4()
        from pydantic import ValidationError
        from command_dto import PublishDeckPayloadDTO

        with self.assertRaises(ValidationError):
            PublishDeckPayloadDTO.model_validate(
                {
                    "presentation": {
                        "schema_version": 1,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "Old schema deck",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [],
                    }
                }
            )

    def test_six_digit_background_rejected(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        from pydantic import ValidationError
        from command_dto import PublishDeckPayloadDTO

        with self.assertRaises(ValidationError):
            PublishDeckPayloadDTO.model_validate(
                {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "Six-digit deck",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [
                            {
                                "metadata": {
                                    "slide_id": str(slide_id),
                                    "background": "#112233",
                                },
                                "elements": [],
                            }
                        ],
                    }
                }
            )

    def test_ordinal_field_rejected_in_slide_metadata(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        from pydantic import ValidationError
        from command_dto import PublishDeckPayloadDTO

        with self.assertRaises(ValidationError):
            PublishDeckPayloadDTO.model_validate(
                {
                    "presentation": {
                        "schema_version": 2,
                        "metadata": {
                            "deck_id": str(deck_id),
                            "name": "Ordinal deck",
                            "owner_session_id": str(uuid4()),
                            "width_px": 1280,
                            "height_px": 720,
                        },
                        "slides": [
                            {
                                "metadata": {
                                    "slide_id": str(slide_id),
                                    "background": "#112233FF",
                                    "ordinal": 0,
                                },
                                "elements": [],
                            }
                        ],
                    }
                }
            )

    def test_publication_initializes_settings_versions_to_zero(self) -> None:
        deck_id, slide_id_a, slide_id_b = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        result = machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {"metadata": {"slide_id": str(slide_id_a), "background": "#112233FF"}, "elements": []},
                    {"metadata": {"slide_id": str(slide_id_b), "background": "#445566FF"}, "elements": []},
                ],
            )
        )
        self.assertEqual(result.status, "PUBLISHED")
        deck = machine.snapshot()["decks"][str(deck_id)]
        self.assertEqual(deck["deck_settings_version"], 0)
        self.assertEqual(deck["slides"][0]["metadata"]["slide_settings_version"], 0)
        self.assertEqual(deck["slides"][1]["metadata"]["slide_settings_version"], 0)

    def test_create_slide_initializes_slide_settings_version_to_zero(self) -> None:
        deck_id, existing_id, new_id = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[{"metadata": {"slide_id": str(existing_id), "background": "#112233FF"}, "elements": []}],
            )
        )
        machine.apply(
            self.structural_command(
                operation="create_slide",
                deck_id=deck_id,
                request_id="create-new-slide",
                payload={
                    "expected_deck_version": 0,
                    "slide": {
                        "metadata": {"slide_id": str(new_id), "background": "#AABBCCFF"},
                        "elements": [],
                    },
                },
            )
        )
        deck = machine.snapshot()["decks"][str(deck_id)]
        new_slide = next(s for s in deck["slides"] if s["metadata"]["slide_id"] == str(new_id))
        self.assertEqual(new_slide["metadata"]["slide_settings_version"], 0)

    def test_get_slide_returns_derived_ordinal_from_array_position(self) -> None:
        deck_id, slide_a, slide_b, slide_c = uuid4(), uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {"metadata": {"slide_id": str(slide_a), "background": "#112233FF"}, "elements": []},
                    {"metadata": {"slide_id": str(slide_b), "background": "#445566FF"}, "elements": []},
                    {"metadata": {"slide_id": str(slide_c), "background": "#AABBCCFF"}, "elements": []},
                ],
            )
        )
        result_a = machine.get_slide(str(deck_id), str(slide_a), applied_index=1)
        result_b = machine.get_slide(str(deck_id), str(slide_b), applied_index=1)
        result_c = machine.get_slide(str(deck_id), str(slide_c), applied_index=1)
        self.assertEqual(result_a["slide"]["metadata"]["ordinal"], 0)
        self.assertEqual(result_b["slide"]["metadata"]["ordinal"], 1)
        self.assertEqual(result_c["slide"]["metadata"]["ordinal"], 2)

    def test_derived_ordinal_updates_after_reorder(self) -> None:
        deck_id, slide_a, slide_b = uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[
                    {"metadata": {"slide_id": str(slide_a), "background": "#112233FF"}, "elements": []},
                    {"metadata": {"slide_id": str(slide_b), "background": "#445566FF"}, "elements": []},
                ],
            )
        )
        machine.apply(
            self.structural_command(
                operation="reorder_slides",
                deck_id=deck_id,
                request_id="reorder",
                payload={"expected_deck_version": 0, "slide_ids": [str(slide_b), str(slide_a)]},
            )
        )
        result_a = machine.get_slide(str(deck_id), str(slide_a), applied_index=1)
        result_b = machine.get_slide(str(deck_id), str(slide_b), applied_index=1)
        self.assertEqual(result_a["slide"]["metadata"]["ordinal"], 1)
        self.assertEqual(result_b["slide"]["metadata"]["ordinal"], 0)

    def test_get_deck_preview_includes_deck_settings_version_and_schema_version(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        machine.apply(
            self.published_deck(
                deck_id=deck_id,
                slides=[{"metadata": {"slide_id": str(slide_id), "background": "#112233FF"}, "elements": []}],
            )
        )
        preview = machine.get_deck_preview(str(deck_id), applied_index=1)
        self.assertEqual(preview["deck_settings_version"], 0)
        self.assertEqual(preview["schema_version"], 2)
