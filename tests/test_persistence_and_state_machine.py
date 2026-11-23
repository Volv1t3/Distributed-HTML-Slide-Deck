"""Public persistence and deterministic application seams for Stage A."""

from __future__ import annotations

import unittest
from uuid import uuid4

from command_dto import CommandEnvelopeDTO, PublishDeckPayloadDTO
from persistence import InMemoryPersistence, new_persisted_state
from raft_core import RaftEngine
from state_machine import KeyValueStateMachine


class PersistenceAndStateMachineTests(unittest.TestCase):
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
                        "schema_version": 1,
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
                                    "background": "#112233",
                                    "ordinal": 0,
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
        self.assertNotIn("locks", snapshot["decks"][str(deck_id)])
        self.assertEqual(machine.apply(command).status, "DECK_ALREADY_PUBLISHED")

    def test_publish_payload_rejects_untyped_element_data_and_duplicate_slide_ids(self) -> None:
        deck_id, slide_id = uuid4(), uuid4()
        invalid = {
            "presentation": {
                "schema_version": 1,
                "metadata": {
                    "deck_id": str(deck_id),
                    "name": "Unsafe deck",
                    "owner_session_id": str(uuid4()),
                    "width_px": 1280,
                    "height_px": 720,
                },
                "slides": [
                    {
                        "metadata": {"slide_id": str(slide_id), "background": "#112233", "ordinal": 0},
                        "elements": [{"element_id": str(uuid4()), "html": "<script>alert(1)</script>"}],
                    }
                ],
            }
        }

        with self.assertRaises(ValueError):
            PublishDeckPayloadDTO.model_validate(invalid)
