"""Public Stage B typed-element and local-observation contracts."""

from __future__ import annotations

import unittest
from uuid import uuid4

from pydantic import ValidationError

from command_dto import CommandEnvelopeDTO, PresentationElementDTO
from state_machine import KeyValueStateMachine


class TypedElementsTests(unittest.TestCase):
    def command(self, deck_id, slide_id, element, session_id):
        return CommandEnvelopeDTO.model_validate(
            {
                "command_schema_version": 1,
                "request_id": "create-element",
                "client_id": "sofia",
                "session_id": str(session_id),
                "origin_node_uuid": str(uuid4()),
                "deck_id": str(deck_id),
                "operation": "create_element",
                "payload": {"slide_id": str(slide_id), "element": element},
            }
        )

    def publish(self, machine, deck_id, slide_id):
        machine.apply(
            CommandEnvelopeDTO.model_validate(
                {
                    "command_schema_version": 1,
                    "request_id": "publish-elements",
                    "client_id": "sofia",
                    "origin_node_uuid": str(uuid4()),
                    "deck_id": str(deck_id),
                    "operation": "publish_deck",
                    "payload": {"presentation": {"schema_version": 1, "metadata": {
                        "deck_id": str(deck_id), "name": "Elements", "owner_session_id": str(uuid4()),
                        "width_px": 1280, "height_px": 720}, "slides": [{"metadata": {
                        "slide_id": str(slide_id), "background": "#112233", "ordinal": 0}, "elements": []}]}},
                }
            )
        )

    def test_each_baseline_kind_accepts_only_its_typed_deterministic_configuration(self):
        text = {"content": "Title", "font_families": ["Arial"], "generic_fallback": "sans-serif",
                "font_size": 3200, "color": "#112233FF", "x": -10, "y": 20,
                "line_spacing": 0, "indentation": 0, "bold": True, "underline": False}
        shape = {"x": -10, "y": 20, "width": 100, "height": 100, "scale_basis_points": 10000,
                 "rotation_centidegrees": 0, "fill": "#112233FF", "border_color": "#00000000",
                 "border_style": "solid", "border_thickness": 0}
        for kind in ("heading", "subtitle", "text_box"):
            self.assertEqual(PresentationElementDTO(element_id=uuid4(), kind=kind, configuration=text).kind, kind)
        for kind in ("rectangle", "square", "circle"):
            self.assertEqual(PresentationElementDTO(element_id=uuid4(), kind=kind, configuration=shape).kind, kind)
        self.assertEqual(PresentationElementDTO(element_id=uuid4(), kind="triangle", configuration={**shape, "sides": 3}).kind, "triangle")
        self.assertEqual(PresentationElementDTO(element_id=uuid4(), kind="regular_polygon", configuration={**shape, "sides": 6}).kind, "regular_polygon")
        self.assertEqual(PresentationElementDTO(element_id=uuid4(), kind="irregular_polygon", configuration={**shape, "vertices": [[0, 0], [100, 0], [20, 50]]}).kind, "irregular_polygon")
        with self.assertRaises(ValidationError):
            PresentationElementDTO(element_id=uuid4(), kind="circle", configuration={**shape, "width": 99})
        with self.assertRaises(ValidationError):
            PresentationElementDTO(element_id=uuid4(), kind="triangle", configuration=shape)
        with self.assertRaises(ValidationError):
            PresentationElementDTO(element_id=uuid4(), kind="irregular_polygon", configuration={**shape, "vertices": [[0, 0], [1, 1]]})

    def test_create_element_commits_version_one_and_first_lock_together(self):
        deck_id, slide_id, element_id, session_id = uuid4(), uuid4(), uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        self.publish(machine, deck_id, slide_id)
        element = {"element_id": str(element_id), "kind": "rectangle", "version": 1, "configuration": {
            "x": 0, "y": 0, "width": 100, "height": 60, "scale_basis_points": 10000,
            "rotation_centidegrees": 0, "fill": "#112233FF", "border_color": "#000000FF",
            "border_style": "solid", "border_thickness": 2, "z_index": 7}}

        result = machine.apply(self.command(deck_id, slide_id, element, session_id))

        self.assertEqual(result.status, "CREATED_AND_LOCKED")
        self.assertEqual(result.version, 1)
        self.assertEqual(result.value["fencing_token"], 1)
        observed = machine.get_element(str(deck_id), str(element_id), applied_index=9)
        self.assertEqual(observed["applied_index"], 9)
        self.assertEqual(observed["element"]["version"], 1)
        self.assertEqual(observed["lock"]["session_id"], str(session_id))
        self.assertEqual(observed["lock"]["fencing_token"], 1)

        lower = {**element, "element_id": str(uuid4()), "configuration": {**element["configuration"], "z_index": -1}}
        machine.apply(self.command(deck_id, slide_id, lower, uuid4()))
        slide = machine.get_slide(str(deck_id), str(slide_id), applied_index=9)
        self.assertEqual([item["configuration"]["z_index"] for item in slide["slide"]["elements"]], [-1, 7])

    def test_local_observations_are_read_only_and_include_replica_progress(self):
        deck_id, slide_id = uuid4(), uuid4()
        machine = KeyValueStateMachine({"kv": {}})
        self.publish(machine, deck_id, slide_id)

        preview = machine.get_deck_preview(str(deck_id), applied_index=4)
        slide = machine.get_slide(str(deck_id), str(slide_id), applied_index=4)
        lock = machine.read_lock_status(str(deck_id), str(uuid4()), applied_index=4)

        self.assertEqual(preview["applied_index"], 4)
        self.assertEqual(preview["deck_version"], 0)
        self.assertEqual(slide["applied_index"], 4)
        self.assertEqual(lock["applied_index"], 4)
        self.assertIsNone(lock["lock"])

