"""Public lifecycle contracts for protected shared-element edits."""

from __future__ import annotations

import unittest
from uuid import uuid4

from command_dto import CommandEnvelopeDTO
from state_machine import KeyValueStateMachine


class ElementLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.deck_id, self.slide_id, self.element_id = uuid4(), uuid4(), uuid4()
        self.sofia, self.santiago = uuid4(), uuid4()
        self.machine = KeyValueStateMachine({"kv": {}})
        self.machine.apply(self.command("publish_deck", {
            "presentation": {"schema_version": 1, "metadata": {
                "deck_id": str(self.deck_id), "name": "Lifecycle", "owner_session_id": str(uuid4()),
                "width_px": 1280, "height_px": 720}, "slides": [{"metadata": {
                    "slide_id": str(self.slide_id), "background": "#112233", "ordinal": 0}, "elements": []}]},
        }))
        created = self.machine.apply(self.command("create_element", {
            "slide_id": str(self.slide_id), "element": self.element(),
        }, session_id=self.sofia))
        self.assertEqual(created.status, "CREATED_AND_LOCKED")

    def command(self, operation, payload, *, session_id=None, request_id=None):
        return CommandEnvelopeDTO.model_validate({
            "command_schema_version": 1,
            "request_id": request_id or f"{operation}-{uuid4()}",
            "client_id": "sofia" if session_id == self.sofia else "santiago",
            "session_id": str(session_id) if session_id is not None else None,
            "origin_node_uuid": str(uuid4()), "deck_id": str(self.deck_id),
            "operation": operation, "payload": payload,
        })

    def element(self):
        return {"element_id": str(self.element_id), "kind": "text_box", "version": 1,
                "configuration": {"content": "Before", "font_families": ["Arial"],
                    "generic_fallback": "sans-serif", "font_size": 24, "color": "#000000FF",
                    "x": 0, "y": 0}}

    def test_update_is_one_atomic_typed_transition_and_version_conflict_reports_current_version(self):
        saved = self.machine.apply(self.command("update_element", {
            "element_id": str(self.element_id), "fencing_token": 1, "expected_version": 1,
            "patch": {"content": "After", "x": 125, "bold": True},
        }, session_id=self.sofia))
        self.assertEqual(saved.status, "SAVED")
        self.assertEqual(saved.version, 2)
        observed = self.machine.get_element(str(self.deck_id), str(self.element_id), applied_index=10)
        self.assertEqual(observed["element"]["configuration"]["content"], "After")
        self.assertEqual(observed["element"]["configuration"]["x"], 125)
        self.assertTrue(observed["element"]["configuration"]["bold"])

        stale = self.machine.apply(self.command("update_element", {
            "element_id": str(self.element_id), "fencing_token": 1, "expected_version": 1,
            "patch": {"content": "Must not save"},
        }, session_id=self.sofia))
        self.assertEqual(stale.status, "VERSION_CONFLICT")
        self.assertEqual(stale.value["current_version"], 2)
        self.assertEqual(self.machine.get_element(str(self.deck_id), str(self.element_id), applied_index=11)["element"]["configuration"]["content"], "After")

    def test_competing_and_repeated_acquisition_leave_one_authoritative_owner(self):
        competing = self.machine.apply(self.command("acquire_lock", {
            "element_id": str(self.element_id),
        }, session_id=self.santiago))
        self.assertEqual(competing.status, "LOCKED")
        self.assertEqual(competing.value["owner_session"], str(self.sofia))
        repeated = self.machine.apply(self.command("acquire_lock", {
            "element_id": str(self.element_id),
        }, session_id=self.sofia))
        self.assertEqual(repeated.status, "ALREADY_HELD")
        self.assertEqual(repeated.value["fencing_token"], 1)
        observed = self.machine.get_element(str(self.deck_id), str(self.element_id), applied_index=9)
        self.assertEqual(observed["lock"]["session_id"], str(self.sofia))
        self.assertEqual(observed["lock"]["fencing_token"], 1)

    def test_release_then_reacquire_fences_old_owner_and_delete_tombstones_identifier(self):
        released = self.machine.apply(self.command("release_lock", {
            "element_id": str(self.element_id), "fencing_token": 1,
        }, session_id=self.sofia))
        self.assertEqual(released.status, "RELEASED")
        acquired = self.machine.apply(self.command("acquire_lock", {
            "element_id": str(self.element_id),
        }, session_id=self.santiago))
        self.assertEqual(acquired.status, "LOCK_ACQUIRED")
        self.assertEqual(acquired.value["fencing_token"], 2)
        old_owner = self.machine.apply(self.command("release_lock", {
            "element_id": str(self.element_id), "fencing_token": 1,
        }, session_id=self.sofia))
        self.assertEqual(old_owner.status, "STALE_TOKEN")

        deleted = self.machine.apply(self.command("delete_element", {
            "element_id": str(self.element_id), "fencing_token": 2, "expected_version": 1,
        }, session_id=self.santiago))
        self.assertEqual(deleted.status, "DELETED")
        self.assertEqual(deleted.version, 2)
        self.assertEqual(self.machine.get_element(str(self.deck_id), str(self.element_id), applied_index=12)["element_tombstoned"], True)
        self.assertEqual(self.machine.apply(self.command("acquire_lock", {
            "element_id": str(self.element_id),
        }, session_id=self.sofia)).status, "NOT_FOUND")

if __name__ == "__main__":
    unittest.main()
