"""Public contracts for the durable, client-local Stage B intention journal."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from client import Client, JournaledClient
from client_journal import ClientIntentionOutbox, Lifecycle


def published_deck(deck_id: str, slide_id: str) -> dict[str, object]:
    return {
        "presentation": {
            "schema_version": 1,
            "metadata": {
                "deck_id": deck_id,
                "name": "Offline deck",
                "owner_session_id": str(uuid4()),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [
                {
                    "metadata": {"slide_id": slide_id, "background": "#112233", "ordinal": 0},
                    "elements": [],
                }
            ],
        }
    }


class ClientJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.journal_path = Path(self.workspace.name) / "client-intentions.jsonl"
        self.deck_id, self.slide_id, self.element_id = (str(uuid4()) for _ in range(3))
        self.session_id = str(uuid4())

    def tearDown(self) -> None:
        self.workspace.cleanup()

    def outbox(self) -> ClientIntentionOutbox:
        return ClientIntentionOutbox("logical-client-1", self.journal_path, self.session_id)

    def test_flushes_before_send_and_replays_the_original_request_identity_after_restart(self) -> None:
        outbox = self.outbox()
        publication = outbox.enqueue(
            "publish_deck", published_deck(self.deck_id, self.slide_id), request_id="publish-original",
        )

        def unavailable(intention):
            persisted = self.journal_path.read_text(encoding="utf-8")
            self.assertIn('"request_id":"publish-original"', persisted)
            self.assertEqual(intention.request_id, publication.request_id)
            return {"ok": False, "error": "TimeoutError"}

        outbox.synchronize(unavailable)
        self.assertEqual(outbox.get(publication.request_id).lifecycle, Lifecycle.PENDING_SYNC)

        restarted = self.outbox()
        replayed = restarted.get(publication.request_id)
        self.assertEqual(replayed.client_id, "logical-client-1")
        self.assertEqual(replayed.request_id, "publish-original")
        self.assertEqual(replayed.lifecycle, Lifecycle.PENDING_SYNC)

        sent: list[str] = []

        def committed(intention):
            sent.append(intention.request_id)
            return {"ok": True, "result": {"status": "PUBLISHED", "value": {"deck_id": self.deck_id}}}

        restarted.synchronize(committed)
        self.assertEqual(sent, ["publish-original"])
        self.assertEqual(restarted.get("publish-original").lifecycle, Lifecycle.COMMITTED)

    def test_pending_slide_deletion_supersedes_only_unsent_descendants(self) -> None:
        outbox = self.outbox()
        outbox.enqueue("publish_deck", published_deck(self.deck_id, self.slide_id), request_id="publish")
        outbox.synchronize(lambda _: {"ok": True, "result": {"status": "PUBLISHED"}})

        created = outbox.enqueue(
            "create_element",
            {
                "deck_id": self.deck_id,
                "slide_id": self.slide_id,
                "element": {"element_id": self.element_id, "kind": "text_box", "configuration": {}},
            },
            request_id="create-element",
        )
        update = outbox.enqueue(
            "update_element",
            {"deck_id": self.deck_id, "element_id": self.element_id, "fencing_token": 1,
             "expected_version": 1, "patch": {"content": "draft"}},
            request_id="update-element",
        )
        deletion = outbox.enqueue(
            "delete_slide",
            {"deck_id": self.deck_id, "slide_id": self.slide_id, "expected_deck_version": 0},
            request_id="delete-slide",
        )

        self.assertEqual(outbox.get(created.request_id).lifecycle, Lifecycle.SUPERSEDED)
        self.assertEqual(outbox.get(update.request_id).lifecycle, Lifecycle.SUPERSEDED)
        self.assertEqual(outbox.get(deletion.request_id).lifecycle, Lifecycle.PENDING_SYNC)
        self.assertEqual(outbox.speculative_projection["pending_slide_deletions"], [self.slide_id])

    def test_pending_deletion_does_not_presume_an_in_flight_descendant_cancelled(self) -> None:
        outbox = self.outbox()
        outbox.enqueue("publish_deck", published_deck(self.deck_id, self.slide_id), request_id="publish")
        outbox.synchronize(lambda _: {"ok": True, "result": {"status": "PUBLISHED"}})
        created = outbox.enqueue(
            "create_element",
            {"deck_id": self.deck_id, "slide_id": self.slide_id,
             "element": {"element_id": self.element_id, "kind": "text_box", "configuration": {}}},
            request_id="in-flight-create",
        )

        def loses_response(intention):
            self.assertEqual(intention.request_id, "in-flight-create")
            deletion = outbox.enqueue(
                "delete_slide",
                {"deck_id": self.deck_id, "slide_id": self.slide_id, "expected_deck_version": 0},
                request_id="later-delete",
            )
            self.assertEqual(outbox.get(created.request_id).lifecycle, Lifecycle.IN_FLIGHT)
            self.assertEqual(outbox.get(deletion.request_id).lifecycle, Lifecycle.PENDING_SYNC)
            return {"ok": False, "error": "lost response"}

        outbox.synchronize(loses_response)
        self.assertEqual(outbox.get(created.request_id).lifecycle, Lifecycle.PENDING_SYNC)

    def test_offline_element_update_waits_for_and_uses_the_authoritative_creation_token(self) -> None:
        outbox = self.outbox()
        outbox.enqueue("publish_deck", published_deck(self.deck_id, self.slide_id), request_id="publish")
        outbox.synchronize(lambda _: {"ok": True, "result": {"status": "PUBLISHED"}})
        outbox.enqueue(
            "create_element",
            {"deck_id": self.deck_id, "slide_id": self.slide_id,
             "element": {"element_id": self.element_id, "kind": "text_box", "configuration": {}}},
            request_id="offline-create",
        )
        update = outbox.enqueue(
            "update_element",
            {"deck_id": self.deck_id, "element_id": self.element_id, "patch": {"content": "later draft"}},
            request_id="after-create",
        )

        def sender(intention):
            if intention.operation == "create_element":
                return {"ok": True, "result": {"status": "CREATED_AND_LOCKED", "value": {
                    "element_version": 1, "fencing_token": 7,
                }}}
            self.assertEqual(intention.operation, "update_element")
            self.assertEqual(intention.arguments["fencing_token"], 7)
            self.assertEqual(intention.arguments["expected_version"], 1)
            return {"ok": True, "result": {"status": "SAVED"}}

        outbox.synchronize(sender)
        self.assertEqual(outbox.get(update.request_id).lifecycle, Lifecycle.COMMITTED)

    def test_polling_is_monotonic_and_never_merges_or_replaces_a_pending_draft(self) -> None:
        outbox = self.outbox()
        observations = iter([
            {"ok": True, "result": {"status": "OK", "applied_index": 10, "deck_id": self.deck_id,
                                     "deck_version": 0, "slide_ids": [self.slide_id]}},
            {"ok": True, "result": {"status": "OK", "applied_index": 10, "deck_id": self.deck_id,
                                     "deck_version": 0, "slide": {"metadata": {"slide_id": self.slide_id}, "elements": []}}},
            {"ok": True, "result": {"status": "OK", "applied_index": 9, "deck_id": self.deck_id,
                                     "deck_version": 0, "slide_ids": []}},
        ])

        def poll_sender(operation, args, request_id):
            return next(observations)

        self.assertTrue(outbox.poll(self.deck_id, poll_sender))
        pending = outbox.enqueue(
            "create_element",
            {"deck_id": self.deck_id, "slide_id": self.slide_id,
             "element": {"element_id": self.element_id, "kind": "text_box", "configuration": {}}},
            request_id="offline-create",
        )
        self.assertFalse(outbox.poll(self.deck_id, poll_sender))
        self.assertEqual(outbox.highest_applied_index, 10)
        self.assertEqual(outbox.base_projection["slide_ids"], [self.slide_id])
        self.assertEqual(outbox.get(pending.request_id).lifecycle, Lifecycle.PENDING_SYNC)
        self.assertEqual(outbox.speculative_projection["pending_element_creations"][0]["element_id"], self.element_id)

    def test_conflict_requires_explicit_reconciliation_with_fresh_lock_and_new_request(self) -> None:
        outbox = self.outbox()
        outbox.record_base_projection({
            "deck_id": self.deck_id,
            "applied_index": 5,
            "deck_version": 0,
            "slide_ids": [self.slide_id],
            "slides": {self.slide_id: {"metadata": {"slide_id": self.slide_id}, "elements": [{
                "element_id": self.element_id, "version": 3, "configuration": {"content": "base"},
            }]}},
        })
        original = outbox.enqueue(
            "update_element",
            {"deck_id": self.deck_id, "element_id": self.element_id, "fencing_token": 2,
             "expected_version": 3, "patch": {"content": "local draft"}},
            request_id="stale-update",
        )
        sent: list[tuple[str, str]] = []

        def sender(intention):
            sent.append((intention.operation, intention.request_id))
            return {"ok": True, "result": {"status": "VERSION_CONFLICT", "value": {
                "current_version": 4, "min_commit_index": 12,
            }}}

        outbox.synchronize(sender)
        self.assertEqual(outbox.get(original.request_id).lifecycle, Lifecycle.NEEDS_MANUAL_RECONCILIATION)
        self.assertEqual(sent, [("update_element", "stale-update")])

        reconciliation_calls: list[tuple[str, dict[str, object], str]] = []

        def reconciliation_sender(operation, args, request_id):
            reconciliation_calls.append((operation, args, request_id))
            if operation == "acquire_lock":
                return {"ok": True, "result": {"status": "LOCK_ACQUIRED", "value": {"fencing_token": 9}}}
            self.assertEqual(operation, "authoritative_refresh_element")
            self.assertEqual(args["min_commit_index"], 12)
            return {"ok": True, "result": {"status": "OK", "applied_index": 15,
                "element_version": 4, "element": {"element_id": self.element_id,
                "version": 4, "configuration": {"content": "remote"}}}}

        context = outbox.prepare_reconciliation(original.request_id, reconciliation_sender)
        self.assertEqual(context["fencing_token"], 9)
        self.assertEqual([call[0] for call in reconciliation_calls], ["acquire_lock", "authoritative_refresh_element"])
        self.assertEqual(len(sent), 1, "preparing reconciliation must not automatically merge or save")

        corrected = outbox.submit_reconciled_update(
            original.request_id, {"content": "user correction"}, request_id="corrected-update",
        )
        self.assertEqual(corrected.request_id, "corrected-update")
        self.assertEqual(corrected.arguments["fencing_token"], 9)
        self.assertEqual(corrected.arguments["expected_version"], 4)

    def test_journaled_client_uses_the_existing_http_client_path_with_the_durable_request_id(self) -> None:
        run_dir = Path(self.workspace.name) / "run"
        run_dir.mkdir()
        (run_dir / "cluster.json").write_text(
            json.dumps({"host_nodes": {"n1": "http://n1:8000"}}), encoding="utf-8",
        )
        client = Client("logical-client-1", Path(self.workspace.name) / "history.jsonl", run_dir=run_dir)
        journaled = JournaledClient(client, self.journal_path, self.session_id)
        journaled.enqueue(
            "publish_deck", published_deck(self.deck_id, self.slide_id), request_id="http-request-id",
        )

        with patch("client._post", return_value={"ok": True, "result": {"status": "PUBLISHED"}}) as post:
            journaled.synchronize()

        payload = post.call_args.args[1]
        self.assertEqual(payload["id"], "http-request-id")
        self.assertEqual(payload["client"], "logical-client-1")
        self.assertEqual(payload["op"], "publish_deck")


if __name__ == "__main__":
    unittest.main()
