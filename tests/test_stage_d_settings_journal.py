"""Stage D — durable settings intentions: journal, FIFO, materialization,
supersession, conflict, reconciliation, and polling tests.

All tests use the existing public ClientIntentionOutbox / JournaledClient seam.
No internal helpers are accessed.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from client_journal import ClientIntentionOutbox, JournaledClient, Lifecycle


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _published_deck_args(deck_id: str, slide_id: str) -> dict:
    return {
        "presentation": {
            "schema_version": 2,
            "metadata": {
                "deck_id": deck_id,
                "name": "My Deck",
                "owner_session_id": str(uuid4()),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [
                {
                    "metadata": {"slide_id": slide_id, "background": "#112233FF"},
                    "elements": [],
                }
            ],
        }
    }


def _deck_settings_args(deck_id: str, expected_version: int = 0, name: str = "New Name") -> dict:
    return {
        "deck_id": deck_id,
        "expected_deck_settings_version": expected_version,
        "patch": {"name": name},
    }


def _slide_settings_args(deck_id: str, slide_id: str, expected_version: int = 0,
                          background: str = "#AABBCCFF") -> dict:
    return {
        "deck_id": deck_id,
        "slide_id": slide_id,
        "expected_slide_settings_version": expected_version,
        "patch": {"background": background},
    }


def _deck_settings_success(deck_settings_version: int) -> dict:
    return {"ok": True, "result": {
        "status": "DECK_SETTINGS_UPDATED",
        "value": {
            "deck_settings_version": deck_settings_version,
            "deck_version": 0,
            "name": "Updated Name",
            "width_px": 1280,
            "height_px": 720,
        },
    }}


def _deck_settings_conflict(current_version: int, min_commit_index: int = 10) -> dict:
    return {"ok": True, "result": {
        "status": "DECK_SETTINGS_VERSION_CONFLICT",
        "value": {
            "deck_settings_version": current_version,
            "deck_version": 0,
            "name": "Server Name",
            "width_px": 1280,
            "height_px": 720,
            "min_commit_index": min_commit_index,
        },
    }}


def _slide_settings_success(slide_settings_version: int) -> dict:
    return {"ok": True, "result": {
        "status": "SLIDE_SETTINGS_UPDATED",
        "value": {
            "slide_settings_version": slide_settings_version,
            "deck_version": 0,
            "background": "#AABBCCFF",
        },
    }}


def _slide_settings_conflict(current_version: int, min_commit_index: int = 15) -> dict:
    return {"ok": True, "result": {
        "status": "SLIDE_SETTINGS_VERSION_CONFLICT",
        "value": {
            "slide_settings_version": current_version,
            "deck_version": 0,
            "background": "#CCDDEE11",
            "min_commit_index": min_commit_index,
        },
    }}


class _BaseJournalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.journal_path = Path(self.workspace.name) / "intentions.jsonl"
        self.deck_id = str(uuid4())
        self.slide_id = str(uuid4())
        self.session_id = str(uuid4())

    def tearDown(self) -> None:
        self.workspace.cleanup()

    def outbox(self) -> ClientIntentionOutbox:
        return ClientIntentionOutbox("client-1", self.journal_path, self.session_id)

    def _establish_published_deck(self, outbox: ClientIntentionOutbox) -> None:
        """Transition outbox to a state where the deck is known-published."""
        outbox.enqueue("publish_deck", _published_deck_args(self.deck_id, self.slide_id),
                       request_id="pub-1")
        outbox.synchronize(lambda _: {"ok": True, "result": {"status": "PUBLISHED",
                                       "value": {"deck_id": self.deck_id}}})


# ---------------------------------------------------------------------------
# 1. Journal flush and replay
# ---------------------------------------------------------------------------

class TestSettingsJournalFlushAndReplay(_BaseJournalTest):
    """Settings intentions are flushed before first send and replayed after restart."""

    def test_deck_settings_flushed_before_send(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-flush-1")

        def check_flushed(intention):
            if intention.operation == "update_deck_settings":
                persisted = self.journal_path.read_text(encoding="utf-8")
                self.assertIn('"request_id":"ds-flush-1"', persisted)
            return _deck_settings_success(1)

        outbox.synchronize(check_flushed)

    def test_deck_settings_survives_restart(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-restart-1")

        # Simulate restart without sending
        outbox2 = self.outbox()
        replayed = outbox2.get("ds-restart-1")
        self.assertEqual(replayed.lifecycle, Lifecycle.PENDING_SYNC)
        self.assertEqual(replayed.operation, "update_deck_settings")

    def test_slide_settings_flushed_before_send(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-flush-1")

        def check_flushed(intention):
            if intention.operation == "update_slide_settings":
                persisted = self.journal_path.read_text(encoding="utf-8")
                self.assertIn('"request_id":"ss-flush-1"', persisted)
            return _slide_settings_success(1)

        outbox.synchronize(check_flushed)

    def test_slide_settings_survives_restart(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-restart-1")

        outbox2 = self.outbox()
        replayed = outbox2.get("ss-restart-1")
        self.assertEqual(replayed.lifecycle, Lifecycle.PENDING_SYNC)
        self.assertEqual(replayed.operation, "update_slide_settings")

    def test_in_flight_settings_intention_is_reset_to_pending_sync_after_restart(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-inflight-1")

        def lose_response(intention):
            if intention.operation == "update_deck_settings":
                return None  # uncertain — no response
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(lose_response)

        # After restart, uncertain in-flight should be PENDING_SYNC
        outbox2 = self.outbox()
        replayed = outbox2.get("ds-inflight-1")
        self.assertEqual(replayed.lifecycle, Lifecycle.PENDING_SYNC)


# ---------------------------------------------------------------------------
# 2. Deck Settings FIFO Lane
# ---------------------------------------------------------------------------

class TestDeckSettingsFIFOLane(_BaseJournalTest):
    """One Deck Settings FIFO lane per deck — later waits for predecessor."""

    def test_second_deck_settings_intention_depends_on_first(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        a = outbox.enqueue("update_deck_settings",
                           _deck_settings_args(self.deck_id, 0, "Name A"), request_id="ds-a")
        b = outbox.enqueue("update_deck_settings",
                           _deck_settings_args(self.deck_id, 0, "Name B"), request_id="ds-b")
        self.assertIn("ds-a", b.dependencies)

    def test_second_deck_settings_not_sent_while_first_is_pending(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name A"), request_id="ds-a")
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name B"), request_id="ds-b")
        sent: list[str] = []

        def track_sent(intention):
            sent.append(intention.operation + ":" + intention.request_id)
            if intention.operation == "update_deck_settings" and intention.request_id == "ds-a":
                return None  # lose response — stays PENDING
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(track_sent)
        # Only ds-a should have been attempted (and lost); ds-b must wait
        self.assertNotIn("update_deck_settings:ds-b", sent)

    def test_second_deck_settings_becomes_eligible_after_first_commits(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name A"), request_id="ds-a")
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name B"), request_id="ds-b")
        sent: list[str] = []

        def respond(intention):
            sent.append(intention.request_id)
            if intention.operation == "update_deck_settings":
                return _deck_settings_success(len([s for s in sent if "ds-" in s]))
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)
        outbox.synchronize(respond)

        self.assertIn("ds-b", sent)
        self.assertEqual(outbox.get("ds-a").lifecycle, Lifecycle.COMMITTED)
        self.assertEqual(outbox.get("ds-b").lifecycle, Lifecycle.COMMITTED)

    def test_deck_settings_version_materialized_from_committed_predecessor(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name A"), request_id="ds-a")
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Name B"), request_id="ds-b")

        materialized_versions: list[int] = []

        def respond(intention):
            if intention.operation == "update_deck_settings":
                ver = intention.arguments.get("expected_deck_settings_version", -99)
                materialized_versions.append(ver)
                return _deck_settings_success(ver + 1)
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)  # sends ds-a with v0 → committed at v1
        outbox.synchronize(respond)  # sends ds-b with materialized v1

        self.assertEqual(materialized_versions, [0, 1],
                         "ds-b must materialize expected_deck_settings_version=1 from ds-a's result")


# ---------------------------------------------------------------------------
# 3. Slide Settings FIFO Lane
# ---------------------------------------------------------------------------

class TestSlideSettingsFIFOLane(_BaseJournalTest):
    """One Slide Settings FIFO lane per slide."""

    def test_second_slide_settings_depends_on_first(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        a = outbox.enqueue("update_slide_settings",
                           _slide_settings_args(self.deck_id, self.slide_id, 0, "#AABBCCFF"),
                           request_id="ss-a")
        b = outbox.enqueue("update_slide_settings",
                           _slide_settings_args(self.deck_id, self.slide_id, 0, "#DDEEFFAA"),
                           request_id="ss-b")
        self.assertIn("ss-a", b.dependencies)

    def test_slide_settings_version_materialized_from_committed_predecessor(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id, 0, "#AABBCCFF"),
                       request_id="ss-a")
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id, 0, "#DDEEFFAA"),
                       request_id="ss-b")

        materialized_versions: list[int] = []

        def respond(intention):
            if intention.operation == "update_slide_settings":
                ver = intention.arguments.get("expected_slide_settings_version", -99)
                materialized_versions.append(ver)
                return _slide_settings_success(ver + 1)
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)
        outbox.synchronize(respond)

        self.assertEqual(materialized_versions, [0, 1],
                         "ss-b must materialize expected_slide_settings_version=1 from ss-a's result")

    def test_different_slides_in_same_deck_do_not_block_each_other(self) -> None:
        outbox = self.outbox()
        slide2_id = str(uuid4())
        # Build a deck with two slides in the base projection
        outbox.record_base_projection({
            "deck_id": self.deck_id,
            "applied_index": 1,
            "deck_version": 0,
            "slide_ids": [self.slide_id, slide2_id],
            "slides": {
                self.slide_id: {"metadata": {"slide_id": self.slide_id}, "elements": []},
                slide2_id: {"metadata": {"slide_id": slide2_id}, "elements": []},
            },
        })
        a = outbox.enqueue("update_slide_settings",
                           _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-slide1")
        b = outbox.enqueue("update_slide_settings",
                           _slide_settings_args(self.deck_id, slide2_id), request_id="ss-slide2")
        # Independent slides must not block each other
        self.assertNotIn("ss-slide1", b.dependencies)
        self.assertNotIn("ss-slide2", a.dependencies)

    def test_slide_settings_fifo_does_not_block_deck_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        slide_settings = outbox.enqueue(
            "update_slide_settings",
            _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-1")
        deck_settings = outbox.enqueue(
            "update_deck_settings",
            _deck_settings_args(self.deck_id), request_id="ds-1")
        # Deck settings must not depend on pending slide settings
        self.assertNotIn("ss-1", deck_settings.dependencies)


# ---------------------------------------------------------------------------
# 4. Cross-resource independence
# ---------------------------------------------------------------------------

class TestSettingsCrossResourceIndependence(_BaseJournalTest):
    """Different settings resources remain independently eligible."""

    def test_deck_and_slide_settings_sent_independently(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-independent")
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-independent")

        sent: list[str] = []

        def respond(intention):
            sent.append(intention.request_id)
            if intention.operation == "update_deck_settings":
                return _deck_settings_success(1)
            if intention.operation == "update_slide_settings":
                return _slide_settings_success(1)
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)
        # Both should be eligible and sent in one synchronize cycle
        self.assertIn("ds-independent", sent)
        self.assertIn("ss-independent", sent)


# ---------------------------------------------------------------------------
# 5. Prerequisites
# ---------------------------------------------------------------------------

class TestSettingsPrerequisites(_BaseJournalTest):
    """Deck settings wait for publish; slide settings wait for publish + create."""

    def test_deck_settings_wait_for_pending_publish(self) -> None:
        outbox = self.outbox()
        outbox.enqueue("publish_deck", _published_deck_args(self.deck_id, self.slide_id),
                       request_id="pub-pending")
        ds = outbox.enqueue("update_deck_settings",
                            _deck_settings_args(self.deck_id), request_id="ds-after-pub")
        # Must depend on pending publication
        self.assertIn("pub-pending", ds.dependencies)

    def test_deck_settings_eligible_after_publish_committed(self) -> None:
        outbox = self.outbox()
        outbox.enqueue("publish_deck", _published_deck_args(self.deck_id, self.slide_id),
                       request_id="pub-1")
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-1")
        sent: list[str] = []

        def respond(intention):
            sent.append(intention.request_id)
            if intention.operation == "publish_deck":
                return {"ok": True, "result": {"status": "PUBLISHED"}}
            if intention.operation == "update_deck_settings":
                return _deck_settings_success(1)
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)  # sends pub-1
        outbox.synchronize(respond)  # sends ds-1

        self.assertIn("pub-1", sent)
        self.assertIn("ds-1", sent)
        self.assertEqual(outbox.get("ds-1").lifecycle, Lifecycle.COMMITTED)

    def test_slide_settings_wait_for_pending_slide_creation(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        new_slide_id = str(uuid4())
        outbox.enqueue("create_slide", {
            "deck_id": self.deck_id,
            "expected_deck_version": 0,
            "slide": {"metadata": {"slide_id": new_slide_id, "background": "#FFFFFFFF"}, "elements": []},
        }, request_id="create-slide-1")
        ss = outbox.enqueue("update_slide_settings",
                            _slide_settings_args(self.deck_id, new_slide_id), request_id="ss-new-slide")
        # Must depend on pending slide creation
        self.assertIn("create-slide-1", ss.dependencies)

    def test_slide_settings_eligible_after_slide_creation_committed(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        new_slide_id = str(uuid4())
        outbox.enqueue("create_slide", {
            "deck_id": self.deck_id,
            "expected_deck_version": 0,
            "slide": {"metadata": {"slide_id": new_slide_id, "background": "#FFFFFFFF"}, "elements": []},
        }, request_id="create-slide-1")
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, new_slide_id), request_id="ss-new-slide")

        sent: list[str] = []

        def respond(intention):
            sent.append(intention.request_id)
            if intention.operation == "create_slide":
                return {"ok": True, "result": {"status": "CREATED", "value": {"deck_version": 1}}}
            if intention.operation == "update_slide_settings":
                return _slide_settings_success(1)
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(respond)  # sends create-slide-1
        outbox.synchronize(respond)  # sends ss-new-slide

        self.assertIn("ss-new-slide", sent)
        self.assertEqual(outbox.get("ss-new-slide").lifecycle, Lifecycle.COMMITTED)


# ---------------------------------------------------------------------------
# 6. Slide deletion supersedes unsent settings
# ---------------------------------------------------------------------------

class TestSlideSettingsSupersession(_BaseJournalTest):
    """A pending slide deletion supersedes only unsent settings for that slide."""

    def test_pending_delete_supersedes_unsent_slide_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        ss = outbox.enqueue("update_slide_settings",
                            _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-1")
        outbox.enqueue("delete_slide", {
            "deck_id": self.deck_id,
            "slide_id": self.slide_id,
            "expected_deck_version": 0,
        }, request_id="del-1")
        self.assertEqual(outbox.get("ss-1").lifecycle, Lifecycle.SUPERSEDED)

    def test_pending_delete_does_not_supersede_in_flight_slide_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-inflight")

        def lose_response(intention):
            if intention.operation == "update_slide_settings":
                # Queue delete while ss is IN_FLIGHT
                outbox.enqueue("delete_slide", {
                    "deck_id": self.deck_id,
                    "slide_id": self.slide_id,
                    "expected_deck_version": 0,
                }, request_id="del-later")
                # Confirm ss is in-flight (not superseded)
                self.assertEqual(outbox.get("ss-inflight").lifecycle, Lifecycle.IN_FLIGHT)
                return None  # uncertain
            return {"ok": True, "result": {"status": "PUBLISHED"}}

        outbox.synchronize(lose_response)
        # After uncertain response, ss-inflight returns to PENDING_SYNC (not SUPERSEDED)
        self.assertEqual(outbox.get("ss-inflight").lifecycle, Lifecycle.PENDING_SYNC)

    def test_delete_does_not_supersede_settings_for_different_slide(self) -> None:
        outbox = self.outbox()
        slide2_id = str(uuid4())
        outbox.record_base_projection({
            "deck_id": self.deck_id,
            "applied_index": 1,
            "deck_version": 0,
            "slide_ids": [self.slide_id, slide2_id],
            "slides": {
                self.slide_id: {"metadata": {"slide_id": self.slide_id}, "elements": []},
                slide2_id: {"metadata": {"slide_id": slide2_id}, "elements": []},
            },
        })
        ss2 = outbox.enqueue("update_slide_settings",
                             _slide_settings_args(self.deck_id, slide2_id), request_id="ss-slide2")
        outbox.enqueue("delete_slide", {
            "deck_id": self.deck_id,
            "slide_id": self.slide_id,  # delete slide1, not slide2
            "expected_deck_version": 0,
        }, request_id="del-slide1")
        # slide2 settings must NOT be superseded
        self.assertEqual(outbox.get("ss-slide2").lifecycle, Lifecycle.PENDING_SYNC)


# ---------------------------------------------------------------------------
# 7. Conflict → NEEDS_MANUAL_RECONCILIATION
# ---------------------------------------------------------------------------

class TestSettingsConflict(_BaseJournalTest):
    """DECK/SLIDE_SETTINGS_VERSION_CONFLICT moves intention to NMR."""

    def test_deck_settings_conflict_moves_to_nmr(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-conflict")
        outbox.synchronize(lambda i: _deck_settings_conflict(2) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        self.assertEqual(outbox.get("ds-conflict").lifecycle, Lifecycle.NEEDS_MANUAL_RECONCILIATION)

    def test_deck_settings_conflict_stores_server_snapshot(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-conflict-snap")
        outbox.synchronize(lambda i: _deck_settings_conflict(5) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        result = outbox.get("ds-conflict-snap").result
        self.assertIsNotNone(result)
        self.assertEqual(result.get("value", {}).get("deck_settings_version"), 5)

    def test_slide_settings_conflict_moves_to_nmr(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-conflict")
        outbox.synchronize(lambda i: _slide_settings_conflict(3) if i.operation == "update_slide_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        self.assertEqual(outbox.get("ss-conflict").lifecycle, Lifecycle.NEEDS_MANUAL_RECONCILIATION)

    def test_slide_settings_conflict_stores_server_snapshot(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-conflict-snap")
        outbox.synchronize(lambda i: _slide_settings_conflict(7) if i.operation == "update_slide_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        result = outbox.get("ss-conflict-snap").result
        self.assertIsNotNone(result)
        self.assertEqual(result.get("value", {}).get("slide_settings_version"), 7)


# ---------------------------------------------------------------------------
# 8. Discard path
# ---------------------------------------------------------------------------

class TestSettingsDiscard(_BaseJournalTest):
    """Discard terminates a settings draft and survives replay."""

    def test_discard_pending_deck_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-discard")
        discarded = outbox.discard("ds-discard")
        self.assertEqual(discarded.lifecycle, Lifecycle.SUPERSEDED)
        self.assertEqual(discarded.reason, "user_discarded")

    def test_discard_nmr_deck_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-nmr-discard")
        outbox.synchronize(lambda i: _deck_settings_conflict(2) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        discarded = outbox.discard("ds-nmr-discard")
        self.assertEqual(discarded.lifecycle, Lifecycle.SUPERSEDED)

    def test_discard_nmr_deck_settings_survives_replay(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-replay-discard")
        outbox.synchronize(lambda i: _deck_settings_conflict(2) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        outbox.discard("ds-replay-discard")

        outbox2 = self.outbox()
        replayed = outbox2.get("ds-replay-discard")
        self.assertEqual(replayed.lifecycle, Lifecycle.SUPERSEDED)

    def test_discard_slide_settings(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id), request_id="ss-discard")
        discarded = outbox.discard("ss-discard")
        self.assertEqual(discarded.lifecycle, Lifecycle.SUPERSEDED)


# ---------------------------------------------------------------------------
# 9. Corrected resubmission path
# ---------------------------------------------------------------------------

class TestSettingsCorrectedResubmission(_BaseJournalTest):
    """submit_reconciled_settings creates a new intention with returned revision."""

    def test_deck_settings_corrected_resubmission_uses_conflict_version(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id, 0, "Old Name"), request_id="ds-original")
        outbox.synchronize(lambda i: _deck_settings_conflict(3) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})

        corrected = outbox.submit_reconciled_settings(
            "ds-original",
            {"name": "Corrected Name"},
            request_id="ds-corrected",
        )
        self.assertEqual(corrected.arguments["expected_deck_settings_version"], 3)
        self.assertEqual(corrected.arguments["patch"], {"name": "Corrected Name"})
        self.assertEqual(corrected.lifecycle, Lifecycle.PENDING_SYNC)

    def test_original_deck_settings_remains_as_audit_history(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-original-audit")
        outbox.synchronize(lambda i: _deck_settings_conflict(3) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        outbox.submit_reconciled_settings(
            "ds-original-audit",
            {"name": "Fixed"},
            request_id="ds-fixed",
        )
        # Original stays in NMR as audit history
        self.assertEqual(outbox.get("ds-original-audit").lifecycle, Lifecycle.NEEDS_MANUAL_RECONCILIATION)

    def test_slide_settings_corrected_resubmission_uses_conflict_version(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_slide_settings",
                       _slide_settings_args(self.deck_id, self.slide_id, 0, "#AABBCCFF"),
                       request_id="ss-original")
        outbox.synchronize(lambda i: _slide_settings_conflict(5) if i.operation == "update_slide_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})

        corrected = outbox.submit_reconciled_settings(
            "ss-original",
            {"background": "#FFEEDDCC"},
            request_id="ss-corrected",
        )
        self.assertEqual(corrected.arguments["expected_slide_settings_version"], 5)
        self.assertEqual(corrected.arguments["patch"], {"background": "#FFEEDDCC"})
        self.assertEqual(corrected.lifecycle, Lifecycle.PENDING_SYNC)

    def test_corrected_deck_settings_does_not_depend_on_nmr_original(self) -> None:
        """Corrected resubmission must not wait for the conflicted original."""
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-orig")
        outbox.synchronize(lambda i: _deck_settings_conflict(3) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})

        corrected = outbox.submit_reconciled_settings(
            "ds-orig", {"name": "Corrected"}, request_id="ds-corrected-nodep",
        )
        # Corrected intention must not depend on the conflicted original
        self.assertNotIn("ds-orig", corrected.dependencies)

    def test_resubmission_requires_new_request_id(self) -> None:
        from client_journal import LocalIntentionRejected
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-same-id")
        outbox.synchronize(lambda i: _deck_settings_conflict(3) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        with self.assertRaises(LocalIntentionRejected):
            outbox.submit_reconciled_settings(
                "ds-same-id", {"name": "X"}, request_id="ds-same-id",
            )

    def test_resubmission_requires_nmr_state(self) -> None:
        from client_journal import LocalIntentionRejected
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-not-nmr")
        # Still PENDING_SYNC — cannot resubmit
        with self.assertRaises(LocalIntentionRejected):
            outbox.submit_reconciled_settings(
                "ds-not-nmr", {"name": "X"}, request_id="ds-resubmit-fail",
            )

    def test_corrected_resubmission_survives_restart(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        outbox.enqueue("update_deck_settings",
                       _deck_settings_args(self.deck_id), request_id="ds-restart-orig")
        outbox.synchronize(lambda i: _deck_settings_conflict(3) if i.operation == "update_deck_settings"
                           else {"ok": True, "result": {"status": "PUBLISHED"}})
        outbox.submit_reconciled_settings(
            "ds-restart-orig", {"name": "Restarted"}, request_id="ds-restart-corr",
        )

        outbox2 = self.outbox()
        replayed = outbox2.get("ds-restart-corr")
        self.assertEqual(replayed.lifecycle, Lifecycle.PENDING_SYNC)
        self.assertEqual(replayed.arguments["expected_deck_settings_version"], 3)


# ---------------------------------------------------------------------------
# 10. Polling and applied_index monotonicity
# ---------------------------------------------------------------------------

class TestSettingsPolling(_BaseJournalTest):
    """Polling notices settings-only commits via monotonic applied_index."""

    def test_poll_stores_deck_settings_version_in_base_projection(self) -> None:
        outbox = self.outbox()

        def sender(operation, args, request_id):
            if operation == "get_deck_preview":
                return {"ok": True, "result": {
                    "status": "OK",
                    "applied_index": 5,
                    "deck_id": self.deck_id,
                    "deck_version": 0,
                    "deck_settings_version": 2,
                    "slide_ids": [],
                    "metadata": {"name": "X", "width_px": 1280, "height_px": 720},
                }}
            return {"ok": True, "result": {"status": "OK", "applied_index": 5, "deck_id": self.deck_id,
                                           "deck_version": 0, "slide": {"metadata": {}, "elements": []}}}

        result = outbox.poll(self.deck_id, sender)
        self.assertTrue(result)
        self.assertEqual(outbox.base_projection.get("deck_settings_version"), 2)

    def test_poll_refreshes_slides_when_applied_index_advances_without_deck_version_change(self) -> None:
        outbox = self.outbox()
        # First poll at applied_index=5
        poll_index = [5]

        def sender(operation, args, request_id):
            if operation == "get_deck_preview":
                return {"ok": True, "result": {
                    "status": "OK",
                    "applied_index": poll_index[0],
                    "deck_id": self.deck_id,
                    "deck_version": 0,
                    "deck_settings_version": poll_index[0] - 4,  # increases with settings commits
                    "slide_ids": [self.slide_id],
                    "metadata": {},
                }}
            if operation == "get_slide":
                return {"ok": True, "result": {
                    "status": "OK",
                    "applied_index": poll_index[0],
                    "deck_id": self.deck_id,
                    "deck_version": 0,
                    "slide": {
                        "metadata": {
                            "slide_id": self.slide_id,
                            "slide_settings_version": poll_index[0] - 4,
                            "background": "#AABBCCFF",
                        },
                        "elements": [],
                    },
                }}
            return {"ok": True, "result": {"status": "OK"}}

        outbox.poll(self.deck_id, sender)
        self.assertEqual(outbox.highest_applied_index, 5)

        # Second poll: applied_index=6 (settings-only commit, deck_version unchanged)
        poll_index[0] = 6
        result = outbox.poll(self.deck_id, sender)
        self.assertTrue(result)
        self.assertEqual(outbox.highest_applied_index, 6)
        self.assertEqual(outbox.base_projection.get("deck_settings_version"), 2)

    def test_poll_rejects_stale_response_with_smaller_applied_index(self) -> None:
        outbox = self.outbox()
        outbox.record_base_projection({
            "deck_id": self.deck_id,
            "applied_index": 10,
            "deck_version": 0,
            "slide_ids": [],
            "slides": {},
        })

        stale_calls = 0

        def sender(operation, args, request_id):
            nonlocal stale_calls
            if operation == "get_deck_preview":
                stale_calls += 1
                return {"ok": True, "result": {
                    "status": "OK",
                    "applied_index": 9,  # stale — smaller than current 10
                    "deck_id": self.deck_id,
                    "deck_version": 0,
                    "slide_ids": [],
                }}
            return {"ok": True, "result": {"status": "OK"}}

        result = outbox.poll(self.deck_id, sender)
        self.assertFalse(result, "stale applied_index must be rejected")
        self.assertEqual(outbox.highest_applied_index, 10, "base must not be overwritten by stale response")


# ---------------------------------------------------------------------------
# 11. No coalescing / independence of patches
# ---------------------------------------------------------------------------

class TestSettingsNeverCoalesced(_BaseJournalTest):
    """Settings patches are never automatically merged or coalesced."""

    def test_two_deck_settings_intentions_remain_distinct(self) -> None:
        outbox = self.outbox()
        self._establish_published_deck(outbox)
        a = outbox.enqueue("update_deck_settings",
                           _deck_settings_args(self.deck_id, 0, "Name A"), request_id="ds-coalesce-a")
        b = outbox.enqueue("update_deck_settings",
                           _deck_settings_args(self.deck_id, 0, "Name B"), request_id="ds-coalesce-b")
        # Both must exist and be separate intentions
        self.assertIn("ds-coalesce-a", [i for i in [a.request_id]])
        self.assertIn("ds-coalesce-b", [i for i in [b.request_id]])
        self.assertEqual(a.arguments["patch"]["name"], "Name A")
        self.assertEqual(b.arguments["patch"]["name"], "Name B")


# ---------------------------------------------------------------------------
# 12. JournaledClient surface area
# ---------------------------------------------------------------------------

class TestJournaledClientSettingsMethods(_BaseJournalTest):
    """JournaledClient exposes submit_reconciled_settings."""

    def _make_client(self) -> JournaledClient:
        def request_sender(operation, arguments, request_id, node):
            if operation == "publish_deck":
                return {"ok": True, "result": {"status": "PUBLISHED"}}
            if operation == "update_deck_settings":
                return _deck_settings_conflict(5)
            if operation == "get_deck_preview":
                return {"ok": True, "result": {
                    "status": "OK", "applied_index": 1,
                    "deck_id": self.deck_id, "deck_version": 0, "slide_ids": [],
                }}
            return None

        return JournaledClient(
            "client-1", self.journal_path, self.session_id,
            request_sender=request_sender,
            local_replica="n2",
            leader_replica="n1",
        )

    def test_journaled_client_has_submit_reconciled_settings(self) -> None:
        client = self._make_client()
        client.enqueue("publish_deck", _published_deck_args(self.deck_id, self.slide_id))
        client.synchronize()
        client.enqueue("update_deck_settings", _deck_settings_args(self.deck_id), request_id="ds-jc-1")
        client.synchronize()
        self.assertEqual(client.outbox.get("ds-jc-1").lifecycle, Lifecycle.NEEDS_MANUAL_RECONCILIATION)

        corrected = client.submit_reconciled_settings(
            "ds-jc-1", {"name": "Corrected"}, replacement_request_id="ds-jc-corrected",
        )
        self.assertEqual(corrected.arguments["expected_deck_settings_version"], 5)


if __name__ == "__main__":
    unittest.main()
