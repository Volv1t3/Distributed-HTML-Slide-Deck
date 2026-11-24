"""Externally visible Stage A.1 adapter behavior through starter callbacks."""

from __future__ import annotations

import json
import os
import queue
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from app import App
from cluster import RUN, keep_histories, write_compose
from node import Request


class RuntimeIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replicas: list[App] = []

    def tearDown(self) -> None:
        for replica in self.replicas:
            replica._events.close()
            replica._log.close()
        self.workspace.cleanup()

    def app(self, *, mode: str = "OPERATIONAL") -> App:
        with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": mode}, clear=False):
            replica = App("n1", self.nodes, self.run_dir)
            replica.start()
        self.replicas.append(replica)
        return replica

    def messages(self, replica: App) -> list[tuple[str, dict[str, object]]]:
        sent: list[tuple[str, dict[str, object]]] = []
        while True:
            try:
                sent.append(replica._out.get_nowait())
            except queue.Empty:
                return sent

    def register(self, replica: App, slot: str) -> None:
        replica.on_message(
            slot,
            {
                "kind": "register_node",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": slot,
                "sender_uuid": str(uuid4()),
                "term": 0,
                "incarnation": 1,
                "membership_fingerprint": replica.engine.membership_fingerprint(),
            },
        )

    def elect_leader(self, replica: App) -> None:
        self.messages(replica)
        self.register(replica, "n2")
        self.register(replica, "n3")
        self.messages(replica)
        replica.engine.start_election()
        vote = next(
            message
            for destination, message in self.messages(replica)
            if destination == "n2" and message["kind"] == "request_vote"
        )
        replica.on_message(
            "n2",
            {
                "kind": "request_vote_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1,
                "reply_to": vote["rpc_id"],
                "vote_granted": True,
            },
        )
        self.assertEqual(replica.status()["role"], "leader")
        self.messages(replica)

    def commit_request(self, replica: App, request: Request) -> dict[str, object]:
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        self.assertIsNone(replica.on_request(request))
        append = next(
            message
            for destination, message in self.messages(replica)
            if destination == "n2"
            and message["kind"] == "append_entries"
            and any(entry["command"]["request_id"] == request.id for entry in message["entries"])
        )
        replica.on_message(
            "n2",
            {
                "kind": "append_entries_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1,
                "reply_to": append["rpc_id"],
                "success": True,
                "match_index": append["entries"][-1]["index"],
            },
        )
        self.assertTrue(done.is_set())
        return response

    def acknowledge_read_index(self, replica: App, *, peer: str = "n2") -> dict[str, object]:
        """Deliver an authentic correlated quorum heartbeat reply for a pending read."""
        probes = [message for destination, message in self.messages(replica)
                  if destination == peer and message["kind"] == "append_entries"
                  and message.get("read_context") is not None]
        self.assertTrue(probes, "a ReadIndex quorum probe must be sent")
        probe = probes[-1]
        replica.on_message(peer, {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": peer,
            "sender_uuid": str(replica.engine.state.peer_bindings[peer]),
            "term": probe["term"], "reply_to": probe["rpc_id"],
            "success": True, "match_index": probe["prev_log_index"],
            "read_context": probe["read_context"],
        })
        return probe

    def test_adapter_persists_core_state_registers_slots_and_keeps_operational_logs_redacted(self) -> None:
        replica = self.app()

        self.assertIn("raft_state", replica.disk)
        self.assertEqual(replica.status()["node_uuid"], replica.disk["raft_state"]["node_uuid"])
        registrations = self.messages(replica)
        self.assertEqual({destination for destination, _ in registrations}, {"n2", "n3"})
        self.assertTrue(all(message["kind"] == "register_node" for _, message in registrations))

        records = [json.loads(line) for line in (self.run_dir / "n1" / "events.log").read_text().splitlines()]
        self.assertEqual(records[0]["event"], "startup")
        self.assertEqual(records[0]["mode"], "OPERATIONAL")
        self.assertNotIn("payload", records[0])

        restarted = self.app()
        self.assertEqual(restarted.status()["node_uuid"], replica.status()["node_uuid"])
        self.assertEqual(restarted.engine.state.incarnation, replica.engine.state.incarnation + 1)

    def test_adapter_retries_registration_for_unbound_peers_on_tick(self) -> None:
        replica = self.app()
        self.messages(replica)
        replica._next_registration = 0

        replica.on_tick()

        retries = [message for _, message in self.messages(replica) if message["kind"] == "register_node"]
        self.assertEqual({message["sender_slot"] for message in retries}, {"n1"})
        self.assertEqual(len(retries), 2)

    def test_adapter_answers_an_initial_registration_with_its_own_handshake(self) -> None:
        replica = self.app()
        self.messages(replica)

        self.register(replica, "n2")

        registrations = [message for destination, message in self.messages(replica) if destination == "n2"]
        self.assertTrue(any(message["kind"] == "register_node" for message in registrations))

    def test_starter_request_routes_preserve_redirects_local_reads_and_reply_only_after_commit(self) -> None:
        replica = self.app()
        self.messages(replica)
        self.register(replica, "n2")
        self.register(replica, "n3")

        replica.engine.leader_slot = "n2"
        self.assertEqual(
            replica.on_request(Request("follow-1", "put", {"key": "deck", "value": "v1"}, "client")),
            {"redirect": "n2"},
        )
        self.assertEqual(
            replica.on_request(Request("local-1", "get", {"key": "deck", "local": True}, "client")),
            {"ok": True, "result": None},
        )

        replica.engine.start_election()
        vote = next(
            message
            for destination, message in self.messages(replica)
            if destination == "n2" and message["kind"] == "request_vote"
        )
        replica.on_message(
            "n2",
            {
                "kind": "request_vote_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1,
                "reply_to": vote["rpc_id"],
                "vote_granted": True,
            },
        )
        self.assertEqual(replica.status()["role"], "leader")
        self.messages(replica)  # The current-term no-op is not this client request.
        request = Request("write-1", "put", {"key": "deck", "value": "v1"}, "client")
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        self.assertIsNone(replica.on_request(request))

        append = next(
            message
            for destination, message in self.messages(replica)
            if destination == "n2" and message["kind"] == "append_entries"
        )
        replica.on_message(
            "n2",
            {
                "kind": "append_entries_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1,
                "reply_to": append["rpc_id"],
                "success": True,
                "match_index": append["prev_log_index"] + len(append["entries"]),
            },
        )

        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": True, "result": "ok"})
        self.assertEqual(replica.disk["raft_state"]["fsm"], {"kv": {"deck": "v1"}})

        deck_id, slide_id, owner_session_id = uuid4(), uuid4(), uuid4()
        publish = Request(
            "publish-1",
            "publish_deck",
            {
                "presentation": {
                    "schema_version": 2,
                    "metadata": {
                        "deck_id": str(deck_id),
                        "name": "Published through Raft",
                        "owner_session_id": str(owner_session_id),
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
            "client",
        )
        published, publish_response = threading.Event(), {}
        replica._waiting[publish.id] = (published, publish_response)
        self.assertIsNone(replica.on_request(publish))
        publish_append = next(
            message
            for destination, message in self.messages(replica)
            if destination == "n2"
            and message["kind"] == "append_entries"
            and any(entry["command"]["operation"] == "publish_deck" for entry in message["entries"])
        )
        replica.on_message(
            "n2",
            {
                "kind": "append_entries_response",
                "protocol_version": 1,
                "rpc_id": str(uuid4()),
                "sender_slot": "n2",
                "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
                "term": 1,
                "reply_to": publish_append["rpc_id"],
                "success": True,
                "match_index": publish_append["entries"][-1]["index"],
            },
        )

        self.assertTrue(published.is_set())
        self.assertEqual(publish_response["result"]["status"], "PUBLISHED")
        self.assertEqual(publish_response["result"]["value"]["deck_id"], str(deck_id))
        self.assertIn(str(deck_id), replica.disk["raft_state"]["fsm"]["decks"])
        self.assertEqual(replica.on_request(publish), publish_response)
        self.assertEqual(
            sum(entry.command.operation == "publish_deck" for entry in replica.engine.state.raft.log),
            1,
        )

    def test_structural_slide_request_traverses_adapter_and_raft_before_mutating_deck(self) -> None:
        replica = self.app()
        self.elect_leader(replica)
        deck_id, original_slide_id, created_slide_id = uuid4(), uuid4(), uuid4()
        publish = Request(
            "publish-structural-path",
            "publish_deck",
            {
                "presentation": {
                    "schema_version": 2,
                    "metadata": {
                        "deck_id": str(deck_id),
                        "name": "Adapter structural deck",
                        "owner_session_id": str(uuid4()),
                        "width_px": 1280,
                        "height_px": 720,
                    },
                    "slides": [
                        {
                            "metadata": {
                                "slide_id": str(original_slide_id),
                                "background": "#112233FF",
                            },
                            "elements": [],
                        }
                    ],
                }
            },
            "client",
        )
        self.assertEqual(self.commit_request(replica, publish)["result"]["status"], "PUBLISHED")

        created = self.commit_request(
            replica,
            Request(
                "create-slide-through-raft",
                "create_slide",
                {
                    "deck_id": str(deck_id),
                    "expected_deck_version": 0,
                    "slide": {
                        "metadata": {
                            "slide_id": str(created_slide_id),
                            "background": "#AABBCCFF",
                        },
                        "elements": [],
                    },
                    "insert_after_slide_id": str(original_slide_id),
                },
                "client",
            ),
        )

        self.assertEqual(created["result"]["status"], "CREATED")
        deck = replica.disk["raft_state"]["fsm"]["decks"][str(deck_id)]
        self.assertEqual(deck["deck_version"], 1)
        self.assertEqual(
            [slide["metadata"]["slide_id"] for slide in deck["slides"]],
            [str(original_slide_id), str(created_slide_id)],
        )

    def test_element_creation_and_local_observations_use_the_existing_adapter_path(self) -> None:
        replica = self.app()
        self.elect_leader(replica)
        deck_id, slide_id, element_id, session_id = uuid4(), uuid4(), uuid4(), uuid4()
        published = self.commit_request(
            replica,
            Request(
                "publish-element-deck", "publish_deck", {"presentation": {"schema_version": 2,
                    "metadata": {"deck_id": str(deck_id), "name": "Element deck",
                        "owner_session_id": str(uuid4()), "width_px": 1280, "height_px": 720},
                    "slides": [{"metadata": {"slide_id": str(slide_id), "background": "#112233FF"},
                        "elements": []}]}}, "client",
            ),
        )
        self.assertEqual(published["result"]["status"], "PUBLISHED")

        created = self.commit_request(
            replica,
            Request(
                "create-element-through-raft", "create_element", {"deck_id": str(deck_id),
                    "session_id": str(session_id), "slide_id": str(slide_id), "element": {
                        "element_id": str(element_id), "kind": "rectangle", "version": 1,
                        "configuration": {"x": 0, "y": 0, "width": 100, "height": 80,
                            "scale_basis_points": 10000, "rotation_centidegrees": 0,
                            "fill": "#112233FF", "border_color": "#000000FF", "border_style": "solid",
                            "border_thickness": 1}}}, "client",
            ),
        )
        self.assertEqual(created["result"]["status"], "CREATED_AND_LOCKED")
        self.assertEqual(created["result"]["value"]["fencing_token"], 1)

        observed = replica.on_request(Request("local-element", "get_element", {
            "deck_id": str(deck_id), "element_id": str(element_id)}, "viewer"))
        self.assertEqual(observed["result"]["status"], "OK")
        self.assertEqual(observed["result"]["applied_index"], replica.engine.state.raft.last_applied)
        self.assertEqual(observed["result"]["lock"]["session_id"], str(session_id))

    def test_protected_element_lifecycle_and_authoritative_conflict_refresh_use_adapter_path(self) -> None:
        replica = self.app()
        self.elect_leader(replica)
        deck_id, slide_id, element_id = uuid4(), uuid4(), uuid4()
        sofia, santiago = uuid4(), uuid4()
        self.commit_request(replica, Request("publish-lifecycle-deck", "publish_deck", {
            "presentation": {"schema_version": 2, "metadata": {"deck_id": str(deck_id),
                "name": "Lifecycle", "owner_session_id": str(uuid4()), "width_px": 1280, "height_px": 720},
                "slides": [{"metadata": {"slide_id": str(slide_id), "background": "#112233FF"}, "elements": []}]},
        }, "client"))
        created = self.commit_request(replica, Request("create-lifecycle-element", "create_element", {
            "deck_id": str(deck_id), "session_id": str(sofia), "slide_id": str(slide_id), "element": {
                "element_id": str(element_id), "kind": "text_box", "version": 1, "configuration": {
                    "content": "Before", "generic_fallback": "sans-serif", "font_size": 20,
                    "color": "#000000FF", "x": 0, "y": 0}},
        }, "client"))
        self.assertEqual(created["result"]["status"], "CREATED_AND_LOCKED")
        saved = self.commit_request(replica, Request("save-lifecycle-element", "update_element", {
            "deck_id": str(deck_id), "session_id": str(sofia), "element_id": str(element_id),
            "fencing_token": 1, "expected_version": 1, "patch": {"content": "After", "x": 20},
        }, "client"))
        self.assertEqual(saved["result"]["status"], "SAVED")
        reused_id = replica.on_request(Request("save-lifecycle-element", "update_element", {
            "deck_id": str(deck_id), "session_id": str(sofia), "element_id": str(element_id),
            "fencing_token": 1, "expected_version": 2, "patch": {"content": "Must not apply"},
        }, "client"))
        self.assertEqual(reused_id["result"]["status"], "REQUEST_ID_REUSE")
        conflict = self.commit_request(replica, Request("stale-lifecycle-save", "update_element", {
            "deck_id": str(deck_id), "session_id": str(sofia), "element_id": str(element_id),
            "fencing_token": 1, "expected_version": 1, "patch": {"content": "Stale"},
        }, "client"))
        self.assertEqual(conflict["result"]["status"], "VERSION_CONFLICT")
        self.assertEqual(conflict["result"]["value"]["current_version"], 2)
        refresh_request = Request("refresh-lifecycle-element", "authoritative_refresh_element", {
            "deck_id": str(deck_id), "element_id": str(element_id),
            "min_commit_index": conflict["result"]["value"]["min_commit_index"],
        }, "client")
        read_done, refresh = threading.Event(), {}
        replica._waiting[refresh_request.id] = (read_done, refresh)
        self.assertIsNone(replica.on_request(refresh_request))
        self.assertFalse(read_done.is_set(), "a stale leader hint must not authorize the read")
        self.acknowledge_read_index(replica)
        self.assertTrue(read_done.is_set())
        self.assertEqual(refresh["result"]["element"]["configuration"]["content"], "After")
        self.assertGreaterEqual(refresh["result"]["applied_index"], conflict["result"]["value"]["min_commit_index"])

        self.assertEqual(self.commit_request(replica, Request("release-lifecycle-element", "release_lock", {
            "deck_id": str(deck_id), "session_id": str(sofia), "element_id": str(element_id), "fencing_token": 1,
        }, "client"))["result"]["status"], "RELEASED")
        acquire_request = Request("acquire-lifecycle-element", "acquire_lock", {
            "deck_id": str(deck_id), "session_id": str(santiago), "element_id": str(element_id),
        }, "client")
        acquired = self.commit_request(replica, acquire_request)
        self.assertEqual(acquired["result"]["value"]["fencing_token"], 2)
        self.assertIsInstance(acquired["committed_index"], int)
        self.assertEqual(replica.on_request(acquire_request)["committed_index"], acquired["committed_index"])
        deleted = self.commit_request(replica, Request("delete-lifecycle-element", "delete_element", {
            "deck_id": str(deck_id), "session_id": str(santiago), "element_id": str(element_id),
            "fencing_token": 2, "expected_version": 2,
        }, "client"))
        self.assertEqual(deleted["result"]["status"], "DELETED")
        self.assertEqual(replica.on_request(Request("deleted-element", "get_element", {
            "deck_id": str(deck_id), "element_id": str(element_id)}, "client"))["result"]["element_tombstoned"], True)

    def test_authoritative_refresh_defers_until_the_leader_applies_the_requested_index(self) -> None:
        replica = self.app()
        self.elect_leader(replica)
        deck_id, slide_id, element_id, session_id = uuid4(), uuid4(), uuid4(), uuid4()
        refresh = Request("deferred-authoritative-refresh", "authoritative_refresh_element", {
            "deck_id": str(deck_id), "element_id": str(element_id), "min_commit_index": 3,
        }, "viewer")
        done, response = threading.Event(), {}
        replica._waiting[refresh.id] = (done, response)

        self.assertIsNone(replica.on_request(refresh))
        self.assertFalse(done.is_set())
        self.assertNotIn("MIN_COMMIT_INDEX_UNAVAILABLE", str(response))

        self.commit_request(replica, Request("publish-deferred-refresh", "publish_deck", {
            "presentation": {"schema_version": 2, "metadata": {"deck_id": str(deck_id),
                "name": "Deferred refresh", "owner_session_id": str(uuid4()), "width_px": 1280, "height_px": 720},
                "slides": [{"metadata": {"slide_id": str(slide_id), "background": "#112233FF"}, "elements": []}]},
        }, "client"))
        self.assertFalse(done.is_set())
        # Publication committed the current-term no-op, allowing ReadIndex to
        # send a quorum probe. Its reply does not bypass the min-index fence.
        self.acknowledge_read_index(replica)
        self.assertFalse(done.is_set())

        self.commit_request(replica, Request("create-deferred-refresh-element", "create_element", {
            "deck_id": str(deck_id), "session_id": str(session_id), "slide_id": str(slide_id), "element": {
                "element_id": str(element_id), "kind": "text_box", "version": 1, "configuration": {
                    "content": "Applied at requested index", "generic_fallback": "sans-serif", "font_size": 20,
                    "color": "#000000FF", "x": 0, "y": 0}},
        }, "client"))

        self.assertTrue(done.is_set())
        self.assertEqual(response["result"]["status"], "OK")
        self.assertGreaterEqual(response["result"]["applied_index"], 3)
        self.assertEqual(response["result"]["element"]["element_id"], str(element_id))

    def test_debug_mode_records_payload_and_shutdown_archives_events(self) -> None:
        replica = self.app(mode="DEBUG")
        replica._event("request_received", payload={"key": "deck", "value": "visible-in-debug"})
        records = [json.loads(line) for line in (self.run_dir / "n1" / "events.log").read_text().splitlines()]
        self.assertEqual(records[-1]["payload"], {"key": "deck", "value": "visible-in-debug"})

        original_run = RUN
        try:
            import cluster

            cluster.RUN = self.run_dir
            original_here = cluster.HERE
            cluster.HERE = self.run_dir / "project"
            cluster.HERE.mkdir()
            with patch.dict(os.environ, {"OPERATIONAL_LOG_MODE": "DEBUG"}, clear=False):
                write_compose(["n1", "n2", "n3"])
            compose = (self.run_dir / "compose.yaml").read_text()
            self.assertIn("image: proyecto-raft:stage-a", compose)
            self.assertIn("build:", compose)
            self.assertIn("OPERATIONAL_LOG_MODE: DEBUG", compose)
            (self.run_dir / "history.jsonl").write_text("{}\n")
            archived = keep_histories()
            self.assertIsNotNone(archived)
            self.assertTrue((archived / "n1" / "events.log").is_file())
        finally:
            import cluster

            cluster.RUN = original_run
            cluster.HERE = original_here


if __name__ == "__main__":
    unittest.main()
