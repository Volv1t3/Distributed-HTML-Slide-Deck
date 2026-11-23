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
                    "schema_version": 1,
                    "metadata": {
                        "deck_id": str(deck_id),
                        "name": "Published through Raft",
                        "owner_session_id": str(owner_session_id),
                        "width_px": 1280,
                        "height_px": 720,
                    },
                    "slides": [
                        {
                            "metadata": {"slide_id": str(slide_id), "background": "#112233", "ordinal": 0},
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
