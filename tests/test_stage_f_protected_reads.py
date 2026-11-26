"""Stage F issue #58: protected deck reads authorized through leader ReadIndex.

Seams under test:
  1. ``KeyValueStateMachine.protected_read`` / ``query_grants`` — the deterministic
     role check and masked projection evaluated on the applied state at the barrier.
  2. ``App.on_request`` — leader binding, bearer admission, ReadIndex fencing and
     linearization against committed grant changes.
  3. ``spec`` — public consistency classification and reference model.
"""

from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest

from app import App
from auth_catalog import load_account_catalog
from command_dto import (
    CommandEnvelopeDTO,
    PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
    presentation_schema_activation_request_id,
)
from node import Request
from stage_e_raft_harness import SimulatedRaftHarnessMixin, issue_token_command
from test_stage_f_grants_and_permissions import _GrantFixture, _grant_cmd, _revoke_cmd


CONTENT_READS = (
    "get_deck_preview", "get_slide", "get_element", "read_lock_status",
    "authoritative_refresh_element", "refresh_element",
)


def _content_args(op: str, deck_id: str) -> dict:
    args: dict = {"deck_id": deck_id}
    if op == "get_slide":
        args["slide_id"] = str(uuid4())
    if op in {"get_element", "read_lock_status", "authoritative_refresh_element", "refresh_element"}:
        args["element_id"] = str(uuid4())
    if op in {"authoritative_refresh_element", "refresh_element"}:
        args["min_commit_index"] = 0
    return args


# ---------------------------------------------------------------------------
# Seam 1: state-machine authorization projection
# ---------------------------------------------------------------------------

def test_owner_preview_carries_effective_access_and_access_version():
    fx = _GrantFixture()
    result = fx.machine.protected_read(
        "get_deck_preview", {"deck_id": fx.deck_id},
        actor_user_id=fx.owner_id, applied_index=9,
    )
    assert result["status"] == "OK"
    assert result["effective_access"] == "OWNER"
    assert result["access_version"] == 0
    assert result["applied_index"] == 9
    assert result["deck_id"] == fx.deck_id


@pytest.mark.parametrize("op", CONTENT_READS)
def test_none_and_unknown_deck_return_identical_access_denied(op):
    fx = _GrantFixture()
    inaccessible = fx.machine.protected_read(
        op, _content_args(op, fx.deck_id), actor_user_id=fx.other_id, applied_index=9,
    )
    unknown = fx.machine.protected_read(
        op, _content_args(op, str(uuid4())), actor_user_id=fx.other_id, applied_index=9,
    )
    assert inaccessible == unknown == {"status": "ACCESS_DENIED"}


@pytest.mark.parametrize("level", ["VIEW", "EDIT"])
@pytest.mark.parametrize("op", CONTENT_READS)
def test_view_and_edit_participants_can_read_every_protected_name(op, level):
    fx = _GrantFixture()
    assert fx.apply(_grant_cmd(
        fx.deck_id, fx.owner_id, fx.owner_client, fx.owner_cred, fx.other_id, level, 0,
    )).status == "ACCESS_GRANTED"
    result = fx.machine.protected_read(
        op, _content_args(op, fx.deck_id), actor_user_id=fx.other_id, applied_index=9,
    )
    # The empty fixture deck has no such slide/element: an authorized caller
    # may learn that a child resource is missing inside a deck it can read.
    assert result["status"] == ("OK" if op == "get_deck_preview" else "NOT_FOUND")
    assert result["deck_id"] == fx.deck_id
    if op == "get_deck_preview":
        assert result["effective_access"] == level
        assert result["access_version"] == 1


def test_revoked_participant_is_denied_content():
    fx = _GrantFixture()
    fx.apply(_grant_cmd(fx.deck_id, fx.owner_id, fx.owner_client, fx.owner_cred, fx.other_id, "EDIT", 0))
    assert fx.apply(_revoke_cmd(
        fx.deck_id, fx.owner_id, fx.owner_client, fx.owner_cred, fx.other_id, 1,
    )).status == "ACCESS_REVOKED"
    assert fx.machine.protected_read(
        "get_deck_preview", {"deck_id": fx.deck_id}, actor_user_id=fx.other_id, applied_index=9,
    ) == {"status": "ACCESS_DENIED"}


def test_ownerless_legacy_deck_is_quarantined_for_protected_reads():
    from stage_e_raft_harness import publish_empty_deck_command
    from state_machine import KeyValueStateMachine

    machine = KeyValueStateMachine({"kv": {}})
    legacy_id = str(uuid4())
    assert machine.apply(publish_empty_deck_command(legacy_id), committed_index=1).status == "PUBLISHED"
    assert machine.protected_read(
        "get_deck_preview", {"deck_id": legacy_id}, actor_user_id=str(uuid4()), applied_index=1,
    ) == {"status": "ACCESS_DENIED"}


def test_query_grants_reports_positive_role_with_revision_metadata():
    fx = _GrantFixture()
    fx.apply(_grant_cmd(fx.deck_id, fx.owner_id, fx.owner_client, fx.owner_cred, fx.other_id, "VIEW", 0))
    assert fx.machine.query_grants(fx.deck_id, actor_user_id=fx.other_id, applied_index=11) == {
        "status": "OK", "effective_access": "VIEW", "deck_id": fx.deck_id,
        "access_version": 1, "applied_index": 11,
    }
    owner = fx.machine.query_grants(fx.deck_id, actor_user_id=fx.owner_id, applied_index=11)
    assert owner["effective_access"] == "OWNER"
    assert set(owner) == {"status", "effective_access", "deck_id", "access_version", "applied_index"}


def test_query_grants_none_is_indistinguishable_from_unknown_deck():
    fx = _GrantFixture()
    inaccessible = fx.machine.query_grants(fx.deck_id, actor_user_id=fx.other_id, applied_index=11)
    unknown = fx.machine.query_grants(str(uuid4()), actor_user_id=fx.other_id, applied_index=11)
    assert inaccessible == unknown == {"status": "OK", "effective_access": "NONE"}


# ---------------------------------------------------------------------------
# Seam 2: App.on_request leader-bound authenticated ReadIndex pipeline
# ---------------------------------------------------------------------------

class ProtectedReadAppTests(SimulatedRaftHarnessMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)
        self.nodes = {slot: f"http://{slot}:8000" for slot in ("n1", "n2", "n3")}
        self.replicas: list[App] = []
        self.tokens: dict[str, str] = {}
        catalog = load_account_catalog()
        self.owner_id = str(catalog.users[0].profile.user_id)
        self.other_id = str(catalog.users[1].profile.user_id)

    def tearDown(self) -> None:
        for replica in self.replicas:
            replica._password_verifier.shutdown()
            replica._events.close()
            replica._log.close()
        self.workspace.cleanup()

    def _app(self, slot: str = "n1") -> App:
        replica = App(slot, self.nodes, self.run_dir)
        replica.start()
        self.replicas.append(replica)
        return replica

    def _leader(self) -> App:
        replica = self._app()
        self._elect_and_commit_noop(replica)
        if replica.engine.state.fsm.get("presentation_schema_policy") is None:
            self._commit_command(replica, CommandEnvelopeDTO.model_validate({
                "command_schema_version": 1,
                "request_id": presentation_schema_activation_request_id(),
                "client_id": PRESENTATION_SCHEMA_RESERVED_CLIENT_ID,
                "origin_node_uuid": str(uuid4()),
                "operation": "activate_presentation_schema_v3",
                "payload": {},
            }))
        return replica

    def _login(self, replica: App, user_id: str) -> str:
        client_id, credential_id = str(uuid4()), str(uuid4())
        token = "read-bearer-" + str(uuid4())
        self._commit_command(replica, issue_token_command(
            user_id, client_id, credential_id, hashlib.sha256(token.encode("ascii")).hexdigest(),
            request_id="read-issue-" + client_id,
        ))
        self.tokens[client_id] = token
        return client_id

    def _commit(self, replica: App, op: str, args: dict, client: str) -> dict:
        return self._commit_admitted_request(
            replica, Request("req-" + str(uuid4()), op, args, client), self.tokens[client],
        )

    def _publish(self, replica: App, owner_client: str) -> tuple[str, str]:
        deck_id, slide_id = str(uuid4()), str(uuid4())
        published = self._commit(replica, "publish_deck", {"presentation": {
            "schema_version": 3,
            "metadata": {"deck_id": deck_id, "name": "Protected", "owner_session_id": str(uuid4()),
                         "owner_user_id": self.owner_id, "width_px": 1280, "height_px": 720},
            "slides": [{"metadata": {"slide_id": slide_id, "background": "#112233FF"}, "elements": []}],
        }}, owner_client)
        self.assertEqual(published["result"]["status"], "PUBLISHED", published)
        return deck_id, slide_id

    def _start_read(self, replica: App, op: str, args: dict, client: str, *,
                    bearer: bool = True, token: str | None = None):
        """Submit a read; return (immediate_response, done_event, response_holder)."""
        request = Request("read-" + str(uuid4()), op, dict(args), client)
        if bearer:
            request.args["_auth"] = {"access_token": token or self.tokens[client]}
        done, response = threading.Event(), {}
        replica._waiting[request.id] = (done, response)
        immediate = replica.on_request(request)
        return immediate, done, response

    def _read(self, replica: App, op: str, args: dict, client: str, **kwargs) -> dict:
        immediate, done, response = self._start_read(replica, op, args, client, **kwargs)
        if immediate is not None:
            return immediate
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        self.assertTrue(done.is_set(), "protected read produced no reply")
        return response

    def test_owner_reads_preview_through_leader_read_index(self) -> None:
        replica = self._leader()
        owner = self._login(replica, self.owner_id)
        deck_id, slide_id = self._publish(replica, owner)
        log_length = len(replica.engine.state.raft.log)

        immediate, done, response = self._start_read(
            replica, "get_deck_preview", {"deck_id": deck_id}, owner,
        )
        self.assertIsNone(immediate, "a protected read must wait for a fresh ReadIndex")
        self.assertFalse(done.is_set())
        self._acknowledge_read(replica)
        replica._progress_auth_work()

        self.assertTrue(done.is_set())
        self.assertEqual(response["result"]["status"], "OK")
        self.assertEqual(response["result"]["effective_access"], "OWNER")
        self.assertEqual(response["result"]["slide_ids"], [slide_id])
        self.assertEqual(len(replica.engine.state.raft.log), log_length, "reads append no Raft entry")

    def _shared_deck(self, level: str | None = None):
        """Leader with an owner deck; optionally grant the second account ``level``."""
        replica = self._leader()
        owner = self._login(replica, self.owner_id)
        other = self._login(replica, self.other_id)
        deck_id, slide_id = self._publish(replica, owner)
        if level is not None:
            granted = self._commit(replica, "grant_access", {
                "deck_id": deck_id, "target_user_id": self.other_id,
                "access_level": level, "expected_access_version": 0,
            }, owner)
            self.assertEqual(granted["result"]["status"], "ACCESS_GRANTED", granted)
        return replica, owner, other, deck_id, slide_id

    def test_follower_never_serves_protected_content_even_with_a_local_grant(self) -> None:
        leader, _, other, deck_id, _ = self._shared_deck("EDIT")
        follower = self._app("n2")
        follower.engine.state.fsm = deepcopy(leader.engine.state.fsm)
        for op in (*CONTENT_READS, "query_grants", "query_operation_receipt"):
            read_args = ({"original_request_id": "missing"} if op == "query_operation_receipt"
                         else _content_args(op, deck_id))
            args = {**read_args, "_auth": {"access_token": self.tokens[other]}}
            follower.engine.leader_slot = None
            self.assertEqual(follower.on_request(Request("f-" + op, op, args, other)),
                             {"ok": False, "error": "NO_LEADER"})
            follower.engine.leader_slot = "n1"
            self.assertEqual(follower.on_request(Request("r-" + op, op, args, other)),
                             {"redirect": "n1"})

    def test_missing_or_invalid_bearer_requires_reauthentication(self) -> None:
        replica, owner, _, deck_id, _ = self._shared_deck()
        for op in (*CONTENT_READS, "query_grants", "query_operation_receipt"):
            read_args = ({"original_request_id": "missing"} if op == "query_operation_receipt"
                         else _content_args(op, deck_id))
            missing = self._read(replica, op, read_args, owner, bearer=False)
            self.assertEqual(missing, {"ok": False, "error": "REAUTH_REQUIRED"})
            forged = self._read(replica, op, read_args, owner, token="not-the-token")
            self.assertEqual(forged, {"ok": False, "error": "REAUTH_REQUIRED"})

    def test_receipt_is_status_only_for_same_client_and_account(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck("EDIT")
        original_id = "edit-" + str(uuid4())
        response = self._commit_admitted_request(replica, Request(original_id, "update_deck_settings", {
            "deck_id": deck_id, "patch": {"name": "Changed"}, "expected_deck_settings_version": 0,
        }, other), self.tokens[other])
        self.assertEqual(response["result"]["status"], "DECK_SETTINGS_UPDATED")
        self._commit(replica, "revoke_access", {
            "deck_id": deck_id, "target_user_id": self.other_id, "expected_access_version": 1,
        }, owner)
        receipt = self._read(replica, "query_operation_receipt", {"original_request_id": original_id}, other)
        self.assertEqual(receipt, {"ok": True, "result": {"status": "COMMITTED_SUCCESS"}})
        self.assertEqual(self._read(replica, "query_operation_receipt", {
            "original_request_id": original_id,
        }, owner), {"ok": True, "result": {"status": "OUTCOME_UNKNOWN"}})
        self.assertEqual(self._read(replica, "query_operation_receipt", {
            "original_request_id": "absent",
        }, other), {"ok": True, "result": {"status": "OUTCOME_UNKNOWN"}})

    def test_receipt_rejection_and_ineligible_publication_are_indistinguishable_from_unknown(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck("EDIT")
        rejected_id = "stale-" + str(uuid4())
        rejected = self._commit_admitted_request(replica, Request(rejected_id, "update_deck_settings", {
            "deck_id": deck_id, "patch": {"name": "Stale"}, "expected_deck_settings_version": 42,
        }, other), self.tokens[other])
        self.assertEqual(rejected["result"]["status"], "DECK_SETTINGS_VERSION_CONFLICT")
        self.assertEqual(self._read(replica, "query_operation_receipt", {
            "original_request_id": rejected_id,
        }, other), {"ok": True, "result": {"status": "COMMITTED_REJECTED"}})
        publication = next(entry.command.request_id for entry in replica.engine.state.raft.log
                           if entry.command.operation == "publish_deck")
        self.assertEqual(self._read(replica, "query_operation_receipt", {
            "original_request_id": publication,
        }, owner), {"ok": True, "result": {"status": "OUTCOME_UNKNOWN"}})

    def test_malformed_read_input_is_rejected_before_any_read_index(self) -> None:
        replica, owner, _, deck_id, _ = self._shared_deck()
        for op, args in (
            ("get_deck_preview", {"deck_id": "not-a-uuid"}),
            ("get_slide", {"deck_id": deck_id}),
            ("refresh_element", {"deck_id": deck_id, "element_id": str(uuid4()), "min_commit_index": -1}),
        ):
            self.assertEqual(self._read(replica, op, args, owner),
                             {"ok": True, "result": {"status": "INVALID_REQUEST"}})
        self.assertEqual(replica._protected_read_waiters, {})

    def test_none_and_unknown_deck_are_identical_across_all_content_reads(self) -> None:
        replica, _, other, deck_id, _ = self._shared_deck()
        for op in CONTENT_READS:
            inaccessible = self._read(replica, op, _content_args(op, deck_id), other)
            unknown = self._read(replica, op, _content_args(op, str(uuid4())), other)
            self.assertEqual(inaccessible, {"ok": True, "result": {"status": "ACCESS_DENIED"}})
            self.assertEqual(inaccessible, unknown)

    def test_view_participant_reads_every_protected_name(self) -> None:
        replica, _, other, deck_id, slide_id = self._shared_deck("VIEW")
        preview = self._read(replica, "get_deck_preview", {"deck_id": deck_id}, other)["result"]
        self.assertEqual((preview["status"], preview["effective_access"], preview["access_version"]),
                         ("OK", "VIEW", 1))
        self.assertIsInstance(preview["applied_index"], int)
        slide = self._read(replica, "get_slide", {"deck_id": deck_id, "slide_id": slide_id}, other)
        self.assertEqual(slide["result"]["status"], "OK")
        for op in ("get_element", "read_lock_status", "authoritative_refresh_element", "refresh_element"):
            result = self._read(replica, op, _content_args(op, deck_id), other)["result"]
            self.assertEqual(result["status"], "NOT_FOUND", op)
            self.assertEqual(result["deck_id"], deck_id)

    def test_query_grants_is_self_only_and_masks_unknown_decks(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck()
        for target in (deck_id, str(uuid4())):
            self.assertEqual(self._read(replica, "query_grants", {"deck_id": target}, other),
                             {"ok": True, "result": {"status": "OK", "effective_access": "NONE"}})
        owner_view = self._read(replica, "query_grants", {"deck_id": deck_id}, owner)["result"]
        self.assertEqual(owner_view["effective_access"], "OWNER")
        self.assertNotIn("grants", owner_view)
        self.assertNotIn("owner_user_id", owner_view)

    def test_revocation_committed_before_read_blocks_content(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck("EDIT")
        self.assertEqual(self._read(replica, "query_grants", {"deck_id": deck_id}, other)
                         ["result"]["effective_access"], "EDIT")
        revoked = self._commit(replica, "revoke_access", {
            "deck_id": deck_id, "target_user_id": self.other_id, "expected_access_version": 1,
        }, owner)
        self.assertEqual(revoked["result"]["status"], "ACCESS_REVOKED")
        self.assertEqual(self._read(replica, "get_deck_preview", {"deck_id": deck_id}, other),
                         {"ok": True, "result": {"status": "ACCESS_DENIED"}})
        self.assertEqual(self._read(replica, "query_grants", {"deck_id": deck_id}, other),
                         {"ok": True, "result": {"status": "OK", "effective_access": "NONE"}})

    def test_read_racing_a_revocation_follows_its_linearization_point(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck("VIEW")
        # The read starts while VIEW is current, but the revoke commits and applies
        # before the barrier completes: the read linearizes after the revoke.
        immediate, done, response = self._start_read(
            replica, "get_deck_preview", {"deck_id": deck_id}, other,
        )
        self.assertIsNone(immediate)
        probe = next(message for destination, message in self._messages(replica)
                     if destination == "n2" and message.get("read_context") is not None)
        revoked = self._commit(replica, "revoke_access", {
            "deck_id": deck_id, "target_user_id": self.other_id, "expected_access_version": 1,
        }, owner)
        self.assertEqual(revoked["result"]["status"], "ACCESS_REVOKED")
        self.assertFalse(done.is_set())
        replica.on_message("n2", {
            "kind": "append_entries_response", "protocol_version": 1,
            "rpc_id": str(uuid4()), "sender_slot": "n2",
            "sender_uuid": str(replica.engine.state.peer_bindings["n2"]),
            "term": probe["term"], "reply_to": probe["rpc_id"], "success": True,
            "match_index": probe["prev_log_index"], "read_context": probe["read_context"],
        })
        self.assertTrue(done.is_set())
        self.assertEqual(response, {"ok": True, "result": {"status": "ACCESS_DENIED"}})

    def test_read_completed_before_revocation_keeps_its_successful_result(self) -> None:
        replica, owner, other, deck_id, _ = self._shared_deck("VIEW")
        before = self._read(replica, "get_deck_preview", {"deck_id": deck_id}, other)
        self.assertEqual(before["result"]["status"], "OK")
        self._commit(replica, "revoke_access", {
            "deck_id": deck_id, "target_user_id": self.other_id, "expected_access_version": 1,
        }, owner)
        self.assertEqual(before["result"]["effective_access"], "VIEW")
        self.assertLess(before["result"]["applied_index"], replica.engine.state.raft.last_applied)

    def test_refresh_alias_waits_for_min_commit_index_before_authorizing(self) -> None:
        replica, owner, other, deck_id, slide_id = self._shared_deck("VIEW")
        element_id = str(uuid4())
        floor = replica.engine.state.raft.last_applied + 1
        immediate, done, response = self._start_read(replica, "refresh_element", {
            "deck_id": deck_id, "element_id": element_id, "min_commit_index": floor,
        }, other)
        self.assertIsNone(immediate)
        self._acknowledge_read(replica)
        replica._progress_auth_work()
        self.assertFalse(done.is_set(), "a confirmed quorum read must not bypass min_commit_index")
        created = self._commit(replica, "create_element", {
            "deck_id": deck_id, "session_id": str(uuid4()), "slide_id": slide_id, "element": {
                "element_id": element_id, "kind": "text_box", "version": 1, "configuration": {
                    "content": "Fenced", "generic_fallback": "sans-serif", "font_size": 20,
                    "color": "#000000FF", "x": 0, "y": 0}},
        }, owner)
        self.assertEqual(created["result"]["status"], "CREATED_AND_LOCKED")
        self.assertTrue(done.is_set())
        self.assertEqual(response["result"]["status"], "OK")
        self.assertGreaterEqual(response["result"]["applied_index"], floor)
        self.assertEqual(response["result"]["element"]["element_id"], element_id)

    def test_leadership_loss_fails_closed_without_content_or_false_none(self) -> None:
        replica, _, other, deck_id, _ = self._shared_deck("VIEW")
        started = []
        for op in ("get_deck_preview", "query_grants"):
            immediate, done, response = self._start_read(replica, op, {"deck_id": deck_id}, other)
            self.assertIsNone(immediate)
            started.append((done, response))
        replica.engine.start_election()
        replica._progress_auth_work()
        for done, response in started:
            self.assertTrue(done.is_set())
            self.assertEqual(response, {"ok": False, "error": "NO_LEADER"})
        self.assertEqual(replica._protected_read_waiters, {})

    def test_bearer_token_never_reaches_the_event_log(self) -> None:
        replica, _, other, deck_id, _ = self._shared_deck("VIEW")
        self._read(replica, "get_deck_preview", {"deck_id": deck_id}, other)
        events = (self.run_dir / "n1" / "events.log").read_text()
        self.assertNotIn(self.tokens[other], events)
        self.assertIn("protected_read_completed", events)


# ---------------------------------------------------------------------------
# Seam 3: public consistency classification and reference model
# ---------------------------------------------------------------------------

def test_protected_reads_are_linearizable_and_generic_get_stays_eventual():
    import spec
    from command_dto import PROTECTED_READ_OPERATIONS

    assert PROTECTED_READ_OPERATIONS == set(CONTENT_READS) | {"query_grants", "query_operation_receipt"}
    for op in PROTECTED_READ_OPERATIONS:
        assert spec.GUARANTEE[op] == "linearizable", op
        assert op in spec.READS, op
    assert spec.GUARANTEE["get"] == "eventual"
    assert "get" in spec.READS
    assert spec.GUARANTEE["grant_access"] == spec.GUARANTEE["revoke_access"] == "linearizable"


def _model_v3_deck(owner_id: str):
    import spec

    deck_id, slide_id = str(uuid4()), str(uuid4())
    state, result = spec.apply(None, "publish_deck", {"presentation": {
        "schema_version": 3,
        "metadata": {"deck_id": deck_id, "name": "Model", "owner_session_id": str(uuid4()),
                     "owner_user_id": owner_id, "width_px": 1280, "height_px": 720},
        "slides": [{"metadata": {"slide_id": slide_id, "background": "#112233FF"}, "elements": []}],
    }, "actor_user_id": owner_id})
    assert result["status"] == "PUBLISHED"
    return state, deck_id, slide_id


def test_reference_model_authorizes_reads_in_grant_and_revoke_order():
    import spec

    catalog = load_account_catalog()
    owner, other = (str(user.profile.user_id) for user in catalog.users[:2])
    state, deck_id, _ = _model_v3_deck(owner)

    _, denied = spec.apply(state, "get_deck_preview", {"deck_id": deck_id, "actor_user_id": other})
    assert denied == {"status": "ACCESS_DENIED"}
    _, none = spec.apply(state, "query_grants", {"deck_id": deck_id, "actor_user_id": other})
    assert none == {"status": "OK", "effective_access": "NONE"}
    _, unknown = spec.apply(None, "query_grants", {"deck_id": str(uuid4()), "actor_user_id": other})
    assert unknown == none

    state, granted = spec.apply(state, "grant_access", {
        "deck_id": deck_id, "target_user_id": other, "access_level": "VIEW",
        "expected_access_version": 0, "actor_user_id": owner,
    })
    assert granted == {"status": "ACCESS_GRANTED", "value": {"access_version": 1},
                       "version": None, "details": {}}
    _, preview = spec.apply(state, "get_deck_preview", {"deck_id": deck_id, "actor_user_id": other})
    assert (preview["status"], preview["effective_access"], preview["access_version"]) == ("OK", "VIEW", 1)

    _, refused = spec.apply(state, "grant_access", {
        "deck_id": deck_id, "target_user_id": other, "access_level": "EDIT",
        "expected_access_version": 1, "actor_user_id": other,
    })
    assert refused["status"] == "ACCESS_DENIED"

    state, revoked = spec.apply(state, "revoke_access", {
        "deck_id": deck_id, "target_user_id": other, "expected_access_version": 1, "actor_user_id": owner,
    })
    assert revoked["status"] == "ACCESS_REVOKED"
    _, after = spec.apply(state, "get_slide", {
        "deck_id": deck_id, "slide_id": str(uuid4()), "actor_user_id": other,
    })
    assert after == {"status": "ACCESS_DENIED"}


def test_reference_model_refresh_alias_matches_authoritative_refresh():
    import spec

    owner = str(load_account_catalog().users[0].profile.user_id)
    state, deck_id, _ = _model_v3_deck(owner)
    args = {"deck_id": deck_id, "element_id": str(uuid4()), "min_commit_index": 0, "actor_user_id": owner}
    assert spec.apply(state, "refresh_element", args) == spec.apply(state, "authoritative_refresh_element", args)
    assert spec.apply(state, "refresh_element", args)[1]["status"] == "NOT_FOUND"
