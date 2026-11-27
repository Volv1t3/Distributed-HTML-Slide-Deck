"""Stage F #59: durable account/deck journal and safe recovery seams."""

import json

import pytest

from authenticated_client import AuthenticatedClient
from client_journal import ClientIntentionOutbox, JournaledClient, Lifecycle, LocalIntentionRejected


def bound(tmp_path, name="deck-a", user="alice"):
    return ClientIntentionOutbox("installation", tmp_path / (name + ".jsonl"), "session",
                                 user_id=user, deck_id=name)


def role(outbox, access, version=1):
    epoch = outbox.begin_permission_observation()
    result = {"status": "OK", "effective_access": access}
    if access != "NONE":
        result.update(access_version=version, applied_index=version)
    assert outbox.observe_permission(result, epoch=epoch)
    return epoch


def edit(outbox, name="edit"):
    outbox.record_base_projection({"deck_id": outbox.deck_id, "applied_index": 1})
    return outbox.enqueue("update_deck_settings", {
        "deck_id": outbox.deck_id, "patch": {"name": name}, "expected_deck_settings_version": 0,
    }, request_id=name)


def test_bound_gate_requires_fresh_role_and_survives_access_loss_restart(tmp_path):
    first = bound(tmp_path)
    edit(first)
    sent = []
    first.synchronize(lambda i: sent.append(i.request_id) or None)
    assert sent == []
    role(first, "EDIT")
    role(first, "VIEW", version=2)
    restarted = bound(tmp_path)
    assert restarted.gate["requires_explicit_reconciliation"]
    assert restarted.gate["last_observed_access"] == "UNKNOWN"
    role(restarted, "EDIT", version=3)
    restarted.synchronize(lambda i: sent.append(i.request_id) or None)
    assert sent == []
    assert restarted.get("edit").arguments["patch"] == {"name": "edit"}


def test_delayed_positive_cannot_override_none_or_newer_view(tmp_path):
    outbox = bound(tmp_path)
    early = outbox.begin_permission_observation()
    role(outbox, "NONE")
    assert not outbox.observe_permission({"status": "OK", "effective_access": "EDIT",
                                          "access_version": 1, "applied_index": 1}, epoch=early)
    assert outbox.gate["last_observed_access"] == "NONE"
    role(outbox, "VIEW", version=2)
    assert not outbox.observe_permission({"status": "OK", "effective_access": "EDIT",
                                          "access_version": 1, "applied_index": 1}, epoch=early)


def test_failed_fresh_permission_check_fences_older_positive(tmp_path):
    outbox = bound(tmp_path)
    early = outbox.begin_permission_observation()
    outbox.suspend_permission()
    assert not outbox.observe_permission({"status": "OK", "effective_access": "EDIT",
                                          "access_version": 1, "applied_index": 1}, epoch=early)
    assert outbox.gate["suspended"]
    assert outbox.gate["last_observed_access"] == "UNKNOWN"


def test_receipt_unknown_retains_id_and_definitive_result_has_no_authority(tmp_path):
    outbox = bound(tmp_path)
    edit(outbox)
    outbox.apply_receipt("edit", {"ok": True, "result": {"status": "OUTCOME_UNKNOWN"}})
    assert outbox.get("edit").lifecycle is Lifecycle.PENDING_SYNC
    outbox.apply_receipt("edit", {"ok": True, "result": {"status": "COMMITTED_SUCCESS"}})
    assert outbox.get("edit").result == {"status": "COMMITTED_SUCCESS"}
    assert outbox.get("edit").request_id == "edit"


def test_denied_patch_is_read_only_and_corrected_work_has_new_id(tmp_path):
    outbox = bound(tmp_path)
    edit(outbox)
    role(outbox, "EDIT")
    outbox.synchronize(lambda _: {"ok": True, "result": {"status": "ACCESS_DENIED"}})
    assert outbox.get("edit").lifecycle is Lifecycle.REJECTED_FINAL
    recovered = outbox.denied_recovery_view()
    assert recovered[0].arguments["patch"] == {"name": "edit"}
    recovered[0].arguments["patch"]["name"] = "tampered"
    assert outbox.get("edit").arguments["patch"] == {"name": "edit"}
    role(outbox, "EDIT", version=2)
    replacement = outbox.approve_reconciled_intention("edit", "update_deck_settings", {
        "deck_id": "deck-a", "patch": {"name": "corrected"}, "expected_deck_settings_version": 3,
    }, request_id="corrected")
    assert replacement.request_id == "corrected"
    assert outbox.get("edit").lifecycle is Lifecycle.REJECTED_FINAL


def test_cross_deck_and_unbound_legacy_journal_fail_closed_without_rewrite(tmp_path):
    first = bound(tmp_path)
    edit(first)
    before = first.journal_path.read_bytes()
    with pytest.raises(LocalIntentionRejected):
        first.enqueue("update_deck_settings", {"deck_id": "deck-b", "patch": {"name": "x"}})
    with pytest.raises(ValueError):
        ClientIntentionOutbox("installation", first.journal_path, "session",
                             user_id="bob", deck_id="deck-a")
    assert first.journal_path.read_bytes() == before
    legacy = tmp_path / "legacy.jsonl"
    legacy.write_text("".join(json.dumps({"kind": "intent", "intention": {
        "request_id": deck, "client_id": "installation", "operation": "update_deck_settings",
        "arguments": {"deck_id": deck, "patch": {"name": deck}},
    }}) + "\n" for deck in ("deck-a", "deck-b")))
    original = legacy.read_bytes()
    with pytest.raises(ValueError):
        ClientIntentionOutbox("installation", legacy, "session", user_id="alice", deck_id="deck-a")
    assert legacy.read_bytes() == original


def test_subordinate_denial_does_not_install_partial_poll(tmp_path):
    outbox = bound(tmp_path)
    role(outbox, "EDIT")
    outbox.record_base_projection({"deck_id": "deck-a", "applied_index": 1, "slide_ids": []})
    def sender(op, args, request_id):
        if op == "get_deck_preview":
            return {"ok": True, "result": {"status": "OK", "effective_access": "EDIT",
                "access_version": 2, "applied_index": 2, "slide_ids": ["slide-a"]}}
        return {"ok": True, "result": {"status": "ACCESS_DENIED"}}
    assert not outbox.poll("deck-a", sender)
    assert outbox.highest_applied_index == 1
    assert outbox.base_projection["slide_ids"] == []
    assert outbox.gate["suspended"]


def test_two_decks_share_transport_but_not_gate_or_account_authority(tmp_path):
    transport = AuthenticatedClient(tmp_path / "client", {"leader": "http://leader"},
                                    tmp_path / "history.jsonl")
    transport._in_memory_credential = {"user_id": "alice", "access_token": "private-bearer"}
    first = JournaledClient(transport.client_id, tmp_path / "a.jsonl", "session",
                            request_sender=transport.journal_sender,
                            local_replica="follower", leader_replica="leader",
                            user_id="alice", deck_id="deck-a")
    second = JournaledClient(transport.client_id, tmp_path / "b.jsonl", "session",
                             request_sender=transport.journal_sender,
                             local_replica="follower", leader_replica="leader",
                             user_id="alice", deck_id="deck-b")
    for journal in (first, second):
        journal.outbox.record_base_projection({"deck_id": journal.outbox.deck_id, "applied_index": 1})
        role(journal.outbox, "EDIT")
        journal.enqueue("update_deck_settings", {"deck_id": journal.outbox.deck_id,
            "patch": {"name": "review"}, "expected_deck_settings_version": 0},
            request_id="edit-" + journal.outbox.deck_id)
    first.outbox.observe_permission({"status": "OK", "effective_access": "NONE"},
                                    epoch=first.outbox.begin_permission_observation())
    sent = []
    first.outbox.synchronize(lambda intent: sent.append(intent.request_id) or None)
    second.outbox.synchronize(lambda intent: sent.append(intent.request_id) or None)
    assert sent == ["edit-deck-b"]
    assert first.outbox.highest_applied_index == second.outbox.highest_applied_index == 1
    assert first.outbox.journal_path != second.outbox.journal_path
    with pytest.raises(LocalIntentionRejected):
        first.poll_if_due("deck-b")
    transport._in_memory_credential = {"user_id": "bob", "access_token": "another-secret"}
    with pytest.raises(LocalIntentionRejected):
        second.synchronize()
    assert "private-bearer" not in first.outbox.journal_path.read_text()
    assert "another-secret" not in second.outbox.journal_path.read_text()


def test_access_loss_stops_new_lock_renewals_before_transport(tmp_path, monkeypatch):
    transport = AuthenticatedClient(tmp_path / "client", ["http://leader"], tmp_path / "history.jsonl")
    transport._in_memory_credential = {"user_id": "alice", "access_token": "secret"}
    journal = JournaledClient(transport.client_id, tmp_path / "a.jsonl", "session",
                              request_sender=transport.journal_sender,
                              local_replica="follower", leader_replica="leader",
                              user_id="alice", deck_id="deck-a")
    sent = []
    monkeypatch.setattr(transport, "_try_nodes", lambda payload, timeout=None:
                        sent.append(payload) or {"ok": True, "result": {"status": "RENEWED"}})
    args = {"deck_id": "deck-a", "element_id": "element-a", "session_id": "session",
            "fencing_token": 1}
    role(journal.outbox, "EDIT")
    assert transport.renewal_sender("renew_lock", args, "renew-1", 1)["result"]["status"] == "RENEWED"
    role(journal.outbox, "VIEW", version=2)
    assert transport.renewal_sender("renew_lock", args, "renew-2", 1) == {
        "ok": True, "result": {"status": "ACCESS_DENIED"},
    }
    assert len(sent) == 1
    assert "secret" not in journal.outbox.journal_path.read_text()


def _journal(tmp_path, sender):
    return JournaledClient("installation", tmp_path / "deck-a.jsonl", "session",
                           request_sender=sender, local_replica="follower",
                           leader_replica="leader", user_id="alice", deck_id="deck-a")


def _replay_sender(calls, *, receipt_status, mutation_responses):
    """Lost first response, masked ACCESS_DENIED replay, then receipt/role reads."""
    def sender(op, args, request_id, node):
        calls.append(op)
        if op == "update_deck_settings":
            return mutation_responses.pop(0)
        if op == "query_operation_receipt":
            assert args == {"original_request_id": "edit"}
            return {"ok": True, "result": {"status": receipt_status}}
        if op == "query_grants":
            return {"ok": True, "result": {"status": "OK", "effective_access": "NONE"}}
        raise AssertionError(op)
    return sender


def test_masked_access_denied_replay_of_a_committed_edit_resolves_to_committed(tmp_path):
    """Spec §14/§15: an exact-ID replay masked as ACCESS_DENIED after access loss
    must not be journaled as a terminal denial when the original committed."""
    calls = []
    sender = _replay_sender(calls, receipt_status="COMMITTED_SUCCESS", mutation_responses=[
        None,  # the committed original's response was lost in transport
        {"ok": True, "result": {"status": "ACCESS_DENIED"}},  # masked exact-ID replay
    ])
    first = _journal(tmp_path, sender)
    edit(first.outbox)
    role(first.outbox, "EDIT")
    first.synchronize()
    assert first.outbox.get("edit").lifecycle is Lifecycle.PENDING_SYNC

    restarted = _journal(tmp_path, sender)
    role(restarted.outbox, "EDIT")
    restarted.synchronize()

    recovered = restarted.outbox.get("edit")
    assert recovered.lifecycle is Lifecycle.COMMITTED
    assert recovered.result == {"status": "COMMITTED_SUCCESS"}
    assert restarted.outbox.denied_recovery_view() == []
    assert restarted.outbox.gate["suspended"]
    assert calls.count("update_deck_settings") == 2
    assert "query_operation_receipt" in calls
    assert _journal(tmp_path, sender).outbox.get("edit").lifecycle is Lifecycle.COMMITTED


def test_masked_replay_with_unknown_receipt_stays_uncertain_and_is_not_resent(tmp_path):
    calls = []
    sender = _replay_sender(calls, receipt_status="OUTCOME_UNKNOWN", mutation_responses=[
        None, {"ok": True, "result": {"status": "ACCESS_DENIED"}},
    ])
    journal = _journal(tmp_path, sender)
    edit(journal.outbox)
    role(journal.outbox, "EDIT")
    journal.synchronize()
    journal.synchronize()

    pending = journal.outbox.get("edit")
    assert pending.lifecycle is Lifecycle.PENDING_SYNC
    assert pending.arguments["patch"] == {"name": "edit"}
    journal.synchronize()
    assert calls.count("update_deck_settings") == 2, "an unknown outcome forbids resubmission"


def test_masked_replay_whose_receipt_is_rejected_becomes_terminal(tmp_path):
    calls = []
    sender = _replay_sender(calls, receipt_status="COMMITTED_REJECTED", mutation_responses=[
        None, {"ok": True, "result": {"status": "ACCESS_DENIED"}},
    ])
    journal = _journal(tmp_path, sender)
    edit(journal.outbox)
    role(journal.outbox, "EDIT")
    journal.synchronize()
    journal.synchronize()
    assert journal.outbox.get("edit").lifecycle is Lifecycle.REJECTED_FINAL
