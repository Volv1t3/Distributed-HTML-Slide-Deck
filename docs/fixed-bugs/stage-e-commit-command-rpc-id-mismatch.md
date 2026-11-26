# Test-infrastructure bug: intermittent rpc_id peer-mismatch in `_commit_command`

**Discovered:** 2026-11-24  
**Fixed in:** `6f57d97` (fix(stage-e): code-review corrections for issue #42)  
**Branch:** `stage-e`  
**Category:** Test-infrastructure defect (no production impact)

---

## Summary

Three tests in `DeckSettingsAuthAdmissionTests` were failing intermittently —
alternating between `OK` and `ERROR` on successive runs with no code changes.
The root cause was a one-line omission in the `_commit_command` test helper:
it picked the first `append_entries` message from the outbound queue without
filtering by destination. When the leader happened to enqueue n3's message
before n2's, the helper ACKed from n2 using n3's `rpc_id`. The Raft leader
silently rejected the cross-peer ACK, the commit never happened, and the FSM
was left without the credential that the rest of the test expected.

---

## Affected tests

| Test | Failure mode |
|---|---|
| `test_exact_id_retry_returns_stored_result` | `KeyError: active_credentials[client_id]` (line 696) |
| `test_exact_id_different_actor_returns_request_id_reuse` | `KeyError: active_credentials[client_id]` (line 743) |
| `test_valid_bearer_token_produces_authenticated_command` | `AssertionError: expected update_deck_settings in log after auth` |

---

## Root cause walkthrough

### Step 1 — what `_commit_command` is supposed to do

`_commit_command` simulates a single Raft quorum commit in a single-node test
harness (n1 as leader, n2 and n3 as simulated peers). Its contract:

1. Drain queued outbound messages.
2. Call `replica.engine.propose(command)` → appends an entry and triggers peer sends.
3. Find the `append_entries` message just sent, extract its `rpc_id`.
4. Send a fake acknowledgement from n2 using that `rpc_id` as `reply_to`.
5. Drain again so `_apply_committed` runs and updates the FSM.

### Step 2 — the broken message selection

When the leader proposes an entry it sends `append_entries` to **both** n2 and
n3. Both messages land in `replica._out`. The helper was written as:

```python
append = next(
    m for _, m in self._messages(replica)
    if m.get("kind") == "append_entries" and m.get("entries")
)
```

The `_` discards the destination. If n3's message arrived first in the queue,
`append` held n3's `rpc_id`. The helper then sent an ACK as n2 (`sender_slot:
"n2"`) with `reply_to` pointing at n3's RPC ID — a cross-peer mismatch.

### Step 3 — why the leader rejected it

`raft_core.py` line 449 (`_handle_append_response`):

```python
pending = self.pending_append_requests.get(response.reply_to)
if pending is None or pending.peer != response.sender_slot or pending.term != response.term:
    return
```

Every outgoing RPC records which peer it was sent to in `pending.peer`. An ACK
from n2 carrying n3's `reply_to` fails `pending.peer != response.sender_slot`
and is **silently dropped** — no error, no log. The commit index never advanced,
`_apply_committed` never ran for the new entry, and the FSM was untouched.

This check is intentional Raft correctness: it prevents one node from spoofing
another peer's acknowledgement. The test helper violated the invariant the
engine was designed to enforce.

### Step 4 — why the failure was intermittent

Python dict iteration order over the leader's peer structures is
insertion-order-stable in theory, but the send order can vary based on pending
RPC state between commits. In practice the queue ordering varied run-to-run:

- **n2 message first** → helper picks n2's `rpc_id` → ACK from n2 matches → **commit succeeds, test passes**
- **n3 message first** → helper picks n3's `rpc_id` → ACK from n2 mismatches → **ACK rejected, test errors**

This produced a ~50/50 pass/fail split across repeated identical runs — the
classic flaky-test signature.

### Step 5 — downstream effect on the third test

`test_valid_bearer_token_produces_authenticated_command` uses `_setup_credential`
which calls `_commit_command` to commit `issue_access_token`. When that commit
failed silently, the credential was absent from `active_credentials`. Later,
`_progress_auth_admitted_commands` verified the bearer token against the FSM
state, found no matching credential, and returned `REAUTH_REQUIRED`. No
`update_deck_settings` entry was ever proposed to the log. The test's assertion
`self.assertTrue(auth_entries, "expected update_deck_settings in log after auth")`
failed with an empty list — with nothing in the traceback pointing to the setup
failure.

---

## The fix

Filter by destination before picking the message:

```python
# Before
append = next(
    m for _, m in self._messages(replica)
    if m.get("kind") == "append_entries" and m.get("entries")
)

# After
append = next(
    m for d, m in self._messages(replica)
    if d == "n2" and m.get("kind") == "append_entries" and m.get("entries")
)
```

This matches the established pattern already used in the same file:

- `_elect_and_commit_noop`: `destination == "n2" and message["kind"] == "request_vote"`
- `_acknowledge_read`: `destination == "n2" and message["kind"] == "append_entries"`

`_commit_command` was the only helper that discarded the destination.

---

## Verification

449 / 449 tests pass after the fix, deterministically across repeated runs.

---

## Production risk

None. `_commit_command` is a test-infrastructure helper with no runtime
equivalent. `raft_core.py` was not changed. The peer-mismatch check in the Raft
engine remains intact and correct.
