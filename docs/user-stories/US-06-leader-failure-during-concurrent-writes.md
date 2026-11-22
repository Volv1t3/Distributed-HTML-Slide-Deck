# US-06 — Leader Failure During Concurrent Editing Requests

- **Status:** Proposed executable acceptance story
- **Related:** US-02, US-03, ADR-001 L-09, L-10, L-16
- **Required course scenario:** `lider`
- **Scenario script:** `scenarios/lider.py`
- **History:** `run/lider.jsonl`
- **Fault:** The incumbent leader crashes during overlapping requests from **two or more client threads**

## User story

**As Sofia and Santiago**, we want our confirmed edits and lock ownership to survive a leader crash, and uncertain requests to be safely retried, so that editing can continue after a new leader is elected without duplicate changes or simultaneous ownership of one element.

## Preconditions

1. Three replicas are healthy; determine the leader and current Raft term. The scenario records initial publication in its own history using `publish_deck`.
2. Deck `D` contains `E_TITLE`, `E_SUBTITLE`, and `E_NOTE`, all at version 1. Sofia has acquired the title lock; Santiago has acquired the subtitle lock. Both acquisitions were committed.
3. The clients share one history path but each has a distinct `Client` instance (`"sofia"`, `"santiago"`). The script uses `threading.Thread` for actual overlapping requests and a barrier to coordinate their start.
4. Expiration must not interfere with this short test: do not let an unlogged automatic expiry change checked state. The existing locks remain valid through a brief election; explicit committed renewals are permitted.

## Main success path

| Step | Actor / fault | Action | Expected outcome |
|---|---|---|---|
| L-01 | Sofia | Publish `D`, acquire `E_TITLE` | Acked authoritative deck and lock state |
| L-02 | Santiago | Acquire `E_SUBTITLE` | Acked independent lock |
| L-03 | Both client threads | Wait on a barrier, then concurrently submit `update_element` with their own current tokens and `expected_version=1` | Both attempts are genuinely overlapping, regardless of response order |
| L-04 | Fault injector | Kill the **currently identified leader** after the write workers start, while requests are in flight | Some replies may be received; others may be uncertain/time out |
| L-05 | Remaining replicas | Elect a leader with a later Raft term | No two legitimate leaders in the same term; committed log prefix preserved |
| L-06 | Both clients | Retry any uncertain logical request using the **same original request ID**, or read the current authoritative state before deciding what new operation to issue | Already-committed request is not applied twice; an uncommitted request may commit after retry |
| L-07 | Sofia/Santiago | Verify versions and content, then release still-held locks through the current leader | No false stale-token ownership; operations can make progress |
| L-08 | Test harness | Restart failed leader with its durable disk, wait for log catch-up and call `Client.final("get_slide", deck_id=D, slide_id=S1)` | Three replicas converge to the same committed edits, versions, and locks |

**Important timing qualification:** a concurrent command may finish *before* the kill or only *after* the election. The test must assert invariants for all allowed interleavings; it must not demand that both writes necessarily fail, or that both necessarily succeed. Every `ok:true`/`SAVED` result must remain present in the final state unless superseded by a later explicitly committed update.

## Additional contention subcase

In a small separate run or a clearly distinguishable phase (with the lock FREE), Sofia and Santiago simultaneously call `acquire_lock(E_NOTE)` while the leader is failing. A single owner may be granted after retry; the other may get `LOCKED`. If neither commits before the fault, the clients may retry until the new leader commits one acquisition. **It is never acceptable for both sessions to obtain valid conflicting ownership.** Use the same `request_id` for retransmitting a single logical acquisition.

Keep the main graded checker history short enough to avoid its per-key search timeout. If the contention subcase makes histories hard to interpret, record it as a separate targeted history using the same `spec.py`.

## Alternative paths and errors

- **Leader dies after commit but before reply:** client outcome is uncertain; exact-ID retry returns the previously recorded result, not a second update. The replicated deduplication table must survive leader replacement.
- **Leader dies before commit:** the request may be absent from the new committed log; a safely retried logical operation can commit later, subject to its current lock and expected version.
- **Client mistakenly retries with a new ID:** this is a *new* request, not an idempotent retry. The version guard may reject it if the old edit already committed, but this is not a proof of request deduplication.
- **Old leader returns:** after restart, it accepts higher terms, steps down, and catches up. It may not acknowledge an obsolete pending edit as newly committed.
- **UI during election:** while uncertain, drafts are marked `UNVERIFIED`; no "saved" indication is displayed before a committed outcome.

## Acceptance criteria

| ID | Given / When | Then |
|---|---|---|
| US06-AC01 | Two clients issue concurrent updates and leader is killed | Requests may time out, but no uncommitted write is falsely acknowledged |
| US06-AC02 | An update was acknowledged before failure | The new leader preserves it and recovered replicas eventually reflect it |
| US06-AC03 | A command's response was lost | Retry with its original `(client,request_id)` is applied at most once |
| US06-AC04 | Existing locks were committed | Leader failure alone does not forget or arbitrarily reassign them |
| US06-AC05 | Two sessions race for a FREE element during failover | At most one valid owner exists in any committed state |
| US06-AC06 | A new leader is elected | No different replicas legitimately lead the same term, as evidenced by role/term logs |
| US06-AC07 | Former leader restarts with intact disk | Its applied state converges with the new leader and other follower |
| US06-AC08 | Writes and eventual reads are measured | Report the election interruption and latency difference between strong edits and local reads |

## Evidence and starter-specific instructions

- The professor expressly requires concurrent writers with **one thread per client** in the leader-failure test. Sharing a `.jsonl` history through two `Client` instances is supported by `client.py`'s process-level history lock.
- `Client.call` retries with the *same generated request ID within that one call*, but it cannot directly replay a chosen ID across separate calls. For exact idempotency assertions, use a supplemental raw HTTP helper whose request ID is controlled, and record its behavior separately (or add a test-only adapter without editing the starter).
- `chaos.main(["kill", "leader"])` resolves the current leader and fails if none is present. Determine and log the leader before fault injection; synchronize threads and record their start/fault timestamps.
- Run `python3 check.py run/lider.jsonl`; save outputs and `roles.log`. The checker regards `ok:false` pending writes as **may have happened** rather than definitely absent, as required for uncertain outcomes.
- Final `Client.final("get_slide", ...)` occurs after the crashed replica is restarted and fully caught up. Compare only canonical state, not transient current-term fields.

## Definition of done

Both clients actively submit writes while leader failure is induced; a new leader is elected, no acknowledged edit is lost, retries do not double-apply, locks remain exclusive, and all three durable replicas converge after the old leader returns.
