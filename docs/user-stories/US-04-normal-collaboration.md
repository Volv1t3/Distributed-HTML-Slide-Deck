# US-04 — Normal Collaboration: Publish, Lock, Edit, Release, and Synchronize

- **Status:** Proposed executable acceptance story
- **Related:** Story-01, US-02, US-03, ADR-001
- **Required course scenario:** `normal`
- **Scenario script:** `scenarios/normal.py`
- **History:** `run/normal.jsonl`
- **Faults:** None
- **Actors:** Sofia (owner), Santiago (collaborator), three replicas

## User story

**As Sofia and Santiago**, we want to edit separate elements of a shared slide deck, have exclusive editing rights to selected elements, and see committed modifications synchronize across our local copies, so that we can collaborate without overwriting each other's content.

## Preconditions and setup

1. Launch three healthy, independent replicas and wait until a single stable Raft leader is reported.
2. Sofia has a locally edited `PRIVATE` presentation containing one slide, `S1`, with `E_TITLE` and `E_SUBTITLE` at local version 1. These local private actions do not require distributed locks and are represented by the snapshot in the next step, not by fake Raft commands.
3. Sofia calls **`publish_deck` in the recorded history**, supplying a canonical snapshot and a new `deck_id`. The response confirms successful publication. The shared deck is thereafter cluster-authoritative.
4. Santiago has access to the same presentation (simulated access is sufficient for the current prototype). Both users obtain an initial synchronized snapshot with element identifiers and versions.
5. Use actual UUIDv4 IDs in the test implementation; aliases above are for readability. Avoid unrecorded automatic expiry while checking the standard history.

## Main success path

| Step | Actor | Operation or action | Expected committed/application result |
|---|---|---|---|
| N-01 | Sofia | `publish_deck(D, initial_snapshot)` | Publication committed; `S1` exists with two unlocked elements at v1 |
| N-02 | Sofia | Select `E_TITLE` -> `acquire_lock(D,E_TITLE)` | `LOCK_ACQUIRED`, token `T_TITLE`, version 1 |
| N-03 | Santiago | Select `E_TITLE` -> `acquire_lock(D,E_TITLE)` | `LOCKED`, owner Sofia; no competing token created |
| N-04 | Santiago | Select `E_SUBTITLE` -> `acquire_lock(D,E_SUBTITLE)` | `LOCK_ACQUIRED`, token `T_SUB`, version 1; independent of title lock |
| N-04a | Sofia | `renew_lock(D,E_TITLE,T_TITLE)` while title remains selected | `RENEWED`, `lease_epoch` advances; same fencing token and content version |
| N-05 | Sofia | `update_element(D,E_TITLE,T_TITLE, expected_version=1, patch=text)` | `SAVED`, version 2; title changes only |
| N-06 | Santiago | `update_element(D,E_SUBTITLE,T_SUB, expected_version=1, patch=text)` | `SAVED`, version 2; subtitle changes only |
| N-07 | Santiago | Create a circle -> `create_element(D,S1,E_CIRCLE,shape,properties)` | `CREATED_AND_LOCKED`, circle at version 1 with Santiago's initial lock in one commit |
| N-08 | Santiago | Attempt title edit with no valid title token | `STALE_TOKEN`; title remains v2 |
| N-09 | Sofia | Attempt title update with obsolete `expected_version=1` and current token | `VERSION_CONFLICT`; title remains v2 |
| N-10 | Sofia | Deselect title -> `release_lock(D,E_TITLE,T_TITLE)` | `RELEASED`; title becomes free |
| N-11 | Santiago | Release subtitle and new circle locks | Both `RELEASED` |
| N-12 | Both | Read `get_slide`/`get_element` from local replicas | May temporarily be stale; does not grant editing authority |
| N-13 | Test harness | Wait for follower application; `Client.final("get_slide", deck_id=D, slide_id=S1)` | All three replicas return identical content, element versions, and lock states |

The explicit N-04a renewal is included to exercise the accepted ADR contract without requiring an unrecorded expiration. Keep the scenario short enough (or renew active locks as needed) to prevent automatic expiration from silently changing state during checker evaluation.

## Alternative paths and errors

- A duplicate creation using a **different** request ID but the same `element_id` is rejected; it does not replace `E_CIRCLE` or reset its fencing counter.
- A UI selection is not authoritative until `LOCK_ACQUIRED` commits. The client shows `PENDING_LOCK`, then `EDITING` or `LOCKED_BY_OTHER`.
- Repeated `acquire_lock` with a new request ID by the current owner gives `ALREADY_HELD` and does not change the token; an exact request replay returns its previously recorded result.
- If a `get_slide` shows stale `FREE`, a competing user still cannot update the locked element because backend token checks occur during committed application.
- `create_element` must not appear in any committed intermediate state as created but unlocked.

## Coverage of all implemented application operations

The professor requires the **normal scenario to exercise all implemented application operations**, not just the core path above. The baseline exercises `publish_deck`, `create_element`, `acquire_lock`, `renew_lock`, `update_element`, `release_lock`, `get_element`, and `get_slide`. If the chosen implementation registers additional commands in `spec.GUARANTEE`, extend this normal script to cover each applicable command: `delete_element` (using a disposable shape and its valid lock), `get_deck_preview`, `read_lock_status`, `grant_access`, `update_slide_settings`, `create_slide`, `reorder_slides`, and authorized `force_unlock` if supported. Treat internal `expire_lock` as a separate, instrumented temporal test rather than an ordinary user command. Local-only private editing actions should have their own persistence/unit demonstration because they are not replicated backend commands. Update `spec.py`, the scenario, and the coverage checklist together; do not claim optional unimplemented commands are covered.

## Acceptance criteria

| ID | Given / When | Then |
|---|---|---|
| US04-AC01 | Private edits precede first publication | Private edits require no Raft lock and publication commits a complete snapshot |
| US04-AC02 | Two users attempt the same element | At most one receives new valid ownership; the other gets `LOCKED` |
| US04-AC03 | Users lock different elements | Both can commit independent changes without a slide-wide structural lock |
| US04-AC04 | Current owner supplies matching token and version | Update changes the value exactly once and increments that element's version |
| US04-AC05 | A stale token or old element version is supplied | Update is rejected; committed content and version do not change |
| US04-AC06 | A collaborator creates a new shape | Element creation and initial ownership occur in one committed transition |
| US04-AC07 | Owners release after editing | Lock records are removed while fencing counters remain persistent |
| US04-AC08 | All writes finish and followers catch up | `get_slide` final results match across all three replicas |
| US04-AC09 | Local reads and committed writes are measured in one scenario | Report p50/p95 latency of `get_element` or `get_slide` versus `acquire_lock` or `update_element` |
| US04-AC10 | More application operations are implemented | Every implemented user-facing operation is exercised by the normal scenario; internal timers have separate tests |

## Evidence and checker expectations

- Run `python3 check.py run/normal.jsonl` and save the output. Expect no linearizability violation for the modeled write and lock operations, and convergence for the eventual `get_slide` final readings.
- Use `spec.key(...)=deck_id`; implement in `spec.apply` all tested transitions, their rejection statuses, and canonical return values. All initialization must appear in this history via `publish_deck`.
- Save explicit assertions for the two different v2 elements, circle v1, and no locks after release. A generic checker pass **does not** independently prove UI selection behavior or authorization.
- Include `get_element` local-read calls during the run for cost comparison, but invoke `Client.final()` on **only one eventual read operation** for this deck in this history, preferably `get_slide`.

## Definition of done

A normal collaboration session can be replayed with only the starter and our `app.py`/`spec.py`, the operations obey ADR-001, the three replicas converge, and the checker and cost evidence are archived.
