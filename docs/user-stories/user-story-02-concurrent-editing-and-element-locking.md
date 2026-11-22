# User Story 02 — Concurrent Editing and Element Locking

- **ID:** US-02
- **Project:** Distributed Slides
- **Status:** Ready for implementation planning; not yet implemented
- **Priority:** Core
- **Actors:** Sofia, Santiago, their respective editor sessions, and a three-replica Raft cluster
- **Depends on:** US-01 (a deck has been published and initially synchronized); [ADR-001 — Element-Level Distributed Locking](ADR-001-element-level-distributed-locking.md)
- **Related ADR tests:** L-01–L-05, L-08–L-09, L-15

## 1. User story

**As a collaborator** on a shared presentation, I want to select and edit an element without another collaborator overwriting my changes, while still allowing others to modify separate elements at the same time, **so that** our work remains consistent without locking an entire slide or presentation.

The collaboration model is **exclusive, per-element, selection-triggered editing**. It does **not** attempt simultaneous character-level editing within one text box.

## 2. Preconditions and scope

1. Sofia created a deck in `PRIVATE` mode and completed the US-01 publication flow. The deck is now `SHARED` and the Raft cluster is authoritative.
2. Sofia and Santiago have different logical `session_id` values. Real authentication and authorization are outside the initial prototype.
3. Both clients have fetched an initial deck snapshot including the committed publication; afterward, their ordinary previews may be stale.
4. The deck contains a title E1 and subtitle E2. Both are initially unlocked; each has an independently tracked content `version`.
5. The Raft cluster has three independent replicas, and at least two are available to commit commands.
6. Shared editor controls support text and basic shape properties (text, style, position, size); HTML code execution and real-time character merges are not in scope.

## 3. Main success scenario — independent editing

1. Sofia selects **E1 (title)**. Selection enters `PENDING_LOCK`, and the client sends `acquire_lock` for `(deck_id, E1)` with her logical session and a unique `request_id`.
2. The leader proposes the acquisition through Raft. The replicated state machine checks that E1 exists, has not been deleted, and has no active lock.
3. Once the entry is committed and applied, Sofia receives `LOCK_ACQUIRED` with a `fencing_token` and the current E1 `version`.
4. Sofia's editor enters `EDITING` and permits her to modify E1 locally. Until a save succeeds, her changes are only an unsaved local draft.
5. Santiago's next preview/lock-status poll shows that E1 is held by Sofia. His client displays a locked/read-only indicator for E1. Delayed UI propagation does not grant him permission to edit.
6. Santiago independently selects **E2 (subtitle)** and sends `acquire_lock` for E2. Because locking is per element, he receives a separate `LOCK_ACQUIRED` and can edit E2 while Sofia edits E1.
7. Sofia submits `update_element(E1, fencing_token, expected_version, patch)`. The command commits through Raft. When applied, the server checks the current owner, fencing token, and expected version in one state-machine transition.
8. The successful edit changes E1's content and increments **E1's version only**. Sofia receives `SAVED` with the new version.
9. Santiago saves E2 through the same process, incrementing **E2's version only**.
10. Each client eventually sees both committed changes through ordinary local replica reads/synchronization.
11. When either user deselects an element, switches focus, or closes the editor normally, the client sends `release_lock` using its current token.
12. Each valid release is committed through Raft. The element becomes `FREE` while its fencing counter and content version remain stored.

**Success postcondition:** E1 and E2 contain the committed edits, each with an independent content version; the users' releases have committed; all healthy replicas eventually apply the same state.

## 4. Concurrent acquisition of the same element

1. E1 is initially `FREE`.
2. Sofia and Santiago select E1 at approximately the same time and independently request `acquire_lock`.
3. Raft commits an order for the two acquisitions. No client ID, device name, timestamp, or UUID lexical comparison chooses the winner.
4. The first applicable command changes E1 from `FREE` to `HELD` and returns `LOCK_ACQUIRED`.
5. The other command observes `HELD` and returns the application-level rejection `LOCKED` with the current owner, if available.
6. The losing client remains `READ_ONLY` for E1, but may select and edit E2.
7. After the owner releases E1 and that release commits, the other client may attempt a fresh acquisition with a **new** `request_id` and receive a **higher** fencing token.

**Required invariant:** At most one current lock owner exists for `(deck_id, E1)` in any committed state.

## 5. Creating an element while collaborating

Creating an element is **not** a two-command `create`-then-`lock` workflow and **does not** require a slide-wide structural lock.

1. Santiago chooses **Add text** or **Add shape** on a shared slide.
2. His editor assigns a new, complete UUIDv4 `element_id` and shows a local `PENDING_CREATE` element.
3. The client sends `create_element(deck_id, slide_id, element_id, type, properties)` with its `session_id` and a stable unique `request_id`.
4. One committed Raft entry checks that the deck/slide exists and that the element ID has never been used; it creates the element with `version = 1` **and** creates Santiago's initial exclusive lock with `fencing_token = 1` and `lease_epoch = 0`.
5. The server returns `CREATED_AND_LOCKED`. Santiago's new element is immediately editable under the returned lock token.
6. Sofia eventually sees the new element and its locked status through synchronization. A stale view cannot authorize competing changes.
7. If Santiago deselected the draft while the request was pending, his client must issue `release_lock` when the successful creation result arrives.

Concurrent creation of distinct elements is allowed. Raft orders the insertions; a future explicit stacking/reordering operation would be specified separately. Deleted element IDs are not reused.

## 6. Operation and consistency contract

| Operation | Purpose | Consistency guarantee | Mechanism / authorization | Expected response |
|---|---|---|---|---|
| `create_element` | Add a new shared text/shape and select it | Linearizable | One Raft commit creates the object and its initial lock | `CREATED_AND_LOCKED` |
| `acquire_lock` | Request exclusive editing authority for an existing element | Linearizable | Raft + committed atomic `FREE` check | `LOCK_ACQUIRED`, `LOCKED`, or `ALREADY_HELD` |
| `update_element` | Save text/style/position/size | Linearizable | Raft + owner/token/version checks at application | `SAVED`, `STALE_TOKEN`, `VERSION_CONFLICT` |
| `release_lock` | Give up authority on deselection | Linearizable | Raft + matching owner/token | `RELEASED` or `STALE_TOKEN` |
| `delete_element` | Delete a selected element | Linearizable | Raft + matching lock and version; durable tombstone | `DELETED` or an application rejection |
| `get_element` | Read current displayed element and version | Eventual consistency | Read locally applied replica state | Possibly stale value/version |
| `get_slide` | Display slide and its elements | Eventual consistency | Read locally applied replica state | Possibly stale slide |
| `read_lock_status` | Display currently known owner/lock state | Eventual consistency | Local replica's applied lock map | Possibly stale UI status; not permission |

All commands use stable `request_id` values for retry deduplication. The distributed ordering is the committed Raft log index/term, not request ID order. Content `version` and fencing token are separate counters with separate meanings.

## 7. Alternative and error paths

### US02-A1 — Selection is rejected because another session owns the element

**Given** E1 has a committed lock held by Sofia. **When** Santiago requests its lock. **Then** the request returns `LOCKED`; Santiago sees a non-editable overlay and can keep reading E1 or edit another element. A UI poll showing `FREE` does not override the rejection.

### US02-A2 — The same session selects an element it already owns

**Given** Sofia owns E1. **When** she sends a new `acquire_lock` request for E1. **Then** the server returns `ALREADY_HELD` with the existing token and does **not** increase fencing or renew the lock. Retrying the original request ID returns its recorded result.

### US02-A3 — An update uses the wrong or stale lock token

**Given** Sofia does not hold the current lock. **When** she attempts `update_element` or `release_lock` with a previous token. **Then** the server returns `STALE_TOKEN` without modifying the element, current lock owner, or content version.

### US02-A4 — An update uses an outdated element version

**Given** the authoritative version is 6 and Santiago submits `expected_version = 5`, even with a currently valid lock. **When** the edit is applied. **Then** it returns `VERSION_CONFLICT`; the editor retains the local draft and offers a refresh/reconciliation action. The committed element remains unchanged.

### US02-A5 — A request is retried after an uncertain response

**Given** a save or creation might have committed but the response was lost. **When** the client retries with the **same** `request_id` and identical command. **Then** the recorded result is returned and no second mutation occurs. Reusing the ID for a different command returns `REQUEST_ID_REUSE`.

### US02-A6 — Deselect happens before lock acquisition finishes

**Given** `acquire_lock` or `create_element` is pending. **When** the user moves focus elsewhere. **Then** the UI may change selection immediately; if the pending request eventually succeeds, the client releases the obtained token. A failed/uncertain response must not be treated as proof that no lock exists; abandonment is ultimately handled by the expiration process in US-03.

### US02-A7 — Connection drops during editing

**Given** a user has unsaved local changes. **When** the client can no longer confirm ownership or save with the cluster. **Then** editing becomes `UNVERIFIED`; the client preserves a local draft and does not show `SAVED`. Reacquisition/reconciliation is defined by US-03.

### US02-A8 — Duplicate element ID or invalid patch

**Given** an `element_id` already exists, including as a tombstone, or a patch contains unsupported fields/types. **When** the operation is processed. **Then** it yields an application-level rejection (`INVALID_ARGUMENT` or an explicitly defined duplicate-ID result) without a partial insertion or edit.

## 8. UI requirements

| UI state | Entry condition | Allowed action | Restricted action |
|---|---|---|---|
| `READ_ONLY` | Not selected; or another owner holds lock | View and select | Confirm shared modification |
| `PENDING_LOCK` | Waiting for `acquire_lock` | Show selection/loading; optionally draft locally | Claim a successful save |
| `EDITING` | `LOCK_ACQUIRED` or `CREATED_AND_LOCKED` committed | Edit locally, submit update, renew, release | Edit other locked resources without their own token |
| `LOCKED_BY_OTHER` | Lock acquisition rejected or status observed | View, retry later, select other element | Update that resource |
| `PENDING_CREATE` | New local shape/text not yet committed | Render as provisional draft | Claim globally shared existence |
| `UNVERIFIED` | Connectivity lost or renewal/commit uncertain | Retain unsaved draft | Claim current lock ownership or committed content |

The UI initially uses periodic polling; it must not assume instant notifications to all clients. Lock ownership is enforced in the replicated backend, not in browser controls.

## 9. Acceptance criteria (Given / When / Then)

| ID | Given | When | Then | ADR mapping |
|---|---|---|---|---|
| US02-AC01 | E1 is free | Sofia and Santiago acquire E1 concurrently | Exactly one gets a new valid lock; the other is rejected | L-01 |
| US02-AC02 | E1 and E2 are free | Sofia selects E1, Santiago selects E2 | Both acquire and edit independently | L-02 |
| US02-AC03 | Shared slide exists | Santiago creates E3 | E3 is created at version 1 and locked by Santiago in one commit | L-03 |
| US02-AC04 | Sofia owns E1 | Santiago attempts an update without Sofia's token | No committed change; `STALE_TOKEN` | L-04 |
| US02-AC05 | E1 is at version 6 | Owner submits `expected_version=5` | `VERSION_CONFLICT` and no mutation | L-05 |
| US02-AC06 | E1 changes owner | Prior owner's release arrives late | New owner's lock is not released | L-08 |
| US02-AC07 | Request response is lost | Client retries unchanged `request_id` | Same result; only one logical mutation | L-09 |
| US02-AC08 | Owner deselects E1 | Release is delayed/lost | UI does not falsely report confirmed release; US-03 can expire lock | L-15 |
| US02-AC09 | User sees stale `FREE` preview | Tries to edit without a committed lock | Authoritative update is rejected | ADR invariant 3 |
| US02-AC10 | Owner saves E1 while other edits E2 | Healthy clients synchronize | Both see the same committed versions/content eventually | ADR invariant 9 |

## 10. Suggested demonstration and tests

1. Create a shared deck with E1 and E2.
2. Launch Sofia and Santiago as separate client sessions.
3. Concurrently request E1; verify one winner and one `LOCKED` response.
4. While Sofia holds E1, allow Santiago to acquire E2 and successfully save.
5. Attempt a stale token edit, an outdated version edit, and a duplicate request ID retry; check that each has the specified outcome.
6. Create E3 and verify its initial lock appears in the **same** committed transition.
7. Release locks and read each replica's final state; check convergence.
8. Compare latency for Raft-committed `acquire_lock` or `update_element` versus a local eventual `get_element` read.

Use the starter's `client.py` histories and `check.py` where its deterministic state model applies; build `spec.py` around a complete shared deck key so element and lock changes are evaluated together.

## 11. Out of scope and implementation notes

- No slide-wide structural lock for normal element creation.
- No LWW CRDT conflict merge for authoritative shared content: the Raft order and element versions govern writes.
- No real-time per-character operational transformation, cursor co-editing, or WebSocket requirement.
- No authentication scheme or production security claims attached to simulated session IDs.
- No automatic lock expiration logic in this story; US-03 governs that feature and its failures.
- The `create_element` behavior in ADR-001 supersedes the preliminary structural-lock wording in `project-proposal.md`.

**Definition of done:** All US02 acceptance criteria pass against a deterministic state-machine implementation and then against Raft-backed operations; UI states match the backend outcome, and concurrent edits of separate elements remain possible.
