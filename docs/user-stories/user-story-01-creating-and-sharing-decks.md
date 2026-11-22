# Story-01 — Creating and Sharing a Collaborative Slide Deck

**Application:** Distributed Slides  
**Actors:** Sofia (presentation owner), Santiago (collaborator), and a three-replica backend cluster  
**Priority:** High  
**Status:** Proposed specification

## User story

As a presentation creator, Sofia wants to create and modify a presentation privately on her computer, then share it with a collaborator so that both users can edit different presentation elements concurrently without accidentally overwriting each other's work.

The application must preserve committed changes despite replica failures, prevent conflicting edits to the same element, and eventually synchronize all replicas and connected clients.

## Preconditions

- Sofia has launched the application.
- The local editor is available, independently of cluster connectivity.
- The backend consists of three replicas: `n1`, `n2`, and `n3`.
- The cluster elects a leader using Raft.
- Sofia and Santiago use distinct client/session identifiers.
- The application uses simulated user identities; production authentication is outside the initial scope.

## Main success scenario

### Phase A — Private presentation creation

1. Sofia opens the application and chooses **Create Blank Presentation**.
2. The application generates a unique `deck_id` and creates an empty presentation in local storage.
3. The presentation enters `PRIVATE` mode.
4. Sofia creates the first slide, adds two circles for decoration, creates a title, and inserts a subtitle.
5. Every created element receives a unique `element_id`.
6. The application saves each modification locally and records it in a local operation journal.
7. The journal assigns each modification a unique identifier consisting of the device ID and a monotonically increasing local counter.
8. Each element also maintains its own local revision number.
9. Sofia can continue editing without requesting locks or contacting the cluster.

**Postcondition:** Sofia owns a locally persisted presentation. No distributed guarantees are required because the presentation has only one authoritative copy.

### Phase B — Publishing the presentation

10. Sofia clicks **Share Presentation**.
11. The application creates a stable snapshot of the locally saved presentation and enters `PUBLISHING` mode.
12. The client submits `publish_deck` to the backend with a unique request identifier.
13. The leader proposes the publication as a Raft log entry.
14. The entry is durably replicated to a majority and committed.
15. The replicated state machine creates the shared presentation using the supplied snapshot.
16. The backend returns the presentation ID, committed publication index, and initial shared state version.
17. Sofia's application changes the presentation mode from `PUBLISHING` to `SHARED`.
18. The client retains its local copy but now treats committed backend state as authoritative.

**Postcondition:** The presentation is available through the replicated backend and is no longer independently authoritative on Sofia's device.

### Phase C — Sharing with Santiago

19. Sofia grants Santiago access to the presentation using `grant_access`, committed through Raft.
20. Santiago opens the invitation from his own application instance.
21. His application requests an initial synchronized snapshot.
22. The backend returns a snapshot containing at least the state committed at the publication index, from a replica that has applied the required entries.
23. Santiago stores the snapshot locally with the corresponding element identifiers and versions.
24. The application displays the slides and begins periodic synchronization.

**Postcondition:** Both users have synchronized working copies of the shared presentation.

### Phase D — Concurrent collaborative editing

25. Santiago selects the subtitle element.
26. Selection highlights the element locally. It does not yet create a distributed lock.
27. Santiago enters editing mode, triggering `acquire_lock`.
28. The leader proposes the lock acquisition to Raft.
29. The replicated state machine checks the authoritative lock record.
30. Because the subtitle is available, the acquisition succeeds and the backend returns a fencing token.
31. Santiago's editor enters `EDITING` mode.
32. Sofia's next synchronization observes the lock and displays a blocking indicator on the subtitle.
33. Sofia can still modify unrelated unlocked elements.
34. Santiago changes the subtitle and submits `update_element`, including his session ID, fencing token, expected element version, and new content.
35. The modification is ordered and committed through Raft.
36. During state-machine application, the current lock token and expected version are validated.
37. The element is updated, and its version increases by one.
38. Santiago receives confirmation that the change was committed.
39. Santiago releases the subtitle lock through `release_lock`.
40. Sofia's application eventually retrieves the updated subtitle and lock state through synchronization.
41. Both users see the same finalized subtitle.

**Postcondition:** The update is preserved by the replicated cluster, the subtitle is unlocked, and both clients eventually converge to the committed content.

## Acceptance criteria

**AC-01 — Private editing:** Given a private presentation, Sofia can create and modify elements without contacting the cluster.

**AC-02 — Local persistence:** Given a private edit, restarting Sofia's application preserves the last locally saved state.

**AC-03 — Atomic publication:** A presentation becomes shared only after its complete initial snapshot has been committed. A failed publication must not expose a partially initialized presentation.

**AC-04 — Stable identity:** Element identifiers remain unchanged when a presentation transitions from private to shared.

**AC-05 — Exclusive editing:** Two sessions attempting to acquire the same unlocked element cannot both receive valid ownership.

**AC-06 — Independent elements:** Locking the title does not prevent another session from editing the subtitle.

**AC-07 — Authorized updates:** An edit succeeds only when its token is current and its expected element version matches the committed version.

**AC-08 — Replication:** A confirmed shared edit survives the failure of any one replica.

**AC-09 — Synchronization:** Once failures and partitions are resolved and new writes stop, every replica and connected client eventually observes the same committed presentation content.

**AC-10 — Network partitions:** A replica without a majority cannot confirm shared writes, lock acquisitions, lock renewals, or lock releases.

**AC-11 — Lock recovery:** An expired or explicitly invalidated lock can eventually be acquired by another session when a majority is available.

**AC-12 — Stale clients:** An edit from a previous lock owner is rejected after the lock has expired or been reassigned.

**AC-13 — Retries:** A repeated request with the same request ID must not apply the same modification twice.

---

# Story-01 — Alternative and Failure Paths

## F-01 — Publication fails before commit

**Given:** Sofia has a private presentation.

**When:** She attempts to publish it while the cluster has no reachable majority.

**Then:** The application keeps the presentation locally and does not confirm the transition to shared mode.

If the request's result is uncertain, the client retries with the same request ID rather than creating a duplicate presentation.

## F-02 — Santiago receives an outdated snapshot

**Given:** The presentation has been published, but Santiago contacts a lagging follower.

**When:** The follower has not applied the required publication index.

**Then:** It must wait, redirect, or reject the initial synchronization request. It cannot claim that an older snapshot is current.

Later preview requests are allowed to return stale data under the application's eventual-consistency guarantee.

## F-03 — Concurrent lock acquisition

**Given:** The subtitle is unlocked.

**When:** Sofia and Santiago simultaneously request the lock.

**Then:** Raft establishes a single command order. The first successful acquisition owns the subtitle, and the competing request receives `LOCKED`.

The result must be the same regardless of which replica initially receives the client requests.

## F-04 — Edit with a stale version

**Given:** Santiago obtained an element snapshot at version 5, but the committed element is now at version 6.

**When:** Santiago submits an edit with `expected_version = 5`.

**Then:** The operation returns `VERSION_CONFLICT` without modifying the shared content.

The client preserves Santiago's local draft and offers to refresh.

## F-05 — Lock owner disconnects

**Given:** Santiago owns a committed lock.

**When:** His connection disappears and renewals stop.

**Then:** The current leader may propose an expiration after the configured inactivity timeout. Expiration becomes effective only when the corresponding state transition is committed.

Another client may acquire the element after expiration is committed.

## F-06 — Network partition isolates the lock owner

**Given:** Sofia owns the title lock.

**When:** Her session becomes isolated on the minority side of a network partition.

**Then:** Her side cannot confirm new edits or renewals.

The majority preserves the committed lock until a valid release, expiration, or authorized invalidation is committed. It may then grant a new lock.

After healing, Sofia's old lock token must not authorize further modifications.

## F-07 — Leader fails during an edit

**Given:** Santiago submits an edit to the leader.

**When:** The leader crashes before Santiago receives a response.

**Then:** Santiago cannot infer whether the edit committed.

He retries using the same request ID after a new leader is elected. The backend returns the recorded result if the operation already committed, or safely processes it if it did not.

## F-08 — A replica restarts without its state

**Given:** A replica has lost its local presentation and log state.

**When:** The replica restarts.

**Then:** It must recover its required log or state from another replica before serving authoritative requests.

If Raft voting metadata has also been erased, safe reintroduction requires additional membership/recovery rules; this is not treated as an ordinary crash restart in the initial system model.

## F-09 — Client goes offline while editing

**Given:** Santiago has made unsaved local edits.

**When:** He loses connectivity to the editing quorum.

**Then:** The editor keeps the pending changes as a local draft and indicates that they are not committed.

When connectivity returns, Santiago must obtain or validate a lock and reconcile his draft against the current version before saving it.

## F-10 — Lock owner attempts a late save

**Given:** Sofia's lock expired and Santiago subsequently acquired the element with a newer token.

**When:** Sofia's delayed save request reaches the current leader.

**Then:** The backend rejects it with `LOCK_EXPIRED` or `INVALID_LOCK_TOKEN`, preserving Santiago's ownership and the current element state.