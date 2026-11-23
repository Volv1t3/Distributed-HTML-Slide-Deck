# Stage B — Distributed Editing Implementation and Stage B.1 Validation Specification

**Status:** Ready for implementation after issue publication  
**Date:** 2026-11-23  
**Prerequisites:** Stage A and Stage A.1 acceptance evidence; ADR-001, ADR-004, ADR-005, and ADR-006  
**Stages covered:** Stage B implementation and Stage B.1 validation

## Problem Statement

The project has a validated Production Raft Core and Raft Adapter, but its replicated application state is still Raft-neutral key/value data. A presentation author cannot yet publish a private deck, create or edit a shared element safely, coordinate concurrent editing, recover local work after a network or client failure, or observe a replica-local rendering view.

The system needs shared presentation editing without weakening Raft authority. Clients must keep working locally during quorum loss, but they are not voters and must never report a local draft as a confirmed shared save. Replicas must deterministically validate the same presentation command, apply the same state transition, retain deduplication results, and return the same result after leader changes or retries.

## Solution

Stage B replaces the Raft-Neutral State Machine with a deterministic shared-presentation state machine while leaving Raft election, replication, membership, transport identity validation, persistence ordering, and commitment rules unchanged. It implements publication, structural slide operations, element lifecycle operations, element-level fencing locks, eventual local reads, authoritative conflict refresh, and replicated request-result deduplication.

A client-local write-ahead intention journal and hierarchical resource index preserve offline work without becoming a second replicated log. The client records each shared intention before sending it, polls replica-local state over the existing HTTP path, tracks pending/superseded/final intentions, and requires user-directed reconciliation for conflicts.

Stage B.1 validates the completed behavior through deterministic state-machine tests, the existing live three-replica runtime, the existing checker/history seam, fault injection, and client-journal recovery scenarios.

## User Stories

1. As a presentation author, I want to publish a complete private deck atomically, so that collaborators receive one authoritative shared starting state.
2. As a presentation author, I want publication retries to return the original result, so that a lost response cannot publish two decks.
3. As a presentation author, I want later private edits to remain durable while publication waits for quorum, so that I can keep working without claiming the deck is shared already.
4. As a collaborator, I want to create a slide at a chosen position, so that I can extend a shared presentation safely.
5. As a collaborator, I want to reorder all active slides deterministically, so that every replica presents the same slide sequence.
6. As a collaborator, I want to delete a slide and its contents atomically, so that no delayed element edit can restore deleted content.
7. As a collaborator, I want to create a text or shape element and receive its first lock in the same confirmed operation, so that another editor cannot take it between creation and selection.
8. As a collaborator, I want to create headings, subtitles, text boxes, rectangles, squares, circles, triangles, regular polygons, and irregular polygons, so that the baseline supports practical slide authoring.
9. As a collaborator, I want my chosen system fonts and a generic fallback stored with text, so that rendering preserves my preference without requiring every device to install the same font.
10. As a collaborator, I want deterministic coordinates, transforms, colors, and stacking order, so that all replicas render the same committed presentation state.
11. As a collaborator, I want to acquire an exclusive element lock before a shared edit, so that concurrent editors cannot both confirm incompatible modifications.
12. As a collaborator, I want one update to atomically change all selected element fields, so that a coherent edit has one version transition rather than independent field races.
13. As a collaborator, I want stale tokens and stale versions rejected deterministically, so that an old editor cannot overwrite newer shared work.
14. As a collaborator, I want to release a lock explicitly, so that another editor can acquire it without waiting for the later Stage C expiration mechanism.
15. As a collaborator, I want to delete an element safely, so that its identifier and prior fencing tokens cannot be reused to resurrect it.
16. As a viewer, I want to read a deck, slide, element, and lock observation from a local replica, so that rendering remains available while replicas converge.
17. As a client, I want each eventual read to include the replica applied index, so that I can avoid visibly replacing a newer observation with a lagging replica response.
18. As a client with a version conflict, I want an authoritative refresh at or beyond the conflict index, so that I reconcile against state known to include the conflicting commit.
19. As a client with pending edits, I want polling to preserve my draft when remote state changes, so that it is never silently overwritten or automatically merged.
20. As a client, I want to discard a conflicting draft or manually correct it, so that a replacement edit is intentional and uses fresh lock and version preconditions.
21. As a client that loses quorum, I want my shared-edit intention flushed locally before network send, so that I can retry it after a crash or lost response.
22. As a client that restarts, I want my durable intention journal replayed into its local scheduling state, so that pending work and final audit history survive restart.
23. As a client, I want independent local intentions to proceed without a global FIFO, so that unrelated work is not needlessly blocked.
24. As a client, I want a pending slide deletion to supersede only unsent child intentions, so that in-flight operations are resolved by Raft rather than assumed cancelled.
25. As a client, I want an offline element creation to appear as pending but wait for its authoritative first lock before dependent updates are sent, so that local drafts never invent a fencing token.
26. As an operator, I want all confirmed results to survive leader failure and retry, so that Stage B preserves Stage A durability and deduplication guarantees.
27. As an operator, I want application validation failures in the per-node Operational Node Log, so that malformed or rejected requests can be investigated without confusing diagnostics with the Raft command log.
28. As a course evaluator, I want Stage B.1 evidence from the real three-replica runtime and client-journal recovery paths, so that correctness is demonstrated above isolated unit tests.

## Implementation Decisions

### Stage boundary

- Stage B owns shared deck publication; shared slide create/delete/reorder; element create/update/delete; acquire/release element locks; eventual local reads; authoritative conflict refresh; presentation DTO validation; replicated request-result deduplication; client polling; and the durable client write-ahead journal.
- Stage B.1 owns acceptance evidence and fault validation, not a new product behavior.
- Stage C owns automatic lock renewal and leader-proposed expiration, plus post-publication deck and slide settings updates, including background changes, metadata changes, and canvas resizing.
- The Production Raft Core remains application-neutral. No Stage B work changes Raft membership, election rules, AppendEntries behavior, commitment rules, transport identity validation, or the persist-before-reply rule.

### Shared-presentation state and command model

- Shared presentation state stores decks, active and tombstoned slides, active and tombstoned elements, slide order, `deck_version`, element versions, active locks, retained fencing counters, immutable element creation sequence, and replicated request-result records.
- `publish_deck` atomically imports one validated private snapshot only when its `deck_id` is absent. It does not import client lock state, fencing counters, or a client claim of authority. An exact retry returns the stored result; a different request for the same identifier returns `DECK_ALREADY_PUBLISHED` and never replaces state.
- `create_slide`, `delete_slide`, and `reorder_slides` are linearizable structural commands with `expected_deck_version`. A successful structural command increments `deck_version` once. There is no deck-wide lock.
- A slide creation supplies a client-generated slide identifier, validated initial metadata, optional predecessor, and expected deck version. An omitted predecessor appends; a missing or deleted predecessor is rejected.
- Reordering supplies a complete permutation of active slide identifiers. Duplicate, omitted, unknown, and tombstoned identifiers are rejected; partial reorder semantics do not exist.
- Deleting a slide atomically tombstones the slide and each descendant element, clears every active descendant lock, and retains tombstones and fencing counters. Raft order determines whether a concurrent element update is applied before deletion or rejected after deletion.
- `create_element` atomically validates the target active deck and slide, checks permanent element-ID uniqueness, creates version one, allocates the first fencing token, and grants the initial lock to the editor session. It never exposes a committed created-but-unlocked element.
- `acquire_lock`, `update_element`, `release_lock`, and `delete_element` validate state-dependent preconditions only while applying the committed command. `delete_element` requires the current token and expected version, then tombstones the element, increments its version, removes its active lock, and retains its fencing counter.
- An element update is one Atomic Element Patch. It validates the prospective whole typed element, requires a valid session/token and expected version, applies once, and increments the element version once. Stage B does not implement field-level CRDT or last-writer-wins semantics inside a locked element.
- Command deduplication is keyed by Logical Client Identity and request ID. The stored canonical fingerprint includes operation and canonical arguments. Reusing an ID with different content yields `REQUEST_ID_REUSE`; exact retries return the stored result without another mutation.
- Logical Client Identity is stable for journal ownership and deduplication. Editor Session is a separate short-lived lock owner. A new editor session cannot inherit an older session's fencing token.

### Typed deterministic presentation model

- All presentation data uses schema-versioned, allow-listed typed blocks and patches. It never accepts arbitrary HTML, CSS, SVG, executable content, or an untyped property map.
- Supported baseline element kinds are `heading`, `subtitle`, `text_box`, `rectangle`, `square`, `circle`, `triangle`, `regular_polygon`, and `irregular_polygon`.
- Text carries content, Font Stack, non-negative integer size, text decorations/styles, alignment/justification, non-negative integer spacing and indentation, and explicit formatting fields. Font Stack stores preferred system family names in order plus one required generic fallback. Font installation is determined by the renderer; fallback is reported to the user but never changes replicated state.
- Shapes carry geometry, scale, rotation, Canonical RGBA Color fills and borders, border style, and non-negative integer border thickness. Color is canonical `#RRGGBBAA`, never an arbitrary CSS expression. Border style is a bounded enumeration.
- Geometry uses fixed-point integers: centipixels for coordinates and dimensions, centidegrees for rotation normalized to `[0, 36000)`, and basis points for scale. `x` and `y` are signed; positive `x` moves right and positive `y` moves down from the top-left slide origin. Width and height are positive; scale, font size, spacing, indentation, and border thickness are non-negative. Off-canvas position is valid state and rendering determines clipping.
- Squares and circles require equal width and height. Triangle is a regular polygon with three sides. Regular polygons use a bounded valid side count; irregular polygons use a bounded ordered local-vertex list. Freeform paths are not part of the baseline.
- Every element has a signed `z_index`. Render order sorts by `z_index`, then the immutable creation sequence assigned at committed creation. `z_index` changes only through a protected element patch.
- The protocol has an overall canonical command-size limit and bounded text/block/vertex collections, even where valid numeric editor values have no application-level upper bound.

### Reads, conflict recovery, and polling

- `get_deck_preview`, `get_slide`, `get_element`, and `read_lock_status` are Local Eventual Reads served from a replica's applied presentation state. They return observation metadata including `applied_index`, relevant versions, and lock state; they never grant editing authority.
- A client polls a configurable local replica every second by default. It first obtains a deck preview and then fetches only changed slides or elements. The client retains the highest observed applied index for its authoritative base projection and does not replace that base with a response from a lagging replica.
- A committed `VERSION_CONFLICT` includes `current_version` and `min_commit_index`. Recovery first commits a fresh lock acquisition, then uses an Authoritative Refresh Read routed to the leader. The read waits until the leader has applied at least the greater of the conflict's `min_commit_index` and the lock-acquisition commit index, then returns current element content, version, and lock/fencing state.
- Polling never automatically overwrites or merges a pending local draft. The UI permits discard or User-Directed Draft Reconciliation. A corrected save is a new request after the fresh lock and bounded authoritative refresh, with the returned current version as expected version.

### Durable intention journal

- Every shared command is durably appended and flushed to a per-Logical-Client Client Write-Ahead Journal before its first network send. The journal is separate from replica persistence and does not reuse Raft disk state.
- The baseline uses append-only journal records and reconstructs the Hierarchical Resource Index and speculative projection by replay on startup. Automatic compaction or deletion is out of scope; terminal records remain audit history.
- Journal lifecycle states are `PENDING_SYNC`, `IN_FLIGHT`, `COMMITTED`, `REJECTED_FINAL`, `SUPERSEDED`, and `NEEDS_MANUAL_RECONCILIATION`. Only `COMMITTED` represents an authoritative shared result.
- A transport failure or `QUORUM_UNAVAILABLE` outcome is uncertain and returns the original record to `PENDING_SYNC` with its original request ID. A deterministic committed application result is final and recorded exactly once.
- Element intentions require successful publication and successful containing-slide creation. Offline creation uses a client-generated element UUID and may appear in the speculative projection as Pending Element Creation, but dependent updates wait for `CREATED_AND_LOCKED` and its authoritative token.
- Destructive intentions may supersede only unsent descendants. A pending slide deletion marks the slide pending deletion and records unsent descendant element operations as superseded. An in-flight descendant is never assumed cancelled; its committed result is reconciled, then a committed deletion removes visible state.

### Result and error conventions

- Application-level validation and business rejections return `ok: true` with deterministic status data and are persisted for deduplication. Infrastructure failures, unavailable quorum, and uncertain response paths return `ok: false` or a timeout and must not be reported as saved.
- Relevant statuses include `CREATED_AND_LOCKED`, `LOCK_ACQUIRED`, `ALREADY_HELD`, `LOCKED`, `SAVED`, `RELEASED`, `DELETED`, `NOT_FOUND`, `SLIDE_DELETED`, `VERSION_CONFLICT`, `STALE_TOKEN`, `NOT_OWNER`, `REQUEST_ID_REUSE`, `DECK_ALREADY_PUBLISHED`, `STRUCTURE_VERSION_CONFLICT`, and `INVALID_ARGUMENT`.
- Every committed successful or deterministic rejected command follows persist-before-reply, including durable replicated request-result recording. Operational failures and validation information are written to the local Operational Node Log with contextual identifiers and payload fingerprints according to Logging Mode.

## Testing Decisions

### Test seam

The primary integration seam is the existing client request path through the unchanged HTTP runtime into the Raft Adapter, Production Raft Core, deterministic state machine, checker histories, fault controls, and operational archives. Stage B must not introduce another consensus path or a replacement HTTP runtime.

The only new seam is the client-local journal interface above that existing request path. It must be testable with a supplied journal location and a controllable request sender, while all authoritative operations still traverse the primary runtime seam.

### Good tests

- Test externally observable command results, replicated state, returned versions/tokens/indexes, journal lifecycle records, and final convergence—not private helper calls or incidental storage layout.
- Use the sequential reference model for deterministic operation behavior and the three-replica runtime for commitment, failover, partition, redirect, and recovery behavior.
- Preserve the existing convention that `ok: false` is uncertain, while a deterministic `ok: true` application status can be modeled and deduplicated.

### Stage B deterministic acceptance

- Validate publication idempotency, existing-deck rejection, and no imported client lock authority.
- Validate all shape/text contracts, canonical values, forbidden arbitrary payloads, geometry/shape invariants, complete typed patches, stacking tie-breaks, and command-size/collection limits.
- Validate atomic create-and-lock, competing acquisition, same-session repeated acquisition, stale token rejection, version conflict, release, deletion tombstones, and request-ID reuse.
- Validate structural expected-version conflicts, positioned slide creation, complete reorder permutation checks, deletion cascade, lock invalidation, and late descendant-command rejection.
- Validate local eventual-read result metadata and authoritative refresh index behavior.
- Validate journal write-ahead persistence, replay, exact-ID retry, lifecycle transitions, dependencies, semantic supersession, pending creation, manual reconciliation, and no automatic merge/authority claim.

### Stage B.1 live acceptance

- Run independent clean-cluster scenarios for normal publication/editing, competing locks, distinct-element concurrency, structural edits, duplicate retry, follower failure/rejoin, leader failure during a pending command, minority partition write rejection, majority-side progress, healing/convergence, and ordinary preserved-disk restart/catch-up.
- Run client-journal scenarios for a response lost after send, client restart before response, offline pending creation, pending-slide deletion supersession, polling from a lagging replica, and conflict refresh followed by user-directed correction.
- Use the existing checker histories for linearizable commands and convergence checks for eventual reads. Extend the sequential reference model only as necessary to represent deterministic Stage B state; do not fabricate internal journal transitions as server history.
- Archive histories, checker results, role traces, and Operational Node Logs using the existing archive workflow. Add machine-readable client-journal evidence beside the scenario evidence.
- Measure and report p50/p95 latency for a representative quorum-confirmed edit or lock operation against a local eventual read in the same environment, plus observed unavailability during election or partition.

## Out of Scope

- Automatic `renew_lock`, leader-proposed `expire_lock`, lease timing, and related race handling.
- Post-publication deck or slide settings updates, including background, canvas resizing, and deck metadata changes.
- Dynamic Raft membership, joint consensus, erased-disk automatic rejoin, snapshots/InstallSnapshot, and changes to Stage A protocol safety mechanisms.
- Field-level CRDTs, last-writer-wins merge, automatic conflict merge, vector clocks, and treating clients as voters or replicas.
- Arbitrary HTML, CSS, SVG, images, custom font distribution, animations, freeform paths, gradients, shadows, filters, and embedded executable content.
- A browser/editor UI, WebSockets, push subscriptions, authentication/authorization, and a replacement HTTP framework.
- Journal compaction, automatic deletion, fairness optimization, and global scheduling order beyond the accepted dependency-aware journal policy.

## Further Notes

- This specification supersedes the narrower Stage B presentation reservation in the Stage A document where ADR-005 and ADR-006 make a more specific decision.
- Stage B must be implemented as new state-machine handlers, DTO validators, adapter routing, reference-model operations, and client-local journal components—not as a second Raft implementation.
- The implementation must preserve the existing starter runtime and Stage A operational diagnostics. Existing local worktree changes unrelated to Stage B remain outside the scope of the implementation.
- Stage B.1 evidence is required before Stage C design or implementation begins.
