# Stage D — Deck and Slide Settings Implementation and Stage D.1 Validation Specification

**Status:** Ready for implementation after issue publication
**Date:** 2026-11-23
**Prerequisites:** Completed Stage C and Stage C.1 acceptance evidence; ADR-004, ADR-005, ADR-006, and ADR-008
**Stages covered:** Stage D implementation and Stage D.1 validation

## Problem Statement

The replicated backend can publish and structurally edit decks, safely edit locked elements, preserve offline intentions, and recover from the Stage C failure scenarios. After publication, however, collaborators cannot rename a deck, resize its shared canvas, or change a slide's background through deterministic replicated commands. The presentation schema also retains six-digit slide colors and stored slide ordinals even though typed elements already use RGBA and the replicated slide array is the real ordering authority.

Adding settings through the structural deck revision would make unrelated operations conflict. A background change to one slide must not prevent a rename, a canvas resize, or a background change to another slide. At the same time, two changes to the same resource must be ordered, deduplicated, reconcilable after a conflict, observable through local eventual reads, and recoverable through the existing durable client journal.

## Solution

Stage D adds Raft-committed `update_deck_settings` and `update_slide_settings` partial-patch operations. Deck settings and each slide's settings receive independent optimistic-concurrency revisions, while the existing structural `deck_version` retains its current meaning. Successful results and resource-specific conflicts return consolidated canonical settings so the client can replace its base observation and either discard or manually correct stale work.

The presentation snapshot contract advances to schema version 2. All presentation colors use canonical uppercase `#RRGGBBAA`, and slide order has one authority: the replicated slide sequence. Derived ordinals are produced from array position and are never independently mutable. Canvas resizing changes only the shared viewport and preserves every element and lock.

The durable client journal gains one FIFO settings lane per deck and per slide. Existing local eventual reads and polling expose settings-only commits through `applied_index`. Stage D.1 validates deterministic behavior, retries, failure recovery, convergence, invariant preservation, and latency through the existing in-process command seam and clean three-replica HTTP/Raft/checker harness.

## User Stories

1. As a collaborator, I want to rename a published deck, so that its shared title can evolve after publication.
2. As a collaborator, I want to change the shared canvas width and height, so that the presentation can target a different viewport.
3. As a collaborator, I want one deck-settings request to change any nonempty subset of name, width, and height atomically, so that related settings appear together.
4. As a collaborator, I want to change one slide's background color after publication, so that each slide can have an independent appearance.
5. As a collaborator, I want every presentation color represented as canonical RGBA, so that slide and element color contracts agree.
6. As a collaborator, I want transparent slide backgrounds represented explicitly, so that alpha is not discarded by a legacy RGB field.
7. As a collaborator, I want a canvas resize to preserve all element coordinates and dimensions, so that resizing does not silently rewrite my layout.
8. As a collaborator, I want a canvas resize to preserve element versions and locks, so that a viewport change does not invalidate editing authority.
9. As a collaborator, I want off-canvas elements to remain valid and discoverable, so that I can drag them back into the resized canvas later.
10. As a collaborator, I want slide order to match the replicated slide sequence, so that every replica reports the same order.
11. As a collaborator, I want slide ordinals derived from current sequence position, so that stale stored numbers cannot contradict the deck.
12. As a collaborator, I want publication, slide creation, reordering, and deletion to return normalized order, so that my client immediately observes the authoritative sequence.
13. As a collaborator, I want deck settings to have their own revision, so that structural edits do not create false settings conflicts.
14. As a collaborator, I want each slide to have its own settings revision, so that changes to different slides can succeed independently.
15. As a collaborator, I want one successful settings command to advance only its target revision once, so that retries and concurrent work remain predictable.
16. As a collaborator, I want a stale deck-settings change rejected with the current deck settings, so that I can understand and correct the conflict.
17. As a collaborator, I want a stale slide-settings change rejected with the current slide settings, so that I can reconcile against committed state.
18. As a collaborator, I want settings conflicts to include a commit-index fence, so that the returned reconciliation base is traceable to committed state.
19. As a collaborator, I want to discard a stale settings draft, so that an unwanted local change is not retried indefinitely.
20. As a collaborator, I want to correct and resubmit a stale settings draft with a new request identity, so that reconciliation is an explicit user action.
21. As a collaborator, I want consecutive changes to the same settings resource sent in FIFO order, so that a later local intention uses the predecessor's confirmed revision.
22. As a collaborator, I want changes to independent settings resources to proceed independently, so that one conflict does not block unrelated work.
23. As a collaborator, I want settings intentions persisted before transmission, so that an uncertain response can be retried after a client restart.
24. As a collaborator, I want an exact request retry to return its original result, so that a lost response cannot increment a settings revision twice.
25. As a collaborator, I want request-ID reuse with different settings rejected, so that deduplication cannot apply the wrong mutation.
26. As a collaborator, I want slide settings to wait for the slide's committed creation, so that settings cannot target speculative server state.
27. As a collaborator, I want a pending slide deletion to supersede only unsent settings changes for that slide, so that in-flight work is resolved by Raft order.
28. As a collaborator, I want settings work against a deleted slide to fail deterministically, so that old work cannot recreate deleted state.
29. As a viewer, I want deck preview reads to include deck settings, settings revision, structural revision, ordered slide identifiers, and applied index, so that rendering has one coherent observation.
30. As a viewer, I want slide reads to include its RGBA background, settings revision, derived ordinal, elements, and applied index, so that rendering has complete local state.
31. As a viewer, I want local polling to notice settings-only commits, so that a change appears even when structural `deck_version` is unchanged.
32. As a viewer, I want polling to reject a response older than my current applied observation, so that a lagging replica cannot visibly roll settings backward.
33. As an operator, I want settings commands to use the existing Raft proposal, persistence, and reply path, so that Stage A safety remains authoritative.
34. As an operator, I want deterministic validation to run during committed application, so that every replica reaches the same result.
35. As an operator, I want malformed patches, missing resources, deleted slides, conflicts, and request reuse represented by stable result statuses, so that clients can react consistently.
36. As an operator, I want the presentation snapshot schema version to document the RGBA and settings-revision upgrade, so that persisted state has an explicit contract.
37. As an operator, I want follower restart and catch-up to preserve settings and derived order, so that recovery includes the complete presentation state.
38. As an operator, I want leader failure followed by an exact-ID retry to produce one settings transition, so that failover cannot duplicate an update.
39. As an operator, I want minority partitions unable to acknowledge a settings change, so that settings preserve quorum safety.
40. As an evaluator, I want five independent clean-cluster Stage D.1 histories, so that normal, concurrent, restart, failover, and partition behavior can be reviewed separately.
41. As an evaluator, I want evidence that Canvas Resize preserves geometry, versions, and locks, so that the no-reflow decision is proven.
42. As an evaluator, I want evidence that every reported ordinal equals array position, so that ordering has one authority.
43. As an evaluator, I want settings-write and local-read latency reported together, so that the cost of coordination is visible.

## Implementation Decisions

### Stage boundary

- Stage D owns post-publication Deck Settings and Slide Settings, settings-specific optimistic concurrency, Derived Slide Ordinals, the presentation schema v2 migration, Settings FIFO Lanes, local observation of settings, and settings reconciliation.
- Stage D.1 owns deterministic acceptance, five clean-cluster scenarios, checker/reference-model coverage, invariant evidence, convergence evidence, and latency measurements.
- Stage E owns users, authentication, sharing, collaborator permissions, and administrative access control.
- Themes, speaker notes, transitions, rendering features, and a complete presentation-product feature set remain outside this course baseline.

### Presentation schema v2 and settings state

- Presentation snapshots use schema version 2. The generic command envelope remains schema version 1.
- All current presentation colors are canonical uppercase `#RRGGBBAA`. Six-digit RGB is removed from current DTOs, persisted presentation state, reference models, fixtures, tests, and documentation. Stage D does not provide dual-format acceptance, implicit alpha insertion, down-conversion, or a compatibility adapter.
- Deck Settings contain nonempty Deck Name and `width_px`/`height_px`. Width and height are strict integers in the inclusive range 320 through 3840.
- Deck Name has no field-specific maximum. The protocol-wide canonical command-size limit is its effective bound.
- Slide Settings contain one canonical RGBA background.
- Publication initializes `deck_settings_version` to zero and each active slide's `slide_settings_version` to zero.
- Structural `deck_version`, `deck_settings_version`, and every `slide_settings_version` are separate revision domains.
- The presentation state retains tombstone semantics. Settings updates never revive deleted decks, slides, or elements.

### Derived slide order

- The replicated slide array is the sole authority for order.
- Derived Slide Ordinal is the zero-based array position returned in snapshots and reads. It is not accepted in a settings patch and is not an independently mutable setting.
- Publication preserves supplied slide-list order as the initial order. Create, reorder, and delete deterministically normalize the resulting sequence before commitment and response.
- A caller cannot provide an ordinal that disagrees with sequence position. Any legacy stored ordinal field is removed from authoritative state rather than synchronized as a second source of truth.

### Deck settings command

- `update_deck_settings` accepts `deck_id`, `expected_deck_settings_version`, and a typed partial patch containing at least one of Deck Name, width, or height.
- Unknown fields, an empty patch, booleans masquerading as integers, invalid dimensions, empty names, and an invalid prospective complete object yield `INVALID_ARGUMENT`.
- The deterministic state machine validates the prospective complete Deck Settings object, applies the patch atomically, and increments `deck_settings_version` exactly once.
- Success returns `DECK_SETTINGS_UPDATED` and a Consolidated Settings Result containing current Deck Settings, `deck_settings_version`, current structural `deck_version`, and the command's committed index.
- A stale expected revision returns `DECK_SETTINGS_VERSION_CONFLICT`, current canonical Deck Settings, current `deck_settings_version`, current `deck_version`, and `min_commit_index`.

### Slide settings command

- `update_slide_settings` accepts `deck_id`, `slide_id`, `expected_slide_settings_version`, and a typed partial patch containing the background field.
- Unknown fields, an empty patch, and a noncanonical RGBA value yield `INVALID_ARGUMENT`.
- The deterministic state machine validates the prospective complete Slide Settings object, applies it atomically, and increments only the target `slide_settings_version` once.
- Success returns `SLIDE_SETTINGS_UPDATED` and a Consolidated Settings Result containing current Slide Settings, `slide_settings_version`, current structural `deck_version`, Derived Slide Ordinal, and the committed index.
- A stale expected revision returns `SLIDE_SETTINGS_VERSION_CONFLICT`, current canonical Slide Settings, current `slide_settings_version`, structural context, and `min_commit_index`.
- A missing target returns `NOT_FOUND`; a known tombstoned slide returns `SLIDE_DELETED`. A settings command after deletion never recreates the slide.

### Identity, deduplication, and result semantics

- Settings commands use Logical Client Identity and request ID through existing replicated request-result deduplication. They do not require Editor Session, Fencing Token, or element Lock Authority.
- An exact request retry returns the stored result without another transition. Reusing an identity with a different canonical fingerprint returns `REQUEST_ID_REUSE`.
- Validation and business rejections are deterministic application results and use the established result envelope. Infrastructure failure, timeout, redirect exhaustion, or unavailable quorum remains uncertain and must not be reported as a saved setting.
- Persist-before-reply and the existing Operational Node Log conventions remain mandatory.

### Canvas resize invariant

- Changing canvas width or height changes the viewport only.
- Canvas Resize does not scale, translate, clamp, reflow, version, lock, unlock, renew, release, or otherwise modify elements.
- Signed and off-canvas element geometry remains valid. A future renderer/editor is responsible for keeping off-canvas elements discoverable and draggable back into view.

### Local eventual reads and polling

- No new settings-specific read route is introduced.
- `get_deck_preview` returns presentation schema version, structural `deck_version`, `deck_settings_version`, Deck Name, width, height, ordered slide identifiers, and `applied_index`.
- `get_slide` returns structural `deck_version`, `slide_settings_version`, canonical RGBA background, Derived Slide Ordinal, elements, and `applied_index`.
- Existing polling detects changes by monotonic `applied_index`, not only `deck_version`. A settings-only commit therefore refreshes the appropriate base observation.
- The client never replaces a newer base observation with a response carrying a smaller applied index and never automatically overwrites a pending local draft.

### Durable settings intentions

- Every settings command is durably appended and flushed to the Client Write-Ahead Journal before its first network send.
- Settings intentions use the existing lifecycle vocabulary. Only `COMMITTED` represents a confirmed shared settings change.
- Each deck has one Deck Settings FIFO Lane. Each slide has one Slide Settings FIFO Lane. A later intention in the same lane waits for its predecessor's terminal result and materializes the confirmed settings revision before transmission.
- Different settings resources remain independently eligible; there is no global settings queue and no deck-wide hierarchical scheduler for ordinary settings changes.
- Deck settings require successful publication. Slide settings require successful publication and committed creation of their slide.
- A pending slide deletion supersedes only unsent settings intentions for that slide. In-flight work completes according to Raft order. A committed delete removes the slide regardless of an earlier settings success.
- Settings patches are never automatically merged or coalesced.
- `DECK_SETTINGS_VERSION_CONFLICT` and `SLIDE_SETTINGS_VERSION_CONFLICT` move the original intention to `NEEDS_MANUAL_RECONCILIATION`. The conflict snapshot is the authoritative base; settings reconciliation does not require an element lock or a new ReadIndex path.
- The user may discard the local intention or submit a corrected patch with the returned revision and a fresh request ID. The stale intention remains terminal audit history.

## Testing Decisions

### Primary test seam

The highest existing integration seam remains the client request path through the HTTP runtime, Raft Adapter, Production Raft Core, deterministic presentation state machine, persistence, operational logging, reference model, checker, and fault controls. Settings commands use this seam and do not introduce another replication or transport path.

The client-journal seam remains the existing supplied journal location plus controllable request sender. It validates write-ahead persistence and scheduling while authoritative commands still traverse the primary runtime in integration coverage.

### Good tests

- Assert externally observable command results, settings snapshots, revisions, structural context, applied indexes, journal lifecycle, and final replica convergence.
- Assert invariants across operations: canvas resize preserves elements and locks; every Derived Slide Ordinal equals array position; settings revisions advance independently; exact retries do not advance twice.
- Use deterministic state-machine/reference-model tests for validation and transition semantics, and the live three-replica harness for commitment, retry, failover, partition, recovery, and convergence.
- Reuse existing Stage B client-journal tests for flush-before-send, replay, lifecycle, dependencies, supersession, and manual reconciliation rather than creating a second client persistence seam.
- Do not test private helper layout, incidental JSON ordering, exact thread scheduling, or a renderer that Stage D does not implement.

### Deterministic Stage D acceptance

- Validate schema v2 publication and slide creation with uppercase RGBA, and reject schema v1/current RGB inputs after the migration.
- Validate nonempty allow-listed settings patches and prospective-complete-object validation.
- Validate independent structural, deck-settings, and per-slide settings revisions.
- Validate success and conflict response contracts, commit fences, missing/deleted resources, exact-ID retry, and request-ID reuse.
- Validate publication/create/reorder/delete normalization and Derived Slide Ordinals.
- Validate Canvas Resize preservation of every element field, element version, lock owner, fencing token, and Lease Epoch.
- Validate Local Eventual Read shapes and polling when `applied_index` advances but `deck_version` does not.
- Validate per-resource FIFO dependency materialization, cross-resource independence, publication/creation prerequisites, deletion supersession, replay, discard, and User-Directed Settings Reconciliation.

### Stage D.1 clean-cluster scenarios

1. **Normal settings and ordering:** publish schema v2, rename the deck, resize the canvas, change multiple slide backgrounds, create/reorder/delete slides, and prove consolidated settings, normalized order, and preserved element/lock state.
2. **Concurrent settings:** allow changes to independent resources to commit, race two changes using the same expected revision on one resource, and prove exactly one success plus one reconcilable settings conflict.
3. **Follower failure and restart:** stop one follower, commit deck and slide settings plus a structural order change on the majority, restart with preserved disk, and prove complete catch-up and identical derived order.
4. **Leader failure and retry:** fail the leader while a settings command is pending, retry the exact request identity through the new leader, and prove one settings-version increment and one stored result.
5. **Partition and healing:** isolate a minority, prove it cannot confirm settings, commit majority-side settings and order changes, heal the partition, prove convergence, and prove client polling rejects stale observations.

Each scenario starts with a clean three-replica cluster and archives independently. The scenario runner must not rely on prior scenario state.

### Evidence and measurements

- Archive raw histories, checker input/output, role traces, fault traces, Operational Node Logs, scenario manifest, metrics, and final observations for every replica.
- Extend the sequential reference model with schema v2, settings revisions, settings commands, resource-specific conflicts, and Derived Slide Ordinals.
- Prove Canvas Resize leaves element geometry, content versions, lock ownership, fencing tokens, and Lease Epochs unchanged.
- Prove every returned ordinal equals the zero-based position in the same authoritative slide sequence.
- Report p50/p95 quorum-confirmed settings-write latency and p50/p95 Local Eventual Read latency in the same environment.
- Run the complete regression suite and whitespace validation before the Stage D.1 gate is accepted.

## Out of Scope

- Users, authentication, sharing, collaborator permissions, administrative access control, or malicious-client resistance; these belong to Stage E.
- Themes, reusable templates, speaker notes, transitions, animations, or rendering effects.
- Browser UI implementation, font installation, renderer layout, clipping, zoom behavior, or the concrete off-canvas recovery interface.
- Slide title, speaker metadata, per-slide dimensions, deck themes, or settings beyond Deck Name, shared width/height, and per-slide background.
- Automatic patch merging, CRDT settings, last-writer-wins settings, or per-field settings revisions.
- A new settings ReadIndex endpoint, settings lock, deck-wide lock, or global settings queue.
- Snapshot installation, Raft log compaction, erased-disk reintroduction, dynamic membership, or changing the fixed voter set.
- Backward-compatible runtime acceptance of schema v1 presentation snapshots or six-digit RGB slide colors.

## Further Notes

- Stage D deliberately builds on the Stage B durable outbox and Stage C failure behavior rather than introducing another client or replication subsystem.
- Resource-specific settings versions are concurrency controls, not authorization credentials.
- The conflict result already contains the committed settings reconciliation base. ADR-007's quorum-confirmed Authoritative Refresh Read remains specific to fresh element-lock conflict recovery.
- UI work must eventually expose both discard and corrected-resubmission choices for settings conflicts and must make off-canvas elements recoverable after Canvas Resize.
- The five Stage D.1 scenarios are the Stage Transition Acceptance Gate. Deterministic tests supplement them and do not replace live-cluster evidence.
