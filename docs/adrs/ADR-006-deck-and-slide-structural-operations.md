# ADR-006 — Versioned Deck and Slide Structural Operations

- **Status:** Accepted
- **Date:** 2026-11-23
- **Scope:** Stage B shared deck and slide structure
- **Related:** ADR-001, ADR-004, ADR-005

## Context

Stage B now includes shared slide creation, deletion, and reordering. These operations affect more than one element and must coexist safely with per-element locks, pending client intentions, and the Raft total order.

## Decision

1. `create_slide`, `delete_slide`, and `reorder_slides` are deterministic Raft state-machine commands in Stage B.
2. The deck has a replicated `deck_version`. Each successful structural command checks `expected_deck_version` during committed application and increments the version once.
3. Structural commands use optimistic concurrency, not a deck-wide lock. Raft supplies their total order; a stale precondition returns a deterministic conflict result.
4. `delete_slide` atomically tombstones the slide and every contained element, clears active descendant locks, and retains descendant fencing counters and tombstones. A subsequent command for a deleted slide or element returns a deterministic deleted/not-found result and cannot resurrect it.
5. If an element update commits before slide deletion, the update is part of ordered history and deletion subsequently removes it. If slide deletion commits first, the update fails. The Raft order is authoritative in either case.
6. `create_slide` carries a client-generated `slide_id`, validated initial metadata, an optional `insert_after_slide_id`, and `expected_deck_version`. No predecessor appends the new slide; a missing or deleted predecessor is rejected deterministically.
7. `reorder_slides` carries the complete permutation of all active slide IDs and `expected_deck_version`. Duplicate, omitted, unknown, or tombstoned IDs are rejected; the state machine never applies a partial reorder.
8. Stage B accepts deck and slide settings only in the initial published snapshot or slide-creation metadata. `update_slide_settings`, deck metadata mutation, and canvas resizing are deferred to Stage D, where their reflow and concurrency semantics will be designed. Stage C is reserved for lock-failure behavior and recovery validation.
9. The replicated slide sequence is the sole authority for slide order. A slide ordinal is derived from its current array position in reads and snapshots; it is not an independently mutable setting and must not diverge after create, reorder, or delete.
10. Stage D settings use separate optimistic revisions: one `deck_settings_version` for name/canvas changes and one `slide_settings_version` per active slide for background changes. Structural `deck_version` retains its existing meaning and is not advanced by settings updates.
11. Canvas resizing changes the shared viewport only. It does not scale, translate, reflow, version, lock, or otherwise mutate elements; signed and off-canvas geometry remains valid state for the renderer/editor to expose and reposition.
12. Settings conflicts are resource-specific committed results: `DECK_SETTINGS_VERSION_CONFLICT` and `SLIDE_SETTINGS_VERSION_CONFLICT`. They return the current canonical settings, current settings revision, and commit-index fence; no new settings ReadIndex operation is required.
13. A client's settings intentions form a FIFO dependency chain only within the same deck-settings or slide-settings resource. Different settings resources remain independent. Existing ancestor checks are retained only for publication, slide existence, and deletion safety.
14. Structural and settings versions advance independently. Slide deletion supersedes only unsent descendant settings intentions; in-flight settings work is resolved by Raft order and cannot revive a deleted slide.
15. Publication array order defines initial Derived Slide Ordinals. Every create, reorder, and delete deterministically renumbers active slides before the new structural state is committed and returned; callers cannot set ordinal through a settings patch.
16. Published decks and slides begin with settings version zero. A successful settings command increments only its target settings version once and returns a Consolidated Settings Result containing the current settings, settings revision, and current structural context.
17. A Settings Version Conflict moves the original client intention to manual reconciliation. The user may discard it or submit a corrected patch with a new request ID and the returned version; the stale request remains terminal audit history.
18. Deck Name is a non-empty string with no separate field-level maximum. The protocol-wide canonical command-size limit remains the effective bound. Canvas dimensions retain their strict integer range of 320 through 3840.

## Consequences

- Structural and element commands remain deterministic without serializing unrelated editing behind a deck lock.
- Delayed element operations and stale fencing tokens cannot revive deleted state.
- The client outbox enforces publication and slide-creation prerequisites, and records only unsent descendants as superseded by pending deletion.
- The Stage B DTO registry, sequential model, outbox dependency rules, and tests must incorporate the deck-version precondition and cascade behavior.
