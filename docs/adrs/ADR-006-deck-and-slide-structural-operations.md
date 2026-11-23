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
8. Stage B accepts deck and slide settings only in the initial published snapshot or slide-creation metadata. `update_slide_settings`, deck metadata mutation, and canvas resizing are deferred to Stage C, where their reflow and concurrency semantics will be designed.

## Consequences

- Structural and element commands remain deterministic without serializing unrelated editing behind a deck lock.
- Delayed element operations and stale fencing tokens cannot revive deleted state.
- The client outbox enforces publication and slide-creation prerequisites, and records only unsent descendants as superseded by pending deletion.
- The Stage B DTO registry, sequential model, outbox dependency rules, and tests must incorporate the deck-version precondition and cascade behavior.
