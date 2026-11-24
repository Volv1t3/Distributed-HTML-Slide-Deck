# ADR-008 — Versioned Deck and Slide Settings

- **Status:** Accepted
- **Date:** 2026-11-23
- **Scope:** Stage D post-publication deck and slide settings
- **Related:** ADR-004, ADR-005, ADR-006; Stage D Settings Implementation Specification

## Context

Stage B made a published presentation structurally editable, and Stage C completed failure behavior for element locks. Deck names, canvas dimensions, and slide backgrounds can still be supplied only in the initial snapshot or slide-creation metadata. Treating these values as structural state would make an unrelated settings edit conflict with slide creation, deletion, or reordering. Treating all settings as one revision would similarly serialize independent changes to different slides.

The existing presentation schema also stores six-digit slide backgrounds while the typed element model uses canonical eight-digit RGBA colors. Slide ordinals are stored even though the replicated slide sequence is the real order, allowing the two representations to diverge.

## Decision

Stage D introduces two linearizable partial-patch commands: `update_deck_settings` for Deck Name and canvas width or height, and `update_slide_settings` for one slide's background. A deck owns one `deck_settings_version`; every active slide owns one independent `slide_settings_version`. Both begin at zero and advance exactly once after a successful command. Structural `deck_version` remains independent.

Settings commands use optimistic concurrency without element locks. A stale revision produces a committed resource-specific conflict containing the current canonical settings, current settings revision, current structural context, and `min_commit_index`. The original client intention becomes terminal manual-reconciliation history. The user may discard it or submit a corrected patch with the returned revision and a new request identity. The system does not automatically merge or coalesce settings patches.

Deck Settings FIFO Lanes and Slide Settings FIFO Lanes order intentions only for the same resource. Independent settings resources may progress concurrently. Publication and slide creation remain existence prerequisites. A pending slide deletion supersedes only unsent settings intentions for that slide; an in-flight settings command is resolved by Raft order and cannot revive deleted state.

The presentation snapshot contract advances to schema version 2. All presentation colors, including slide backgrounds, are canonical uppercase `#RRGGBBAA`. Current DTOs, state, fixtures, reference models, and tests move to the v2 representation together; Stage D does not retain an RGB compatibility or conversion path. The generic command-envelope schema remains version 1.

The replicated slide sequence is the sole ordering authority. Derived Slide Ordinal is computed from array position for snapshots and reads and cannot be patched. Publication, creation, reordering, and deletion return a normalized consolidated order.

Canvas Resize changes the shared viewport only. It preserves element coordinates, dimensions, versions, stacking, and Lock Authority. Off-canvas elements remain valid and must remain discoverable to a later renderer/editor.

Existing Local Eventual Reads expose the new settings and revisions; no settings-specific read endpoint is added. Polling uses `applied_index`, so a settings-only commit is observable even when `deck_version` is unchanged.

Stage D does not add settings authorization. Logical Client Identity and request identity provide replay and deduplication structure, not authentication. Users, authentication, sharing, collaborator permissions, and administrative access control belong to Stage E.

## Consequences

- Unrelated structure, deck settings, and individual slide settings do not create false optimistic-concurrency conflicts.
- Same-resource settings changes retain deterministic FIFO causality, while independent settings resources remain concurrently eligible.
- A settings conflict contains enough committed state for user-directed correction without acquiring an element lock or performing an additional Authoritative Refresh Read.
- Slide order has one representation, eliminating stale persisted ordinals.
- The v2 migration is intentionally repository-wide and breaking for old RGB fixtures or snapshots; compatibility adapters are outside the course-scale baseline.
- Canvas resizing can make existing elements fall outside the viewport, and UI work must expose a recovery interaction rather than mutating replicated geometry.

## Alternatives rejected

- Reusing `deck_version` for all settings: creates conflicts between unrelated structure and presentation settings.
- One deck-wide settings revision: serializes independent slide backgrounds and deck settings.
- Per-field settings versions or CRDT merging: unnecessary complexity for the baseline partial-patch model.
- Persisting caller-supplied ordinals: duplicates the authoritative sequence and permits divergence.
- Automatically scaling elements during Canvas Resize: silently changes independent element state and Lock Authority expectations.
- Accepting both RGB and RGBA indefinitely: retains ambiguous current state and a conversion layer the Stage D migration explicitly removes.
