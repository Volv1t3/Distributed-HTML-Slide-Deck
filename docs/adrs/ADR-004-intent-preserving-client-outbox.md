# ADR-004 — Intent-Preserving Client Outbox

- **Status:** Accepted
- **Date:** 2026-11-22
- **Scope:** Stage B offline client drafts, reconciliation, and synchronization scheduling
- **Related:** ADR-001; ADR-003; project proposal

## Context

Shared edits require quorum commitment, but a client may temporarily be unable to reach a quorum. The user must be able to continue editing a local draft without treating the client as a Raft voter or claiming an authoritative commit. A simple global FIFO unnecessarily blocks independent work, while vector clocks would duplicate ordering machinery that Raft already provides for replicas.

## Decision

The client maintains a durable sequential journal of validated local intentions and a hierarchical resource index for deck, slide, and element scopes. Raft remains the only authoritative cross-client ordering mechanism.

Before enqueue, local structural validation rejects an edit whose projected deck, slide, or element is missing or pending deletion. The scheduler recognizes four operation-specific relationships: independent work may proceed independently, result prerequisites wait for their required result, destructive work may supersede unsent descendant work when safe, and invalid descendants never enter synchronization.

The outbox preserves safe final editing intent rather than requiring every local journal entry to appear in exact order in the Raft log. A superseded intention remains auditable with its reason. In-flight intentions are never presumed cancelled; their committed result is reconciled.

When quorum is unavailable, a draft is `PENDING_SYNC`, not a shared save. After a `VERSION_CONFLICT`, the original request remains final; the client obtains an authoritative snapshot at or after the response's `min_commit_index`, acquires a fresh committed lock, allows manual correction, then submits a new request with fresh preconditions and a new request ID.

Every shared intention is appended and durably flushed to the client-local journal before its first network send. This Client Write-Ahead Journal enables exact-ID retry after a crash or a lost response, but it neither commits the operation nor makes the client a Raft replica. Intentions use `PENDING_SYNC`, `IN_FLIGHT`, `COMMITTED`, `REJECTED_FINAL`, `SUPERSEDED`, or `NEEDS_MANUAL_RECONCILIATION`; only `COMMITTED` is authoritative.

An offline creation allocates its UUID locally and may appear in the speculative projection. Dependent updates wait for `CREATED_AND_LOCKED`, including its authoritative fencing token. A publication captures an immutable snapshot; later edits remain separate dependent intentions. Exact publication retries are deduplicated, while a different request for an existing deck returns `DECK_ALREADY_PUBLISHED`.

Ordinary synchronization polls replica-local deck, slide, element, and lock observations with `applied_index` metadata. A `VERSION_CONFLICT` instead requires an Authoritative Refresh Read at or beyond `min_commit_index`. Polling preserves a pending draft; the UI permits discard or manual reconciliation but never automatic merge.

Element intentions require successful publication and containing-slide creation. A queued `delete_slide` supersedes only unsent descendants. In-flight descendants are never presumed cancelled and resolve in Raft order. Terminal journal records remain append-only local audit history in the baseline.

## Consequences

- Clients continue useful local work without claiming shared authority.
- Lock fencing, version checks, and Raft ordering remain authoritative.
- Stage B must expose authoritative conflict-refresh data and durable client draft state.
- The initial scheduler is dependency-aware; fairness refinements are deferred until needed.

## Alternatives rejected

- Treating clients as Raft replicas or voters.
- Automatic replay of a request that has a final conflict result.
- Vector-clock ordering across clients.
- A fixed element-before-slide priority order.
