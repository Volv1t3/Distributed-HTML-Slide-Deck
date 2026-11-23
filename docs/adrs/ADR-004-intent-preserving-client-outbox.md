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
