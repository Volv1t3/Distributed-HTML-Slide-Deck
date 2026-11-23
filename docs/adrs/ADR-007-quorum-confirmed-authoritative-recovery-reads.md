# ADR-007 — Quorum-Confirmed Authoritative Recovery Reads

- **Status:** Accepted
- **Date:** 2026-11-23
- **Scope:** Linearizable element refresh during Stage B conflict recovery
- **Related:** ADR-003, ADR-004; Stage B Distributed Editing Implementation Specification

## Context

Conflict recovery requires an element observation at or after both the conflicting command and the client's fresh lock acquisition. Routing the request to a process that locally reports `leader` and waiting for its applied index is insufficient: a former leader can remain isolated, retain that role temporarily, and serve stale state after a majority has elected a replacement.

Stage B originally prohibited Raft-core changes. Live Stage B.1 validation exposed that this boundary would leave the specified Authoritative Refresh Read unsafe.

## Decision

`authoritative_refresh_element` uses a volatile Raft ReadIndex round before returning state. The leader must first have a committed entry from its current term, obtain a fresh correlated quorum response through empty AppendEntries probes, and apply through the greater of the confirmed read index and the request's `min_commit_index`. An isolated or demoted leader returns an availability or redirect result instead of stale state.

The read round does not append a command, establish a lease, create a second consensus mechanism, or change ordinary local eventual reads. Read contexts and pending results are volatile. AppendEntries responses echo an optional read context only for an empty probe whose preceding prefix was verified; an echoed context does not independently advance replication state.

This is a narrow safety exception to the Stage B boundary in ADR-003. Membership, elections, normal replication, current-term commitment, persistence ordering, and transport identity validation remain unchanged.

## Consequences

- Conflict reconciliation cannot accept a stale observation from an isolated former leader.
- `min_commit_index` remains an additional application freshness fence rather than a substitute for quorum confirmation.
- Authoritative refresh availability now follows quorum availability; ordinary rendering reads remain replica-local and eventual.
- Stage B.1 must include deterministic protocol tests and a live stale-former-leader scenario that proves refusal during isolation and convergence after healing.

## Alternatives rejected

- Trusting the local `leader` role: a partitioned former leader can remain stale.
- Waiting only for `last_applied >= min_commit_index`: equal numeric indexes do not prove the process is still authoritative.
- Logging each read as a Raft command: safe but adds unnecessary replicated writes and latency.
- A time-based leader lease: introduces clock assumptions not otherwise required by the implementation.
