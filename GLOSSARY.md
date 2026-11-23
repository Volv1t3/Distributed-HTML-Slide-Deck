# Glossary

## Production Raft Core

The reusable protocol component derived from the validated Lab 5 decision rules and adapted to standard Raft requirements. It owns consensus state, elections, append replication, commitment, durable hard state, and ordered application callbacks. It does not own slide, element, lock, or UI semantics.

## Raft Adapter

The `app.py` boundary that connects the production Raft core to the instructor runtime's existing node callbacks, transport, persistence facilities, and client response conventions.

## Protocol Identity Validation

The non-cryptographic consistency check applied to every Raft RPC. The runtime source transport slot, the DTO sender slot, and the persistent node UUID registered for that slot must agree before the message can change Raft state. It validates configured-cluster consistency but is not authentication against a malicious sender and does not define client-request deduplication.

## RPC Correlation

The protocol-level association of a reply with the exact RequestVote or AppendEntries call that produced it. An RPC identifier, peer slot, term, and acknowledged log range prevent delayed, duplicate, or fabricated replies from advancing replication state. It is distinct from client-request deduplication.

## Stage Transition Acceptance Gate

The required simulated-system test suite that validates a completed implementation stage before the next stage begins. For Stage A.1, it exercises integrated Raft behavior through the instructor runtime rather than relying only on isolated unit tests.

## Erased-Disk Quarantine

The Stage A limitation for a configured transport slot whose local durable Raft state has been erased. The slot remains part of the fixed voter configuration, but its new process identity may not vote, acknowledge writes, or automatically rejoin. An ordinary restart with intact disk is different: it retains its identity and catches up through AppendEntries. Safe reintroduction after erasure requires a separately designed and operator-approved recovery protocol.

## Durable Acknowledgment

The rule that a Raft reply is not sent until the state it asserts has been persisted. Votes persist term and vote first, AppendEntries success persists log changes first, and client write success follows commitment, deterministic application, and durable request-result recording.

## Replica Validation Environment

The supported Stage A runtime environment for strict DTO validation. Both the local virtual environment and a project-built Docker image used for the instructor cluster include the pinned Pydantic v2 dependency, while the instructor's `node.py` transport interface remains unchanged.

## Operational Node Log

An append-only, per-replica diagnostic record of request handling, validation failures, protocol events, warnings, and errors. It uses JSON records in a `.log` file with MDC-style contextual fields. It is never Raft authority, is not replicated, and must not be used to reconstruct consensus or application state. The replicated Raft command log remains the sole ordered record of valid operations.

## Logging Context

The structured per-event context carried by operational logging, analogous to Java MDC. It includes the replica slot and may include node UUID, term, role, peer slot, RPC ID, client ID, request ID, operation name, log-index range, payload fingerprint, result, and error code.

## Logging Mode

The local, non-replicated diagnostic-data policy for a replica. `OPERATIONAL` logs identifiers, operation names, outcomes, error codes, and fingerprints. `DEBUG` additionally logs complete client payloads for investigation. The mode is selected only at startup through local environment configuration and never changes consensus or state-machine behavior.

## Operational Log Archive

The retained copy of every replica's operational log produced when a cluster run ends. `cluster.py down` archives each `run/<slot>/events.log` in the timestamped `historiales/` run directory alongside role evidence. No automatic rotation or deletion policy removes archived logs.

## Raft-Neutral State Machine

The deterministic Stage A application used to prove consensus integration before presentation editing is introduced. It supports replicated `kv_put`, local eventual `kv_get`, and an internal `raft.noop`, all of which traverse the typed command, replication, commitment, application, persistence, and deduplication path.

## Local Eventual Read

A read served from one replica's locally applied state without a leader round trip or quorum confirmation. It may be stale and therefore cannot grant shared editing authority or authorize a future lock operation.

## Fixed Voter Set

The immutable set of configured transport slots from `self.nodes` that defines Raft membership for a run. The engine requires at least three voters and calculates quorum as `len(voters)//2 + 1`; clients and newly observed UUIDs are never added as voters by registration.

## Election Timing Policy

The local scheduler policy for leader election and heartbeats. Stage A uses a 150 ms default heartbeat and a randomized, locally configurable 450–900 ms election deadline range, both compatible with the instructor runtime's 50 ms tick. These timing values affect liveness demonstrations, not consensus membership or safety rules.

## Current-Term No-Op

An internal command appended by a newly elected leader in its current term. When a quorum commits it, the leader establishes a safe current-term commit point that may also commit preceding log entries.

## Minority Write Rejection

The rule that a node in an isolated or minority partition may retain a temporary leader-role hint but cannot acknowledge an authoritative write without a quorum. It returns `QUORUM_UNAVAILABLE` or times out for writes, while local eventual reads remain available.

## Client Draft Outbox

The durable client-local queue of uncommitted shared-edit intents created while quorum is unavailable. Its operations may update a speculative local projection and display `PENDING_SYNC`, but they are not committed shared changes. On reconnection, the client resubmits the original request IDs in local order; the current Raft leader assigns authoritative global order and may return a deterministic domain result or conflict.

## Authoritative Conflict Refresh

The client recovery read required after a committed `VERSION_CONFLICT`. The conflict result returns the committed element version and a `min_commit_index`; the client then retrieves an element snapshot whose applied index is at least that commit index. The snapshot supplies current content, version, lock and fencing state for reconciliation and must not be replaced by an arbitrary stale eventual read.

## Manual Conflict Resolution

The Stage B reconciliation policy for a rejected shared edit. The original command remains conclusively recorded as `VERSION_CONFLICT` and is never replayed under its old request ID. The client retains its local draft, refreshes an authoritative base, lets the user make corrections, and submits the corrected edit as a new command with a new request ID and fresh preconditions.

## Reconciliation Lock Sequence

The order used to resubmit a pending shared-element draft. After reconnection, the client obtains a newly committed lock, fetches an authoritative element snapshot at or after the lock-acquisition commit index, lets the user correct the draft, then submits a new update with the new fencing token, fresh version, and new request ID. A denied lock retains the draft as pending and never authorizes reuse of a stale token.

## Durable Intention Journal

The client-local, append-only sequence of validated editing intentions. It preserves each client's local history and original request IDs without becoming a second distributed log. The client derives scheduling and reconciliation state from the journal, while Raft remains the authoritative cross-client ordering mechanism.

## Hierarchical Resource Index

The local scheduling view of a durable intention journal, indexed by deck, slide, and element scope. It identifies ancestor and descendant relationships and helps determine whether operations are independent, require a prior result, supersede unsent descendant work, or are invalid descendants. It is not a universal execution-priority order.

## Local Structural Validation

The client-side check performed before an editing intention enters the outbox. The current local projection must contain the target deck, slide, and element, and none may be pending deletion. A rejected local intention never reaches the synchronization scheduler; the authoritative state machine independently enforces the equivalent committed-state rules.

## Semantic Supersession

The local cancellation of an unsent intention whose effect is made irrelevant by a later destructive intention, such as an unsent element update under a pending slide deletion. The journal retains the superseded record and its reason. In-flight operations are not assumed cancelled; Raft determines their committed outcome.

## Intent-Preserving Outbox

The Stage B outbox guarantee. It preserves the user's safe final editing intent and records superseded intentions with their reasons, rather than requiring every local operation to appear in exact client-history order in the Raft log. Required causal prerequisites remain ordered; Raft determines authoritative ordering across clients.

## Stage A

The implementation stage that establishes the production Raft core, typed and versioned DTO boundaries, durable state, and a deterministic Raft-neutral state machine derived from the lab implementation and the project starter requirements.

## Stage A.1

The integration stage that follows the Stage A core and precedes Stage B. It connects the Raft core to the project's existing HTTP/RPC runtime and completes backend lifecycle and protocol state transitions needed before distributed editing operations are introduced.
