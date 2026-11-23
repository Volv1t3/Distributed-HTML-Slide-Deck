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

The client recovery read required after a committed `VERSION_CONFLICT`. The conflict result returns the committed element version and a `min_commit_index`; the client then retrieves an element snapshot only after the leader completes a fresh quorum-confirmed ReadIndex round and applies through at least the required commit index. The snapshot supplies current content, version, lock and fencing state for reconciliation and must not be replaced by an arbitrary stale eventual read.

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

## Typed Element Configuration

The schema-versioned, allow-listed collection of validated fields that defines an element's content and rendering semantics. It is represented by explicit element-kind blocks and typed patches, rather than an arbitrary HTML, CSS, or properties map.

## Canvas Coordinate System

The presentation-local coordinate system whose origin is the top-left of the slide: positive `x` moves right and positive `y` moves down; negative coordinates represent positions left of or above that origin. `z_index` is a separate stacking coordinate.

## Fixed-Point Geometry

The deterministic integer representation for presentation geometry and transforms. The renderer converts canonical values to display pixels; replicas never round geometry differently.

## Deterministic Stacking Order

The element presentation order that sorts first by replicated `z_index` and then by immutable creation sequence assigned at committed creation.

## Atomic Element Patch

One validated `update_element` command that replaces any permitted combination of an element's typed configuration fields as a single replicated state transition. It requires one valid fencing token and expected version, and increments the element version once.

## Logical Client Identity

The stable opaque client identifier used to namespace request deduplication and own durable client outbox records. It is distinct from an editing session and does not authorize a lock.

## Editor Session

The short-lived identifier for one running editor instance. A lock belongs to an editor session, while a logical client identity owns retry and durable-journal history.

## Lock Authority

The current replicated authority to modify one element, identified by its owning Editor Session, Fencing Token, and Lease Epoch together with the element version required by a protected write.

## Lease Epoch

The replicated renewal generation of an active element lock. It advances only when `renew_lock` commits and prevents an older expiration proposal from releasing a more recently renewed lock.

## Lock Maintenance Service

The client-side coordinator that maintains one renewal worker for each lock whose element remains selected. It never grants authority; it preserves or loses confidence according to committed lock results.

## Maintained Lock Confidence

The client-local assessment of a selected lock as `VERIFIED`, `RENEWING`, `UNVERIFIED`, `LOST`, or `STOPPED`. It controls client behavior but never replaces the replicated Lock Authority.

## Renewal Worker

The background activity dedicated to renewing one selected element's Lock Authority. Its lifetime follows that selection and does not block renewal of another selected element.

## Transient Renewal Attempt

The single exact-ID `renew_lock` request retained locally for one selected lock until it receives a definitive result or selection ends. It is coordination state, not a Durable Intention Journal record.

## Scoped Authority Gate

The client rule that withholds protected mutations for one element while its Maintained Lock Confidence is uncertain or lost. It does not block release, observation, unrelated elements, or independent structural work.

## Lock Expiration Coordinator

The leader-local component that observes committed Lock Authority generations and proposes expiration after inactivity. Its timers are ephemeral and never constitute replicated authority.

## Internal Expiration Proposal

A leader-originated request to release one inactive lock generation through Raft. It is guarded by the expected Fencing Token and Lease Epoch and is never a public client operation.

## Outbox Intention Lifecycle

The client-local state of a durable intention: `PENDING_SYNC`, `IN_FLIGHT`, `COMMITTED`, `REJECTED_FINAL`, `SUPERSEDED`, or `NEEDS_MANUAL_RECONCILIATION`. Only `COMMITTED` is authoritative.

## Client Write-Ahead Journal

The rule that a shared intention is appended and flushed locally before its first network send. It enables exact-ID retry after a crash or lost response without making the client a Raft replica.

## Replica Applied Index

The highest committed Raft index applied by a replica to its local presentation state. Every Local Eventual Read returns it as observation metadata.

## Authoritative Refresh Read

A leader-routed recovery read that first completes a fresh quorum-confirmed ReadIndex round, waits until applied state reaches the greater of that read index and the required `min_commit_index`, then returns current element content, version, and lock/fencing information. An isolated former leader refuses or redirects it rather than serving stale state.

## Client Polling Synchronization

The HTTP-based process that periodically reads replica-local state to converge a non-authoritative client display. It never grants editing authority or replaces authoritative refresh.

## User-Directed Draft Reconciliation

The UI recovery choice to discard a conflicting draft or manually correct it after authoritative refresh. A corrected save has a fresh lock, expected version, and request ID; clients never increment authoritative versions.

## Structural Presentation Operation

A deterministic shared command that changes deck or slide structure rather than one element's configuration. Stage B includes slide creation, deletion, and reordering.

## Deck Version

The replicated optimistic-concurrency revision for a deck's slide structure. Each successful structural operation increments it once; stale expected versions are rejected without a deck-wide lock.

## Deferred Presentation Settings

Deck-wide and slide-level settings mutations after publication, including canvas resizing, metadata changes, and backgrounds. They are Stage D scope.

## Stage C

The failure-behavior stage for automatic lock renewal, committed lock expiration, stale-authority rejection, and preserved-disk replica recovery.

## Stage C.1 Validation

The Stage C acceptance gate: five clean-cluster failure scenarios plus targeted renewal, expiration, failover, and stale-authority races.

## Stage D

The later presentation-settings stage for post-publication deck and slide configuration changes deferred by ADR-006.

## Preserved-Disk Replica Recovery

The recovery of a configured replica that retains its durable identity and Raft state, then converges through ordinary AppendEntries repair. It excludes snapshot installation and Erased-Disk Quarantine recovery.

## Stage B.1 Validation

The Stage B acceptance gate: deterministic state-machine tests, live three-replica integration under failures, and durable client-journal synchronization evidence.

## Slide-Structure Dependency

An outbox prerequisite requiring a deck to be published and its containing slide committed before descendant element submission. Unsent descendants of a pending slide deletion are superseded; in-flight commands are resolved by Raft order.

## Font Stack

An ordered list of user-selected system font families plus a generic fallback. Renderers report fallback when a preferred font is unavailable; availability never changes shared state.

## Canonical RGBA Color

The explicit `#RRGGBBAA` presentation color, rather than a palette or arbitrary CSS expression.

## Pending Element Creation

A client-local projection entry for an element whose UUID is journaled but whose creation has not committed. Dependent updates wait for `CREATED_AND_LOCKED` and its authoritative token.
