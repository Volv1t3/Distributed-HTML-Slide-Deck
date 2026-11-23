# Stage C — Failure Behavior Implementation and Stage C.1 Validation Specification

**Status:** Ready for implementation after issue publication
**Date:** 2026-11-23
**Prerequisites:** Completed Stage B and Stage B.1 acceptance evidence; ADR-001, ADR-002, ADR-003, ADR-004, ADR-006, and ADR-007
**Stages covered:** Stage C implementation and Stage C.1 validation

## Problem Statement

The shared presentation backend can grant exclusive element locks, reject stale fencing tokens, persist local editing intentions, recover committed state after ordinary replica restarts, and reconcile version conflicts. A lock nevertheless remains held indefinitely when its editor disconnects without releasing it. The client has no automatic renewal service, the leader has no inactivity coordinator, and the replicated state machine cannot yet distinguish a current renewal from an obsolete expiration proposal.

This prevents abandoned elements from becoming editable again. It also leaves the failure behavior incomplete across client disconnects, leader changes, follower restart, minority partitions, and majority loss. The system must recover abandoned locks without allowing a stale editor, delayed renewal, old release, isolated former leader, or obsolete expiration proposal to overwrite current authority.

## Solution

Stage C completes the existing element-lock model with Raft-committed `renew_lock` and internal `expire_lock` transitions. A client-side Lock Maintenance Service maintains one background Renewal Worker for each selected element. A leader-local Lock Expiration Coordinator observes applied Lock Authority generations, tracks inactivity with a monotonic clock, and proposes guarded expiration through the existing Raft path. Neither local clocks nor threads directly change replicated state.

The client retains user edits during uncertainty but applies a Scoped Authority Gate so protected mutations are not sent with unverified or lost authority. A committed expiration retains the per-element fencing counter, allowing a later acquisition to receive a strictly larger token. Old tokens remain rejected by the existing state-machine guard.

Stage C.1 validates the behavior through deterministic temporal tests and five clean-cluster scenarios covering normal renewal, follower restart and catch-up, leader failure under concurrency, partition expiration and reassignment, and majority loss. Existing per-node Operational Node Logs receive enough structured instrumentation to prove real internal expiration proposals and committed outcomes without inventing client operations.

## User Stories

1. As an editor, I want my selected element lock renewed automatically, so that ordinary editing does not lose authority while I remain active.
2. As an editor, I want renewal to run independently of UI polling, so that rendering or event-loop delays do not silently stop lock maintenance.
3. As an editor working on multiple selected resources, I want a delayed renewal for one element not to block renewal of another element.
4. As an editor, I want every successful lock result to include the element version, fencing token, and Lease Epoch, so that my client has a complete Lock Authority observation.
5. As an editor, I want a committed renewal to advance the Lease Epoch without changing the element version, so that lock maintenance does not look like a content edit.
6. As an editor, I want uncertain renewal outcomes distinguished from deterministic authority loss, so that my client does not falsely claim success or unnecessarily discard work.
7. As an editor, I want local editing to continue while renewal is unverified, so that a temporary network failure does not interrupt drafting.
8. As an editor, I want protected mutations retained but unsent while authority is unverified, so that stale authority cannot be presented as a shared save.
9. As an editor, I want an exact-ID renewal retry after timeout, redirect failure, or quorum loss, so that uncertainty cannot increment the Lease Epoch twice accidentally.
10. As an editor, I want renewal to recover from `UNVERIFIED` to `VERIFIED` after the same attempt receives `RENEWED`, so that queued work can resume safely.
11. As an editor, I want a proven `STALE_TOKEN`, `NOT_OWNER`, or `NOT_FOUND` result to mark the maintained lock as `LOST`, so that the client stops renewing invalid authority.
12. As an editor, I want lost-authority drafts to use explicit User-Directed Draft Reconciliation, so that reacquisition never silently overwrites newer content.
13. As an editor, I want deselection to stop future renewal and durably request release, so that a lost release response can still be retried after restart.
14. As an editor, I want a renewal already in flight during deselection to finish safely without scheduling another renewal.
15. As an editor, I want a client restart to forget ephemeral selection, so that a crashed editor naturally stops renewing abandoned locks.
16. As a collaborator, I want an abandoned lock to expire after stable inactivity, so that another editor can eventually acquire the element.
17. As a collaborator, I want a new acquisition after expiration to receive a strictly larger fencing token, so that the former owner cannot regain authority with an old token.
18. As a collaborator, I want an old editor's update, renewal, or release rejected after reassignment, so that delayed messages cannot affect the new owner.
19. As a collaborator, I want a renewal committed before an old expiration proposal to make that proposal a no-op, so that current authority is preserved.
20. As a collaborator, I want an expiration committed before renewal to make the later renewal stale, so that a released generation cannot be resurrected.
21. As an operator, I want only the current leader to propose expiration, so that followers and isolated former leaders cannot release locks unilaterally.
22. As an operator, I want expiration to become effective only after Raft commitment and application, so that every replica observes the same authority transition.
23. As an operator, I want a newly elected leader to give inherited locks a fresh full grace period, so that monotonic timestamps are never compared across processes.
24. As an operator, I want one expiration proposal per token and epoch generation, so that timer ticks do not flood the replicated log.
25. As an operator, I want repeated internal expiration proposals deduplicated across leader changes, so that failover cannot repeat the transition.
26. As an operator, I want obsolete expiration proposals to return `NO_OP_STALE_EXPIRY`, so that expected races remain deterministic application outcomes rather than operational failures.
27. As an operator, I want an internal expiration request rejected at the public client boundary, so that clients cannot invoke leader-only maintenance behavior.
28. As an operator, I want the follower that retained its disk to catch up renewed, expired, and reassigned lock state through AppendEntries, so that recovery includes the complete presentation authority model.
29. As an operator, I want preserved-disk recovery to retain fencing counters and deduplication results, so that restart cannot make an old token valid again.
30. As an operator, I want no lock transition acknowledged when a majority is unavailable, so that failure behavior preserves Raft safety.
31. As an operator, I want client and replica timing values validated at startup, so that an invalid renewal policy fails explicitly.
32. As an operator, I want the supported replica set to use one inactivity timeout, so that failover behavior is predictable.
33. As an evaluator, I want operational logs to identify expiration proposal, commitment, stale no-op, leadership, token, epoch, and request identity, so that internal behavior is auditable.
34. As an evaluator, I want checker evidence to distinguish raw client history from actual server-originated expiration transitions, so that no user command is fabricated.
35. As an evaluator, I want deterministic tests for both renewal-versus-expiration orders, delayed old-token release, and leader change after renewal, so that timing-sensitive safety is demonstrated without relying only on sleeps.
36. As an evaluator, I want five independent clean-cluster histories, so that normal, restart, failover, partition, and no-majority behavior can be reviewed separately.
37. As an evaluator, I want renewal latency and expiration recovery time reported beside ordinary local-read latency, so that the coordination cost is visible.

## Implementation Decisions

### Stage boundary

- Stage C owns automatic renewal, committed expiration, Maintained Lock Confidence, Scoped Authority Gate behavior, preserved-disk lock-state catch-up, and deterministic temporal safety.
- Stage C.1 owns the five clean-cluster failure scenarios, actual internal-transition evidence, checker projections, latency measurements, and the Stage Transition Acceptance Gate.
- Stage D owns post-publication deck and slide settings, canvas changes, metadata changes, and backgrounds.
- Snapshots, InstallSnapshot, erased-disk reintroduction, dynamic membership, authentication, and log compaction are not part of Stage C.

### Replicated Lock Authority

- Lock Authority consists of Editor Session, Fencing Token, Lease Epoch, and the current element version required by protected writes.
- `CREATED_AND_LOCKED`, `LOCK_ACQUIRED`, and `ALREADY_HELD` return element version, current fencing token, and current Lease Epoch. A new lock generation begins at epoch zero.
- `renew_lock` is a public linearizable command available to the owning Editor Session. It requires the current fencing token, increments Lease Epoch exactly once after commit, returns `RENEWED`, and leaves element version unchanged.
- A missing active lock or mismatched token returns `STALE_TOKEN`. A matching token with the wrong session returns `NOT_OWNER`. Missing or deleted resources return `NOT_FOUND`.
- `expire_lock` is an internal linearizable command. It compares expected fencing token and expected Lease Epoch during deterministic application.
- An exact match returns `EXPIRED`, removes the active lock, and retains the fencing counter. A missing, changed, deleted, or otherwise obsolete generation returns `NO_OP_STALE_EXPIRY`.
- Renewal, release, and expiration never increment the element version. Content updates and deletions retain their existing version rules.
- The existing stale-token guard remains authoritative for update, delete, release, and renewal. A later acquisition always increments the retained per-element fencing counter.

### Lock Maintenance Service

- The client layer owns a Lock Maintenance Service because only the client knows whether an element remains selected.
- The service has one coordinator background thread and one dedicated Renewal Worker per selected element. A blocked worker for one element cannot delay another element.
- Each worker owns at most one Transient Renewal Attempt. It creates one request ID when due and retries that exact identity after uncertain outcomes. It creates a new request ID only after a committed `RENEWED` result.
- Renewal attempts are coordination state and never enter the Durable Intention Journal. Committed server results remain replicated and deduplicated normally.
- The selected-lock registry and renewal attempts are process-local. Client restart does not reconstruct selection or automatic renewal.
- The coordinator starts lazily when the first lock is registered, waits without polling when no locks are maintained, and supports explicit close and context-manager cleanup.
- Worker shutdown is cooperative. A stop signal prevents new attempts, HTTP requests use bounded timeouts, and shutdown joins workers. A response already in flight may finish but cannot schedule another renewal.
- The background implementation uses an injected monotonic clock and controllable synchronization primitives so tests can observe thread behavior without exposing a production manual-tick API.

### Maintained Lock Confidence and outbox behavior

- `VERIFIED` follows a committed acquisition or renewal.
- `RENEWING` means one renewal request is in flight while the last committed authority remains the latest known authority.
- `UNVERIFIED` follows an uncertain infrastructure outcome such as timeout, unavailable quorum, or unresolved routing. The worker retries the same request ID while selection remains.
- `LOST` follows a deterministic authority rejection. Renewal stops and recovery requires explicit acquisition plus Authoritative Refresh Read and User-Directed Draft Reconciliation.
- `STOPPED` follows deselection or service shutdown. No new renewal is scheduled.
- Protected `update_element` and `delete_element` intentions for an `UNVERIFIED` or `LOST` element remain durable but are not transmitted. Their dependent protected mutations remain ordered behind them.
- Release, replica-local observations, Authoritative Refresh Reads, unrelated element work, and independent structural work remain eligible.
- Deselecting a `VERIFIED`, `RENEWING`, or `UNVERIFIED` lock creates one durable release intention with the current token. Deselecting `LOST` authority does not create a release.
- An in-flight renewal and release are allowed to race through Raft. Either committed order is safe under the token guard.

### Lock Expiration Coordinator

- The Lock Expiration Coordinator sits beside the Raft Adapter; the Production Raft Core remains application-neutral.
- The coordinator uses the adapter's injected monotonic clock and derives its state from applied replicated locks.
- A newly observed token/epoch generation starts a full inactivity timeout. A changed token or epoch replaces the previous generation. A removed lock removes its timer and proposal marker.
- Only committed acquisition and committed renewal establish or reset inactivity. Element updates do not renew locks implicitly.
- A newly elected leader initializes every inherited active lock at its current monotonic time and grants a full new grace period. It never compares timestamps from another process.
- Once a generation is due, the coordinator emits at most one Internal Expiration Proposal until the generation changes, disappears, or reaches a committed outcome.
- Step-down discards every local timer and in-flight proposal marker. A previously appended expiration entry may still commit according to Raft order.
- The internal command uses the reserved logical client identity `_lock_expiration`, has no Editor Session, and uses a deterministic request ID derived from deck, element, fencing token, and Lease Epoch.
- The command fingerprint excludes proposer identity, allowing a replacement leader to reproduce the same logical request. Replicated request-result deduplication retains `EXPIRED` and `NO_OP_STALE_EXPIRY` outcomes.
- Public client routing rejects `expire_lock` before proposal. The reserved identity is protocol structure, not authentication.

### Timing and configuration

- Production defaults are a 3-second renewal interval and a 10-second inactivity timeout.
- Values are positive fractional seconds supplied through explicit constructor settings with environment-backed defaults.
- Startup validates `3 × renewal_interval <= inactivity_timeout` wherever both values are configured.
- Every replica in the supported cluster receives the same inactivity timeout. The client receives its renewal interval and the expected timeout used for validation.
- The system promises eventual expiration only while stable leadership and quorum persist. It does not promise an exact expiration instant.
- Stage C.1 uses a 0.20-second renewal interval, a 0.80-second inactivity timeout, and the unchanged 0.05-second starter tick. Assertions include scheduling tolerance and wait for committed observable state.

### Preserved-Disk Replica Recovery

- Stage C strengthens the existing AppendEntries recovery path; it does not add another recovery protocol.
- A stopped follower may miss content updates, renewals, expiration, reassignment, and later edits. After ordinary restart with intact identity and disk, it repairs the missing log suffix and applies the same final presentation and lock state.
- Acceptance compares element content/version, active lock or absence, fencing counter, Lease Epoch, and relevant deduplication results.
- Erased-Disk Quarantine remains unchanged. A process with lost identity does not automatically rejoin.

### Operational evidence and checker projection

- Each replica's existing Operational Node Log is extended with structured events for timer initialization, renewal observation, expiration proposal, proposal identity, committed `EXPIRED`, committed `NO_OP_STALE_EXPIRY`, step-down cleanup, and inherited-lock grace initialization.
- Events carry the existing Logging Context plus deck, element, fencing token, Lease Epoch, request ID, term, and result where applicable. Operational logs never become Raft authority.
- Raw client histories remain unchanged and never invent `expire_lock` as a user request.
- Stage C.1 extracts actual committed internal transitions from the Operational Node Logs and replicated results into a separate internal-transition JSONL trace.
- The sequential reference model understands the deterministic `expire_lock` transition. A transparent checker projection merges the actual server-originated transition into committed order and labels its origin.
- Archives retain raw client history, internal transition trace, checker projection, checker output, operational logs, roles, scenario manifest, fault trace, and metrics.

### Accepted prototype limitation

- Periodic renewals append Raft entries and replicated deduplication records indefinitely.
- Stage C documents and measures this cost but does not add log compaction, request-result pruning, snapshots, or InstallSnapshot.

## Testing Decisions

### Primary test seam

The highest integration seam remains the existing HTTP client request path through the Raft Adapter, Production Raft Core, deterministic presentation state machine, persistence, fault controls, operational archives, reference model, and checker. `renew_lock` uses this seam without a replacement transport.

Internal expiration uses the same adapter-to-Raft proposal path but enters through the leader-local Lock Expiration Coordinator rather than the public client route. Its actual proposal and committed result are observed through replicated state and the Operational Node Log.

### Deterministic seams

- State-machine tests validate response shapes, epoch advancement, unchanged element versions, stale tokens, wrong owners, exact expiration, stale expiration, fencing-counter retention, tombstones, and deduplication.
- Adapter/component tests use injected monotonic clocks to validate leadership transitions, full inherited grace, timer replacement on token/epoch change, one proposal per generation, step-down cleanup, and public expiration rejection.
- Lock Maintenance Service tests use controlled request senders, synchronization events, bounded joins, and short injected timing to validate automatic worker creation, per-element isolation, exact-ID retries, state transitions, cooperative stop, and no thread leaks.
- Client-journal tests validate Scoped Authority Gate behavior, unrelated-work progress, durable release handoff, and explicit reconciliation after `LOST`.
- Temporal tests cover renewal before old expiration, expiration before renewal, delayed old-token release after reassignment, and leader change immediately after renewal.

Good tests assert externally observable command results, state, logs, histories, confidence transitions, and convergence. They do not depend on private container layout or exact thread scheduling order.

### Stage C.1 clean-cluster scenarios

1. **Normal operation:** acquire or create-and-lock, observe automatic renewals, save, deselect, release, and verify convergence and unchanged element version during renewals.
2. **Follower failure and restart:** stop one follower, commit renewals and edits on the majority, allow expiration or release and reassignment, restart with intact disk, and compare complete presentation and Lock Authority state.
3. **Leader failure under concurrency:** fail the leader while renewal and competing lock work are in flight, retry stable request identities, verify a fresh inherited grace period, and preserve every confirmed outcome.
4. **Partition and healing:** isolate the lock owner with the minority, prevent minority renewal success, commit majority-side expiration and reacquisition, reject the old token after healing, and converge every replica.
5. **Design-limit majority loss:** remove the ability to form a quorum and demonstrate that renewal, expiration, acquisition, release, and edit cannot be falsely confirmed; restore quorum and recover.

Each scenario starts from a clean three-replica cluster, archives independently, uses the Stage C.1 timing profile, and validates all final replicas after writes cease.

### Measurements

- Report p50/p95 committed renewal latency.
- Report p50/p95 Local Eventual Read latency in the same environment.
- Record observed expiration-to-reacquisition time with scheduling tolerance.
- Record leader-election and majority-loss intervals without successful authoritative responses.
- Report renewal count and resulting log growth for the bounded scenario run without extrapolating production capacity.

## Out of Scope

- Post-publication deck and slide settings, canvas resizing, metadata updates, and background changes; these belong to Stage D.
- Snapshot creation, InstallSnapshot, Raft log compaction, request-result pruning, or production retention policy.
- Erased-disk automatic reintroduction, dynamic membership, joint consensus, or changing the fixed voter set.
- Authentication, authorization, malicious-client resistance, or treating fencing tokens as credentials.
- A public `expire_lock`, administrative force-unlock, or follower-local expiration.
- Exact real-time expiration guarantees, replicated timestamps, clock synchronization, or old-leader monotonic timestamp transfer.
- Automatic reacquisition or automatic conflict merge after `LOST` authority.
- Browser UI implementation, WebSockets, or push notifications.
- Automatic lock renewal for unselected elements or restoration of selection after client restart.

## Further Notes

- Stage B already implements fencing-token validation for protected mutations and release. Stage C extends the existing invariant rather than replacing it.
- Ordinary AppendEntries follower repair already exists. Stage C strengthens its domain evidence for renewed, expired, and reassigned locks.
- The accepted no-compaction limitation is appropriate for the course-scale prototype but must not be described as production-scale retention.
- The five distributed scenarios are the Stage C.1 gate. The four targeted temporal races supplement them and do not replace live cluster evidence.
