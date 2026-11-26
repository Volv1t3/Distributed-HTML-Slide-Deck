# ADR-016 — Permission-Aware Client Synchronization and Durable Recovery

- **Status:** Accepted for Stage F implementation planning (F05); not yet implemented
- **Date:** 2026-11-26
- **Stage:** F — F05: permission-aware client synchronization, local observation, and manual recovery
- **Depends on:** ADR-004 (intent-preserving client outbox), ADR-007 (authoritative conflict refresh), ADR-008 (settings revisions), ADR-009–011 (account and credential model), ADR-012 (account-bound local drafts), ADR-013 (grant lifecycle and access_version), ADR-014 (operation matrix), ADR-015 (revocation, read routing, receipts, recovery)
- **Source implementation reviewed:** `Volv1t3/Distributed-HTML-Slide-Deck`, `master`, `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a` on 2026-11-26
- **Implementation intent:** derive an agent-ready F05 contract; do not re-open or silently override accepted ADR-012–015 decisions. This behavior is **not implemented** at the reviewed commit.

## 1. Context and implementation evidence

Stage B's client intention journal is append-only and preserves the exact `(client_id, request_id)` of a business intention before sending it. Stage D added settings revision and reconciliation rules. Stage E's `AuthenticatedClient.call()` injects `_auth` only at transport time and persists the signed-in account's `user_id` alongside the credential. Stages F01–F04 add ownership, per-deck grants, owner/editor/viewer authorization, Raft-order revocation, and a safe read boundary.

Code inspection reveals the integration points and risks:

| Current code | Observed behavior | Required F05 adaptation |
|---|---|---|
| `client_journal.py:Lifecycle`, `OPERATION_POLICIES` | Six established intention states, per-operation successes | Preserve these states and their meanings; add **no** `BLOCKED_BY_ACCESS` intention status |
| `ClientIntentionOutbox._eligible()` | Gates on intention dependencies and the existing element lock-confidence gate | Also gate **post-publication EDIT operations by their deck**, preventing direct outbox send bypass |
| `ClientIntentionOutbox.synchronize()` and `_send_one()` | Move a selected intention to `IN_FLIGHT` before send | Enforce the deck gate before either ordinary send or explicit recovery send |
| `ClientIntentionOutbox._record_response()` | `VERSION_CONFLICT`/settings conflicts are manual; success matches operation policy; other concrete results become `REJECTED_FINAL` | Consume `ACCESS_DENIED` without repeat sending; handle safe receipt statuses through a distinct receipt transition, not as original mutation successes |
| `ClientIntentionOutbox._recover_uncertain_in_flight()` | Following restart, resets `IN_FLIGHT` to `PENDING_SYNC` for exact-ID recovery | Prevent post-restart automatic resends before fresh permission verification; respect previously durable access-loss/reconciliation gate |
| `ClientIntentionOutbox.speculative_projection` | Only derives certain pending/in-flight overlay artifacts; ignores `REJECTED_FINAL` | Ensure denied editing material remains **recoverable** for manual user action; do not turn it into an active server mutation |
| `ClientIntentionOutbox.poll()` | Preview followed by slide/element/lock reads; one `_base_projection` and global `_highest_applied_index` | Consume permission-bearing authoritative preview; never overwrite a newer permission observation; handle a protected-read denial during a poll without installing an apparently complete partial refresh |
| `JournaledClient.poll_if_due()` | Polls configured `local_replica`; `synchronize()` uses `leader_replica` | Stage F protected reads must target current leader, honoring redirect/fail-closed ReadIndex behavior per ADR-015 |
| `AuthenticatedClient.login()` | Credential includes `user_id`; `call()` injects bearer on demand | Use authenticated user identity as a gate key; keep secrets out of journal and gate |
| `lock_maintenance.py:RenewalWorker` | Independent lease renewal worker can submit `renew_lock` | When permission drops below EDIT, stop **new** renewals per F07; do not invent a cancel RPC or release command |

The current journal can record intentions for multiple `deck_id` scopes, but its materialized base projection is a single dictionary rather than a multi-deck cache. **ADR-016 resolves this mismatch by requiring one durable outbox per deck, bound to an authenticated account, with shared authenticated transport across outboxes.**

## 2. Binding decisions carried forward from ADR-015

1. **Authorization never comes from the client cache.** Raft at-apply permission checks and the leader's ReadIndex-protected reads remain authoritative. The client cache only controls UI and avoids inadmissible submission attempts.
2. **Deck-scoped gate, not a new intention lifecycle.** Preserve `PENDING_SYNC`, `IN_FLIGHT`, `COMMITTED`, `REJECTED_FINAL`, `SUPERSEDED`, and `NEEDS_MANUAL_RECONCILIATION`. Add gate state separately, keyed by authenticated account and deck.
3. **Preserve work.** On `VIEW`, `NONE`, or unverifiable/unknown authority, suspend new shared editing submissions. Never automatically delete unsent patches, captured intention arguments, or historical request identities. A direct terminal `ACCESS_DENIED` ends that exact Raft intention, not the user's ownership of local draft material.
4. **No silent recovery after regrant.** Restored `EDIT`/`OWNER` does not automatically release a gate that was suspended following access loss. The user must refresh the deck, review affected local edits, and explicitly reconcile them using current domain preconditions.
5. **Already-in-flight operations are not canceled.** They may have committed before revocation, committed with `ACCESS_DENIED` after it, or remained uncertain. No leader cancellation RPC, rollback, or automatic resend is introduced.
6. **Minimal operation receipts.** The read-only `query_operation_receipt` returns only `COMMITTED_SUCCESS`, `COMMITTED_REJECTED`, or `OUTCOME_UNKNOWN`, for the original authenticated client installation's eleven ADR-014 editing operations. The client preserves unknown requests, uses definitive receipt results only for their original request, and does not use a receipt as a lock token, version proof, or a grant.
7. **Private drafts remain local-first.** Newly created local, unpublished drafts remain subject to ADR-012's established signed-in account identity, not a non-existent shared-deck participant grant. `publish_deck` uses ADR-012's creator check; it is **not** reclassified as an ordinary EDIT-grant mutation.
8. **No push notifications.** Permission discovery occurs through `get_deck_preview` and an explicit `query_grants` refresh (e.g., after `ACCESS_DENIED`). The entire shared-deck read path is leader-bound ReadIndex per ADR-015.

## 3. Client state separation

Do not conflate these three distinct forms of local state:

- **Durable intention record:** each original business operation, full local non-secret arguments, request ID, lifecycle, dependency edges, and recorded result. It is append-only journal history, stored inside an outbox that belongs to exactly one `(user_id, deck_id)` scope.
- **Durable deck synchronization gate:** stable decisions about suspended submission, whether restoration requires explicit reconciliation, and the account/deck to which the decision belongs. Must survive restart independently of transient in-memory observations.
- **Volatile permission observation:** the most recently verified `OWNER`, `EDIT`, `VIEW`, or `NONE` role for an authenticated account/deck, possibly with `access_version` and `applied_index`; used for UI and freshness ordering, not as a long-lived proof of authority.

**Illustrative internal structures (names/serialization are not new public protocol values):**

```python
@dataclass
class DeckSyncGate:
    user_id: UUID
    deck_id: UUID
    suspended: bool
    requires_explicit_reconciliation: bool
    last_observed_access: Literal["OWNER", "EDIT", "VIEW", "NONE", "UNKNOWN"]
    last_access_version: int | None = None

@dataclass
class PermissionObservation:
    user_id: UUID
    deck_id: UUID
    effective_access: Literal["OWNER", "EDIT", "VIEW", "NONE"]
    access_version: int | None
    applied_index: int | None
    observation_epoch: int  # local ordering fence; not a server credential
```

The client may choose a sidecar file or journal-compatible records for gate durability, but it must atomically/durably record a gate closure before any later operation is eligible for a new send, resist crash/restart, and prevent a different account from inheriting another user's permission or unreconciled intentions. **One journal/outbox instance is bound at initialization to exactly one account and one deck; multiple such instances reuse the same authenticated installation and request transport.** Credentials, bearer tokens, credential digests, password material, and server-stamped actor fields must not enter the client intention journal or gate storage. Positive cached roles require fresh verification after restart; an unverified restored client must not submit protected editing operations.

## 4. Gate lifecycle and eligibility

| Observed event | Durable gate consequence | Original intention consequence |
|---|---|---|
| First startup / new authenticated account with no fresh deck role | Suspend EDIT submission until a fresh authoritative observation | No mutation; pending records remain intact |
| Fresh `OWNER`/`EDIT` with **no prior access-loss reconciliation requirement** | May permit normal eligible submissions | Existing exact-ID retry semantics remain, subject to domain dependencies |
| Fresh `VIEW` or `NONE` | Suspend EDIT submission; record restoration requiring manual review when affected work exists | Preserve unsent and uncertain intentions; already committed results remain terminal |
| Edit or lock operation returns `ACCESS_DENIED` | Suspend immediately, then request `query_grants` when available | An acknowledged terminal denial is recorded; never retry its ID as a new edit |
| Quorum unavailable, bearer unverifiable, or authority unknown | Suspend new protected mutation submission until verified | Treat transport uncertainty as uncertainty, not as confirmed denial |
| Fresh `EDIT`/`OWNER` after `VIEW`/`NONE` loss | Keep access-loss reconciliation gate closed for affected work | Refresh current server state and require explicit user decision; do **not** auto-send backlog |
| User explicitly approves corrected/new edit after refresh | Apply established per-intention manual workflow; release only specifically approved work | New or corrected logical operation gets new request ID and valid current preconditions |
| User explicitly discards a pending edit | Existing `discard()` / `SUPERSEDED` path | No server mutation; historical journal record remains auditable |

Any direct `ClientIntentionOutbox.synchronize()`, `JournaledClient.synchronize()`, `_send_one()` recovery path, or future direct submission adapter must honor the same effective gate. Do not rely only on disabling UI controls. The journal scheduler must evaluate the intention's **own `deck_id`**, rather than treating all decks on a client as having one role. In addition, account switching must not transfer eligibility from the previous account.

If no access-loss interval has been observed and a restarted client reauthenticates as the original owner/editor, its normal exact-ID retry semantics may resume after fresh verification; do not convert every restart into an irreversible forced manual reconciliation. Conversely, if a durable access-loss gate already requires review, successful reauthentication or a positive grant query must never silently clear that requirement.

## 5. Monotonic permission observations and remote polling

The `get_deck_preview` result is now ReadIndex-authorized and contains the authenticated caller's effective role, `access_version`, and `applied_index`. `query_grants` returns the self-role with these fields for `OWNER`/`EDIT`/`VIEW`; its `OK`/`NONE` response intentionally omits deck-specific revision information and cannot be treated as a higher numbered access revision.

1. Store observations by **authenticated user and deck**, never globally across unrelated decks.
2. Accept a newer observed access revision and reject a delayed positive observation known to predate an accepted newer positive/negative observation. Where `NONE` has no version, use a local request/observation epoch to prevent an old in-flight preview from re-enabling editing after a later no-access result. This epoch is **only** a local response-ordering device.
3. `ACCESS_DENIED` immediately suspends mutation submission; an online `query_grants` follows. If the query fails or quorum is lost, the client remains suspended rather than inferring `NONE` or claiming a fresh role.
4. `EDIT` enables editing controls only when the local gate also permits an edit workflow. `VIEW` disables editing and lock controls; `NONE` hides shared remote content and stops protected deck polling while leaving already stored local work intact; `OWNER` additionally exposes sharing controls.
5. Each subordinate read (`get_slide`, `get_element`, `read_lock_status`) remains independently protected under ADR-015. A preview is **not** a reusable authorization token. If a subordinate read is denied midway through a poll, do not install the partial projection as a successful, complete refresh. Authoritative reads may legitimately observe different committed points as Raft advances; a poll is not a transactionally consistent deck-wide snapshot.
6. The `ClientIntentionOutbox.record_base_projection()` monotonic `applied_index` behavior remains useful for content observations, but it is not itself a permission authority. Treat access-state monotonicity separately from ordinary deck content projection monotonicity.

## 6. Safe receipt recovery and request identity

A minimal receipt is an **observation of committed evidence**, not a successful re-execution:

| Receipt | Client interpretation | Original ID |
|---|---|---|
| `COMMITTED_SUCCESS` | Mark the original intention committed without resending it; do not invent a missing lock token/version from the receipt | Retain original |
| `COMMITTED_REJECTED` | Record a generic terminal rejection (normally `REJECTED_FINAL` in the existing journal); do not fabricate `NEEDS_MANUAL_RECONCILIATION` conflict details absent from the safe receipt. Preserve local edit material for later user-driven repair | Retain original; do not retry |
| `OUTCOME_UNKNOWN` | Remain uncertain; offer another explicit receipt check later; do not infer failure or resubmit while unauthorized | Retain original unchanged |

Do not route these receipt keywords through `OPERATION_POLICIES.success_statuses` as though they were responses to the original mutation: that policy currently expects statuses such as `SAVED`, `RENEWED`, or `DECK_SETTINGS_UPDATED`. Implement a distinct receipt-to-journal reconciliation adapter. Any receipt-confirmed success lacking its original result's creation lock token or revision must not be used to synthesize those values for dependent operations; obtain fresh authorized state and appropriate lock authority before continuing.

For already acknowledged terminal `ACCESS_DENIED`, the journal preserves the rejected historical entry but never reuses the original ID as a corrected mutation. For an intention that was never submitted, an explicit user-approved send may retain its original ID only if the original logical payload and preconditions remain valid and no new business intention is being substituted. A corrected/rebased mutation must use a **new request ID**, preserving Stage B–D's historical reconciliation contract.

## 7. Accepted F05 scope decisions and compatibility rules

### Q1 — Read-only recovery view over retained denied intentions: accepted Option A

An acknowledged `ACCESS_DENIED` is recorded as the existing terminal `REJECTED_FINAL` lifecycle. The intention, its original request ID, and its non-secret author-supplied arguments remain in the append-only journal. A **read-only local recovery view** allows the caller to list or select these saved intentions for review and obtain the patch without modifying the lifecycle or making it eligible for the scheduler. This recovery view is **distinct from** `speculative_projection`, which remains a preview of live pending/in-flight work. The recovery view must enforce the signed-in account/outbox scope and must not transmit journal history or recovered content to the server without a new explicit user action. If access is later restored, a corrected/rebased replacement operation receives a new request ID and current version/lock preconditions. No second durable recovery store is required.

### Q2 — One durable outbox per deck: accepted Option A

Each `ClientIntentionOutbox`/`JournaledClient` pair is bound to **one `(authenticated user_id, deck_id)` pair** for its entire lifetime. Its append-only journal, `_base_projection`, `_highest_applied_index`, hierarchical resource indexes, deck synchronization gate, and outstanding manual-reconciliation context all refer exclusively to that deck/account. Every `enqueue`, `poll`, recovery action, and synchronization attempt checks this binding; cross-deck or cross-account use is rejected locally **before any send**. The implementation may store account and deck binding in the journal header/metadata or an equivalently durable sidecar, but the association must be validated on replay and account switch.

A single `AuthenticatedClient` installation, identified by its stable `client_id`, may create several independent outboxes with separate durable paths; it supplies the bearer only at transport time for whichever account is currently authenticated. Sharing transport is **not** sharing permission, journal, projection, gate, or request identity state. A credential rotation for the same account does not change an outbox's binding; an account change cannot unlock or reuse another account's outbox. The existing global `_highest_applied_index` therefore stays meaningful **per outbox**, rather than comparing unrelated decks' indexes; callers do not need to implement a simultaneous multi-deck projection cache inside a single outbox.

**Legacy journal compatibility:** Existing pre-Stage-F journal files that already mix intentions from multiple decks must never be silently reinterpreted as one deck, merged under a guessed account, or truncated. At startup, detect inconsistent/boundaryless records and fail closed for protected synchronization until an explicit, lossless migration or operator-controlled recovery creates properly scoped journals. Preserve original files and request IDs. Existing single-deck journals may be adopted only when the caller can establish a trustworthy account/deck binding; unverified account ownership does not grant replay authority. This is a **client journal migration** issue, separate from ADR-018's historical server deck-schema migration.

## 8. Implementation constraints

- Prefer extending `ClientIntentionOutbox`'s eligibility scheduler and `JournaledClient` facade rather than modifying instructor-owned runtime or Raft core.
- Preserve existing intent ordering, structural-dependency relationships, settings FIFO lanes, materialized preconditions, lock-confidence gating, and append-only audit records.
- Reuse `AuthenticatedClient.call()` for `query_grants`, `query_operation_receipt`, and the updated protected reads so `_auth` remains transport-only.
- An `ACCESS_DENIED` on a protected read is not a successful content observation; it should trigger a self-permission refresh rather than populating the local projection with private data.
- Do not automatically release/reacquire editor locks on role changes: server enforcement and Stage C/F07 expiration/fencing remain responsible for those authority transitions.
- A private offline draft does not become a new published deck mutation merely because a user queues edits while disconnected. Publication remains the explicit ownership-gated boundary.

## 9. Acceptance criteria for implementation agents

1. A VIEW/NONE or UNKNOWN gate prevents all eleven ADR-014 post-publication EDIT operations from leaving either direct outbox or normal facade synchronization, while private-draft work remains intact.
2. Gate isolation is by user and deck; access loss on D1 does not suspend an unrelated permitted D2, and user/account switching cannot reuse a previous account's positive gate.
3. Gate closure is durable across restart; an old positive role is not trusted on restart; restoring EDIT does not automatically submit suspended edits.
4. The outbox retains the original serialized patches/arguments and request IDs after a denial or uncertain send; acknowledged ACCESS_DENIED does not loop or reuse the ID.
5. An in-flight mutation may complete before or after revocation according to Raft; no cancel/rollback behavior is introduced.
6. A recovered uncertain request uses only `query_operation_receipt` to establish a safe outcome while revoked; UNKNOWN remains unknown. A committed result cannot be counted twice.
7. The receipt-to-journal adapter maps success/rejection/unknown safely without exposing private result fields or synthesizing element lock authority.
8. A delayed positive preview cannot reenable a user after a later NONE or VIEW observation; a failed fresh query leaves the gate suspended.
9. Stage F protected content polling honors leader routing, bearer checks, ReadIndex, and subordinate read denials; incomplete polling never installs a misleading fully refreshed projection.
10. Existing Stage B–E tests for dependency scheduling, lock-confidence gating, settings FIFO, exact-ID retries, credential rotation/redaction, and model guarantees retain their intended semantics after targeted updates.
11. A user can retrieve an `ACCESS_DENIED` intention's saved non-secret patch through a read-only recovery view, while its `REJECTED_FINAL` lifecycle and original ID remain terminal and ineligible.
12. Two outboxes for D1 and D2 under the same authenticated installation keep different journal files, projections, indexes, and gates; a denial on D1 does not gate D2; enqueue/poll of a wrong deck is rejected before transmission.
13. Account change cannot reuse a prior account's outbox or positive role observation; credential rotation for the **same** account retains the correct binding; restart validates the persisted scope.
14. A historical mixed-deck journal is preserved and blocked from automatic Stage F replay pending an explicit, lossless split rather than silently dropping or cross-assigning intentions.
15. Legacy pre-Stage-F server deck handling remains fail-closed pending ADR-018; this ADR does not infer or migrate owners from old session data.

## 10. Decision closure

- **Q1: accepted A.** Recover terminal denied edit material through a read-only local view of existing append-only journal records; no duplicate recovery-draft store; no automatic replay.
- **Q2: accepted A.** One durable, account-bound outbox per deck; multiple outboxes share `AuthenticatedClient` transport but not their persisted state.
- **Open architectural questions for ADR-016: none.** Implementation agents may select internal method/serialization names as long as the tests and behavioral contracts above are met.

## References

- ADR-004 — Intent-Preserving Client Outbox
- ADR-007 — Quorum-Confirmed Authoritative Recovery Reads
- ADR-008 — Versioned Deck and Slide Settings
- ADR-012 — Account-Bound Deck Creation and Immutable User Ownership
- ADR-013 — Raft-Replicated Deck Access Grants and Independent Permission Versioning
- ADR-014 — Deck Operation Permission Matrix and Deterministic Authorization Boundary
- ADR-015 — Revocation Ordering, Authoritative Permission Discovery, and Permission-Safe Client Recovery
- Current repository: `client_journal.py`, `authenticated_client.py`, `app.py`, `lock_maintenance.py`, `command_dto.py`, `tests/test_client_journal.py`, `tests/test_stage_e_journal_auth.py`
