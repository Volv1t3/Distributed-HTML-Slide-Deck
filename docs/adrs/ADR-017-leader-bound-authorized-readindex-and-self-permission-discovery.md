# ADR-017 — Leader-Bound Authorized ReadIndex, Self-Permission Discovery, and Safe Read Results

- **Status:** Accepted (architecture decision; implementation pending Stage F)
- **Date:** 2026-11-26
- **Stage:** F — F06: protected shared-deck read implementation and authoritative permission discovery
- **Depends on:** ADR-007 (ReadIndex recovery); ADR-009–011 (account and bearer binding); ADR-012 (published owner); ADR-013 (access state); ADR-014 (role/operation matrix); ADR-015 (leader-only linearizable read boundary, `query_grants`, safe receipts); ADR-016 (per-deck client polling/gate)
- **Verified implementation baseline:** `Volv1t3/Distributed-HTML-Slide-Deck`, `master`, commit `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a`, inspected 2026-11-26
- **Implementation status:** Design only; no Stage F protected-read authorization is claimed to exist at the baseline commit.

## 1. Context and code-level gap

The current code's existing deck content reads are not secure under a committed grant revocation:

| Implementation entry | Actual baseline behavior | Required change |
|---|---|---|
| `app.py:App.on_request()` | Directly dispatches `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status` to `_presentation_read()` | Route through leader-only bearer-authenticated ReadIndex authorization |
| `app.py:App._presentation_read()` | Reads `KeyValueStateMachine(self.engine.state.fsm)` on **whatever replica was contacted**; does not check bearer, account, or grants | Never return protected v3 deck content from follower-local FSM state |
| `app.py:App._authoritative_element_refresh()` | Begins leader ReadIndex for `authoritative_refresh_element` and `refresh_element`; no Stage F bearer/role check | Use the same authorized read pipeline, retaining `min_commit_index` fence |
| `app.py:App._reply_authoritative_refreshes()` | Waits for current-term quorum and applied index; then calls `get_element()` | Add current bearer and effective-role verification **after** barrier, before projection |
| `app.py:App._start_auth_admitted_command()` / `_progress_auth_admitted_commands()` | Shows already-existing volatile token extraction, current-term ReadIndex, and current authenticated-client record checking for guarded mutations | Reuse/factor behavior for reads; do not store bearer in Raft or persisted event history |
| `raft_core.py:begin_read_index()`, `read_index_status()`, `finish_read_index()` | Already implements current-term quorum correlation and the local applied-through-fence check | Reuse as-is; no new Raft read command, message, or algorithm change |
| `state_machine.py:get_deck_preview/get_slide/get_element/read_lock_status()` | Project typed deck data without deck ACL; missing decks return `NOT_FOUND` | Authorize *before* these content-producing methods; add safe effective-self-role projection |
| `client_journal.py:ClientIntentionOutbox.poll()` | Polls preview, slides, elements, locks in sequence; `JournaledClient.poll_if_due()` targets `local_replica` | Send to leader; treat each subordinate read as separately protected; do not install partial denied projections |
| `spec.py:GUARANTEE` | Four ordinary deck reads marked `eventual`; refresh marked `linearizable` | Mark all protected shared-deck read names `linearizable`, add new read operations and model/test coverage |

This ADR specifies **how to implement** the already-accepted ADR-015 authorization and consistency decisions. It must not dilute those guarantees in the name of preserving the prior follower-local read path.

## 2. Protected read surface and roles

The following public operation names must use a **fresh leader-owned ReadIndex** and authorize against the applied state at the barrier:

| Public operation | Minimum current role / identity | Result scope |
|---|---|---|
| `get_deck_preview` | `VIEW` (OWNER/EDIT/VIEW) | Existing preview + caller `effective_access` + `access_version` + `applied_index` |
| `get_slide` | `VIEW` | Existing slide projection |
| `get_element` | `VIEW` | Existing element/version/lock projection |
| `read_lock_status` | `VIEW` | Existing element-lock observation (not lock authority) |
| `authoritative_refresh_element` | `VIEW` | Existing authoritative element refresh, respecting caller `min_commit_index` |
| `refresh_element` | `VIEW` | Exact semantic alias, not separate weaker permission path |
| `query_grants` | Any **valid authenticated actor**, including current NONE | Only the caller's own effective permission, not the grant map |
| `query_operation_receipt` | Same original **client installation + authenticated account**, independent of current deck grant | Only minimal outcome of an eligible earlier editing request |

`query_grants` and `query_operation_receipt` are special authenticated reads and are **not** general deck-content access. They cannot be used to obtain a deck's owner, slides, elements, other participants, or contents. The existing Stage A `get` remains a local/eventual generic KV read and must not be repurposed to expose protected deck state. The local/private unpublished draft remains a distinct, account-bound client workflow.

## 3. Shared leader-bound read completion pipeline

For all eight operation names above:

1. **Dispatch/leader gate.** `App.on_request()` routes protected reads to a single audited guarded-read family before reaching `KeyValueStateMachine` projections. Nonleaders redirect to a known current leader or fail with `NO_LEADER`; there is no follower-local success fallback. Validate allowed operation, typed/UUID input, required IDs, and forbidden caller-supplied actor stamp fields (`authenticated_actor_user_id`, `credential_id`, `credential_generation`).
2. **Volatile bearer extraction.** Require `_auth.access_token` and the wire `req.client` identity. Validate the token against the current replicated Stage E credential record bound to that client (digest comparison, revoked flag, matching user). Keep raw token only in volatile application memory while the request is pending, as in the existing Stage E admission waiter; remove it from logs/events and any FSM command or journal.
3. **Quorum barrier.** Start `engine.begin_read_index(min_commit_index=floor)` on the current leader. The floor is zero for ordinary reads and permission discovery, and the caller-provided validated nonnegative `min_commit_index` for authoritative refresh. A pending waiter binds a unique request, client, canonical operation, parsed arguments, captured leader term, and ReadIndex context. It must be deleted and `finish_read_index` invoked on success, failure, or leadership change.
4. **Authoritative completion.** Proceed only on `ReadIndexStatus.READY`, when leader term still matches and local `last_applied` has reached the returned fence. Recheck the current bearer/actor binding and compute the effective deck permission **against this applied FSM state**. A bearer admitted before the barrier is not a read authorization lease.
5. **Non-yielding authorization+projection.** In the existing serialized event-loop completion, evaluate role and prepare the safe result without yielding to a concurrent application of a new committed grant/revocation. Return only authorized content (or a masked denial/special safe result). ReadIndex remains a read: do not call `engine.propose()` and do not append a Raft entry.
6. **Fail closed.** Invalid or missing bearer is `REAUTH_REQUIRED`. No leader, stale term, lost majority, timeout, or incomplete apply fence returns redirect/unavailable (`NO_LEADER`, `QUORUM_UNAVAILABLE`, `READ_INDEX_UNAVAILABLE` or equivalent), never stale content and never a falsely inferred `NONE`.

The implementation may share helpers between `_progress_auth_admitted_commands()` and new protected-read waiters. It must not make auth state or bearer material part of the command fingerprint, request history, `App._event()` payload, or durable journal. Do not rely on a preview's earlier positive role to authorize a later `get_slide` or `get_element`.

## 4. `query_grants` and canonical permission observation

**Request:** operation `query_grants`, with `deck_id: UUID` and existing Stage E transport-bound bearer/client identity. No caller-declared subject UUID is required and none should be trusted to select a different account.

**Authorized current role:** `status="OK"`, `effective_access` in `OWNER|EDIT|VIEW`, `deck_id`, current `access_version`, and `applied_index`.

**No current role or unknown deck:** `status="OK", effective_access="NONE"`, indistinguishable between a real inaccessible deck and a nonexistent deck. Do **not** include owner, metadata, roster, deck `access_version`, or other resource-specific fields. This is a self-permission report, not an `ACCESS_DENIED` exception.

**Recommended canonical wire field:** `effective_access` for both `query_grants` and the permission-bearing `get_deck_preview`, aligning with ADR-015's illustrative payload. Alternative spelling `my_access_level` is not needed unless an existing shipped client already requires it (none exists in the reviewed baseline).

The client orders positive observations using `access_version`/`applied_index`, and protects a negative `NONE` observation from stale earlier positive responses with the ADR-015/016 local request-generation fence. It does not invent a deck-specific version for NONE. The permission cache and gate never replace server-side checks.

## 5. Existing protected content projections and denial behavior

- A present v3 deck is readable only to its owner or a current `VIEW`/`EDIT` grant holder. The entrypoint performs that check **before** obtaining a typed content projection. `NONE` has no content, version, lock, or metadata disclosure.
- Once authorized to the deck, preserve the existing `KeyValueStateMachine` domain statuses for missing slide/element, tombstones, and lock observations. Content reads retain their `applied_index` as an observation watermark, not a transferable authorization token.
- A revocation committed before a new protected read starts makes that read fail authorization on its ReadIndex snapshot. A read racing a revocation may legitimately linearize before it and succeed. No retroactive deletion of already cached client content is claimed.
- Keep `refresh_element` exactly equivalent to `authoritative_refresh_element`, including bearer, authorization, `min_commit_index`, term fencing, and response handling. Never forward the alias to old unauthenticated `_authoritative_element_refresh` behavior.
- For `get_deck_preview`, the authorized response includes `effective_access` and `access_version`. The raw metadata's `owner_user_id` follows the authorized preview only; unknown/nonparticipants cannot receive it through content endpoints.
- **Accepted Q1 — missing-deck response masking:** after successful authentication and ReadIndex, both an unknown `deck_id` and an existing deck for which the caller has `NONE` access return the **same outward `ACCESS_DENIED` result** on all six protected content operations. Use the same outer response envelope, result schema, status, and non-resource-specific details (prefer `{"ok":true,"result":{"status":"ACCESS_DENIED"}}`); do not return `NOT_FOUND`, a deck-specific index/revision, ID, owner, metadata, or any field that distinguishes the two cases. The authorization/projection helper must not leak the distinction through normal return values. Timing or other side channels require separate engineering review; uniform return values are the normative guarantee. An authenticated user with VIEW/EDIT/OWNER for an **existing** deck may still receive `NOT_FOUND` or `SLIDE_DELETED` for a missing child resource.

## 6. `query_operation_receipt` — status-only ReadIndex lookup

Apply ADR-015 D6 without adding a second replicated receipt table:

1. The client submits a new read request ID plus `original_request_id`; the server derives the original lookup key from **current authenticated** `req.client` and `original_request_id` using `sha256_json([client_id, original_request_id])`. It must not accept a caller's claim to another client's original ID or a supplied original actor.
2. After the same leader/ReadIndex/bearer barrier, find the `request_results` record and its corresponding original committed log entry. Verify the underlying original actor stamp binds the **same user** now authenticated on that client. For older un-attributable commands, do not disclose results.
3. Whitelist precisely the eleven post-publication editing operations in ADR-014/015; exclude publish, grants, auth management, internal expiration, and generic KV. Map the original dedup record's **operation-specific** established success status to `COMMITTED_SUCCESS`; other committed outcomes to `COMMITTED_REJECTED`.
4. If no eligible attributable committed record is established, return **`OUTCOME_UNKNOWN`** without exposing record existence/type, original status, data, failed lock/version, conflict snapshots, or deck existence. This can later become conclusive; it does **not** mean the operation failed or will never apply.
5. This receipt is an observation only: it cannot authorize a mutation or grant a lock. Replaying the original editing command under a revoked role must not divulge the stored full result. The client uses the ADR-016 receipt adapter, not normal mutation-success policies, to update its local journal.

**Receipt response:** `{"status":"COMMITTED_SUCCESS"}` / `{"status":"COMMITTED_REJECTED"}` / `{"status":"OUTCOME_UNKNOWN"}`, optionally including the caller's original request ID as correlation only. No result `value`, `details`, `deck_id`, or actor identity is included.

## 7. Spec, transport, and client integration

- In `spec.py:GUARANTEE`, change `get_deck_preview`, `get_slide`, `get_element`, and `read_lock_status` from `eventual` to `linearizable`; retain `authoritative_refresh_element` as `linearizable`; include `refresh_element` and `query_grants` in read operations and models, plus status-only `query_operation_receipt`.
- In `client_journal.py`, have `JournaledClient.poll_if_due()` use the authenticated leader transport path rather than `_local_replica`. Preserve the per-outbox applied-index monotonicity and explicit denied-partial-poll handling from ADR-016.
- `AuthenticatedClient.call()` supplies the current token only when sending; do not copy `_auth` into local durable editing intentions or recovery views. Treat a protected read's `REAUTH_REQUIRED` as an authentication problem, a content `ACCESS_DENIED` as a permission problem, and quorum/leader failure as availability uncertainty.
- Update deterministic/reference-model and runtime tests that formerly assumed unauthenticated follower-local deck reads. Do **not** weaken role/ReadIndex requirements solely to satisfy Stage B–D legacy tests. Preserve Stage A KV read guarantees.
- No HTTP push, subscription, new Raft voter messages, token in snapshots, all-replica authorization acknowledgement, or cancellation protocol is introduced.

## 8. Acceptance criteria for implementation agents

1. A direct follower request for each protected read redirects/fails, including while its local FSM still contains an old EDIT grant; it never returns content.
2. The current leader returns no protected data on lost quorum, no current-term entry barrier, stale/changed term, incomplete apply fence, or malformed bearer; no false `NONE` is synthesized from unavailability.
3. OWNER, EDIT, VIEW can invoke all six existing protected read names; NONE cannot. VIEW can still read following EDIT→VIEW while all editing/lock mutations are blocked by ADR-014.
4. All six existing read names, including `refresh_element`, use the exact same bearer and `VIEW` guard; no alias or direct `App` request bypass.
5. `query_grants` returns only the caller's effective role, with safe revision metadata on positives and indistinguishable `OK/NONE` for inaccessible/nonexistent deck identifiers.
6. `get_deck_preview` carries canonical `effective_access`, current `access_version`, and `applied_index` only when caller is authorized.
7. A read begun after a confirmed committed revoke never succeeds by consulting stale grant state. A racing read may succeed only when its read linearization precedes the revoke.
8. Receipt success/rejection/unknown are determined from persisted dedup results and original authenticated actor, for only the eleven eligible operations and the same client/account; revoked editors learn no deck content via receipts or old dedup replies.
9. Delayed positive preview cannot override a newer VIEW/NONE; an unauthorized subordinate read blocks installation of a partial snapshot; a separate outbox's deck projection is unaffected.
10. `spec.py` and end-to-end tests assert the new linearizable read guarantees and maintain legacy `get` eventual behavior, with no Raft core protocol modification.
11. A nonexistent deck and an inaccessible real deck have **identical outward `ACCESS_DENIED` responses** across all six protected content reads; an authorized user may distinguish missing slide/element resources only inside a deck they can read.

## 9. Resolved question and implementation handoff

**Q1 — Missing-deck existence masking: Option A accepted, 2026-11-26.** An authenticated caller with no current access cannot distinguish an unknown deck identifier from an existing private deck based on the outward result of a protected content read. All six protected content operations return identical `ACCESS_DENIED` envelopes for both cases. The `query_grants` self-permission exception remains `OK/NONE` for both, as established in ADR-015. Missing child-resource statuses are available only after successfully authorizing an existing deck.

**No remaining ADR-017 architecture questions.** The implementation agent may choose the internal helper/waiter types, subject to the protocol, non-disclosure, and tests above. Preserve the existing Stage A KV behavior and Raft ReadIndex algorithm. Separate ADR-018 covers lock expiration and legacy v2 deck access policy.

## References

- ADR-007 — Quorum-Confirmed Authoritative Recovery Reads
- ADR-012 — Account-Bound Deck Creation and Immutable User Ownership
- ADR-013 — Raft-Replicated Deck Access Grants and Independent Permission Versioning
- ADR-014 — Deck Operation Permission Matrix and Deterministic Authorization Boundary
- ADR-015 — Revocation Ordering, Authoritative Permission Discovery, and Permission-Safe Client Recovery
- ADR-016 — Permission-Aware Client Synchronization and Durable Recovery
- Current repository: `app.py`, `raft_core.py`, `state_machine.py`, `command_dto.py`, `authenticated_client.py`, `client_journal.py`, `spec.py`, and affected tests
