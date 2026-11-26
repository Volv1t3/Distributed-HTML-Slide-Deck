# ADR-015 — Revocation Ordering, Authoritative Permission Discovery, and Permission-Safe Client Recovery

- **Status:** Accepted for Stage F implementation planning; not yet implemented
- **Date:** 2026-11-26
- **Stage:** F — F04, with binding read-boundary decisions for F06 and client-recovery decisions for F05/F07
- **Related decisions:** ADR-004 (durable intention journal), ADR-007 (authoritative ReadIndex recovery), ADR-008 (versioned settings), ADR-009–011 (identity, authentication, credentials), ADR-012 (immutable creator ownership), ADR-013 (Raft grants and `access_version`), ADR-014 (per-operation permission matrix)
- **Reviewed implementation:** `Volv1t3/Distributed-HTML-Slide-Deck`, `master` at `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a` (2026-11-26)
- **Implementation scope:** New `query_grants` and `query_operation_receipt` read operations; leader-routed protected presentation reads; revocation/write ordering; permission-aware client synchronization; tests and project limitations. This decision does **not** claim code already exists.

## Context

ADR-012 fixes a published deck to one registered account owner. ADR-013 stores only currently active `VIEW`/`EDIT` participant grants inside each replicated deck, grants the owner exclusive mutation authority, and requires an independent version-fenced `access_version`. ADR-014 classifies eleven current presentation-edit operations as requiring at least `EDIT`, six public deck-read operation names (including the `refresh_element` alias) as requiring at least `VIEW`, and sharing operations as owner-only. It requires fresh admission and Raft-apply permission checks for mutations and does not permit unauthenticated deck reads.

The current implementation predates Stage F: `app.py` serves `get_deck_preview`, `get_slide`, `get_element`, and `read_lock_status` from a local replica without bearer authorization. `authoritative_refresh_element`/`refresh_element` are leader ReadIndex reads but likewise lack deck authorization. `client_journal.py` polls the preview and then subordinate slide, element, and lock endpoints, maintains monotonic `applied_index` observations, and persists individual business intentions. It currently has no per-deck access gate. `raft_core.py` already implements `begin_read_index`, `read_index_status`, and `finish_read_index` with a current-term quorum and applied-index barrier. `raft_core._apply_committed()` already persists request-result dedup records under a digest of `(client_id, request_id)`.

Stage F is the project's last planned backend implementation phase for now. A correct, bounded leader-read design is preferable to adding a new follower authorization protocol, active-notification subsystem, or cancellation coordinator. The historical eventual local-read *availability* guarantee is deliberately narrowed for protected shared-deck operations, while local private drafts remain usable offline.

## Decision

### D1. Committed Raft order determines edit/revocation outcomes

1. Effective permissions are determined by the committed deck state at the operation's deterministic Raft application, never solely by the client's cached role, submission time, lock possession, or previous leader admission.
2. If an otherwise-valid `update_element` (or other `EDIT` operation) is applied **before** a grant's removal or downgrade, the edit may succeed subject to the existing structural/version/lock checks. A later revocation does not retroactively roll it back.
3. If a removal or `EDIT`→`VIEW` downgrade is applied **before** an editing command, that command returns **`ACCESS_DENIED`**, with no mutation or private conflict payload, even if the old leader had admitted it earlier.
4. The above applies to `acquire_lock`, `release_lock`, `renew_lock`, and all eleven ADR-014 protected post-publication mutations. Internal `expire_lock` remains an independently fenced maintenance command; it is not a participant permission operation.
5. Do **not** introduce client-to-leader cancellation requests or attempt to remove already-proposed Raft entries. Once an operation is in flight, its outcome must be observed or recovered; cancellation cannot reliably prevent commitment.
6. A permission change modifies only the deck's grant map and `access_version` as specified by ADR-013; it does not mutate locks, slide data, element versions, or settings versions merely by virtue of changing a grant.

### D2. The leader is the read execution and authorization boundary

7. All protected shared-deck read routes are **leader-bound** and complete a **fresh quorum-confirmed ReadIndex** before returning presentation data: `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status`, `authoritative_refresh_element`, and its `refresh_element` alias. The new self-permission discovery operation `query_grants` follows the same rule. No new Raft read command is appended to the log.
8. A follower receiving one of these operations **redirects to the current leader**, or reports a no-leader/unavailable condition. It must not fall back to its local applied deck or grant snapshot, even when a client has an old positive permission cache entry.
9. The current leader (a) validates structural request fields; (b) extracts the wire-only bearer and validates current Stage E authentication, including `client_id` binding; (c) establishes a current-term ReadIndex quorum barrier; (d) waits until its FSM is applied through the returned fence; (e) resolves the deck's current effective role on that applied state; and (f) produces the read projection or a safe denial, without yielding control between authority evaluation and protected-data serialization. The exact order of bearer checking versus waiting may be optimized, but **no protected result is disclosed before the barrier and the final role check**.
10. `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status`, and `authoritative_refresh_element` require `OWNER`, `EDIT`, or `VIEW`; `NONE` returns **`ACCESS_DENIED`** without deck content or privileged existence/version information. The alias `refresh_element` is identical, not a bypass.
11. A request overlapping a revocation may linearize before it and legitimately return content, but a read begun **after a successful revocation response** cannot disclose protected deck content under the old grant. No claim is made that already-delivered client data can be erased.
12. The resulting protected read operations are **linearizable**, not just authenticated. Update `spec.py`/model tests accordingly. This is an intentional change from Stages B–D, scoped to shared-deck reads; the legacy Stage A `get` operation remains its existing eventual KV read and must never expose deck state.
13. On loss of quorum, current-leader uncertainty, or an uncompleted apply fence, fail closed with transport-level `QUORUM_UNAVAILABLE`, `NO_LEADER`, `READ_INDEX_UNAVAILABLE`, or equivalent existing unavailability error. **Never** return cached protected content disguised as a successful fresh read. Do not mislabel quorum failure as `ACCESS_DENIED` or grant `NONE`.
14. This decision avoids a trusted follower-to-leader authorization RPC, all-voter acknowledgments, push notifications, periodic server-permission broadcast, and a new replicated read index.

### D3. `query_grants` is authoritative self-permission discovery

15. Introduce **`query_grants`** as an authenticated, read-only, leader-routed, ReadIndex-backed operation. The minimum request input is `deck_id: UUID`; Stage E bearer and `client_id` bind the caller's account. The caller need not assert a `user_id`; if a user UUID is accepted for compatibility, it must exactly match the bearer-derived user. Never trust a caller-supplied account UUID as the subject of the query.
16. `query_grants` resolves **only the authenticated caller's effective access**: `OWNER`, `EDIT`, `VIEW`, or **`NONE`**. `OWNER` and `NONE` are effective *query classifications*, not values in `DeckAccessGrantDTO` (which remains `VIEW`/`EDIT`).
17. `query_grants` is intentionally exempted from the normal `VIEW` permission requirement for content reads: a properly authenticated caller may query its own access even after revocation. For both **(i)** an existing deck on which the caller has no grant and **(ii)** a nonexistent deck identifier, return the indistinguishable result `status="OK", effective_access="NONE"`. Do not disclose deck existence, owner, metadata, grant list, or `access_version` on this `NONE` path. The result can include generic response correlation but no resource-specific state.
18. For `OWNER`/`EDIT`/`VIEW`, an `OK` response may include `deck_id`, `effective_access`, `access_version`, and the authoritative `applied_index` used for the observation. This self-permission response never exposes another participant's grant. Owner-only full grant enumeration, if required, is a separate F08 endpoint with its own authorization contract.
19. A malformed request is `INVALID_REQUEST`; missing/revoked/invalid authentication is Stage E `REAUTH_REQUIRED` (not `NONE`); unavailable quorum is an unavailable error (not `NONE`). A VIEW participant may read deck content through the protected read endpoints; a downgraded VIEW participant receives `ACCESS_DENIED` only on prohibited **editing** operations, not on `get_slide`.
20. Successful `get_deck_preview` responses additionally include the current self-permission (`my_access_level` or `effective_access` — use one canonical name in the implementation specification), its `access_version`, and `applied_index` alongside the existing preview data. This is the normal passive discovery path during synchronization. A current authorization denial causes a client to request `query_grants` immediately when online.

**Illustrative authorized query result (additional response fields omitted):**

```json
{"status":"OK","deck_id":"<uuid>","effective_access":"VIEW","access_version":4,"applied_index":142}
```

**Illustrative safe no-access/nonexistent result:**

```json
{"status":"OK","effective_access":"NONE"}
```

### D4. Client permission observation and UI behavior

21. The authenticated client stores an in-memory **per-account, per-deck permission observation** holding effective access, `access_version` (when disclosed), and `applied_index` (when disclosed). The cache exists for UI responsiveness and observation ordering only; it is not a server authorization capability.
22. A later authoritative observation may replace an earlier one. A delayed response with a smaller comparable `access_version` or earlier applicable `applied_index` must never re-enable EDIT after a newer VIEW/NONE observation. Treat a `NONE` result without a deck-specific revision as a revocation/no-access observation and do not let an older in-flight preview overwrite it; implementation may use a client-local observation generation/sequence to order responses. Do not compare per-deck permission counters from different decks.
23. EDIT keeps presentation editing tools enabled (subject to the local synchronization gate). VIEW hides/disables presentation modification and lock controls while retaining authorized reading. NONE stops protected remote deck polling, hides deck data from the shared remote view, and leaves existing locally stored work untouched. OWNER additionally enables sharing controls. No client-side display state overrides server checks.
24. A successful preview provides normal permission discovery. An `ACCESS_DENIED` returned from an operation triggers immediate `query_grants` when possible; if unavailable, keep the last observation for display but **fail closed for new protected synchronization** until authority is refreshed. There are no active permission-change notifications.
25. ReadIndex observations are not authorization leases. Their freshness applies at the moment of the read, not indefinitely. Even if the UI displays EDIT, every later protected server operation independently authenticates and authorizes.

### D5. Separate, durable deck-level synchronization gate

26. Do **not** add a new per-intention `BLOCKED_BY_ACCESS` lifecycle state to the journal. The existing individual states (`PENDING_SYNC`, `IN_FLIGHT`, `COMMITTED`, `REJECTED_FINAL`, `SUPERSEDED`, `NEEDS_MANUAL_RECONCILIATION`) retain their domain meanings.
27. Add a **per-account, per-deck synchronization gate** outside the individual intention lifecycle. It records whether edit synchronization is suspended and whether explicit reconciliation is required after loss/restoration of EDIT. Gate *decisions* must survive app restart, while cached positive role observations must be revalidated after restart before remote submission. Authentication secrets never enter the gate or intention journal.
28. On observed `VIEW` or `NONE`, suspend new editing submissions for that deck. Preserve local unsynchronized intentions, patches, original request IDs, and any previously stored local projection; never silently discard or automatically resend them. Local private-draft editing remains governed by ADR-012's established signed-in context.
29. On `UNKNOWN`/unverified permission or unavailable quorum, default to suspended remote editing synchronization; no successful fresh authorization is inferred. On a subsequent authorized `EDIT`/`OWNER`, **do not** automatically release the reconciliation gate for work suspended during an access-loss interval.
30. After restored EDIT/OWNER authority, require the user to refresh current state, inspect pending local edits, and **explicitly** reconcile/restart sending each affected business intention. The existing precondition, revision, lock/fencing, and corrected-intention workflows apply. A genuinely corrected or newly submitted intent receives a **new** request ID. A previous terminal `ACCESS_DENIED` is never retried under its old ID.
31. An intention already submitted before the gate closes continues as an uncertain in-flight operation. The client does not send a cancellation RPC, does not report a rollback, and does not erase its request identity. Its final outcome is determined through the original response if received or the receipt protocol below.
32. A mere client retry loop must never turn `ACCESS_DENIED` into a permitted write; known unauthorized requests stop. `REAUTH_REQUIRED` may still require reauthentication as in Stage E, but reauthentication alone does not imply deck permission has been restored.

**Conceptual non-normative gate shape:**

```python
@dataclass
class DeckSyncGate:
    deck_id: UUID
    last_effective_access: Literal["OWNER", "EDIT", "VIEW", "NONE", "UNKNOWN"]
    access_version: int | None
    suspended: bool
    requires_explicit_reconciliation: bool
```

### D6. Minimal `query_operation_receipt` for uncertain editing mutations

33. Introduce **`query_operation_receipt`**, an authenticated, read-only, leader-routed ReadIndex lookup over **existing replicated committed request-result/dedup records**. It does **not** append a Raft entry or create a second replicated receipt table.
34. The lookup identifies the **original** request by its `original_request_id`; the requesting authenticated `client_id` is taken from the current transport identity. The lookup key is the already-used `sha256_json([client_id, original_request_id])`. The new receipt query has its own transport request ID, distinct from the original mutation request ID. No caller-supplied `client_id`, actor, or original operation is trusted to select another party's record.
35. The receipt is restricted to the **same original authenticated client installation and the same bound account**. Credential rotation for that installation is permitted; the new valid bearer must still resolve to the original user account. Verify the record's underlying committed command/log entry and original server-stamped actor identity before disclosure; if an older historical command lacks reliable actor attribution, do not disclose a receipt on that basis.
36. The receipt mechanism supports **exactly the eleven post-publication presentation mutations in ADR-014**: `create_slide`, `delete_slide`, `reorder_slides`, `create_element`, `update_element`, `delete_element`, `acquire_lock`, `release_lock`, `renew_lock`, `update_deck_settings`, and `update_slide_settings`. It excludes `grant_access`, `revoke_access`, `publish_deck`, bearer/token issuance or revocation, `expire_lock`, `kv_put`, and internal commands.
37. A receipt's payload contains no deck content, metadata, element/slide/lock fields, grant details, conflict snapshots, original result `value`/`details`, other users' identities, or bearer secrets. It may identify the original request and a generic outcome, but not any privileged result. It does not authorize a new mutation or reveal a current deck grant.
38. Receipt outcomes are **exact protocol keywords**:

| Receipt status | Exact meaning | Required client behavior |
|---|---|---|
| `COMMITTED_SUCCESS` | A committed dedup result exists for an eligible editing operation and its original business status is one of that operation's established success statuses (`OPERATION_POLICIES.success_statuses`, including `ALREADY_HELD` when applicable). | Record the operation as committed, preserving its original request ID. Do not reapply. |
| `COMMITTED_REJECTED` | A committed dedup result exists for an eligible editing operation but its original status is not a success. This includes committed `ACCESS_DENIED`, stale lock, version conflict, and other denied mutations. | Preserve local work as appropriate, record terminal result, and never silently retry the old request. No private failure details are disclosed. |
| `OUTCOME_UNKNOWN` | At the authoritative read point, no eligible committed result can be established for this client's original request. This **does not prove** that it failed, was never proposed, or will never commit. | Preserve uncertain original request; permit subsequent explicit receipt checks; do not resubmit the edit while unauthorized. |

39. A direct **pre-proposal** permission or credential rejection is not a committed Raft result and therefore cannot be inferred from a later receipt. When the client receives a direct `ACCESS_DENIED` result, it may record the terminal denial locally. When the response was lost before the client could tell whether proposal occurred, `OUTCOME_UNKNOWN` remains correct.
40. The authority to inspect a **minimal receipt** is intentionally narrower and independent of current deck content access: a later revoked participant may retrieve their own prior operation's outcome, but **must not** retrieve the original result payload through an ordinary deduplicated mutation replay. ADR-014's current-permission reauthorization of stored business results remains in force.
41. A missing or ineligible record must not expose another user's request, another operation type, grant state, or private deck existence. Malformed input and known ineligible operation requests may return `INVALID_REQUEST` without a stored result. The complete response schema and error masking should be locked by the implementation specification; the three outcome statuses above are normative.
42. On `OUTCOME_UNKNOWN`, the client can query again later, including after a leader transition. Do not introduce an automatic timer-triggered mutation resend or mark the intention terminal. A source request may commit after the receipt's read linearization point; later receipts can change from UNKNOWN to a committed result.

### D7. Error/status separation and validation precedence

43. Return keywords from ADR-013 and ADR-014 keep their established meanings. In particular, `ACCESS_DENIED` is a current role denial, `UNAUTHENTICATED` applies at deterministic committed mutation checking, and `REAUTH_REQUIRED` applies to missing/invalid bearer admission. `ACCESS_NOT_FOUND` is strictly an **absent grant on `revoke_access`**, not a general read/no-access status.
44. `query_grants` with no effective role returns `status="OK", effective_access="NONE"` (including unknown deck) rather than `ACCESS_DENIED`. Other protected reads return `ACCESS_DENIED` on insufficient access, with public existence masking defined in the implementation specification.
45. Fresh read failure is not an authorization result: an inability to reach a majority does not imply NONE, VIEW, or EDIT. It is an availability failure. No read succeeds on an obsolete follower permission snapshot.
46. A duplicate editing request's stored full result must not be re-exposed to a revoked caller by an exact-ID retry. `query_operation_receipt` is the expressly authorized status-only alternative.
47. Once a committed edit is reported successful through a minimal receipt, the client marks the matching intention COMMITTED but must not treat that as permission to fetch its content. That requires an independently authorized content read.

**Normative new protocol status catalog:**

| Identifier | Applies to | Meaning |
|---|---|---|
| `OK` | `query_grants`, permitted deck reads | Successfully observed authoritative self-role or authorized content; `effective_access="NONE"` is still `OK` for `query_grants`. |
| `ACCESS_DENIED` | Protected deck read or edit | Current authenticated user lacks the required role; no protected content/privileged conflict details. |
| `REAUTH_REQUIRED` | Read or mutation admission | Missing/invalid/revoked bearer; no protected result. |
| `QUORUM_UNAVAILABLE` | Leader ReadIndex path | No fresh quorum result; fail closed without asserting an access role. |
| `COMMITTED_SUCCESS` | Receipt | Supported original editing request has a committed successful result. |
| `COMMITTED_REJECTED` | Receipt | Supported original editing request has a committed rejected result. |
| `OUTCOME_UNKNOWN` | Receipt | No conclusive committed result observed; preserve uncertainty. |
| `INVALID_REQUEST` | Request parsing/unsupported receipt target | Malformed or ineligible request structure, without confidential disclosures. |

Other existing transport outcomes (e.g. `NO_LEADER`/redirect/`READ_INDEX_UNAVAILABLE`) retain their existing meanings. This ADR does not rename any existing editing success, lock error, or version-conflict status.

### D8. Failure, availability, and project limitations

48. An authorized protected read started after a completed revocation is forbidden to return new protected content, including from lagging or isolated followers. Reads may legitimately return content before a concurrent revocation linearizes; previously cached/downloaded content cannot be revoked retroactively.
49. A majority of Raft voters is required for fresh shared-deck reads; the three-voter cluster tolerates one unavailable voter but not loss of majority. This is a documented **availability regression** from the previous follower-local eventual read implementation and an accepted non-production optimization tradeoff.
50. Local-first draft data and durable pending intentions are not destroyed by loss of quorum or grant. Client cached permission/UI information may be stale or unavailable; it cannot authorize protected server operations, and positive permissions are rechecked after restart.
51. Stage F does not include push permission notifications, websocket subscriptions, follower-delegated authorization, 2PC, all-replica commit acknowledgments, instant deletion of previously downloaded data, automatic conflict resolution, distributed operation cancellation, account-wide cross-device receipt lookup, or production-scale leader read optimization.
52. Implemented service behavior and exact privacy masking must be proven by tests before describing the deck read path as secure. An ADR alone is not an implemented guarantee.

## Implementation alignment to current code

| File / mechanism | Required Stage F changes | Preservation requirement |
|---|---|---|
| `app.py` `on_request()` | Route all six existing protected read names plus `query_grants` and `query_operation_receipt` to leader-only, authenticated ReadIndex completion. Use safe redirects; do not use `_presentation_read()` directly for unauthenticated deck data. | Preserve login, account/token flows, Stage A KV routes, request auditing/redaction, and single-threaded node semantics. |
| `app.py` ReadIndex waiters | Reuse/generalize `_authoritative_element_refresh()` and `_reply_authoritative_refreshes()` while keeping per-request term, read context, apply fence, and cancellation-on-leadership-loss. | Existing authoritative element refresh and alias behavior. |
| `state_machine.py` | Add effective-role resolution from `metadata.owner_user_id` and `access.grants`; decorate safe read projection with self-role where appropriate; recheck EDIT/OWNER during committed mutations. | Deterministic FSM replay; no mutation on denial; existing element/slide/settings/lock preconditions. |
| `raft_core.py` | No algorithm changes expected; reuse existing quorum/read status and already persisted `request_results`. | No new Raft RPC or log entry for reads. |
| `command_dto.py` / `auth_dto.py` | Typed validation for read requests and sharing commands; forbid client actor claims; keep bearer secrets out of state. | Existing command-envelope schema and canonical business fingerprints. |
| `client_journal.py` | Add separate durable per-deck gate, authority-aware send scheduling, safe no-regression observations, and uncertain receipt recovery. | Existing intention lifecycle and exact request identities; no automatic discard/replay. |
| `authenticated_client.py` | Use existing bearer injection for both new read calls; no persistently logged bearer/credential material. | Current redaction and credential rotation behavior. |
| `spec.py` and tests | Mark protected reads linearizable, test all ADR-014 role/operation combinations, race orders, partitions and receipt privacy. | Stage A legacy semantics and historical deterministic replay requirements. |

## Required acceptance scenarios

1. **Matrix completeness:** all ADR-014 protected reads use `VIEW+`, all eleven mutating operations use `EDIT+`, grants use OWNER only, and special publication uses ADR-012 creator matching. Aliases and direct node requests have no bypass.
2. **Revocation racing write:** test `update_element → revoke_access` and `revoke_access → update_element`; verify first may succeed with valid lock/version and second returns `ACCESS_DENIED`, with deterministic replica convergence.
3. **Downgrade:** `EDIT`→`VIEW` causes later editing and `renew_lock` denial but still permits authorized `get_slide`/`get_element` content reads.
4. **Stale follower:** a direct protected read sent to a follower lagging before revocation must redirect or return an unavailable result, never release content; leader read after revocation denies.
5. **ReadIndex failure:** isolated leader, new election, lost majority, expired read context and un-applied commit fence never result in successful protected reads or a false `NONE` response.
6. **Self-query privacy:** `query_grants` returns OWNER/EDIT/VIEW with safe revision information and returns indistinguishable `OK/NONE` for no grant and nonexistent deck without exposing `access_version` or metadata.
7. **No-op grants:** `ACCESS_UNCHANGED` and `ACCESS_NOT_FOUND` retain unchanged access revision and cannot bypass a stale expected revision; do not conflate with `query_grants` NONE.
8. **Client observation ordering:** a late positive preview cannot overwrite a newer VIEW/NONE; UI updates are not treated as authorization; on a failed fresh query, synchronization fails closed.
9. **Durable gate:** downgrade/revoke preserves journal intentions and blocks sending; restart does not automatically re-enable; restored EDIT still requires explicit reconciliation.
10. **In-flight request:** commit-before-revoke returns success where applicable, revoke-before-commit returns denial; no cancellation or rollback is introduced. Lost responses keep original request IDs.
11. **Receipt successes/rejections:** receipt returns minimal correct status for each of eleven eligible operations, including existing `ALREADY_HELD` as a success and `ACCESS_DENIED` as rejected, with no original result `value`/`details` or deck content.
12. **Receipt authorization:** original same client/account can retrieve after bearer rotation and deck revocation; a different client or actor cannot. A known ineligible request cannot access its full result via the receipt path.
13. **Receipt uncertainty:** no committed record yields `OUTCOME_UNKNOWN`; a later commit may produce definitive status; an unknown result never drives resubmission while unauthorized.
14. **Historical regression:** no change to Raft wire protocol; Stage A KV proof routes and original domain mutation semantics remain intact. Pre-v3 decks require separate F08 deterministic migration/quarantine and may not be exposed as owner-private through guessed ownership.

## Alternatives considered and rejected

- **Push notification:** more operational machinery without replacing at-apply checks; Stage F uses discovery through synchronization and explicit query.
- **Keep protected preview/slide reads fully local/eventual:** stale isolated followers can disclose content after committed revocation; violates ADR-014's intended privacy boundary.
- **Leader authorizes but follower serves content:** preserves eventual content freshness but adds a new trusted inter-replica exchange and complicated check/use fencing. Deferred beyond this project stage.
- **All-replica commit acknowledgments or 2PC:** does not solve stale isolated-read authority and needlessly harms write availability.
- **New Raft log entry for every grant/receipt query:** unnecessary because ReadIndex already confirms a committed read point.
- **Automatic deletion of unsynced work:** violates durable-intention/local-first commitments.
- **Automatic replay immediately after re-grant:** can submit outdated, no-longer-intended modifications; manual reconciliation is required.
- **New per-intention `BLOCKED_BY_ACCESS` state:** duplicates a deck-level condition and complicates stable journal replay.
- **Client cancellation of in-flight writes:** cannot undo an already committed Raft entry and would demand a new cancellation protocol.
- **Full dedup result replay to revoked users:** leaks protected conflict or element data; use minimal receipts instead.
- **Receipts for all Raft operations / account-wide devices:** broader security/recovery contracts not justified by Stage F's existing editing-journal need.
- **Treat `OUTCOME_UNKNOWN` as failure:** a receipt is a snapshot of committed evidence, not proof that an in-flight proposal cannot later commit.

## Outstanding scope, not a reopening of F04

- **F05:** implement the permission observation cache, durable deck sync gate, explicit post-restoration reconciliation and request-identity handling. Core behavior is decided here; exact client storage/API shape is an implementation-spec choice.
- **F06:** implement the leader ReadIndex read protocol and response masking for all protected reads. Architecture is decided here; test for network partition, follower lag, and term changes.
- **F07:** decide the minimum policy for locks left behind by a downgraded/revoked editor session. Recommended implementation default: deny all subsequent user-originated EDIT operations, including renewal/release; leave an existing lock to expire under Stage C's fenced internal `expire_lock`, without an automatic lock-revocation mutation. Client-local work remains intact under F05.
- **F08:** define owner-only grant enumeration (if required by UI/version-conflict reconciliation), minimal registered-target discovery, and fail-closed deterministic migration/handling of schema-v2 ownerless decks. Do not derive ownership from a session ID or old unauthenticated command.

## References

- `docs/adrs/ADR-012-account-bound-deck-creation-and-immutable-ownership.md`
- `docs/adrs/ADR-013-raft-replicated-deck-access-grants-and-independent-permission-versioning.md`
- `docs/adrs/ADR-014-deck-operation-permission-matrix-and-authorization-boundary.md`
- `app.py`, `state_machine.py`, `raft_core.py`, `command_dto.py`, `auth_dto.py`, `client_journal.py`, `authenticated_client.py`, `spec.py`
- `tests/test_read_index.py`, `tests/test_runtime_integration.py`, `tests/test_raft_replication.py`, `tests/test_stage_e_journal_auth.py`
