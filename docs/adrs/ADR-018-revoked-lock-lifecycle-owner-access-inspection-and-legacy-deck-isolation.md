# ADR-018 — Revoked-Editor Lock Lifecycle, Owner Access Inspection, and Legacy v2 Deck Isolation

- **Status:** Accepted (architecture decision; implementation pending Stage F). F07 settled; F08 Q1 B1, Q2 A, and Q3 B accepted (three questions total).
- **Date:** 2026-11-26
- **Stage:** F — F07 (lock/permission lifecycle) and F08 (owner access inspection and backward-compatibility boundary)
- **Depends on:** ADR-001 (element locks), ADR-004 (client intention journal), ADR-009–011 (account/credential boundaries), ADR-012 (v3 immutable owner), ADR-013 (replicated ACL and `access_version`), ADR-014 (role-per-operation matrix), ADR-015 (Raft ordering and no forced lock mutation), ADR-016 (per-deck journal/gate), ADR-017 (leader-bound authenticated ReadIndex and uniform inaccessible/unknown-deck denial)
- **Verified source baseline:** `Volv1t3/Distributed-HTML-Slide-Deck`, `master`, commit `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a`, reviewed 2026-11-26
- **Implementation status:** Stage F is design-only at this baseline. Current published deck schema is v2, without trusted `owner_user_id` or replicated deck grants.

## 1. Context and implementation facts

There are three related closure problems, not three opportunities to redesign Raft:

1. Revoking EDIT permission may leave an existing lock owned by the now-revoked editor's session. The lock must not confer authority, and cleanup must remain correct if the leader changes.
2. `query_grants` intentionally returns only the **caller's own** role. The owner may need a separate read to inspect the grant map and resolve `ACCESS_VERSION_CONFLICT` without exposing the roster to other users.
3. The pre-Stage-F snapshot schema v2 has no verified account owner in its deck metadata. Stage F must support explicit v2→v3 lineage where committed publication provenance genuinely establishes the account owner; otherwise it must quarantine the old deck rather than guess.

Concrete baseline:

| Source | Observed behavior | Stage F implication |
|---|---|---|
| `state_machine.py:_acquire_lock/_release_lock/_renew_lock` | Existing commands guard by a session-owned fencing token; `renew_lock` increments `lease_epoch`; there is currently no deck ACL | Add mandatory deterministic EDIT check *before* session/lock/version inspection (ADR-014) |
| `state_machine.py:_expire_lock` | Internal command compares `expected_token` and `expected_lease_epoch`, and returns `EXPIRED` or `NO_OP_STALE_EXPIRY` | Keep autonomous, permission-independent cleanup after an editor loses access |
| `lock_expiration.py:LockExpirationCoordinator` | Leader-local inactivity tracking; inherited locks get a grace period after leadership change; expiration proposals have generation-dependent deterministic request IDs | Reuse existing expiry flow; no permission-triggered Raft cancellation/rewrite |
| `lock_maintenance.py:RenewalWorker` | Background worker keeps renewing when selected; `LockMaintenanceService.deregister_lock` optionally performs a release callback | On known permission loss, stop renewal; do not send a new user-originated `release_lock` merely to tidy up |
| `command_dto.py:PresentationSnapshotDTO` | Strict `schema_version: Literal[2]`; metadata owner currently means `owner_session_id`, not account UUID | New schema v3, historical v2 replay, and an explicit provenance-gated migration need a version-aware compatibility strategy |
| `state_machine.py` published-deck state | Records `schema_version`, metadata stripped of `owner_session_id`, slides/locks/etc.; no trusted owner | Never synthesize `owner_user_id` on read or recovery; migrate only on verified committed provenance, otherwise quarantine |
| `app.py:App.on_request()` | Has no public grant-roster read, registered-user search, deck listing, or deck deletion | New F08 read must be explicit, bearer-gated, and bounded; do not silently add directory/global discovery |
| `raft_core.py` | Durable `request_results` deduplication and current-term ReadIndex already implemented | Reuse existing commit and read guarantees; no new Raft wire primitive |

## 2. F07 — Normative lock policy (already determined by accepted ADRs)

This subsection **records existing accepted policy**, not a new decision to re-open:

1. `grant_access`/`revoke_access` alter only the deck's grant map and independent `access_version`. They **must not** mutate `locks`, `fencing_counters`, `element.version`, deck structural revision, or settings revisions as a side effect (ADR-013/015).
2. An externally submitted `acquire_lock`, `release_lock`, `renew_lock`, `update_element`, or `delete_element` must pass current user EDIT/OWNER authorization when first applied, even if the caller still possesses an old session/fencing token. Revoke-before-edit yields `ACCESS_DENIED`; edit-before-revoke can succeed subject to normal preconditions. Follow ADR-014's credential-before-role-before-lock/version error precedence.
3. An existing lock **remains committed state** after downgrade/revocation. It cannot authorize a mutation and may temporarily block another authorized editor until Stage C expiration. Do not introduce a new permission-revocation cancellation command or forced `expire_lock` path.
4. The existing leader-only `LockExpirationCoordinator` should propose `expire_lock` on inactivity with the current `(fencing_token, lease_epoch)` generation. The FSM must apply it without EDIT permission; stale expiry proposals return `NO_OP_STALE_EXPIRY`, including after renewal or reassignment.
5. A client that observes `VIEW`/`NONE` or loses trusted current permission stops initiating *new* lock renewals and treats its previous lock authority as unusable for editing. Use `deregister_lock(element_id, release_callback=None)` or an equivalent stop path, to avoid generating an unauthorized release request. Any renewal already in flight has an ordinary Raft-ordered outcome and is **not** canceled.
6. Upon later restoration of EDIT, do not assume a previous lock survived, still belongs to the same session, or retains its prior fencing token. Explicit reconciliation obtains a fresh lock and authoritative state, subject to ADR-016. The mere grant restoration never resurrects locally cached lock authority.
7. A new leader's lock-expiration grace window is intentionally conservative. The project does not promise instant lock cleanup after revocation or cleanup during lost majority; those are availability limits, not authorization exceptions.

**F07 implementation acceptance:** revoke while lock held; no direct lock rewrite; denied renew/update/release after revoke; no unexpected lock reveal in denial; expiry through existing fence; stale expiry no-op after new lease; leadership change grace; client renewal worker stops; pending local edits remain intact; owner/other active editor needs normal lock mechanics, not privilege-based lock theft.

## 3. F08 — Owner-only access-state read (Q2 accepted: Option A)

The public `query_grants(deck_id)` operation from ADR-015/017 is intentionally self-only and **must not** be expanded to return the roster. To support owner reconciliation after `ACCESS_VERSION_CONFLICT`, this ADR **requires** a distinct read-only operation, named `query_access_state`:

- **Request:** `deck_id` plus existing authenticated Stage E client/bearer; no actor, target-user, or owner UUID assertion.
- **Execution:** current leader, fresh ReadIndex, credential validation, then owner role check against the applied FSM. No Raft log append.
- **Authorized OWNER response:** `status="OK"`, `deck_id`, `access_version`, `applied_index`, and the current `grants` (target registered user UUID and `VIEW|EDIT`), sorted by target UUID for stable tests. `OWNER` is implicit and is not a grant entry. No password, bearer, raw credential record, or unrelated account catalog entries.
- **Not-owner or missing deck:** exactly the same public `ACCESS_DENIED` envelope, with no deck metadata/version/roster or existence clue, as ADR-017's protected reads.
- **Unavailable state:** fail closed with existing leader/quorum errors rather than returning a stale roster.
- **Mutation isolation:** reading access state never increments `access_version`; changing the grants still requires the owner's separate, version-fenced `grant_access` or `revoke_access` Raft command under ADR-013.

`query_access_state` is therefore an OWNER-only management read and **not** a self-permission query. Its name is normative for Stage F. Implement it with the ADR-017 shared guarded-read pipeline; no optimistic client-maintained UUID/grant registry is required for authoritative sharing management.

## 4. F08 — Conditional, deterministic v2→v3 migration (Q1 accepted: B1)

**Accepted Q1 — B1:** Retain an explicit versioned v2→v3 migration capability for historic published decks whose **original, successful, Raft-committed publication** establishes a registered account owner through trustworthy Stage E authentication provenance. A deck without that proof is retained as v2 and quarantined. This provides schema lineage without a user-claim/takeover mechanism.

**Why v2 history matters:** ADR-005/Stage D advanced the current published snapshot DTO to version 2, but intentionally rejected v1 inputs and did not provide a general runtime importer for historical v1 publications. Stage F goes further: a v3 migration assigns a security principal, which requires proof beyond field-default conversion. At the baseline, `PresentationSnapshotDTO` accepts only v2, and `state_machine.py` removes the prepublication `owner_session_id` on publish. The replicated deck record therefore cannot by itself establish a historic owner.

### 4.1 Accepted invariants

1. New post-cutover published decks use the ADR-012 v3 owner contract and begin private with `access_version=0` and no grants. The generic command-envelope schema remains v1 unless its wire semantics independently change.
2. Prior committed v2 publication commands and old v2 FSM snapshots remain readable and replayable, including strict historical DTO validation where applicable. No startup, read, or leader-election path silently rewrites a deck or assigns ownership.
3. **Eligible provenance** is a successful committed historical `publish_deck` record with an authenticated, server-stamped user identity whose proof was validated under Stage E at original application, and whose identity matches a registered account in the replicated account catalog. The original publication's deduplicated result and deck identifier must agree. A self-reported actor, a guessed account, a `client_id`, an `owner_session_id`, a later editing operation, and an uncommitted or rejected publication do **not** qualify.
4. A normal current-state deck record cannot prove eligibility on its own. If the authoritative committed publication record, its authentication provenance, or other necessary verification material cannot be recovered and checked deterministically by replicas, the deck remains quarantined. No owner is inferred from incomplete data.
5. An **explicit Raft-committed migration command** performs the v2→v3 transition atomically: verify source schema and immutable provenance, assert the chosen owner, retain all existing deck/slide/element content, tombstones, settings and structural revisions, active locks, fencing counters and creation sequences, initialize only the new access state (`schema_version=1`, `access_version=0`, empty grants), and set `metadata.owner_user_id` and presentation `schema_version=3`. The command must be idempotent under exact request-result deduplication; no partial assignment is possible.
6. **Deterministic validation requirement:** It is not enough for the leader to assert that it found a publisher. The implementation specification must choose and test an identical, authoritative provenance-verification mechanism at the replicated apply boundary. Since `KeyValueStateMachine.apply()` currently receives the command and committed index, *not* the Raft log or `request_results`, implementation agents must explicitly bridge this gap (for example via a verified replicated provenance record / deterministic validation context), rather than blindly trusting proof fields supplied in the migration payload. That bridging design is an implementation prerequisite, not permission for an insecure shortcut.
7. Migration must be Raft-ordered, with a precise post-cutover acceptance boundary for new v3 publication and historical v2 replay. Replays of old logs may encounter v2 publications before Stage E enforcement; those remain v2 and are not retroactively treated as authenticated. Clock-based or node-local cutover flags are forbidden.
8. Until migration is committed, an ownerless v2 deck is **quarantined**: `query_grants` returns `OK/NONE`; protected content reads and any owner-only roster read yield the same `ACCESS_DENIED` shape as unknown decks; external deck editing and grant mutations are denied. Historical bytes and indexes remain intact. A successfully migrated deck follows ordinary OWNER/EDIT/VIEW/NONE authorization; migration does not grant access to unrelated users.
9. This ADR does not add a general account-claim process, owner transfer, or administrative override for unprovable decks. Such mechanisms require a separate threat model and decision.

### 4.2 Acceptance tests

- Stage E-authenticated, committed v2 publication with validated provenance migrates exactly once; correct `owner_user_id`, private `access_version=0`, empty grants, and all preexisting deck/slide/element/settings/lock/fencing state unchanged.
- Pre-Stage-E or otherwise unprovable v2 publication stays quarantined, without existence disclosure or synthesized ownership.
- Log replay, persisted-state reload, new leader election, exact migration command retry, and conflicting/wrong-owner migration attempts converge deterministically.
- A spoofed `authenticated_actor_user_id`, a replayed request with mismatched fingerprint, or client-provided guessed proof cannot assign ownership.
- Historical v2 commands still replay after Stage F code upgrade, but post-cutover new v2 `publish_deck` requests cannot bypass the v3 boundary.

**Implementation caution:** Stage E stamping demonstrates that a *subset* of historical publications can carry useful provenance, not that every existing v2 deck can be migrated. The safe default when provenance cannot be demonstrated is quarantine.

## 5. F08 — Owner-only exact-email registered-user resolution (Q3 accepted: Option B)

The owner-only `query_access_state` from §3 lists *existing* grants, but the current `grant_access` API from ADR-013 expects the target registered **user UUID**. Stage E already persists a deterministic catalog of registered users with `user_id`, `first_name`, `last_name`, and unique normalized `email`; its validated auth state includes an `email_index` mapping to UUIDs (`auth_dto.py`). The user accepted **Q3 Option B:** provide a narrow lookup that resolves a known complete email into the registered user's UUID, without a broad directory or global search API.

**Normative public operation: `resolve_registered_user`.**

1. **Request:** `{ "deck_id": "<UUID>", "email": "person@example.com" }` supplied by the authenticated client installation with its normal Stage E `_auth` bearer. The client does not provide an actor or target UUID. Require a complete, bounded, normalized email string (use the existing canonical strip/lowercase normalization and email-form validation); no prefix, substring, wildcard, pagination, or bulk lookup.
2. **Execution:** route to the current leader. Use fresh authenticated ReadIndex, validate the current bearer against the bound `client_id`, then require **OWNER** on the requested active schema-v3 deck against the same applied FSM. Do not return stale results from followers or make a Raft write. Quarantined historical v2 decks are not owner-authorized until their proof-checked migration has committed.
3. **Authorized owner, registered exact email:** return `status="OK"` and a `user` object containing **only** `user_id`, `first_name`, `last_name`, and canonical `email` from the replicated public profile. `applied_index` may be included as a read watermark. The response contains no password verifier, digest, bearer, credential generation, client binding, other user records, or grants. This result is discovery only: it is **not** authorization and does not create a grant.
4. **Authorized owner, no matching email:** return `status="USER_NOT_FOUND"` (no account fields). The distinction from `OK` is intentionally visible **only to verified deck owners**. Unknown/missing deck and existent but nonowned deck must return the *same* `ACCESS_DENIED` envelope, as in ADR-017 and §3, regardless of the supplied valid email; neither the presence of the deck nor a matching account may be inferred from this denial.
5. **Authentication/unavailability:** invalid or missing bearer uses the existing `REAUTH_REQUIRED` boundary. Failure to confirm leader/quorum returns existing unavailable/redirect errors, not a stale profile or a fabricated absence. Bad input returns `INVALID_REQUEST` without consulting any catalog; input-validation errors must never include account or deck existence information.
6. **Mutation boundary:** once resolved, the owner explicitly invokes `grant_access` with the returned UUID and the required `expected_access_version`. The separate Raft mutation still checks registered target, owner role, and CAS atomically at application (ADR-013/014). No direct grant-by-email, automatic invitation, new-user registration, or access token is introduced.
7. **Privacy tradeoff:** a verified deck owner can test whether an *exact supplied email* is present among pre-provisioned accounts. This is a deliberate, narrowly scoped account-existence disclosure for usability, **not** a promise of complete user-enumeration resistance. The operation must not enable **deck enumeration** by response status or payload. Broad user directories, prefix queries, and lists of all registered accounts remain out of scope.

**Q3 acceptance:** The current owner can resolve a registered exact email and use the UUID in a separate grant command; nonowners receive uniform `ACCESS_DENIED` for both existing inaccessible and nonexistent decks (including emails that would otherwise match); nonexistent exact email yields `USER_NOT_FOUND` only for a verified owner; case/whitespace-normalized email resolves identically; malformed/wildcard queries fail; no secrets or other accounts leak; leader change and quorum loss cause no stale positive response. `query_access_state` remains separately owner-only and authoritative; the self-only `query_grants` remains unchanged.

No `delete_deck`, transfer of ownership, owner takeover of editor locks, global cross-user deck search, global account directory, or public participant list is introduced by this ADR.

## 6. Implementation touchpoints and acceptance tests

| File/module | Required Stage F implementation work |
|---|---|
| `state_machine.py` | Apply EDIT check before lock state/version checks; preserve internal `expire_lock`; implement atomic v2→v3 migration only with deterministically validated original publication provenance; quarantine unprovable v2; implement accepted owner-only safe access projection |
| `lock_maintenance.py` and caller integration | Stop renewal workers on permission loss; don't enqueue an unauthorized release as cleanup; don't reuse stale fencing tokens on restored grant |
| `lock_expiration.py` and `app.py` | Reuse current inactivity-based internal expiration and deterministic generation request IDs, including leader re-seeding; no new Raft cancellation semantics |
| `command_dto.py`, historical log/result verification, and persisted-state loader | Support new v3 publications and historical v2 replay; implement an explicit Raft-committed, proof-checked v2→v3 migration with deterministic cutover and preserved state |
| `app.py` | Route `query_access_state` and `resolve_registered_user` through ADR-017 leader/bearer/ReadIndex pipeline; require owner before access-state or exact-email catalog lookup; mask nonexistent vs inaccessible deck responses |
| `client_journal.py` | Durable deck gate and explicit reconciliation per ADR-016; no replay of terminal `ACCESS_DENIED` intention; retain rejected material in read-only recovery view |
| `spec.py` and tests | Model new v3 private ownership and grant outcomes; test lock/revoke ordering, expiry, owner roster read, exact-email resolver and owner-only disclosure, eligible v2→v3 migration, unprovable-v2 quarantine, historical replay/failover, and nonenumerating deck-error envelopes |

Acceptance evidence must include both (a) deterministic replay/state convergence after restart and leader change and (b) public-data non-disclosure from unauthorized clients. An ADR acceptance is not evidence Stage F is already implemented.

## 7. Decision register — maximum three, one at a time

**Q1 — Legacy v2 published decks: ACCEPTED, B1 (conditional migration).** Implement the deterministic provenance-gated Raft migration contract in §4; leave decks with unprovable ownership in quarantine. Do not re-open unless implementation evidence shows that the necessary committed-authentication provenance cannot be established safely.

**Q2 — Owner-only access roster: ACCEPTED, A.** Add bounded `query_access_state` OWNER-only authenticated ReadIndex read as defined in §3. Owner sees target UUIDs, `VIEW|EDIT`, current `access_version`, and `applied_index` for access-management reconciliation. Same `ACCESS_DENIED` for not-owner and missing deck. It is authoritative; the client is not responsible for retaining a UUID-to-grant map.

**Q3 — Registered-user discovery: ACCEPTED, B.** Implement `resolve_registered_user(deck_id, email)` as an exact-email, owner-only authenticated ReadIndex lookup of the pre-provisioned public account profile; return only UUID, normalized email, and names on match. Return `USER_NOT_FOUND` only after deck OWNER authorization; return uniform `ACCESS_DENIED` for missing/foreign decks. No partial/bulk searches, no account secrets, and no change to UUID-based `grant_access`.

**ADR-018 decision status: CLOSED.** Exactly three questions were resolved. All Stage F07/F08 architectural choices in this ADR are accepted. Deterministic migration-provenance verification remains a mandatory implementation prerequisite, not a license to trust a leader-supplied owner guess. Full Stage F implementation and tests are still pending.

## References

- ADR-001, ADR-004, ADR-012, ADR-013, ADR-014, ADR-015, ADR-016, ADR-017
- `app.py`, `command_dto.py`, `state_machine.py`, `lock_maintenance.py`, `lock_expiration.py`, `raft_core.py`, `client_journal.py`, `spec.py`
