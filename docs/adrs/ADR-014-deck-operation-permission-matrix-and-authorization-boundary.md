# ADR-014 — Deck Operation Permission Matrix and Deterministic Authorization Boundary

- **Status:** Accepted for Stage F implementation planning; not yet implemented
- **Date:** 2026-11-26
- **Stage:** F — F03: owner, editor, and viewer permissions over implemented operations
- **Related:** ADR-001, ADR-004, ADR-006, ADR-007, ADR-008, ADR-009, ADR-010, ADR-011, ADR-012, ADR-013
- **Source reviewed:** `master` at `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a` on 2026-11-26, especially `app.py`, `state_machine.py`, `command_dto.py`, `auth_dto.py`, `authenticated_client.py`, `client_journal.py`, `lock_expiration.py`, and `spec.py`
- **Scope:** Normative permission classification and ordering for **existing** presentation operations and the two new sharing mutations; not a change to the Raft protocol or a claim that Stage F authorization has already been implemented

## Context

ADR-012 establishes account-bound local deck creation, presentation schema v3, immutable `metadata.owner_user_id`, and owner-private publication. ADR-013 establishes per-deck `access.grants` with exactly two grantable levels (`VIEW`, `EDIT`), the owner's exclusive permission-management authority, `grant_access` upsert, `revoke_access` removal, and an independent `access_version`. This ADR maps those accepted decisions onto actual code operations. A registered account is not a global `VIEW` or `EDIT` user: **effective access is determined independently for each published deck**.

The current implementation only authenticates presentation mutations. In `state_machine.py`, `_authorize_external_mutation()` checks the current Stage E credential but not a deck access map. `app.py` routes `get_deck_preview`, `get_slide`, `get_element`, and `read_lock_status` to `_presentation_read()` without a bearer check; it also exposes `authoritative_refresh_element` and its alias `refresh_element` through a quorum-confirmed but currently unauthenticated read path. The client outbox's `poll()` uses all four local read operations. None of these existing read paths yet enforces owner-private access. No `grant_access`, `revoke_access`, `delete_deck`, or access-list endpoint exists in `master`.

## Decision

### 1. Effective per-deck authority

For a **v3 published deck**, resolve a verified authenticated `user_id` against the deck's current committed owner and grants:

```python
# Conceptual policy function — not yet a repository implementation.
def effective_deck_role(deck, authenticated_user_id):
    if authenticated_user_id == deck["metadata"]["owner_user_id"]:
        return "OWNER"
    grant = deck["access"]["grants"].get(str(authenticated_user_id))
    return grant["access_level"] if grant is not None else "NONE"
```

1. `OWNER` is implicit, immutable, and **never** a grant value. It has read, presentation-edit, and permission-management authority.
2. `EDIT` is an explicit participant grant. It has read and ordinary presentation-edit authority, including presentation settings and editor locks, but cannot manage grants or ownership.
3. `VIEW` is an explicit participant grant. It has read-only deck-content and lock-status observation authority, but cannot perform any shared presentation mutation or permission-management command.
4. `NONE` (no current grant, not owner) has no deck-content read or mutation authority. The presence of a valid Stage E bearer does not confer deck access.
5. Accounts, clients, credentials, and editor sessions remain distinct: grants are keyed by **user account**, not by `client_id` or `session_id`. One user retains the same effective role on a deck from any correctly authenticated, independently bound client installation.
6. A `VIEW`/`EDIT` grant is never sufficient to publish a snapshot *as somebody else's owner*; publication is separately creator-fenced as described below.

**Access-level ordering:** `OWNER` satisfies any deck policy; `EDIT` satisfies `EDIT` and `VIEW`; `VIEW` satisfies only `VIEW`; `NONE` satisfies none. This ordering grants no owner-management capability to `EDIT`.

### 2. Normative matrix: existing presentation mutations

**Legend:** `ALLOW` means *eligible after authentication and deck permission checks*; existing structural, settings, lock/fencing, and version preconditions still apply. `DENY` means `ACCESS_DENIED` for a currently authenticated but insufficiently authorized actor, without mutation or conflict-state disclosure. The table reflects actual public operation names and corresponding state-machine methods in `master`.

| Public operation | Existing state-machine handler | OWNER | EDIT | VIEW | NONE | Required authority / retained preconditions |
|---|---|:---:|:---:|:---:|:---:|---|
| `create_slide` | `_create_slide()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; `expected_deck_version`, structural rules |
| `delete_slide` | `_delete_slide()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; structural version, slide tombstone cascade |
| `reorder_slides` | `_reorder_slides()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; complete slide permutation, structural version |
| `create_element` | `_create_element()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; valid slide/element, creates initial session-fenced lock |
| `update_element` | `_update_element()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; element version, existing session/fencing token |
| `delete_element` | `_delete_element()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; element version, existing session/fencing token |
| `acquire_lock` | `_acquire_lock()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; lock availability and session identity |
| `release_lock` | `_release_lock()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; existing lock's session and fencing token |
| `renew_lock` | `_renew_lock()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; existing lock's session, fencing token, lease epoch |
| `update_deck_settings` | `_update_deck_settings()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; name and canvas width/height, `expected_deck_settings_version` |
| `update_slide_settings` | `_update_slide_settings()` | ALLOW | ALLOW | DENY | DENY | `EDIT`; slide background, `expected_slide_settings_version` |

These are **presentation settings**, not permission settings. The `EDIT` participant may rename or resize a deck and change slide backgrounds and structural slide order. Those operations must not modify `access_version`, the grant map, or `owner_user_id`. Conversely, grant mutations must not increment `deck_version`, `deck_settings_version`, any `slide_settings_version`, element versions, or lock fencing counters.

**Lock authority is independent.** Even the deck owner may not bypass the existing `session_id`/fencing-token lock guard to change a lock owned by a different editor session. `EDIT` means permission to *attempt* a protected edit, not automatic possession of another session's lock. After permission loss, all new client-originated acquire/renew/release/edit attempts are denied; handling lingering locks is delegated to F07.

### 3. Normative matrix: existing presentation reads

| Public operation | Current handler/route | OWNER | EDIT | VIEW | NONE | Required authority / note |
|---|---|:---:|:---:|:---:|:---:|---|
| `get_deck_preview` | `App._presentation_read()` → `get_deck_preview()` | ALLOW | ALLOW | ALLOW | DENY | `VIEW`; deck metadata, settings, slide IDs, applied index |
| `get_slide` | `App._presentation_read()` → `get_slide()` | ALLOW | ALLOW | ALLOW | DENY | `VIEW`; slide and element content |
| `get_element` | `App._presentation_read()` → `get_element()` | ALLOW | ALLOW | ALLOW | DENY | `VIEW`; element content/version and observed lock |
| `read_lock_status` | `App._presentation_read()` → `read_lock_status()` | ALLOW | ALLOW | ALLOW | DENY | `VIEW`; lock/session observation, not lock authority |
| `authoritative_refresh_element` | `App._authoritative_element_refresh()` | ALLOW | ALLOW | ALLOW | DENY | `VIEW`; retains quorum-confirmed ReadIndex and `min_commit_index` fence |
| `refresh_element` | Alias of `_authoritative_element_refresh()` | ALLOW | ALLOW | ALLOW | DENY | **Identical** `VIEW` and freshness requirements; no alternate bypass |

`ClientIntentionOutbox.poll()` and `JournaledClient.poll_if_due()` are **client-side workflows, not additional public Raft operations**. They invoke `get_deck_preview`, `get_slide`, `get_element`, and `read_lock_status`; all four underlying calls must be protected. Returning a preview alone must not make subordinate unauthenticated reads legal.

The matrix **defines who may read**, not the final stale-follower-proof mechanism. The current local eventual-read implementation is insufficient for private deck confidentiality under committed revocation. F06 must specify quorum freshness, routing, response timing, partition behavior, and guarantees for all these paths. Do not advertise owner-private deck reads as implemented until F06 passes its acceptance gate. A quorum-confirmed `authoritative_refresh_element` is not currently an authorization check and must gain one.

### 4. Special publication and new sharing operations

| Operation | Subject/authorization | Allowed principal | Required consequence |
|---|---|---|---|
| `publish_deck` | **New deck**; no preexisting deck ACL yet | Registered, currently authenticated **creator** whose server-stamped `authenticated_actor_user_id` equals `presentation.metadata.owner_user_id` | Atomically create schema-v3 deck, immutable owner, `access_version=0`, empty grants; retain existing `PUBLISHED`/duplicate-deck semantics when applicable |
| `grant_access` | Existing deck | **OWNER only** | ADR-013's version-fenced upsert and `ACCESS_GRANTED` / `ACCESS_UPDATED` / `ACCESS_UNCHANGED` |
| `revoke_access` | Existing deck | **OWNER only** | ADR-013's version-fenced removal and `ACCESS_REVOKED` / `ACCESS_NOT_FOUND` |

Publication authorization is **not** an owner check against a nonexistent resource. The server-stamped authenticated actor is the proof; the creator's `owner_user_id` inside the local snapshot is a required claim that must match it. A mismatch yields **`OWNER_MISMATCH`** (a Stage F publication denial with no deck created), while invalid credentials retain Stage E's existing authentication results. The owner's registration is verified against replicated account state; a caller-supplied `owner_session_id` or `client_id` is never used as proof of ownership.

The full `grant_access` / `revoke_access` DTO, version precedence, target-user validation, no-op behavior, and return values remain **normative under ADR-013**. This ADR does not re-open or rename those contracts. Only the owner may receive the detailed participant list or access-version conflict details. A future `get_access_state`/grant-management read, if implemented, is **owner-only**; its API shape and discovery semantics are deferred to F08.

### 5. Operations outside per-deck authorization

These are present in the codebase but **must not** be mistakenly classified as owner/editor/viewer presentation operations:

| Operation | Existing entry/handler | Stage F treatment |
|---|---|---|
| `login` | `App._login()` | Password-authenticated **account** operation; no deck role required |
| `revoke_access_token` | `App` authenticated admission → FSM `_revoke_access_token()` | Revokes **this client's credential**, not anyone's deck grant; permitted without owning any deck; preserves Stage E fence |
| `get` | `App._get()` → `get_local()` | Stage A legacy KV read; not a deck read and must not expose deck state via a new KV projection |
| `put` (replicated `kv_put`) | `App.on_request()` → FSM `kv_put` | Stage A legacy KV compatibility path; not a deck mutation or grant manager |
| `raft.noop` | Raft internal | No end-user deck permission; never a public client command |
| `initialize_auth_catalog` | Internal authenticated-catalog coordinator | No end-user deck permission; never a public client command |
| `upgrade_auth_schema_v2` | Internal auth migration coordinator | No end-user deck permission; never a public client command |
| `issue_access_token` | Password-admitted internal issuance command | No end-user deck permission; never a public client command |
| `expire_lock` | `_expire_lock()` / leader lock-expiration coordinator | **Internal** lease maintenance, not a client EDIT operation; existing expected token/lease-epoch fence remains operative even after grant revocation |
| Local private-draft creation/editing | Client-local model; no public `create_deck` Raft command | Must have an established registered owner identity under ADR-012; no per-edit Raft permission transaction while offline |

There is **no implemented `delete_deck`** command in the reviewed repository. `delete_slide` and `delete_element` are not deck deletion. A future full-deck delete would require an explicit owner-only lifecycle decision, tombstone/replay contract, and tests; it is **not** silently added by this ADR. Likewise, no public grant listing, deck listing/discovery, or user-search endpoint exists in the reviewed operation dispatch.

### 6. Common guard semantics and precedence

1. The leader must apply Stage E's **fresh quorum-confirmed bearer admission** to every protected deck mutation; `grant_access` and `revoke_access` join `BEARER_GUARDED_OPERATIONS` in `command_dto.py` and the protected `App.on_request()` dispatch. Client assertions of actor identity are rejected, and raw tokens are stripped before command creation.
2. Resolve a valid authenticated `user_id` to the *current effective deck role* for the intended operation. At admission, a known insufficient role may be rejected **before proposal**, but this cannot replace a check at apply time. For a first application, the deterministic FSM rechecks the current credential first, then the required deck role **before** existing lock, version, and structural preconditions or any resource-specific conflict payload.
3. If Stage E credential validation fails during committed apply, return `UNAUTHENTICATED`, not `ACCESS_DENIED`. If the token is missing or invalid before proposal, return Stage E's pre-proposal `REAUTH_REQUIRED`. If a valid actor is not permitted to perform the operation, return **`ACCESS_DENIED`** without mutation. “Operation not permitted” is a *user-facing description*, not a new protocol status.
4. For a missing or inaccessible deck, **do not reveal existence** to an unauthorized caller. The final public `NOT_FOUND` versus masked `ACCESS_DENIED` mapping is an F06/F08 response-privacy decision; until it is defined, no caller may receive another deck's name, content, revisions, locks, owner, or grants through an error response.
5. After successful authorization, preserve Stage B–D semantics *as-is*: `LOCKED`, `STALE_TOKEN`, `NOT_OWNER` (session-based lock ownership), `VERSION_CONFLICT`, `STRUCTURE_VERSION_CONFLICT`, `DECK_SETTINGS_VERSION_CONFLICT`, `SLIDE_SETTINGS_VERSION_CONFLICT`, tombstones, valid success results, and their existing optimistic concurrency rules. In particular, `NOT_OWNER` in the lock guard **does not mean** the account is not the deck owner.
6. Raft's durable request deduplication (`client_id`, `request_id`, canonical logical fingerprint) remains authoritative. An exact-ID replay does not reapply an edit, but **current authentication and permission to receive the prior result must still be checked before disclosure**. A revoked/downgraded user must not receive otherwise private stored result payloads solely because an old request succeeded. New commands with reused IDs and different content/actor remain `REQUEST_ID_REUSE` after the safe admission boundary.
7. Revoking or downgrading an EDIT grant in Raft order stops **subsequent** protected EDIT commands at apply, even if they were admitted earlier. A prior committed edit may succeed. Access revocation does not revoke the participant's authentication credential or affect unrelated decks. An owner re-grant does not retroactively turn a terminal `ACCESS_DENIED` / `UNAUTHENTICATED` request into a success; a new business request identity is needed.
8. Grant edits do not create a deck-wide editor lock and do not participate in Stage D's Deck Settings FIFO Lane or Slide Settings FIFO Lanes. Access revisions and presentation revisions are separate domains.
9. Never trust the local draft's owner claim, local projection, cached grant, client UUID, or editor session as the server-side authorization decision. No client UI action overrides committed permission state.

### 7. Return statuses and exact meanings in F03

The uppercase names below are normative. Statuses from ADR-013 retain their already-accepted spelling and precise meanings.

| Status | Exact meaning in this decision | Mutation / disclosure |
|---|---|---|
| `ACCESS_DENIED` | A **currently authenticated** actor lacks the required deck role (OWNER for permission changes, EDIT for presentation mutations, VIEW for protected reads). | No mutation; do not expose deck content, revisions, or grant state. |
| `UNAUTHENTICATED` | Existing Stage E committed-at-apply credential rejection, regardless of the deck's grant state. | No protected mutation. |
| `REAUTH_REQUIRED` | Existing Stage E admission-time missing/stale/revoked credential, before protected proposal. | No protected proposal; journal intention may remain pending. |
| `OWNER_MISMATCH` | `publish_deck` authenticated actor does not equal the v3 snapshot's declared `owner_user_id`. | No deck created, no grants or revisions established. |
| `PUBLISHED` | Existing successful, committed deck publication, now with immutable matching owner and empty initial access map. | Create new deck and initial access state atomically. |
| `ACCESS_GRANTED`, `ACCESS_UPDATED`, `ACCESS_REVOKED` | ADR-013's effective owner-authorized grant create/change/remove results. | Exactly one increment to that deck's `access_version`; no presentation revisions touched. |
| `ACCESS_UNCHANGED`, `ACCESS_NOT_FOUND` | ADR-013's committed **no-op** results for same-role grant and absent-target revocation respectively. `ACCESS_NOT_FOUND` is about the **participant grant**, not deck existence. | No mutation or revision increment. |
| `ACCESS_VERSION_CONFLICT` | ADR-013's owner-authorized stale `expected_access_version` failure, which precedes no-op grant evaluation. | No mutation; owner-only safe revision/conflict details. |
| `INVALID_ACCESS_TARGET`, `INVALID_TARGET_USER` | ADR-013's non-owner-target prohibition and unregistered target-user rejection. | No mutation. |
| `REQUEST_ID_REUSE` | Existing Raft logical request-ID/fingerprint mismatch. | No new mutation; result disclosure still gated. |
| `INVALID_REQUEST` | Malformed wire/DTO structure or caller-supplied authoritative actor fields. | No protected mutation; no resource data disclosure. |

For **authorized content reads**, existing `OK`, `NOT_FOUND`, `SLIDE_DELETED`, etc. preserve their domain meanings; existence and detail disclosures remain subject to the new access checks. An authenticated VIEW participant who attempts `update_slide_settings` must receive `ACCESS_DENIED`, **not** `SLIDE_SETTINGS_VERSION_CONFLICT` or `NOT_OWNER` from the element lock guard.

### 8. Client-local-first and durable-intention effects

- ADR-012's locally owned private drafts require an established registered user identity, but not continuous Raft access for every local edit. Client-local data may continue to exist while disconnected and is not a server-authorized shared edit merely because it is visible in a local viewport.
- `AuthenticatedClient.call()` injects the bearer at transport time; `JournaledClient` and the Durable Intention Journal keep request IDs stable without storing secrets. These mechanisms remain intact.
- When sharing permission is insufficient, an edit must not be submitted or represented as committed; a committed at-apply denial is a **terminal result** for that request identity. A pre-proposal permission failure also must not be treated as a network retry that can gain authority by repeatedly resending. Exact client lifecycle and recovery/failure UI rules belong to F05/F07.
- Loss of VIEW permission must stop remote polling and authoritative refreshes from disclosing **new** deck data. Data previously delivered to a client's local storage cannot be retroactively erased or cryptographically revoked by an ACL change; the application makes no such promise.
- `read_lock_status` is a permitted VIEW-level **observation**; it never grants the right to acquire, renew, release, update, or delete under an editor lock.

## Invariants

- **F03-I01 — Default deny:** `NONE` cannot read or mutate a v3 published deck; authenticated identity alone grants no deck access.
- **F03-I02 — Owner-only ACL:** only `OWNER` can `grant_access` and `revoke_access`; `EDIT` cannot delegate rights.
- **F03-I03 — Edit capability:** `OWNER` and `EDIT` alone may attempt all eleven existing post-publication mutation operations listed above, subject to existing guards.
- **F03-I04 — Read capability:** `OWNER`, `EDIT`, and `VIEW` alone may perform all six existing read and authoritative-refresh names once F06 has secured their path.
- **F03-I05 — No lock bypass:** a role does not confer another session's fencing authority; an existing lock also does not confer a deck role.
- **F03-I06 — Immutable published owner:** `publish_deck` requires registered actor == claimed creator, and cannot create an ownerless/partly shared deck.
- **F03-I07 — No foreign-result leakage:** permission denial precedes domain-conflict disclosures and all dedup-result disclosure is reauthorized.
- **F03-I08 — Revision isolation:** presentation writes cannot mutate grant state; grant mutations cannot advance presentation/lock revisions.
- **F03-I09 — Log-order revocation:** earlier grant removal or EDIT→VIEW downgrade blocks later EDIT commands without rolling back earlier committed successes.
- **F03-I10 — Internal paths distinguished:** account login/revoke, Raft/internal commands, and Stage A KV proof operations do not acquire user deck roles.
- **F03-I11 — No false privacy claim:** owner-private read confidentiality is not considered implemented until F06 protects follower/local and authoritative reads under revocation.

## Acceptance criteria and implementation touchpoints

1. **Policy registry coverage:** enumerate actual public routes in `app.py`, `BEARER_GUARDED_OPERATIONS` and strict validators in `command_dto.py`, and all relevant handlers in `state_machine.py`. Verify each is classified once as `CREATE_OWN_DECK`, `OWNER_ONLY`, `EDIT`, `VIEW`, `ACCOUNT_SCOPE`, `INTERNAL`, or `LEGACY_KV`; reject any newly added unclassified product endpoint rather than defaulting to unauthenticated behavior.
2. **Fresh creation:** authenticated Sofia may publish a schema-v3 draft claiming Sofia; Santiago cannot publish the same draft by claiming Sofia. Publication creates owner and empty access state atomically; no owner grants, and no owner-by-session fallback.
3. **Full mutation matrix:** for each of eleven listed post-publication presentation mutations, demonstrate OWNER and EDIT success when prior preconditions are satisfied and VIEW/NONE `ACCESS_DENIED` with **no change to any presentation or grant revision**. Include lock create/acquire, renew, release, update, and delete separately.
4. **Full read matrix:** after the F06 read design is implemented, exercise `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status`, `authoritative_refresh_element`, and `refresh_element` under all four effective roles, including follower lag and leader failure. No unauthorized payload/version/lock disclosure.
5. **Owner-only grants:** apply all ADR-013 statuses for owner actions, EDIT and VIEW attempts, and no-grant callers. In particular, preserve `ACCESS_UNCHANGED` and `ACCESS_NOT_FOUND` without `access_version` increments.
6. **Guard precedence:** a valid VIEW bearer plus a correct lock token does not update an element; a valid EDIT bearer with a stale lock token gets the *existing* lock failure; a valid EDIT bearer with stale Deck Settings version gets the *existing* settings version conflict; an unauthorized actor gets neither conflict snapshot.
7. **Revocation races:** compare `edit → revoke_access` versus `revoke_access → edit` and `EDIT→VIEW → renew_lock`, including already-admitted but not-yet-applied commands. Only Raft-ordered authorized transitions mutate state.
8. **Idempotency and result privacy:** exact retry under the same account/new bearer never mutates twice; changed payload is `REQUEST_ID_REUSE`; a currently revoked participant cannot retrieve a private old committed success/conflict payload through replay.
9. **Client compatibility:** polling protects all four subordinate reads, and denied new work does not become an endless retry; historical journal IDs and lock/session fencing remain stable. F05/F07 will settle exact local reconciliation.
10. **Regression:** Stage A KV proof behavior and internal Raft/lock-expiration/auth commands retain their defined paths; existing Stage B–E tests and reference-model expectations are upgraded for v3 without silently rewriting historical pre-migration log entries.
11. **Preserved-disk/legacy gate:** historical schema-v2 decks with no trusted `owner_user_id` are never assigned a guess or served as owner-private before F08's deterministic migration/transition policy.

## Consequences

- A single deck-scoped policy check can be integrated with the existing Stage E credential guard and reused by Raft-applied mutations and correctly authorized read adapters.
- `EDIT` participants retain the established Stage B–D editing experience without gaining rights to modify participant grants.
- `VIEW` participants receive only presentation observation authority, including lock status, and never gain editing or lock authority.
- Previously valid local-eventual reads will require a new authorization/freshness path; F06 will evaluate cost, availability, lagging followers, and whether the leader must serve the access-sensitive observation.
- Current `owner_session_id` and `client_id` semantics remain unchanged and cannot substitute for `owner_user_id`.
- Some accepted historical Stage A–E test fixtures must be updated to v3; historical committed log replay requires a separately defined compatibility policy.

## Alternatives rejected

- **Use authentication alone for resource access:** permits any logged-in user to edit any published deck.
- **Let EDIT change grants:** violates ADR-012's immutable owner-only management.
- **Let VIEW acquire or release locks:** gives a read-only participant a mutation channel and creates inconsistent editor authority.
- **Authorize before proposal only:** an access revoke could commit before an already-admitted edit.
- **Check a lagging follower's local grant map as a privacy guarantee:** committed revocations may be absent from that follower's state.
- **Reuse `owner_session_id` or client UUID as account authorization:** neither is the registered user principal.
- **Require a separate Raft write for every local private-draft edit:** violates the accepted local-first boundary without improving distributed permission safety.
- **Treat `expire_lock` as a participant mutation:** could prevent safe internal expiration after a participant loses access.
- **Apply a grant-role rule to `login`, token revocation, or Stage A KV proof operations:** misclassifies independent account/system operations as deck content mutations.

## Deferred decisions

- **F04/F05:** exact grant-revocation ordering/recovery UI, permission-aware journal lifecycle, result-redaction API wiring, and permission change notifications.
- **F06:** complete protected-read protocol under lagging followers, partitions, grants/revocations, and freshness fences; including the definitive masked missing/inaccessible-deck response contract.
- **F07:** cleanup/expiration of locks held by downgraded/revoked participants, offline draft reconciliation, and whether forced cleanup is a separate internal command.
- **F08:** authorized account lookup, deck discovery, owner-only access-list reads, and legacy presentation-v2 deck ownership/access migration.
- **Future deck lifecycle:** `delete_deck`, archive, transfer ownership, co-owners, and public links are not introduced by ADR-014.

## References

- `docs/adrs/ADR-012-account-bound-deck-creation-and-immutable-ownership.md`
- `docs/adrs/ADR-013-raft-replicated-deck-access-grants-and-independent-permission-versioning.md`
- `docs/adrs/ADR-010-user-client-session-and-authenticated-command-boundary.md`
- `docs/adrs/ADR-008-versioned-deck-and-slide-settings.md`
- `docs/adrs/ADR-006-deck-and-slide-structural-operations.md`
- `docs/specs/Stage-E-Authentication-Implementation-Spec.md`
- `docs/user-stories/user-story-01-creating-and-sharing-decks.md`
- Source files at baseline commit: `app.py`, `command_dto.py`, `state_machine.py`, `auth_dto.py`, `client_journal.py`, `authenticated_client.py`, `lock_expiration.py`, `spec.py`
