# ADR-013 — Raft-Replicated Deck Access Grants and Independent Permission Versioning

- **Status:** Accepted (architecture decision; implementation pending Stage F)
- **Date:** 2026-11-26
- **Stage:** F — Deck ownership and sharing, decision F-02
- **Related:** ADR-004 (Durable Intention Journal), ADR-006 (structural operations), ADR-008 (independent settings revisions), ADR-009 (pre-provisioned accounts), ADR-010 (authenticated actors), ADR-011 (credential lifecycle), ADR-012 (account-bound creation and immutable ownership)
- **Implementation baseline:** `master` at `3ecced2432fd` (reviewed 2026-11-26)
- **Successor:** Stage F implementation specification and F-03–F-08 authorization, revocation, read-consistency, client-recovery, and legacy-migration decisions

## Context

ADR-012 establishes that a newly created presentation belongs to an authenticated, registered `user_id` and that a v3 publication atomically retains its immutable `metadata.owner_user_id` in the replicated deck. Publication is owner-private by default. Stage E supplies stable user/client identities, bearer authentication, server-stamped actors, at-apply credential checks, and Raft request deduplication, but does not yet grant or enforce resource-specific deck permissions.

Stage B supports slide and element operations and editor-session fencing; Stage D maintains independently versioned deck and slide settings and local settings FIFO lanes. Stage F must enable owners to share a published deck with registered users without turning editor rights into permission-management rights, conflating permission changes with presentation revisions, or adding another authoritative database.

On the reviewed baseline, `BEARER_GUARDED_OPERATIONS` does not include sharing commands; the replicated deck does not have an access map or access revision. This record defines a **new contract**, not already implemented behavior. Protecting reads against stale replicas, the complete F-03 operation permission matrix, and the v2-to-v3 legacy-deck migration are separate decisions.

## Decision

### 1. Owner versus participant authority

1. Each v3 published deck has exactly one **immutable** registered `owner_user_id` under its authoritative metadata, as defined by ADR-012. Ownership is not an entry in the grant map and cannot be reassigned through either sharing operation.
2. A published deck has **zero or more participant grants**, each keyed by a registered `user_id` other than the owner. A participant receives exactly one of two levels: `VIEW` or `EDIT`. No third participant role, co-owner, public-link principal, anonymous account, or guest access is introduced.
3. A `VIEW` participant may read the deck subject to Stage F's eventual protected-read contract, but may not perform presentation mutations or modify sharing. An `EDIT` participant may read and perform ordinary permitted presentation editing operations (including slide structure, element mutations and locks, and deck/slide presentation settings) subject to Stage E credential checks and pre-existing Raft/lock/version rules. **Neither participant level permits sharing mutations.**
4. The owner has full deck access and is the **only account** permitted to create, change, or remove grants. A participant grant cannot delegate further sharing authority. This record defines the high-level roles; F-03 will freeze the per-operation matrix.

### 2. Replicated access state and schema

5. The access-control state is stored **inside the authoritative Raft-replicated deck record**, rather than as a separate global authentication catalog or leader-local cache. For each new v3 published deck, `access.access_version == 0` and `access.grants == {}` at the same publication commit that establishes its owner.
6. The access state uses a separately versioned DTO (`schema_version: 1`), independent of presentation schema v3, the generic Raft command-envelope version, and auth-state schema v2. `access_version` is a strictly nonnegative integer, incremented exactly once per successful **effective** sharing mutation.
7. Each grant map key is the canonical string form of its registered target UUID and must equal its value's `user_id`. Map entries must be strictly validated, with no duplicate account keys, owner grants, unknown access levels, or unregistered targets. The last constraint requires deterministic comparison with the replicated account catalog, not UUID syntax validation alone.
8. `access_version` is **independent of** `deck_version`, `deck_settings_version`, each `slide_settings_version`, element revisions, fencing tokens, and credential generations. Successful grant changes never alter these other revisions, content, or lock records.
9. The map holds **current grants only**. Revocation removes the grant; there is no permanent grant tombstone, grant UUID, per-grant generation, or individual grant revision. Raft commit history and durable deduplication remain available as existing audit/consistency mechanisms, while current effective authority is determined solely by the owner and the current grant map. No grant history may be consulted to infer current permissions.
10. An explicit later owner-authorized `grant_access` may re-invite a revoked participant. Reinvitation is not a replay of the removed grant; it is a fresh operation with a fresh request ID and current expected access version.

**Normative proposed DTOs (not yet implemented):**

```python
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictInt

from auth_dto import ActorStampDTO
from dto_base import StrictDTO


class DeckAccessGrantDTO(StrictDTO):
    user_id: UUID
    access_level: Literal["VIEW", "EDIT"]


class DeckAccessStateDTO(StrictDTO):
    schema_version: Literal[1]
    access_version: StrictInt = Field(ge=0)
    grants: dict[str, DeckAccessGrantDTO] = Field(default_factory=dict)


class GrantAccessPayloadDTO(ActorStampDTO):
    target_user_id: UUID
    access_level: Literal["VIEW", "EDIT"]
    expected_access_version: StrictInt = Field(ge=0)


class RevokeAccessPayloadDTO(ActorStampDTO):
    target_user_id: UUID
    expected_access_version: StrictInt = Field(ge=0)
```

The validator additionally checks canonical map keys, matching grant `user_id` values, absence of the owner, and registered target identity against the deterministic replicated account state. `ActorStampDTO` fields are **server-stamped**, not accepted from the caller. The existing `CommandEnvelopeDTO.deck_id` carries the target deck; `client_id` and `request_id` retain their Stage E meanings.

### 3. Exactly two permission-management operations

11. **`grant_access`** is an owner-only Raft-committed *upsert*: if the target has no grant, create it at the requested `VIEW`/`EDIT` level; if an existing grant has another level, replace its level. If an existing grant already equals the requested level, return `ACCESS_UNCHANGED` with no mutation and no access-version increment. The command is not allowed to grant access to the deck owner.
12. **`revoke_access`** is an owner-only Raft-committed removal of the target's current grant. If present, remove the grant and increment `access_version`; if absent, return **`ACCESS_NOT_FOUND`** with no mutation and no revision increment. Never revoke a target's user account, bearer token, or rights to other decks.
13. The two commands require `expected_access_version`, compared with the access state **during deterministic Raft application**. If stale, return `ACCESS_VERSION_CONFLICT` even when the proposed operation would otherwise be a no-op. A correct expected revision followed by an effective mutation increments the revision exactly once; a correct revision followed by `ACCESS_UNCHANGED` or `ACCESS_NOT_FOUND` leaves it unchanged.
14. Permission-changing commands do not increment `access_version` merely because they were appended or committed. A rejected or no-op command may still have a durable deduplication result; only successful effective state changes increment the revision.
15. No `update_access` operation is introduced. `grant_access` performs both initial grant and level change. No automatic conflict rebasing, silent owner-driven repeat, implicit transfer of ownership, or bulk grant mutation is introduced.

### 4. Authentication, authorization, ordering, and privacy

16. Both sharing operations are **bearer-guarded external commands**. The leader runs Stage E's fresh quorum-confirmed admission read, validates the bearer, and stamps the authenticated account and non-secret credential reference before proposal. The client must not supply authoritative actor identifiers. The raw bearer is never part of the Raft command.
17. Admission rejects a caller who is not currently authorized as deck owner before proposal where possible, but such a check is **not sufficient**: deterministic apply must revalidate the current credential, deck owner, and current access revision in Raft log order before mutation.
18. For a first application, the logical validation order is: strict command structure; valid current credential; target deck and owner-only authority; `expected_access_version`; registered non-owner target; operation-specific existing-grant/no-op rule; atomic change. Resource existence, version, grant-map details, and user-account discovery must not be exposed to unauthorized users through responses. Structural DTO rejection may occur earlier without disclosing resource state. The Stage F specification will standardize public masking of deck-not-found versus inaccessible-deck errors.
19. Grant/revoke outcomes are serialized in committed Raft order. If `grant_access(expected=4)` commits and changes the revision to 5, a later `revoke_access(expected=4)` fails as `ACCESS_VERSION_CONFLICT` regardless of the target. Sharing operations do not borrow the structural `deck_version` and do not acquire an editor lock.
20. Deduplication continues to use `(client_id, request_id)` plus the canonical business fingerprint including the authenticated **account** identity, excluding transient bearer/credential-reference fields as in ADR-010. A prior committed sharing request is never reapplied on retry; a changed target, access level, actor, or expected revision under the same ID is `REQUEST_ID_REUSE`.
21. A previously committed result is not automatically safe to reveal to a later caller. Before disclosing a deduplicated result, Stage F admission must validate current authentication and the caller's current right to receive that sharing result. In particular, current permission management remains owner-only even on replay.
22. Permission denial is **distinct from** credential failure. A valid bearer is necessary but not sufficient to change grants. An unauthorized caller receives `ACCESS_DENIED`, not a success and not an implicit grant. Stage E's `UNAUTHENTICATED` / `REAUTH_REQUIRED` retain their established committed-at-apply versus pre-proposal meanings.
23. The current grant map must never be changed by a participant's presentation edit, local read, token rotation, editor-session change, or client installation change.

### 5. Normative return-status catalog

The exact uppercase identifiers below are **protocol return values**, not loose descriptive terms. Future `grant_access` and `revoke_access` implementations, DTO/reference-model transitions, journal policies, and tests must use these spellings.

| Return status | Origin / exact meaning | Effect on access state |
|---|---|---|
| **`ACCESS_GRANTED`** | `grant_access`: the target was absent and a new `VIEW` or `EDIT` grant was created. | Insert target grant; `access_version += 1`. |
| **`ACCESS_UPDATED`** | `grant_access`: an existing participant changed level (`VIEW`↔`EDIT`). | Replace level; `access_version += 1`. |
| **`ACCESS_UNCHANGED`** | `grant_access`: target already has the requested level **and** expected version matches. Valid committed no-op; not an error requiring a new grant. | No mutation; revision unchanged. |
| **`ACCESS_REVOKED`** | `revoke_access`: an existing participant grant was removed. | Delete grant; `access_version += 1`. |
| **`ACCESS_NOT_FOUND`** | `revoke_access`: the identified registered non-owner has **no current grant** and expected version matches. This concerns the *grant*, not the deck or user account. | No mutation; revision unchanged. |
| **`ACCESS_VERSION_CONFLICT`** | Either command: `expected_access_version` does not equal the deck's current access revision at apply time. This wins over an otherwise unchanged/absent-grant result, after credential and owner authorization succeed. | No mutation; revision unchanged. |
| **`ACCESS_DENIED`** | Either command: the authenticated actor is not the deck's immutable owner. This includes `VIEW`/`EDIT` participants attempting grant management. Must not reveal the grant list or current revision. | No mutation; revision unchanged. |
| **`INVALID_ACCESS_TARGET`** | Either command: target is the deck owner (who can never appear in the participant map). | No mutation; revision unchanged. |
| **`INVALID_TARGET_USER`** | Either command: the target UUID is well-formed but is not a registered account in the replicated catalog. | No mutation; revision unchanged. |
| **`REQUEST_ID_REUSE`** | Raft dedup: same `(client_id, request_id)` with different canonical logical operation/actor/payload. | No new mutation; original durable result unaffected. |
| **`UNAUTHENTICATED`** | Existing Stage E at-apply credential guard failed (including a credential invalidated earlier in log order). This is *not* a deck permission result. | No grant mutation. |
| **`REAUTH_REQUIRED`** | Existing Stage E pre-proposal missing/invalid bearer result; journal intention remains pending for reauthentication. | No Raft sharing mutation proposed. |
| **`INVALID_REQUEST`** | Command/payload violates strict DTO or request structure: malformed UUID, unsupported access level, missing required expected version, caller-provided actor assertion, or invalid field. | No grant mutation. |

**Distinct resource-not-found case:** A missing or inaccessible *deck* must not be conflated with `ACCESS_NOT_FOUND` (an absent participant grant on an accessible deck). Existing `NOT_FOUND` may be retained or externally masked as an authorization-safe result by F-03/F-06; this ADR does not freeze the public deck-discovery status.

**Result payload convention:** For `ACCESS_GRANTED`, `ACCESS_UPDATED`, and `ACCESS_REVOKED`, return safe `deck_id`, `target_user_id`, new `access_version`, and the effective access level (or `None` on revocation) where applicable. `ACCESS_UNCHANGED` / `ACCESS_NOT_FOUND` can return the unchanged revision to the verified owner. `ACCESS_VERSION_CONFLICT` can include current revision and a reconciliable, owner-authorized access snapshot with an appropriate commit-index fence. The complete result DTO shape and owner-only read endpoint are Stage F implementation-spec details; errors must never disclose password verifiers, token digests, bearer secrets, or private grants to non-owners.

**Deterministic precedence for validated first applications:**

```text
credential failure               -> UNAUTHENTICATED
not owner / not authorized       -> ACCESS_DENIED
stale expected access revision   -> ACCESS_VERSION_CONFLICT
target is the deck owner         -> INVALID_ACCESS_TARGET
target not a registered account  -> INVALID_TARGET_USER
grant_access, target absent      -> ACCESS_GRANTED (+1)
grant_access, level differs      -> ACCESS_UPDATED (+1)
grant_access, same level         -> ACCESS_UNCHANGED (+0)
revoke_access, target present    -> ACCESS_REVOKED (+1)
revoke_access, target absent     -> ACCESS_NOT_FOUND (+0)
```

Strict malformed-command validation and Raft's *already-committed* dedup handling are upstream boundaries rather than entries in this first-apply sequence. Missing/inaccessible deck responses must be handled without leaking its existence. A no-op still consumes and durably records its request identity, but **not** an access revision.

### 6. Illustrative serialized history

```text
Access on deck D (owner Sofia) begins at version 0, no participants.

index 100: grant_access(D, Santiago, EDIT, expected=0, actor=Sofia)
           -> ACCESS_GRANTED; version 1

index 101: grant_access(D, Santiago, EDIT, expected=1, actor=Sofia)
           -> ACCESS_UNCHANGED; version 1

index 102: grant_access(D, Santiago, VIEW, expected=1, actor=Sofia)
           -> ACCESS_UPDATED; version 2

index 103: revoke_access(D, Santiago, expected=2, actor=Sofia)
           -> ACCESS_REVOKED; version 3; grant removed

index 104: revoke_access(D, Santiago, expected=3, actor=Sofia)
           -> ACCESS_NOT_FOUND; version 3

index 105: grant_access(D, Santiago, EDIT, expected=2, actor=Sofia)
           -> ACCESS_VERSION_CONFLICT; version 3; no resurrection

index 106: grant_access(D, Santiago, VIEW, expected=3, actor=Sofia)
           -> ACCESS_GRANTED; version 4; explicit re-invitation
```

A separate participant may hold `EDIT` on D and `VIEW` on another deck. The same `user_id` may act from different authenticated `client_id` installations; no per-client grant duplication is necessary.

## Invariants and acceptance evidence

- **I-01 — Owner immutability:** exactly one registered owner; grant operations cannot alter `metadata.owner_user_id`.
- **I-02 — Participant uniqueness:** at most one grant per non-owner registered user per deck, with exactly one legal level.
- **I-03 — Default deny:** absent participant means no deck access; publication begins with an empty grant map.
- **I-04 — Exclusive sharing authority:** only the current authenticated owner may mutate participant grants, both at admission and apply.
- **I-05 — Effective-change revision:** one increment per successful grant/level-change/removal; none for `ACCESS_UNCHANGED`, `ACCESS_NOT_FOUND`, conflicts, or denials.
- **I-06 — Independent revisions:** grant changes do not alter any structural, slide/deck-settings, element, or lock revision.
- **I-07 — Deterministic serialization:** identical committed commands and starting states yield identical access maps, revisions, and statuses on every replica.
- **I-08 — Replay safety:** same request ID never applies twice; changed same-ID intent yields `REQUEST_ID_REUSE` without mutation.
- **I-09 — No stale resurrection:** a grant after a revoke requires a new owner-authorized command using the current access revision. Old commands with stale revisions cannot re-grant.
- **I-10 — No cross-deck effect:** granting/revoking D1 changes no permission state on D2 or the target's account credentials.
- **I-11 — No unauthorized disclosure:** participants/nonmembers cannot use conflict responses or dedup replay to inspect owner-only grant metadata.

Future deterministic tests must cover all return statuses and precedence; valid upsert/downgrade/revoke/reinvite; duplicate and changed-ID replay; concurrent owner-device commands at one version; unknown and owner targets; failover, restart, follower catch-up, and preserved Raft replay; unaffected presentation versions; and credential rotation without an access-level change.

## Compatibility and implementation boundaries

This ADR depends on ADR-012's **new v3 publication** and establishes a per-deck access record initialized atomically at publication. The existing command envelope remains version 1 unless a separate migration decision requires otherwise. The Stage F implementation must add strict command validators, application handlers, authenticated admission routing, sequential-model behavior, journal operation metadata, and tests without modifying the instructor-owned Raft protocol.

**Legacy v2 deck migration is not authorized by this decision.** Such decks have no trusted `owner_user_id` or access state. Until a separate deterministic migration is agreed, they must not be assigned fabricated owners or treated as already private. The migration design must preserve historical replay and distinguish the old deck-version domain from the new permission-version domain.

No public sharing functionality should be described as security-complete while Stage E's unauthenticated follower-local reads can disclose deck contents. Protected-read freshness and revocation race guarantees are deferred to F-06. An access-version fence controls **permission-management mutations**, not freshness of an arbitrary follower's local read.

## Consequences

- Grant state is small, explicit, and deterministic, integrated with the existing Raft state machine.
- An owner can invite, promote, downgrade, revoke, and re-invite registered users without introducing invitation tokens, public links, or separate account roles.
- Deck-wide access-version conflicts may occur for operations targeting different users. That is deliberate: permission changes are infrequent and should be consciously reconciled, not automatically merged or rebased.
- No-op grant/revoke attempts produce stable durable outcomes while avoiding artificial revision churn.
- A revoked participant can still authenticate to their own account and use unrelated authorized decks.
- Security enforcement requires checks both at the leader's fresh admission boundary and during committed application. Neither client-side UI hiding nor UUID matching suffices.
- Existing element lock and offline intention behaviors after access loss remain separate Stage F responsibilities.

## Alternatives rejected

- **Treat OWNER as a grantable access level:** duplicates immutable ownership authority and creates contradictory ownership records.
- **A global user-level EDIT/VIEW role:** permissions are deck-specific, not intrinsic account classes.
- **Separate `update_access`:** unnecessary; owner-only `grant_access` upsert explicitly names the desired level.
- **Reuse `deck_version` or deck-settings revisions:** creates unrelated presentation-edit conflicts.
- **Per-participant revision counters:** complexity without demonstrated need for this baseline.
- **Permanent revoked-grant tombstones:** unlike bearer secrets, participants may legitimately be invited again; current grant state plus owner-only version-fenced commands is sufficient for F-02 mutation semantics.
- **Increment access revision for no-op outcomes:** generates false conflicts without changing authority.
- **Blindly retry stale permission changes:** could silently undo an intentional downgrade/revocation; owner reconciliation is required.
- **Grant sharing authority to EDIT participants:** violates immutable owner-only management.
- **A second authoritative access database or 2PC transaction manager:** unnecessary; Raft already orders and persists the grant mutations.

## Deferred Stage F decisions

1. **F-03:** freeze the complete per-operation permission matrix and distinguish permission-denial/public masking semantics.
2. **F-04/F-05:** permission-revocation/write races, apply-time denial and owner recovery, client notification, and permission-aware journal reconciliation.
3. **F-06:** protected deck reads, lagging-follower safety, freshness/availability tradeoffs and revocation visibility.
4. **F-07:** editor-session locks, lock renewal/expiration, and offline intention behavior after a downgrade/revoke.
5. **F-08:** authorized user lookup, deck discovery, and a deterministic ownership/access migration for pre-Stage-F v2 published decks.
6. Stage F implementation spec: exact request/response shapes, explicit tests and issue DAG, preserved-disk compatibility, and acceptance scenarios.

## References

- `docs/adrs/ADR-004-intent-preserving-client-outbox.md`
- `docs/adrs/ADR-006-deck-and-slide-structural-operations.md`
- `docs/adrs/ADR-008-versioned-deck-and-slide-settings.md`
- `docs/adrs/ADR-009-preprovisioned-user-accounts-and-raft-storage.md`
- `docs/adrs/ADR-010-user-client-session-and-authenticated-command-boundary.md`
- `docs/adrs/ADR-011-replicated-bearer-token-rotation-and-revocation.md`
- `docs/adrs/ADR-012-account-bound-deck-creation-and-immutable-ownership.md`
- `docs/specs/Stage-E-Authentication-Implementation-Spec.md`
- `docs/user-stories/user-story-01-creating-and-sharing-decks.md`
