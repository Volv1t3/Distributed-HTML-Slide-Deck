# Stage F — Deck Ownership, Sharing, and Protected Reads Implementation Specification

**Status:** Implementation contract — reviewed revision (Stage F not yet implemented). **Authority:** accepted ADR-012–018 and the proposed corrective ADR-019 (its activation mechanism remains subject to formal ADR acceptance). **Baseline:** Stage E code at GitHub `master` commit `267a2674174dc255943e359a9df3d8c1fceaa6e7` (2026-11-26); the eight Stage F slices are Issues #54–#61 under #53. This specification is not evidence that any Stage F behavior has been implemented.

## Problem Statement

Stage E authenticates accounts but does not establish deck ownership or determine whether an authenticated user may read, edit, or share a particular deck. Existing follower-local presentation reads expose deck data without a fresh permission check. Existing v2 publications have no trustworthy owner. Editors also need to preserve local work and determine uncertain operation outcomes when access changes. Stage F must close these gaps without losing historical Raft replay or changing the established lock, settings, and intention identities.

## Solution

An established signed-in User Account owns each new private draft and each new v3 published deck. A committed publication atomically creates immutable ownership and empty access state. The owner may grant `VIEW` or `EDIT` to registered accounts through independently versioned Raft commands. The leader authorizes all shared-deck reads through a fresh ReadIndex and checks each mutation again at deterministic apply. The client retains account-and-deck-scoped intentions behind a durable synchronization gate after permission loss. Historical v2 decks remain quarantined unless an explicit Raft migration verifies their original committed authenticated publication provenance. A replicated schema-activation fence separates historical replay from new v3 publication.

## User Stories

1. As a signed-in creator, I want a private local draft bound to my account, so that reconnecting does not change its owner.
2. As a disconnected creator, I want to continue local draft editing under my established account, so that a temporary outage does not erase my work.
3. As a creator, I want publication to confirm my account identity, so that another account cannot claim my draft.
4. As an owner, I want a newly published deck to be private, so that replication alone does not share it.
5. As an owner, I want to grant a registered person VIEW or EDIT, so that collaboration is deliberate.
6. As an owner, I want to change a participant's level, so that access can evolve without changing deck content.
7. As an owner, I want to revoke a participant, so that their future reads and mutations fail.
8. As an owner, I want a version conflict on concurrent grant changes, so that I can review the current access state before retrying.
9. As an owner, I want to inspect the current grant roster, so that I can reconcile sharing decisions.
10. As an owner, I want to resolve a known complete email to a registered account, so that I can grant access to the correct user ID.
11. As an EDIT participant, I want to modify slides, elements, locks, and settings subject to ordinary preconditions, so that sharing retains editor behavior.
12. As a VIEW participant, I want to read deck content without editing or managing grants, so that my role is enforced consistently.
13. As a nonparticipant, I want my own effective permission reported safely, so that the client can stop remote polling without learning private deck data.
14. As a revoked participant, I want my unsent local edits preserved, so that I can review them if access returns.
15. As a revoked participant, I want a minimal receipt for an uncertain earlier operation, so that I can distinguish a committed outcome without obtaining private content.
16. As a regranted editor, I want to review and reconcile suspended edits explicitly, so that old patches and lock tokens are not submitted automatically.
17. As an editor with multiple decks, I want each deck's journal and permission gate isolated, so that losing access to one does not block another.
18. As an editor on another device, I want the same account's grant to apply there after login, so that access is tied to my account rather than an installation.
19. As an editor whose grant was revoked while a lock existed, I want existing lock state preserved until normal expiration, so that revocation cannot rewrite Raft lock history.
20. As an owner, I want a later authenticated read to reflect committed revocation, so that lagging followers cannot leak protected content.
21. As a replica operator, I want historical v2 log entries to replay unchanged, so that an upgrade does not stall catch-up or restart.
22. As a replica operator, I want unverifiable legacy decks quarantined, so that no ownership is guessed from sessions or client IDs.
23. As a replica operator, I want eligible v2 decks migrated through a verified committed transition, so that their old content and revisions survive with a trustworthy owner.
24. As an evaluator, I want clean three-replica fault scenarios and secret scans, so that permissions, availability, and confidentiality claims are backed by evidence.

## Implementation Decisions

### Identity, compatibility, and publication

1. Keep `user_id`, `client_id`, `request_id`, `session_id`, and `node_uuid` separate. A local draft is account-bound; offline local editing may continue under established signed-in identity, but remote operations require current Stage E authentication. Sign-out or account switch cannot reassign a draft. No anonymous creation.
2. Retain strict frozen v2 and new v3 presentation DTOs, discriminated by the integer `schema_version`; v3 requires UUID `owner_user_id` and retains `owner_session_id` as a non-authority editor-session field. Reject malformed/unknown versions without fallback. Preserve generic command-envelope schema 1 and historical command bytes/fingerprints.
3. Introduce an internal, deterministic, idempotent Raft activation command. At committed index N it stores a separately versioned `v3_enforced_from_index=N+1` policy. Only an auth-ready leader with coordinated Stage F binaries on all voters proposes it. The public adapter rejects new publications while activation is pending. Historical v2 entries before the fence replay their old behavior; v3 before the fence is unavailable; v2 at/after the fence is `UNSUPPORTED_PRESENTATION_SCHEMA` with no deck creation. Activation never migrates existing decks.
4. At v3 publication, Stage E stamps the authenticated actor. Apply checks current credential, registered owner, `owner_user_id == actor`, absence of an existing deck, and structural constraints before any mutation. Ownership and version-1 access record (`access_version=0`, no grants) appear atomically; no silent owner rewrite or implicit sharing. Keep the immutable authoritative owner in the deck record and carry it through settings reconstruction without admitting it as a settings patch. Strip `owner_session_id` from authoritative state. A different client of the same user retains owner authority.
5. Refresh auth-catalog status after a successful admission ReadIndex. If auth schema v2 is unready or the configured fingerprint mismatches, return uncommitted `AUTH_UNAVAILABLE` before bearer lookup, proposal, or dedup disclosure. All guarded reads use the same readiness predicate. Leader/quorum failures retain their unavailable precedence.

### Grants and deterministic authority

6. A deck has exactly one immutable owner and zero or more current grants keyed by canonical registered user UUID. Grants contain only `VIEW` or `EDIT`; `OWNER` and `NONE` are effective classifications, never grant values. The access DTO has its own schema version and `access_version`, independent of deck, settings, slide, element, lock, and credential revisions.
7. Only owner-authorized `grant_access` (upsert) and `revoke_access` (remove) change grants. Both require `expected_access_version` and use Stage E bearer admission plus server actor stamping. Apply validates credential, owner, expected access revision, registered nonowner target, then existing grant. Effective add/change/remove increments access revision once; unchanged/absent/conflict/denied commands do not. Regrant after revoke requires a fresh command and current revision. Preserve exact statuses `ACCESS_GRANTED`, `ACCESS_UPDATED`, `ACCESS_UNCHANGED`, `ACCESS_REVOKED`, `ACCESS_NOT_FOUND`, `ACCESS_VERSION_CONFLICT`, `ACCESS_DENIED`, `INVALID_ACCESS_TARGET`, `INVALID_TARGET_USER`, and Stage E auth/dedup statuses. `ACCESS_NOT_FOUND` means an absent grant, never a missing deck.
8. Resolve effective role as OWNER for the verified owner, EDIT/VIEW for a current grant, otherwise NONE. External presentation mutations, including all eleven post-publication editing operations and deck/slide settings, require EDIT or OWNER. Sharing mutations require OWNER. Credential validity and deck role are checked at apply in committed order before lock, fencing, structure, or version checks. A valid old lock never overrides revoked access. Permission changes never rewrite locks or other revisions; internal inactivity `expire_lock` remains permission-independent. A revoked client's renewal service stops new renewals without sending cleanup release; existing in-flight renewal follows normal Raft order.
9. Missing deck, quarantined v2 deck, and existing inaccessible deck must have privacy-safe outward handling: protected content/owner management reads return identical `ACCESS_DENIED` envelopes for unknown and inaccessible decks; `query_grants` returns indistinguishable `OK/NONE`. Sharing and mutation denials reveal no owner, grants, revisions, or private conflict snapshots. Current role is checked before full dedup-result disclosure; a revoked caller cannot obtain an old full result by exact-ID replay.

### Protected reads and receipt recovery

10. Route `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status`, `authoritative_refresh_element`, and `refresh_element` through one leader-owned authenticated ReadIndex family. Each read uses a fresh current-term quorum fence, applies through that index, rechecks bearer and VIEW-or-higher role, then projects without yielding. `refresh_element` is an exact alias and retains `min_commit_index`. Followers redirect/fail; quorum loss never becomes a role denial or stale success. Each subordinate read independently reauthorizes. Stage A generic KV `get` stays unchanged.
11. `query_grants` is a self-only authenticated ReadIndex read, allowed even for NONE. Positive OWNER/EDIT/VIEW responses carry `effective_access`, `deck_id`, `access_version`, and `applied_index`; missing/inaccessible decks return only safe `OK/NONE`. Authorized preview uses the same canonical `effective_access` field. `query_access_state` is separate OWNER-only ReadIndex roster inspection, with sorted target UUIDs and current revision; nonowners/missing deck receive identical denial. `resolve_registered_user` performs bounded exact normalized-email resolution only after an OWNER ReadIndex check; a verified owner receives a minimal public profile or `USER_NOT_FOUND`, while other callers cannot distinguish accounts/decks.
12. `query_operation_receipt` is a read-only ReadIndex lookup over existing committed dedup records. Derive the original key from authenticated current `client_id` and `original_request_id`, verify the original log actor matches the same account, and whitelist precisely the eleven post-publication editing commands. Return only `COMMITTED_SUCCESS`, `COMMITTED_REJECTED`, or `OUTCOME_UNKNOWN`; no original result fields, deck data, grant data, or credential details. UNKNOWN is inconclusive and may later become conclusive. A revoked editor may learn this minimal status but cannot retrieve the original full result.

### Client synchronization and legacy state

13. Bind each durable outbox permanently to one `(user_id, deck_id)` and keep distinct journals/projections/indexes/gates across decks; multiple outboxes may share one authenticated installation transport. On replay, verify stored scope. Reject mixed-deck or unproven legacy journal adoption without truncating or reassigning historical intentions.
14. Keep existing six intention lifecycle states. Add a separate durable per-account/per-deck synchronization gate and volatile, monotonically ordered permission observation. VIEW/NONE/UNKNOWN or unavailable authority suspends new protected sends from every outbox entry point; an acknowledged `ACCESS_DENIED` becomes terminal for that request identity and triggers self-permission refresh. Preserve unsent/in-flight patches and IDs. A positive role after restart requires fresh verification; a restored role after an observed loss does not auto-release the review gate. Explicit user-directed reconciliation uses fresh state, lock authority, and a new ID for corrected work; a read-only local recovery view exposes retained denied patch material only within its account/deck scope.
15. Poll shared decks through authenticated leader reads, not follower-local projections. Permission observation uses access revision, applied index, and a local response-generation fence for NONE; delayed positives cannot override newer denial. A subordinate denied read prevents installing a partial successful poll. Receipt reconciliation has a dedicated adapter: SUCCESS marks the original committed without inventing lock/version output; REJECTED records generic terminal denial; UNKNOWN preserves uncertainty and forbids unauthorized resubmission.
16. Keep ownerless v2 decks intact and quarantined: no external deck edit, protected content, or grant access, and self-permission reports NONE. Migration is explicit and Raft committed only when the original successful committed v2 `publish_deck` record, dedup result, deck ID, and Stage E server-stamped registered actor prove ownership. **Provenance bridge:** at migration apply, the Raft engine supplies an immutable deterministic verification context derived from its retained committed original log entry and matching request-result record; the FSM cross-checks that context and current source deck against migration parameters before applying. The command alone, leader-local assertion, client-supplied actor, session/client ID, or later edit is never proof. If log/result evidence is absent or cannot be checked identically by all voters, deny migration and leave quarantine. Preserve content, tombstones, settings, locks, counters, and revisions; add only owner/access state and schema v3. No log compaction is introduced, so retained committed history is the expected evidence source. Snapshot/restart tests must prove the bridge or fail closed when evidence is unavailable.

## Testing Decisions

- Primary acceptance seam: public authenticated request through the serialized App event loop and the existing clean three-replica scenario runner. Secondary seam: deterministic committed-command/replay tests, including persisted follower catch-up. Tests assert visible result, replicated state, secrecy, and journal lifecycle rather than private helper calls.
- Preserve Stage A–E regression expectations while intentionally updating legacy tests that assumed public follower-local presentation reads. Never weaken Stage F guards to make those fixtures pass. Use the existing reference model and checker for publication, grants, mutations, and linearizable protected reads; generic KV remains eventual.
- Test strict v2/v3 decoding, activation at N/N+1, old unapplied v2 replay, post-fence v2 denial, readiness mismatch and v1 upgrade, v3 publication owner mismatch, owner immutability through settings and token rotation, and legacy quarantine/migration proof failures.
- Test every grant status and precedence, version independence, duplicate and changed-ID replay, owner devices, target validation, role matrix across structure/element/lock/settings, and both revoke/edit commit orders.
- Test all protected read names, follower redirect, current-term/quorum/apply fences, unknown-deck masking, owner-only roster/email discovery, self-only `query_grants`, receipt whitelist and actor binding, and nondisclosure after revocation.
- Test per-deck outbox restart, mixed-journal fail-closed migration, access-loss gate, no automatic resend, retained patch recovery, monotonic delayed observations, lock renewal stop/expiry, and two independent decks on one client.
- Run independently clean live scenarios for publication/sharing, downgrade/revocation races, stale followers, leader replacement, legacy replay/migration, and client restart. Archive checker projections, role/fault traces, applied indexes, authorization outcomes, and latency. Scan public journals, histories, operational logs, and archives for raw passwords and bearer tokens; replicated token digests remain internal verifier state and must never appear in public projections.

## Out of Scope

Account registration, public links, guests, anonymous drafts, owner transfer, co-owners, account takeover or administrative grant override, broad account directories, global deck search, grant tombstones, automatic permission push, follower-delegated authorization, automatic conflict merge, lock theft or instant permission-triggered expiration, cancellation of in-flight operations, retroactive erasure of downloaded data, log compaction, new Raft wire messages, and production TLS/session expiry.

## Detailed Implementation Contract (normative handoff)

The following sections expand the numbered implementation decisions above rather than replacing them. ADRs retain precedence where a concise example here omits a condition. Implementation agents must preserve Stage A–E public status meanings, deduplication, Raft safety and existing journal intention lifecycles. **Do not turn a Stage F requirement into a claim about the Stage E baseline.**

### A. Explicit boundaries, source owners and execution sequence

| Boundary / code today | Stage F responsibility | Applicable issue |
|---|---|---|
| `command_dto.py:PresentationSnapshotDTO`, `PublishDeckPayloadDTO`, `validate_committed_command()` | Strict versioned v2/v3 payloads and internal activation DTO; preserve old committed bytes | #54 |
| `state_machine.py:apply()`, published deck data, `PresentationMetadataDTO`, settings handlers | Historical v2 replay; v3 owner/access creation; immutable ownership; role checks at committed apply | #54, #55, #57 |
| `app.py:_progress_auth_admitted_commands()` and `_progress_login_reads()` | Refresh readiness after successful ReadIndex; `AUTH_UNAVAILABLE` without proposal when auth-v2 unready | #54 |
| `app.py:_presentation_read()`, `_authoritative_element_refresh()`, `_reply_authoritative_refreshes()` | Replace follower-local/unprotected shared reads with common leader + ReadIndex + bearer/role completion | #58 |
| `raft_core.py:begin_read_index()` / `read_index_status()` | Reuse current-term quorum fencing; no new consensus message | #58 |
| `raft_core.py:state.request_results` plus retained `raft.log` | Same-client status-only receipts; verified legacy-publication provenance context | #59, #60 |
| `client_journal.py:ClientIntentionOutbox`, `JournaledClient.poll_if_due()` | One deck-bound outbox, durable gate, leader polling, local denied-patch recovery | #59 |
| `lock_maintenance.py`, `lock_expiration.py` | Stop new renewal attempts on loss; retain independently fenced leader expiry | #57, #59 |
| `spec.py:GUARANTEE`, `tests/`, `scenarios/` | Model all new commands and outcomes; guarded deck reads become linearizable | #54–#61 |

**Recommended dependency order:** #54 establishes DTO compatibility, readiness and the *activation mechanism*; #55 and #57 implement owner-state creation plus authoritative mutation checks; #56 implements grants and owner-only queries; #58 closes every protected read route; #59 completes client/receipt safety; #60 migration; #61 final gate. The issue-number order is not a license to activate v3 as soon as #54 passes. Where practical, implement server role helpers and query DTOs in parallel, but integrate them behind an inactive schema fence until authorization and read hardening are complete. #60's provenance implementation may be developed independently once its evidence contract is pinned; it must not weaken quarantine to unblock progress.

### B. Cutover is a cluster-wide security gate, not merely a DTO switch

1. Schema activation is a **separate internal Raft command**, persisted as `v3_enforced_from_index = activation_index + 1`, idempotent and unavailable through `/client`. No startup-time upgrade, feature flag, or local clock can substitute for this replicated boundary. A preserved v2 entry applied before the boundary keeps its original semantic and auth-enforcement behavior, even if executed by Stage F code later. The same binary must decode both versions, but public admission accepts only post-activation v3.
2. **Prerequisite before proposing activation:** Stage F code and configs are deployed on all three fixed voters; auth catalog v2 is READY with matching fingerprint; protected-read routing, apply-time permission checks, privacy-safe dedup replies, quarantine and v3 creation paths are wired and pass integration tests. An operator must verify these conditions because current Raft membership messages do **not** prove the installed application build version. Do not claim automatic cluster-wide binary attestation exists. If these prerequisites are not met, keep publication unavailable and do not activate v3.
3. **Before activation:** the new public adapter does not propose any new `publish_deck` (v2 or v3), returning the agreed `PRESENTATION_SCHEMA_UNAVAILABLE`. Previously committed v2 entries may still be applied under their historical indices. New v3 entries before the fence have no business effect. **After activation:** public v2 is rejected before proposal and an exceptional committed v2 command at or after the fence deterministically returns `UNSUPPORTED_PRESENTATION_SCHEMA` without creating a deck. Unknown and malformed versions do not fall back to historical v2.
4. Activation does not grant permission to legacy v2 decks and does not retroactively migrate them. All protected paths must reject access to unverified v2 decks, whether or not the caller possesses a prior lock/session or is currently authenticated.
5. After a minority partition or leadership change, only the committed fence in applied FSM determines the active publication schema; no follower-local override. A binary incapable of safely interpreting that fence must not participate in the activation deployment.

### C. DTOs, persisted state and immutable metadata

**Distinct contracts:** `PresentationSnapshotV2DTO` is the strict frozen Stage D publication form. `PresentationSnapshotV3DTO` requires `metadata.owner_user_id` and retains local/publication `owner_session_id`. The tagged union is selected *only* by integer `schema_version`; reject booleans, absent tags, versions 1/4, and malformed v3 instead of coercing. Historical `command_schema_version` remains 1. Do not modify previous request fingerprints or dedup records.

**Authoritative v3 deck projection** (illustrative; use the exact ADR-013 names/types):

```json
{
  "schema_version": 3,
  "metadata": {
    "deck_id": "<deck-uuid>",
    "name": "Example",
    "owner_user_id": "<verified-account-uuid>",
    "width_px": 1280,
    "height_px": 720
  },
  "access": {
    "schema_version": 1,
    "access_version": 0,
    "grants": {}
  }
}
```

These fields coexist with the existing `slides`, `deck_version`, `deck_settings_version`, `slide_tombstones`, `element_tombstones`, `locks`, `fencing_counters` and `next_creation_sequence`. This is a partial projection, not a replacement for those fields. The owner is authoritative **at `deck["metadata"]["owner_user_id"]`** per ADR-012; do not silently relocate it to the deck root or make it a grant. `owner_session_id` is removed at publish and never interpreted as an account credential.

`state_machine.py:_update_deck_settings()` currently validates a prospective `PresentationMetadataDTO` using a synthetic `owner_session_id`. Its Stage F replacement must validate *only settings-editable fields* (prefer a dedicated settings DTO or a version-aware constructed metadata DTO) and must preserve the authoritative `owner_user_id` verbatim; its typed patch whitelist must reject owner fields. Other settings operations and canvas resize preserve all presentation and access revision domains. Owner and grant data must not leak through error/conflict payloads to an unauthorized actor.

**Apply guard ordering (first applications):** validate canonical command/payload; validate committed credential under the historical/current auth rule; determine deck ownership and effective role; return privacy-safe permission failure if unauthorized; only then disclose current lock/structure/revision/business conflicts or mutate. For v3 publication, compare the claimed input owner to the server-stamped actor, verify account registration, and handle deck-ID collisions without disclosing another account's deck/version. Do not simply return `DECK_ALREADY_PUBLISHED` containing `deck_version` to an unrelated actor; an owner-safe collision retains appropriate idempotent behavior. The exact public collision status should be fixed in code/contract tests before shipping, without renaming legacy committed results.

### D. Wire/request, permission and return-value contract

All examples below are **logical operation arguments**, not complete wire envelopes. `Request(id, op, args, client)` supplies stable IDs. `_auth.access_token` is injected only by the authenticated transport; it is never persisted inside a Raft command or journal and no caller-supplied actor field is trusted.

| Operation | Required logical args | Authorization/read path | Success or safe outcome |
|---|---|---|---|
| `publish_deck` | `presentation` (strict v3 snapshot with owner UUID) | Leader bearer admission + activation fence + committed actor/owner match | `PUBLISHED`, private owner and access state atomically |
| `grant_access` | `deck_id`, `target_user_id`, `access_level=VIEW|EDIT`, `expected_access_version` | Leader admission; OWNER required at apply | `ACCESS_GRANTED`, `ACCESS_UPDATED`, `ACCESS_UNCHANGED` |
| `revoke_access` | `deck_id`, `target_user_id`, `expected_access_version` | Leader admission; OWNER required at apply | `ACCESS_REVOKED` or `ACCESS_NOT_FOUND` (absent **grant**, not deck) |
| `query_grants` | `deck_id` | Authenticated, leader ReadIndex, any role including NONE | `OK` and self `effective_access` only; nonexistent/inaccessible indistinguishable `NONE` |
| `query_access_state` | `deck_id` | Authenticated, leader ReadIndex, OWNER only | `OK`, sorted grants, `access_version`, `applied_index` |
| `resolve_registered_user` | `deck_id`, exact complete `email` | Authenticated, leader ReadIndex, OWNER only | `OK` with registered public profile or `USER_NOT_FOUND` |
| `query_operation_receipt` | `original_request_id` | Authenticated, leader ReadIndex, same client **and** original user | `COMMITTED_SUCCESS`, `COMMITTED_REJECTED`, `OUTCOME_UNKNOWN` only |
| Four deck reads + two refresh names | Per existing endpoint; refresh also needs `min_commit_index` | Authenticated, leader ReadIndex, VIEW+ | Content only on current authorization; otherwise masked `ACCESS_DENIED` |

Grant commands must accept canonical registered UUIDs and require `expected_access_version`. Their authorization/status precedence is **credential → deck/owner role → access-version CAS → target validation → grant existence/change**, with the exact invalid-target rules and no-op statuses from ADR-013. Only an effective grant change increments `access_version`; no grant mutation changes presentation, element, settings, lock, or credential revisions. OWNER is implicit and cannot be stored as a grant. Keep `INVALID_ACCESS_TARGET`, `INVALID_TARGET_USER`, `ACCESS_VERSION_CONFLICT`, `ACCESS_DENIED`, and `ACCESS_NOT_FOUND` distinct.

For nonowners, missing decks and inaccessible existing decks use the same public `ACCESS_DENIED` envelope for protected content reads and owner-management reads. For `query_grants`, both yield `status="OK", effective_access="NONE"`, without deck ID, index, grants or version. Successful authenticated preview includes caller `effective_access`, `access_version` and `applied_index`. Do **not** add a `legacy: true` indicator for unknown/quarantined v2 decks: it would allow enumeration. Ownership and roster fields are only projected on routes/roles authorized by their respective ADRs.

`resolve_registered_user` normalizes the exact email by the existing auth catalog's trim-and-lowercase rule; the normalized registered index is authoritative. No partial search, bulk list or unauthenticated response. Account nonexistence is visible **only after verified deck OWNER authorization**, a deliberate bounded disclosure under ADR-018.

**Unified failures:** missing/invalid/revoked bearer at admission → `REAUTH_REQUIRED`; auth catalog not READY after fresh ReadIndex → `AUTH_UNAVAILABLE`; no leader or quorum → corresponding transport availability failure, *not* a fabricated NONE/ACCESS_DENIED; committed credential failure → `UNAUTHENTICATED`; committed role failure → `ACCESS_DENIED`; malformed request → `INVALID_REQUEST`/existing validation outcome as applicable. Status location matters: transport errors must not be misrecorded as committed journal outcomes.

### E. Protected-read completion and dedup result privacy

All six content/refresh routes, self-permission and receipt queries, owner roster and email resolution share **one audited leader-bound completion pattern**:

1. Redirect/refuse nonleaders; parse typed request and reject caller-supplied authority stamps; hold bearer only in a volatile waiter. Do not serve a protected route via old `_presentation_read()` or an alias-specific bypass.
2. Start a fresh current-term `begin_read_index` with `min_commit_index=0` unless authoritative refresh requires the supplied floor. Bind waiter to request, client, captured term, canonical operation and read context; clean up the waiter and `finish_read_index()` on every terminal path.
3. Once READY **and applied**, re-evaluate auth catalog readiness (matching fingerprint and auth-state v2), current bearer/client/user binding and effective role against the applied FSM; serialize authorization and projection without yielding. If majority, term or apply fence fails, return availability failure and **no** content.
4. For protected content, require current OWNER/EDIT/VIEW. For self `query_grants`, permit NONE with safe shape. For receipts, apply same-client/same-account original actor proof instead of current deck grant. For owner queries, require OWNER before looking up roster/email.
5. A multi-request client poll is **not an atomic multi-resource snapshot**: each subordinate read gets its own fresh fence and role check. If any subordinate read is denied/unavailable, the client must not install an allegedly complete base projection. Because individual applied indices may differ across calls, the client must not label the assembled view as a single consistent Raft snapshot; monotonic per-resource refresh or a separately specified batching API would be needed for that stronger claim.

**Dedup is not authorization.** `raft_core.py:propose()` returns a stored `request_results` result for an exact `(client_id, request_id, fingerprint)` match. `app.py:_immediate_result_response()` and `_reply_committed()` may expose stored values. Stage F must gate **all** externally visible full-result paths with a fresh current authorization decision, including exact-ID duplicate replays, delayed in-flight replies and settings/version/lock conflict results. Do not change the original stored result or replay its business mutation. If the caller is no longer authorized, return only safe denial; use `query_operation_receipt` for eligible minimal outcomes. A committed historical result remains in durable state but is not a disclosure entitlement.

For receipt queries, derive `sha256_json([authenticated_client_id, original_request_id])`; inspect the persisted original committed log command and matching dedup result; verify its server-stamped original actor is the same **account**, not merely an installation sharing a name. Only the eleven ADR-015 eligible editing commands count. A missing, ineligible, actorless or unprovable original result returns `OUTCOME_UNKNOWN` without revealing why. Map original operation-specific success statuses (including `ALREADY_HELD`) to `COMMITTED_SUCCESS`; all other proven committed outcomes to `COMMITTED_REJECTED`. Do not echo original result values, original operation type, lock tokens, revisions or deck fields. `OUTCOME_UNKNOWN` may be followed by later success; it must not trigger automated unauthorized resend.

### F. Deterministic v2→v3 migration evidence contract

A legacy v2 deck is always **preserved and quarantined until migration commits**: no protected content read, external deck mutation, grant management, or inferred owner. Read/status denial must not distinguish it from another inaccessible or unknown deck.

The migration command must identify a currently stored v2 `deck_id` and a specific original publication reference (index and original request identity), without trusting a claimed owner. Its name and exact DTO are an implementation detail of Issue #60; this contract restricts the proof and effects.

At *deterministic application* of the migration entry, the Raft core constructs an **immutable verification context** using only already durably committed evidence: the retained original log entry, its `publish_deck` operation and deck ID, the original `request_results` dedup key/fingerprint/status (`PUBLISHED`), the original index and authenticated server-stamped actor, plus the authentication enforcement boundary. All voters independently derive and validate the same context against the same log/state. Reject any original publication without valid Stage E-enforced identity provenance, absent/discordant or uncommitted result, actorless pre-enforcement record, wrong deck, or altered source schema. Merely passing a copy of user/credential fields in the migration payload or validating a currently active credential is not equivalent proof. If evidence has been lost or diverges, fail closed and leave v2 unchanged. **No snapshots, log pruning or arbitrary historical reconstruction are added to paper over missing proof.**

On proof success, one committed migration changes only the deck's `schema_version` to 3, installs the verified immutable `metadata.owner_user_id` and initializes access schema 1/version 0/empty grants. The exact original slides, metadata settings, deck/slide/element revisions, tombstones, creation sequence, locks and fencing counters survive byte-equivalently where canonical representation permits. An exact repeated migration request has stable dedup result, not a second assignment. Different attempts cannot overwrite an already-migrated owner. Do not reinterpret a later editor's actor as original publisher.

**Important implementation seam:** `KeyValueStateMachine.apply(command, committed_index)` currently has no log/dedup parameter. Make the provenance bridge an explicit, read-only input from Raft's committed state; do not let the FSM reach mutable leader-local variables or perform nondeterministic I/O. Verify its contents identically on all voters. This narrowly scoped interface extension is necessary even though the Raft consensus/network algorithm remains unchanged.

### G. Durable client authority and local-intention recovery

Keep a dedicated journal/outbox instance for each permanent `(user_id, deck_id)` and the six existing lifecycle states. Each instance has a separate projection, `highest_applied_index`, resource index, append-only event log, and durable permission gate. A single `AuthenticatedClient` transport can serve multiple instances without sharing their pending work or per-deck positive authorization. Do not duplicate the whole journal as a new multi-deck scheduler when scoped instantiation and strict scope checks suffice.

| Event | Durable local effect | Remote behavior |
|---|---|---|
| New or restarted outbox without fresh authorized role | Preserve intention history; gate unavailable/closed | Do not send protected mutations |
| Fresh OWNER/EDIT observation, no prior loss requiring review | Record permission observation; gate may become eligible under ADR-016 rules | Still require lock/dependency/precondition checks |
| VIEW/NONE or committed `ACCESS_DENIED` | Close gate; stop renewal; persist denial/suspension | No further edit requests; query self-role when possible |
| Transport/quorum/auth unavailability | Preserve uncertainty and original ID; do not guess NONE | No protected send until current authority verified |
| Access restored after observed loss | Preserve closed review gate and old patches | User reviews/reconciles with fresh state, lock and **new ID** for corrected operations |
| Terminal denied edit | Keep terminal lifecycle and retained original arguments | Read-only local recovery view; original request never auto-replayed |
| Receipt `COMMITTED_SUCCESS` / `COMMITTED_REJECTED` / `OUTCOME_UNKNOWN` | Confirm success without fabricated details / terminal generic rejection / remain uncertain | Never infer lock tokens or auto-resubmit unknown work while unauthorized |

The existing `JournaledClient.poll_if_due()` points to `local_replica`, and the present `ClientIntentionOutbox` has a single projection/index. Stage F must route protected polling through leader transport, scope each outbox to one deck and reject cross-deck enqueues/replay. Do not adopt or truncate legacy mixed-deck journals without a proven, lossless explicit migration. `poll()` must reject partial subordinate reads that failed authorization or quorum; stale positive permission responses must not override a newer NONE observation. New permission observations cannot be authority to bypass the server's current apply-time guard.

Lock maintenance: once current EDIT/OWNER is lost or cannot be verified, stop **new** `RenewalWorker` sends; do not synthesize an unauthorized cleanup `release_lock`. Retain committed locks until Stage C `expire_lock` matches `(fencing_token, lease_epoch)`. A fresh grant requires a new lock acquisition and authoritative element read, not reuse of cached fencing tokens.

### H. Required test fixtures, error paths and evidence

Implement the following as **black-box behavioral guarantees**, supplemented by deterministic FSM tests. Every new API status must have a fixed response shape and an explicit handling path in the journal. Add negative tests, not only success smoke tests.

| Acceptance gate | Minimum evidence | Severity if absent |
|---|---|---|
| **G0 — Cutover safety** | All three nodes deployed with protected read/role/response guards **before** internal activation; activation index N and fence N+1 durable on restart/failover; v2 prior entries apply; new v2 cannot publish | Blocking |
| **G1 — Auth readiness** | Persisted auth v1 / unready / fingerprint mismatch after ReadIndex return `AUTH_UNAVAILABLE` without proposal or result disclosure; normal READY and historical pre-enforcement replay still work | Blocking |
| **G2 — Ownership integrity** | Missing, changed and wrong-account owners denied; same user on different device remains OWNER; settings cannot change/drop owner; draft sign-out/account switch isolated | Blocking |
| **G3 — ACL semantics** | Every ADR-013 grant status and CAS race; unchanged/absent operations do not increment access revision; EDIT cannot grant; revocation affects subsequent committed mutations | Blocking |
| **G4 — All read routes** | All six content reads, alias, self role, owner roster and email lookup use fresh leader ReadIndex; stale follower/minority fail closed; unknown/inaccessible return identical shape | Blocking |
| **G5 — Result privacy** | Same-ID replay and delayed committed response after revocation never disclose full private success or conflicts; status-only receipt preserves original-account/client restrictions | Blocking |
| **G6 — Client durability** | Two decks/one account, separate accounts, restart, uncertain receipt, mixed journal replay, denied terminal patch view, no automatic resend after regrant | Blocking |
| **G7 — Conditional migration** | Pre-Stage-E/unprovable v2 stays quarantined; provable post-enforcement v2 migrates with same content/revisions/locks; bad proof and missing old log/result cannot migrate; failover/restart deterministic | Blocking |
| **G8 — Regression and confidentiality** | Stage A–E regressions with documented intentional change to protected read expectations; current KV `get` remains eventual/isolated; no raw tokens/passwords in public logs/history/journal | Blocking |

Required failure-ordering tests include `edit → revoke`, `revoke → edit`, `read → revoke`, `revoke → read`, access downgrade with held lock, duplicate result after revoke, expired lock after revocation, quorum loss after ReadIndex starts, leader change during admission and read, request ID reuse, positive permission response delivered after NONE, and migration under missing historical provenance. Check not just response statuses but committed index, every replica's durable state and whether a disclosure occurred.

**Evidence checklist per issue:** link exact source change and test name; attach deterministic fixture plus clean 3-node scenario if it crosses the network boundary; retain original and final applied indexes, role/failure trace, redacted request and result, expected/actual status, and regression output. Do not claim an end-to-end security guarantee when only a state-machine unit test passed. Preserve the validated baseline and archive the Stage F checker/model projection updates.

### I. Existing GitHub slices and handoff constraints

- **#54 — Schema foundation:** v2/v3 strict replay, committed activation *mechanism*, auth readiness; do **not** activate before G0.
- **#55 — Account-owned publication:** immutable v3 owner and private grants, metadata/settings preservation, privacy-safe publication collision handling.
- **#56 — Sharing:** registered target validation, grant CAS/no-ops, owner roster and exact-email lookup.
- **#57 — Apply-time authorization:** complete eleven-operation matrix, lock renew/expiry ordering and full-result nondisclosure.
- **#58 — Protected reads:** all six names, guarded ReadIndex pipeline, self-role, no follower-local bypass and privacy-shape tests.
- **#59 — Journal and receipts:** per-deck durable gate, read-only denied-patch recovery, leader polling, uncertain operation receipt.
- **#60 — Migration:** deterministic original-commit provenance, retained evidence and fail-closed quarantine.
- **#61 — Validation:** execute and archive complete deterministic and three-node gates; only now claim Stage F privacy behavior implemented.

Issue blockers in GitHub describe **work scheduling**, not proof that activation prerequisites are met. An implementation coordinator must verify G0 independently and must not activate a partially secured cluster merely because #54 is closed. A security claim needs an end-to-end integration gate, not just passing DTO unit tests.

## Further Notes

- The Stage F cutover requires coordinated binaries on all voters before activation. The committed fence, not process startup time, is authoritative.
- Existing ADR-012 publication-input ownership and ADR-018 migration ownership agree when authoritative owner is retained as immutable deck state and settings updates preserve it. Historical v2 decks never acquire an owner through an input-field default.
- Stage F intentionally trades follower-local read availability for quorum-confirmed privacy. A minority cannot serve fresh protected content.
- Source of decisions: ADR-012 through ADR-019. Any future disagreement must be resolved by amending the relevant accepted ADR and this specification rather than by changing an implementation silently.