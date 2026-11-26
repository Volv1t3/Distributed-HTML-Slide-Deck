# Stage F — Independent Architecture Consistency and Implementation Readiness Audit

**Commit inspected:** `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a`
**Branch:** `master` (synced with `origin/master`, PR #52 merged)
**Date:** 2026-11-26
**Classification:** NOT READY

---

## 1. Executive Assessment

Stage F is directionally sound but **cannot be implemented directly from the existing ADRs** without resolving two critical architectural gaps and one high-severity integration flaw. Four high-severity findings must be resolved before implementation agents can produce compatible, deterministic code without excessive redesign.

### What is ready

- The ownership/grant/role model (ADRs 12–14) is consistent and well-reasoned.
- The per-deck gate and outbox-separation direction (ADR-016) is architecturally correct.
- The lock lifecycle rules (ADR-018 F07) are straightforward and compatible.
- The `query_grants` / `query_operation_receipt` patterns (ADR-015/017) are clean.

### What blocks implementation

| # | Finding | Severity | Summary |
|---|---------|----------|---------|
| F-001 | v2→v3 DTO union and replay incompatibility | **Critical** | `PresentationSnapshotDTO` hard-codes `Literal[2]`; v3-only breaks v2 log replay |
| F-002 | Provenance verification bridge undefined | **Critical** | `apply()` has no access to Raft log or `request_results`; migration cannot verify original publisher |
| F-003 | Ownership storage location contradicts mutable metadata path | **High** | `owner_user_id` in `PresentationMetadataDTO` breaks `_update_deck_settings` reconstruction |
| F-004 | Bearer admission missing auth-readiness gate | **High** | `_progress_auth_admitted_commands` never checks `auth_catalog_ready` |
| F-005 | No legacy-deck interim authorization policy | **High** | v2 decks have no `owner_user_id`; all Stage F checks would fail on them |
| F-006 | `_authorize_external_mutation` does credential only | **High** | No deck-role check; ADR-014's EDIT/OWNER matrix needs a new authorization handler |
| F-007 | Protected reads are completely unauthenticated | **High** | `_presentation_read()` is follower-local, no bearer, no role check |
| F-008 | Multi-deck client outbox requires major refactoring | **High** | `ClientIntentionOutbox` is single-journal, single-projection; ADR-016 requires one per deck |

---

## 2. Source Inventory

| Source | Location | Version | Status |
|--------|----------|---------|--------|
| Core implementation | `master` at `3ecced2` | Reviewed | Complete |
| ADR-001 through ADR-011 | `docs/adrs/ADR-*` (11 files) | Committed to `master` | Accepted |
| ADR-012 | `docs/adrs/ADR-012-account-bound-deck-creation-and-immutable-ownership.md` | Untracked file | Accepted (not implemented) |
| ADR-013 | `docs/adrs/ADR-013-raft-replicated-deck-access-grants-and-independent-permission-versioning.md` | Untracked file | Accepted (not implemented) |
| ADR-014 | `docs/adrs/ADR-014-deck-operation-permission-matrix-and-authorization-boundary.md` | Untracked file | Accepted (not implemented) |
| ADR-015 | `docs/adrs/ADR-015-revocation-read-freshness-and-permission-safe-client-recovery.md` | Untracked file | Accepted (not implemented) |
| ADR-016 | `docs/adrs/ADR-016-permission-aware-client-synchronization-and-durable-recovery.md` | Untracked file | Accepted (not implemented) |
| ADR-017 | `docs/adrs/ADR-017-leader-bound-authorized-readindex-and-self-permission-discovery.md` | Untracked file | Accepted (not implemented) |
| ADR-018 | `docs/adrs/ADR-018-revoked-lock-lifecycle-owner-access-inspection-and-legacy-deck-isolation.md` | Untracked file | Accepted (not implemented) |
| Stage E Auth Spec | `docs/specs/Stage-E-Authentication-Implementation-Spec.md` | Committed | Complete |
| User Story 01 | `docs/user-stories/user-story-01-creating-and-sharing-decks.md` | Committed | Draft |
| Reference model | `spec.py` | Committed | v2 only; no grants, ownership, or Stage F operations |
| Deterministic tests | `tests/*.py` (36 files) | Committed | 550 passed, 4 deselected, 33 subtests |
| GLOSSARY.md | Root | Committed | Missing ownership, grant, permission roles |
| Project proposal | `docs/project-proposal.md` | Committed | Stage F scoped as final back-end phase |

### Missing sources

- No Stage F implementation specification exists (expected — ADR-012 successor says "Stage F implementation spec" is future work).
- No integration test suite or scenario scripts for Stage F scenarios.
- `spec.py` has no `grant_access`, `revoke_access`, `query_grants`, `query_operation_receipt`, `resolve_registered_user`, `query_access_state`, or v3 deck model.

---

## 3. ADR Consistency Matrix

| ADR | Dependencies | Internal Consistency | Code Readiness | Outstanding Issues |
|-----|-------------|---------------------|---------------|--------------------|
| **012** — Owner-bound creation | 004, 006, 008, 009, 010, 011 | **Consistent** with Stage E boundary | **Not ready** — v3 DTO shape conflicts with `_update_deck_settings`; no ownership field in deck storage | F-003, F-005, F-009 |
| **013** — Access grants | 012 | **Consistent** internally; grant/revoke DTOs well-specified | **Not ready** — No `DeckAccessStateDTO`, no `grant_access`/`revoke_access` in code | F-006 |
| **014** — Permission matrix | 012, 013 | **Consistent**; role/operation mapping is complete | **Not ready** — `_authorize_external_mutation` does not check deck roles | F-006, F-007 |
| **015** — Revocation & receipts | 012, 013, 014 | **Mostly consistent**; receipt design is minimal and safe | **Not ready** — No `query_grants`, `query_operation_receipt`; no protected read routing | F-004, F-007 |
| **016** — Client sync & gate | 004, 012, 013, 014, 015 | **Consistent**; per-deck outbox is correct | **Not ready** — `ClientIntentionOutbox` is single-outbox, single-projection; `poll_if_due` targets `local_replica` | F-008, F-018 |
| **017** — Leader ReadIndex reads | 012, 013, 014, 015, 016 | **Consistent**; missing-deck masking resolved (Q1) | **Not ready** — No leader routing, no bearer check on reads, no ReadIndex pipeline for reads | F-007, F-010 |
| **018** — Lock lifecycle & migration | 012, 013, 014, 015, 016, 017 | **Partially consistent** — migration provenance bridge is explicitly deferred | **Not ready** — No provenanced migration command, no quarantine state, no `query_access_state`, no `resolve_registered_user` | F-001, F-002, F-011, F-012 |

### Cross-ADR conflicts

| Conflict | ADRs | Issue |
|----------|-------|-------|
| `owner_user_id` in `PresentationMetadataDTO` vs `_update_deck_settings` | 012, 006, 008 | Ownership placed in mutable metadata; settings reconstruction doesn't preserve it |
| v2 replay compatibility vs v3-only DTO | 012, 017, 018 | DTO `Literal[2]`→`Literal[3]` breaks replay; discriminated union needed |
| Migration provenance verification | 012, 018 | `apply()` has no access to log or `request_results`; bridge not designed |
| Bearer admission without readiness gate | 015, 017, Stage E spec | `_progress_auth_admitted_commands` missing `auth_catalog_ready` check |

---

## 4. Prioritized Findings

---

### F-001 — v2/v3 PresentationSnapshotDTO discriminated union required [CRITICAL]

| Field | Value |
|-------|-------|
| **Classification** | Architectural contradiction |
| **Affected ADRs** | ADR-012 §2 (v3 shape), ADR-017 §7 (spec/model updates), ADR-018 §4 (migration) |
| **Code evidence** | `command_dto.py:144-155` — `PresentationSnapshotDTO` with `schema_version: Literal[2]` |
| **Documentation** | ADR-012 §2: normative shape uses `schema_version: Literal[3]` |

**Problem:** If `PresentationSnapshotDTO` is changed to `Literal[3]` as specified in ADR-012's normative shape, and `PublishDeckPayloadDTO` validates it (implicitly through `validate_committed_command`), then **all historical v2 `publish_deck` entries in the Raft log fail deterministic replay**. A follower catching up, or any replica restarting with unapplied entries, would encounter a v2 entry whose payload DTO cannot be parsed as v3. `RaftEngine._apply_committed()` (`raft_core.py:598-621`) does not catch DTO validation errors; they would crash the replica.

**Concrete failure scenario:**
1. Cluster runs Stage E with 3 committed v2 `publish_deck` entries.
2. Stage F deploys with v3-only `PresentationSnapshotDTO`.
3. Replica A restarts and replays from last snapshot. Entry at index 42 requires `schema_version: Literal[3]` but stored payload has `schema_version: 2`.
4. `PublishDeckPayloadDTO.model_validate(command.payload)` raises `ValidationError`.
5. `machine.apply()` propagates the error as an unhandled exception; replica crashes or stalls. Raft cannot advance `last_applied`.

**Impact:** Correctness, availability, replay compatibility — **blocker**.

**Recommended resolution:** Introduce a discriminated union `PresentationSnapshotV2DTO | PresentationSnapshotV3DTO` in `PublishDeckPayloadDTO`. `validate_committed_command` accepts either. The state machine applies v2 entries with the existing behavior (no ownership) and v3 entries with ownership checks. Add an explicit replay test with retained v2 entries.

**Implementation dependency:** Must be resolved before any Stage F coding begins; otherwise every test run or deployment after the first v3 publication would break if a v2 entry remains in the log.

**Confidence:** High

---

### F-002 — Migration provenance verification bridge is undefined [CRITICAL]

| Field | Value |
|-------|-------|
| **Classification** | Ambiguous/incomplete requirement |
| **Affected ADRs** | ADR-018 §4.1 item 6, §4.2 |
| **Code evidence** | `state_machine.py:104-105` — `apply()` receives only `CommandEnvelopeDTO` and `committed_index` |
| **Documentation** | ADR-018 §4.1: "implementation agents must explicitly bridge this gap" |

**Problem:** ADR-018 requires a Raft-committed v2→v3 migration that verifies "original, successful, Raft-committed publication establishes a registered account owner through trustworthy Stage E authentication provenance." But `KeyValueStateMachine.apply()` receives only the current command and its commit index — it has **no access to the original `publish_deck` entry's command payload, the `request_results` deduplication record, or the Raft log**. The verification material (server-stamped actor fields, `credential_id`, `credential_generation`) lives in the original `publish_deck` command's `payload`, which is only available by reading the Raft log at the original publication index.

**Concrete failure scenario:**
1. A migration command commits at index 200, claiming to migrate deck D (published at index 50).
2. The migration command contains `original_publish_index: 50` and `claimed_owner_user_id: "<UUID>"`.
3. `state_machine.apply()` at `committed_index=200` has no way to independently verify that the entry at index 50 had valid server-stamped authentication proving that specific `user_id`.
4. The implementation would either: (a) trust the migration payload blindly (insecure), (b) forward the original entry's payload in the migration command (breaks log size limits and is a trust claim), or (c) require `apply()` to have cross-entry access (requires architecture change).

**Impact:** Security — without a verified bridge, either ownership can be fabricated or migration is unimplementable.

**Recommended resolution:** One of:
- **Option A (preferred):** Store a deterministic provenance summary in the deck record at publication time (e.g., `publisher_user_id`, `publisher_credential_generation`). This populates at v3 publish and could be retroactively filled during a separate provenance-extraction phase before migration.
- **Option B:** The migration command carries the original publication's authenticated fields, and `validate_committed_command` validates them against `request_results` — but this requires `request_results` to be accessible to the state machine, which currently it is not.

The ADR must specify which mechanism is chosen.

**Implementation dependency:** Requires architecture decision on FSM access to request_results or a persistent provenance field. Blocks migration command design.

**Confidence:** High

---

### F-003 — Ownership in mutable metadata conflicts with settings mutation path [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Architectural contradiction |
| **Affected ADRs** | ADR-012 §2 (normative shape), ADR-014 §2 (role resolution), ADR-008 (versioned settings) |
| **Code evidence** | `state_machine.py:631-652` — `_update_deck_settings` reconstructs `PresentationMetadataDTO` with `owner_session_id` placeholder; `command_dto.py:30-37` — `PresentationMetadataDTO` has `owner_session_id` but not `owner_user_id` |
| **Documentation** | ADR-012: `D.metadata.owner_user_id` is normative |

**Problem:** `owner_user_id` is an **immutable security property** but ADR-012 places it inside `PresentationMetadataDTO`, which is used as a mutable display metadata structure:
- `_update_deck_settings` (`state_machine.py:631-652`) creates a *new* `PresentationMetadataDTO` from patch fields plus a placeholder `owner_session_id`. If `owner_user_id` becomes a required field, this call breaks.
- If patched to copy `owner_user_id` from current metadata, ownership lives inside a structure that `_update_deck_settings` mutates freely (lines 648-652: `deck["metadata"]["name"] = patch.name`, etc.).
- `get_deck_preview` (`state_machine.py:871-884`) returns `deepcopy(deck["metadata"])` to any caller — unauthenticated, no bearer check. This would expose `owner_user_id` to everyone.

**Concrete failure scenario:**
1. Sofia publishes deck D (v3, `owner_user_id=Sofia`).
2. Sofia (or an EDIT collaborator) calls `update_deck_settings` with `name="New Title"`.
3. `_update_deck_settings` attempts `PresentationMetadataDTO.model_validate(prospective)` with `owner_user_id` missing → `ValidationError` → `INVALID_ARGUMENT`.
4. All subsequent settings mutations fail.

**Impact:** Availability — settings mutations break for every v3 deck; security — `owner_user_id` leaks via preview.

**Recommended resolution:** Store authoritative ownership as a **top-level deck field** (`deck["owner_user_id"]`), not inside `metadata`. The `owner_user_id` may still be included in `PresentationMetadataDTO` at publication-input validation, but the authoritative copy lives at the deck root. `get_deck_preview` checks authorization before including `owner_user_id`. `_update_deck_settings` does not touch `owner_user_id`.

**Implementation dependency:** Requires amending ADR-012's normative shape or adding a clarifying section on ownership storage separation.

**Confidence:** High

---

### F-004 — Bearer admission missing auth-readiness gate [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Existing defect |
| **Affected ADRs** | ADR-011, ADR-015, Stage E Spec §3 |
| **Code evidence** | `app.py:753-807` — `_progress_auth_admitted_commands()` never calls `_refresh_auth_catalog_status()` or checks `auth_catalog_ready` |
| **Documentation** | Stage E Spec §3: "all newly authenticated admission remains unavailable until v2 is ready"; `app.py:916-924` shows the login path does check readiness |

**Problem:** Unlike the login path (`app.py:918`), bearer command admission skips the `auth_catalog_ready` check. A v1 or `FINGERPRINT_MISMATCH` auth state can admit bearer commands through `_progress_auth_admitted_commands`:
- Auth state is v1 (schema upgrade pending): bearer command still goes through credential validation against v1 state.
- Auth state has fingerprint mismatch: bearer command still goes through.

This contradicts the Stage E spec requirement that "all newly authenticated admission remains unavailable until [auth] schema v2 is ready."

**Concrete failure scenario:**
1. Leader has auth in `UPGRADE_PENDING` state (v1).
2. Client sends `publish_deck` with valid v1 bearer.
3. `_progress_auth_admitted_commands` checks `active_credentials` against v1 state — finds the credential, stamps actor, proposes command.
4. Command applies successfully even though the spec says it should be blocked pending v2 readiness.

**Impact:** Security — auth schema upgrade guarantees are bypassed.

**Recommended resolution:** After line 773 (`ReadIndexStatus.READY`), add `self._refresh_auth_catalog_status()` and check `self.auth_catalog_ready`. If not ready, reply `{"ok": False, "error": "AUTH_UNAVAILABLE"}` (matching login path behavior). Add regression tests verifying that `UPGRADE_PENDING` and `FINGERPRINT_MISMATCH` states block bearer admission.

**Implementation dependency:** None — standalone fix. Should be applied before Stage F to keep the admission contract correct.

**Confidence:** High

---

### F-005 — Legacy v2 deck interim authorization policy undefined [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Ambiguous/incomplete requirement |
| **Affected ADRs** | ADR-012 §3 (immutable ownership), ADR-014 §1 (role resolution), ADR-018 §4 (quarantine) |
| **Code evidence** | `state_machine.py:219-250` — `_authorize_external_mutation()` checks only credential validity, not ownership |
| **Documentation** | ADR-018 §4: "ownerless v2 deck is quarantined"; ADR-014: NONE cannot read/mutate |

**Problem:** ADR-018 says v2 decks are "quarantined" but doesn't define concrete authorization behavior in terms of the current code:
- `_authorize_external_mutation()` currently does only Stage E credential checks (no deck role). It will continue to **allow** edits on v2 decks even after Stage F adds EDIT checks, because there's no `owner_user_id` to compare against.
- If `effective_deck_role()` from ADR-014 returns `NONE` for v2 decks (no owner, no grants), then ALL existing v2 decks become unreadable and uneditable without warning.
- ADR-018's quarantine means "preserve ownerless decks but allow no mutation" — but the actual code path for "no owner" hasn't been designed.

**Concrete failure scenario:**
1. After Stage F deploys, every pre-existing v2 deck has `metadata` with no `owner_user_id` (it doesn't exist in the schema).
2. ADR-014's `effective_deck_role()` resolves `owner_user_id` from metadata — it's absent — so role is `NONE`.
3. All existing v2 decks become completely inaccessible. Owners cannot read their own content.
4. Users cannot edit, cannot migrate (migration not implemented), cannot even see their decks.

**Impact:** Compatibility, availability — backward-incompatible silently breaks all existing data.

**Recommended resolution:**
- Add an explicit `LEGACY_UNOWNED` designation for v2 decks with no `owner_user_id`.
- Define interim behavior: under `LEGACY_UNOWNED`, mutations continue under Stage E rules (authenticated-only, no deck ownership check). Protected reads eventually require authenticated access.
- The `query_grants` response for `LEGACY_UNOWNED` returns `NONE` with an optional `legacy: true` indicator (no deck content disclosure via this alone).
- Migration to v3 moves the deck out of `LEGACY_UNOWNED`.

**Implementation dependency:** Requires amending ADR-014 and ADR-018 with clear legacy policy. Blocks any Stage F authorization logic deployment.

**Confidence:** High

---

### F-006 — `_authorize_external_mutation` does only credential checks [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap |
| **Affected ADRs** | ADR-014 §2 (EDIT matrix), ADR-014 §6 (guard precedence) |
| **Code evidence** | `state_machine.py:219-250` — `_authorize_external_mutation()` checks `active_credentials` against `client_id`, returns `UNAUTHENTICATED` or `None`; no deck role check |
| **Documentation** | ADR-014 §6: "deterministic FSM rechecks the current credential first, then the required deck role before existing lock, version, and structural preconditions" |

**Problem:** Every mutation handler (`_create_slide`, `_delete_slide`, `_update_element`, `_acquire_lock`, `_update_deck_settings`, `_renew_lock`, etc.) calls `_authorize_external_mutation(command, payload, committed_index)` first. This checks only that the credential is valid — it does **not** verify that the caller has `EDIT` or `OWNER` permission on the target deck. Adding such a check requires:
1. A new method that resolves `effective_deck_role()` from the current deck state.
2. A deck-id resolution path accessible from each handler (currently `_structural_deck(command)` or `command.deck_id`).
3. The role check ordered **after** credential validation but **before** lock/version/settings checks (per ADR-014 §6).

**Impact:** Security — without this, Stage F authorization at apply time is entirely missing.

**Recommended resolution:** Add `_check_deck_permission(command, deck_id, required_role, committed_index)` to `KeyValueStateMachine`. Call it from each handler after credential validation. The method resolves `deck["owner_user_id"]` and `deck["access"]["grants"]` to determine role.

**Implementation dependency:** F-003 must be resolved first (ownership storage location must be settled before the check can look it up). F-005 must be resolved (legacy policy determines behavior when ownership is absent).

**Confidence:** High

---

### F-007 — Protected presentation reads are completely unauthenticated [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Existing defect / Implementation gap |
| **Affected ADRs** | ADR-014 §3 (read matrix), ADR-015 D2 (leader-bound reads), ADR-017 §2 (protected read surface) |
| **Code evidence** | `app.py:1202-1221` — `_presentation_read()` reads from local FSM without bearer check, no leader routing; `app.py:1223-1234` — `_authoritative_element_refresh()` starts ReadIndex but no bearer check |
| **Documentation** | ADR-017 §2: all six read operations must use fresh leader-owned ReadIndex and authorization |

**Problem:** The current read path is the **direct opposite** of what ADR-017 requires:
- `_presentation_read()` (lines 1202-1221) serves from whatever replica was contacted — follower or leader. No bearer is extracted, no auth check, no ReadIndex.
- `_authoritative_element_refresh()` (lines 1223+) starts a ReadIndex round but does not extract or validate a bearer token, does not check deck authorization.
- `_reply_authoritative_refreshes()` (not shown) calls `get_element()` after ReadIndex completes but has no bearer/role check.
- `poll_if_due()` targets `local_replica` (line 935), which would be a follower.

**Concrete failure scenario:**
1. Sofia publishes deck D with Sofia as owner (v3).
2. Santiago has no grant for deck D.
3. Santiago sends `get_deck_preview` to follower node B (which is the `local_replica` for Santiago's client).
4. `_presentation_read()` returns full deck metadata, slide IDs, deck version — no bearer required, no authorization.
5. Owner-private guarantee is violated.

**Impact:** Security — the core privacy promise of ADR-014/017 is completely unimplemented.

**Recommended resolution:** Implement the ADR-017 shared guarded-read pipeline:
1. Re-route all six protected read operations to a new handler that requires bearer and ReadIndex.
2. Add `AuthAdmittedReadWaiter` (or reuse pattern from `AuthAdmittedCommandWaiter`).
3. Check `auth_catalog_ready` before processing reads.
4. Implement `effective_deck_role()` and enforce VIEW+ requirement.
5. Mask inaccessible/nonexistent deck as `ACCESS_DENIED`.

**Implementation dependency:** F-004 (readiness gate), F-005 (legacy policy), F-006 (role resolution), F-003 (ownership location). This is the most code-intensive change in Stage F.

**Confidence:** High

---

### F-008 — Multi-deck outbox requires fundamental refactoring [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap / Integration risk |
| **Affected ADRs** | ADR-016 §2-3 (per-deck outbox), ADR-016 §7 Q2-A (one per deck) |
| **Code evidence** | `client_journal.py:171-195` — `ClientIntentionOutbox.__init__` takes single `logical_client_id`, single `journal_path`, single `session_id`; `client_journal.py:879-981` — `JournaledClient` owns one outbox; `poll_if_due()` uses `local_replica` (line 935) |
| **Documentation** | ADR-016 §3: "One durable outbox per deck; multiple outboxes share AuthenticatedClient transport" |

**Problem:** The entire `ClientIntentionOutbox` and `JournaledClient` architecture assumes one outbox per authenticated client:
- Single `journal_path` for all intentions
- Single `_base_projection` (line 187)
- Single `_highest_applied_index` (line 188)
- Single `_decks`, `_slides`, `_elements` index (lines 819-866) built from one projection
- `OPERATION_POLICIES` is a flat dict with no per-deck namespace
- `poll()` (line 286) takes `deck_id` but stores the projection globally; polling multiple decks would overwrite

ADR-016 requires **one outbox per `(user_id, deck_id)` pair** sharing only the `AuthenticatedClient` transport.

**Concrete failure scenario:**
1. Sofia works on decks D1 and D2.
2. She polls D1 → `_base_projection` gets D1's data.
3. She enqueues an edit on D2 → `_validate_local_scope` checks `scope.deck_id` against `_decks` — D2 is not in `_decks` because projection is from D1 → `LocalIntentionRejected`.
4. Cross-deck work is impossible with a single outbox.

**Impact:** Maintainability, compatibility — every multi-deck user is broken; ADR-016's design requires a new outbox factory and per-deck journal lifecycle.

**Recommended resolution:**
1. Create a `DeckOutbox` or `DeckJournal` class bound to exactly one `(user_id, deck_id)`.
2. Create a `MultiDeckJournaledClient` or `SessionManager` that owns a mapping of `deck_id → DeckOutbox`.
3. Each outbox has its own `journal_path`, `_base_projection`, `_highest_applied_index`, and gate.
4. `AuthenticatedClient.call()` provides shared transport.

**Implementation dependency:** This is a large refactoring of the client journal. Can proceed in parallel with server-side changes but must be completed before any client tests exercise multiple decks.

**Confidence:** High

---

### F-009 — `publish_deck` existence disclosure before ownership check [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Security / privacy |
| **Affected ADRs** | ADR-012 §3 (ownership validation), ADR-014 §6 (guard precedence) |
| **Code evidence** | `state_machine.py:179-185` — `DECK_ALREADY_PUBLISHED` returned before any ownership check |
| **Documentation** | ADR-014 §6: "for a missing or inaccessible deck, do not reveal existence" |

**Problem:** `publish_deck` at apply time checks `deck_id in self._decks` (line 180) and returns `DECK_ALREADY_PUBLISHED` with current `deck_version` **before** any credential or ownership check. This allows a non-owner to probe deck IDs and learn existence, version, and (in the current code) not create the deck. ADR-014 §6 requires that missing/inaccessible deck responses not disclose existence.

**Concrete failure scenario:**
1. Santiago knows (or guesses) deck_id for Sofia's deck D.
2. Santiago publishes a fake snapshot claiming D's deck_id.
3. Response: `DECK_ALREADY_PUBLISHED` with `deck_version` — confirms D exists and its revision.
4. Even if Santiago's credential fails later, existence was already leaked.

**Impact:** Security — deck-ID probing reveals existence and version before authentication/authorization.

**Recommended resolution:** Reorder the apply-time checks: credential validation → ownership check → deck existence check. Since `DECK_ALREADY_PUBLISHED` is a structural precondition that could leak existence to an unauthenticated caller, the ordering should be:
1. `_authorize_external_mutation` (credential check)
2. `authenticated_actor_user_id == owner_user_id` (or mismatched owner)
3. `deck_id in self._decks` (existence-dependent result)

If a valid credential submits a publication for an existing deck, the response becomes `OWNER_MISMATCH` (or `ACCESS_DENIED`) rather than `DECK_ALREADY_PUBLISHED` only after authentication passes.

**Implementation dependency:** F-006 (role check in apply) provides the foundation.

**Confidence:** High

---

### F-010 — ReadIndex completion pipeline does not exist for content reads [HIGH]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap |
| **Affected ADRs** | ADR-015 D2, ADR-017 §3 |
| **Code evidence** | `app.py:1223-1234` — `_authoritative_element_refresh()` calls `begin_read_index` but no bearer/auth/role check; `_reply_authoritative_refreshes` (lines 1130-1190) completes ReadIndex but no auth |
| **Documentation** | ADR-017 §3: "dispatch/leader gate → volatile bearer extraction → quorum barrier → authoritative completion → non-yielding authorization+projection" |

**Problem:** The existing `_authoritative_element_refresh` / `_reply_authoritative_refreshes` pattern shows the infrastructure for ReadIndex waiters, but:
- No bearer is extracted or validated.
- No deck permission check is performed after ReadIndex READY.
- The waiters have no concept of `effective_access`.
- No `AuthAdmittedReadWaiter` data class exists (only `AuthoritativeRefreshWaiter` and `AuthAdmittedCommandWaiter`).

For the six protected reads plus `query_grants`, `query_operation_receipt`, `query_access_state`, and `resolve_registered_user`, a new generalized ReadIndex waiter family is needed.

**Impact:** Security — the entire protected read pipeline must be built from scratch.

**Recommended resolution:** Create a new `AuthAdmittedReadWaiter` data class (modeled on `AuthAdmittedCommandWaiter` but read-only). Build `_start_auth_admitted_read()` and `_progress_auth_admitted_reads()` methods. All seven read operations route through this single pipeline.

**Implementation dependency:** F-004 (readiness gate), F-007 (bearer on reads), F-006 (role check). This is the central implementation artifact for Stage F protected reads.

**Confidence:** High

---

### F-011 — `resolve_registered_user` and email-index access design gaps [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Ambiguous/incomplete requirement |
| **Affected ADRs** | ADR-018 §5 (Q3) |
| **Code evidence** | `auth_dto.py:264` — `email_index` is available in `AuthStateBaseDTO`; `app.py:380-401` — `_refresh_auth_catalog_status` has access to `validate_auth_state` |
| **Documentation** | ADR-018 §5: "resolve_registered_user returns user_id, first_name, last_name, email; only for verified OWNER on an existing v3 deck" |

**Problem:** The resolution operation requires:
1. A new public operation (`resolve_registered_user`) routed through the leader/ReadIndex pipeline.
2. Authorization requiring `OWNER` on an existing deck.
3. Lookup in the replicated auth state's `email_index` — but this state lives at `self.engine.state.fsm["auth"]`, not in the state machine's internal model (`self._auth` in `KeyValueStateMachine`).
4. Masked `ACCESS_DENIED` for nonexistent or inaccessible decks.

The design is viable, but several details are unspecified:
- Does `email_index` normalization (strip/lowercase) match the pre-provisioned catalog's email normalization already done in `AccountCatalogDTO`?
- What is the exact route name and request shape?
- Can this be a ReadIndex-only operation (no Raft write), or does it need to be proposed? (ADR-018 says ReadIndex, but the `email_index` is part of the auth state, which is accessible from the FSM snapshot.)

**Impact:** Maintainability — missing implementation details will cause ambiguity for implementation agents.

**Recommended resolution:** Specify the exact route (`query_resolve_user` or `resolve_registered_user`), request schema (`{deck_id, email}`), response shape for each case, and normalization rules. This is a small spec addition.

**Implementation dependency:** F-007 (protected read pipeline) provides the ReadIndex infrastructure; F-006 (role check) enables OWNER verification.

**Confidence:** Medium

---

### F-012 — `BEARER_OPTIONAL_OPERATIONS` unused; comment count mismatch [LOW]

| Field | Value |
|-------|-------|
| **Classification** | Existing defect / code rot |
| **Code evidence** | `command_dto.py:360` — `BEARER_OPTIONAL_OPERATIONS` defined; `app.py:283` comment says "12 guarded operations" but `len(BEARER_GUARDED_OPERATIONS) == 13`; `app.py:40` imports `BEARER_OPTIONAL_OPERATIONS` but never uses it |

**Impact:** Maintainability — dead code and misleading comment will confuse implementation agents.

**Recommended resolution:** Remove `BEARER_OPTIONAL_OPERATIONS` and its import. Fix comment to say "13 guarded operations" (or future-proof with count reference).

**Confidence:** High

---

### F-013 — `spec.py` has no Stage F models, no grants, no v3 decks [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap / Test gap |
| **Code evidence** | `spec.py:1-795` — no `grant_access`, `revoke_access`, `query_grants`, `query_operation_receipt`, `resolve_registered_user`, `query_access_state`; no v3 deck creation; `GUARANTEE` marks reads as `eventual` |
| **Documentation** | ADR-017 §7: "change four reads from eventual to linearizable" |

**Problem:** The reference model (`spec.py`) is the bedrock for deterministic testing. Without Stage F operations modeled, no deterministic tests can verify the grant lifecycle, ownership checks, or protected read behavior. The `GUARANTEE` dict at line 12 still marks `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status` as `eventual`, directly contradicting ADR-017's requirement that they become `linearizable`.

**Impact:** Test gap — no deterministic verification possible for Stage F until `spec.py` is updated.

**Recommended resolution:** Add to `spec.py`:
1. `grant_access` and `revoke_access` operations with ADR-013's state transitions.
2. `query_grants` returning self-role.
3. v3 deck creation with `owner_user_id`.
4. Update `GUARANTEE` for protected reads.
5. `update_deck_settings` on v3 decks (preserves owner).
6. Update `PRESENTATION_MUTATIONS` and `PRESENTATION_READS` sets.
7. Ownership-check enforcement on mutations.

**Implementation dependency:** Should precede or accompany server-side implementation.

**Confidence:** High

---

### F-014 — `refresh_element` alias is an unimplemented bypass risk [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap |
| **Code evidence** | `app.py:269-270` — `refresh_element` directly calls `_authoritative_element_refresh`; `app.py:1223-1234` — `_authoritative_element_refresh` has no bearer/auth check |
| **Documentation** | ADR-014 §3: "refresh_element: Identical VIEW and freshness requirements; no alternate bypass"; ADR-017 §5: "never forward the alias to old unauthenticated behavior" |

**Problem:** The `refresh_element` alias at line 269 routes to `_authoritative_element_refresh` — the same unauthenticated ReadIndex path. Any Stage F protected-read implementation must ensure both names route through the new auth pipeline, and no future reader accidentally adds a bypass.

**Impact:** Security — alias could be missed during implementation.

**Recommended resolution:** Both names must route through the new `_start_auth_admitted_read` pipeline. Add a test verifying `refresh_element` and `authoritative_refresh_element` use identical authorization.

**Confidence:** High

---

### F-015 — `expire_lock` internal command vs EDIT permission conflict [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap |
| **Affected ADRs** | ADR-018 §2 (F07), ADR-001 (locks) |
| **Code evidence** | `state_machine.py:162-163` — `expire_lock` routed directly to `_expire_lock` without `_authorize_external_mutation` |
| **Documentation** | ADR-018 §2 item 3: "internal expire_lock remains an independently fenced maintenance command; it is not a participant permission operation" |

**Problem:** `expire_lock` currently skips `_authorize_external_mutation` entirely (line 162: `if command.operation == "expire_lock": return self._expire_lock(command)`). This is correct per ADR-018 — internal lock expiration must remain independent of participant permissions so a stale lock can be cleaned up even after the editor is revoked. However, there is no explicit test that `expire_lock` succeeds after EDIT→VIEW downgrade or full revocation.

**Impact:** Correctness risk — no regression guard that future changes don't accidentally add permission checks to `expire_lock`.

**Recommended resolution:** Add a deterministic test verifying `expire_lock` works after EDIT→VIEW downgrade and after full revocation.

**Confidence:** Medium

---

### F-016 — `ENFORCED_FROM_INDEX` boundary for v2 publication stamps [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Integration risk |
| **Code evidence** | `state_machine.py:228-249` — `enforced_from_index` controls whether actorless commands are rejected; `auth_dto.py:263` — `enforced_from_index` field |
| **Documentation** | ADR-018 §4.1 item 7: "Replays of old logs may encounter v2 publications before Stage E enforcement; those remain v2 and are not retroactively treated as authenticated" |

**Problem:** Stage E was deployed mid-history. Some historical v2 `publish_deck` entries have server-stamped actor fields (post-enforcement), some don't (pre-enforcement). ADR-018 requires that only "Stage E-authenticated, committed v2 publication with validated provenance" can migrate. But the `enforced_from_index` boundary means some publications have stamps and some don't — the code must distinguish these, and the provenance bridge (F-002) must work with partial history.

This is not a contradiction but an implementation constraint that must be reflected in migration tests.

**Impact:** Correctness — migration will fail silently if it doesn't account for the enforcement boundary.

**Recommended resolution:** In the migration command's provenance verification, check both the original publication's `committed_index` and whether actor stamps were enforced at that index. Pre-enforcement publications cannot have trusted stamps → cannot migrate.

**Confidence:** Medium

---

### F-017 — GLOSSARY.md missing Stage F terms [LOW]

| Field | Value |
|-------|-------|
| **Classification** | Optional improvement |
| **Code evidence** | `GLOSSARY.md` — has `User Account`, `Authenticated Actor`, `Access Credential`, `Stage E`, `Stage F` |
| **Documentation** | ADR-012 through ADR-018 introduce: Deck Owner, Owner-Private, Access Grant, Collaborator Role, Access Version, Permission Observation, Deck Sync Gate |

**Recommended resolution:** Add definitions for: `Deck Owner`, `Owner-Private`, `Access Grant` (VIEW/EDIT), `Access Version`, `deck_access_state`, `query_grants`, `Permission Observation`, `Deck Sync Gate`, `LEGACY_UNOWNED`.

**Confidence:** High

---

### F-018 — `poll_if_due()` targets `local_replica` but ADR-017 requires leader [MEDIUM]

| Field | Value |
|-------|-------|
| **Classification** | Implementation gap |
| **Code evidence** | `client_journal.py:932-938` — `poll_if_due` passes `self._local_replica` to `outbox.poll` |
| **Documentation** | ADR-017 §7: "have JournaledClient.poll_if_due() use the authenticated leader transport path" |

**Problem:** `poll_if_due()` targets `local_replica` — a follower — for all deck observation reads. ADR-017 requires leader-bound ReadIndex for all protected reads. The poll function must be rewritten to target the leader, handle redirect/fail-closed behavior, and accept that a leader-unavailable condition prevents polling entirely.

**Impact:** Compatibility — Stage F breaks the poll contract by requiring leader routing for reads that were follower-local.

**Recommended resolution:** `poll_if_due()` must use `_leader_replica` for protected deck reads. The `_local_replica` becomes irrelevant for shared-deck observation.

**Confidence:** High

---

### F-019 — Stage A `get` operation isolation from deck state [LOW]

| Field | Value |
|-------|-------|
| **Classification** | Test gap |
| **Code evidence** | `spec.py:37` — `READS = {"get", ...}` |
| **Documentation** | ADR-014 §5: "get remains a local/eventual generic KV read and must not be repurposed to expose protected deck state" |

**Problem:** The Stage A `get` operation reads from `_kv` (generic KV store), while decks are stored in `_decks`. Currently they are separate namespaces, but no test verifies that a deck read through `get` (by some accident or future change) exposes protected data. A regression test is cheap.

**Recommended resolution:** Add a test: after publishing a v3 deck, `get("decks/<deck_id>")` returns nothing (KV is separate). This verifies the isolation ADR-014 §5 requires.

**Confidence:** Medium

---

## 5. Security and Correctness Invariants

| Invariant | Stage F Source | Existing Test | Status |
|-----------|---------------|---------------|--------|
| I-01: Owner immutability | ADR-012 §3, ADR-013 I-01 | None | Missing |
| I-02: Participant uniqueness | ADR-013 I-02 | None | Missing |
| I-03: Default deny | ADR-013 I-03 | None | Missing |
| I-04: Exclusive sharing authority | ADR-013 I-04, ADR-014 F03-I02 | None | Missing |
| I-05: Effective-change revision | ADR-013 I-05 | None | Missing |
| I-06: Independent revisions | ADR-013 I-06, ADR-014 F03-I08 | None | Missing |
| I-07: Deterministic serialization | ADR-013 I-07 | Stage A–E tests | Coverage exists for existing ops |
| I-08: Replay safety | ADR-013 I-08 | Stage E dedup tests | Existing; needs extension to grant ops |
| I-09: No stale resurrection | ADR-013 I-09 | None | Missing |
| I-10: No cross-deck effect | ADR-013 I-10 | None | Missing |
| I-11: No unauthorized disclosure | ADR-013 I-11 | None | Missing |
| F03-I01: Default deny | ADR-014 | None | Missing |
| F03-I05: No lock bypass | ADR-014 | None | Missing |
| F03-I07: No foreign-result leakage | ADR-014 | None | Missing |
| F03-I09: Log-order revocation | ADR-014 | None | Missing |
| F03-I10: Internal paths distinguished | ADR-014 | Existing for expire_lock | Partial |
| LEADER-READ: Quorum failure fails closed | ADR-017 §3.6 | `test_read_index.py` | Exists for element refresh; needs extension |
| MASK-DECK: Inaccessible = nonexistent | ADR-017 §5 (Q1) | None | Missing |
| RECEIPT-PRIVACY: No deck content in receipt | ADR-015 D6-37 | None | Missing |
| V2-QUARANTINE: No guessed ownership | ADR-018 §4 | None | Missing |

### Critical missing tests

1. Deterministic test of every ADR-014 role/operation combination (4 roles × ~17 operations).
2. Race-condition test: `update_element` vs `revoke_access` in both orderings, deterministic.
3. Replay test: retain v2 publish entry, upgrade code to v2/v3 union, verify replay succeeds.
4. Provenance-migration test: correctly- and incorrectly-stamped v2 publication → migration.
5. Read-privacy test: non-owner requests each protected read → `ACCESS_DENIED`, no content.
6. Receipt privacy test: revoked editor queries receipt → `COMMITTED_SUCCESS`/`REJECTED`, no content.
7. Multi-deck outbox isolation test.
8. `auth_catalog_ready` gate for bearer admission (regression for F-004).

---

## 6. Failure-Scenario Analysis

### Scenario 1: Revocation race with in-flight edit

```
Raft log order:
  index 100: update_element(D, element=E1, actor=Santiago)   [EDIT grant holder]
  index 101: revoke_access(D, target=Santiago, actor=Sofia)   [owner removes EDIT]
```

**Expected (per ADR-015 D1):**
- Index 100 applies: succeeds (Santiago had valid EDIT at apply time). Element E1 is updated.
- Index 101 applies: Santiago's grant is removed. No rollback of index 100.
- Future commands from Santiago on deck D → `ACCESS_DENIED`.

**What the current code does:** Index 100 would succeed if credential check passes. Index 101 cannot be proposed because `revoke_access` doesn't exist yet. Post-Stage-F, the check at `revoke_access` apply time must verify Sofia is owner, then remove Santiago's grant.

**Gap:** If `revoke_access` is admitted at index 100 (before `update_element` at index 102), the ordering reverses. The server must admit both, commit both, and let Raft order determine the outcome. ADR-015's decision (no cancellation) means Santiago's edit may succeed even though revocation was admitted first. This is correct behavior for the chosen architecture but must be explicitly communicated in API documentation and demonstrated in tests.

**Test needed:** `test_revocation_race_edit_first` and `test_revocation_race_revoke_first` — deterministic ordered apply.

### Scenario 2: Stale follower after committed revocation

```
1. Leader commits revoke_access(D, Santiago) at log index 150.
2. Follower B is isolated (partitioned) and still has Santiago with EDIT grant.
3. Santiago sends get_deck_preview to Follower B (direct request).
```

**Expected (per ADR-017 §3.1, D2-8):**
- Non-leader redirects to leader or returns `NO_LEADER`.
- Follower B must NOT serve content from its local FSM.
- Santiago's preview request fails with redirect/unavailable, not with `ACCESS_DENIED` from follower.

**What the current code does:** `_presentation_read()` serves from local FSM with no routing check. Santiago gets full deck content from the stale follower. **This is a critical security failure** that ADR-017 is explicitly designed to fix.

**Gap:** The entire protected-read pipeline (bearer extraction, leader routing, ReadIndex, authorization) does not exist. Every read is currently vulnerable to stale-follower content disclosure.

**Test needed:** Integration test with an isolated follower: apply revocation on leader, then query follower → no content.

### Scenario 3: Multi-client, same account, one revoke

```
1. Sofia has two authenticated clients: C1 and C2, both bound to Sofia's account.
2. Sofia grants Santiago EDIT on deck D.
3. Santiago uses client C1 to acquire_lock(element=E1).
4. Sofia revokes Santiago's grant via revoke_access from client C2.
5. Santiago (C1) attempts update_element(E1) — now revoked.
```

**Expected (per ADR-014 F03-I09, ADR-015 D1):**
- Step 5: `update_element` → `ACCESS_DENIED` at apply time.
- The lock on E1 remains held by Santiago's session but expires normally under Stage C.
- Santiago cannot renew the lock (renewal requires EDIT, which is gone).

**Gap:** `LockExpirationCoordinator` and `RenewalWorker` behavior after revocation:
- The client-side `RenewalWorker` must stop when permission is observed as VIEW/NONE.
- But the worker checks `LockMaintenanceService`, not deck permission.
- There's no current bridge between permission observation and renewal-worker lifecycle.

**Risk:** A revoked-but-cached client may continue to renew locks for up to the lease window, holding locks that the owner cannot acquire. ADR-018 §2.5 says "stop renewal" but the mechanism is unspecified.

### Scenario 4: Pre-Stage-E v2 publication migration attempt

```
1. Cluster has a v2 deck D published at log index 30 (Stage B, no actor stamps).
2. After Stage E deployment, D has no server-stamped actor in its publish entry.
3. A migration command attempts to migrate D to v3 with claimed owner "Sofia."
```

**Expected (per ADR-018 §4.1):**
- Migration verification finds no authenticated provenance.
- Deck D remains v2, quarantined.
- No ownership assigned. No content disclosed.

**Gap:** The provenance-verification mechanism (F-002) doesn't exist. Without it:
- Option A: migration trusts the caller-supplied owner → insecure (impersonation).
- Option B: migration fails to find provenance and rejects → safe but blocks all pre-Stage-E decks.
- Option C: migration doesn't distinguish provenance quality → may migrate some with valid stamps (post-enforcement) and some without.

**Risk:** Without the bridge, migration is either insecure or completely blocked for all historical decks. ADR-018 acknowledges this as a gap.

---

## 7. Implementation Dependency Graph

```
F-004 (bearer readiness gate) ── standalone server fix, no dependencies
        │
        ▼
F-001 (v2/v3 DTO union) ────── separate refactoring, no runtime deps
        │
        ▼
F-003 (ownership storage) ──── depends on F-001 (union shape settled)
        │
        ▼
F-005 (legacy policy) ──────── depends on F-003 (ownership location decided)
        │
        ▼
F-006 (deck role check) ──���─── depends on F-003, F-005
        │
        ▼
F-009 (publish existence) ──── depends on F-006
        │
        ▼
F-010 (protected read pipeline) ── depends on F-006, F-004
        │
        ├── F-007 (read auth) ──────── depends on F-010
        ├── F-014 (refresh alias) ──── depends on F-010
        ├── F-011 (resolve_user) ───── depends on F-010, F-006
        ├── F-012 (BEARER_OPTIONAL) ── standalone cleanup
        ├── F-013 (spec.py update) ─── depends on F-001, F-003, F-006
        ├── F-017 (GLOSSARY) ───────── standalone doc fix
        │
        ▼
F-008 (multi-deck outbox) ──── independent client-side refactoring
        │
        ▼
F-018 (poll targets leader) ── depends on F-008, F-010
        │
        ▼
F-002 (migration bridge) ───── depends on F-001, F-006, F-013
        │
        ▼
(v2→v3 migration command) ──── depends on F-002, F-005, F-013
```

### Parallelizable work streams

1. **Stream A — Server authorization (sequential):** F-004 → F-001 → F-003 → F-005 → F-006 → F-009 → F-010 → F-007 → F-014
2. **Stream B — Client journal (semi-independent):** F-008 → F-018 (requires Stream A for the read pipeline)
3. **Stream C — Migration (sequential, needs Stream A):** F-002 → migration command
4. **Stream D — Spec/model/test updates (parallel to A):** F-013, F-015, F-016, F-017, F-012
5. **Stream E — Remaining operations (needs A):** F-011, `query_operation_receipt`, `query_access_state`

---

## 8. Missing Tests and Validation Plan

| Scenario | Required Guarantee | Existing Coverage | Missing Verification | Priority |
|----------|-------------------|-------------------|---------------------|----------|
| Every operation × 4 roles | ADR-014 matrix | None | Deterministic tests for all ~68 combinations | P0 |
| v2 replay with v3 DTO | Log replay compatibility | None | Retain v2 publish entry, verify apply succeeds after DTO change | P0 |
| Bearer admission blocked before v2 ready | Spec §3, F-004 | None | Regression test in auth_catalog tests | P0 |
| Revocation race (edit first) | ADR-015 D1 | None | Deterministic ordered apply | P1 |
| Revocation race (revoke first) | ADR-015 D1 | None | Deterministic ordered apply | P1 |
| Protected read: stale follower | ADR-017 D2-8 | None | Integration with partition | P1 |
| Protected read: inaccessible = nonexistent | ADR-017 Q1 | None | Both cases return identical `ACCESS_DENIED` | P1 |
| Migration: valid provenance | ADR-018 §4 | None | v2 with authenticated stamps → v3 | P1 |
| Migration: no provenance | ADR-018 §4 | None | Pre-enforcement v2 → quarantined | P1 |
| Migration: wrong actor | ADR-018 §4 | None | Different user claims ownership → denied | P1 |
| Receipt: same client after revocation | ADR-015 D6 | None | Revoked editor gets receipt but no content | P1 |
| Receipt: different client on same record | ADR-015 D6 | None | Other client gets `OUTCOME_UNKNOWN` | P2 |
| Grant: OWNER-only enforcement | ADR-013 I-04 | None | EDIT cannot grant/revoke | P1 |
| Grant: version conflict | ADR-013 §3-13 | None | Stale `expected_access_version` → `ACCESS_VERSION_CONFLICT` | P1 |
| Grant: no-op no revision increment | ADR-013 I-05 | None | `ACCESS_UNCHANGED` and `ACCESS_NOT_FOUND` leave version | P1 |
| Lock: expire after revoke | ADR-018 §2 | None | `expire_lock` succeeds after EDIT→VIEW | P2 |
| Lock: renew after revoke | ADR-018 §2 | None | `renew_lock` → `ACCESS_DENIED` | P2 |
| Multi-deck outbox isolation | ADR-016 §7 | None | Two outboxes, independent gates | P2 |
| Update settings preserves owner | ADR-012 §3 | None | Settings mutation on v3: `owner_user_id` unchanged | P2 |
| Legacy v2 edit permitted | F-005 | None | `LEGACY_UNOWNED` v2 allows Stage E-style auth-only edit | P2 |
| Auth_catalog_ready gate on bearer admission | (none in code) | None | Regression: `FINGERPRINT_MISMATCH` and `UPGRADE_PENDING` block admission | P0 |

---

## 9. Open Architectural Questions

### Q-A: How does `KeyValueStateMachine.apply()` access publication provenance for migration verification?

| Field | Value |
|-------|-------|
| **Why unresolved** | `apply()` receives only `(command, committed_index)`. The provenance (original server-stamped actor, `credential_id`, original publication's log index) lives in the Raft log entry at the original publication index, which `apply()` cannot access. |
| **Conflicting evidence** | ADR-018 §4.1-6: "implementation agents must explicitly bridge this gap... rather than blindly trusting proof fields supplied in the migration payload." The gap is identified but no bridge is specified. |
| **Viable alternatives** | **(A)** Store the publisher's `authenticated_actor_user_id` as a persistent field in the deck record at publication time. This makes it available for future migration commands to read from current state. Requires adding `publisher_user_id` to the deck structure created at lines 197-212 of `state_machine.py`. **(B)** Change the migration command to carry the original publication's critical fields, and validate them in `validate_committed_command()` against a new facility that can read `request_results` — but this requires architectural changes to the command validation pipeline. **(C)** Use the migration command's `original_publish_index` to look up the Raft log entry during `apply()` — but `apply()` has no reference to the Raft log at all. |
| **Recommendation** | **Option A** is the smallest, safest change. Add `publisher_user_id` (and optionally `publisher_credential_generation`) to the deck record at create time. For new v3 publications this is the authenticated actor. For existing v2 decks, this field is absent. A migration command can then verify that the target deck's stored `publisher_user_id` matches the claimed owner. Legacy decks (pre-enforcement) have no publisher → cannot migrate. This adds ~5 lines to `state_machine.py`. |
| **Blocks implementation?** | **Yes** — migration (ADR-018 §4) cannot be implemented without resolving this. |

### Q-B: Should `owner_user_id` live in `metadata` or at the deck root?

| Field | Value |
|-------|-------|
| **Why unresolved** | ADR-012's normative shape places it in `PresentationMetadataDTO`. But `metadata` is mutable through `_update_deck_settings`, and `get_deck_preview` returns `deepcopy(deck["metadata"])` without authorization. |
| **Viable alternatives** | **(A)** Keep it in metadata but add special-case preservation in `_update_deck_settings` and `get_deck_preview` authorization. Risk: future settings operations may forget. **(B)** Store at deck root (`deck["owner_user_id"]`). Resolve role from deck root, not metadata. Include it in preview response only after authorization. This is cleaner and matches the `deck["access"]` pattern from ADR-013. |
| **Recommendation** | **Option B** — consistent with where ADR-013 stores `access`, and avoids the mutable-metadata coupling. Amend ADR-012's normative shape to show the authoritative owner at deck root. |
| **Blocks implementation?** | **Yes** — F-003 cannot be fixed without this decision. |

---

## 10. Minimum Correction Plan

### Mandatory (Critical/High — resolve before Stage F implementation)

| Order | Task | Finding | Effort |
|-------|------|---------|--------|
| 1 | Fix bearer admission readiness gate (add `auth_catalog_ready` check to `_progress_auth_admitted_commands`) | F-004 | 1-2h |
| 2 | Create v2/v3 discriminated union in `PresentationSnapshotDTO`; add replay test with legacy v2 entry | F-001 | 3-4h |
| 3 | Decide ownership storage location (deck root vs. metadata); amend ADR-012; implement deck-root storage | F-003, Q-B | 4-6h |
| 4 | Define `LEGACY_UNOWNED` policy and implement it in `effective_deck_role()` and mutation/allowed checks | F-005 | 4-6h |
| 5 | Add `_check_deck_permission()` to `KeyValueStateMachine`; call from every mutation handler after credential check | F-006 | 6-8h |
| 6 | Reorder `publish_deck` apply checks: credential → owner → existence | F-009 | 2-3h |
| 7 | Build `AuthAdmittedReadWaiter` and shared ReadIndex pipeline for protected reads | F-010, F-007, F-014 | 16-24h |
| 8 | Resolve migration provenance bridge (add `publisher_user_id` to deck record; implement provenanced migration) | F-002, Q-A | 8-12h |
| 9 | Implement grant DTOs and state-machine handlers for `grant_access` / `revoke_access` | ADR-013 | 8-12h |
| 10 | Implement `query_grants` and `get_deck_preview` with `effective_access` | ADR-015 D3, ADR-017 §4 | 4-6h |
| 11 | Implement `query_operation_receipt` | ADR-015 D6 | 6-8h |
| 12 | Refactor client journal for per-deck outbox, projection, and gate | F-008, ADR-016 | 16-24h |
| 13 | Update `spec.py` with all Stage F operations and linearizable guarantees | F-013 | 8-12h |
| 14 | Write deterministic tests for ADR-014 matrix, revocation races, and receipt privacy | §8 | 16-24h |
| 15 | Remove `BEARER_OPTIONAL_OPERATIONS`, fix comment | F-012 | 0.25h |

### Improvement (Medium — do after mandatory items)

| Task | Finding | Effort |
|------|---------|--------|
| Implement `resolve_registered_user` | F-011 | 4-6h |
| Implement `query_access_state` (owner-only roster read) | ADR-018 §3 | 4-6h |
| Add deterministic tests for lock/revoke lifecycle | F-015, ADR-018 §2 | 4-6h |
| Update GLOSSARY.md with Stage F terms | F-017 | 1h |
| Add Stage F terms to project proposal | ADR-014 | 1h |
| Implement `refresh_element` alias through new read pipeline (verify no bypass) | F-014 | 1h |

---

## 11. Final Implementation-Readiness Verdict

### 1. Should implementation begin now?

**No.** Two critical blockers (F-001, F-002) and five high-severity gaps (F-003 through F-008) mean any implementation agent starting now would produce code that either:
- Cannot coexist with historical log data (F-001)
- Cannot implement migration securely (F-002)
- Breaks on settings mutations (F-003)
- Bypasses auth schema guarantees (F-004)
- Has no defined behavior for all existing data (F-005)
- Cannot authorize any operation (F-006, F-007)

### 2. Exact blockers that must be resolved first

| Blocker | Why | Resolution |
|---------|-----|------------|
| v2/v3 DTO union (F-001) | Without it, the first v3 entry breaks all subsequent log replay | Discriminated union in `PublishDeckPayloadDTO` |
| Provenance bridge (F-002) | Without it, migration is either insecure or unimplementable | Store `publisher_user_id` in deck record at publish time |
| Ownership storage location (F-003) | Without it, settings mutations and previews are broken | Decide deck-root vs. metadata; amend ADR-012 |
| Legacy deck policy (F-005) | Without it, all existing v2 decks break | Define `LEGACY_UNOWNED` with Stage E-style access |

### 3. ADRs that need amendment

| ADR | Amendment |
|-----|-----------|
| **ADR-012** | Normative shape should show `owner_user_id` at deck root (not inside `PresentationMetadataDTO`), with a note that it appears in metadata only at publication-input validation |
| **ADR-012** | Add explicit discriminated-union section for `PresentationSnapshotV2DTO | PresentationSnapshotV3DTO` |
| **ADR-012** or **ADR-018** | Add `LEGACY_UNOWNED` policy definition |
| **ADR-018** | Specify Option A (store `publisher_user_id` at deck root) as the provenance bridge mechanism |

### 4. Smallest necessary specification changes

Before any implementation:
1. A one-page **"Stage F — Authorization Foundation"** spec covering:
   - `effective_deck_role()` function
   - Guard precedence (credential → role → lock/version/existence)
   - `legacy_unowned` policy definition
   - Protected-read pipeline architecture
2. A one-page **"Stage F — DTO and Schema Changes"** spec covering:
   - `PresentationSnapshotV2DTO` and `PresentationSnapshotV3DTO` union
   - `DeckAccessStateDTO`, `DeckAccessGrantDTO`
   - `publisher_user_id` deck-field addition
   - New command DTOs (`GrantAccessPayloadDTO`, `RevokeAccessPayloadDTO`, migration command)

### 5. Proposed implementation-agent sequencing strategy

| Phase | Contents | Parallel streams |
|-------|----------|-----------------|
| **Phase 0 — Foundation** | F-004, F-012, ADR amendments, GLOSSARY update, spec.py update | Doc + server (parallel) |
| **Phase 1 — Server authorization** | F-001, F-003, F-005, F-006, F-009 | Sequential (must build on each other) |
| **Phase 2 — Server reads** | F-010, F-007, F-014, query_grants, get_deck_preview[effective_access] | Sequential after Phase 1 |
| **Phase 3 — Grants** | grant_access, revoke_access, ACCESS_VERSION_CONFLICT, all return statuses | Sequential after Phase 1 |
| **Phase 4 — Client** | F-008, F-018, per-deck gate, receipt adapter, recovery view | Parallel to Phases 2-3 |
| **Phase 5 — Migration** | F-002, provenanced migration command, quarantine enforcement | Sequential after Phase 1 |
| **Phase 6 — Query operations** | resolve_registered_user, query_access_state, query_operation_receipt | Parallel to Phase 4 |
| **Phase 7 — Tests** | All P0-P2 tests from §8 | Parallel to Phases 1-6 |

### 6. Top five risks to track during Stage F

1. **Provenance bridge feasibility:** If `publisher_user_id` at publication time is rejected, the migration mechanism remains undefined — this blocks Phase 5 entirely.
2. **Multi-deck outbox refactoring breadth:** The current `ClientIntentionOutbox` is 980 lines tightly coupled to single-deck assumptions. Refactoring to per-deck journals touches `JournaledClient`, `AuthenticatedClient.journal_sender()`, `poll_if_due()`, all test fixtures, and scenario scripts. This is the largest single code change in Stage F.
3. **Protected read latency regression:** Every read operation currently completes in one event-loop tick (follower-local, zero RPCs). Stage F requires leader ReadIndex (one round trip + local apply). This will increase read latency, especially during leader transitions or quorum unavailability.
4. **Backward compatibility for existing journal files:** ADR-016 requires that existing pre-Stage-F journals that mix multiple decks "fail closed for protected synchronization until an explicit, lossless migration." This means existing test fixtures and scenario archives may need migration. The impact depends on whether existing test journals span multiple decks (the tests at `test_client_journal.py` appear single-deck, but `scenarios/` data may not be).
5. **Deterministic test complexity explosion:** The ADR-014 matrix alone is ~68 operation/role combinations. Adding revocation races, receipt paths, migration scenarios, and quarantine behavior could produce 300+ deterministic test cases. The existing `spec.py` model must be extended comprehensively. Without this, the system cannot claim security correctness.

---

**Report produced 2026-11-26 by independent architecture audit. No source-code changes were made.**