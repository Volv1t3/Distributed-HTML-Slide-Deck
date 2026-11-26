# ADR-019 — Presentation Schema Replay Compatibility and Authentication-Readiness Admission Fence

- **Status:** Proposed for acceptance (corrective Stage F architecture decision; not yet implemented)
- **Date:** 2026-11-26
- **Scope:** Independent audit findings **F-001** (presentation v2/v3 decoding and deterministic replay) and **F-004** (bearer admission without auth schema-v2 readiness)
- **Supersedes/clarifies:** The v3-only *new-publication* interpretation of ADR-012's proposed `PresentationSnapshotDTO` shape. It does **not** change ADR-012's requirement that newly created/published Stage F decks use v3; it preserves ADR-018's conditional migration and fail-closed quarantine of unowned v2 decks.
- **Related:** ADR-005 (v2 presentation contract); ADR-009–011 (accounts and schema-v2 authentication); ADR-012 (v3 ownership); ADR-013–014 (grants and permission enforcement); ADR-017 (leader-bound protected reads); ADR-018 (provenance-gated v2→v3 migration); `docs/specs/Stage-E-Authentication-Implementation-Spec.md`, §6; independent audit `stage-f-prespec-validation.md`, F-001/F-004.
- **Repository baseline:** `Volv1t3/Distributed-HTML-Slide-Deck`, `master` SHA `3ecced2432fdd6afa979eec79c0aa7dc4a566b9a`. The code at that revision has not implemented Stage F.

## 1. Context and defects

### F-001 — A current-schema-only DTO would make old committed publications unapplyable

At baseline, `command_dto.py` declares `PresentationSnapshotDTO.schema_version: Literal[2]` and `PublishDeckPayloadDTO.presentation: PresentationSnapshotDTO`. Both `validate_committed_command()` and the state machine validate publication payloads. `RaftEngine._apply_committed()` calls `KeyValueStateMachine.apply(command, committed_index=entry.index)` without swallowing validation failures. Replacing the only accepted publication DTO with `Literal[3]` would therefore reject a legitimate historical v2 `publish_deck` entry **if that entry still needs to be applied** under Stage F binaries. This affects retained history, follower catch-up with unapplied entries, and reconstruction/replay tests; an ordinary preserved-disk restart generally loads a persisted FSM and `last_applied` and need not reapply the full historical log.

ADR-012 requires v3 **for new Stage F publications**; ADR-018 requires historical v2 preservation, replay, and conditional, provenance-verified migration. These requirements are compatible only if decoding historical commands is distinct from the rule admitting new commands.

### F-004 — Bearer authentication may be admitted while auth-state v2 is unready

`app.py:_progress_login_reads()` refreshes the auth-catalog status after a successful ReadIndex and requires `auth_catalog_ready`. By contrast, `_progress_auth_admitted_commands()` checks token digest/current credential after ReadIndex but currently omits that readiness gate. In an existing auth-state schema-v1 `UPGRADE_PENDING` state, or where the persisted catalog fingerprint mismatches the configured catalog, a bearer mutation may proceed despite the Stage E specification's requirement that **all newly authenticated admission remains unavailable until auth-state v2 is ready**. This is a Stage E defect to correct before Stage F depends on the same authentication boundary.

## 2. Decision A — Keep explicit v2 and v3 publication DTOs

1. Introduce a **frozen historical** `PresentationSnapshotV2DTO` whose `schema_version` is exactly integer `2`, with the Stage D v2 publication metadata, element/slide validation and other original constraints. Do not reinterpret v2 metadata as account ownership or opportunistically insert `owner_user_id` into old commands.
2. Introduce `PresentationSnapshotV3DTO` whose `schema_version` is exactly integer `3`, using ADR-012's required account `owner_user_id: UUID` in its v3 publication metadata, alongside the existing local `owner_session_id`. New v3 validation retains the established typed, bounded slide/element/color and ID-uniqueness rules. Replicated storage continues to strip `owner_session_id` on a successful publication.
3. `PublishDeckPayloadDTO.presentation` must decode **both versions based on the explicit `schema_version` discriminator** (e.g., Pydantic `Annotated[PresentationSnapshotV2DTO | PresentationSnapshotV3DTO, Field(discriminator="schema_version")]`). There must be no silent conversion, default version, compatibility-only owner field, or fallback from malformed v3 to v2. Schema values other than the strict integers `2` and `3` are invalid.
4. Preserve historical validation across **both** `validate_committed_command()` and the deterministic state-machine application path. Do not weaken the generic command-envelope version (`command_schema_version=1`) or modify original command fingerprints, request IDs, deduplicated outcomes, or retained log bytes. An old v2 command must remain distinguishable from a new v3 command using only committed data.
5. Names are implementation details, but the two historical/current types and the union must remain unambiguous; do not globally substitute the v3-only class for every reference to the former `PresentationSnapshotDTO` without first updating old decoding paths and fixtures.

### Historical decoding is not permission to create new v2 decks

A broad v2/v3 decoder alone would accidentally permit new ownerless publications. The **admission and deterministic application rules** below separate decode compatibility from authorization.

## 3. Decision B — Replicated presentation-schema v3 activation boundary

1. Stage F introduces a **single deterministic, internal Raft-committed activation command**, tentatively named `activate_presentation_schema_v3`. It is not exposed through the public `/client` operation allowlist. Its `CommandEnvelopeDTO` has `command_schema_version=1`, a reserved internal `client_id` (e.g., `_presentation_schema`), no `deck_id`, no `session_id`, and no extension blocks. The internal request identity is deterministic and collision-aware in the same sense as the existing Stage E catalog/schema upgrade commands. `validate_committed_command()` rejects nonconforming internal activation envelopes.
2. On first successful apply at index **N**, activation persists a separately versioned presentation-policy record in the FSM, for example:

   ```json
   {"presentation_schema_policy": {"schema_version": 1, "v3_enforced_from_index": 143}}
   ```

   where `143` is an illustrative value for an activation committed at index `N = 142`; the actual stored boundary is the integer `N + 1` calculated from `committed_index`. The record is **not** a rewrite of every deck. Repeating the activation returns an idempotent already-active outcome and never shifts the boundary. The policy is persisted and replicated alongside `fsm.decks` and `fsm.auth`.
3. Activation is proposed only once the Stage E auth catalog is **READY** (matching configured fingerprint and auth-state schema v2) and the Stage F authorization/read-routing implementation is deployed to **all voters**. A deployment-local readiness/configuration check may control *when* an operator/leader proposes activation, but never replaces the Raft-committed policy as the source of truth for command application. This project does not promise mixed-version rolling upgrade compatibility; coordinate binary rollout before proposing the new internal command.
4. **Before activation:** already-committed historical v2 `publish_deck` entries are decoded and applied with their original v2 business behavior and Stage E historical actor-enforcement rules. The upgraded Stage F public adapter must *not* accept new v2 or v3 publication requests while activation is pending: reply with an availability/readiness error without proposing a publication. This ensures the cutover is explicit rather than defined by local process startup time. Historical committed entries ahead of `last_applied` must still drain in their original log order.
5. **From index `v3_enforced_from_index` onward:** deterministic `publish_deck` application requires presentation schema **v3** and the ADR-012 registered-owner/actor match. An exceptional v2 publication entry committed at or after the fence receives a stable **`UNSUPPORTED_PRESENTATION_SCHEMA`** result with *no* new deck/grants, rather than a validation crash or ownerless publication. The public adapter rejects new schema-v2 publications before proposal with the same semantic status. A v3 publication before the fence likewise has no business effect and receives `PRESENTATION_SCHEMA_UNAVAILABLE` if encountered in committed history. No automatic v2→v3 conversion occurs in this command.
6. At application, **validation order matters**: parse the historical-version-compatible envelope first; determine the committed schema policy and index-based publication rule; only then proceed to version-appropriate ownership/credential/business checks. Do not reject a historical v2 command merely because the running binary now supports v3. Other post-activation deck mutations and protected reads obey ADR-014/017/018, including quarantine of v2 decks without verified ownership; no Stage E legacy access bypass is introduced.
7. Previously committed v2 request-result deduplication records remain authoritative barriers against reapplication. Exact-ID retries never create a new deck, but **client-visible result disclosure** is still subject to the permission-safe replay/receipt rules of ADR-014/015/017; the schema cutover does not permit disclosing a private result by replaying an old request ID.
8. The v3 activation mechanism **does not migrate existing v2 decks**. ADR-018's separate, later, provenance-verified Raft migration remains the only transition that can assign a trustworthy owner to an existing v2 deck. Do not infer an owner from client/session ID or from an untrusted payload.

**Boundary invariant:** An entry at index `< v3_enforced_from_index` is interpreted according to its original committed schema/version rules; a new v2 `publish_deck` at or after the fence cannot produce an ownerless deck. The activation entry itself changes only the schema-policy record.

## 4. Decision C — Enforce auth-state readiness at the bearer-admission ReadIndex fence

1. Continue routing bearer-guarded commands to the leader and completing a fresh current-term ReadIndex before authorizing, proposing, or disclosing a stored result. On `ReadIndexStatus.READY`, **refresh the local authenticated-catalog status from the freshly applied replicated auth state and configured fingerprint**, just as `_progress_login_reads()` already does.
2. **If `auth_catalog_ready` is false**, terminate the volatile admission waiter with `{"ok": false, "error": "AUTH_UNAVAILABLE"}` before token lookup, operation construction, proposal, or request-result disclosure. The check covers missing auth state (`PENDING`), auth-schema-v1 state (`UPGRADE_PENDING`), invalid/unparseable state, and `FINGERPRINT_MISMATCH`. An auth-unready rejection is an admission failure, **not a committed business denial**, and must not advance a request's deduplication history or journal lifecycle as though it committed.
3. **If ready**, validate the current bearer against `auth.active_credentials[client_id]` and preserve the existing constant-time digest comparison, revoked-record check, server-stamped actor/credential references, and deterministic at-apply credential guard. Invalid/missing bearer retains `REAUTH_REQUIRED`; ready schema alone never authenticates a client.
4. Keep existing precedence for leadership loss, stale captured term and quorum failure: they cause redirect/`NO_LEADER`/`QUORUM_UNAVAILABLE` as appropriate, **not** a stale local `AUTH_UNAVAILABLE` determination. A preflight readiness check may reject early, but only the fresh post-ReadIndex readiness check authorizes continued admission.
5. Use the **same readiness predicate** for all Stage F authenticated read waiters (protected content reads, self-permission query, owner-only queries, status-only operation receipts). No protected read may bypass readiness by using a different waiter type. No bearer/plaintext secret enters Raft, disk snapshots, intention journals, or normal logs.
6. Recheck current credential validity and deck permissions at deterministic application for mutations according to ADR-011/014. The new readiness check does not replace the at-apply guard or alter Raft ordering, token-rotation behavior, or private-read authorization.

**Readiness invariant:** `ReadIndex READY` alone does not authorize admission; only `READY` **and** `auth_catalog_status == READY` **and** a valid bound bearer can proceed. Unready auth never exposes deduplicated results.

## 5. Status and compatibility contract

| Situation | Public result | Durable effect |
|---|---|---|
| Historical v2 publication at an index before activation | Existing historical publication outcome, subject to original auth-enforcement boundary | Replays deterministically; v2 deck remains v2 |
| New publication while Stage F activation is pending | `{"ok":false,"error":"PRESENTATION_SCHEMA_UNAVAILABLE"}` | No proposed publication |
| New v2 publication after activation | `{"ok":true,"result":{"status":"UNSUPPORTED_PRESENTATION_SCHEMA"}}` | No proposed publication; no new deck |
| Exceptional committed v2 publication after activation | `UNSUPPORTED_PRESENTATION_SCHEMA` committed result | No deck created |
| Valid v3 publication after activation | ADR-012/014 v3 outcome | Owner-private deck plus initial access state, atomically |
| Bearer-admitted mutation with `UPGRADE_PENDING` / `FINGERPRINT_MISMATCH` | `{"ok":false,"error":"AUTH_UNAVAILABLE"}` | No new command/proposal/dedup result |
| Malformed or missing `_auth` | `REAUTH_REQUIRED` (current transport behavior) | No proposal |
| Isolated leader / lost quorum | `NO_LEADER`, redirect, or `QUORUM_UNAVAILABLE` | No stale authenticated admission or protected read |

Here the named schema-policy statuses are **new Stage F response contracts** for the two exceptional/not-ready publication situations. They do not replace statuses established for ownership mismatch, access denial, existing-deck collision, or request identity reuse. Privacy-safe response handling for deck-ID collisions is a separate ADR-012/014 integration issue; do not inadvertently expose an existing deck/version through schema validation.

## 6. Implementation touchpoints

| File / component | Concrete work |
|---|---|
| `command_dto.py` | Add explicit `PresentationSnapshotV2DTO` and `PresentationSnapshotV3DTO`, associated versioned metadata DTOs, discriminated publication union, and internal schema-activation validation; retain command-envelope v1. |
| `state_machine.py` | Apply idempotent schema activation; persist policy fence; branch v2 historical vs v3 current publication by committed index/policy without raising on historical v2; create private v3 state according to ADR-012/013; preserve historical mutation semantics. |
| `app.py` | Guard public publication while activation pending and refuse new v2 after activation; propose activation only from a coordinated, auth-ready Stage F leader; add post-ReadIndex auth-catalog readiness check to `_progress_auth_admitted_commands()`; factor readiness into Stage F guarded-read pipeline. |
| `raft_core.py` / `persistence.py` / `raft_dto.py` | **No Raft consensus or storage-protocol change required**; existing committed index, FSM snapshot, and log contain the necessary cutover ordering. Only use the existing internal-command pattern. |
| `spec.py` / tests / scenarios | Model activation and both publication generations; update fresh Stage F fixtures to v3; retain legacy v2 scenarios as historical replay tests; prove auth schema-v2 readiness precedes bearer admission. |
| Stage F implementation specification | Declare activation sequencing, exact schemas, statuses, and coordinator preconditions; pin current-catalog fingerprint and Stage F deployment requirements. |

## 7. Mandatory acceptance tests

### F-001: historical decoding and deterministic schema cutover

1. **DTO decoding:** canonical v2 fixtures validate strictly as v2; v3 fixtures require valid `owner_user_id`; v1, v4, booleans, unknown/missing versions, and ownerless v3 are rejected. No fallback from invalid v3 to v2.
2. **Unapplied v2 log replay:** construct a persisted follower with an older v2 publication entry not yet applied, load Stage F binaries, apply that entry before the activation fence, and verify its original outcome, state and `last_applied`; verify subsequent v3 activation and publication succeed.
3. **Preserved-disk restart:** restart after v2 had already applied, preserving FSM/log/dedup records; verify no forced full-log replay, no deck rewrite or owner assignment, and no break in old state loading.
4. **Deterministic activation:** activate at index N on each voter; require persisted `v3_enforced_from_index == N+1` on all. Repeat activation without changing the fence or any deck data. A public request cannot forge activation.
5. **Post-fence denial:** propose malformed/exceptional v2 publication after activation in a deterministic engine test; verify committed rejection and no ownerless deck. New public v2 requests must be rejected before Raft proposal. New v3 publication after activation must create exactly one owner-private deck.
6. **Quarantine unchanged:** legacy unowned v2 decks remain preserved and inaccessible under ADR-018; no inferred ownership, grant, or unprotected read path.
7. **Dedup / failover:** exact-ID retries, leader replacement, and a minority partition do not shift the committed fence, reapply a publication, or return protected historical payloads to unauthorized clients.

### F-004: authentication-state readiness

8. **v1 / upgrade pending:** use a persisted auth-v1 state with an otherwise matching active bearer and a quorum-capable leader. After ReadIndex READY, a guarded mutation returns `AUTH_UNAVAILABLE` and generates no proposal/new request result.
9. **Catalog mismatch:** use validly encoded auth-v2 state whose fingerprint differs from the locally configured fixture; even with an otherwise valid credential, bearer admission returns `AUTH_UNAVAILABLE` after ReadIndex. Verify no call to `engine.propose` and no prior-result disclosure.
10. **Normal READY:** with matching auth-state v2 and valid current bearer, existing Stage E command admission, result statuses, rotation and at-apply credential check continue to work unchanged.
11. **Leader failure and quorum loss:** pending bearer waiters terminate/redirect without authenticating against an unconfirmed stale state; no password/bearer secret appears in persisted state, logs, or journals.
12. **Read-path parity:** when Stage F guarded reads are implemented, they enforce the same readiness condition and do not deliver content, grants, receipts, or deduped results from an unready auth state.
13. **Historical compatibility:** replaying actorless pre-Stage-E entries prior to `fsm.auth.enforced_from_index` remains possible; F-004's new admission-time readiness check must not retroactively invalidate those entries.

## 8. Consequences and alternatives

**Benefits:** Historical v2 commands remain decodable without making v2 a valid new ownerless publication format; the v3 cutover is a deterministic Raft event rather than a wall-clock deployment heuristic. Stage E's existing auth readiness guarantee is enforced consistently for bearer admission, including before dedup-result disclosure. The generic Raft protocol, membership, and durable intention identities remain unchanged.

**Costs:** One small internal schema-activation command and replicated policy field, extra versioned publication DTOs, a coordinated all-voter Stage F rollout, and new replay/cutover tests. Initialization must not race normal publication admission. This ADR does **not** add a general v1→v2 presentation importer or a migration ownership claim mechanism.

**Alternatives rejected:**

- **Replace `Literal[2]` with `Literal[3]` globally:** breaks compatibility for old committed commands that remain to be applied.
- **Accept v2 and v3 indefinitely at public publication admission:** allows new ownerless v2 decks after Stage F permission enforcement.
- **Choose the schema boundary using local process startup time or a feature flag alone:** nodes can disagree after restart, partition, or staggered rollout.
- **Rewrite old v2 logs/snapshots on load to synthesize v3 ownership:** nondeterministic/insecure and contrary to ADR-012/018.
- **Treat ReadIndex readiness as sufficient bearer authorization regardless of auth-state schema readiness:** violates the Stage E admission contract.
- **Apply auth-schema readiness retroactively to historic commands:** would change old committed execution semantics; this is an admission-only correction.

## 9. Follow-up scope deliberately excluded

This ADR resolves only **F-001 and F-004**. It does **not** resolve the separate F-002 provenance verification bridge for migrating already published v2 decks, F-003 metadata/settings validation coupling, or publication collision/privacy response details. Those remain implementation-specification tasks under ADR-012/014/018 and must not be mistaken for solved problems. All Stage F guarded reads, mutation-role checks, operation receipts and client synchronization still require code and tests under ADR-013–018.

**Implementation status:** Proposed design; no GitHub commit or code change has been performed by this ADR document.
