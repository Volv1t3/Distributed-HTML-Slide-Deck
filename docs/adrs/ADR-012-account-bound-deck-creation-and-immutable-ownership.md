# ADR-012 — Account-Bound Deck Creation and Immutable User Ownership

- **Status:** Accepted (architecture decision; implementation pending Stage F)
- **Date:** 2026-11-26
- **Stage:** F — Deck ownership and sharing, decision F-01
- **Related:** ADR-004 (Durable Intention Journal), ADR-006 (structural deck operations), ADR-008 (versioned settings), ADR-009 (registered accounts), ADR-010 (authenticated actor and identity boundaries), ADR-011 (replicated credentials); Stage E Authentication Implementation Specification; Story-01 (creating and sharing decks)
- **Successor:** Stage F implementation specification and subsequent ADRs for grants, role permissions, read authorization, and migration of legacy published decks

## Context

The application follows a local-first presentation workflow: a user creates and edits a private draft locally, may subsequently publish its complete snapshot through Raft, and may independently grant other registered users access. Publication makes replicated state authoritative; publication **does not itself grant access** to another account.

Stages A–D implemented publication, editing, locks, and independent deck/slide settings revisions. Stage E introduced registered `user_id` identities, persistent client installation IDs, bearer authentication, server-stamped command actors, and deterministic credential checks at Raft application. Stage E deliberately did not implement deck ownership or resource-specific permissions. Consequently, authentication currently proves who sent a mutation but not that the sender owns or may modify the target deck. Existing follower-local presentation reads also remain unauthenticated.

The current presentation snapshot has `schema_version: 2` and `PresentationMetadataDTO` fields `deck_id`, `name`, `owner_session_id`, `width_px`, and `height_px`. At publication, the state machine deliberately strips `owner_session_id` because an editor-session UUID is not a registered account or an authoritative ownership claim. No current published deck has a trusted `owner_user_id`.

Stage F must make ownership an account-level property from creation onward, preserve the separation between user, client, and editor-session identities, and prevent a caller from claiming someone else's account through editable local metadata.

## Decision

### 1. Account identity is mandatory for deck creation and editing

1. **No anonymous deck model.** A new presentation may be created only in the context of a previously authenticated, registered User Account. The local creator is the account identified by the established `user_id`, not a client installation or editor session. Account identity is required for creating, changing, or deleting a local deck and for subsequent publication, collaboration, settings mutations, and sharing operations. This ADR does not add user registration, guest accounts, public links, or anonymous invitations.
2. **Local-first does not mean identity-free or always-online.** Once the application has established a signed-in account identity, private drafts and their local modifications need not contact the Raft cluster on every action. A temporary network or quorum outage does not, by itself, replace or erase that identity; continued offline local editing is permitted under the client's established signed-in context. Offline access is a local application policy, **not** proof that a remotely revoked credential is still valid. Any later distributed operation must pass the existing Stage E fresh-leader authentication checks.
3. The local draft retains its owner's `user_id` across ordinary app restarts. A missing signed-in identity, explicit sign-out, or a different currently selected account must not silently reassign ownership or authorize edits to another account's local draft. The eventual client-side credential/account-state UX is specified separately; no identity may be inferred from `client_id`, `session_id`, a file path, or a draft's unverified metadata alone.

### 2. Presentation schema v3 records creator ownership at the source

4. The **presentation snapshot schema advances from v2 to v3** for the Stage F ownership contract. `PresentationMetadataDTO` gains a required `owner_user_id: UUID`. New presentations must set this field at local creation from the established signed-in account. It is not a nullable field and has no anonymous sentinel or default.
5. Retain `owner_session_id: UUID` in local/published-input metadata for the existing editor-session lifecycle and backward-compatible local editing semantics. It identifies an editor session, **not** the owner account and not a privilege. Its presence never grants sharing rights, nor does a successful credential rotation change the deck's owner. As in Stage E, the backend removes `owner_session_id` when installing authoritative replicated deck metadata.
6. A DTO can validate that `owner_user_id` is a correctly typed UUID, but UUID syntax alone does not demonstrate registration or authorization. Actual registration and caller identity are verified against Stage E's replicated account and credential state.

**Normative proposed shape (not yet implemented):**

```python
class PresentationMetadataDTO(StrictDTO):
    deck_id: UUID
    name: StrictStr = Field(min_length=1)
    owner_user_id: UUID           # account that created the local presentation
    owner_session_id: UUID        # editor-session identity; never a permission
    width_px: StrictInt = Field(ge=320, le=3840)
    height_px: StrictInt = Field(ge=320, le=3840)


class PresentationSnapshotDTO(StrictDTO):
    schema_version: Literal[3]
    metadata: PresentationMetadataDTO
    slides: list[PublishedSlideDTO] = Field(default_factory=list, max_length=256)
```

Existing strict schema-version validation, uniqueness of slide/element identifiers, canvas bounds, and other presentation DTO requirements remain in force.

### 3. Ownership becomes authoritative atomically at publication

7. For a new v3 `publish_deck`, the authenticated leader uses Stage E's quorum-confirmed admission and **server-stamps** the actor identity and non-secret credential reference. The untrusted local `presentation.metadata.owner_user_id` is carried as the creator's claimed identity, not treated as proof of ownership.
8. During **deterministic Raft application**, before creating any deck state, the replica validates the committed credential/actor under Stage E rules and requires:

   - a registered `owner_user_id` present in the replicated User Account catalog;
   - `presentation.metadata.owner_user_id == authenticated_actor_user_id`;
   - no previously published deck under the requested `deck_id`; and
   - the existing structural and schema preconditions.

   If any required condition fails, the publication does not create a deck or grant, advance a presentation revision, or silently rewrite its claimed owner. An ownership mismatch receives a stable, non-success ownership/authorization result (exact external status to be standardized by the Stage F specification); authentication denial retains Stage E's established outcome semantics.
9. Successful committed publication **atomically creates the published deck with immutable `owner_user_id`** equal to the verified creator account. The authoritative owner is stored with the deck's replicated metadata so future permissions can be resolved against the same committed state. `owner_session_id` is excluded from that authoritative metadata, as before. The deck and its owner never exist as two separately acknowledged writes.
10. Ownership belongs to the **User Account**, not to the publishing `client_id` or editor `session_id`. The same user may act through multiple independently authenticated client installations. Token rotation, sign-in to a new device, restart, or a new editing session does not transfer ownership.
11. This ADR defines **immutable ownership** for Stage F. It introduces no transfer-ownership, co-owner, owner-delete/recreate, or account migration operation. Those would require a separate explicit decision and deterministic command contract.

### 4. Owner-only sharing authority; separate publication and grants

12. A newly published deck is **owner-private by default**, even though its bytes are replicated across Raft replicas. Its existence in the cluster is not a public invitation, and it has no implicit grants to other accounts.
13. A separate Raft-committed access-grant operation is required before another registered user may obtain collaborative access. **Only the owner account may add, change, or remove collaborators' permissions.** A collaborator's editing rights, once granted, do not imply the power to share onward, revoke someone else, or manage ownership. The exact grant DTOs, roles, mutation matrix, and revocation semantics are deferred to subsequent Stage F decisions.
14. Every permission-changing command must use the Stage E authenticated principal and be checked against the **current owner and grant state at deterministic application**. Admission-time checks may reject early, but they cannot replace apply-time authorization. No raw bearer credential is replicated in sharing commands.
15. Read authorization and revocation freshness are a separate Stage F decision: owner-private is the required target behavior, **not a claim that the current unauthenticated follower-local read endpoints already enforce privacy**. The backend must not advertise distributed private sharing as complete until those reads are protected.

## Invariants and illustrative command order

- **Owner integrity:** For every new v3 published deck `D`, `D.metadata.owner_user_id` equals the registered account represented by the verified publishing actor at D's creation commit index.
- **No delegated ownership:** Neither `owner_session_id` nor `client_id` can establish or override ownership.
- **Immutable identity:** Once assigned, `owner_user_id` does not change through edits, grants, revocations, token rotations, or client/session changes.
- **Atomic creation:** A failed publication adds neither an ownerless deck nor a partial deck.
- **Private default:** No user other than the owner receives implicit access merely because the deck is Raft-replicated.
- **Owner-only ACL mutations:** Only owner-authorized committed transitions can change deck sharing policy.

Illustrative ordering:

```text
index 80: publish_deck(D, creator=sofia, actor=sofia)  -> PUBLISHED; owner=D:sofia
index 81: grant_access(D, target=santiago, actor=sofia) -> may succeed under later ACL contract
index 82: grant_access(D, target=other, actor=santiago) -> deny; editor role cannot share
```

If Sofia's local draft declares Sofia as owner but Santiago submits the publication under his valid login, the committed publication is denied; the system does **not** silently relabel the owner as Santiago.

## Presentation schema v2 → v3 and compatibility boundary

Stage F implementations and their tests must use schema v3 for **new publications**. The generic Raft command-envelope schema and auth-state schema are independent of the presentation snapshot schema and are not automatically incremented by this ADR. The existing deck structural revision, independent settings revisions, element revisions, lock fencing, intent journal request IDs, and canonical color/slide-order conventions are preserved.

**Legacy published v2 decks require a distinct migration policy**, which this ADR does not invent. Earlier v2 state has no trusted owner field; a historical `owner_session_id` or `client_id` cannot be treated as a User Account. Stage E log actor stamps may supply useful provenance for some entries, but no legacy owner may be inferred without an explicitly agreed deterministic migration rule. Preserved Raft logs/snapshots and compatibility tests must not be silently rewritten into arbitrary ownership assignments.

The Stage F specification will define the relevant persisted-state migration, deterministic replay compatibility, migration entry boundaries, and the specific updates needed for Stage A–E deterministic and live-cluster test fixtures. Until migration and permission enforcement are complete, an existing v2 deck must not be misrepresented as owner-private.

## Consequences

- Deck ownership becomes an explicit user-level domain invariant from initial local creation through distributed publication.
- A disconnected signed-in user may continue private local editing, but cannot use a local claim to bypass server credential or ownership checks upon publication.
- The replica remains the sole authority over *distributed* ownership, which is committed atomically with deck creation and survives leader failover.
- Existing `owner_session_id` semantics are retained for local/editor compatibility without letting session IDs become security principals.
- Presentation schema v3 and local-client draft creation must be implemented together; tests that construct v2 snapshots will need intentional upgrades or historical-replay treatment.
- Legacy published deck migration, collaborator permissions, discovery, read consistency, and precise sharing command statuses remain explicit follow-up work, not implicit behavior.
- The current plain HTTP transport's confidentiality limitations and the Stage E pre-provisioned account model remain unchanged.

## Alternatives considered

- **Assign ownership only at publication:** rejected; it contradicts account-bound creation and makes local creator ownership undefined.
- **Allow anonymous drafts and bind them to an account later:** rejected for the Stage F baseline; anonymous ownership and transfer/reclaim semantics are out of scope.
- **Use `owner_session_id` as owner:** rejected; editor sessions are transient locking identities, not registered users.
- **Use `client_id` as owner:** rejected; one account may use several installations, and client ID is not an authenticated principal.
- **Trust caller-supplied `owner_user_id` without verification:** rejected; it would allow ownership impersonation.
- **Grant sharing automatically during publication:** rejected; publication and collaboration are distinct decisions, and distributed decks are owner-private initially.
- **Store ownership solely in a separate unversioned in-memory registry:** rejected; it could diverge from the replicated deck and disappear on failover.
- **Silently assign owners to all historical v2 decks:** rejected until a deterministic provenance and migration policy is accepted.

## Deferred Stage F decisions

1. Sharing relation and role DTOs (OWNER/EDITOR/VIEWER and permitted actions).
2. Grant/change/revoke command contract, outcome precedence, and deduplication.
3. Protected read semantics and freshness under lagging followers/revocation (F06).
4. Editor lock and durable intention behavior after collaborator permission loss.
5. Legacy v2 deck ownership migration and user-facing discovery/listing.
6. Local account-state and offline-sign-in UX details; no anonymous mode and no implicit account transfer.

## References

- `docs/adrs/ADR-006-deck-and-slide-structural-operations.md`
- `docs/adrs/ADR-008-versioned-deck-and-slide-settings.md`
- `docs/adrs/ADR-009-preprovisioned-user-accounts-and-raft-storage.md`
- `docs/adrs/ADR-010-user-client-session-and-authenticated-command-boundary.md`
- `docs/adrs/ADR-011-replicated-bearer-token-rotation-and-revocation.md`
- `docs/specs/Stage-E-Authentication-Implementation-Spec.md`
- `docs/user-stories/user-story-01-creating-and-sharing-decks.md`
