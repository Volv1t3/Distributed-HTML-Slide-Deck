# ADR-009 — Pre-Provisioned User Accounts in the Existing Raft State Machine

- **Status:** Accepted
- **Date:** 2026-11-24
- **Stage:** E authentication foundation; Stage F owns sharing/access grants
- **Related:** ADR-002, ADR-003, ADR-004, ADR-007; Stage D/D.1 issue #28; ADR-010, ADR-011
- **Implementation reference:** `Stage-E-Authentication-Implementation-Spec.md`

## Context

Distributed HTML Slide Deck already operates a deterministic state machine backed by durable Raft state. `raft_dto.PersistedRaftStateDTO` stores a JSON `fsm` snapshot; `persistence.NodeDiskPersistence` delegates to the instructor runtime's `self.disk`/`self.save()` mechanism. The runtime writes each replica's own `run/<slot>/disk.json`. The replicas **do not** share a database or process memory.

Stage E needs user identities for subsequent authentication and, later, deck ownership and sharing. A registration service, email delivery, recovery workflow, external database, or database container would not improve the course's consensus demonstration sufficiently to justify the extra components. The design accepts pre-registered classroom accounts as fixtures.

## Decision

1. **No database service is introduced.** User records, password verifiers, client-to-account bindings, and credential metadata belong to the existing Raft state-machine snapshot and are durably persisted along with other application state.
2. **No public `create_user` operation in this phase.** A small, deterministic, versioned account catalog is pre-provisioned. The catalog includes only classroom/test users; fixture plaintext passwords are permitted only in clearly designated test setup material, never in the replicated snapshot.
3. Every account has a **permanent `user_id: UUID`** and a validated profile: `first_name`, `last_name`, normalized unique `email`. The user ID is distinct from client, editor-session, request, and replica identifiers.
4. Persist a separate **Password Verifier**, never a reversible password or plaintext password. For the baseline it is `PBKDF2-HMAC-SHA256`, with a unique randomly generated salt per account, explicit iteration count, and a derived hash. The committed verifier contains the algorithm, iterations, salt, and hash, **not** the password.
5. Fixture material, including UUIDs, salts, hashes, and canonical email addresses, is generated **once** and distributed unchanged. Neither the leader nor followers call a random generator or password hasher to *construct* fixture records during state-machine application. Password *verification* happens outside the deterministic apply function.
6. A user's created-deck count is derived from authoritative deck ownership data when that feature is defined, rather than maintained as an independently mutable counter.
7. Public user-profile DTOs never expose password verifiers or access-token digests. Sensitive storage is treated as internal-only state.
8. On an ordinary preserved-disk restart, restore the stored catalog and credentials without reseeding or overwriting. For a **fresh Stage E cluster**, `App.start` supplies the same validated genesis FSM only when no durable state exists. For a **pre-existing Stage D persisted cluster**, a leader-local coordinator proposes one internal, idempotent, Raft-committed catalog initialization keyed by the catalog fingerprint; never silently mutate distinct existing snapshots on individual restarts.
9. `cluster.py down` removes `run/` after archiving diagnostic histories; it is **not** a durable cluster-preservation operation. Tests must distinguish preserved-disk node restart from fresh-cluster initialization.
10. Catalog initialization changes only `fsm.auth`. It preserves Stage D presentation schema v2, decks, independent settings revisions, tombstones, locks, KV compatibility state, and request-result records. The catalog is deliberately small enough to fit the existing Raft log-entry bound.

## Data ownership and model boundaries

```text
StateMachineSnapshot
├── decks: {...}                # existing presentation state
├── kv: {...}                   # Stage A compatibility
└── auth:
    ├── schema_version: 2
    ├── catalog_fingerprint: <canonical SHA-256>
    ├── enforced_from_index: <first actor-required log index>
    ├── users: {user_id: UserRecord}
    ├── email_index: {normalized_email: user_id}
    ├── client_bindings: {client_id: user_id}
    ├── active_credentials: {client_id: CredentialRecord}
    └── retired_credentials: {credential_id: RetiredCredentialRecord}
```

The email index is a deterministic projection of user records and must be validated for uniqueness. The credential record represents exactly one current generation per bound client, and `retired_credentials` permanently retains rotated-out credentials for lifetime uniqueness. The separate **account catalog fixture remains schema v1**; this diagram describes the **authentication-state schema v2**. ADR-011 specifies the permanent retirement invariant and the Raft-committed upgrade required for preserved authentication-state v1 snapshots. Never transform a v1 snapshot opportunistically when reading from disk.

`enforced_from_index` is `1` for fresh Stage E genesis. When an existing Stage D cluster applies catalog initialization at index `N`, it becomes `N + 1`. Older actorless entries remain replayable during recovery, but an external presentation mutation at or above the boundary requires Stage E actor metadata. This preserves existing command-envelope schema version 1 without silently authorizing new legacy-shaped writes.

## Security and determinism

- Prefer Python standard-library `hashlib.pbkdf2_hmac`, `secrets`, `hashlib`, and `hmac.compare_digest`; existing Pydantic v2 provides DTO validation. A proposed baseline is 600,000 PBKDF2-HMAC-SHA256 iterations, with per-account work-factor metadata. Benchmark the login latency on the course environment; do not silently weaken production parameters to make tests fast.
- Generate and freeze fixture salts/hash outputs ahead of time. The state machine validates and installs only the canonical serialized fixture.
- Never log email/password combinations, passwords, verifiers, bearer tokens, or digests in ordinary diagnostics or client journals. Audit records may include opaque account/client IDs and outcomes.
- Ordinary HTTP is retained for instructor compatibility but **does not provide network confidentiality**. The result is a classroom demonstration, not deployable internet-facing authentication. Production deployment would require TLS, host access controls, and more robust account security.

## Consequences

- The model remains single-source-of-truth and recoverable after ordinary Raft failures, without adding database replication or another transaction coordinator.
- Stage E adds versioned, validated authentication state, deterministic genesis/migration, and test fixtures, not a full account lifecycle service.
- Credential confidentiality at rest is limited by the existing plaintext JSON snapshot: password verifiers and access-token digests are stored in clear JSON as **non-reversible verification values**, not encrypted secrets. Restrict filesystem access.
- Stage F will specify which decks an authenticated account may read or modify and how ownership is assigned to decks that predate authorization. **This ADR by itself does not enforce deck authorization.**

## Alternatives considered

- PostgreSQL/SQLite as a second authoritative store: rejected for this phase because Raft already owns deterministic application data. SQLite may later serve as a non-authoritative local projection, not a competing source of truth.
- Pure process-local memory: rejected because it loses account and credential state on failure.
- Register-user operation and email confirmation: deferred.
- Plain SHA-256 for human passwords: rejected because it is too fast for offline password guessing.
- Random salt generation separately on each replica during apply: rejected as nondeterministic.
