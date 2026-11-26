# ADR-010 — User, Logical Client, Editor Session, and Authenticated Command Boundary

- **Status:** Accepted
- **Date:** 2026-11-24
- **Stage:** E, authentication foundation
- **Related:** ADR-003, ADR-004, ADR-007, ADR-009, ADR-011
- **Implementation reference:** `Stage-E-Authentication-Implementation-Spec.md`

## Context

The existing instructor `POST /client` message includes `id`, `client`, `op`, and `args`. The current application copies the unauthenticated `req.client` into `CommandEnvelopeDTO.client_id`; `raft_core.RaftEngine._request_key()` already hashes `(client_id, request_id)` for replicated deduplication. The durable client journal uses this same logical client identity. Protected element locks separately use `session_id`, and nodes already have persistent `node_uuid` identities. None of those IDs proves the sender controls a user account.

Stage E must add authentication **without changing the meaning of the existing identities** or breaking retries, lock ownership, and the journal.

## Decision

1. **Separate five concepts:** `user_id` (permanent registered account), `client_id` (stable installation/journal UUID), `request_id` (one logical operation), `session_id` (editor instance/lock owner UUID), and `node_uuid` (Raft replica). Also add an **opaque bearer `access_token`**, which is a secret credential rather than an identity.
2. One user can own **multiple** logical clients (e.g., laptop and desktop). A client installation retains the same UUID across application restarts; it is bound to one user for this phase. If a different user starts using the same installation, they must use a distinct client identity and journal namespace.
3. Authentication uses a **password-authenticated `login`** operation against pre-provisioned accounts. The response identifies the stable `user_id`, the supplied/validated `client_id`, and a new opaque token, plus a non-secret credential-generation revision. It does **not** create a new `user_id` on each login.
4. The **quorum-confirmed Raft leader is the authentication-admission boundary**. Before password or bearer validation, dedup-result disclosure, or proposal, it completes a fresh Auth Admission Read using the existing ReadIndex mechanism and applies through the confirmed index. Followers and isolated former leaders never grant authority based on `req.client` or stale local credential state. A follower may redirect a write to the leader.
5. The server stamps `authenticated_actor_user_id`, `credential_id`, and `credential_generation` (or equivalent non-secret proof metadata) into a validated command. These fields must **not** be accepted as authoritative assertions from client-supplied payloads. The raw token is never a command-envelope field.
6. **Two checks are required.** At admission, reject invalid bearer credentials before proposing. During deterministic committed application, check the credential's binding, active generation, and revoked status **again** against the state as of that Raft log index, before performing the requested business transition. This ensures `revoke → update` in the log cannot apply the update, even if the leader originally admitted it before revocation.
7. Preserve the existing `(client_id, request_id)` deduplication key. Extend canonical command fingerprints to include the server-asserted account identity, but **exclude** the transport bearer secret and credential generation so a pending intention can be retried after reauthentication without changing its logical operation. A request with the same ID but different business content or different authenticated actor remains `REQUEST_ID_REUSE`.
8. Existing `session_id` and fencing tokens remain the **only** element-lock authority. An authenticated user does not automatically inherit another editor session's locks. A valid element edit requires both successful authentication and its ordinary lock/version checks.
9. Authentication credentials are transported **ephemerally** through the instructor's existing `/client` request path. `node.py` currently forwards only the `args` dictionary; therefore a reserved wire-only authentication field inside `args` is needed. The adapter strips this field before constructing canonical command payloads. A new auth-aware transport preserves explicit journal request IDs and redacts before writing checker-compatible histories. Starter-owned `client.py` remains unchanged; ordinary `Client.call()` with a password or token in `args` is unsafe and must not be used for login or authenticated mutations.
10. The password and bearer token never appear in Raft entries, canonical request fingerprints, deduplication records, client intention journals, `client.py` histories, or operational logs, including DEBUG mode. Authentication errors should reveal neither account existence nor password correctness.
11. Stage E authenticates external presentation mutations, including Stage D deck/slide settings commands, but does not change their business preconditions. Stage D Local Eventual Reads and polling remain unauthenticated until Stage F defines deck-read authorization. Internal `raft.noop`, `expire_lock`, catalog initialization, and the legacy Stage A KV proof surface have explicit non-user rules.
12. Admission failure and committed denial are different client outcomes. Missing, stale, or revoked credentials detected before proposal return an uncertain `REAUTH_REQUIRED` signal so an existing outbox intention remains pending and retries with the same ID after login. A credential invalidated earlier in Raft order produces a committed terminal `UNAUTHENTICATED` result; retry cannot turn that request identity into a mutation.
13. Preserve command-envelope schema version 1 for log compatibility. Auth catalog state records `enforced_from_index`: actorless historical entries below it may finish replay, while protected external mutations at or above it require server-stamped actor metadata. Pre-activation dedup records remain barriers against reapplication but are never attributed to a user or exposed as authenticated successes.

## Current implementation versus required additions

| Concern | Current repository | Stage E addition |
|---|---|---|
| `req.client` | Arbitrary caller-supplied string | Stable UUID whose account binding is authenticated |
| `req.id` | Retry-preserving operation identity | Unchanged |
| `CommandEnvelopeDTO.client_id` | Unverified string | Verified logical client ID; bind to authenticated actor |
| `CommandEnvelopeDTO.session_id` | Editor lock ownership | Unchanged and not a login session |
| `CommandEnvelopeDTO.origin_node_uuid` | Leader's generated node UUID | Unchanged |
| `client_journal.Intention.client_id` | Durable journal identity | Unchanged, persists across token rotation |
| `Client.call`/history | Generates its own request ID and logs `args` as supplied | Leave unchanged; add a redacting auth-aware transport with explicit request ID and compatible history records |
| `app.py` request diagnostics | Can include request payload | Sanitize **before** logging/fingerprinting any wire-only authentication data |

## Log-order examples

- `update_element @ index 40`, `revoke_access_token @ 41`: update may succeed if lock/version checks pass.
- `revoke_access_token @ 40`, `update_element @ 41`: update must deterministically return `UNAUTHENTICATED` (or equivalent stable denial), with no element mutation.
- An exact-ID replay of an earlier **already committed** successful update is not a new mutation. Admission must still validate the current credential before exposing its stored result; a newly authenticated client may retrieve the idempotent result. This distinction must be tested separately from new writes.

## Consequences

- Authentication is an extension of the existing command path, not an independent consensus system.
- Authorization to decks is **not** granted by authentication. Stage F will define owner/editor/viewer access and enforce those permissions during apply.
- Until Stage F, any authenticated account can target any deck if the existing structure, version, and Lock Authority checks pass. Stage E does not bind an Editor Session or fencing token to a User Account.
- Until Stage F exists, the app must not claim user-private distributed deck isolation. Account records and credential metadata are internal only; user-profile reads must not expose verifiers.
- The current HTTP transport lacks TLS. This classroom-only model does not protect bearer secrets against network observers.

## Alternatives considered

- Use `client_id` as proof of identity: rejected because it is visible, client supplied, and needed for deduplication/journals.
- Replace `client_id` with `user_id`: rejected because multiple installations must maintain independent journals and retries.
- Use `session_id` as login identity: rejected because it is an editor lock-session identity.
- Carry raw bearer secrets in the Raft command log: rejected.
- Authenticate only at request admission: rejected because it cannot enforce revocation order during committed application.
