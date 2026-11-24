# Stage E — Authentication Foundation Implementation Specification

- **Status:** Accepted for implementation planning; **not** implemented
- **Date:** 2026-11-24
- **Target repository:** `Volv1t3/Distributed-HTML-Slide-Deck`
- **Source baseline reviewed:** `master` at `3961aa27c0` on 2026-11-24, plus Stage D/D.1 GitHub issues #28–#36. Stage D work is concurrent and **must not be overwritten** by this specification.
- **Prerequisites:** Stage D and D.1 delivered and accepted; ADR-001–ADR-007, Stage D's reserved ADR-008, and proposed ADR-009–ADR-011.
- **Scope:** Stage E authentication foundation and its Stage E.A acceptance gate. Stage F owns deck ownership, sharing, permissions, and authorization. User administration and full account management remain later work.

## 1. Goal and boundary

Introduce a minimal, testable account and credential system without changing Raft election, replication, commit rules, fixed membership, ReadIndex semantics, or the instructor's HTTP/fault architecture. A quorum-confirmed current leader authenticates requests; a deterministic state machine rechecks credential validity in log order so a revocation committed earlier prevents a later write. Pre-provisioned accounts, durable per-client credentials, and a persistent client journal survive ordinary replica/client restarts.

Stage E authenticates actors but does not authorize them to particular decks. It consumes Stage D's presentation schema v2, independent settings revisions, Settings FIFO Lanes, settings conflict results, and Local Eventual Reads without redefining them. Stage F will add ownership and permission decisions on top of this boundary.

This is **not** a secure internet-facing identity provider. The instructor runtime uses unencrypted HTTP and intentionally trusts its test environment. Production-grade TLS, rate limiting, secret management, and administrative security are outside the baseline, but secrets must still be excluded from project logs and Raft state.

### User stories

- **E-A01:** As a pre-registered user, I authenticate with my email and password without implementing a registration service.
- **E-A02:** As a user with multiple devices, I keep the same `user_id` but distinct stable `client_id` values and independent journals.
- **E-A03:** As a client, I receive an unpredictable bearer token after committed login and reuse it after restarting.
- **E-A04:** As an operator, I can kill or restart a leader without losing committed credential associations or revocations.
- **E-A05:** As a client, I can reauthenticate after a lost login response, generating a new token while retaining my journal identity and operation IDs.
- **E-A06:** As a user, I can revoke my own client credential and ensure later committed writes using it fail.
- **E-A07:** As a collaborator, a protected edit is validated against both authentication state and ordinary editor-lock/version preconditions.
- **E-A08:** As an evaluator, I can prove auth data never leaks into persisted Raft log, histories, operational logs, or client intention journal.
- **E-A09:** As an evaluator, I can validate log-order revocation/write races and leader failover in clean three-replica scenarios.

## 2. Explicit non-goals

- No `create_user`, delete user, change password, password reset, email confirmation, OAuth, SSO, recovery codes, MFA, or email service.
- No deck ownership or user-to-deck permissions yet; Stage F will design them. In Stage E, `AUTHENTICATED` **does not mean authorized for a particular deck**. Do not claim cross-user deck isolation.
- No token-expiration deadlines, refresh tokens, JWT signing keys, shared IVs, key-management server, TLS termination, database container, or second consensus protocol.
- No modification to the instructor-owned Raft network routes (`POST /client`, `POST /msg`, `GET /status`), fault injection semantics, checker engine, or dynamic membership.
- No schema v1/v2 presentation reshaping here: consume Stage D's final presentation DTOs and use a separate authentication-state schema.

## 3. Verified existing interfaces and proposed integration

| Existing component | Verified capability | New integration responsibility |
|---|---|---|
| `node.py` | `Request(id, op, args, client)` delivered sequentially to `App.on_request`; `self.disk/self.save` with fsync; HTTP `/client` | Keep unchanged. Carry wire-only auth credentials in a reserved field in `args`; strip at adapter boundary. |
| `client.py` | Starter-owned `Client.call` generates its own request ID and logs supplied `args` to JSONL | Keep unchanged. Add a separate auth-aware client transport with the same checker-compatible history shape, explicit wire request IDs, and pre-serialization redaction. |
| `client_journal.py` | `ClientIntentionOutbox(logical_client_id, journal_path, session_id)` durably writes intent operation/arguments and preserves ID; `JournaledClient` injects a request sender | Store no password/bearer token in intentions; inject the current credential only inside the request sender immediately before transport. Preserve the journal's request ID as the HTTP `id`. |
| `command_dto.py` | Strict Pydantic v2 command envelope with `client_id`, `request_id`, `session_id`, `origin_node_uuid`, optional TLV blocks | Add server-stamped actor/credential reference fields and typed authentication commands; no raw token/password in any committed command. |
| `raft_core.py` | Dedup key SHA-256 of `[client_id, request_id]`; durable apply + `request_results` | Leave Raft protocol intact; adjust logical command fingerprint to include account actor, not transport credential generation. |
| `state_machine.py` | Deterministic shared command apply; `snapshot()` | Add versioned `auth` state, issue/revoke transitions, and a credential-generation guard before every external presentation mutation, including Stage D settings commands. |
| `persistence.py` | `PersistedRaftStateDTO.fsm` stored via `NodeDiskPersistence` | Retain as existing JSON snapshot store; no new DB. |
| `app.py` | Leader redirects, proposal and reply handling, operational logs, quorum ReadIndex | Redact before any event logging; complete a fresh quorum-confirmed Auth Admission Read before token checks or dedup-result disclosure; keep PBKDF2 outside the single-threaded node event loop. |
| `spec.py`, `scenarios/`, `tests/` | Deterministic checker model and live 3-node harness | Extend model for safe auth operations and create a Stage E.A clean-cluster validation gate. |

### Repository compatibility

`master` currently contains ADR-001 through ADR-007; Stage D issue #28 references **ADR-008**, which is reserved for Stage D. This package therefore uses **ADR-009–ADR-011**. If Stage D creates other ADRs in the meantime, reconcile numbering at merge time, not by renaming Stage D's documents.

### Stage D and Stage F integration contract

- Stage D remains the sole owner of presentation schema v2, canonical RGBA colors, Derived Slide Ordinals, `deck_version`, `deck_settings_version`, per-slide `slide_settings_version`, and settings reconciliation.
- `publish_deck`, structural slide commands, element and public lock commands, `update_deck_settings`, and `update_slide_settings` are authenticated external mutations in Stage E. Authentication adds an actor guard but does not add an Editor Session, Fencing Token, or deck-wide lock to Stage D settings.
- A valid credential guard runs before the existing business transition. If it succeeds, every Stage D status, version rule, conflict snapshot, commit fence, deduplication rule, and Canvas Resize invariant remains unchanged.
- `get_deck_preview`, `get_slide`, `get_element`, `read_lock_status`, and ordinary client polling remain unauthenticated Local Eventual Reads in Stage E. Without Stage F permissions, authenticating these follower-local observations would neither provide access control nor a current revocation guarantee.
- `authoritative_refresh_element` retains its existing quorum-confirmed freshness contract but does not become a deck-authorization check in Stage E. Stage F must define read authorization before any read is described as private.
- Stage E does not infer a deck owner from `owner_session_id`, `client_id`, or the authenticated actor and does not add owner state to published decks. Stage F must explicitly define ownership assignment or migration for decks that predate it.
- Until Stage F, any valid User Account may target any deck; only the existing structural, settings-version, element-version, and Lock Authority preconditions can reject that mutation. Editor Sessions and fencing tokens are not bound to a User Account in Stage E.
- Stage D settings intentions remain in their per-resource FIFO lanes. A pre-proposal authentication failure keeps the existing intention pending; a committed at-apply authentication denial is terminal for that request identity and requires a new business request after reauthentication.
- The generic command envelope remains schema version 1 so preserved Stage A–D log entries remain decodable. Optional actor fields are a backward-compatible extension, and `fsm.auth.enforced_from_index` deterministically separates historical actorless entries from Stage E protected commands.

### Stage E operation boundary

| Operation class | Stage E authentication rule |
|---|---|
| Password `login` | No bearer token; requires a fresh Auth Admission Read and password verification before internal issuance is proposed. |
| External presentation mutations | Bearer required at admission and credential reference rechecked at deterministic apply. This includes publication, slide structure, elements, public lock operations, and both Stage D settings commands. |
| `revoke_access_token` | Current bearer required; deterministic fenced revocation at apply. |
| Local Eventual Reads and polling | No bearer requirement until Stage F defines deck-read authorization. |
| `authoritative_refresh_element` | Existing ReadIndex freshness only in Stage E; Stage F will add resource authorization. |
| `raft.noop`, `expire_lock`, catalog initialization | Explicit internal actors; never accepted as public client operations. |
| Stage A `put`/`kv_put` | Legacy consensus-test surface, not a presentation product operation. It remains an explicitly allow-listed compatibility path so historical Stage A evidence can run. |

## 4. Identifier contract

| Field | Format | Creator and authority | Persistence |
|---|---|---|---|
| `node_uuid` | UUIDv4 | Replica bootstrap | Replica hard state |
| `user_id` | UUID | Pre-provisioned account catalog | Raft `auth.users` |
| `client_id` | UUID for Stage E external clients | Client installation; binding accepted by authenticated leader | Client local config/journal + Raft `auth.client_bindings` |
| `request_id` | Nonempty stable string (UUID recommended) | Client operation, including login | Journal where applicable and replicated dedup key |
| `session_id` | UUID | Client editor instance | Transmitted for element lock authority; lock state replicated |
| `credential_id` | UUID | Leader on token issuance | Raft `auth.active_credentials` |
| `credential_generation` | Strict positive int | Deterministic state machine increments | Raft auth state and login response |
| `access_token` | 32 random bytes, URL-safe encoded | Leader on login | Only client local secret storage and leader volatile pending response; **never** Raft persistence |

A `user_id` is not a token and never changes upon login. A `client_id` is not a user and never changes merely because an access token rotates. A `session_id` does not authenticate a user. Login and journal operations share one request-ID namespace per client so an authentication attempt cannot collide with a business intention.

For backward compatibility, `CommandEnvelopeDTO.client_id` may remain string-shaped for allow-listed internal actors and the legacy Stage A proof surface. The Stage E external authentication boundary validates product client IDs as UUIDs before constructing a protected command.

## 5. Pydantic v2 DTOs — normative fields

Use the project's existing `StrictDTO` base (forbid extras, strict typing), `UUID`, `StrictStr`, `StrictInt`, and `Literal`, with explicit schema versions. The following is a **structural contract**; it is not a drop-in import for the current `command_dto.py` until Stage D is integrated.

```python
from typing import Literal
from uuid import UUID
from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator
from dto_base import StrictDTO

class UserRecordDTO(StrictDTO):
    user_id: UUID
    first_name: StrictStr = Field(min_length=1, max_length=120)
    last_name: StrictStr = Field(min_length=1, max_length=120)
    email: StrictStr = Field(min_length=3, max_length=254)
    # Validate and canonicalize fixture email once (trim + lowercase).
    # Reject duplicate canonical addresses; do not claim RFC-complete validation.

class PasswordVerifierDTO(StrictDTO):
    algorithm: Literal["pbkdf2-hmac-sha256"]
    iterations: StrictInt = Field(ge=600_000)
    salt_hex: StrictStr = Field(pattern=r"^[0-9a-f]{32,64}$")
    hash_hex: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")

class StoredUserDTO(StrictDTO):
    profile: UserRecordDTO
    verifier: PasswordVerifierDTO

class CredentialRecordDTO(StrictDTO):
    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    credential_generation: StrictInt = Field(ge=1)
    revoked: StrictBool = False

class RetiredCredentialDTO(StrictDTO):
    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    credential_generation: StrictInt = Field(ge=1)
    retirement_reason: Literal["ROTATED", "REVOKED"]

class IssueAccessTokenPayloadDTO(StrictDTO):
    user_id: UUID
    client_id: UUID
    credential_id: UUID
    token_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")

class RevokeAccessTokenPayloadDTO(StrictDTO):
    client_id: UUID
    expected_credential_id: UUID
    expected_generation: StrictInt = Field(ge=1)

class AuthenticatedActorDTO(StrictDTO):
    user_id: UUID
    credential_id: UUID
    credential_generation: StrictInt = Field(ge=1)
```

The account fixture and normal command DTOs must reject unknown keys, `bool` values masquerading as integer versions, invalid UUIDs, invalid digests, duplicate emails/IDs, and malformed wire authorization fields. For a minimal pre-provisioned catalog, strict trimmed/lowercase email validation is sufficient; `EmailStr` would require explicitly adding and pinning `email-validator`, since `requirements.txt` currently only pins `pydantic==2.14.2`.

Additional protocol rules:

- The new `CommandEnvelopeDTO` has optional `authenticated_actor_user_id`, `credential_id`, and `credential_generation`. For all external authenticated presentation mutations, including Stage D settings commands, they are **mandatory and server-stamped**. `raft.noop`, internally generated lock expirations, catalog initialization, and the legacy Stage A proof operation have explicitly allow-listed non-user semantics. No generic fallback from `client_id` to `user_id` is permitted.
- `issue_access_token` is a trusted **password-admitted** internal command. It has `user_id` and client binding fields but no actor bearer reference, because no active bearer credential may exist on first login. The external login caller supplies neither `credential_id` nor `token_digest`: after successful password admission the current leader generates the credential UUID and raw random bearer, computes `SHA-256(raw_token)`, retains the raw bearer only in volatile response state, and places only the generated credential ID plus digest in the internal command. Authorization is proved by the leader's credential check, not repeated by followers. Followers validate its deterministic shape and account/binding invariants.
- `revoke_access_token` requires the current bearer on admission and its credential reference in the command. It targets a specific `expected_credential_id`/generation, **not** an unqualified "revoke whatever is active" (otherwise delayed logout could cancel a newer login). If credential changed first, return `STALE_CREDENTIAL` deterministically.
- For authenticated **business mutations**, request fingerprinting includes account identity but excludes the raw bearer and transient credential reference/generation so the same logical business request can survive reauthentication. Account switching on one client cannot reuse previous request IDs to claim another actor. This exclusion does **not** apply to the internal `issue_access_token` command: its server-generated `credential_id` and `token_digest` are replicated verifier material and are intentionally part of that command's fingerprint, so changing either under the same `(client_id, request_id)` yields `REQUEST_ID_REUSE`. The raw bearer token is never fingerprinted or replicated.
- Auth-specific DTOs do not replace presentation DTOs or TLV element blocks. Authentication belongs in a small `auth_dto.py` and/or guarded command variants, not inside renderable element configurations.

## 6. Replicated state and initialization

Normative auth-state schema **v2** (the frozen `AccountCatalogDTO` fixture remains independently versioned as schema v1). New states and committed upgrades must include the required retirement map:

```json
{
  "schema_version": 2,
  "catalog_fingerprint": "<64-hex canonical fixture digest>",
  "enforced_from_index": 1,
  "users": {"<user-uuid>": {"profile": {}, "verifier": {}}},
  "email_index": {"sofia@example.test": "<user-uuid>"},
  "client_bindings": {"<client-uuid>": "<user-uuid>"},
  "active_credentials": {"<client-uuid>": {"user_id": "<user-uuid>", "credential_id": "<uuid>", "token_digest": "<64-hex>", "credential_generation": 1, "revoked": false}},
  "retired_credentials": {"<retired-credential-uuid>": {"user_id": "<user-uuid>", "client_id": "<client-uuid>", "credential_id": "<retired-credential-uuid>", "token_digest": "<64-hex>", "credential_generation": 1, "retirement_reason": "ROTATED"}}
}
```

Each map is a JSON-serializable canonical projection. `client_bindings` and `active_credentials` are keyed by `client_id`, whose registration binds it to one `user_id`; the conceptual cardinality is one current credential per `(user_id, client_id)`. `retired_credentials` is keyed by its credential UUID and never deleted. Both credential IDs and digests are unique across the entire current+retired set. Rotation retires the replaced record with `ROTATED` or, if already revoked, `REVOKED`. Active revoked records remain present until rotated. Deterministic issuance checks credential ID before digest, regardless of insertion order.

`enforced_from_index` is the first committed log index at which external presentation mutations require a server-stamped actor. It is `1` for a fresh Stage E genesis. Applying `initialize_auth_catalog` to an existing cluster at index `N` sets it to `N + 1`, allowing older actorless Stage A–D entries to finish deterministic replay without allowing a new actorless mutation after activation. The state machine receives `committed_index` through the existing apply seam and enforces this boundary before business mutation.

**Fresh cluster:** build one deterministic, versioned fixture file containing all account UUIDs, canonical emails, password verifiers, and fixture digest. Copy the **identical** file to all replicas. `App.start` validates it and supplies a deterministic initial FSM only when `NodeDiskPersistence.load()` reports true disk genesis; the Raft engine must not overwrite an existing snapshot. **Existing Stage D cluster:** a leader-local initializer proposes one idempotent internal `initialize_auth_catalog` command whose request ID is derived from the catalog fingerprint. Login remains unavailable until the command is majority committed and applied. The bounded catalog command must fit the existing `LogEntryDTO` size limit and preserve `kv`, Stage D presentation schema v2, decks, settings revisions, tombstones, locks, and deduplication state byte-for-byte outside `fsm.auth`.

An existing, persisted auth-state schema **v1** is validated without adding v2 fields and remains **UPGRADE_PENDING**, not login-ready, until the leader commits `upgrade_auth_schema_v2`. This internal command is keyed by the configured catalog fingerprint, has a deterministic collision-resistant request identity, and preserves the catalog and all existing user/client/credential fields, generation counts, revoked flags, and original enforcement index. It changes only `fsm.auth.schema_version` to 2 and initializes an empty retired history. If legacy active records reuse an ID/digest, the command rejects with `AUTH_SCHEMA_MIGRATION_CONFLICT` and preserves the snapshot; operators must resolve conflicting records before proceeding. A repeated committed upgrade returns `AUTH_SCHEMA_ALREADY_V2`. Because schema v1 did not retain previously rotated-away secrets, the lifetime guarantee begins with the surviving v1 records at this migration boundary and all subsequent issuance; it does **not** retroactively establish non-reuse for irretrievably lost v1 records. Older v1 entries may be replayed before the upgrade in their original log order. All newly authenticated admission remains unavailable until v2 is ready.

Never add accounts by opportunistically rewriting a follower's `fsm` on restart. Each node compares the persisted catalog fingerprint with its configured fixture and disables authentication on mismatch. Ordinary preserved-disk restart restores auth state as-is; a new `cluster.py up` deliberately resets the entire run directory.

Existing request-result records created before `enforced_from_index` have no trustworthy actor provenance. They remain durable barriers against reapplication, but Stage E does not expose them as authenticated successes or infer an actor from `client_id`; reusing such an ID returns a stable legacy/reuse rejection and the user creates a new authenticated request. Post-activation exact-ID retries retain the normal actor-aware result behavior.

No account registration/write endpoint is implemented. A future account-management stage may extend this schema, but tests must not assume dynamic users can be created now.

## 7. Wire protocol: ephemeral credentials and redacted histories

The instructor's `node.py` forwards only `req.args` and cannot see HTTP Authorization headers in `App.on_request`. Preserve the instructor runtime by reserving a **wire-only**, adapter-stripped field, e.g. `_auth`, in `args`. It is **not** a command payload field. Sample conceptual wire requests:

```json
{
  "id": "login-attempt-uuid",
  "client": "<stable-client-uuid>",
  "op": "login",
  "args": {"email": "sofia@example.test", "password": "<transient-password>", "client_id": "<same-stable-client-uuid>"}
}
```

```json
{
  "id": "edit-request-uuid",
  "client": "<stable-client-uuid>",
  "op": "update_element",
  "args": {
    "_auth": {"access_token": "<transient-bearer-secret>"},
    "deck_id": "<deck-uuid>", "session_id": "<editor-uuid>",
    "element_id": "<element-uuid>", "fencing_token": 3,
    "expected_version": 7, "patch": {"content": "Revised"}
  }
}
```

Required implementation contract:

1. Add an `AuthenticatedClient` or equivalent new module; do not edit the starter-owned `client.py`. It owns ephemeral password/token bytes, accepts an explicit request ID for journal sends, and installs credentials **only immediately before** the raw HTTP request. Its history remains checker-compatible and redacts before serialization. Do not create a second independent retry protocol for ordinary commands.
2. `JournaledClient.enqueue()` receives only domain `arguments` and recursively rejects reserved authentication keys such as `_auth`, `password`, and `access_token`; `ClientIntentionOutbox` retains original request IDs through token rotation. The request sender must place that exact ID in the HTTP `id` field. A login attempt is **not** an offline journal intention.
3. The client-history writer must redact secrets before serializing **anything**. The server's initial `app.on_request` diagnostic logging must also sanitize `req.args` before payload fingerprints or DEBUG payload emission. Do not rely on the existing `Client.call()` path with raw password in `args`—it currently logs all supplied args.
4. On receipt, copy and strip reserved wire auth before constructing the existing `CommandEnvelopeDTO`; reject unexpected auth fields or caller attempts to set actor identity. If authentication fails, do not append a protected business command.
5. Never place raw bearer tokens in `self.disk`, `request_results`, `events.log`, `.jsonl` histories, `client_journal`, debug logs, checker traces, or exception strings. The only legitimate durable client secret location is a caller-configured client-state directory outside the disposable `run/` tree. The directory and token file use restrictive permissions and atomic replacement; the token store is **not** the intention journal and is not archived by `cluster.py down`.
6. The current plain HTTP transport means network confidentiality is absent. This is not a production security promise; document the limitation and use only classroom credentials.

## 8. Login protocol

### 8.1 Leader admission

1. Client loads its durable installation `client_id`, submits email/password, and keeps at most one active login attempt per client.
2. A follower redirects to the leader. Before consulting the account catalog, the leader completes a **fresh quorum-confirmed Auth Admission Read** using the existing ReadIndex mechanism and waits until its auth state is applied through that index. An isolated former leader cannot verify a password, expose a dedup result, or issue an acknowledged token.
3. After the barrier, the leader normalizes and looks up the email, then sends a copied verifier and bounded password bytes to a bounded background password-verification worker. The worker performs `hashlib.pbkdf2_hmac("sha256", ...)` and `hmac.compare_digest` but never reads or mutates Raft state. The pending admission records the leader term; the node's single event-loop thread consumes the result only if leadership and term still match, otherwise it discards the result and redirects or returns unavailable. Enforce input byte-size bounds and basic retry/backoff limits so the CPU-intensive verifier cannot block Raft ticks or create unbounded workers.
4. On success, validate the client binding: unbound client UUID may be attached to that authenticated account; already-bound client UUID must match the account. Reject attempts to bind the same UUID to another user without an explicit later-stage account/device migration protocol.
5. Generate 32 secure random bytes, URL-safe encode them, generate `credential_id: UUID`, compute `sha256(raw_token)` (lowercase 64-character hex), and retain the raw token **only** in a volatile pending-login table owned by this leader incarnation.
6. Propose typed `issue_access_token` with the new digest. Persist via the normal Raft mechanism. A first attempt creates the binding and current record atomically; subsequent login atomically retires the previous record into `retired_credentials`, replaces digest/ID, and increments generation (no CAS of old token needed). Both the new ID and digest must not occur in any current or retired record, even for another client. `CREDENTIAL_ID_REUSE` takes precedence over `TOKEN_DIGEST_REUSE`; either outcome leaves the entire state unchanged.
7. After the command is committed, applied, and its result recorded durably, reply `AUTHENTICATED` with `user_id`, `client_id`, new `credential_id`, `credential_generation`, and the **volatile** raw token. Do not return token before commit; do not include it in the durable result object.

### 8.2 Response-loss handling

- Before generating random token material, inspect replicated request results for the `(client_id, request_id)` key. A prior safe issuance result without a matching volatile pending secret returns `LOGIN_SECRET_UNAVAILABLE`; it must not construct a new digest and accidentally return `REQUEST_ID_REUSE`.
- A duplicate delivery of the *same* login request ID within the leader's pending-login scope must reuse that pending token or explicitly return a recoverable login status; it must not create new random data under an already-used request identity.
- If issuance committed but the leader crashed before delivering the raw token, the successor has the digest **but cannot reconstruct the secret**. The successor returns `REAUTH_REQUIRED`/`LOGIN_SECRET_UNAVAILABLE` for that exact attempt (not a fake successful token response).
- The client prompts "Logging in is taking longer than usual…", then performs a **new** password-authenticated login with a **new request ID** and the **same** client ID. Use bounded exponential backoff and a user-visible cancellation action; do not loop indefinitely in a tight network retry.
- Success rotates the token, replaces the old credential, and returns a strictly increased generation. The client installs the new secret only if its generation is not older than the latest locally accepted generation; delayed old responses must not overwrite it. If an earlier secret is invalidated by a later committed login, reauthenticate.
- Existing offline intention request IDs are **never regenerated** merely because a bearer token was rotated; a terminally rejected command may require user-directed reconciliation/new ID under the established journal rules.

### 8.3 Proposed login result example

```json
{
  "ok": true,
  "result": {
    "status": "AUTHENTICATED",
    "user_id": "11111111-1111-4111-8111-111111111111",
    "client_id": "22222222-2222-4222-8222-222222222222",
    "credential_id": "33333333-3333-4333-8333-333333333333",
    "credential_generation": 2,
    "access_token": "<secret-only-in-wire-response>"
  }
}
```

The `access_token` is assembled at the leader's response layer, **not** serialized into `ClientResultDTO` or request dedup results.

## 9. Authenticated operation and deterministic replay

For every bearer-authenticated request, the leader first completes a fresh Auth Admission Read and applies through its confirmed index. Only then may it verify the raw token from the ephemeral transport, compute its digest, match the active credential plus `client_id` binding, or disclose an existing deduplication result. Upon success it constructs a command with server-stamped `authenticated_actor_user_id`, `credential_id`, and `credential_generation`. Followers validate the command structure, never see the raw token, and apply in normal Raft log order.

**Before any business mutation** (and after the existing committed request-result deduplication path has established first application), the deterministic state machine checks:

```python
# Illustration of the required guard, not a substitute for typed DTOs.
record = auth.active_credentials.get(command.client_id)
if (record is None or record.revoked
        or record.user_id != command.authenticated_actor_user_id
        or record.credential_id != command.credential_id
        or record.credential_generation != command.credential_generation):
    return ClientResultDTO(status="UNAUTHENTICATED")
# Then run existing lock/version/structure/settings operation checks.
```

If `revoke` or `rotate` is committed earlier than the business write, an old credential reference fails; a write earlier in the Raft log can succeed. Merely admitting a command before revocation does not guarantee that command can mutate after revocation. Never use wall-clock time to establish this order.

An exact-ID retry of a **post-activation committed** operation returns its stored deterministic result under a currently authenticated client, without reapplying the operation. Stage E must distinguish this case from a new command arriving after revocation. Request fingerprint includes authenticated actor identity and canonical business operation; bearer token and credential-generation fields do not change the logical fingerprint. A pre-activation dedup record is never retroactively attributed to a User Account.

For Stage D settings commands, the auth guard runs before `expected_deck_settings_version` or `expected_slide_settings_version` evaluation. A committed `UNAUTHENTICATED` denial changes no structural or settings revision and stores a terminal result for that request ID. A later settings intention in the same FIFO lane derives its precondition from the last successfully committed settings result, not from the denied predecessor.

At the transport/outbox boundary, invalid or missing credentials detected **before proposal** return `ok: false` with `REAUTH_REQUIRED` (or raise the equivalent request-sender signal). The Authenticated Client reauthenticates, and the unchanged journal intention retries with its original request ID. `UNAUTHENTICATED` with `ok: true` is reserved for a denial that was committed and applied in log order; that request ID cannot be converted into a success by retry and a new business attempt needs a new ID.

### Revocation

- Public `revoke_access_token`/`logout` revokes **this client's current token**. It requires valid current auth at admission and an `expected_credential_id` plus `expected_generation` matched at apply. If already rotated, return `STALE_CREDENTIAL` and do not revoke the newer login.
- Applying a valid revocation sets `revoked=true` and increments the generation or writes an equivalent fenced tombstone. Return deterministic `TOKEN_REVOKED` with safe IDs/revision, after majority commit and durable application.
- Existing tokens remain invalid across leader failover and restart. A fresh password login may establish a new credential generation on that bound client.
- If there is no quorum, issuance/revocation returns uncertain transport/quorum failure; it must never be reported `AUTHENTICATED`/`TOKEN_REVOKED` as a committed result.

## 10. Outcome and error contracts

| Status / condition | `ok` | Dedup / mutation semantics |
|---|---|---|
| `AUTHENTICATED` | `true` | Only response assembled after committed `issue_access_token`; raw secret volatile only |
| `INVALID_CREDENTIALS` | `true` or documented uniform auth failure | No auth issuance proposed; do not distinguish missing email vs wrong password |
| `TOKEN_REVOKED` | `true` | Revoke result committed and persisted exactly once |
| `UNAUTHENTICATED` | `true` only for a deterministically committed at-apply denial | No business mutation or version increment; the request identity is terminal |
| `STALE_CREDENTIAL` | `true` | Deterministic revoke/credential conflict; newer credential remains active |
| `REQUEST_ID_REUSE` | `true` | Same logical ID with different fingerprint; no new mutation |
| `REAUTH_REQUIRED` | `false` | Pre-proposal credential failure; an existing business intention remains pending and retries with the same ID after login |
| `LOGIN_SECRET_UNAVAILABLE` | `false` | A committed login cannot supply its lost volatile token; client submits a new login attempt ID with the same `client_id` |
| `NO_LEADER`, `QUORUM_UNAVAILABLE`, timeout | `false` or redirect | Uncertain; never claim issuance/revocation committed |

Avoid returning raw user verifier, credential digest, or distinguishable account-existence errors in any response.

## 11. Deterministic acceptance — tests and required assertions

**Unit and in-process component tests** (use existing `unittest` conventions):

1. Fresh fixture has fixed UUIDs, unique normalized emails, identical frozen verifier records and catalog fingerprint on all three nodes.
2. Fixture password checks accept the test password and reject wrong password; PBKDF2 verifier metadata is persisted with correct salt/hash lengths; no plaintext in `fsm`.
3. Invalid UUID, extra DTO field, bool-as-integer, incorrect digest, reused client ID under another account, and malformed fixture fail deterministically.
4. First issue binds account to client, returns generation 1; re-login rotates atomically with generation 2; old digest stops authenticating.
5. Revoke current credential succeeds once; duplicate exact-ID revoke does not increment twice; stale revoke cannot invalidate a newer credential.
6. `update before revoke` may succeed; `revoke before update` deterministically rejects the later operation and leaves element/settings versions unchanged. Repeat for a token rotation before an old-token edit.
7. Repeat the log-order guard for `update_deck_settings` and `update_slide_settings`: committed auth denial leaves all Stage D revision domains unchanged, while a valid actor still receives the original resource-specific settings conflict behavior.
8. Element lock session/fencing checks still work after token validation; an authenticated account does not inherit another `session_id`'s lock.
9. Exact-ID retry under a new valid token returns the original committed result once; same ID with a different actor or payload produces `REQUEST_ID_REUSE` (or the defined account-binding rejection).
10. Actorless entries below `enforced_from_index` remain replayable during preserved-disk catch-up; actorless external mutations at or above the boundary are rejected before business mutation. Pre-activation dedup results cannot be disclosed as authenticated successes.
11. An old revoked token cannot retrieve a stored committed result or induce a new proposal; a new authenticated token may retrieve an authorized client's stored result without mutation.
12. A committed login with lost volatile token returns a recoverable login response, never a fake token; reauthentication under a new ID rotates and returns a usable token.
13. Delayed login response generation 1 cannot overwrite generation 2 in the client's credential store.
14. Client restart preserves `client_id` and journal IDs; raw token is stored only in the dedicated client credential store outside `run/`, never a journal event.
15. Diagnostic scanner rejects literal test password, literal bearer token, email/password combinations, and corresponding unredacted wire field values in `run/*.jsonl`, archived histories, `events.log`, persisted Raft state, request dedup records, and DEBUG logs.
16. An isolated former leader cannot validate a bearer, disclose a stored result, or issue a token because its Auth Admission Read cannot obtain a fresh quorum.
17. A bounded password-verification worker does not block heartbeats, elections, message processing, or ticks, never mutates Raft state from its worker thread, and cannot propose after its captured leader term becomes stale.
18. Local Eventual Reads and polling retain Stage D behavior without bearer authentication; tests make clear that Stage E provides no deck privacy.
19. Old Stage A–D tests continue to pass; internal `raft.noop`, catalog initialization, legacy `kv_put`, and leader-proposed lock expiration obey explicitly identified internal actor rules.

**Stage E.A live acceptance gate — independently clean three-replica runs**:

| Scenario | Failure injection and proof |
|---|---|
| Normal login and command | Pre-provisioned users; login returns valid credential; client UUID and request ID preserved; authenticated edit and Stage D settings changes retain their normal version/lock rules. |
| Rotation and revocation | Login twice on one client; older token rejected, later token accepted; revoke and confirm no subsequent new edit applies; retry ID does not double-rotate/revoke. |
| Follower lag, leader failover | n1 and n3 durably replicate `issue_access_token` while n2 lags; n1 fails; n3 wins; new leader applies token record and authenticates with it; n2 catches up after recovery. |
| Lost login response | Kill/restart leader after committed issuance but before token delivery; new login ID and stable client ID; generation increases; no bearer secret available in the old result. |
| Network partition and concurrent revoke/write | Minority cannot commit issuance/revocation; majority can. Exercise both Raft orders and show correct version/credential outcomes; heal and verify convergence on all three nodes. |

**Evidence**: Archive scenario manifest, clean run ID, raw and safely redacted client histories, checker-compatible projection where applicable, deterministic reference outputs, role traces, fault records, `events.log`, client credential-generation trace **without secrets**, and final `auth` digest summaries on all replicas. For nondeterministic issuance, do not compare random secrets in `spec.py`; model and check the committed non-secret state transitions. Measure p50/p95 password-authenticated login, quorum-confirmed token issue/revoke, and authenticated edit latencies separately; compare with unchanged Stage D baseline.

## 12. Suggested implementation sequence and completion gates

1. **E.0 — Catalog and DTOs:** Introduce `auth_dto.py`, bounded versioned fixture builder and fingerprint, `fsm.auth` schema, deterministic fresh-state initializer or internal Raft catalog migration, and tests. Preserve the complete Stage D FSM outside `auth`; no external DB.
2. **E.1 — Quorum-confirmed login issuance:** Implement the asynchronous Auth Admission Read, bounded background password verifier, volatile raw-token store, typed `issue_access_token`, Raft commit/apply, safe login response, and lost-response/new-ID recovery.
3. **E.2 — Client identity and secret-free transport:** Implement a new `AuthenticatedClient`, persistent local `client_id` and credential storage outside `run/`, explicit wire request IDs, reserved `_auth` field, redacted histories/events, and `JournaledClient` request-sender compatibility. Do not change instructor-owned `node.py` or `client.py`.
4. **E.3 — Protected command authorization and revocation:** Add quorum-confirmed bearer admission, server-stamped actor/credential command fields, the at-apply guard for Stage B–D mutations, fenced `revoke_access_token`, exact-ID behavior, outbox reauthentication behavior, and internal-command exceptions. Update `spec.py` reference model.
5. **E.A — Stage-transition gate:** Run deterministic suite, old Stage A–D regressions, five independent live scenarios, secret-leak scanner, invariant/convergence evidence, checker projections, and latency measurements. Stage E authentication is not complete until this gate passes.

A task is not done when only the leader reports a successful token: it is done when the relevant Raft command is committed, deterministically applied, persisted, and behaves correctly under failover and retry.

## 13. Stage F decisions deliberately excluded from Stage E

1. Deck ownership association: whether a published deck must have a server-stamped owner account UUID and whether private decks remain exclusively client-local until publication.
2. Sharing relationship DTO and roles: owner/editor/viewer, grant/revoke semantics, permissions for structural/settings/element/read operations, and whether a user may share further.
3. Permission checks in **both** command admission and deterministic apply; rules for private versus shared deck reads, lock state, and outbox behavior after access revocation.
4. User listing/lookup API and how to avoid exposing email/profile data to other accounts.
5. Administrative override or revocation of *other* users' client credentials.

These are **not** silently specified or implemented by the authentication-foundation documents. Stage F must also decide how to assign ownership to decks published before ownership exists; Stage E records authenticated command actors but does not treat command-log provenance as deck ownership.

## 14. References

- Repository: https://github.com/Volv1t3/Distributed-HTML-Slide-Deck
- Stage D/D.1 issue #28: https://github.com/Volv1t3/Distributed-HTML-Slide-Deck/issues/28
- ADR-004: `docs/adrs/ADR-004-intent-preserving-client-outbox.md`
- ADR-007: `docs/adrs/ADR-007-quorum-confirmed-authoritative-recovery-reads.md`
- OWASP Password Storage Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
- OWASP Session Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
