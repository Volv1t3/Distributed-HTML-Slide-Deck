# ADR-002 — Integrating Lab 5 Raft with the CMP5001 Project Starter

- **Status:** Proposed for acceptance; Stage A implementation pending
- **Date:** 2026-11-22
- **Scope:** Consensus infrastructure, message contracts, node identity, persistence, validation boundaries, and integration ownership
- **Supersedes:** No accepted ADR. Complements **ADR-001 — Element-Level Distributed Locking**.
- **Related:** US-01 through US-08; `Stage-A-Raft-Implementation-Spec.md`
- **Source code inspected:** `LaboratorioCinco-SantiagoArellano-00328370.zip` (`raft_core.py`, `test_student.py`, `README.md`, `escenario.py`) and `cmp5001-proyecto-starter.zip` (`node.py`, `app.py`, `cluster.py`, `client.py`, `spec.py`, `check.py`, `chaos.py`).

## 1. Context

Distributed Slides is a local-first presentation editor. Shared decks are authoritative in a **single Raft-replicated state machine**. Selection-triggered element locks, updates, publication, renewals, and expirations become effective in committed log order; local slide/element previews may be stale (ADR-001).

The Lab 5 submission supplies useful Raft *decision rules* in `raft_core.py` but is not a runnable, communicating Raft cluster. The instructor starter supplies an HTTP-based replica runtime, a local disk API, a client with histories and redirects, fault injection, and a checker. Its example `app.py` deliberately does not replicate anything. Stage A must connect these pieces **without transplanting Lab 5's separate runtime or replacing the starter's message/failure semantics**.

### 1.1. Verified interfaces and reusable work

| Existing artifact | What it actually provides | Stage A decision |
|---|---|---|
| Lab `Entry(term, command: str)` | Immutable term/command record | Preserve the conceptual `term + command`; migrate command to validated JSON object and make entry index explicit in wire/storage DTOs. |
| Lab `Node` | In-memory term, vote, role, log, `commit_index`, `last_applied`, `match_index` | Refactor to a pure Raft engine state object; remove hardcoded membership and initialize volatile leader fields only on election. |
| Lab `request_vote(...)` | Term transition, one vote per term, up-to-date log check | Reuse algorithm; persist term/vote **before** replying; adapt membership and incoming-message validation. |
| Lab `advance_commit(...)` | Current-term-only majority commit | Reuse algorithm; replace `MEMBERS={A,B,C}` with configured voter set; include leader's own durable entry. |
| Lab `receive_ack(...)` | Higher-term stepdown and match-index advancement | Reuse principles, not the raw handler: require known peer, correlated request, successful verified prefix and bounded index. |
| Lab `repair_uncommitted_suffix(...)` | Whole-log, term-based common-prefix repair for a lab model | **Replace as production repair algorithm** with Raft AppendEntries `prev_log_index/prev_log_term` and per-follower `next_index`. Keep helper only for lab tests/comparison. |
| Starter `node.Node` | `start`, `on_request`, `on_message`, `on_tick`, `status`; `send`, `reply`, `now`, `disk`, `save` | Subclass from `app.py`; implement only these hooks. |
| Starter `node.py` HTTP | `POST /msg`, `POST /client`, `GET /status`; one serialized callback loop | **Keep intact.** No FastAPI for replica-to-replica networking. |
| Starter `cluster.py` | Creates `n1..nN` transport slots and `NODES` endpoints; Docker or `--local` | Use it as the configured voter registry; never hardcode node names or N=3 in the engine. |
| Starter `client.py` | `req.id`, `req.op`, `req.args`, `req.client`, leader redirect, `.jsonl` history | Honor its request/retry/response conventions. |
| Starter `check.py` and `spec.py` | Linearizability and convergence against a deterministic `apply` function; leader-term logs | Make Stage A key-value tests checkable and Stage B commands modelable. |

Lab helper unit tests are useful regression tests, **not evidence** of election correctness, log safety during partitions, or durable replicated state. The Lab archive refers to a `runtime` helper not included in that archive; it must not be copied as the project's runtime.

## 2. Decisions

### D1 — Preserve the instructor's runtime; split protocol from application

Implement a **pure Raft engine**, an **adapter in `app.py`**, a **deterministic state machine**, and a **typed DTO layer**. The Raft engine only knows terms, log entries, commands encoded as canonical JSON data, votes, replication, commitment and application callbacks. It must never import `presentation`, `element`, `lock`, or UI types.

The command interpreter decodes typed operation payloads after Raft decides when entries are applied. Pydantic validation at the client/replica boundary is separate from Raft safety. Validated canonical envelopes are what the log stores.

### D2 — Generated durable node identities with configured transport membership

**Never hardcode `A/B/C` or `n1/n2/n3` into Raft logic.** Each fresh replica creates a full UUIDv4 `node_uuid` and persists it **before participating in an election**. On an ordinary restart with disk intact, it reuses the same UUID. Peers exchange `RegisterNode`/`Hello` messages to learn `transport_slot -> node_uuid` bindings and reject contradictory identities within an incarnation.

**Compatibility boundary:** the unchanged starter *already* defines voter transport slots (`self.nodes`, `self.peers`, `NODES`) and its client/chaos tooling refers to those slots, e.g. `n1`. Registration binds a generated logical identity to each configured slot; it **does not dynamically add voting members**. The configured slot list defines quorum and the network transport. True consensus membership changes require a separate Raft joint-consensus design and are **out of Stage A**. Do not represent a newly registered UUID as a new voting member without such a protocol.

- `transport_slot`: starter-provided routing label and redirect target, not user/device identity.
- `node_uuid`: generated, stable persisted replica identity.
- `incarnation`: locally persisted/monotone restart generation for diagnostics; not an election term and not a substitute for Raft safety.
- `actor_id`, `session_id`, `element_id`, `request_id`: application identities, **not** Raft voter IDs.
- A wiped disk is **not** an ordinary restart: losing `voted_for` can permit a second vote in a term. An erased-voter rejoin is quarantined/outside the baseline failure model until a separately specified safe reintroduction protocol exists. The professor's erased-disk scenario must be disclosed and demonstrated as a limit if not solved.

### D3 — Standard Raft log replication, not whole-log overwrite

Implement RequestVote and AppendEntries with explicit `prev_log_index`, `prev_log_term`, `entries`, and `leader_commit`; persistent follower log consistency checks; leader `next_index`/`match_index`; retries after negative acknowledgments (optionally conflict hints); and a stable committed prefix. Empty AppendEntries are heartbeats and **must not truncate** extra follower entries. A follower never overwrites a committed entry. An RPC acknowledgment is evidence only for the exact leader term and entry range it acknowledges.

Raft's **log index** establishes a total order; UUIDs or timestamps do not arbitrate concurrent commands. Element `version` checks and fencing tokens remain **application-level** atomic preconditions at apply time.

### D4 — Persist safety-critical state before acknowledging

Use `self.disk` and `self.save()` supplied by `node.py`; do **not** introduce shared state, a separate database replicator, or independent disk files as a second authority.

Persist `node_uuid`, `current_term`, `voted_for`, log entries, and any durability-required commit/apply metadata and state-machine/dedup snapshot. A vote reply follows persistence of the vote; an AppendEntries success reply follows persistence of appended/rewritten entries. A client receives `ok:true` for a write only after the corresponding command is **committed, applied, and durably represented** (replayable log or consistent snapshot). Uncertain responses may be retried with the same request ID.

The starter's `save()` writes a temporary JSON file, `fsync`s it and uses `os.replace`; this is suitable for **process kill/restart** demonstrations. It does not explicitly `fsync` the parent directory, so do **not** claim full power-loss/crash-consistent storage semantics beyond the starter's provided mechanism.

### D5 — Versioned, extensible JSON command envelope with typed TLV-like blocks

Choose a canonical, versioned JSON envelope containing stable identifiers, request metadata, operation kind, preconditions, and an array of `{type_id, schema_version, required, length_bytes, value}` blocks. Each block is validated against a Pydantic v2 model registered by type/version. This is **TLV-inspired**, not an interoperable binary TLV wire format. Length is the UTF-8 length of the block's canonical JSON `value`, not a second untrusted framing mechanism.

The initial registry covers generic key-value test commands and reserved presentation/geometry/text/shape/lock block codes. **All voting replicas in Stage A must run compatible code/schema versions.** Unknown mandatory blocks and unknown semantic command versions are rejected **before append**; known optional metadata may be ignored only by an explicitly documented deterministic rule. Do not accept arbitrary executable Python objects, dynamic imports, or raw HTML/script content in a replicated command.

Validation occurs at two distinct points:

1. **Leader admission:** reject malformed, unauthorized or unsupported client operations before proposing an entry. A malformed operation must not change the log or state.
2. **Every replica's deterministic apply:** validate the persisted canonical envelope again; a validation failure for a committed log entry is a **fatal consistency fault**, not a silently skipped operation. Application-level conditional rejections (e.g. `LOCKED`, `VERSION_CONFLICT`) are valid deterministic results of a committed command and must be replicated/deduplicated.

No application semantics may depend on local wall-clock timestamps, local UUID generation, or unordered map iteration at apply time. Time and random IDs may appear as **precomputed data** in the committed command.

### D6 — Use Pydantic for DTO validation, not FastAPI for consensus transport

Adopt **Python >=3.11** and **Pydantic v2** for strict DTO validation in this project. The starter currently has **no third-party dependency installation** and its Docker image is `python:3.12-slim` with no installation step. Therefore:

- Stage A's supported development/test execution uses a Python virtual environment containing the pinned Pydantic dependency.
- Stage A.1 builds a project Docker image from the pinned requirements and uses it through the existing cluster launcher. The image supplies Pydantic without changing `node.py`'s HTTP routes, callbacks, fault injection, or command-line contract.
- Do not replace `node.py`'s HTTP server with FastAPI/Uvicorn: that would bypass the starter's fault injection, request waiting, and checker-oriented status behavior. A **separate optional browser API** may later use FastAPI without changing Raft transport.
- If the course disallows additional runtime packages, the dependency decision must be revisited explicitly and equivalent stdlib validators specified; do not silently fall back to unvalidated commands.

### D9 — Per-node operational diagnostics are not consensus state

Every replica writes append-only structured JSON operational records using local MDC-style context. `OPERATIONAL` mode records identifiers, fingerprints, outcomes, and error codes; startup-selected `DEBUG` mode may include full payloads. These records are local diagnostics only: they are neither replicated nor used to recover Raft or state-machine authority. Cluster shutdown archives them with the existing role and history evidence.

### D7 — A small Raft-neutral Stage A state machine proves integration first

Before any presentation-specific commands, implement a deterministic replicated `kv_put`/`kv_get` and an internal current-term `raft.noop`. `kv_put` travels through the exact command envelope, deduplication, persistence, election, and replication paths that Shared Slides will use later. `kv_get` is a local/eventually consistent preview, not a linearizable read. This prevents UI/domain complexity from masking consensus defects. The complete application DTO registry and editor operations are staged for **Stage B**, but their schema boundaries and reserved block identifiers are specified now.

### D8 — One committed ordering and deterministic retry results

Deduplication is keyed by `(req.client, req.id)` (or a fully specified equivalent session/request pair), with a canonical hash of operation name and arguments. Retrying an identical logical request must not update state twice. Reusing a request ID with different content returns a deterministic `REQUEST_ID_REUSE` rejection. Because the existing `client.py` uses labels plus only eight hex UUID digits, the adapter **accepts** its IDs; new browser clients generate complete UUIDv4 values. The Raft log, not request-ID lexical ordering, determines mutation order.

## 3. Structural responsibility map

```text
Existing professor runtime (unchanged)
  node.py: HTTP /msg,/client,/status; serialized callback loop;
           send/broadcast/reply/now; disk/save; fault injection
      |
      v
app.py — RaftAdapter(node.Node)
  start()      -> restore persistent engine + deterministic FSM
  on_request() -> validate client DTO; read locally OR propose to leader
  on_message() -> validate protocol RPC; dispatch to Raft engine
  on_tick()    -> election/retry/heartbeat/apply; later lease expiry
  status()     -> {role, term, leader_id?, commit_index?, last_applied?}
      |
      +-- raft_core.py -> RaftEngine (no deck/lock imports)
      +-- raft_dto.py  -> RequestVote, AppendEntries, Reply, Entry DTOs
      +-- command_dto.py / tlv_registry.py -> Pydantic command envelope
      +-- state_machine.py -> deterministic KV first, slides later
      +-- persistence.py -> serialize through self.disk + self.save()
      +-- spec.py -> checker reference model for exposed operations
```

**The adapter must preserve these starter response rules:**

- Non-leader write: `{"redirect": "<known transport slot>"}` when leader known; otherwise `{"ok": false, "error": "NO_LEADER"}` or timeout.
- Committed application rejection: `{"ok": true, "result": {"status": "LOCKED"}}` (valid response, no retry required).
- Infrastructure failure/unknown commit result: `{"ok": false, "error": "QUORUM_UNAVAILABLE"}` or timeout (client may retry the same logical request).
- Initial or final local read with `args.local == true`: serve local applied state without redirecting; may be stale until convergence.
- `status()` retains `role` and integer `term`; these are inspected by `chaos.py` and `check.py`/`roles.log`.

## 4. Explicitly rejected alternatives

| Alternative | Why rejected for Stage A |
|---|---|
| Hardcode `A/B/C` or `n1/n2/n3` | Fails configurable cluster size and generated-identity requirement. |
| Make arbitrary registered UUIDs voting members immediately | Unsafe membership changes; starter topology remains configured. |
| Elect a winner by UUID/timestamp | Does not establish Raft's real-time linearizable commit order. |
| Apply the Lab 5 whole-log repair method directly | Lacks standard previous-index/term consistency and per-follower retries. |
| Write on leader and asynchronously notify followers | Can acknowledge edits/locks lost on leader failure. |
| Validate only on the leader and silently ignore unknown commands on followers | Risks divergent state machines. |
| Put lock semantics inside Raft core | Couples consensus safety to business logic and blocks reuse/testing. |
| Replace `node.py` with FastAPI | Breaks the grading starter's exact runtime/fault behavior for no consensus benefit. |
| Use a shared registry file as live consensus state | Violates the no-shared-state replica model. Bootstrap topology is configuration only. |
| Add snapshot installation and dynamic membership in Stage A | Substantial extra protocol; log replay suffices for three-node prototype with bounded histories. |

## 5. Consequences, boundaries, and risks

**Benefits.** Direct reuse of tested Lab 5 reasoning, unchanged instructor transport, composable structured commands, clear safety boundaries, visible test evidence, and a path to ADR-001/US-01–08 without repeating consensus work.

**Costs.** Correct AppendEntries, election timing, durable storage ordering, delayed acknowledgments, and exact-ID retries are nontrivial. Persisting an entire `disk.json` for every log append is acceptable for the small project but produces more IO and O(log-size) file rewrite work.

**Explicit limits.** Static voting membership; no automatic power-loss guarantees; no safe `start --empty` without reintroduction design; no Byzantine/malicious node protection; no production authentication; no HTTP body-size protection prior to starter parsing; no snapshot compaction initially. Local reads may be stale and are not suitable for acquiring editing authority. Automatic lease expiration belongs to Stage B and ADR-001, not Raft itself.

## 6. Acceptance criteria for ADR-002

1. `node.py`, `client.py`, `chaos.py`, and `check.py` still operate with unmodified APIs.
2. At least three configured replicas each generate and persist distinct full UUIDv4 identities; ordinary restart preserves identity; Raft quorum is calculated from configured voting slots.
3. At most one leader is elected **per term**; a new leader can be elected after one leader crashes.
4. A committed `kv_put` survives any one process failure, failover, and subsequent replica catch-up.
5. An isolated minority cannot acknowledge a new write; a connected majority can commit.
6. A follower with divergent uncommitted suffix converges via `next_index`/AppendEntries, never overwriting its committed prefix.
7. Every acknowledged vote, successful log append, and committed client result obeys the persistence/response ordering defined above.
8. Malformed DTOs, unknown required TLVs, bad terms/indexes, unknown sender slots, and incompatible protocol versions are rejected without state corruption.
9. Duplicate request IDs do not repeat a committed change; a conflicting duplicate is rejected deterministically.
10. `spec.py`, history files, and `check.py` demonstrate strong-write correctness, eventual local-read convergence, terms, and latency; additional invariant tests cover cases the checker cannot prove.
11. Stage B can register `publish_deck`, `create_element`, `update_element`, and lock commands **without changing the Raft log or message DTOs**.

## 7. Sources and downstream decision

**Source-derived:** the functions and file roles in §1.1, the starter's three HTTP routes, persistence API, local-vs-Docker launcher, `req` fields, configured `NODES` membership and checker behavior. **Design decisions:** every D-number, proposed DTO schema, persistent UUID registration and dependency policy. These are not pre-existing features of the uploads.

- Lab: `raft_core.py`, `test_student.py`, `README.md` in `LaboratorioCinco-SantiagoArellano-00328370.zip`.
- Starter: `node.py`, `app.py`, `cluster.py`, `client.py`, `spec.py`, `check.py`, `chaos.py` in `cmp5001-proyecto-starter.zip`.
- Professor: `CMP5001-Proyecto.pdf`, pp. 1–4 (three replicas, crash/partition/recovery tests, no externally managed consensus).
- Raft paper: https://raft.github.io/raft.pdf
- Pydantic v2 docs: https://docs.pydantic.dev/latest/concepts/models/
- Accepted domain ADR: `ADR-001-element-level-distributed-locking.md`.

**Implementation contract:** `Stage-A-Raft-Implementation-Spec.md`. ADR-001 stays authoritative for selection-triggered element locks. Where user stories show placeholder names or simpler example commands, the Stage A infrastructure contract governs Raft and transport; the domain ADR governs actual lock semantics.
