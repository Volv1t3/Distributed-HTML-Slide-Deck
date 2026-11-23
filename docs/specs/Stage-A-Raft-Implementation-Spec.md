# Stage A Core and Stage A.1 Runtime Integration Specification

- **Status:** Ready for implementation review; no code implemented by this document
- **Date:** 2026-11-22
- **Audience:** Implementation agent, reviewer, course defense
- **Related architecture decisions:** [`ADR-002-raft-core-and-starter-integration.md`](../adrs/ADR-002-raft-core-and-starter-integration.md), [`ADR-003-stage-a-core-and-a1-runtime-integration.md`](../adrs/ADR-003-stage-a-core-and-a1-runtime-integration.md), [`ADR-004-intent-preserving-client-outbox.md`](../adrs/ADR-004-intent-preserving-client-outbox.md)
- **Domain decision:** [`ADR-001-element-level-distributed-locking.md`](./ADR-001-element-level-distributed-locking.md)
- **Source of truth:** Uploaded Lab 5 `raft_core.py` and instructor `node.py`/`app.py`/`cluster.py`/`client.py`/`spec.py`/`check.py`/`chaos.py`

## 0. Objective, scope and deliverables

Implement a production Raft protocol for a fixed-membership replica cluster in two gates. Stage A proves the pure protocol core with a tiny key-value state machine. Stage A.1 proves that core in the supplied transport, persistence, fault, and checker environment. The same versioned command envelope and DTO registry must later support Distributed Slides presentation, element, and lock operations without changing Raft.

### Definition of Stage A

**Stage A — required core:** Raft election and voting, heartbeat policy, replication and retry, current-term commit rule and no-op, deterministic application, injected persistence/transport boundaries, Pydantic validation, generated identity models, request deduplication, and `kv_put` plus local `kv_get` proof commands. Completion requires deterministic unit and component evidence.

**Stage A.1 — required integration:** the `app.py` Raft adapter over the unchanged starter callbacks, persistence through `self.disk`/`self.save()`, durable slot/UUID registration, redirects, RPC correlation, structured per-node operational logs, the Pydantic Docker image, compatible `status`/`spec.py`, and live fault evidence from the local and container clusters.

**Designed now, implemented in Stage B:** `publish_deck`, `create_element` with initial lock, `acquire_lock`, `update_element`, `release_lock`, `renew_lock`, `expire_lock`, typed slide/text/circle/rectangle payloads, eventual UI syncing. Their future DTOs and reserved TLV codes are included to prevent protocol redesign.

**Not Stage A or A.1:** FastAPI server replacing `node.py`; full editor UI; dynamic Raft membership/joint consensus; compacted snapshots/InstallSnapshot; safe automatic restart from erased Raft metadata; production-grade authentication or full power-loss safety. Stage B owns the intent-preserving client outbox and presentation state-machine behavior defined by ADR-004.

### Non-negotiable constraints from instructor runtime

1. Keep `node.py` and its routes (`POST /msg`, `POST /client`, `GET /status`) unchanged. Do not spin up a second HTTP server in a replica.
2. `node.py` serializes callbacks. All mutable Raft and FSM state is touched only by `start`, `on_request`, `on_message`, `on_tick`. HTTP sender threads only enqueue outgoing JSON; messages may be delayed, lost, duplicated or reordered.
3. `self.send(dst, dict)` uses a **transport slot** in `self.nodes`; it does not create a Raft member. HTTP clients likewise use those slots for redirects.
4. `self.disk` is already loaded from `run/<slot>/disk.json`, and `self.save()` atomically rewrites that JSON. The app must persist before a success acknowledgment where required.
5. The status contract must include `role` and integer `term`; `chaos.py` uses it to discover the leader and `node.py` writes changes to `roles.log`.
6. The starter client sends `Request(id, op, args, client)`, not a Pydantic object. Normalize those fields **inside** `on_request`. Reuse `req.id` on retries.
7. `client.final()` sets `args.local=true`, requiring a local read from **each** replica without redirect; use it after catch-up to test convergence.
8. Do not copy the Lab 5 `cluster.py`: it is different from the project starter. The Lab 5 archive itself does not contain a working network runtime.

## 1. Planned repository structure and ownership

```text
project-root/
├── node.py                       # instructor-owned: unchanged
├── cluster.py                    # instructor-owned: unchanged for --local
├── client.py                     # instructor-owned: unchanged
├── chaos.py / check.py           # instructor-owned: unchanged
├── app.py                        # Stage A integration: subclass node.Node
├── raft_core.py                  # adapted pure engine from Lab 5
├── raft_dto.py                   # Pydantic Raft RPC and persistent entry models
├── command_dto.py                # command envelopes + TLV blocks
├── tlv_registry.py               # typed registry/validators/serialization
├── state_machine.py              # deterministic KV now; presentation later
├── persistence.py                # typed storage facade using self.disk/save
├── spec.py                       # instructor checker contract for KV now
├── requirements.txt              # Pydantic version pin
├── tests/
│   ├── test_lab_compat.py
│   ├── test_dto_validation.py
│   ├── test_raft_election.py
│   ├── test_append_entries.py
│   ├── test_commit_recovery.py
│   └── test_dedup_and_persistence.py
└── scenarios/
    ├── stage_a_normal.py
    ├── stage_a_replica.py
    ├── stage_a_lider.py
    ├── stage_a_particion.py
    └── stage_a_majority_loss.py
```

For grading, the final domain-specific scripts retain the professor's expected names `normal.py`, `replica.py`, `lider.py`, `particion.py`, and the deliberate-failure scenario. Stage A test scripts are integration precursors, not substitutes for US-04–08.

### Component contracts

| Component | May import | Must not do |
|---|---|---|
| `raft_core.py` | standard Python types, protocol data, injectable storage/clock callbacks | Import slide/lock DTOs, start HTTP servers, inspect wall-clock command timestamps, open shared files |
| `raft_dto.py` | Pydantic models, `command_dto` for log entry type | Interpret editor commands |
| `command_dto.py`, `tlv_registry.py` | Pydantic, `json`, `hashlib`, registry definitions | Start elections or decide lock ownership |
| `state_machine.py` | operation envelope, deterministic command handlers | Generate UUIDs/timestamps, call network, elect a leader, infer commit |
| `app.py` | `node.Node`, engine, DTOs, FSM, persistence | Bypass log for shared writes, directly mutate follower state from client writes |
| `persistence.py` | JSON serialization and injected `disk`/`save()` | Create shared replica storage or implement a second independent log |

## 2. Identity, membership and registration

### 2.1. The three identities that must stay distinct

| Identifier | Generation / source | Persistence | Usage |
|---|---|---|---|
| `transport_slot` | `self.id` / `self.nodes` supplied by starter (`n1`, etc. are examples only) | Configuration | HTTP destination, redirects, configured voting set |
| `node_uuid` | `uuid.uuid4()` on first boot, full UUID string | Permanent in local `disk.json` | Stable replica identity in RPC headers, diagnostics, registration |
| `incarnation` | Increment on ordinary process start, with intact disk | Persistent diagnostic counter | Distinguishes process restarts; **not** a term or voter replacement mechanism |

The voter set is derived at runtime: `voters = frozenset(self.nodes)`, `peers = voters - {self.id}`, `quorum = len(voters)//2 + 1`. Reject startup for fewer than 3 voters. Every slot must be unique; derive no voter name from the operating system or user profile. A peer `RegisterNode` RPC associates one durable UUID with one known slot. Store observed bindings for diagnostics and conflict detection, but **do not use registration to change voter count**.

Registration is peer-to-peer using `self.send`. It is not an external coordinator; the leader is elected through Raft regardless of the order in which registration messages arrive. An RPC must claim the sending slot as seen by `on_message(src, msg)`, and the UUID must match the known registration for that slot. Unknown slots are rejected. For a known slot whose binding is still unknown, accept its `RegisterNode` first; retry registration/handshake as necessary. A changed UUID at a known slot is quarantined pending a safe rejoin policy rather than accepted silently.

**Erased disk:** a fresh UUID at an old voter slot with no `voted_for` cannot be treated as ordinary crash recovery. Baseline `chaos.py start <slot> --empty` is a documented unsafe/unhandled boundary, or else Stage B must implement a safe reintroduction procedure before claiming that test. Preserved-disk `kill/start` is fully supported in Stage A.

### 2.2. Membership fingerprint

Derive a deterministic `membership_fingerprint = sha256(canonical_json(sorted(voters))).hexdigest()` to detect accidentally mismatched cluster configurations. It is a configuration guard, **not** a proof of authenticated identity or a dynamic membership protocol. Every peer must agree before processing its consensus RPCs. `node_uuid` is not part of this fingerprint: normal restarts must not change quorum identity.

## 3. DTO design and versioning conventions

### 3.1. Validation policy

Use **Pydantic v2** `BaseModel`, `Field`, `ConfigDict(extra="forbid", validate_default=True)`, `model_validate`, `model_dump(mode="json")`, `field_validator`, `model_validator` and a discriminated union via `TypeAdapter`. Require actual JSON booleans/integers for protocol control fields; avoid surprising coercion from strings (e.g. `"5"` into term 5). Version fields expressed as `Literal[1]` need an explicit pre-validator rejecting Python `True` (which otherwise may match integer 1). UUIDs arrive as JSON strings and are parsed to `UUID`. All payloads must remain JSON-compatible; reject NaN/Infinity, recursion, unknown required extensions, malformed fields and unsupported schema versions. Never call `model_construct()` on untrusted wire input.

`protocol_version=1` is the consensus wire version; `command_schema_version=1` is the persisted command contract; individual TLV blocks have their own `schema_version`. Do not confuse these versions with a Raft term, per-element `version`, or `lease_epoch`.

**Size limits after JSON parsing (starter limitation):** a typical Raft RPC <= 512 KiB, an individual entry <= 128 KiB, a TLV <= 64 KiB, up to 64 blocks per command, and up to 32 entries per AppendEntries. Stage A's KV commands should be much smaller. Since starter `node.py` parses the body before invoking `app.py`, these limits defend the application model but do **not** enforce a pre-parse HTTP body cap. Publish snapshots may need separately bounded/chunked entries later; do not send arbitrarily large presentations as one command.

### 3.2. Canonical JSON and payload fingerprints

```python
import json
import hashlib

def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")

def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()
```

Only call these functions on values converted to JSON-compatible primitives (e.g. Pydantic `.model_dump(mode="json")`). `length_bytes` measures `canonical_bytes(tlv.value)`, **not** the entire command. Store a canonical fingerprint of `(client_id, op, args)` (exclude transient origin node and timestamps) with a committed request result. On retry, compare fingerprints before returning the stored result. A mismatched repeat returns `REQUEST_ID_REUSE` without applying a second mutation.

### 3.3. Required DTO catalog — protocol

| DTO | Required fields | Validation and semantics |
|---|---|---|
| `NodeRegistrationDTO` | `kind="register_node"`, `protocol_version`, `rpc_id`, `sender_slot`, `sender_uuid`, `term`, `incarnation`, `membership_fingerprint` | Sender slot in configured voter registry; no membership change; UUID consistent with stored binding. |
| `RequestVoteDTO` | `kind="request_vote"`, common header, `candidate_last_log_index`, `candidate_last_log_term` | Both indexes/terms >=0; if index 0, term 0. Persistent `voted_for` check. |
| `RequestVoteResponseDTO` | `kind="request_vote_response"`, header, `reply_to`, `vote_granted` | Valid term and correlated candidate request; vote counted once per known voter. |
| `AppendEntriesDTO` | `kind="append_entries"`, header, `prev_log_index`, `prev_log_term`, `entries`, `leader_commit` | Entries contiguous from `prev_log_index+1`, 0/0 genesis sentinel, bounded batch size. |
| `AppendEntriesResponseDTO` | `kind="append_entries_response"`, header, `reply_to`, `success`, `match_index`, optional `conflict_index`, `conflict_term` | Failure hints >=1 if present; positive ACK must match a pending sent RPC and cannot claim untransmitted prefix. |
| `LogEntryDTO` | `index`, `term`, `command: CommandEnvelopeDTO` | 1-based index, term >=1, valid canonical JSON command, persistent. |
| `CommandEnvelopeDTO` | `command_schema_version`, `request_id`, `client_id`, `origin_node_uuid`, `operation`, `payload`, `blocks` | `request_id` accepts starter IDs; browser uses UUIDv4; `client_id` and operation nonblank. Optional `deck_id`, `session_id`, `expected_version`, audit timestamp. |
| `TLVBlockDTO` | `type_id`, `schema_version`, `required`, `length_bytes`, `value` | Registered type/version validated by Pydantic; canonical UTF-8 length verified; unknown mandatory block rejected. |
| `ClientResultDTO` | `status`, optional `value`, `version`, `details` | Pure deterministic result for state-machine commands; app wraps it in starter `ok/result` response. |
| `PersistedRaftStateDTO` | `storage_version`, `node_uuid`, `incarnation`, `current_term`, `voted_for`, `log`, `commit_index`, `last_applied`, `fsm`, `request_results` | Must obey `0 <= last_applied <= commit_index <= len(log)` with no log compaction. Additional stable peer bindings are permitted. |

#### Wire RPC field definitions (normative)

Common header (all replica messages):

```json
{
  "kind": "append_entries",
  "protocol_version": 1,
  "rpc_id": "ebc7d340-62b7-4d30-970a-7a18b6bd2747",
  "sender_slot": "n1",
  "sender_uuid": "b1ca9029-a4c0-4b4d-88d0-c40cfecc729d",
  "term": 4
}
```

`RequestVote`: `candidate_last_log_index: int`, `candidate_last_log_term: int`.

`RequestVoteResponse`: `reply_to: UUID`, `vote_granted: bool`.

`AppendEntries`: `prev_log_index: int`, `prev_log_term: int`, `leader_commit: int`, `entries: LogEntryDTO[]`.

`AppendEntriesResponse`: `reply_to: UUID`, `success: bool`, `match_index: int`, `conflict_index: int|null`, `conflict_term: int|null`.

`RegisterNode`: `incarnation: int`, `membership_fingerprint: 64-char lowercase hex SHA-256`.

All message receivers MUST check the term; `on_message(src, msg)` must reject any claimed `sender_slot` inconsistent with `src`. A response correlation ID identifies **the outstanding RPC**, not a log entry's `request_id`.

### 3.4. Required DTO catalog — command envelope

```json
{
  "command_schema_version": 1,
  "request_id": "sofia-66c82af0",
  "client_id": "sofia",
  "session_id": null,
  "origin_node_uuid": "b1ca9029-a4c0-4b4d-88d0-c40cfecc729d",
  "submitted_at_utc": "2026-11-22T18:21:00Z",
  "deck_id": null,
  "operation": "kv_put",
  "expected_version": null,
  "payload": {"key": "demo", "value": "Raft committed"},
  "blocks": []
}
```

`submitted_at_utc` is **audit-only** (must never decide Raft ordering or lease expiration). `origin_node_uuid` identifies the node that assembled the validated envelope; it is not a dynamic voter ID or fencing token. `session_id` and `deck_id` are optional at Stage A; they become required for relevant shared-editor commands in Stage B.

`request_id` is an opaque, nonempty UTF-8 identifier (<=128 characters), not necessarily a UUID: the professor's unchanged `client.py` creates `clientlabel-8hex` IDs. Preserve these IDs exactly. Do not derive ordering from them. For future browser writes prefer full UUIDv4.

### 3.5. Typed TLV-like extension blocks and registry

```json
{
  "type_id": 257,
  "schema_version": 1,
  "required": true,
  "length_bytes": 37,
  "value": {"height": 20, "width": 80, "x": 5, "y": 10}
}
```

The example `length_bytes` is 37 for the canonical JSON shown. Producers must compute the actual UTF-8 byte count from canonical JSON rather than relying on fixed examples. The leader validates each block and re-serializes canonical values. Followers validate the exact same schema before application.

| Hex type ID | Name | Pydantic value model | Stage |
|---|---|---|---|
| `0x0001` | Presentation metadata | `PresentationMetadataDTO` | Reserved for B |
| `0x0002` | Slide metadata | `SlideMetadataDTO` | Reserved for B |
| `0x0101` | Element geometry | `GeometryDTO` | Reserved for B |
| `0x0102` | Text content/style | `TextBlockDTO` | Reserved for B |
| `0x0103` | Appearance | `AppearanceDTO` | Reserved for B |
| `0x0104` | Shape classification | `ShapeBlockDTO` | Reserved for B |
| `0x0201` | Lock precondition | `LockPreconditionDTO` | Reserved for B |
| `0x0202` | Element-version precondition | `VersionPreconditionDTO` | Reserved for B |
| `0x0301` | Audit metadata | `AuditMetadataDTO` | Optional metadata only |

**Registry rules:**

- Key a validator registry by `(type_id, schema_version)`; the value is a Pydantic model class and documented semantic purpose.
- Reject unknown `required=true` blocks before append. For Stage A, reject **all** unknown blocks, including optional ones, to avoid inconsistent behavior. Optional-known audit blocks may be carried while intentionally ignored by the state machine.
- Reject repeated unique blocks (geometry/text/shape/precondition), missing mandatory blocks for an operation, mutually incompatible shape+text blocks, and blocks whose `length_bytes` differs from canonical JSON.
- Store blocks as ordered arrays, but handlers must use the unique `type_id` keys and fixed semantics; the order of independent metadata blocks cannot affect application results.
- Multiple blocks **in one command** are applied atomically (a single log entry). Multiple separate commands are not an atomic transaction unless a future command explicitly models one.
- Do not interpret user-supplied `length_bytes` as raw memory pointer lengths; Python JSON has already framed the value. This is a type/size contract, not a binary parser.

### 3.6. Reserved Stage B presentation DTOs (baseline constraints)

These types describe the agreed *minimal* Distributed Slides feature set; they are **not** a mandate to implement the editor in Stage A. Any later change to allowed shapes or styling is a schema-versioned extension, not a reason to modify the Raft log type.

| DTO | Fields | Validation |
|---|---|---|
| `PresentationMetadataDTO` | `deck_id: UUID`, `name: str`, `owner_session_id: UUID`, `width_px: int`, `height_px: int` | Name 1–120 chars, dimensions 320–3840, positive. |
| `SlideMetadataDTO` | `slide_id: UUID`, `background: #RRGGBB`, `ordinal: int` | Ordinal >=0; background valid six-digit hex. |
| `GeometryDTO` | `x, y, width, height: finite float` | Nonnegative x/y, positive width/height, finite numbers; off-canvas geometry policy deferred to presentation logic. |
| `TextBlockDTO` | `text`, `font_family`, `font_size`, `color`, `bold`, `italic`, `align` | text <= 10000 chars; font size 8–144; align in left/center/right; color #RRGGBB. |
| `AppearanceDTO` | `fill`, `stroke: #RRGGBB|null`, `stroke_width`, `opacity` | width >=0, finite; opacity [0,1]. |
| `ShapeBlockDTO` | `shape: "circle" | "rectangle"` | `circle` requires a suitable equal-width/height geometry at domain validation time. |
| `LockPreconditionDTO` | `session_id: UUID`, `fencing_token: int` | Positive token; matches replicated lock state during **apply**, not only leader admission. |
| `VersionPreconditionDTO` | `expected_version: int` | Version >=1; checked atomically during apply. |
| `ElementDTO` | `element_id: UUID`, `slide_id: UUID`, `kind: text|circle|rectangle`, `version: int`, `deleted: bool`, `blocks: TLVBlockDTO[]` | Required block combinations by kind; stable UUID; tombstone and version persist. |
| `SlideDTO` | `slide_id`, `metadata`, `elements[]` | Unique element IDs; valid slide references. |
| `PresentationDTO` | `deck_id`, `metadata`, `slides[]`, `schema_version` | Unique slide/element IDs, valid snapshot references; only snapshot payload, not an independent replication engine. |
| `PublishDeckPayloadDTO` | `presentation: PresentationDTO` | Private-to-shared atomic import through one committed command (subject to bounded size). |
| `CreateElementPayloadDTO` | `deck_id`, `slide_id`, `element: ElementDTO`, `session_id` | One committed creation and first lock acquisition per ADR-001. |
| `UpdateElementPayloadDTO` | `deck_id`, `element_id`, `patch`, `expected_version`, `session_id`, `fencing_token` | Typed patch; token/version validated on apply. |
| `Acquire/Release/RenewLockPayloadDTO` | `deck_id`, `element_id`, `session_id`, `fencing_token` when applicable | Separate deterministic command kinds per ADR-001. |
| `ExpireLockPayloadDTO` | `deck_id`, `element_id`, `expected_fencing_token`, `expected_lease_epoch` | Leader proposes; state machine compares expected values at apply time. |

Do **not** bind presentation metadata or the element content version to a Raft term; Raft terms can advance without an element changing. `create_element` does not require an advance slide-wide structural lock (ADR-001). Deletion should create a tombstone to prevent ID reuse.

### 3.7. Reference Pydantic v2 implementation skeleton

The following is a **contract skeleton**; place it into the indicated modules and add the full registry, operation payload and nested semantic validators. It demonstrates the required validation behavior rather than implementing the transport or Raft algorithm.

```python
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import (
    BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr,
    TypeAdapter, field_validator, model_validator,
)


class StrictDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


class TLVBlockDTO(StrictDTO):
    type_id: StrictInt = Field(ge=1, le=65535)
    schema_version: StrictInt = Field(ge=1)
    required: StrictBool
    length_bytes: StrictInt = Field(ge=0, le=65536)
    value: dict[str, Any]

    @model_validator(mode="after")
    def check_size(self):
        if len(canonical_bytes(self.value)) != self.length_bytes:
            raise ValueError("TLV length_bytes does not match canonical value")
        return self


class CommandEnvelopeDTO(StrictDTO):
    command_schema_version: Literal[1]

    @field_validator("command_schema_version", mode="before")
    @classmethod
    def require_integer_command_schema(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("command schema must be integer 1")
        return value
    request_id: StrictStr = Field(min_length=1, max_length=128)
    client_id: StrictStr = Field(min_length=1, max_length=128)
    session_id: UUID | None = None
    origin_node_uuid: UUID
    submitted_at_utc: datetime | None = None  # metadata only
    deck_id: UUID | None = None
    operation: StrictStr = Field(min_length=1, max_length=80)
    expected_version: StrictInt | None = Field(default=None, ge=0)
    payload: dict[str, Any]
    blocks: list[TLVBlockDTO] = Field(default_factory=list, max_length=64)

    @field_validator("submitted_at_utc")
    @classmethod
    def require_utc_audit_time(cls, value):
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != timedelta(0)
        ):
            raise ValueError("submitted_at_utc must be UTC")
        return value

    @field_validator("payload")
    @classmethod
    def ensure_json_payload(cls, value):
        canonical_bytes(value)  # rejects non-JSON values and NaN/Infinity
        return value


class LogEntryDTO(StrictDTO):
    index: StrictInt = Field(ge=1)
    term: StrictInt = Field(ge=1)
    command: CommandEnvelopeDTO


class RpcHeaderDTO(StrictDTO):
    protocol_version: Literal[1]

    @field_validator("protocol_version", mode="before")
    @classmethod
    def require_integer_protocol_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("protocol version must be integer 1")
        return value
    rpc_id: UUID
    sender_slot: StrictStr = Field(min_length=1, max_length=64)
    sender_uuid: UUID
    term: StrictInt = Field(ge=0)


class RegisterNodeDTO(RpcHeaderDTO):
    kind: Literal["register_node"]
    incarnation: StrictInt = Field(ge=1)
    membership_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")


class RequestVoteDTO(RpcHeaderDTO):
    kind: Literal["request_vote"]
    candidate_last_log_index: StrictInt = Field(ge=0)
    candidate_last_log_term: StrictInt = Field(ge=0)


class RequestVoteResponseDTO(RpcHeaderDTO):
    kind: Literal["request_vote_response"]
    reply_to: UUID
    vote_granted: StrictBool


class AppendEntriesDTO(RpcHeaderDTO):
    kind: Literal["append_entries"]
    prev_log_index: StrictInt = Field(ge=0)
    prev_log_term: StrictInt = Field(ge=0)
    leader_commit: StrictInt = Field(ge=0)
    entries: list[LogEntryDTO] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def check_indexes(self):
        if self.prev_log_index == 0 and self.prev_log_term != 0:
            raise ValueError("genesis prev_log_term must be zero")
        for i, entry in enumerate(self.entries, start=1):
            if entry.index != self.prev_log_index + i:
                raise ValueError("AppendEntries indexes must be contiguous")
        return self


class AppendEntriesResponseDTO(RpcHeaderDTO):
    kind: Literal["append_entries_response"]
    reply_to: UUID
    success: StrictBool
    match_index: StrictInt = Field(ge=0)
    conflict_index: StrictInt | None = Field(default=None, ge=1)
    conflict_term: StrictInt | None = Field(default=None, ge=0)


RaftMessage = Annotated[
    Union[RegisterNodeDTO, RequestVoteDTO, RequestVoteResponseDTO,
          AppendEntriesDTO, AppendEntriesResponseDTO],
    Field(discriminator="kind"),
]
RAFT_MESSAGE_ADAPTER = TypeAdapter(RaftMessage)

# Receiving a network message:
# msg = RAFT_MESSAGE_ADAPTER.validate_python(raw_message)
# validate src-slot match, membership, term, and RPC correlation afterward.
```

**Additional validators to implement** (do not omit): `RequestVote` 0/0 genesis pair; AppendEntries positive term for non-bootstrap messages; payload-specific DTO dispatch (`kv_put`, `raft.noop`, future presentation kinds); maximum canonical encoded entry/message sizes; TLV registry type/version verification; duplicates/required-block combinations; canonical `client_id/request_id` dedup fingerprint; persistent-state invariants; `submitted_at_utc` audit type; strict UUID binding on received messages; conflict hint rules; forbid unexpected `local` from mutation payloads (that flag is read-only `client.final` metadata).


### 3.7.1. Domain and persistent-state model definitions (Stage B reserved, Stage A storage required)

The following additional Pydantic definitions make the storage and future presentation types concrete. These are **schema contracts**; only storage models, `KVPutPayloadDTO`, and result models are mandatory for Stage A runtime. The presentation handlers come in Stage B.

```python
from typing import Any, Literal
from uuid import UUID
from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

# Import StrictDTO, TLVBlockDTO, LogEntryDTO, canonical_bytes from the Stage A DTO modules.
ColorHex = StrictStr

class KVPutPayloadDTO(StrictDTO):
    key: StrictStr = Field(min_length=1, max_length=256)
    value: StrictStr = Field(max_length=10000)  # Stage A KV test: strings only

class ClientResultDTO(StrictDTO):
    status: StrictStr = Field(min_length=1, max_length=80)
    value: Any | None = None
    version: StrictInt | None = Field(default=None, ge=0)
    details: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_json_result(self):
        canonical_bytes({"value": self.value, "details": self.details})
        return self

class RaftHardStateDTO(StrictDTO):
    current_term: StrictInt = Field(ge=0)
    voted_for: StrictStr | None = None
    log: list[LogEntryDTO] = Field(default_factory=list)
    commit_index: StrictInt = Field(ge=0)
    last_applied: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def check_log_bounds(self):
        if not (0 <= self.last_applied <= self.commit_index <= len(self.log)):
            raise ValueError("invalid commit/apply bounds")
        if any(entry.index != i for i, entry in enumerate(self.log, 1)):
            raise ValueError("log indexes are not contiguous")
        return self

class DedupRecordDTO(StrictDTO):
    fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    result: ClientResultDTO
    committed_index: StrictInt = Field(ge=1)

class PersistedRaftStateDTO(StrictDTO):
    storage_version: Literal[1]

    @field_validator("storage_version", mode="before")
    @classmethod
    def require_integer_storage_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("storage version must be integer 1")
        return value
    node_uuid: UUID
    incarnation: StrictInt = Field(ge=1)
    membership_fingerprint: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    peer_bindings: dict[str, UUID] = Field(default_factory=dict)
    raft: RaftHardStateDTO
    fsm: dict[str, Any] = Field(default_factory=lambda: {"kv": {}})
    request_results: dict[str, DedupRecordDTO] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_dedup_indexes(self):
        canonical_bytes(self.fsm)
        if any(d.committed_index > self.raft.last_applied
               for d in self.request_results.values()):
            raise ValueError("dedup snapshot ahead of applied state")
        return self

class PresentationMetadataDTO(StrictDTO):
    deck_id: UUID
    name: StrictStr = Field(min_length=1, max_length=120)
    owner_session_id: UUID
    width_px: StrictInt = Field(ge=320, le=3840)
    height_px: StrictInt = Field(ge=320, le=3840)

class SlideMetadataDTO(StrictDTO):
    slide_id: UUID
    background: StrictStr = Field(pattern=r"^#[0-9a-fA-F]{6}$")
    ordinal: StrictInt = Field(ge=0)

class GeometryDTO(StrictDTO):
    x: float = Field(ge=0, allow_inf_nan=False)
    y: float = Field(ge=0, allow_inf_nan=False)
    width: float = Field(gt=0, allow_inf_nan=False)
    height: float = Field(gt=0, allow_inf_nan=False)

class TextBlockDTO(StrictDTO):
    text: StrictStr = Field(max_length=10000)
    font_family: Literal["sans", "serif", "mono"]
    font_size: StrictInt = Field(ge=8, le=144)
    color: StrictStr = Field(pattern=r"^#[0-9a-fA-F]{6}$")
    bold: StrictBool = False
    italic: StrictBool = False
    align: Literal["left", "center", "right"] = "left"

class AppearanceDTO(StrictDTO):
    fill: StrictStr = Field(pattern=r"^#[0-9a-fA-F]{6}$")
    stroke: StrictStr | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")
    stroke_width: float = Field(default=0, ge=0, allow_inf_nan=False)
    opacity: float = Field(default=1, ge=0, le=1, allow_inf_nan=False)

class ShapeBlockDTO(StrictDTO):
    shape: Literal["circle", "rectangle"]

class LockPreconditionDTO(StrictDTO):
    session_id: UUID
    fencing_token: StrictInt = Field(ge=1)

class VersionPreconditionDTO(StrictDTO):
    expected_version: StrictInt = Field(ge=1)

class ElementDTO(StrictDTO):
    element_id: UUID
    slide_id: UUID
    kind: Literal["text", "circle", "rectangle"]
    version: StrictInt = Field(ge=1)
    deleted: StrictBool = False
    blocks: list[TLVBlockDTO]
    # Operation-specific block requirements enforced by TLV registry.

class SlideDTO(StrictDTO):
    metadata: SlideMetadataDTO
    elements: list[ElementDTO] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_element_ids(self):
        ids = [e.element_id for e in self.elements]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate element ID")
        if any(e.slide_id != self.metadata.slide_id for e in self.elements):
            raise ValueError("element references another slide")
        return self

class PresentationDTO(StrictDTO):
    schema_version: Literal[1]

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_schema_version(cls, value):
        if type(value) is not int or value != 1:
            raise ValueError("presentation schema must be integer 1")
        return value
    metadata: PresentationMetadataDTO
    slides: list[SlideDTO] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_slide_ids(self):
        ids = [s.metadata.slide_id for s in self.slides]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate slide ID")
        return self

class PublishDeckPayloadDTO(StrictDTO):
    presentation: PresentationDTO

class CreateElementPayloadDTO(StrictDTO):
    deck_id: UUID
    slide_id: UUID
    element: ElementDTO
    session_id: UUID

class UpdateElementPayloadDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    session_id: UUID
    fencing_token: StrictInt = Field(ge=1)
    expected_version: StrictInt = Field(ge=1)
    patch_blocks: list[TLVBlockDTO] = Field(min_length=1)

class AcquireLockPayloadDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    session_id: UUID

class ReleaseLockPayloadDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    session_id: UUID
    fencing_token: StrictInt = Field(ge=1)

class RenewLockPayloadDTO(ReleaseLockPayloadDTO):
    pass

class ExpireLockPayloadDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    expected_fencing_token: StrictInt = Field(ge=1)
    expected_lease_epoch: StrictInt = Field(ge=0)

class DeleteElementPayloadDTO(UpdateElementPayloadDTO):
    # Must not carry changes: replace with dedicated narrow fields on implementation.
    patch_blocks: list[TLVBlockDTO] = Field(default_factory=list, max_length=0)

class GetElementArgsDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    local: StrictBool = False

class GetSlideArgsDTO(StrictDTO):
    deck_id: UUID
    slide_id: UUID
    local: StrictBool = False

class GetDeckPreviewArgsDTO(StrictDTO):
    deck_id: UUID
    local: StrictBool = False

class ReadLockStatusArgsDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    local: StrictBool = False

class GrantAccessPayloadDTO(StrictDTO):
    deck_id: UUID
    collaborator_session_id: UUID
    access: Literal["editor", "viewer"]

class ForceUnlockPayloadDTO(StrictDTO):
    deck_id: UUID
    element_id: UUID
    authorized_session_id: UUID
    expected_fencing_token: StrictInt = Field(ge=1)

class CreateSlidePayloadDTO(StrictDTO):
    deck_id: UUID
    metadata: SlideMetadataDTO

class ReorderSlidesPayloadDTO(StrictDTO):
    deck_id: UUID
    ordered_slide_ids: list[UUID] = Field(min_length=1)
    expected_deck_version: StrictInt = Field(ge=1)

class UpdateSlideSettingsPayloadDTO(StrictDTO):
    deck_id: UUID
    slide_id: UUID
    new_background: StrictStr = Field(pattern=r"^#[0-9a-fA-F]{6}$")
    expected_slide_version: StrictInt = Field(ge=1)
```

The registry, rather than the shape DTO alone, must enforce that a text element has one geometry and one text block, while a circle/rectangle has geometry + shape + appearance; a circle must have equal width and height under the chosen numeric representation. Reject duplicate block IDs, wrong deck/slide references, and illegal property patches. A published snapshot must not import a client's authoritative lock table or forged fencing counters. The optional access and slide/reorder DTOs above reserve a **shape**, not an authorization or ordering policy; their operations must not be enabled until a separate domain ADR defines permissions and deck/slide version semantics. The `DeleteElementPayloadDTO` placeholder deliberately allows no patch blocks and should be narrowed to a dedicated common mutation-preconditions base class during Stage B. Read argument DTOs define only the eventual preview path, not linearizable read-index semantics.

### 3.8. Structured command examples

**Raft internal no-op** (leader appends once on becoming leader, term shown in entry):

```json
{
  "index": 17,
  "term": 5,
  "command": {
    "command_schema_version": 1,
    "request_id": "internal-noop-term-5-index-17",
    "client_id": "_system",
    "session_id": null,
    "origin_node_uuid": "b1ca9029-a4c0-4b4d-88d0-c40cfecc729d",
    "submitted_at_utc": null,
    "deck_id": null,
    "operation": "raft.noop",
    "expected_version": null,
    "payload": {},
    "blocks": []
  }
}
```

**Future shape creation uses multiple typed blocks:** a `create_element` command contains `GeometryDTO` + `ShapeBlockDTO` + `AppearanceDTO`, and the base payload includes `deck_id`, `slide_id`, `element_id`, `session_id`. All blocks must validate before the command is proposed; creation plus initial lock happens atomically **when the committed entry is applied**, not when the leader receives it. A text element uses `GeometryDTO` + `TextBlockDTO` (+ optional appearance). Do not append separate per-block log entries for one atomic creation.

## 4. Persistent and volatile state models

### 4.1. Local `self.disk` schema (uncompacted-log version)

```json
{
  "storage_version": 1,
  "node_uuid": "b1ca9029-a4c0-4b4d-88d0-c40cfecc729d",
  "incarnation": 2,
  "membership_fingerprint": "<64 hex digits>",
  "peer_bindings": {"n2": "<uuid>", "n3": "<uuid>"},
  "raft": {
    "current_term": 5,
    "voted_for": "n1",
    "log": [],
    "commit_index": 0,
    "last_applied": 0
  },
  "fsm": {"kv": {}},
  "request_results": {}
}
```

`voted_for` is the **configured transport slot** associated with the candidate UUID validated for that term; do not use a randomly regenerated UUID as the vote key. Every persisted log entry contains its term and canonical command. `request_results` contains `(client_id, request_id) -> {fingerprint, deterministic_result, committed_index}`; encode tuple keys as an unambiguous composite string or nested map, not `str(tuple)`.

**Atomic persistence invariant:** Write the entire mutually consistent `raft`, `fsm` and dedup snapshot via one `self.save()` whenever their durable values change. This starter provides whole-file replacement, not a transactional multi-file database. Never persist a `last_applied` value ahead of the matching FSM and request-result state. On restart validate snapshot indexes, entry sequence and schema. If the persisted FSM is missing/corrupt, recovery must be a deliberate replay/reconstruction from committed entries, not silent data reset.

### 4.2. Volatile state (rebuild, do not persist as authority)

- `role: follower|candidate|leader` (restore as follower after boot).
- `leader_slot: str|null` (only hint for redirects).
- `votes_received: set[transport_slot]` for current election term.
- `election_deadline: float` from `self.now()` (local monotonic time).
- `heartbeat_deadline: float` and in-flight RPC IDs.
- Leader-only `next_index[peer]`, `match_index[peer]`, initialized on every new election.
- `pending_clients[log_index or request-key]` holding exact original `req.id` and reply state.
- Optional peer registration retry timers; never persisted as authoritative lease times.

No random timeout, local wall-clock timestamp or volatile counter may be used in a deterministic FSM condition or persistent lock expiry decision. Stage B lease expiration remains a **leader-proposed committed command** with expected token/lease epoch (ADR-001).

### 4.3. Boot and replay ordering

1. `node.Node.__init__` loads `self.disk`; `App.start()` validates the envelope and membership fingerprint.
2. If fresh disk, generate and persist `node_uuid`, `incarnation=1`, `term=0`, `voted_for=null`, empty log, commit/apply zero, empty FSM/results **before** any voting participation.
3. If ordinary restart, load the existing UUID, increment persisted incarnation, reconstruct engine, role=follower, leader hint unknown, and reset local election deadline.
4. Validate `0 <= last_applied <= commit_index <= log_len`. Validate contiguous log indexes and each command's schema. If FSM snapshot is consistent, retain it; if not, rebuild **only from committed log prefix** in a controlled recovery mode.
5. Replay committed but unapplied entries **in ascending index order**; persist `{FSM, request_results, last_applied}` together after application before serving requests. Do not apply uncommitted suffix entries.
6. Register with known peers; use only configured slots for quorum. Begin elections once the local durable state and identity validation have succeeded.

## 5. Core Raft algorithm — normative transitions

Use one-based log indexes; empty log's last index and term are both zero. The elected leader can append a `raft.noop` in its new term to establish a current-term commit point. The leader must persist its own appended entry before advertising its replication and cannot confirm a client command until that command is committed and applied.

### 5.1. Election and RequestVote

1. Choose randomized election timeout per follower/candidate, e.g. 300–600 ms (tunable to the starter's 50-ms ticks and message delays). Heartbeat interval, e.g. 100 ms, is shorter than minimum election timeout. These are demonstration defaults, not synchrony/safety assumptions.
2. When a deadline passes without valid leader contact or a granted vote: increment term, switch to candidate, set `voted_for=self.id`, persist term/vote, count self once, send `RequestVote` to known peers with last log index/term.
3. Receiver: reject older term; when a newer term is observed, persist the new term, clear vote, become follower; grant at most one vote per term if the candidate's log is at least as up to date lexicographically `(last_term, last_index)`; persist grant before reply.
4. Candidate counts votes only from configured unique slots, in the current term, for its outstanding election. At majority, become leader; set self `match_index=last_index`, followers' `next_index=last_index+1` and `match_index=0`; append/persist current-term no-op and send initial AppendEntries.
5. On a valid AppendEntries with term >= current term, become/remain follower and reset election timer. On any RPC response with higher term, persist stepdown immediately.
6. Same-term competing leaders are a protocol violation. `roles.log` must never report different leaders with the same term.

### 5.2. AppendEntries follower handler

Given `term`, `prev_log_index`, `prev_log_term`, `entries`, `leader_commit`:

1. Validate sender slot/UUID, configured membership, DTO, and protocol. If term is lower than local term, reject with current term.
2. If term is greater, persist the updated term/vote reset and become follower. If same-term valid leader, recognize it as leader (do not accept a conflicting different leader as normal).
3. Verify the local log contains `prev_log_index` and its term matches `prev_log_term`; otherwise respond `success=false`, `conflict_index`/`conflict_term` hints, and **do not modify log or commit index**.
4. Starting at `prev_log_index+1`, compare incoming entries against existing entries. The first conflict in the **uncommitted suffix** causes deletion from that index onward, followed by append of the incoming entries. Matching index+term implies matching command under the Raft log-matching invariant; a hash mismatch is a fatal corruption/invariant error, not silently overwritten. Preserve every committed entry.
5. If the incoming entries are a **matching prefix** of a longer follower uncommitted suffix, keep the extra suffix; a heartbeat or short batch is **not** authorization to truncate it.
6. Persist the resulting log before returning `success=true`, including no response that claims an entry was durably stored before `save()` completes.
7. Set `commit_index = max(old_commit, min(leader_commit, local_last_index))` when the leader's commit evidence is valid; persist the change and apply newly committed entries in order. The follower may send the append success response after the durable log append; the leader's response does not imply that the follower has applied the new `leader_commit` yet.
8. Return success `match_index = prev_log_index + len(entries)` (known prefix proven by **this RPC**, not blindly the follower's entire possibly divergent log).

The acceptance flow must preserve Raft log matching and committed-prefix safety even with reordering or duplicates.

### 5.3. AppendEntries leader handler and next-index repair

1. Maintain per-follower `next_index`, `match_index`, last sent RPC ID and its entry range/term. Send AppendEntries starting at that follower's `next_index` with `prev_index = next_index-1`, correct `prev_term`, and a bounded batch. Empty entries are heartbeats.
2. Ignore out-of-term, unknown-slot, unknown/uncorrelated or impossible acknowledgments. A successful ACK may advance a follower's `match_index` **only through the explicitly acknowledged range**, bounded by local leader log length and the request's last index. Set `next_index=match_index+1`.
3. A failure with a valid conflict hint moves `next_index` back to the first potentially matching index; otherwise decrement conservatively to at least 1. Never let delayed negative ACKs regress a follower below a more recently proven `match_index`.
4. Retry until the follower catches up or a higher term causes stepdown. A failure is not evidence of a replicated entry.
5. Compute a majority commit point for an entry **from the leader's current term** where `match_index >= N` on a quorum. Mark the newly committed prefix and persist it. Previous-term entries become committed indirectly through that current-term entry.
6. Apply committed commands in order. For an associated client request, persist its deterministic result and dedup record, then `self.reply(original_req_id, {"ok": true, "result": result})`.
7. Send heartbeats/AppendEntries with `leader_commit` to propagate the commit point. A follower that rejoins with intact disk can recover its missing prefix/suffix using the same mechanism; no snapshot RPC is needed for Stage A's bounded logs.

**Critical rule:** Do not call the original Lab `repair_uncommitted_suffix` as the distributed follower's append handler; it was a *whole-log helper* with term-only common-prefix matching, not the complete AppendEntries protocol.

### 5.4. State-machine apply and deduplication

A replicated command transition is deterministic:

```text
apply_committed(entry at index N):
    assert N == last_applied + 1 and N <= commit_index
    validate committed command schema and supported operation
    if (client_id, request_id) already in replicated result table:
        compare canonical operation fingerprint
        result = stored_result (or REQUEST_ID_REUSE if fingerprint differs)
        do not mutate domain state
    else:
        result = state_machine.apply(command)
        store result + fingerprint + N in dedup table
    persist fsm + dedup + last_applied=N atomically through self.save()
    reply to any waiting client only AFTER persistence
```

A command that is well-formed but fails a domain condition (`LOCKED`, stale version, etc.) still has a deterministic committed result and dedup record. A malformed command must be denied at leader admission; if an invalid command somehow appears in the committed log, fail closed and surface a consistency fault rather than diverging by skipping it.

**Leader-side dedup optimization:** Before appending, if the exact request is already committed, return its stored result; if the same request is already uncommitted/in flight, attach the new waiter to the original log index rather than append a duplicate. The replicated apply-time check remains mandatory because a request may be appended twice across terms before the first copy commits.

### 5.5. Client request handling

```text
on_request(req):
    normalize req.id, req.op, req.args, req.client into validated envelope
    if op is a LOCAL_READ (kv_get or later get_slide/get_element):
        return local applied state, including args.local=true path
    if syntactically invalid:
        return ok:true, result:{status:"INVALID_REQUEST", ...}  # definitive rejection
    if not leader:
        return redirect:<known live configured slot> or ok:false/no leader
    if committed duplicate:
        return recorded deterministic result
    if conflicting duplicate:
        return ok:true, result:{status:"REQUEST_ID_REUSE"}
    validate supported operation and TLVs; append/persist command
    record waiter keyed by original req.id; replicate; return None
on_message(src,msg):
    validate Pydantic RPC + membership identity + correlation
    dispatch election/append/registration to engine; persist before replies
on_tick():
    handle randomized election deadlines / heartbeats / retries
    apply newly committed entries (if not applied immediately)
status():
    return {role, term, leader_id?, node_uuid?, commit_index?, last_applied?}
```

A successful transport reply from the starter with `ok:true` means a **definitive answer**, even if the application result is `LOCKED` or `INVALID_REQUEST`. A timeout or `ok:false` leaves the outcome uncertain and may trigger retries. The *same* request ID must be reused for one logical attempt; a new request ID represents a new operation and may legitimately execute again. Client-side `req.args.local` is for reads only and must never authorize shared writes on an isolated node.

### 5.6. Partition and restart semantics

- Partition 1/2: only side with two reachable voters can elect/maintain a committing leader. Isolated old leader may temporarily report `role=leader`, but must **never acknowledge a new write** without current majority confirmation.
- Pause/slow nodes: randomized election timeouts can trigger extra elections; terms and leader changes are safe even when liveness temporarily suffers.
- Kill one follower: two-voter majority continues writes. Restart same follower with intact disk and repair missing log entries.
- Kill leader: majority elects replacement; no acknowledged command may disappear; retries of uncertain requests use exact IDs.
- Kill two replicas: remaining node cannot commit a new write; local eventually consistent reads may still be available.
- Heal: missing logs and committed results converge when all replicas communicate and writes stop.
- Erased disk: do not pretend a restarted erased voter retains stable vote/identity. Quarantine or declare outside the safety model until a safe membership/recovery extension is designed.

## 6. Mapping and adapting the Lab 5 code

| Lab implementation | Concrete change | Unit/cluster proof |
|---|---|---|
| `MEMBERS = {"A","B","C"}` | Remove; inject immutable `voters` from `self.nodes` and compute quorum dynamically | Parameterized tests with 3 and 5 arbitrary slot strings |
| `Node.node_id` | Keep configured `transport_slot` separately from durable generated `node_uuid` | Kill/restart preserves UUID; no hardcoded keys |
| `Node.current_term`, `voted_for` | Load/save as durable hard state; restore role=follower | Repeated vote same term cannot switch candidate across reboot |
| `Entry.command: str` | Replace with validated `CommandEnvelopeDTO` persisted as JSON; add `index` on wire and assert contiguous log | Reject unknown schema; parse/replay durable command |
| `request_vote` | Keep term/log freshness logic; add persisted vote, known-member check, higher-term handling | Deny stale log and second vote in same term |
| `advance_commit` | Retain current-term-only commit rule; make membership generic and count leader self | No premature commit of old-term entry without current-term anchor |
| `receive_ack` | Add correlated RPC, known peer, bounded proven index and monotonic match-index checks | Delayed/fake ACK cannot make majority |
| `repair_uncommitted_suffix` | Replace runtime usage with AppendEntries prev-index/term + retry; keep helper in lab tests if desired | Divergent suffix repaired without committed-prefix changes |
| `commit_index`, `last_applied` | Separate commitment from deterministic FSM application; durable snapshot invariant | Never apply uncommitted entry; survives reboot |

### Suggested method interface for the engine

```python
class RaftEngine:
    def __init__(self, slot, voters, restored_state, clock, persist, send): ...
    def on_tick(self) -> None: ...
    def on_vote_request(self, src, dto) -> None: ...
    def on_vote_response(self, src, dto) -> None: ...
    def on_append_entries(self, src, dto) -> None: ...
    def on_append_response(self, src, dto) -> None: ...
    def propose(self, command) -> int | None: ...  # leader only; log index
    def committed_unapplied(self) -> list: ...
    def role(self) -> str: ...
    def current_term(self) -> int: ...
```

The engine receives I/O functions and a clock **by injection**. It must not subclass the instructor's Node. `app.App` owns `self.save`, `self.send`, `self.reply`, and domain interpretation. For the agent, retain function-level lab regression tests, but do not preserve the Lab 5 dataclass interface at the expense of correct runtime invariants.

## 7. Detailed test and acceptance matrix

| ID | Test | Expected evidence |
|---|---|---|
| A-01 | Fresh 3-replica boot | Three different persisted `node_uuid` values; each knows 3 configured voter slots; one leader/term |
| A-02 | Restart follower with disk | Same node UUID/term/vote/log restored, role starts follower, then catches up |
| A-03 | Voting after reboot | Cannot vote for two candidates in same term; up-to-date log rule enforced |
| A-04 | 5 arbitrary voters | Dynamic quorum=3; no A/B/C hardcoding |
| A-05 | Heartbeats | Idle leader remains stable; tick schedule works with starter's 50-ms callbacks |
| A-06 | Leader appends KV command | No success until majority durable append + committed apply; every follower eventually applies |
| A-07 | Fail leader mid-write | All acknowledged writes retained; ambiguous request may safely retry |
| A-08 | Duplicate same ID | Same result, only one domain effect; result survives leader change |
| A-09 | Same ID, different payload | Deterministic `REQUEST_ID_REUSE` and no mutation |
| A-10 | Divergent uncommitted follower suffix | Backoff/repair with prev-index/term; never erase committed prefix |
| A-11 | Short heartbeat to longer follower | Follower does not truncate extra uncommitted suffix |
| A-12 | ACK spoof/late reply | Ignore unknown peer, wrong term, uncorrelated ID, impossible index |
| A-13 | Minority partition | No local fresh write commit; possible stale local `kv_get`; majority continues |
| A-14 | Majority loss (2 failures) | No confirmed new writes; outstanding requests time out/unavailable |
| A-15 | Heal/restart | All replicas ultimately report equal final local KV result and indexes |
| A-16 | Pydantic invalid envelopes | Rejected: malformed type/term, unknown required TLV, wrong TLV length, unknown operation, invalid hex, NaN, duplicate unique blocks |
| A-17 | Protocol/version mismatch | Controlled rejection, no corrupted persisted state |
| A-18 | Crash around persistence boundary | Reboot restores vote/log and never exposes acknowledged-but-lost entry |
| A-19 | `check.py` | Stage A `kv_put` linearizable; `kv_get` eventual and converges in final reads; no duplicate term leader; write/read costs captured |
| A-20 | Hooks/API compatibility | Unmodified `node.py`, `client.py`, `chaos.py`, `check.py` operate via documented callbacks and routes |

### Mapping Stage A tests to US-01–08

| User story | Stage A infrastructure proof | Domain behavior deferred |
|---|---|---|
| US-01 — Local creation/publishing | Valid command envelopes, deterministic whole-entry state transition, durably replicated writes | Presentation snapshot and share API |
| US-02 — Concurrent editing/locking | Concurrent client request ordering, dedup, atomic compare/check path | Element locks, fencing and version checks |
| US-03 — Recovery/expiration | Leader change, durable state recovery, current-term commits | Committed renew/expire timing and stale-token cases |
| US-04 — Normal collaboration | `stage_a_normal` KV path, read/write cost difference | Publish/lock/edit/release full story |
| US-05 — Follower failure | `stage_a_replica` persistent follower catch-up | Presentation/lock state |
| US-06 — Leader failure | `stage_a_lider` concurrent threads and exact-ID retries | Two editors' lock/update semantics |
| US-07 — Partition/heal | `stage_a_particion` majority/minority/repair | Stale preview and owner lock effects |
| US-08 — No majority | `stage_a_majority_loss` safety/availability boundary | Collaborative edit UI response |

### Checker and evidence rules

- Stage A `spec.py`: `GUARANTEE = {"kv_put":"linearizable", "kv_get":"eventual"}`; `READS={"kv_get"}`, `INITIAL=None`, `key(op,args)=args["key"]`, `apply` models put and get. Choose a committed `kv_put` result shape that is easy to represent in the spec (e.g. `"ok"`).
- The checker does not verify actual Raft message traces; write targeted assertions for prefix matching, duplicate votes and ACK correlation.
- `check.py` uses the history's `ok` and `result` fields; app-level rejections must return `ok:true` with a deterministic result. Failed `ok:false` operations may have happened and are treated as uncertain.
- A local eventual `kv_get` may return stale values during a partition, but final reads after healing must converge. Use one `Client.final("kv_get", key="demo")` per scenario, ensuring all replicas have caught up.
- `Client.call(op, node="n1")` follows a leader redirect; for an isolated-minority *direct* probe send a single raw `POST /client` without following an application redirect and record that result separately.
- Save `historiales/` histories, checker output, role logs, and additional machine-readable trace for partition/failure observations. `cluster.py down` removes `run/` after archiving histories/role logs—save any custom diagnostics first.
- Compare strong `kv_put` latency against eventual local `kv_get` latency **in the same scenario**; use the checker cost section plus explicit measurements where useful.

### Suggested execution (local mode)

```bash
python3 -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python cluster.py up --local
python scenarios/stage_a_normal.py
python check.py run/stage_a_normal.jsonl
python cluster.py status
python cluster.py down
```

Each scenario requires a clean cluster and independent history. Stage A.1 builds the project Docker image from the pinned requirements before claiming container support. Do not silently switch the protocol to FastAPI merely to resolve dependency installation.

## 8. Python environment, dependencies, and network stack

| Requirement | Stage A choice | Rationale |
|---|---|---|
| Python | `>=3.11,<3.14` (test primarily with 3.12, matching starter Docker image) | Course requires 3.11+; bounded initial matrix simplifies verification. |
| Pydantic | v2, pin e.g. `pydantic==2.13.5` in `requirements.txt` | Reproducible DTO validation; uses `model_validate`, discriminated union and v2 validators. |
| JSON | `json` standard library, canonical encoding | Compatible with starter `send`, disk, and client. |
| HTTP | `http.server` / `urllib.request` through instructor `node.py` | Already implements transport, partitions, status and request waiting. |
| FastAPI/Uvicorn | **Not installed in Stage A** | Would replace/double the tested transport unnecessarily. Can be a separate UI-facing gateway in later stage. |
| Test runner | Python `unittest` + instructor checker | No pytest dependency required. |
| Docker | Project-built image based on `python:3.12-slim` with pinned requirements installed | Gives local and container replicas the same DTO validation contract while retaining the starter transport. |

**Policy / grading risk to acknowledge:** the starter was supplied *without additional packages*. It does not say that `node.py` automatically installs Pydantic. If course constraints or grading environment prohibit any non-stdlib dependency, Pydantic's use requires instructor approval or a documented alternative DTO-validation implementation. The primary Stage A implementation is specified with Pydantic as requested, so local execution requires installing it.

### Network topology — do not introduce a second consensus path

```text
client.py / browser adapter
        |
        | POST /client  (existing runtime)
        v
   node.py HTTP handler
        |
        v
   on_request -> RaftAdapter -> RaftEngine -> self.disk/save
        |
        | self.send(peer_slot, PydanticRPC.model_dump(mode="json"))
        v
   node.py POST /msg -> peer.on_message(src,msg)

GET /status -> App.status() (role/term required)
```

FastAPI is an option **only** for a future standalone frontend/BFF service that forwards client operations to the three starter nodes and does not perform consensus, persist authoritative state, or bypass majority rules.

## 9. Implementation order for the agent (hard gates)

1. **Stage A baseline and contracts:** preserve the Lab 5 reference, implement strict DTOs, command envelopes, registry, and deterministic core tests.
2. **Stage A protocol core:** implement identity models, election, AppendEntries, current-term commit, no-op, deterministic KV application, and deduplication behind injected persistence/transport interfaces.
3. **Stage A completion gate:** prove the core through deterministic component tests, including malformed input, vote/log safety, repair, current-term commitment, and duplicate request outcomes.
4. **Stage A.1 adapter and persistence:** connect the engine to the starter callbacks, `self.disk`/`self.save()`, slot registration, redirects, and operational diagnostics.
5. **Stage A.1 environment:** build the dependency-capable Docker image and preserve the supplied local/container launcher behavior.
6. **Stage A.1 fault evidence:** run normal, follower, leader, partition, majority-loss, restart, and catch-up scenarios through the real starter seam; collect checker histories, role traces, and operational archives.
7. **Stage B handoff:** reserve presentation/shape/TLV IDs and implement ADR-004's client synchronization model only after Stage A.1 evidence is complete.

## 10. Agent execution instructions and definition of done

**Work from the checked-in sources.** Do not replace the instructor `node.py`, `cluster.py`, `client.py`, `chaos.py`, `check.py` with Lab versions, third-party Raft libraries, or a new HTTP framework. Reuse the Lab 5 safety logic where correct, explain each departure, and preserve tests. Do not declare completion on unit tests alone; collect live cluster histories and fault evidence. Do not mark current code as fully integrated before those tests actually pass.

**Stage A is complete only when** the pure engine satisfies its deterministic protocol-core tests for fixed membership, voting, replication, commitment, deterministic application, and duplicate request handling.

**Stage A.1 is complete only when** the fixed-membership starter cluster can elect and re-elect leaders, durably replicate and apply a typed KV command, recover a lagging follower, preserve committed writes across leader failure, refuse minority writes, prevent duplicate effects on retries, validate malformed JSON contracts, archive structured operational diagnostics, and show correct `check.py` histories and roles using the professor's unchanged runtime.

Once Stage A is passing, implement `publish_deck`, slide elements, and selection-triggered locks as **new state-machine handlers and TLV validators**, not as new Raft algorithms. That is the architectural purpose of this integration.

## 11. References and reconciliation notes

**Directly observed source behavior:** Lab `raft_core.py` function signatures, hardcoded `MEMBERS`, whole-log repair behavior; starter `node.py` callbacks/HTTP/disk/ticks; `client.py` request IDs/redirects; `cluster.py` configured slots/Docker image; `check.py` models and histories; accepted ADR-001 and US-02–08 application outcomes.

**Proposed in this spec (not already implemented):** generated UUID node registry; all Pydantic DTOs and TLV codes; peer registration; RPC correlation, retry policy, no-op, storage snapshots, state-machine architecture, tests, and dependency decisions. Do not describe those as features already present in the provided code.

- Raft consensus reference: https://raft.github.io/raft.pdf
- Pydantic v2 models: https://docs.pydantic.dev/latest/concepts/models/
- Pydantic discriminated unions: https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions
- Pydantic strict types: https://docs.pydantic.dev/latest/concepts/strict_mode/
- FastAPI reference (later optional gateway): https://fastapi.tiangolo.com/
- Course: `CMP5001-Proyecto.pdf` (pp. 1–4)
- Accepted lock rules: `ADR-001-element-level-distributed-locking.md`
- User stories: US-01–US-08, including US-04/05/06/07/08 fault requirements
