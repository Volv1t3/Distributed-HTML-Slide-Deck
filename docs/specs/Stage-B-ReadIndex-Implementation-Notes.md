# Stage B.1 — Linearizable Authoritative Reads via Raft ReadIndex

**Status:** Implemented in the attached patched workspace; targeted tests and live fault injection passed on 2026-11-23.

**Scope:** `authoritative_refresh_element` / `refresh_element` only. Replica-local `get_element`, `get_slide`, `get_deck_preview`, and `read_lock_status` remain eventually consistent.

## Problem

The previous authoritative read path checked only `engine.role == "leader"` and `last_applied >= min_commit_index`. A partitioned former leader could pass both checks after a new majority elected a leader and committed an update, and return a stale element snapshot.

## Implementation decision

Use a real **quorum-confirmed Raft ReadIndex** round, not an extra committed log entry per read and not a clock-based leader lease. Each request has its own generated `read_context` (UUID), is bound to a Raft term, and is confirmed only by responses to heartbeats initiated for that request.

1. **Leader eligibility.** Only the local Raft leader begins a read; others redirect or return `NO_LEADER`.
2. **Committed current-term entry.** A read does not start quorum confirmation until the leader has committed at least one log entry in its current term (normally the leader-election `raft.noop`). This establishes an up-to-date commit index.
3. **Capture index.** Record `read_index = commit_index` after that prerequisite.
4. **Confirm leadership with a quorum.** Send a fresh, empty `AppendEntries` heartbeat to peers with the read's UUID in `read_context`. Replies must match the sender slot, registered UUID, term, exact outstanding RPC ID, and read context. The leader counts its own vote plus acknowledgments from distinct voters. An AppendEntries log mismatch is allowed to confirm *term/leader contact* for the read, but never changes the peer's replication progress.
5. **Apply barrier.** Return the state-machine snapshot only when `last_applied >= max(read_index, min_commit_index)` and the leader has not changed term or role.
6. **Timeout and leader changes.** A read without quorum returns `QUORUM_UNAVAILABLE` after the configured read timeout (3 seconds by default). Stepping down invalidates outstanding reads, which return `NO_LEADER` or redirect. A completed/failed read releases its volatile context and correlated RPC records.

All read state is volatile; no `raft.noop` is added per read. Heartbeats use existing HTTP/RPC transport and the original instructor runtime, with `read_context` added to strict Pydantic RPC DTOs. The field is optional for standard replication traffic and must be echoed by a follower handling a ReadIndex heartbeat.

### Safety refinement in AppendEntries

The follower's `leader_commit` advancement is bounded to the log prefix verified by the received RPC: `min(leader_commit, prev_log_index + len(entries))`. Previously it used the entire follower log length, which could cause a read-only heartbeat to commit an unrelated divergent suffix. Standard replication tests continue to pass.

### Contracts

- `begin_read_index(min_commit_index: int = 0) -> UUID` starts a new volatile read barrier.
- `read_index_status(context) -> (status, index)` exposes `PENDING`, `READY`, `NOT_LEADER`, `QUORUM_UNAVAILABLE`, or `UNKNOWN`.
- `finish_read_index(context)` cleans up ephemeral protocol state.
- `authoritative_refresh_element` always waits for a completed read barrier, then returns the element and the applied-index metadata; `min_commit_index` remains an additional freshness fence, not a replacement for the quorum check.
- `spec.GUARANTEE['authoritative_refresh_element']` is now `linearizable` to reflect the actual API contract.

## Tests added and adapted

`tests/test_read_index.py` validates: the current-term committed-entry prerequisite; correlation to the correct read context; refusal of delayed/forged/unrelated replies; heartbeat rejection as leadership confirmation without changing replication progress; minority read unavailability; higher-term stepdown; future applied-index fences; safe follower commit bounds; strict DTO validation; and the reference-model guarantee declaration.

The two older runtime tests were changed to acknowledge the read's quorum heartbeat instead of expecting immediate local-only responses. Run the deterministic suite with:

```bash
python -m unittest discover -s tests -q
```

`tests/live_readindex_smoke.py` is a manually executable live three-replica regression. It uses the instructor's `cluster.py` and `chaos.py`, commits an edit on the new majority leader, probes the isolated former leader with an *older* valid `min_commit_index`, verifies refusal to return an authoritative stale value, and then checks convergence after healing:

```bash
python tests/live_readindex_smoke.py
```

The script tears down its local cluster and saves a machine-readable `readindex-live-evidence.json` artifact. It uses local ports 8001–8003 and should be run without an existing cluster using those ports.

## Stage B.1 client-journal checker evidence

The raw client-journal server history remains a complete audit. The professor's protected checker cannot reconstruct exact request identity, a Raft-assigned `min_commit_index`, or the `applied_index` returned by a ReadIndex operation, so the scenario also archives a transparent checker projection. It retains model-reproducible records verbatim, omits only repeated request identities and results containing those Raft-assigned indexes, and records each omission with its complete source record in a companion projection audit. Deterministic tests, the raw journal evidence, and the live ReadIndex fault test validate those omitted protocol concerns independently.
