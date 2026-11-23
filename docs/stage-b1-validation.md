# Stage B.1 Validation

Stage B.1 validates distributed presentation editing without replacing or modifying the instructor runtime. Every authoritative request still uses `node.py`'s `POST /client` path, the existing Raft adapter, the existing fault controls, and the existing `check.py` checker. Client-journal evidence exercises the public `JournaledClient`/`ClientIntentionOutbox` seam above that same request path.

## Deterministic gate

Use the repository's shared environment:

```bash
/Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/.venv/bin/python -m unittest discover -s tests -v
```

The gate covers publication and slide structure, the typed element and lock lifecycle, replicated request deduplication, eventual observations, bounded authoritative refresh, durable journal replay, dependency materialization, supersession, monotonic polling, and manual reconciliation. `tests/test_stage_b1_validation.py` additionally validates the independent checker model and archive-compatible evidence contracts.

## Clean three-replica live gate

Run all scenarios in sequence:

```bash
/Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/.venv/bin/python scenarios/run_stage_b1.py
```

The runner creates a new three-replica local cluster for each scenario and always calls the unchanged `cluster.py down` path afterward. A single scenario can be selected with `--scenario NAME`.

The ADR-007 stale-former-leader regression is a second mandatory live gate:

```bash
/Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/.venv/bin/python tests/live_readindex_smoke.py
```

It proves an isolated former leader refuses an authoritative refresh, the majority returns the newer committed element, the read appends no Raft command, and all replicas converge after healing.

| Scenario | Independent evidence |
|---|---|
| `stage_b_normal` | Publication, create/reorder/delete slide structure, atomic create-and-lock, repeated edits, exact duplicate retry, eventual reads, convergence, edit-versus-read p50/p95. |
| `stage_b_concurrency` | Two overlapping acquisitions of one element, exactly one owner, and concurrent edits of two distinct elements. |
| `stage_b_replica` | Follower stop, majority progress, ordinary restart with preserved disk, and catch-up. |
| `stage_b_lider` | Pending edit on an isolated leader, leader failure, exact-ID retry on the replacement, preserved-disk restart, and measured election interruption. |
| `stage_b_particion` | Minority write without acknowledgement, majority-side progress, healing, and convergence. |
| `stage_b_client_journal` | Abrupt client crash after server send but before response handling, durable in-flight recovery with exact-ID retry, offline creation and dependent update, deletion supersession, lagging polling, and user-directed conflict correction after fresh lock and authoritative refresh. |

## Archived evidence

Every evidence artifact is JSONL under `run/`, so the unchanged archive workflow copies it into the timestamped `historiales/` directory together with `roles.log` and each replica's `events.log`:

- the primary checker history;
- the captured checker result and complete stdout;
- the latency summary with quorum-edit and Local Eventual Read p50/p95;
- fault and exact-retry probes;
- the scenario manifest;
- the raw Durable Intention Journal and client-journal lifecycle evidence where applicable.

The protected checker receives operation arguments but cannot reconstruct request identity or Raft-assigned indexes. Exact duplicate retries are therefore omitted from its projection and retained in the complete server audit. Conflict and Authoritative Refresh Read operations remain checker-visible after only `min_commit_index` or `applied_index` is removed from the projected result. Deterministic deduplication tests and the ReadIndex protocol/live fault tests supplement that transparent limitation; no fabricated server operations are added to a checker history.

The client-journal scenario therefore preserves a complete server audit containing every request and a checker projection containing every model-reproducible operation. A companion projection audit records the omitted exact duplicate and the original/projected forms of each index-normalized conflict or authoritative read.

## Boundaries

Automatic lock renewal/expiration remains Stage C scope. The scenarios use ordinary preserved-disk restarts, not erased-disk reintroduction. They make no claim beyond the starter's process-kill durability mechanism or the fixed three-voter configuration.
