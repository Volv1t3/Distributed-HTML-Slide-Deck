# Distributed Slides — Distributed-System Test Stories

**Status:** Proposed acceptance-test specification; implementation pending  
**Date:** 2026-11-22  
**Language:** English  
**Normative dependencies:** `ADR-001-element-level-distributed-locking.md`, `user-story-02-concurrent-editing-and-element-locking.md`, `user-story-03-failure-recovery-and-lock-expiration.md`, `CMP5001-Proyecto.pdf`, `cmp5001-proyecto-starter.zip`  
**Purpose:** Define the five required course scenarios *before* deciding how to adapt an earlier Raft implementation.

## Scope and scenario registry

| Story | Starter scenario | Fault introduced | Core evidence | Related decisions |
|---|---|---|---|---|
| [US-04 — Normal Collaboration](US-04-normal-collaboration.md) | `scenarios/normal.py` | None | Publish, atomic create-and-lock, independent edits, conflicts, release, final convergence; compare strong-write and local-read latency | ADR L-01–L-05, L-09; US-02 |
| [US-05 — Follower Failure and Recovery](US-05-replica-failure-and-recovery.md) | `scenarios/replica.py` | Kill and restart one **follower**, preserving disk | Majority commits while follower is down; recovered follower catches up | ADR L-13; US-03 |
| [US-06 — Leader Failure During Concurrent Writes](US-06-leader-failure-during-concurrent-writes.md) | `scenarios/lider.py` | Kill the leader during requests from two threads | New leader, no loss of acknowledged writes, no duplicate application, mutually exclusive locks | ADR L-09–L-10, L-16; US-02, US-03 |
| [US-07 — Network Partition and Healing](US-07-network-partition-and-healing.md) | `scenarios/particion.py` | Isolate one node, preferably the incumbent leader, from the other two | Minority no commits; majority progress; convergence after healing | ADR L-11–L-12; US-03 |
| [US-08 — Availability Boundary](US-08-majority-loss-design-limit.md) | `scenarios/limite.py` | Stop two of three replicas | No majority, so no confirmed new writes; previously committed state survives recovery | ADR L-14; US-03 |

These stories test three replicas (`n1`, `n2`, `n3`) and two simulated editors (`sofia`, `santiago`). Names identify test sessions and are **not** authorization credentials. The UI is optional for tests; the official scripts exercise the replicated backend directly.

## Shared contracts and fixtures

1. Start with an empty cluster and wait for a stable Raft leader before publishing. For every scenario, call a logged `publish_deck` at the beginning, so the checker can reconstruct state from the same history.
2. The published fixture contains a deck and one slide with independent elements, `E_TITLE` and `E_SUBTITLE`, each initially at **version 1** and **unlocked**. Use complete UUIDv4 identifiers for actual created objects; readable names here are aliases in the test explanation.
3. Every mutating command in shared mode goes through the Raft log. `create_element` **atomically creates and initially locks** the element. A selected existing element requires `acquire_lock`; saving requires `update_element` with session, token, and expected element version. Deselecting requires `release_lock`.
4. Check statuses inside successful transport replies, e.g. `{"ok":true,"result":{"status":"LOCKED"}}`. A rejection (`LOCKED`, `STALE_TOKEN`, `VERSION_CONFLICT`) is a **valid application response**, not a lost quorum. A true infrastructure problem uses timeout / `ok:false` / redirect.
5. `req.client` represents the editor session. `req.id` is the request ID. `Client.call(...)` internally reuses its ID during automatic retries but creates a *new* ID for a new call; a deliberate replay of the exact same ID requires a supplemental direct HTTP helper or test hook. Never make a retry with a new ID and call it an idempotency test.
6. Save the primary `.jsonl` history under `run/`; after healing and log catch-up, call **one** `Client.final("get_slide", deck_id=..., slide_id=...)` per history. The checker groups eventual final results by `spec.key()`: avoid mixing unrelated `get_element` and `get_slide` final reads for the same deck key in a single history.
7. Use `spec.key(...) = deck_id` for publication, lock, element mutations, and deck/slide reads in the prototype. `spec.apply(...)` must model a deterministic single-copy deck including locks, fencing counters, element versions, and deduplication outputs. Do not include nondeterministic clock timestamps or leader-specific metadata in checker-comparable results.
8. The normal scenario must cover **every implemented user-facing application operation** (even optional features if actually implemented). Standard graded histories should not include **unrecorded internal lock expirations** that change checked state: use stable active renewals or a scenario configuration with automatic expiration disabled. The complete lock-expiration algorithm remains required by ADR-001 and is tested in a separate focused harness with recorded internal events (see below).
9. The five scenarios use **durable ordinary restarts** (`chaos.py start nX`), not erased-disk reintroductions (`--empty`), unless safe Raft reintroduction is explicitly implemented. The latter is a separate documented limit, not silently treated as a normal restart.
10. Store `check.py` output for every scenario and report its limitations. In particular, 'no violations' on an empty or inapplicable checker section does not establish lock/lease correctness; supplement with explicit invariants and state assertions.

## Testing restrictions inherited from the starter

- `node.py` calls application callbacks serially (`start`, `on_request`, `on_message`, `on_tick`) and ticks at approximately 50 ms.
- `chaos.py` supports `kill`, `start`, `pause`, `partition`, `delay`, and `heal`. A partition interrupts **replica-to-replica messages**, but HTTP clients can still contact every live replica.
- **Important:** even with `Client.call(..., node="n1")`, `client.py` follows any `redirect` response. Therefore, `node=` alone does **not** prove a minority replica cannot commit. Use a one-hop direct `POST /client` probe that records a redirect without following it, or correlate the request with Raft logs. Keep such raw-probe evidence separately from the principal `Client` history.
- `client.final` reads all replicas with `local=True`; perform it only after healing, restart, and observed catch-up. An eventual read can be stale while recovery is in progress; the final checkpoint must have converged.
- `check.py` tests linearizability of operations labeled `linearizable`, final convergence for `eventual` operations, at most one leader **per term** from `roles.log`, per-operation latency, and gaps between successful responses. It does **not** prove lock exclusivity across all possible schedules or expiry correctness without a faithful spec and supplemental tests.
- Each story should provide: scenario script, its `.jsonl` history, its checker text output, and concise observations about operation results and latency. The project ZIP and report must include the required evidence.

## Standard execution pattern

```bash
python3 cluster.py up --local
python3 scenarios/normal.py
python3 check.py run/normal.jsonl > run/normal.check.txt
# Repeat with a fresh cluster for each of the other scenarios.
# Avoid calling `cluster.py down` before saving evidence: `down` archives run/ histories.
python3 cluster.py down
```

For the scenario scripts, reuse `Client` and `chaos.main([...])`; use a `threading.Thread` per concurrent editor in the leader-failure story. Ensure the main script waits for the cluster to stabilize and for final-state synchronization rather than relying on an arbitrary fixed sleep as proof.

## Supplemental safety and temporal tests (outside the five grading histories)

These extend **US-03** but are not substitutes for the five scenarios:

- **EX-01 Renewal wins:** `renew_lock` commits with epoch N+1 before an `expire_lock(token, epoch=N)` applies. The expiration must be `NO_OP_STALE_EXPIRY`.
- **EX-02 Expiration wins:** `expire_lock(token, epoch=N)` commits first; late `renew_lock` returns `STALE_TOKEN`; the next acquisition receives a larger token.
- **EX-03 Delayed release:** A release with an old fencing token cannot release a lock held by a new session.
- **EX-04 New leader:** A new leader reconstructs its *own* monotonic inactivity timers and allows a fresh grace period for inherited locks; an old already-proposed expiration, if later committed, still obeys token/epoch guards.
- **EX-05 Minority-side old owner:** A client isolated from the majority retains an unsaved draft. After majority-side committed expiration and reassignment, the old client is rejected and cannot overwrite new content following healing.

Because `client.py` cannot record internal `expire_lock` entries on its own, use a separate transparent state-machine/log trace in these supplementary tests. Do not insert fabricated client calls into the official history or treat their checker verdict as proof of those internal transitions.

## Definition of done for this test-story package

- All five named scenario scripts exist and use the agreed operation contracts.
- Each primary history can be checked with `check.py` using the implemented `spec.py`.
- Each story's explicit acceptance criteria are asserted independently of the checker when necessary.
- `roles.log` is available to inspect leader terms in the failover scenarios.
- Both strong write latency (`acquire_lock` or `update_element`) and local eventual read latency (`get_element` or `get_slide`) are captured within the same scenario.
- Recovery and final convergence are verified on **all three replicas**.
- The availability boundary (two failed replicas) is reported as a **liveness/availability failure**, not a demonstrated safety or linearizability violation.

## Deferred integration question — earlier Raft homework

The earlier Raft homework source is **not part of the uploaded starter or the available documents inspected for this package**, so no compatibility decision is made here. When it is provided, compare its persistence, terms/votes, AppendEntries, log conflict resolution, commit/apply ordering, recovery, request deduplication, and reply semantics with `node.py`'s callbacks and `self.disk` / `self.save()` interface. Reuse correct Raft mechanisms in `app.py`; do not replace the starter's `node.py`, `cluster.py`, `client.py`, or `chaos.py` with laboratory versions.
