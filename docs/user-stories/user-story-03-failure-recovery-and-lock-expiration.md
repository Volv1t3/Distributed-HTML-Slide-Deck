# User Story 03 — Failure Recovery and Lock Expiration

- **ID:** US-03
- **Project:** Distributed Slides
- **Status:** Ready for implementation planning; not yet implemented
- **Priority:** Core safety and recovery; expiration can follow the non-expiring-lock implementation milestone
- **Actors:** Sofia, Santiago, their editor sessions, Raft leader and followers
- **Depends on:** US-01 (shared deck), US-02 (element-level locks and edits), [ADR-001 — Element-Level Distributed Locking](ADR-001-element-level-distributed-locking.md)
- **Related ADR tests:** L-06–L-08, L-10–L-16

## 1. User story

**As a collaborator** on a shared presentation, I want abandoned element locks to become available again after disconnects, without allowing an old editor to overwrite new work, **so that** collaboration can resume safely after leader failures and network partitions.

**As the person responsible for demonstrating the distributed system**, I want predictable safety rules and testable recovery outcomes so that I can explain what a majority can and cannot do during failures.

The design prioritizes **safety over availability**: operations requiring the Raft log may stop when a majority is unavailable, but the system must never confirm incompatible lock ownership or overwrite committed content using stale authority.

## 2. Preconditions and system assumptions

1. The deck is `SHARED` and the three replicas are independent, with no shared memory or disk. A majority of **two of three** is required to commit authoritative changes.
2. A selected element has an active replicated lock record containing the owning session, `fencing_token`, and `lease_epoch`.
3. All acquisitions, renewals, releases, edits, deletions, and expirations use Raft commits; ordinary local previews may be eventually consistent.
4. The current leader uses its **local monotonic clock** to *propose* inactivity expirations. Local time never directly changes committed lock state.
5. The initial suggested settings are `LOCK_INACTIVITY_TIMEOUT = 10 s`, `LOCK_RENEW_INTERVAL = 3 s`, and starter `TICK = 50 ms`. These are configuration examples, **not exact-time guarantees**.
6. Ordinary crash recovery assumes nodes retain the durable Raft metadata and data needed by their specified recovery procedure. Disk-erased rejoin is a distinct boundary case.
7. Client sessions are logical identifiers, not authenticated real-world identities. Tokens prevent stale requests in the state model; they do not provide production authentication.

## 3. Main success scenario — a disconnected editor's lock expires

1. Santiago selects E1 and successfully commits `acquire_lock`, receiving `fencing_token = N`, `lease_epoch = 0`, and E1's current content version.
2. Santiago remains selected. His client periodically sends `renew_lock(deck_id, E1, N)` before the configured inactivity timeout.
3. The leader orders each renewal through Raft. At committed application, the lock's `lease_epoch` advances. The leader updates its **ephemeral monotonic activity timer** for the current lock generation and epoch.
4. Santiago unexpectedly loses connectivity. His browser no longer receives confirmed renewals or saves. It marks the editing session `UNVERIFIED` and preserves unsaved work as a local draft.
5. The majority-side leader eventually observes no new committed lock activity for the configured inactivity period and proposes a single internal `expire_lock` carrying `(E1, expected_token=N, expected_lease_epoch=current_epoch)`.
6. The proposal is replicated and committed through Raft. The state machine validates that E1 still has the indicated token and epoch.
7. If both match, expiration removes E1's active lock but **retains the per-element fencing counter**. The authoritative state is now `FREE`.
8. Sofia requests `acquire_lock` for E1. After a Raft commit, she receives a new `fencing_token = N+1` (or a larger strictly increasing value).
9. Sofia saves an edit using her new token and the current E1 content version; the change is committed and acknowledged.
10. Santiago reconnects and attempts to save his older draft using token N. The server responds `STALE_TOKEN`, without changing Sofia's content or version.
11. Santiago refreshes the element to the committed version; he may preserve his draft locally and, if he wants to retry, must acquire a **new** lock and reconcile against current content.
12. After connectivity is restored, all replicas eventually apply the same lock generation, fencing counter, element version, and content.

**Success postcondition:** The abandoned lock is recoverable, an old owner cannot modify the resource, the replacement owner can commit changes, and surviving replicas converge after healing.

## 4. Important safety rule — expiration is a committed transition

The lock's state machine is:

```text
FREE --acquire_lock/create_element--> HELD(token=N, epoch=0)
HELD(token=N, epoch=K) --renew_lock--> HELD(token=N, epoch=K+1)
HELD(token=N, epoch=K) --release_lock--> FREE
HELD(token=N, epoch=K) --expire_lock(token=N, epoch=K)--> FREE
HELD(token=N, epoch=K) --delete_element--> FREE + tombstone
```

A timeout **does not itself release a lock**. It only prompts the leader to propose a command. A follower or isolated former leader cannot expire a lock unilaterally. Every replica applies the same outcome from the committed Raft log.

`expire_lock` is an **internal** command. It must not be exposed as a public API available to any client. A user-initiated `force_unlock` would require authorization and its own decision; it is not assumed here.

## 5. Renewal/expiration race

Suppose E1 is `HELD(token=7, lease_epoch=2)`. Two commands may be in flight:

| Committed order | Applied outcome | Final authority |
|---|---|---|
| `renew_lock(7)` then `expire_lock(7,2)` | Renewal increments epoch to 3; expiration becomes `NO_OP_STALE_EXPIRY` | Existing owner retains token 7 |
| `expire_lock(7,2)` then `renew_lock(7)` | Expiration releases; later renewal is `STALE_TOKEN` | Resource is free until a new acquisition |

Neither client-side timestamps nor UUID ordering decide the outcome. **Raft commit order** determines which valid state-machine transition takes effect. `lease_epoch` guards against late expiration of a more recently renewed lock.

## 6. Leader failure while a lock is held

1. Sofia holds E1 through a previously committed acquisition and may have a committed renewal.
2. The Raft leader fails. No replica may claim a new lock or a saved update merely because the failed leader had received a request.
3. With two surviving replicas, Raft elects a new leader using a higher term. Committed lock state, fencing counters, versions, and deduplication results remain authoritative.
4. The new leader reconstructs **ephemeral** inactivity tracking from its committed state and allows a fresh full grace period before proposing expiration for inherited locks. It must not compare monotonic timestamps originating on the old leader.
5. A previously proposed expiration in the Raft log can still become committed; therefore the grace period is a policy for **new** expiration proposals, not a promise that no expiration will occur during failover.
6. Clients whose response outcome is unknown retry the exact same logical request with the same `request_id`. Deduplication returns a previously committed outcome or permits one safe application if it had not committed.
7. The replacement leader may confirm edits/renewals only after they are committed by a majority.

**Required outcome:** No previously acknowledged lock or edit is lost, no two sessions receive incompatible active ownership, and stale tokens do not become valid again after election.

## 7. Network partition scenarios

### 7.1. Partition into a minority (one node) and majority (two nodes)

1. The network splits into `n1 / (n2, n3)`.
2. The isolated side may serve **eventually consistent local reads**, potentially showing outdated element and lock state.
3. The isolated side cannot commit `acquire_lock`, `renew_lock`, `release_lock`, `update_element`, `create_element`, `delete_element`, or `expire_lock`.
4. The majority elects or retains a valid Raft leader and can commit those operations, subject to the existing lock owner and version guards.
5. If the isolated client owns an element, the majority **does not immediately discard the lock**. It can only reassign the element after a committed valid release or expiration.
6. Once the inactivity policy allows it, the majority may commit `expire_lock`; another session may then acquire the element with a larger fencing token.
7. When the network heals, the former minority catches up from the authoritative log before claiming current state. Previously isolated clients synchronize; their old tokens and stale drafts do not override later committed work.

**Important distinction:** Majority availability is not the same as immediate availability of a particular locked element. Until its committed lock expires or is released, the element remains held even if its owner is unreachable.

### 7.2. Minority-side edit or renewal is attempted

**Given** Sofia is isolated with a local copy indicating she owns E1. **When** she attempts `renew_lock` or `update_element`. **Then** the request cannot receive a successful committed result. The UI retains a draft and displays `UNVERIFIED` rather than asserting that E1 is safely locked or saved.

### 7.3. A quorum cannot form

**Given** two of three replicas have failed, or no partition has two mutually communicating replicas. **When** a client submits a lock acquisition, renewal, release, or edit. **Then** the operation cannot be acknowledged as committed. Local previews may still be available; authoritative edits are unavailable until a majority is restored.

## 8. Other alternative and error paths

### US03-A1 — Late renewal after committed expiration

A renewal carrying an old token reaches the current leader after expiration. It is rejected with `STALE_TOKEN`; it does not resurrect the previous lock or overwrite a newly acquired token.

### US03-A2 — Stale expiration proposal arrives after successful renewal

The expiration's `expected_lease_epoch` no longer equals the current epoch. Its committed application returns `NO_OP_STALE_EXPIRY` and leaves the current owner unchanged.

### US03-A3 — Delayed release arrives after lock reassignment

The old token no longer matches the active lock. `release_lock` returns `STALE_TOKEN`; it must not release the new owner's lock.

### US03-A4 — Client deselects while disconnected

The client attempts `release_lock` but receives no confirmed outcome. The UI may deselect immediately but must not assert the server-side lock was released; it remains held until release or expiration is committed. The reconnecting client may retry its original release request ID.

### US03-A5 — A follower crashes and rejoins

While a follower is down, the two healthy replicas continue committing edits and lock transitions. When it rejoins with valid persistent Raft metadata, it catches up missing log/state before reporting itself current. The final element and lock state must match the other replicas.

### US03-A6 — A node restarts with an erased disk

Losing term/vote/log state is **not** equivalent to ordinary restart and must not be assumed safe without a documented reintroduction mechanism. This is an explicit system-model boundary unless implemented separately and demonstrated.

### US03-A7 — The leader fails before returning an answer

The client cannot conclude whether its command committed. It retries with the **same** `request_id`; Raft and replicated deduplication prevent double application. If no majority is available, the client must not receive a false success.

### US03-A8 — Timer precision is poor

Network latency, scheduling pauses, and elections delay expiration beyond 10 seconds. The application makes **no exact deadline guarantee**. Safety is preserved because the decision is a committed state transition and every edit checks its current token.

## 9. Operations and guarantees

| Operation/process | Guarantee | Mechanism | Who initiates it? |
|---|---|---|---|
| `renew_lock` | Linearizable | Raft-committed conditional increment of `lease_epoch` | Lock-holding client while selected |
| `expire_lock` | Linearizable transition | Leader-only proposal; Raft commit; token + epoch guard | Current leader, on inactivity tick |
| `acquire_lock` | Linearizable | Raft + conditional `FREE` check + incremented fencing counter | Client acquiring or reacquiring |
| `release_lock` | Linearizable | Raft + matching current owner/token | Client on deselection |
| `update_element` | Linearizable | Raft + current token + expected element version | Client save |
| `get_element` / `get_slide` | Eventual consistency | Local applied replica state | Client preview/polling |
| `read_lock_status` | Eventual consistency | Local applied replica lock map | Client lock indicator |
| Raft leader election | Raft safety | Terms, durable votes, log freshness, majority | Replicas |
| Raft follower catch-up | Convergence to committed state | Log replication or consistent state transfer | Leader/follower runtime |

An `ok:true` response with a rejection status such as `STALE_TOKEN` is an **application outcome**; `ok:false`, redirect, or timeout indicates an **infrastructure/coordination outcome**. A node without a quorum must not return a successful lock or edit result.

## 10. Non-functional properties and observability

- **Safety:** One active owner per element; token and version guards on every shared modification; no false commits.
- **Durability:** A confirmed acquisition/edit/renewal survives any single replica crash under the defined persistence assumptions.
- **Recovery:** An abandoned lock eventually becomes free when a stable majority and leader persist long enough for expiration to commit; there is **no bounded liveness guarantee** during arbitrary failures.
- **Convergence:** After network healing and cessation of writes, replicas eventually agree on current content, versions, locks, and fencing counters.
- **Client behavior:** Pending/uncertain operations are visibly distinct from committed operations; unsaved drafts remain local until revalidated.
- **Measurable cost:** Every confirmed renewal consumes Raft log replication, unlike a cheap local eventual read. Record latency (e.g., p50/p95), election unavailability, and periods without successful responses.

## 11. Acceptance criteria (Given / When / Then)

| ID | Given | When | Then | ADR mapping |
|---|---|---|---|---|
| US03-AC01 | E1 is HELD with token 7, epoch 2 | Renewal commits before old expiration | Epoch becomes 3; stale expiration is no-op | L-06 |
| US03-AC02 | E1 is HELD with token 7, epoch 2 | Expiration commits before renewal | Lock becomes free; renewal rejected; next token greater than 7 | L-07 |
| US03-AC03 | E1 was reassigned | Previous owner's release arrives | New lock remains held | L-08 |
| US03-AC04 | A lock was committed | Leader dies during competing acquisitions | New leader preserves committed authority and only valid outcomes are confirmed | L-10 |
| US03-AC05 | Network splits 1 vs 2 | Clients request writes on each side | Majority may commit; minority cannot confirm writes | L-11 |
| US03-AC06 | Lock owner is isolated in minority | Majority expires, reassigns, network heals | Old token rejected; all replicas converge | L-12 |
| US03-AC07 | A follower is stopped | Writes commit on other two and follower restarts | Follower catches up full element and lock state | L-13 |
| US03-AC08 | Two replicas unavailable | Client attempts lock/update | No new write is confirmed | L-14 |
| US03-AC09 | Client deselects and release is interrupted | UI changes focus | Does not falsely claim lock released; expiration can recover | L-15 |
| US03-AC10 | E1 has committed renewals | Leader changes | New leader rebuilds timers; invalid tokens still rejected | L-16 |
| US03-AC11 | Client has an unsaved draft during partition | Majority reassigns lock | Draft cannot overwrite newer committed content without fresh lock/version check | ADR invariants 4–5 |
| US03-AC12 | Inactivity exceeds configured timeout | No stable majority exists | No unilateral expiration/lock grant is acknowledged | ADR invariants 2, 8 |

## 12. Test scenarios and checker limitations

### Required distributed-system scenarios

1. **Normal operation:** Acquire, renew, edit, release; verify versions and convergence.
2. **Replica failure/restart:** Crash one follower, continue renewals/updates with 2/3, restore follower and compare final state.
3. **Leader failure under concurrency:** Concurrent clients attempt lock operations while the leader is killed; retry stable request IDs; verify confirmed results persist.
4. **Network partition and healing:** Separate 1/2, isolate a lock owner on the minority, allow a majority-side committed expiration and reacquisition, heal and verify stale-token rejection and convergence.
5. **Design-limit failure:** Lose two replicas or break the required majority; show that commits cease rather than fabricating success.

### Additional targeted temporal tests

- **T1:** Renewal commits before a previously proposed expiration; expiration is stale.
- **T2:** Expiration commits first; subsequent renewal fails.
- **T3:** A delayed release from an older token cannot release a new owner.
- **T4:** A leader changes immediately after renewal; no old-clock interpretation or token resurrection occurs.

**`check.py` limitation:** `client.py` history logs may omit automatic internal `expire_lock` transitions. As a result, the checker may be unable to explain a later lock acquisition using only recorded client operations. Keep the standard linearizability/convergence scenarios deterministically auditable and create a separate targeted test for expiration or add transparent instrumentation for internal log transitions. Never invent user commands in the checker history.

## 13. Implementation sequencing

1. First implement the US-02 state-machine locking model and Raft integration with expiration disabled.
2. Persist `lease_epoch`, fencing counters, and deduplication results as part of the replicated state.
3. Add client-side periodic `renew_lock` (example: every 3 seconds while selected).
4. Add leader-only monotonic inactivity tracking and one in-flight `expire_lock` proposal per generation/epoch.
5. Apply expiration deterministically using expected token and epoch; do not replicate timestamps or independently expire in followers.
6. Add new-leader grace handling and tests for in-flight old expiration proposals.
7. Add client `UNVERIFIED` state, local drafts, reconnect/revalidate behavior, and error indicators.
8. Run crash/partition/recovery tests and supplementary renewal-versus-expiration race tests; document costs.

**Definition of done:** Every US03 acceptance criterion is demonstrated under the stated fault model; committed exclusivity is maintained through leader changes and partitions; expired locks can be safely reassigned; stale clients cannot overwrite newer work; and automatic expiration behavior is tested separately where generic histories cannot model it.
