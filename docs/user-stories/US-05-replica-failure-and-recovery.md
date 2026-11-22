# US-05 — Follower Failure and Recovery During Collaborative Editing

- **Status:** Proposed executable acceptance story
- **Related:** US-02, US-03, ADR-001 L-13
- **Required course scenario:** `replica`
- **Scenario script:** `scenarios/replica.py`
- **History:** `run/replica.jsonl`
- **Fault:** One follower is killed and later restarted **with durable state intact**

## User story

**As Sofia and Santiago**, we want to continue editing a shared deck when one backend replica fails, and see that replica recover all committed edits and lock state when it returns, so that one server failure does not lose our work or block the surviving majority.

## Preconditions

- The three-node cluster has a stable leader. The test identifies a **follower dynamically** through `status()`; do not kill a hardcoded node that may be leader.
- The fixture is created by a **recorded** `publish_deck` operation. It contains `E_TITLE` and `E_SUBTITLE`, each version 1 and unlocked.
- A lock owner has an active session; expiration is disabled for the deterministic history or maintained through committed renewals. Kill only one replica at a time.

## Main success path

| Step | Action | Expected observation |
|---|---|---|
| R-01 | Sofia publishes deck `D` and selects `E_TITLE` | `publish_deck` and `acquire_lock` confirmed; title owned by Sofia |
| R-02 | Determine which of `n1`,`n2`,`n3` is a follower; record that identity | Fault target is **not** the current leader |
| R-03 | `chaos.main(["kill", follower])` | Exactly two replicas remain available, forming quorum 2/3 |
| R-04 | Sofia commits `update_element` on title, v1 -> v2 | `SAVED`, no dependence on the unavailable follower |
| R-05 | Santiago acquires `E_SUBTITLE`, edits it v1 -> v2, releases | All three writes commit on surviving majority |
| R-06 | Sofia releases her title lock | Committed release while follower still down |
| R-07 | `chaos.main(["start", follower])` **without** `--empty` | Replica restarts using its preserved disk and obtains missing log entries |
| R-08 | Poll eventual `get_slide(local=True)` on recovered follower until the expected applied state is present | Recovered copy includes both v2 edits, both released locks, and nondecreasing fencing counters |
| R-09 | `Client.final("get_slide", deck_id=D, slide_id=S1)` | All three replicas report equal finalized state |

Santiago's editing session is allowed to use the same Raft cluster while Sofia holds the separate title lock; independence is important. No confirmation may depend on the failed follower acknowledging the write.

## Alternative paths and errors

- **Follower is already down when a client requests a local preview:** preview can fail or time out. This is not a violation of the majority's ability to commit.
- **Follower returns before fully caught up:** it must not claim authoritative current state or acknowledge new shared writes on its own. Local eventual reads may lag while it catches up, but the final-state checkpoint occurs after convergence.
- **Follower restart is slow:** the harness polls for a bounded test duration and reports non-convergence as a failure; do not replace the check with a fixed sleep treated as evidence.
- **Returned follower reports an old lock as held:** that is permitted only as a temporarily stale local preview; it must learn the committed release before final verification.
- **`start --empty`:** deliberately excluded from this normal recovery story; erased Raft voting metadata requires its own safe reintroduction policy and is documented as a model boundary unless implemented.

## Acceptance criteria

| ID | Given / When | Then |
|---|---|---|
| US05-AC01 | One follower fails | A healthy pair can continue committing authorized acquisitions, updates, and releases |
| US05-AC02 | Edit is acknowledged while follower is down | Its committed content and element version survive recovery |
| US05-AC03 | Lock is acquired or released while follower is down | Returned follower eventually reconstructs correct ownership, fencing counter, and current version |
| US05-AC04 | Failed follower returns with disk | It catches up through Raft log/state transfer without shared disk access |
| US05-AC05 | All nodes are healthy and writes stop | Three final local snapshots agree |
| US05-AC06 | Edits and local reads occur during the scenario | Latency and availability are recorded, including the cost while operating with only 2/3 nodes |

## Evidence and checker expectations

- `run/replica.jsonl` contains the publication, all operations and one final snapshot read per replica; raw fault event timestamps / selected follower identity go in a separate scenario trace.
- Run and save `python3 check.py run/replica.jsonl`. Confirm no write-order violation, eventual convergence, and no duplicate leader per term in `roles.log`.
- Assert title v2, subtitle v2, no current locks, unchanged or increased persistent fencing counters, and identical final values on n1/n2/n3.
- Compare strong-write latency (`update_element`) with local-preview latency (`get_element` or `get_slide`) in the same history.

## Definition of done

Any one follower can be killed and later restored with its original disk while clients continue to use the two-node majority. The recovered replica has every confirmed operation in the right order and all three snapshots converge.
