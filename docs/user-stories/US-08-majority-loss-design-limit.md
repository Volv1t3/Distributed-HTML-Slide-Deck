# US-08 — Design Limit: Majority Loss and Safe Refusal to Commit

- **Status:** Proposed executable acceptance story
- **Related:** US-03, ADR-001 L-14, course requirement for an intentional failure/boundary scenario
- **Required course scenario:** Deliberately exercise a documented system limit
- **Scenario script:** `scenarios/limite.py`
- **History:** `run/limite.jsonl`
- **Fault:** Stop **two of three** replicas simultaneously, leaving no majority

## User story

**As Sofia**, I want the editor to preserve my already confirmed work and explicitly tell me when it cannot safely save new shared changes, so that losing a majority does not produce false success or conflicting distributed locks. I accept temporary unavailability when the system exceeds its one-replica failure tolerance.

## System-model boundary being tested

- The three-replica Raft group tolerates the failure of **one** replica for continued committed writes.
- With **two** replicas stopped, no 2/3 quorum exists. No remaining node can safely commit a new log entry, including lock acquisition, renewal, update, or release.
- The intended failure is **availability/liveness**, not loss of a confirmed edit or a violation of safety. The project report should state precisely which assumption was exceeded.
- Local or follower-applied `get_slide` previews are still permissible and may be stale. The browser must show that such views are not proof of a successfully committed save.

## Preconditions

1. A fresh cluster has three healthy replicas and a stable leader. The script logs `publish_deck(D, ...)` with `E_TITLE` and `E_SUBTITLE` at version 1.
2. Sofia acquires `E_TITLE`, commits `update_element` from v1 to v2 (e.g., text = `"Confirmed before failure"`), and receives `SAVED`. She releases the lock. The resulting committed state is the durability baseline.
3. The test identifies node roles and selects two distinct nodes to kill. It records the survivors so it can later restart the **same** nodes with their original durable disks.
4. Any automatic expiration that could change checked state is suppressed for the graded deterministic history or explicitly accounted for through a separate timed test.

## Main expected-failure path

| Step | Action | Expected observation |
|---|---|---|
| B-01 | Publish deck, acquire title, commit title update v1 -> v2 and release | Prior content is confirmed and replicated to a majority |
| B-02 | Kill two of the three replicas using `chaos.main(["kill", name])` for each target | One replica remains; quorum 2/3 is impossible |
| B-03 | Sofia submits a new `acquire_lock(E_TITLE)` to the surviving node | No `LOCK_ACQUIRED` confirmation without quorum; request may time out, redirect unsuccessfully, or yield `ok:false` |
| B-04 | Santiago attempts to create another element or acquire `E_SUBTITLE` | Cannot receive `CREATED_AND_LOCKED` or `LOCK_ACQUIRED` until majority restored |
| B-05 | If a previously held token exists, attempt `update_element` with it during loss of quorum (separate controlled subcase) | No `SAVED` response may be confirmed for a newly proposed edit |
| B-06 | Query `get_slide` from the surviving node | May return the last locally applied snapshot, but it cannot imply that B-03–B-05 were committed |
| B-07 | Restore **one** failed node with its disk intact | Quorum of two can become available; election/replication may need time |
| B-08 | Retry an uncertain logical command using its original request ID, or read state and submit a new intent | Only a legitimately committed command can be acknowledged; if a retry commits, its result must be deterministic and not duplicated |
| B-09 | Restore the last failed replica; wait until all three catch up | All three nodes converge on the committed history, including the original v2 title |
| B-10 | Invoke one `Client.final("get_slide", deck_id=D, slide_id=S1)` | All three final snapshots agree |

For B-05, establish a distinct valid owned lock before removing quorum if necessary; do not attempt to edit with a token that was already released and misinterpret `STALE_TOKEN` as a failure caused by quorum loss. If B-03/B-04 require majority commits to record application rejections, timeout is an acceptable and expected outcome.

## Alternative paths and errors

- **An old leader remains alive alone:** it cannot infer continued majority from its `role` variable. Any new successful write reply without actual commitment is a critical safety defect.
- **The surviving node redirects to a dead leader:** the client may time out; this is not a successful operation. After restoration, eventual leader discovery is required for progress.
- **Sofia's UI still displays her private draft:** the draft is not a replicated confirmed edit. The editor should mark it `UNVERIFIED`, not `SAVED`.
- **A pending write later commits after recovery:** that is allowed if it remains a valid command and the system obtains a quorum; inspect its request ID and final order rather than assuming every timed-out command is absent.
- **Two replicas return empty-disk:** this test deliberately does **not** use `start --empty`. Loss of Raft voting/log metadata requires separately defined safe reintroduction rules and should not be conflated with the majority-loss availability boundary.

## Acceptance criteria

| ID | Given / When | Then |
|---|---|---|
| US08-AC01 | Two replicas are unavailable | No live replica acknowledges a newly proposed shared write as committed |
| US08-AC02 | Sofia attempts an edit without quorum | Client reports uncertainty/unavailability and retains unsaved draft |
| US08-AC03 | A local preview remains available | It is explicitly classified as eventual and does not grant lock authority |
| US08-AC04 | A pre-failure edit was acknowledged | It survives restart and appears in the recovered replicated state |
| US08-AC05 | One stopped replica returns with disk | A stable majority can eventually resume valid commits |
| US08-AC06 | All nodes return and new writes stop | Every replica's final element versions/content/lock state converge |
| US08-AC07 | Same logical request is retried after an uncertain outcome | It is not applied more than once; stale token/version checks still apply |
| US08-AC08 | Project report describes the outcome | It identifies failure of **write availability beyond f=1**, not a demonstrated failure of Raft safety |

## Evidence and checker expectations

- Preserve `run/limite.jsonl`, results of `check.py`, role/term logs, the identities of the two stopped replicas, and a trace of fault/recovery times.
- In `client.py`, an unsuccessful call has `ok:false` and `complete:null`; `check.py` treats this as possibly executed or not executed. It does **not** assume that a timeout necessarily means no commit.
- Directly inspect whether a new request ever receives `LOCK_ACQUIRED`, `CREATED_AND_LOCKED`, or `SAVED` from the lone surviving node **before** quorum returns; do not rely only on the checker summary.
- Preserve one post-recovery `Client.final("get_slide", ...)` and verify the original acknowledged title v2. The final state may include a post-recovery retry if its operation legitimately committed.
- Record the span during which writes received no successful responses; contrast with eventual local-read latency.

## Definition of done

The system intentionally becomes unavailable for new commits when two replicas fail. It never falsely confirms an edit or lock, retains pre-failure acknowledged data, and resumes after quorum is restored. The report explains this as a documented limit of the three-node majority model.
