# US-07 — Network Partition: Majority Progress, Minority Safety, and Healing

- **Status:** Proposed executable acceptance story
- **Related:** US-02, US-03, ADR-001 L-11 and L-12
- **Required course scenario:** `particion`
- **Scenario script:** `scenarios/particion.py`
- **History:** `run/particion.jsonl`
- **Fault:** One replica is isolated from the other two; afterward the partition is healed

## User story

**As Sofia and Santiago**, we want a shared presentation to protect its committed edits and locks during a network partition. We accept that the isolated minority cannot confirm new edits, while users connected to a majority can keep working on available elements and all copies eventually converge after healing.

## Preconditions

1. Start three healthy replicas and record `publish_deck(D, ...)` with two elements, `E_TITLE` and `E_SUBTITLE`, both v1.
2. Determine the current leader dynamically. Prefer to isolate this **incumbent leader** from the two other replicas, so the remaining majority must elect a replacement.
3. Sofia obtains and holds `E_TITLE` before the partition. The title is intentionally locked by an editor whose later network access to the majority may be limited. Leave `E_SUBTITLE` FREE for Santiago to edit on the majority.
4. Prevent unrecorded automatic expiration from changing checked state in this primary run. A second, supplemental temporal run can exercise committed expiration and reassignment.

## Main success path

| Step | Actor/fault | Action | Expected result |
|---|---|---|---|
| P-01 | Sofia | Publish `D` and acquire title lock | Committed shared state includes Sofia's title ownership |
| P-02 | Fault injector | `chaos.main(["partition", isolated_leader, "/", other1, other2])` | Replica communication splits 1 vs 2; client HTTP endpoints remain accessible |
| P-03 | Majority pair | Elect higher-term leader among two communicating replicas | Majority can again commit Raft commands |
| P-04 | Isolated replica | Probe a **new** `acquire_lock` or `update_element` request directly against the isolated node without following HTTP/application redirects | No successful new commit may be reported by that isolated node; it may redirect, time out, or return `ok:false` |
| P-05 | Santiago through majority | `acquire_lock(E_SUBTITLE)` | `LOCK_ACQUIRED` committed by 2/3 |
| P-06 | Santiago through majority | `update_element(E_SUBTITLE, expected_version=1, token=T)` | `SAVED`, subtitle version 2 |
| P-07 | Santiago through majority | `release_lock(E_SUBTITLE,T)` | `RELEASED` committed by 2/3 |
| P-08 | Both sides | Read eventual `get_slide` from isolated and majority replicas | Isolated view may be stale, even if it still claims an old leader role locally; no preview grants authority |
| P-09 | Fault injector | `chaos.main(["heal"])` | Replica-to-replica communication restored |
| P-10 | Cluster | Wait until formerly isolated replica observes higher term and applies missing commits | All nodes include subtitle v2 and final lock status |
| P-11 | Sofia | Release her title lock through current leader (if still held) | Normal committed release, no stale-token escape |
| P-12 | Test harness | `Client.final("get_slide", deck_id=D, slide_id=S1)` | Identical final presentation and lock state on all three replicas |

A majority can accept modifications to a FREE element, but **it cannot bypass Sofia's already committed title lock**. To edit `E_TITLE`, Santiago must wait for a valid release, a committed expiration, or an explicitly authorized invalidation (if that optional feature is implemented).

## Critical starter nuance: what “minority rejects” means

The starter's `chaos.py partition` blocks **node-to-node messages only**, not clients. A browser/client connected to the isolated old leader can still reach the healthy majority by redirecting through the client network.

Moreover, `Client.call(op, node="n1")` follows `{"redirect":"n2"}` *before* honoring its pinned-node retry logic. Consequently:

- A request that originated at the isolated node but is **redirected and committed by the majority** is not a minority write and does not violate safety.
- To prove the isolated **replica itself** cannot commit, use a separate one-hop `POST /client` helper that does not follow application redirects, and record `target`, returned redirect/error/timeout, and the absence of a commit response from that node.
- Do **not** use a successful rerouted `Client.call` as evidence that the minority made progress.

This direct probe may belong in a supplementary JSON trace; the official `Client` `.jsonl` remains the primary checker history.

## Alternative paths and errors

- **Old leader retains `role=leader` temporarily:** it still lacks a majority and cannot confirm fresh writes. On receiving a newer term after healing, it steps down.
- **Majority cannot obtain title lock:** expected `LOCKED` while Sofia holds the title; this is independent of whether the network has a majority.
- **Sofia's release does not reach the majority:** title remains held. Its abandonment is handled by US-03 expiration, not by pretending a client-side deselection released it.
- **Santiago's write completes during healing:** its committed log position determines its application order. The returning node must not overwrite the majority's newer value with its stale copy.

## Supplemental extension: owner isolation, expiration, reassignment

To demonstrate the *full* US-03 temporal story (in a separate targeted trace): isolate Sofia's session with the minority, stop her renewals to the majority, let the majority leader commit `expire_lock(E_TITLE, expected_token, expected_lease_epoch)`, then let Santiago acquire the newly FREE title with a larger fencing token and edit it. After healing, Sofia's old-token save/release must return `STALE_TOKEN` and every replica must converge to Santiago's state.

Do **not** silently mix this automatic internal expiration with a standard `check.py` history that has no trace of the expiry; the checker would lack a causal state transition to reconstruct.

## Acceptance criteria

| ID | Given / When | Then |
|---|---|---|
| US07-AC01 | Network splits into 1 and 2 | Minority cannot independently confirm a fresh shared write/lock |
| US07-AC02 | Two replicas remain connected | New or existing majority leader can commit valid operations |
| US07-AC03 | Sofia's title lock predates partition | Neither side may grant conflicting title ownership until valid release/expiration |
| US07-AC04 | Santiago edits unlocked subtitle on majority | Edit is committed and remains after healing |
| US07-AC05 | Minority serves a local preview | It may be stale but cannot authorize an edit |
| US07-AC06 | Network heals and writes stop | All three final slide snapshots agree, including element versions and locks |
| US07-AC07 | Isolated old leader rejoins | It observes later term/log and cannot resurrect obsolete content |
| US07-AC08 | Supplemental expiry/reassignment occurs | Old token cannot modify or release the new owner's lock |

## Evidence and checker expectations

- Primary `run/particion.jsonl`: recorded setup and majority operations, plus one `Client.final("get_slide",...)` after heal/catch-up. Avoid checking unmodeled internal expirations in this primary history.
- Supplemental `run/particion-probes.json`: who was isolated, leader terms, one-hop direct outcomes, and operation timestamps; precise name is a project convention, not a starter requirement.
- Run and retain `python3 check.py run/particion.jsonl` output and role logs. Inspect explicit 1-vs-2 operations separately; a checker pass does not by itself show which physical side accepted the request.
- Measure strong-write latency while election occurs and local read latency on an isolated but running replica.

## Definition of done

The isolated node cannot commit authoritative modifications without quorum, the majority commits edits on free elements subject to existing locks, the old leader gives way after healing, and all three replicas converge. The separate time-based extension verifies safe expiration and fencing.
