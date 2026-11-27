# Stage F validation and completion report (#61)

Stage F adds immutable deck ownership, owner-managed VIEW/EDIT grants, a deck
role matrix, leader-bound protected reads, owner-only access management
reads, safe operation receipts, permission-aware client recovery and a v3
schema fence with legacy quarantine and proof-gated migration (ADR-012 to
ADR-019, spec #53). This report covers the deterministic gate, the clean
three-replica live gate, the earlier-stage regressions and the evidence
archive. It names every gate that could not run live.

## How to run

```sh
# Deterministic gate (no cluster)
PYTHONPATH=. .venv/bin/python -m pytest -q -m "not integration"

# Live gate: each scenario on its own clean local three-replica cluster
python3 scenarios/run_stage_f.py                          # all four
python3 scenarios/run_stage_f.py --scenario stage_f_sharing
PYTHONPATH=. .venv/bin/python -m pytest -q -m integration tests/test_stage_f_validation.py

# Earlier-stage live regressions under the Stage F binary
python3 scenarios/run_stage_b1.py
python3 scenarios/run_stage_c1.py
python3 scenarios/run_stage_d1.py
python3 scenarios/run_stage_ea.py
```

`run_stage_f.py` refuses to start if `run/` already exists. It starts three
local replicas, runs the scenario, copies the evidence to
`historiales/<timestamp>-<scenario>/` (Raft `disk.json`, `roles.log`,
`events.log` and every scenario JSONL file), and stops the cluster even when
an assertion fails. All replicas run the same Stage F binary, so the Stage B
to F runners set `STAGE_F_ALL_VOTERS_READY=1` and wait until every replica
has applied the v3 activation fence. The legacy scenario is the exception: it
starts from the completed Stage E release (`3ecced2`, extracted with
`git archive`) and performs the rolling upgrade itself. Only local processes
run; nothing contacts an external service.

## Results at a glance

| Gate | Result |
|---|---|
| Deterministic suite | **691 passed**, 8 integration tests deselected, 35 subtests (674 before the review fixes) |
| `stage_f_sharing` live | **PASSED**, including owner roster and exact-email resolution |
| `stage_f_partition` live | **PASSED** |
| `stage_f_failover` live | **PASSED** |
| `stage_f_legacy` live | **PASSED**, including the live migration apply step |
| Stage E live regressions (5 scenarios) | **PASSED** |
| Stage D live regressions (5 scenarios) | **PASSED** after porting |
| Stage C live regressions (5 scenarios) | **PASSED** after porting |
| Stage B live regressions (6 scenarios) | **PASSED** after porting |
| Stage A live regressions (5 scenarios) | **PASSED**; every KV object now reports `converge` |
| Literal password/bearer scan of every run tree | 0 findings in all Stage F runs and all ported Stage B to D archives |
| Replicated token digests in public projections | 0 findings in all Stage F runs |

## Unavailable live gates

None of the Stage F acceptance gates is unavailable any more. Two limits
remain and are deliberate:

1. **Migration provenance is not modelled by `check.py`.** The per-deck
   reference model applies `migrate_legacy_deck` as a state transition. The
   provenance proof lives in each replica's retained Raft log and dedup table,
   which the per-deck model does not have. The live scenario checks the proof
   separately: provenance is identical on all voters, and the deterministic
   suite covers every proof failure.
2. **The operator spool is a local channel.** `node.py` must stay unchanged,
   so there is no operator HTTP route. An operator triggers a migration by
   writing a request file into the leader's run directory (see below).

## Review fixes (after the #61 code review)

- **Owner management reads (S1, issues #56 and #58).** `query_access_state`
  and `resolve_registered_user` now exist. Both go through the same
  leader-bound bearer, readiness and ReadIndex pipeline as protected reads.
  The owner gets the grant roster sorted by target UUID with
  `access_version` and `applied_index`, or the minimal public profile
  (`user_id`, `first_name`, `last_name`, `email`) for one complete normalized
  email, or `USER_NOT_FOUND`. A non-owner, an unknown deck and a quarantined
  legacy deck all receive the same bare `ACCESS_DENIED`, before any catalog
  lookup. Wildcard (`*`, `%`, `?`), partial, whitespace-containing and
  oversize emails fail as `INVALID_REQUEST` before any ReadIndex. Both reads
  are linearizable in `spec.py`.
- **Masked replay classification (S2).** The server masks a retained deck
  result as `ACCESS_DENIED` when the caller lost access before an exact-ID
  resend (commit `283a98a`). The client journal used to treat that denial as
  terminal, so a committed edit whose first response was lost could be
  journaled as denied. The outbox now durably counts send attempts. A denial
  answering a resend keeps the original `PENDING_SYNC`, closes the gate and
  asks `query_operation_receipt`: `COMMITTED_SUCCESS` marks it committed,
  `COMMITTED_REJECTED` makes it terminal, and `OUTCOME_UNKNOWN` keeps it
  uncertain and unsent. A first-send denial stays terminal.
- **Operator migration trigger (S4).** Each replica serves explicit requests
  dropped into `<run>/<slot>/operator/<name>.json` with the body
  `{"operation": "migrate_legacy_deck", "deck_id": "<uuid>"}`. The request is
  consumed once and answered in `<name>.result.json`: `PROPOSED` with a log
  index on the leader, `NOT_LEADER` on a follower,
  `LEGACY_PROVENANCE_UNAVAILABLE` without proof, `INVALID_REQUEST` for any
  other body. Nothing runs unless a request file exists, so startup,
  elections and ticks still never migrate a deck (ADR-018 §4).

## Defect found and fixed during #61

Validation found a confidentiality gap in spec §9. A revoked editor could
replay an exact committed request ID and receive the full stored result from
the Raft deduplication table. Commit `283a98a` checks the caller's current
committed role before disclosing a stored result, for the eleven
role-guarded deck mutations and the owner-only grant changes. A caller who no
longer holds the required role receives `{"status": "ACCESS_DENIED"}` and no
log entry is appended. The client side of that masking is the S2 fix above.

## Deterministic coverage (acceptance criterion 1)

| Area | Tests |
|---|---|
| Schema cutover, v2/v3 decoding, activation at N/N+1, replay, readiness | `test_stage_f_schema_activation.py`, `test_stage_e_publish_deck_auth.py`, `test_runtime_integration.py` |
| Grant statuses, precedence, revision independence, ordering | `test_stage_f_grants_and_permissions.py`; `spec.check_committed_access_order` tests in `test_stage_f_validation.py` |
| Role matrix across structure/element/lock/settings, v2 after fence | `test_stage_f_grants_and_permissions.py` |
| Privacy masking, follower refusal, self-only `query_grants` | `test_stage_f_protected_reads.py` |
| Owner roster and exact-email resolution (state machine, App pipeline, model) | `test_stage_f_protected_reads.py` |
| Receipt safety and exact-ID replay masking | `test_stage_f_protected_reads.py` |
| Journal recovery, including masked-replay receipt resolution | `test_stage_f_client_recovery.py`, `test_stage_f_owned_drafts.py`, `test_client_journal.py`, `test_stage_e_journal_auth.py` |
| Proof-gated migration, operator spool, migration in the reference model | `test_stage_f_legacy_migration.py` |
| Stage A KV convergence in `spec.converged_ok` | `test_stage_a_scenarios.py` |
| Evidence helpers, reference, projection, latency buckets, scans, runners | `test_stage_f_validation.py` |

## Live scenarios (acceptance criterion 2)

Archived JSONL evidence from the final run is preserved in
`docs/stage-f-evidence/<scenario>/`, with local paths redacted to `<repo>`.
The complete local archives, including each replica's `disk.json`,
`roles.log` and `events.log`, are under `historiales/` (gitignored):
`20261127-001744-741186-stage_f_sharing`,
`20261127-001800-343443-stage_f_partition`,
`20261127-001804-303048-stage_f_failover` and
`20261127-001810-609101-stage_f_legacy`.

### `stage_f_sharing`: publication, sharing, owner discovery, downgrade and revocation

Accounts: Sofia (owner), Mateo (participant) and Elena (nonparticipant). Steps:

- The fence is applied on all replicas. A v3 publication binds Sofia as
  owner. Publishing for another owner is refused, and a v2 publication after
  the fence returns `UNSUPPORTED_PRESENTATION_SCHEMA`.
- The new deck is private. Mateo's `query_grants` and `get_deck_preview`
  results for it are byte-identical to those for an unknown deck
  (`OK/NONE`, `ACCESS_DENIED`). His edit attempt is denied. A direct
  protected read to a follower returns only a redirect to the leader.
- Owner discovery: Sofia resolves `" MATEO@EXAMPLE.TEST "` to Mateo's UUID
  with exactly the four public profile fields, gets `USER_NOT_FOUND` for an
  absent email and `INVALID_REQUEST` for a wildcard, and reads an empty
  roster at `access_version` 0. Mateo's roster and resolution attempts are
  byte-identical `ACCESS_DENIED` for the shared deck and an unknown deck.
- With VIEW, Mateo can read the preview and slide. His edit and his attempt
  to share are denied.
- Grant ordering: `ACCESS_VERSION_CONFLICT`, then `ACCESS_UPDATED` (EDIT),
  then `ACCESS_UNCHANGED`. Owner and unregistered targets return
  `INVALID_ACCESS_TARGET` and `INVALID_TARGET_USER`. The roster then shows
  Mateo with EDIT at `access_version` 2.
- With EDIT, Mateo's element create, fenced update and deck settings change
  succeed. After a downgrade to VIEW, a still-valid lock does not override
  the role, and the lock itself is preserved.
- After revocation, Mateo's content and role reads match those for an
  unknown deck, his exact-ID replay is masked, and receipts return
  `COMMITTED_SUCCESS`, `COMMITTED_REJECTED` and `OUTCOME_UNKNOWN`.
- The applied index converged at 23 on all replicas.

### `stage_f_partition`: stale followers, minority reads and race order

- A partitioned follower keeps a stale EDIT grant while the majority commits
  the revocation. Mateo's direct read and permission query there return
  `NO_LEADER`; through the majority his read returns `ACCESS_DENIED`. After
  healing, the follower converges.
- An isolated one-node minority leader answers the owner's reads and the
  revoked user's `query_grants` with `QUORUM_UNAVAILABLE`: no content and no
  false `NONE`. The new majority leader serves the owner.
- Both commit orders (edit before revoke, revoke before edit) were run with
  overlapping requests and a 200 ms injected delay. Log indices confirm each
  order, and the committed access-order reference agrees. One leader per
  term.

### `stage_f_failover`: failover, client restart and preserved-disk replay

- Mateo's durable per-account, per-deck `JournaledClient` verifies its role,
  polls through the leader and commits a journaled edit. The leader is
  killed, a replacement is elected, the installation restarts, and an edit
  commits on the new leader after a fresh EDIT observation.
- After revocation, a journaled edit ends `REJECTED_FINAL` (`ACCESS_DENIED`),
  the gate suspends, and later intentions stay unsent. After a restart and a
  regrant, the review gate stays closed, nothing is resent automatically, and
  the read-only recovery view shows the denied patch.
- Whole-cluster preserved-disk restarts leave ownership, grants, revisions
  and the fence identical.

### `stage_f_legacy`: rolling upgrade, quarantine and live migration

- Stage E binaries write an authenticated v2 deck with a settings change and
  a locked element. Each voter is upgraded to the Stage F binary on its
  preserved disk, followers first. Without coordination no activation is
  committed and publication returns `PRESENTATION_SCHEMA_UNAVAILABLE`.
- After a coordinated restart, the fence commits at index 9, above the v2
  publication at index 3. The legacy deck record is byte-identical on every
  replica before and after replay. The quarantined deck's `query_grants` and
  preview match those for an unknown deck, and an edit and a grant on it are
  denied.
- Migration: provenance (the committed `publish_deck` entry stamped with
  Sofia's account plus a `PUBLISHED` dedup record at the same index) is
  identical on all voters. An operator request to a follower's spool returns
  `NOT_LEADER`. The same request to the leader's spool returns `PROPOSED`,
  and `migrate_legacy_deck` commits at index 12 on all replicas. The deck
  becomes v3 owned by Sofia with a private empty grant map; content,
  settings, locks and revisions are unchanged. An exact retry returns the
  stored `LEGACY_DECK_MIGRATED`. Sofia's preview returns `OWNER` with the
  Stage E rename, and her roster is empty.
- A whole-cluster preserved-disk replay leaves the fence and the migrated
  deck unchanged. `check.py` linearizes the history including the recorded
  operator migration.

## Archive contents (acceptance criterion 3)

Each scenario's manifest lists its files:

- **Checker:** `*_checker_history.jsonl`, `*_projection_audit.jsonl` and
  `*_checker.jsonl`. The unchanged `check.py` found every object
  linearizable and one leader per term in all four scenarios. The projection
  stamps the server-derived `actor_user_id` per client installation and
  strips Raft-assigned `applied_index` from reads. It audits each exclusion:
  role-guarded mutations denied at apply (checked by the access reference
  instead), operation receipts, publication-boundary rejections, and read
  inputs rejected before ReadIndex.
- **Role and fault traces:** `*_role_trace.jsonl`,
  `*_access_reference.jsonl` (`spec.check_committed_access_order`),
  `*_faults.jsonl` and `n*/roles.log` (in `historiales/`).
- **Auth and access summaries:** `*_access_summaries.jsonl` holds, per
  replica, the owner, sorted grant roster, access, deck and settings
  revisions, and fence. It contains no token digests, and the scenario fails
  if the three replicas differ.
- **Request-result and applied-index convergence:**
  `*_request_results.jsonl` compares the durable dedup outcome of every
  committed mutation on all three replicas.
- **Latency:** `*_metrics.jsonl` keeps separate buckets. Representative p50
  values from `stage_f_sharing`: protected read 3.6 ms, read denial 3.5 ms,
  permission query 3.7 ms, receipt 3.5 ms, grant change 9.3 ms, rejected
  grant command 8.9 ms, authorized edit 9.4 ms, role denial 8.3 ms and login
  107 ms. The partition race buckets include the deliberate 200 ms delay.
- **Scanner:** `secret_scan` probe. The whole `run/` tree, including Raft
  disks, is scanned for every fixture password and issued bearer, and public
  projections are scanned for every replicated token digest. Both scans
  found nothing.

## Earlier-stage regressions (acceptance criterion 4)

- **Deterministic Stage A to E:** all tests pass, inside the 691 total. No
  Stage F guard was weakened.
- **Stage E live:** all five scenarios pass (`stage_e_normal`, `rotation`,
  `failover`, `lost_login`, `partition`).
- **Stage B, C and D live: all sixteen scenarios pass.** They were ported
  the same way Stage E was (commit `742b21c`):
  1. The runners mark identical-binary voters coordinated and wait for the
     v3 fence.
  2. Every scenario signs in as the scenario account and publishes v3 decks
     it owns (`owned_deck`). `ScenarioClient` is the starter `Client` with the
     bearer added only to the wire payload; `recorded_request` sends the
     identity's current bearer by default.
  3. Anonymous and follower-local presentation reads were replaced. A
     scenario's own reads go through the leader with the bearer, and
     per-replica convergence compares each replica's applied durable deck,
     slide, element or lock (`raft_state`, `durable_deck`,
     `durable_element`, `durable_lock_observation`).
  4. `finalize_checked_scenario` feeds `check.py` an audited projection
     (`*_actor_checker_history.jsonl`, `*_actor_projection_audit.jsonl`) that
     stamps the bearer-derived `actor_user_id` and removes Raft-assigned
     `applied_index` and `schema_version` from protected reads.
  5. Scenario-specific changes: `stage_b_lider` waits for
     `auth_admitted_read_started`, because bearer admission confirms a fresh
     ReadIndex before appending and an isolated leader therefore holds the
     edit at admission. `stage_b_client_journal`'s lagging-follower poll now
     proves the follower refuses to serve, and the pending draft and
     watermark survive. `stage_c_mayoria_loss` merges the committed
     server-originated lock expiration into its checker input, as
     `stage_c_particion` already did, because reads are now linearizable.
- **Stage A live:** all five KV scenarios pass, and `check.py` now reports
  `converge` for every object. `spec.converged_ok`, which `check.py` uses for
  every eventual final read, only understood presentation observations since
  Stage B (`7b38c47`). It now also accepts a bare KV value when some
  real-time-respecting order of the applied writes leaves that value.

### Deviations from older eventual-read assumptions

- Presentation reads are linearizable, leader-only and bearer-checked.
  Followers redirect or fail, and a minority leader returns
  `QUORUM_UNAVAILABLE`. `"local": True` is ignored for these operations.
- A per-replica convergence check can no longer use a deck read. It uses
  `/status` applied-index equality plus the replica's durable snapshot.
- The Stage A generic KV `get` is unchanged and still eventual.
- New publications need the coordinated v3 fence and a v3 snapshot owned by
  the publisher.
- A stored dedup result for a deck command is disclosed only to a caller
  whose current role still permits that command.

## Notes for reviewers

- `docs/stage-f-evidence/` holds about 350 KB of redacted JSONL so that the
  evidence survives outside the gitignored `historiales/`. It was refreshed
  from the final run after the review fixes.
- Stage E's own `*_auth_summaries.jsonl` still records `token_digest` by
  design. Stage F's public projections never do, and the Stage F scanner
  enforces this.
- The fixture passwords are the classroom test credentials in
  `fixtures/auth/catalog-v1.json`. They are scanned for, never archived.
