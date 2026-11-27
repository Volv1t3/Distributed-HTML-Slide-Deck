# Stage F validation and completion report (#61)

Stage F adds immutable deck ownership, owner-managed VIEW/EDIT grants, a deck
role matrix, leader-bound protected reads, safe operation receipts,
permission-aware client recovery and a v3 schema fence with legacy quarantine
(ADR-012 to ADR-019, spec #53). This report covers the deterministic gate,
the clean three-replica live gate, the earlier-stage regressions and the
evidence archive. It names every gate that could not run live.

## How to run

```sh
# Deterministic gate (no cluster)
PYTHONPATH=. .venv/bin/python -m pytest -q -m "not integration"

# Live gate: each scenario on its own clean local three-replica cluster
python3 scenarios/run_stage_f.py                          # all four
python3 scenarios/run_stage_f.py --scenario stage_f_sharing
PYTHONPATH=. .venv/bin/python -m pytest -q -m integration tests/test_stage_f_validation.py
```

`run_stage_f.py` refuses to start if `run/` already exists. It starts three
local replicas, runs the scenario, copies the evidence to
`historiales/<timestamp>-<scenario>/` (Raft `disk.json`, `roles.log`,
`events.log` and every scenario JSONL file), and stops the cluster even when
an assertion fails. All replicas run the same Stage F binary, so the runner
sets `STAGE_F_ALL_VOTERS_READY=1` and the leader commits the v3 activation
fence. The legacy scenario is the exception: it starts from the completed
Stage E release (`3ecced2`, extracted with `git archive`) and performs the
rolling upgrade itself. Only local processes run; nothing contacts an
external service.

## Results at a glance

| Gate | Result |
|---|---|
| Deterministic suite | **674 passed**, 8 integration tests deselected, 35 subtests (baseline before #61: 660) |
| `stage_f_sharing` live | **PASSED** |
| `stage_f_partition` live | **PASSED** |
| `stage_f_failover` live | **PASSED** |
| `stage_f_legacy` live | **PASSED** (migration apply step **unavailable live**, see below) |
| Stage E live regressions (5 scenarios) | **PASSED** after fixture updates |
| Stage A live regressions (5 scenarios) | **PASSED** (checker convergence caveat, see below) |
| Stage B, C and D live regressions | **Not re-run**: blocked by intentional Stage F read changes, see below |
| Literal password/bearer scan of every run tree | 0 findings in all Stage F runs |
| Replicated token digests in public projections | 0 findings in all Stage F runs |

## Unavailable live gates

1. **Legacy migration apply (`migrate_legacy_deck`).**
   `App.propose_legacy_deck_migration` is an in-process operator entry point.
   It has no HTTP route and no operator transport, and it must not run at
   startup, election or tick (ADR-018). A live cluster therefore has no way to
   propose a migration. The legacy scenario proves everything up to that
   point: an authenticated v2 deck written by Stage E binaries, unchanged
   replay through a rolling upgrade, quarantine after the fence, and identical
   retained provenance on all three voters. That provenance is the committed
   `publish_deck` entry stamped with Sofia's account, plus a `PUBLISHED`
   dedup record at the same index. The migration itself (provenance
   verification, owner/access attachment, idempotent retry, restart replay
   and replacement leader) is covered only by
   `tests/test_stage_f_legacy_migration.py`. The archive records this as a
   `live_gate_unavailable` probe.
2. **`query_access_state` and `resolve_registered_user`.** Spec §11 defines
   these as an owner-only roster read and an owner-only exact-email lookup.
   Neither exists on `codex/stage-f` at e26b6ba: they are not in
   `PROTECTED_READ_OPERATIONS`, and the adapter answers `INVALID_REQUEST`.
   They cannot be validated, live or deterministically, until they are built.
   This is an implementation gap from an earlier slice, not something this
   ticket can fill.

## Defect found and fixed

Validation found a confidentiality gap in spec §9. A revoked editor could
replay an exact committed request ID and receive the full stored result from
the Raft deduplication table. For example, a `DECK_SETTINGS_UPDATED` replay
returned the deck name and revisions. Commit `283a98a` checks the caller's
current committed role before disclosing a stored result. The check covers
the eleven role-guarded deck mutations and owner-only grant changes. A caller
who no longer holds the required role receives `{"status": "ACCESS_DENIED"}`
and no log entry is appended. A current owner's exact replay of its own grant
change still returns the stored result. Deterministic test:
`test_revoked_editor_cannot_recover_a_full_result_by_exact_id_replay`. Live
proof: `stage_f_sharing` probe `revocation_privacy.replay_status = ACCESS_DENIED`.

## Deterministic coverage (acceptance criterion 1)

| Area | Tests |
|---|---|
| Schema cutover, v2/v3 decoding, activation at N/N+1, replay, readiness | `test_stage_f_schema_activation.py`, `test_stage_e_publish_deck_auth.py`, `test_runtime_integration.py` |
| Grant statuses, precedence, revision independence, ordering | `test_stage_f_grants_and_permissions.py`; `spec.check_committed_access_order` tests in `test_stage_f_validation.py` (edit-before-revoke and revoke-before-edit) |
| Role matrix across structure/element/lock/settings, v2 after fence | `test_stage_f_grants_and_permissions.py` |
| Privacy masking (unknown equals inaccessible, follower refusal, self-only `query_grants`) | `test_stage_f_protected_reads.py` |
| Receipt safety (whitelist, actor binding, rejection indistinguishable from unknown, exact-ID replay masking) | `test_stage_f_protected_reads.py` |
| Journal recovery (scoped outbox, gate, restart, monotonic observations, no auto-resend, renewal stop) | `test_stage_f_client_recovery.py`, `test_stage_f_owned_drafts.py`, `test_client_journal.py`, `test_stage_e_journal_auth.py` |
| Proof-gated migration | `test_stage_f_legacy_migration.py` |
| Evidence helpers, reference, projection, latency buckets, scans, runners | `test_stage_f_validation.py` |

## Live scenarios (acceptance criterion 2)

Archived JSONL evidence from the final run is preserved in
`docs/stage-f-evidence/<scenario>/`, with local paths redacted to `<repo>`.
The complete local archives, including each replica's `disk.json`,
`roles.log` and `events.log`, are under `historiales/` (gitignored):
`20261126-235229-182856-stage_f_sharing`,
`20261126-235244-652322-stage_f_partition`,
`20261126-235248-222584-stage_f_failover` and
`20261126-235254-249464-stage_f_legacy`.

### `stage_f_sharing`: publication, sharing, downgrade and revocation

Accounts: Sofia (owner), Mateo (participant) and Elena (nonparticipant). Steps:

- The fence is applied on all replicas at index 3.
- A v3 publication binds Sofia as owner. Publishing for another owner returns
  `OWNER_MISMATCH`, and a v2 publication after the fence returns
  `UNSUPPORTED_PRESENTATION_SCHEMA`.
- The new deck is private. Mateo's `query_grants` and `get_deck_preview`
  results for it are byte-identical to those for an unknown deck
  (`OK/NONE`, `ACCESS_DENIED`). His edit attempt is denied.
- A direct protected read to a follower returns only a redirect to the
  leader.
- With VIEW, Mateo can read the preview and slide. His edit and his attempt
  to share are denied.
- Grant ordering: a stale revision returns `ACCESS_VERSION_CONFLICT`, then
  `ACCESS_UPDATED` (EDIT), then `ACCESS_UNCHANGED`. Owner and unregistered
  targets return `INVALID_ACCESS_TARGET` and `INVALID_TARGET_USER`.
- With EDIT, Mateo's element create, fenced update and deck settings change
  succeed.
- After a downgrade to VIEW, a still-valid lock does not override the role
  (`ACCESS_DENIED`), and the lock itself is preserved.
- After revocation, a second revoke returns `ACCESS_NOT_FOUND`, and Mateo's
  content and role reads match those for an unknown deck. His exact-ID replay
  is masked. Receipts return `COMMITTED_SUCCESS`, `COMMITTED_REJECTED` and
  `OUTCOME_UNKNOWN`, and Elena's receipt for Mateo's request ID returns
  `OUTCOME_UNKNOWN`.
- The applied index converged at 23 on all replicas.

### `stage_f_partition`: stale followers, minority reads and race order

- A follower is partitioned while the majority commits Mateo's revocation.
  Its durable state still shows the stale EDIT grant at access version 1,
  against version 2 on the leader. Mateo's direct read and permission query
  there return `NO_LEADER`, so no stale success is served. Through the
  majority, his read returns `ACCESS_DENIED`. After healing, the follower
  converges to version 2.
- The leader is then isolated as a one-node minority. The owner's preview and
  slide reads there, and the revoked user's `query_grants`, all return
  `QUORUM_UNAVAILABLE`: no content and no false `NONE`. The majority elects a
  new leader, and the owner's read through it succeeds.
- Both commit orders were run with requests overlapping in flight, using a
  200 ms injected delay on the leader's outbound Raft messages:
  - Edit before revoke: `DECK_SETTINGS_UPDATED`, then `ACCESS_REVOKED`.
  - Revoke before edit: `ACCESS_REVOKED`, then a committed `ACCESS_DENIED`.

  Log indices confirm each order, and the committed access-order reference
  agrees.
- One leader per term. The applied index converged at 15.

### `stage_f_failover`: failover, client restart and preserved-disk replay

- Mateo's durable per-account, per-deck `JournaledClient` must verify its role
  freshly, then poll through the leader before its first journaled edit
  commits.
- The leader is killed and a replacement is elected in 0.62 s.
- The installation restarts from its private directory. Its gate starts
  suspended, then reopens after a fresh EDIT observation, and an edit commits
  on the new leader.
- After revocation, a journaled edit ends `REJECTED_FINAL`
  (`ACCESS_DENIED`). The gate becomes suspended with observed role `NONE`,
  and a further intention is retained unsent.
- After a second restart and an owner regrant, a fresh EDIT observation does
  not release the review gate. Nothing is resent automatically. The retained
  intention keeps its ID and patch, and the read-only recovery view shows the
  denied patch. Receipts return `COMMITTED_SUCCESS`, `COMMITTED_REJECTED` and
  `OUTCOME_UNKNOWN` (never sent).
- The old leader restarts on its preserved disk, then all three replicas are
  killed and restarted on preserved disks. Ownership, grants, revisions and
  the fence are identical before and after, and the applied index is 12, then
  13.

### `stage_f_legacy`: rolling upgrade and quarantine

- Stage E binaries write an authenticated v2 deck with a settings change and
  a locked element.
- Each voter is upgraded to the Stage F binary on its preserved disk,
  followers first. Without coordination, no activation is committed and
  publication returns `PRESENTATION_SCHEMA_UNAVAILABLE`.
- After a coordinated restart, the fence commits at index 9, above the v2
  publication at index 3. The legacy deck record is byte-identical on every
  replica before and after replay.
- The quarantined deck's `query_grants` and preview results match those for
  an unknown deck. An edit and a grant on it are denied. A v2 publication
  after the fence is refused, and a v3 publication succeeds.
- Migration provenance is retained identically on all voters. The live
  migration apply step is **unavailable** (see above).
- A whole-cluster preserved-disk replay leaves the fence and the legacy deck
  unchanged.

## Archive contents (acceptance criterion 3)

Each scenario's manifest lists its files:

- **Checker:** `*_checker_history.jsonl`, `*_projection_audit.jsonl` and
  `*_checker.jsonl`. The unchanged `check.py` found every object linearizable
  and one leader per term in all four scenarios. The projection stamps the
  server-derived `actor_user_id` per client installation and strips
  Raft-assigned `applied_index` from reads. It excludes three kinds of record
  and audits each exclusion:
  - role-guarded mutations denied at apply (checked by the access reference
    instead);
  - operation receipts (their proof is the retained Raft log, which the
    per-deck model lacks);
  - publication-boundary rejections.
- **Role and fault traces:**
  - `*_role_trace.jsonl`: the client-observed role, status, access version
    and applied index for each request.
  - `*_access_reference.jsonl`: `spec.check_committed_access_order` replays
    the committed log independently and predicts every role decision.
  - `*_faults.jsonl`: injections and election timings.
  - `n*/roles.log` (in `historiales/`).
- **Auth and access summaries:** `*_access_summaries.jsonl` holds, per
  replica, the owner, sorted grant roster, access, deck and settings
  revisions, and fence. It contains no token digests, and the scenario fails
  if the three replicas differ.
- **Request-result and applied-index convergence:** `*_request_results.jsonl`
  holds the durable dedup outcome of every committed mutation on all three
  replicas, compared for equality. Applied indices are in the probes and the
  manifest.
- **Latency:** `*_metrics.jsonl` keeps separate buckets: login, protected
  read, read denial, permission query, receipt, effective grant change,
  rejected grant command, authorized edit, committed role denial, and
  unanswered requests. Representative p50 values from `stage_f_sharing`:
  protected read 5.2 ms, permission query 3.7 ms, receipt 3.8 ms, grant
  change 10.5 ms, authorized edit 11.5 ms, role denial 8.6 ms and login
  107 ms. The partition race buckets include the deliberate 200 ms delay.
- **Scanner:** `secret_scan` probe. The whole `run/` tree, including Raft
  disks, is scanned for every fixture password and issued bearer. Public
  projections (all run-root JSONL files and `events.log`) are scanned for
  every replicated token digest. Both scans found nothing, and the journal
  directory was also bearer-free.

## Earlier-stage regressions (acceptance criterion 4)

- **Deterministic Stage A to E:** all tests pass, inside the 674 total.
  Fixture updates for protected reads were made earlier on `codex/stage-f`,
  in commits #58 and #59. This ticket weakened no Stage F guard.
- **Stage E live:** all five scenarios pass (`stage_e_normal`, `rotation`,
  `failover`, `lost_login`, `partition`) after three intentional fixture
  updates in commit `742b21c`:
  1. `run_stage_ea.py` marks its identical-binary voters coordinated and
     waits for the v3 fence before the scenario starts.
  2. The scenarios publish v3 decks owned by the signed-in scenario account
     (`owned_deck`).
  3. The shared helpers `assert_all_replicas_deck_settings`,
     `assert_all_replicas_slide_settings` and `assert_all_replicas_lock_state`
     now compare each replica's applied durable snapshot instead of sending
     an anonymous follower-local read.
- **Stage A live:** all five KV scenarios pass, and `check.py` reports every
  object linearizable with one leader per term. Its KV convergence line
  prints `NO CONVERGE` because `spec.converged_ok`, unchanged since Stage B
  (`7b38c47`), models only presentation reads. Stage F did not cause this.
- **Stage B, C and D live: not re-run.** These scenarios record anonymous,
  per-replica presentation reads in their histories (`client.final`,
  `get_slide_obs`, `wait_for_*_settings_version`, `wait_for_lease_epoch`),
  publish v2 decks, and feed `check.py` without an actor. Stage F
  intentionally refuses all of that. Porting them means authenticated
  leader-read history, v3 publication and actor stamping across about 4,000
  scenario lines. That work is left open and is not claimed here.

### Deviations from older eventual-read assumptions

- Presentation reads (`get_deck_preview`, `get_slide`, `get_element`,
  `read_lock_status`, `authoritative_refresh_element`, `refresh_element`)
  used to be follower-local or optionally local. They are now linearizable,
  leader-only and bearer-checked. Followers redirect or fail, and a minority
  leader returns `QUORUM_UNAVAILABLE`. `"local": True` is ignored for these
  operations.
- A per-replica convergence check can no longer use a deck read. It uses
  `/status` applied-index equality plus the replica's durable snapshot.
- The Stage A generic KV `get` is unchanged and still eventual.
- New publications need the coordinated v3 fence and a v3 snapshot owned by
  the publisher. A v2 publication is unavailable before the fence and
  unsupported after it.
- A stored dedup result for a deck command is disclosed only to a caller
  whose current role still permits that command.

## Notes for reviewers

- `docs/stage-f-evidence/` adds about 350 KB of redacted JSONL so that the
  evidence survives outside the gitignored `historiales/`.
- Stage E's own `*_auth_summaries.jsonl` still records `token_digest` by
  design. Stage F's public projections never do, and the Stage F scanner
  enforces this.
- The fixture passwords are the classroom test credentials in
  `fixtures/auth/catalog-v1.json`. They are scanned for, never archived.
