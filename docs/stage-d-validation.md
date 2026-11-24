# Stage D Validation

Stage D adds independently versioned Deck Settings and Slide Settings to the
existing schema-v2 presentation model. Every authoritative request still uses
`node.py`'s `POST /client` path, the unchanged Raft adapter, the existing fault
controls, and the existing `check.py` checker. Client-journal FIFO lanes and
settings deduplication evidence exercise the public `JournaledClient` /
`ClientIntentionOutbox` seam above that same request path.

The Gate distinguishes three independent counters:

- **`deck_version`** — incremented by structural mutations (publish, create,
  reorder, delete slide).
- **`deck_settings_version`** — incremented exclusively by `update_deck_settings`
  commands; Canvas Resize must NOT advance `deck_version` or change any element
  or Lock Authority field.
- **`slide_settings_version`** — per-slide counter, independent across slides;
  incremented exclusively by `update_slide_settings` commands.

Stage E access-control boundaries are intentional future scope and must not be
anticipated here.

## Deterministic gate

Use the repository's shared environment:

```bash
/Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/.venv/bin/python -m unittest discover -s tests -v
```

The gate covers all prior stages plus:

- **`tests/test_stage_d_deck_settings.py`** — DTO strict validation, state-machine
  transitions, Canvas Resize Invariant (zero element/lock field changes), dedup/
  replay, and reference-model agreement for `update_deck_settings`.
- **`tests/test_stage_d_slide_settings.py`** — DTO strict validation, per-slide
  state-machine, independence invariant across slides, `NOT_FOUND`/`SLIDE_DELETED`
  paths, dedup/replay, and reference-model agreement for `update_slide_settings`.
- **`tests/test_stage_d_settings_journal.py`** — flush-before-send ordering, FIFO
  within a single settings resource, cross-resource independence (deck vs. slide,
  two slides), dependency materialization, deletion supersession, conflict→NMR,
  discard, corrected resubmission, and polling monotonicity.

Expected total: ≥ 336 deterministic tests passing.

## Clean three-replica live gate

Run all five Stage D.1 scenarios in sequence:

```bash
/Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/.venv/bin/python scenarios/run_stage_d1.py
```

The runner creates a new three-replica local cluster for each scenario and
always calls the unchanged `cluster.py down` path afterward. A single scenario
can be selected with `--scenario NAME`.

| Scenario | Independent evidence |
|---|---|
| `stage_d_normal` | Publish schema-v2 deck, rename (DSV 0→1), canvas resize (DSV 1→2), Canvas Resize Invariant across all replicas (element geometry/version/lock preserved), two independent slide backgrounds, deck_settings_version stability during slide changes, Derived Slide Ordinals after create/reorder/delete. |
| `stage_d_concurrency` | Phase A: independent-resource concurrent deck rename + slide2 background both succeed. Phase B: same-resource race for slide1 produces exactly one `SLIDE_SETTINGS_UPDATED` and one `SLIDE_SETTINGS_VERSION_CONFLICT` with `min_commit_index`. Phase C: discard path. Phase D: corrected resubmission with conflict-returned revision → SSV 2. |
| `stage_d_replica` | Follower stop, majority commits deck and slide settings + structural changes, preserved-disk restart, full applied-index catch-up, final settings/ordinal convergence on all replicas. |
| `stage_d_lider` | Isolated leader with pending `update_deck_settings`, leader failure, exact-ID retry on replacement leader proves one increment, `REQUEST_ID_REUSE` with different payload rejected. |
| `stage_d_particion` | Minority cannot confirm settings (no `DECK_SETTINGS_UPDATED`), majority commits deck rename + slide background + structural change, partition healed, all replicas converge on settings and structure, polling monotonicity verified, settings-only commit visible on recovered replica. |

Aggregate timing is written to `historiales/stage_d1_metrics.json`. Each
scenario writes its own `run/<scenario>_metrics.jsonl` with p50/p95 quorum-
confirmed settings-write latency and p50/p95 Local Eventual Read latency.

## Archived evidence

Every artifact is JSONL under `run/`, copied into a timestamped `historiales/`
directory by the unchanged archive workflow together with `roles.log` and each
replica's `events.log`:

- **`<scenario>.jsonl`** — raw client history with invoke/complete timestamps.
- **`<scenario>_checker.jsonl`** — checker projection (index fields stripped,
  exact duplicates excluded).
- **`<scenario>_projection_audit.jsonl`** — full normalization audit with original
  and projected forms.
- **`<scenario>_checker.jsonl` (checker output)** — checker stdout/returncode
  confirming all modelled operations satisfy the sequential reference model.
- **`<scenario>_metrics.jsonl`** — p50/p95 edit and read latencies plus observed
  unavailability seconds.
- **`<scenario>_manifest.jsonl`** — scenario boundary, evidence file list, and
  runtime constraints.
- **`<scenario>_probes.jsonl`** — fault injection probes (partition/heal timing,
  minority rejection, applied-index monotonicity).

The checker receives operation arguments but not request identity or Raft-
assigned indexes. Exact duplicate retries are excluded from the projection and
retained in the full audit. `min_commit_index` is stripped from conflict
responses; `committed_index` is stripped from settings-updated responses. No
fabricated server operations are added to any checker history.

## Invariant validation commands

Canvas Resize Invariant is verified inside `stage_d_normal` by reading every
element from every replica immediately after `update_deck_settings` with
`width_px`/`height_px` and asserting each field (geometry, version, lock state)
is byte-identical to its pre-resize snapshot. The same assertion is encoded in
`tests/test_stage_d_deck_settings.py` for deterministic coverage.

Derived Slide Ordinal correctness is verified inside `stage_d_normal` after
every structural transition (create, reorder, delete) by reading `slide_ids`
from `get_deck_preview` and comparing to the expected sequence.

## Known limitations

- Scenarios use ordinary preserved-disk restarts, not erased-disk reintroduction.
- Three-voter fixed configuration; dynamic membership changes are not modelled.
- Stage E access-control boundaries are future scope; no enforcement is added
  here and evidence files make no access-control claims.
- The checker cannot reconstruct exact-ID deduplication or Raft-assigned indexes
  from public operation arguments alone; both are covered by deterministic tests
  and the transparent projection audit.

## Whitespace validation

```bash
grep -rn $'\t' /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_normal.py \
     /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_concurrency.py \
     /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_particion.py \
     /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_replica.py \
     /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_lider.py \
     /Users/santiagoarellano/Documents/Projects/Distributed-HTML-Slide-Deck/scenarios/stage_d_support.py
```

Expected: no output (zero tab characters).
