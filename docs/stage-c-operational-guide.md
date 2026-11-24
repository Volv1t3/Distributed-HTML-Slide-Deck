# Stage C Operational Guide

Stage C adds automatic lock renewal, committed lock expiration, and
Maintained Lock Confidence to the distributed presentation backend.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `LOCK_RENEWAL_INTERVAL` | `3.0` | Client renewal interval in seconds |
| `LOCK_INACTIVITY_TIMEOUT` | `10.0` | Server inactivity timeout in seconds |

**Startup validation**: both values must be positive and satisfy
`3 × LOCK_RENEWAL_INTERVAL ≤ LOCK_INACTIVITY_TIMEOUT`.  The coordinator raises
`ValueError` at startup if this constraint is violated.

### Stage C.1 test profile

```
LOCK_RENEWAL_INTERVAL=0.20 LOCK_INACTIVITY_TIMEOUT=0.80
```

## Response contracts

New statuses introduced in Stage C:

| Status | Command | Meaning |
|---|---|---|
| `RENEWED` | `renew_lock` | Lease Epoch incremented; element version unchanged |
| `STALE_TOKEN` | `renew_lock` | No active lock or token mismatch |
| `NOT_OWNER` | `renew_lock` | Token matches but session does not |
| `NOT_FOUND` | `renew_lock` | Deck, slide, or element does not exist |
| `EXPIRED` | `expire_lock` (internal) | Exact token+epoch match; lock removed, fencing counter retained |
| `NO_OP_STALE_EXPIRY` | `expire_lock` (internal) | Token or epoch no longer current; no state change |
| `INVALID_REQUEST` | `expire_lock` (public client) | Public clients may not invoke `expire_lock` |

Existing successful lock responses (`CREATED_AND_LOCKED`, `LOCK_ACQUIRED`,
`ALREADY_HELD`) now include `fencing_token` and `lease_epoch`.

## Maintained Lock Confidence states

The client-side `LockMaintenanceService` tracks one of five confidence states
per selected element:

| State | Meaning | Outbox behaviour |
|---|---|---|
| `VERIFIED` | Last committed result was an acquisition or renewal | Protected mutations transmitted normally |
| `RENEWING` | One renewal request is in flight | Protected mutations transmitted normally |
| `UNVERIFIED` | Infrastructure failure (timeout, unavailable) | Protected mutations retained, not transmitted |
| `LOST` | Deterministic rejection (`STALE_TOKEN`, `NOT_OWNER`, `NOT_FOUND`) | Protected mutations retained, not transmitted; deselect creates no release |
| `STOPPED` | Deselection or service shutdown | No further renewals scheduled |

Deselecting a `VERIFIED`, `RENEWING`, or `UNVERIFIED` lock creates one durable
release intention.  Deselecting a `LOST` lock creates none.

## Operational Node Log events (Stage C additions)

Each replica's `events.log` gains these structured JSON events:

| Event | Fields | Meaning |
|---|---|---|
| `lock_expiration_coordinator_leader_init` | `term`, `inherited_lock_count` | New leader seeded grace-period timers |
| `lock_expiration_coordinator_stepdown` | `term` | Coordinator ephemeral state cleared on step-down |
| `expire_lock_proposed` | `deck_id`, `element_id`, `fencing_token`, `lease_epoch`, `request_id`, `term` | Internal expiration proposal submitted |
| `expire_lock_committed_expired` | `deck_id`, `element_id`, `fencing_token`, `lease_epoch`, `request_id` | `EXPIRED` committed; lock removed |
| `expire_lock_committed_noop` | `deck_id`, `element_id`, `fencing_token`, `lease_epoch`, `request_id` | `NO_OP_STALE_EXPIRY` committed; no change |

Operational logs are **never** Raft authority and must not be used to
reconstruct consensus state.

## Known limitation

Periodic renewals append Raft log entries and replicated deduplication records
indefinitely.  Stage C documents and measures this cost but does not add log
compaction, request-result pruning, snapshots, or `InstallSnapshot`.  This is
appropriate for the course-scale prototype and must not be described as a
production-scale retention policy.

## Running the Stage C.1 acceptance gate

```bash
# All five scenarios
python scenarios/run_stage_c1.py

# Single scenario
python scenarios/run_stage_c1.py --scenario stage_c_normal

# Custom metrics output path
python scenarios/run_stage_c1.py --metrics-out /tmp/my_metrics.json
```

Each scenario starts a clean three-replica cluster with the Stage C.1 timing
profile, runs to completion, archives results under `historiales/`, and tears
down the cluster.  Aggregate metrics are written to
`historiales/stage_c1_metrics.json`.

## Running the deterministic test suite

```bash
# All deterministic tests (excludes live-cluster integration tests)
python -m pytest tests/ -m "not integration" -q

# Full suite including integration markers (requires a running cluster)
python -m pytest tests/ -q
```

Stage C deterministic tests:

| File | Coverage |
|---|---|
| `tests/test_stage_c_renew_lock.py` | `renew_lock` state machine, DTO, spec model |
| `tests/test_stage_c_expire_lock.py` | `expire_lock` state machine, coordinator, public rejection |
| `tests/test_stage_c_lock_maintenance.py` | `LockMaintenanceService`, confidence transitions, Scoped Authority Gate |
| `tests/test_stage_c_temporal_safety.py` | Four timing races (renewal-first, expiration-first, delayed release, leader-change) |
| `tests/test_stage_c1_validation.py` | Reference-model unit tests; integration markers for live-cluster scenarios |
