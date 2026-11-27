"""Secret-free evidence helpers for Stage F clean three-replica scenarios.

Stage F trades follower-local presentation reads for leader-bound, bearer
checked ReadIndex reads. Per-replica convergence is therefore proven from
each replica's durable Raft snapshot and ``/status`` applied index, never
from a follower-local deck read. No helper here records a password, bearer
or replicated token digest.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Iterable

SCENARIOS = Path(__file__).resolve().parent
if str(SCENARIOS) not in sys.path:
    sys.path.insert(0, str(SCENARIOS))

SLOTS = ("n1", "n2", "n3")


def _write_records(path: Path, records: Iterable[dict]) -> None:
    Path(path).write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def _raft_state(run_dir: Path, slot: str) -> dict:
    return json.loads((Path(run_dir) / slot / "disk.json").read_text())["raft_state"]


def _deck_summary(deck: dict | None) -> dict | None:
    if deck is None:
        return None
    access = deck.get("access") or {}
    return {
        "schema_version": deck.get("schema_version", 2),
        "owner_user_id": (deck.get("metadata") or {}).get("owner_user_id"),
        "access_version": access.get("access_version"),
        "grants": sorted([user, level] for user, level in (access.get("grants") or {}).items()),
        "deck_version": deck.get("deck_version"),
        "deck_settings_version": deck.get("deck_settings_version", 0),
    }


def access_summary(run_dir: Path, slot: str, deck_ids: Iterable[str]) -> dict:
    """Project one replica's non-secret ownership/access state for comparison."""
    state = _raft_state(run_dir, slot)
    fsm = state["fsm"]
    policy = fsm.get("presentation_schema_policy") or {}
    return {
        "slot": slot,
        "last_applied": state["raft"]["last_applied"],
        "v3_enforced_from_index": policy.get("v3_enforced_from_index"),
        "decks": {deck_id: _deck_summary((fsm.get("decks") or {}).get(deck_id)) for deck_id in deck_ids},
    }


def write_access_summaries(run_dir: Path, output: Path, deck_ids: Iterable[str]) -> list[dict]:
    """Compare ownership, grant rosters, revisions and fence on all three replicas."""
    deck_ids = list(deck_ids)
    summaries = [access_summary(run_dir, slot, deck_ids) for slot in SLOTS]
    _write_records(output, summaries)
    assert all(item == {**summaries[0], "slot": item["slot"]} for item in summaries), summaries
    return summaries


# Pre-provisioned catalog fixtures (fixtures/auth/catalog-v1.json). These are
# test-only classroom credentials; scenarios scan every artifact for them.
ACCOUNTS = {
    "sofia": ("sofia@example.test", "Sofia-Classroom-1", "11111111-1111-4111-8111-111111111111"),
    "mateo": ("mateo@example.test", "Mateo-Classroom-2", "22222222-2222-4222-8222-222222222222"),
    "elena": ("elena@example.test", "Elena-Classroom-3", "33333333-3333-4333-8333-333333333333"),
}
PASSWORDS = [password for _, password, _ in ACCOUNTS.values()]


def owned_deck(deck_id: str, slide_ids: list[str], *, name: str, owner_user_id: str) -> dict:
    """Build a v3 publication whose immutable owner is the publishing account."""
    from stage_b_support import published_deck

    publication = published_deck(deck_id, slide_ids, name=name)
    publication["presentation"]["schema_version"] = 3
    publication["presentation"]["metadata"]["owner_user_id"] = owner_user_id
    return publication


def wait_for_activation(run_dir: Path, *, slots: Iterable[str] = SLOTS, timeout: float = 20.0) -> int:
    """Wait until each replica durably applied the committed v3 activation fence."""
    import time

    deadline = time.monotonic() + timeout
    fences: dict[str, int | None] = {}
    while time.monotonic() < deadline:
        for slot in slots:
            try:
                policy = _raft_state(run_dir, slot)["fsm"].get("presentation_schema_policy") or {}
            except (OSError, ValueError, KeyError):
                policy = {}
            fences[slot] = policy.get("v3_enforced_from_index")
        if all(fences.values()) and len(set(fences.values())) == 1:
            return next(iter(fences.values()))
        time.sleep(0.2)
    raise RuntimeError(f"v3 activation fence not applied on every replica: {fences}")


def committed_entries(run_dir: Path, slot: str) -> tuple[list[dict], dict]:
    state = _raft_state(run_dir, slot)
    raft = state["raft"]
    return ([entry for entry in raft["log"] if entry["index"] <= raft["commit_index"]],
            state["request_results"])


def write_access_order_reference(run_dir: Path, output: Path, slot: str) -> list[dict]:
    """Check durable role decisions against the independent committed-order reference."""
    from spec import check_committed_access_order

    entries, results = committed_entries(run_dir, slot)
    evidence = check_committed_access_order(entries, results)
    _write_records(output, evidence)
    return evidence


def write_role_trace(history: Path, output: Path, *, actors: dict[str, str]) -> list[dict]:
    """Client-observed role/permission trace: statuses only, never content or secrets."""
    trace = []
    for record in _records(history):
        result = record.get("result") if isinstance(record.get("result"), dict) else {}
        if record.get("op") not in LEADER_READS | ROLE_GUARDED_MUTATIONS | {
            "grant_access", "revoke_access", "query_operation_receipt", "publish_deck",
        }:
            continue
        trace.append({
            "request_id": record.get("id"), "operation": record.get("op"),
            "actor_user_id": actors.get(record.get("client")),
            "deck_id": (record.get("args") or {}).get("deck_id"),
            "ok": record.get("ok"), "status": result.get("status"), "error": record.get("error"),
            "effective_access": result.get("effective_access"),
            "access_version": result.get("access_version")
            if "access_version" in result else (result.get("value") or {}).get("access_version")
            if isinstance(result.get("value"), dict) else None,
            "applied_index": result.get("applied_index"),
            "invoke": record.get("invoke"), "complete": record.get("complete"),
        })
    _write_records(output, trace)
    return trace


def assert_no_leaks(run_dir: Path, secrets: list[str]) -> dict:
    """Literal password/bearer scan of the whole run tree plus digest scan of public files."""
    from secret_scanner import scan_artifacts

    leaks = scan_artifacts(Path(run_dir), PASSWORDS + list(secrets))
    assert not leaks, f"secret literal appeared in run artifacts: {leaks}"
    digests = digests_in_raft_disks(run_dir)
    public = scan_public_projections(run_dir, digests)
    assert not public, f"replicated token digest appeared in public projections: {public}"
    return {"literal_secret_findings": 0, "scanned_literals": len(PASSWORDS) + len(secrets),
            "replicated_digests_checked": len(digests), "public_digest_findings": 0}


def finalize_access_evidence(
    scenario: str, run_dir: Path, history: Path, *, actors: dict[str, str],
    deck_ids: list[str], request_ids: list[tuple[str, str]], reference_slot: str = "n1",
) -> dict[str, Path]:
    """Write the common Stage F archive set after replicas converged."""
    from stage_d_support import write_checker_evidence
    from stage_e_support import write_request_result_summaries

    paths = {name: run_dir / f"{scenario}_{name}.jsonl" for name in (
        "checker_history", "projection_audit", "checker", "metrics", "access_summaries",
        "request_results", "access_reference", "role_trace",
    )}
    write_access_checker_projection(history, paths["checker_history"], paths["projection_audit"],
                                    actors=actors)
    write_checker_evidence(paths["checker_history"], paths["checker"], run_dir=run_dir)
    write_access_latencies(history, paths["metrics"])
    write_access_summaries(run_dir, paths["access_summaries"], deck_ids)
    write_request_result_summaries(run_dir, paths["request_results"], request_ids)
    write_access_order_reference(run_dir, paths["access_reference"], reference_slot)
    write_role_trace(history, paths["role_trace"], actors=actors)
    return paths


def digests_in_raft_disks(run_dir: Path) -> set[str]:
    """Collect replicated bearer digests: internal verifier state, never public."""
    digests: set[str] = set()
    for disk in Path(run_dir).glob("*/disk.json"):
        auth = json.loads(disk.read_text())["raft_state"]["fsm"].get("auth") or {}
        for bucket in ("active_credentials", "retired_credentials", "legacy_quarantined_credentials"):
            for record in (auth.get(bucket) or {}).values():
                if isinstance(record, dict) and isinstance(record.get("token_digest"), str):
                    digests.add(record["token_digest"])
    return digests


def scan_public_projections(run_dir: Path, digests: set[str]) -> list[Path]:
    """Return public evidence files (histories, projections, operational logs) naming a digest."""
    needles = [digest.encode() for digest in digests if digest]
    public = list(Path(run_dir).glob("*.jsonl")) + list(Path(run_dir).glob("*/events.log"))
    return sorted(path for path in public if any(needle in path.read_bytes() for needle in needles))


def _percentiles(values: list[float]) -> dict:
    from statistics import median

    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ms": round(median(ordered), 3) if ordered else None,
        "p95_ms": round(ordered[min(len(ordered) - 1, int(.95 * len(ordered)))], 3) if ordered else None,
    }


def write_access_latencies(history: Path, output: Path) -> dict:
    """Measure Stage F client calls in disjoint buckets (p50/p95, milliseconds).

    Successful content reads, privacy-safe read denials, self-permission
    queries, receipts, effective grant changes, rejected grant commands,
    authorized edits and committed role denials never share a bucket.
    Requests that received no successful response are counted separately.
    """
    buckets: dict[str, list[float]] = {name: [] for name in (
        "login", "protected_read", "protected_read_denied", "permission_query",
        "operation_receipt", "grant_change", "grant_rejected", "authorized_edit",
        "access_denied_mutation",
    )}
    unavailable = 0
    for record in _records(history):
        operation = record.get("op")
        result = record.get("result") if isinstance(record.get("result"), dict) else {}
        status = result.get("status")
        if record.get("ok") is not True or not isinstance(record.get("complete"), (int, float)):
            unavailable += 1
            continue
        duration = (record["complete"] - record["invoke"]) * 1000
        if operation == "login" and status == "AUTHENTICATED":
            bucket = "login"
        elif operation == "query_grants":
            bucket = "permission_query"
        elif operation == "query_operation_receipt":
            bucket = "operation_receipt"
        elif operation in LEADER_READS:
            bucket = "protected_read_denied" if status == "ACCESS_DENIED" else "protected_read"
        elif operation in {"grant_access", "revoke_access"}:
            bucket = ("grant_change" if status in {"ACCESS_GRANTED", "ACCESS_UPDATED", "ACCESS_REVOKED"}
                      else "grant_rejected")
        elif operation in ROLE_GUARDED_MUTATIONS:
            bucket = "access_denied_mutation" if status == "ACCESS_DENIED" else "authorized_edit"
        else:
            continue
        buckets[bucket].append(duration)
    summary = {"event": "stage_f_latency_summary",
               **{name: _percentiles(values) for name, values in buckets.items()},
               "unavailable": {"count": unavailable},
               "measurement": "client invoke to complete for successful HTTP responses"}
    _write_records(output, [summary])
    return summary


ROLE_GUARDED_MUTATIONS = {
    "create_slide", "delete_slide", "reorder_slides", "create_element", "acquire_lock",
    "release_lock", "update_element", "delete_element", "renew_lock",
    "update_deck_settings", "update_slide_settings",
}
# Publication rejections enforced by the v3 fence and owner binding (ADR-012/019);
# the per-deck reference model has no notion of the fence or the publisher.
PUBLICATION_BOUNDARY = {"OWNER_MISMATCH", "UNSUPPORTED_PRESENTATION_SCHEMA"}
LEADER_READS = {
    "get_deck_preview", "get_slide", "get_element", "read_lock_status",
    "authoritative_refresh_element", "refresh_element", "query_grants",
}


def write_access_checker_projection(
    history: Path, checker: Path, audit: Path, *, actors: dict[str, str],
) -> dict:
    """Project Stage F history onto ``check.py`` with the bearer-derived actor.

    The server never trusts a wire actor; the scenario knows which account each
    client installation signed in as and stamps ``actor_user_id`` for the
    reference model only. Two classes of records are excluded and audited:
    role-guarded mutations denied at apply (``spec.py`` models grant changes and
    read authorization, while the committed access-order reference checks the
    mutation role matrix) and operation receipts (their proof is the retained
    Raft log and dedup table, which the per-deck reference state lacks).
    """
    from stage_e_support import write_auth_checker_projection

    stamped: list[dict] = []
    exclusions: list[dict] = []
    normalizations: list[dict] = []
    for record in _records(history):
        result = record.get("result") if isinstance(record.get("result"), dict) else None
        operation = record.get("op")
        reason = None
        if operation == "query_operation_receipt":
            reason = "receipt_requires_retained_raft_log"
        elif operation in ROLE_GUARDED_MUTATIONS and result and result.get("status") == "ACCESS_DENIED":
            reason = "role_denial_checked_by_committed_access_reference"
        elif operation == "publish_deck" and result and result.get("status") in PUBLICATION_BOUNDARY:
            reason = "publication_boundary_checked_by_scenario"
        if reason is not None:
            exclusions.append({
                "event": "checker_projection_exclusion", "reason": reason,
                "request_id": record.get("id"), "operation": operation,
                "result_status": result.get("status") if result else None,
            })
            continue
        projected = json.loads(json.dumps(record))
        if record.get("client") in actors and isinstance(projected.get("args"), dict):
            projected["args"]["actor_user_id"] = actors[record["client"]]
        if operation in LEADER_READS and result is not None:
            removed = [field for field in ("applied_index", "schema_version") if field in result]
            for field in removed:
                projected["result"].pop(field)
            if removed:
                normalizations.append({
                    "event": "checker_projection_normalization", "reason": "raft_assigned_read_index",
                    "request_id": record.get("id"), "operation": operation, "removed_fields": removed,
                })
        stamped.append(projected)
    staged = Path(checker).with_name(Path(checker).stem + "_stamped.jsonl")
    _write_records(staged, stamped)
    try:
        write_auth_checker_projection(staged, checker, audit)
    finally:
        staged.unlink(missing_ok=True)
    with Path(audit).open("a") as stream:
        for item in normalizations + exclusions:
            stream.write(json.dumps(item, sort_keys=True) + "\n")
    return {"checker_records": len(_records(checker)), "excluded": len(exclusions),
            "normalized": len(normalizations)}
