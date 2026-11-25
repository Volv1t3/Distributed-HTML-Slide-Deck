"""Secret-free evidence helpers for Stage E.A live scenarios."""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
from statistics import median
import sys
import time
import urllib.request
from typing import Callable

SCENARIOS = Path(__file__).resolve().parent
if str(SCENARIOS) not in sys.path:
    sys.path.insert(0, str(SCENARIOS))

from stage_b_support import write_checker_projection
from stage_d_support import write_checker_evidence
from authenticated_client import AuthenticatedClient, _redact
from dto_base import sha256_json
from spec import check_committed_auth_order


def lose_committed_login_response(
    client: AuthenticatedClient, request_id: str, email: str, password: str,
    on_committed: Callable[[str, int, str], None],
) -> tuple[dict, str]:
    """Discard a committed HTTP login result before the client installs it.

    The callback verifies durable issuance and can stop the leader. Only a
    transient diagnostic copy of the bearer is returned for literal scanning.
    """
    original_post = client._raw_post
    lost: dict[str, str] = {}

    def intercept(url: str, payload: dict, timeout: float | None = None) -> dict:
        response = original_post(url, payload, timeout)
        result = response.get("result") if isinstance(response, dict) else None
        if payload.get("op") == "login" and isinstance(result, dict) and result.get("status") == "AUTHENTICATED":
            token = result["access_token"]
            lost["token"] = token
            on_committed(result["credential_id"], result["credential_generation"],
                         hashlib.sha256(token.encode()).hexdigest())
            raise ConnectionError("simulated login response loss after commit")
        return response

    client._raw_post = intercept
    try:
        outcome = client.login(request_id, email, password)
    finally:
        client._raw_post = original_post
    assert lost and outcome == {"ok": False, "error": "ConnectionError"}, outcome
    return outcome, lost["token"]


EDIT_OPERATIONS = {
    "publish_deck", "create_element", "update_element", "update_deck_settings",
    "update_slide_settings",
}
EDIT_SUCCESS_STATUSES = {
    "PUBLISHED", "CREATED_AND_LOCKED", "SAVED", "DECK_SETTINGS_UPDATED",
    "SLIDE_SETTINGS_UPDATED",
}


def recorded_bearer_probe(
    node_url: str, history: Path, client_id: str, access_token: str,
    request_id: str, operation: str, arguments: dict,
) -> dict:
    """Send a retained older bearer and record only non-secret outcome fields."""
    payload = {
        "id": request_id, "client": client_id, "op": operation,
        "args": {**arguments, "_auth": {"access_token": access_token}},
    }
    request = urllib.request.Request(
        node_url + "/client", json.dumps(payload).encode(),
        {"Content-Type": "application/json"},
    )
    started = time.time()
    with urllib.request.urlopen(request, timeout=10.0) as response:
        outcome = json.loads(response.read())
    safe_result = outcome.get("result")
    if isinstance(safe_result, dict):
        safe_result = {"status": safe_result.get("status")}
    record = {
        "id": request_id, "client": client_id, "op": operation,
        "args": arguments, "invoke": started,
        "complete": time.time() if outcome.get("ok") else None,
        "ok": bool(outcome.get("ok")), "result": safe_result,
        "error": outcome.get("error"),
    }
    with history.open("a") as stream:
        stream.write(json.dumps(_redact(record, {access_token}), sort_keys=True) + "\n")
    return outcome


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _write_records(path: Path, records: list[dict]) -> None:
    path.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))


def write_auth_checker_projection(history: Path, checker: Path, audit: Path) -> None:
    """Keep admitted business IDs; audit auth transitions outside spec.py."""
    business = history.with_name(history.stem + "_business.jsonl")
    records = _records(history)
    excluded = [
        record for record in records
        if record.get("op") in {"login", "revoke_access_token"}
        or record.get("error") in {"REAUTH_REQUIRED", "LOGIN_SECRET_UNAVAILABLE"}
        or (isinstance(record.get("result"), dict)
            and record["result"].get("status") == "UNAUTHENTICATED")
    ]
    _write_records(business, [record for record in records if record not in excluded])
    try:
        write_checker_projection(business, checker, audit)
    finally:
        business.unlink(missing_ok=True)
    exclusions = [{
        "event": "checker_projection_exclusion", "reason": "authentication_admission_or_lifecycle",
        "request_id": record["id"], "operation": record["op"],
        "result_status": (record.get("result") or {}).get("status")
        if isinstance(record.get("result"), dict) else None,
        "error": record.get("error"),
    } for record in excluded]
    with audit.open("a") as stream:
        for record in exclusions:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def _summary(values: list[float]) -> dict:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ms": round(median(ordered), 3) if ordered else None,
        "p95_ms": round(ordered[min(len(ordered) - 1, int(.95 * len(ordered)))], 3) if ordered else None,
    }


def write_auth_latencies(history: Path, output: Path, *, events_dir: Path) -> dict:
    """Measure login end-to-end, committed issuance phase, and business edits."""
    records = _records(history)
    def durations(operations: set[str], statuses: set[str]) -> list[float]:
        return [
            (record["complete"] - record["invoke"]) * 1000
            for record in records
            if record.get("op") in operations and record.get("ok") is True
            and isinstance(record.get("complete"), (int, float))
            and isinstance(record.get("result"), dict)
            and record["result"].get("status") in statuses
        ]

    starts: dict[str, float] = {}
    issuance: list[float] = []
    for path in events_dir.glob("*/events.log"):
        for event in _records(path):
            request_id = event.get("request_id")
            if event.get("event") == "login_issuance_proposed":
                starts[request_id] = event["timestamp"]
            elif event.get("event") == "login_completed" and request_id in starts:
                if event.get("outcome") in (None, "AUTHENTICATED"):
                    issuance.append((event["timestamp"] - starts.pop(request_id)) * 1000)
    summary = {
        "event": "stage_e_latency_summary",
        "login": _summary(durations({"login"}, {"AUTHENTICATED"})),
        "token_issue": _summary(issuance),
        "token_revoke": _summary([
            (record["complete"] - record["invoke"]) * 1000
            for record in records
            if record.get("op") == "revoke_access_token" and record.get("ok") is True
            and isinstance(record.get("complete"), (int, float))
            and isinstance(record.get("result"), dict)
            and record["result"].get("status") == "TOKEN_REVOKED"
        ]),
        "authenticated_edit": _summary(durations(EDIT_OPERATIONS, EDIT_SUCCESS_STATUSES)),
        "committed_auth_denial": _summary(durations(EDIT_OPERATIONS, {"UNAUTHENTICATED"})),
        "token_issue_measurement": "leader event: issuance proposed to login completed",
    }
    baseline_path = SCENARIOS.parent / "docs" / "stage-ea-stage-d-baseline.jsonl"
    baseline_bytes = baseline_path.read_bytes()
    baseline = json.loads(baseline_bytes)
    assert baseline["event"] == "latency_summary" and baseline["quorum_edit"]["count"] > 0
    summary["stage_d_comparison"] = {
        "source": "historiales/20261124-222700/stage_d_normal_metrics.jsonl",
        "preserved_copy": "docs/stage-ea-stage-d-baseline.jsonl",
        "sha256": hashlib.sha256(baseline_bytes).hexdigest(),
        "login": {"stage_d": None, "stage_e": summary["login"]},
        "token_issue": {"stage_d": None, "stage_e": summary["token_issue"]},
        "token_revoke": {"stage_d": None, "stage_e": summary["token_revoke"]},
        "authenticated_edit": {"stage_d": baseline["quorum_edit"], "stage_e": summary["authenticated_edit"]},
        "note": "Stage D has no authentication operations; edit populations differ and are descriptive only.",
    }
    _write_records(output, [summary])
    return summary


def write_auth_summaries(run_dir: Path, output: Path, client_id: str) -> list[dict]:
    """Read non-secret replicated credential records from durable snapshots."""
    summaries = []
    for slot in ("n1", "n2", "n3"):
        disk = json.loads((run_dir / slot / "disk.json").read_text())
        state = disk["raft_state"]
        auth = state["fsm"]["auth"]
        credential = auth["active_credentials"].get(client_id)
        summaries.append({
            "slot": slot,
            "schema_version": auth["schema_version"],
            "catalog_fingerprint": auth["catalog_fingerprint"],
            "last_applied": state["raft"]["last_applied"],
            "bound_user_id": auth["client_bindings"].get(client_id),
            "credential_user_id": credential["user_id"] if credential else None,
            "credential_id": credential["credential_id"] if credential else None,
            "credential_generation": credential["credential_generation"] if credential else None,
            "token_digest": credential["token_digest"] if credential else None,
            "revoked": credential["revoked"] if credential else None,
        })
    _write_records(output, summaries)
    assert all(item == {**summaries[0], "slot": item["slot"]} for item in summaries), summaries
    return summaries


def durable_issuance_evidence(
    run_dir: Path, slots: tuple[str, str], client_id: str,
    request_id: str, credential_id: str,
) -> list[dict]:
    """Prove an issuance entry is durable, committed, and applied on both voters."""
    proof = []
    for slot in slots:
        disk = json.loads((run_dir / slot / "disk.json").read_text())
        state = disk["raft_state"]
        raft = state["raft"]
        entries = [
            entry for entry in raft["log"]
            if entry["command"]["operation"] == "issue_access_token"
            and entry["command"]["request_id"] == request_id
            and entry["command"]["client_id"] == client_id
        ]
        assert len(entries) == 1, f"{slot}: issuance entry missing or duplicated"
        entry = entries[0]
        index = entry["index"]
        assert raft["commit_index"] >= index and raft["last_applied"] >= index, (
            f"{slot}: issuance entry not committed and applied"
        )
        credential = state["fsm"]["auth"]["active_credentials"].get(client_id)
        assert credential is not None and credential["credential_id"] == credential_id, (
            f"{slot}: issued credential absent from durable FSM"
        )
        assert entry["command"]["payload"]["credential_id"] == credential_id
        assert entry["command"]["payload"]["token_digest"] == credential["token_digest"]
        proof.append({
            "slot": slot, "committed_index": index,
            "commit_index": raft["commit_index"], "last_applied": raft["last_applied"],
            "credential_id": credential_id,
            "credential_generation": credential["credential_generation"],
            "token_digest": credential["token_digest"],
            "revoked": credential["revoked"],
        })
    assert proof[0]["committed_index"] == proof[1]["committed_index"], proof
    assert proof[0]["token_digest"] == proof[1]["token_digest"], proof
    return proof


def write_request_result_summaries(
    run_dir: Path, output: Path, requests: list[tuple[str, str]],
) -> list[dict]:
    """Compare durable dedup outcomes for selected IDs on all three replicas."""
    records: list[dict] = []
    for client_id, request_id in requests:
        key = sha256_json([client_id, request_id])
        per_slot = []
        for slot in ("n1", "n2", "n3"):
            disk = json.loads((run_dir / slot / "disk.json").read_text())
            stored = disk["raft_state"]["request_results"].get(key)
            assert stored is not None, f"{slot}: no durable result for {request_id}"
            record = {
                "slot": slot, "client_id": client_id, "request_id": request_id,
                "request_key": key, "committed_index": stored["committed_index"],
                "fingerprint": stored.get("fingerprint"),
                "result": stored["result"],
            }
            per_slot.append(record)
        assert all(item == {**per_slot[0], "slot": item["slot"]} for item in per_slot), per_slot
        records.extend(per_slot)
    _write_records(output, [_redact(record) for record in records])
    return records


def write_auth_order_reference(run_dir: Path, output: Path, slot: str) -> list[dict]:
    """Check committed auth order against the independent sequential model."""
    disk = json.loads((run_dir / slot / "disk.json").read_text())["raft_state"]
    raft = disk["raft"]
    entries = [entry for entry in raft["log"] if entry["index"] <= raft["commit_index"]]
    result = check_committed_auth_order(entries, disk["request_results"])
    _write_records(output, result)
    return result


def finalize_auth_evidence(
    history: Path, checker_history: Path, checker_audit: Path,
    checker_result: Path, metrics: Path, *, run_dir: Path,
) -> dict:
    """Produce the same checker and latency evidence for every Stage E.A scenario."""
    write_auth_checker_projection(history, checker_history, checker_audit)
    write_checker_evidence(checker_history, checker_result, run_dir=run_dir)
    return write_auth_latencies(history, metrics, events_dir=run_dir)
