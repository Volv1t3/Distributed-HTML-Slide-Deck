"""Secret-free evidence helpers for Stage E.A live scenarios."""

from __future__ import annotations

import json
from pathlib import Path
import sys

SCENARIOS = Path(__file__).resolve().parent
if str(SCENARIOS) not in sys.path:
    sys.path.insert(0, str(SCENARIOS))

from stage_b_support import write_checker_projection


EDIT_OPERATIONS = {
    "publish_deck", "create_element", "update_element", "update_deck_settings",
    "update_slide_settings",
}


def _records(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _write_records(path: Path, records: list[dict]) -> None:
    path.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))


def write_auth_checker_projection(history: Path, checker: Path, audit: Path) -> None:
    """Keep business IDs intact while excluding login, which spec.py cannot model."""
    business = history.with_name(history.stem + "_business.jsonl")
    records = _records(history)
    _write_records(business, [record for record in records if record.get("op") != "login"])
    try:
        write_checker_projection(business, checker, audit)
    finally:
        business.unlink(missing_ok=True)
    exclusions = [
        {"event": "checker_projection_exclusion", "reason": "nondeterministic_token_issuance",
         "request_id": record["id"], "operation": "login"}
        for record in records if record.get("op") == "login"
    ]
    with audit.open("a") as stream:
        for record in exclusions:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def _summary(values: list[float]) -> dict:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50_ms": round(ordered[min(len(ordered) - 1, int(.5 * len(ordered)))], 3) if ordered else None,
        "p95_ms": round(ordered[min(len(ordered) - 1, int(.95 * len(ordered)))], 3) if ordered else None,
    }


def write_auth_latencies(history: Path, output: Path, *, events_dir: Path) -> dict:
    """Measure login end-to-end, committed issuance phase, and business edits."""
    records = _records(history)
    def durations(operations: set[str]) -> list[float]:
        return [
            (record["complete"] - record["invoke"]) * 1000
            for record in records
            if record.get("op") in operations and record.get("ok") is True
            and isinstance(record.get("complete"), (int, float))
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
        "login": _summary(durations({"login"})),
        "token_issue": _summary(issuance),
        "authenticated_edit": _summary(durations(EDIT_OPERATIONS)),
        "token_issue_measurement": "leader event: issuance proposed to login completed",
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
            "credential_id": credential["credential_id"] if credential else None,
            "credential_generation": credential["credential_generation"] if credential else None,
            "token_digest": credential["token_digest"] if credential else None,
            "revoked": credential["revoked"] if credential else None,
        })
    _write_records(output, summaries)
    assert all(item == {**summaries[0], "slot": item["slot"]} for item in summaries), summaries
    return summaries
