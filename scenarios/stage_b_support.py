"""Shared fixtures and evidence helpers for executable Stage B.1 scenarios."""

from __future__ import annotations

import json
import random
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Iterable
from uuid import NAMESPACE_URL, uuid5

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cluster

_history_lock = threading.Lock()


def _post(url: str, data: dict[str, Any], wait: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url + "/client",
        json.dumps(data).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=wait) as response:
        return json.loads(response.read())


def append_evidence(path: str | Path, event: str, **fields: Any) -> None:
    """Append one archive-compatible machine-readable evidence record."""

    record = {"timestamp": time.time(), "event": event, **fields}
    with _history_lock, Path(path).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def recorded_request(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    operation: str,
    arguments: dict[str, Any],
    *,
    node: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Use the unchanged ``POST /client`` seam with an exact request ID.

    This is a scenario-only equivalent of ``Client.call`` for retry evidence;
    it follows redirects and records the standard checker history fields.
    """

    if not request_id:
        raise ValueError("request_id must not be empty")
    nodes = cluster.load()["host_nodes"]
    target = node or random.choice(list(nodes))
    start, deadline = time.time(), time.time() + timeout
    response: dict[str, Any] = {"ok": False, "error": "sin respuesta"}
    served = None
    while time.time() < deadline:
        try:
            response = _post(
                nodes[target],
                {"id": request_id, "op": operation, "args": arguments, "client": client_name},
                max(0.5, min(6.0, deadline - time.time())),
            )
        except Exception as error:
            response = {"ok": False, "error": type(error).__name__}
        served = target
        if response.get("redirect") in nodes:
            target = response["redirect"]
            continue
        if response.get("ok"):
            break
        if node is None:
            target = random.choice([candidate for candidate in nodes if candidate != target] or [target])
        time.sleep(0.2)
    record = {
        "id": request_id,
        "client": client_name,
        "op": operation,
        "args": arguments,
        "invoke": start,
        "complete": time.time() if response.get("ok") else None,
        "ok": bool(response.get("ok")),
        "result": response.get("result"),
        "error": response.get("error"),
        "node": served,
    }
    Path(history_path).parent.mkdir(parents=True, exist_ok=True)
    with _history_lock, Path(history_path).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    return response


def _percentile(values: list[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    return round(ordered[min(len(ordered) - 1, int(fraction * len(ordered)))], 3)


def _latencies(records: list[dict[str, Any]], operations: set[str]) -> list[float]:
    return [
        (record["complete"] - record["invoke"]) * 1000
        for record in records
        if record.get("op") in operations
        and record.get("ok") is True
        and isinstance(record.get("complete"), (int, float))
    ]


def write_latency_summary(
    history_path: str | Path,
    output_path: str | Path,
    *,
    edit_operations: set[str],
    read_operations: set[str],
    unavailability_seconds: float | None = None,
) -> dict[str, Any]:
    """Write machine-readable p50/p95 evidence from one client history."""

    records = [
        json.loads(line)
        for line in Path(history_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    edit_latencies = _latencies(records, edit_operations)
    read_latencies = _latencies(records, read_operations)

    def summarize(values: list[float]) -> dict[str, Any]:
        return {
            "count": len(values),
            "p50_ms": _percentile(values, 0.5),
            "p95_ms": _percentile(values, 0.95),
        }

    summary = {
        "quorum_edit": summarize(edit_latencies),
        "local_eventual_read": summarize(read_latencies),
        "observed_unavailability_seconds": (
            round(unavailability_seconds, 3) if unavailability_seconds is not None else None
        ),
    }
    Path(output_path).write_text(
        json.dumps({"event": "latency_summary", **summary}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def write_checker_evidence(
    history_path: str | Path,
    output_path: str | Path,
    *,
    run_dir: str | Path = "run",
) -> dict[str, Any]:
    """Run the unchanged checker and preserve its complete output as JSONL."""

    command = [
        sys.executable,
        str(ROOT / "check.py"),
        str(history_path),
        "--run",
        str(run_dir),
    ]
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    record = {
        "event": "checker_result",
        "history": str(history_path),
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    Path(output_path).write_text(
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise RuntimeError(f"checker rejected {history_path}:\n{completed.stdout}{completed.stderr}")
    return record


def write_checker_projection(
    source_history: str | Path,
    checker_history: str | Path,
    audit_path: str | Path,
) -> dict[str, int]:
    """Project a complete server audit onto the unchanged checker's inputs.

    ``check.py`` receives operation arguments but not request identity or the
    Raft log index assigned to a committed command.  Consequently, it cannot
    reproduce an exact-ID deduplication response or Raft-assigned index
    metadata. Exact duplicate requests are excluded. Conflict and
    authoritative-read operations remain checker-visible after removing only
    the unmodelled index field. The complete originals and their projected
    forms are preserved in a companion audit.
    """

    source_records = [
        json.loads(line)
        for line in Path(source_history).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    seen_request_ids: set[str] = set()
    retained: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    normalized_records = 0
    for record in source_records:
        request_id = record.get("id")
        result = record.get("result")
        if isinstance(request_id, str) and request_id in seen_request_ids:
            audit.append({
                "event": "checker_projection_exclusion",
                "reason": "duplicate_request_identity",
                "request_id": request_id,
                "operation": record.get("op"),
                "record": record,
            })
            continue
        projected = json.loads(json.dumps(record))
        reason = None
        if (
            isinstance(result, dict)
            and result.get("status") == "VERSION_CONFLICT"
            and isinstance(result.get("value"), dict)
            and "min_commit_index" in result["value"]
        ):
            reason = "raft_assigned_conflict_index"
            projected["result"]["value"].pop("min_commit_index")
        elif record.get("op") == "authoritative_refresh_element" and (
            isinstance(result, dict) and "applied_index" in result
        ):
            reason = "raft_assigned_authoritative_read_index"
            projected["result"].pop("applied_index")
        if isinstance(request_id, str):
            seen_request_ids.add(request_id)
        retained.append(projected)
        if reason is not None:
            normalized_records += 1
            audit.append({
                "event": "checker_projection_normalization",
                "reason": reason,
                "request_id": request_id,
                "operation": record.get("op"),
                "record": record,
                "projected_record": projected,
            })

    Path(checker_history).write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in retained),
        encoding="utf-8",
    )
    Path(audit_path).write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in audit),
        encoding="utf-8",
    )
    return {
        "source_records": len(source_records),
        "checker_records": len(retained),
        "excluded_records": len(source_records) - len(retained),
        "normalized_records": normalized_records,
    }


def write_manifest(path: str | Path, scenario: str, **evidence: Any) -> None:
    """Record the clean-cluster scenario boundary and its evidence files."""

    append_evidence(
        path,
        "scenario_manifest",
        scenario=scenario,
        replica_count=3,
        runtime_boundary="unchanged node.py POST /client and Raft adapter",
        **evidence,
    )


def finalize_checked_scenario(
    scenario: str,
    history_path: str | Path,
    *,
    edit_operations: set[str],
    read_operations: set[str],
    unavailability_seconds: float | None = None,
    extra_evidence: Iterable[str] = (),
) -> None:
    """Persist checker, cost, and manifest records under ``run/*.jsonl``."""

    history = Path(history_path)
    prefix = ROOT / "run" / scenario
    checker_path = prefix.with_name(prefix.name + "_checker.jsonl")
    metrics_path = prefix.with_name(prefix.name + "_metrics.jsonl")
    manifest_path = prefix.with_name(prefix.name + "_manifest.jsonl")
    write_checker_evidence(history, checker_path, run_dir=ROOT / "run")
    write_latency_summary(
        history,
        metrics_path,
        edit_operations=edit_operations,
        read_operations=read_operations,
        unavailability_seconds=unavailability_seconds,
    )
    write_manifest(
        manifest_path,
        scenario,
        history=history.name,
        checker=checker_path.name,
        metrics=metrics_path.name,
        extra_evidence=list(extra_evidence),
    )


def last_history_entry(path: str | Path) -> dict[str, Any]:
    """Return the final non-empty standard client-history record."""

    lines = [line for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"history {path} is empty")
    return json.loads(lines[-1])


def require_status(response: dict[str, Any], expected: str) -> dict[str, Any]:
    """Return a successful application result or fail the executable scenario."""

    result = response.get("result") if isinstance(response, dict) else None
    if response.get("ok") is not True or not isinstance(result, dict) or result.get("status") != expected:
        raise RuntimeError(f"expected {expected}, received {response}")
    return result


def wait_for_event(
    slot: str,
    event: str,
    *,
    request_id: str | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    """Wait for one operational-log observation without reading Raft authority."""

    path = ROOT / "run" / slot / "events.log"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            for line in reversed(path.read_text(encoding="utf-8").splitlines()):
                record = json.loads(line)
                if record.get("event") == event and (
                    request_id is None or record.get("request_id") == request_id
                ):
                    return record
        time.sleep(0.05)
    raise RuntimeError(f"did not observe {event} for {request_id or slot}")


def wait_for_presentation_catch_up(
    client: Any,
    deck_id: str,
    slide_id: str,
    *,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Wait for equal applied indexes, then record one final slide read set."""

    config = cluster.load()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = cluster.status(config)
        applied = [status.get("last_applied") for status in statuses.values() if status]
        if len(applied) == len(config["names"]) and len(set(applied)) == 1:
            finals = client.final("get_slide", deck_id=deck_id, slide_id=slide_id)
            if not all(response.get("ok") is True for response in finals.values()):
                raise RuntimeError(f"final presentation reads were unavailable: {finals}")
            values = {json.dumps(response["result"], sort_keys=True) for response in finals.values()}
            if len(values) != 1:
                raise RuntimeError(f"presentation replicas did not converge: {finals}")
            return next(iter(finals.values()))["result"]
        time.sleep(0.2)
    raise RuntimeError("presentation replicas did not catch up before the scenario deadline")


def stable_uuid(name: str) -> str:
    """Return a repeatable UUID valid for clean-cluster scenario fixtures."""

    return str(uuid5(NAMESPACE_URL, f"distributed-slides-stage-b1:{name}"))


def published_deck(deck_id: str, slide_ids: Iterable[str], *, name: str) -> dict[str, Any]:
    """Build a canonical empty presentation snapshot for a live scenario."""

    return {
        "presentation": {
            "schema_version": 1,
            "metadata": {
                "deck_id": deck_id,
                "name": name,
                "owner_session_id": stable_uuid(f"{name}:owner"),
                "width_px": 1280,
                "height_px": 720,
            },
            "slides": [
                {
                    "metadata": {
                        "slide_id": slide_id,
                        "background": "#112233",
                        "ordinal": ordinal,
                    },
                    "elements": [],
                }
                for ordinal, slide_id in enumerate(slide_ids)
            ],
        }
    }


def text_element(element_id: str, content: str, *, x: int = 0, z_index: int = 0) -> dict[str, Any]:
    """Build one fully explicit deterministic text element."""

    return {
        "element_id": element_id,
        "kind": "text_box",
        "version": 1,
        "configuration": {
            "content": content,
            "font_families": ["Arial"],
            "generic_fallback": "sans-serif",
            "font_size": 2400,
            "color": "#000000FF",
            "x": x,
            "y": 0,
            "z_index": z_index,
            "line_spacing": 0,
            "indentation": 0,
            "bold": False,
            "italic": False,
            "underline": False,
            "strikethrough": False,
            "align": "left",
        },
    }
