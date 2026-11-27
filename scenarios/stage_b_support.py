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
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cluster
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from auth_admission import redact_auth_data  # noqa: E402
from auth_dto import is_auth_secret_key  # noqa: E402
from client import Client  # noqa: E402
from command_dto import PROTECTED_READ_OPERATIONS  # noqa: E402

_history_lock = threading.Lock()

# Stage F: client identity -> signed-in account, and client identity -> bearer.
# Both live only in the scenario process; bearers are never written anywhere.
_SCENARIO_ACTORS: dict[str, str] = {}
_SCENARIO_BEARERS: dict[str, dict[str, str]] = {}

# Catalog user used by Stage C/D integration scenarios that now run under auth.
SCENARIO_EMAIL: str = "sofia@example.test"
SCENARIO_PASSWORD: str = "Sofia-Classroom-1"
# The scenario account's catalog user ID. Under Stage F's v3 publication
# boundary a scenario deck must name its publishing account as owner.
SCENARIO_OWNER: str = "11111111-1111-4111-8111-111111111111"


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
    _auth: dict[str, Any] | None = None,
    node: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Use the unchanged ``POST /client`` seam with an exact request ID.

    _auth, when supplied, is injected into the wire args immediately before
    the HTTP POST and is stripped from every JSONL history record (ADR-010 §10).
    """

    if not request_id:
        raise ValueError("request_id must not be empty")
    nodes = cluster.load()["host_nodes"]
    target = node or random.choice(list(nodes))
    start, deadline = time.time(), time.time() + timeout
    if _auth is None:
        # Stage F: a signed-in scenario identity always sends its current bearer.
        _auth = _SCENARIO_BEARERS.get(client_name)
    # Wire args: inject _auth but never persist it.
    wire_args = {**arguments, "_auth": _auth} if _auth is not None else arguments
    # History args: strip any auth-secret keys so tokens never enter journals.
    history_args = {k: v for k, v in arguments.items() if not is_auth_secret_key(k)}
    response: dict[str, Any] = {"ok": False, "error": "sin respuesta"}
    served = None
    while time.time() < deadline:
        try:
            response = _post(
                nodes[target],
                {"id": request_id, "op": operation, "args": wire_args, "client": client_name},
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
        "args": history_args,
        "invoke": start,
        "complete": time.time() if response.get("ok") else None,
        "ok": bool(response.get("ok")),
        "result": redact_auth_data(response.get("result")),
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
    *,
    internal_records: Iterable[dict[str, Any]] = (),
) -> dict[str, int]:
    """Project a complete server audit onto the unchanged checker's inputs.

    ``check.py`` receives operation arguments but not request identity or the
    Raft log index assigned to a committed command.  Consequently, it cannot
    reproduce an exact-ID deduplication response or Raft-assigned index
    metadata. Exact duplicate requests are excluded. Conflict and
    authoritative-read operations remain checker-visible after removing only
    the unmodelled index field. The complete originals and their projected
    forms are preserved in a companion audit.

    ``internal_records`` carries server-originated transitions, such as a
    leader-proposed ``expire_lock``, that never appear in raw client history.
    They are merged in real-time order and labelled separately in the audit.
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
        elif (
            isinstance(result, dict)
            and result.get("status") in {
                "DECK_SETTINGS_VERSION_CONFLICT", "SLIDE_SETTINGS_VERSION_CONFLICT",
            }
            and isinstance(result.get("value"), dict)
            and "min_commit_index" in result["value"]
        ):
            reason = "raft_assigned_settings_conflict_index"
            projected["result"]["value"].pop("min_commit_index")
        elif (
            isinstance(result, dict)
            and result.get("status") in {"DECK_SETTINGS_UPDATED", "SLIDE_SETTINGS_UPDATED"}
            and isinstance(result.get("value"), dict)
            and "committed_index" in result["value"]
        ):
            reason = "raft_assigned_settings_commit_index"
            projected["result"]["value"].pop("committed_index")
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

    internal = list(internal_records)
    combined = retained + internal
    if internal:
        combined.sort(
            key=lambda record: (
                record.get("invoke")
                if isinstance(record.get("invoke"), (int, float))
                else 0.0
            )
        )
        for record in internal:
            audit.append({
                "event": "checker_projection_internal_transition",
                "reason": "server_originated_internal_command",
                "origin": record.get("origin"),
                "request_id": record.get("id"),
                "operation": record.get("op"),
                "projected_record": record,
            })

    Path(checker_history).write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in combined),
        encoding="utf-8",
    )
    Path(audit_path).write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in audit),
        encoding="utf-8",
    )
    summary: dict[str, int] = {
        "source_records": len(source_records),
        "checker_records": len(combined),
        "excluded_records": len(source_records) - len(retained),
        "normalized_records": normalized_records,
    }
    if internal:
        summary["internal_records"] = len(internal)
    return summary


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


def project_scenario_actors(
    source_history: str | Path,
    output_history: str | Path,
    audit_path: str | Path,
) -> dict[str, int]:
    """Stamp the bearer-derived account and drop Raft read indexes for ``check.py``.

    Stage F authorizes every presentation read against the caller's account,
    so the reference model needs ``actor_user_id``. The server derives it from
    the bearer and never trusts it from the wire; the scenario knows which
    account each client identity signed in as. Protected reads also carry the
    Raft-assigned ``applied_index`` (and the preview's ``schema_version``),
    which the per-deck model does not reproduce. Every change is audited.
    """

    records = [
        json.loads(line)
        for line in Path(source_history).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    audit: list[dict[str, Any]] = []
    stamped = normalized = 0
    for record in records:
        actor = _SCENARIO_ACTORS.get(record.get("client"))
        arguments = record.get("args")
        if actor is not None and isinstance(arguments, dict) and "actor_user_id" not in arguments:
            arguments["actor_user_id"] = actor
            stamped += 1
        result = record.get("result")
        if record.get("op") in PROTECTED_READ_OPERATIONS and isinstance(result, dict):
            removed = [field for field in ("applied_index", "schema_version") if field in result]
            for field in removed:
                result.pop(field)
            if removed:
                normalized += 1
                audit.append({
                    "event": "checker_projection_normalization",
                    "reason": "raft_assigned_read_index",
                    "request_id": record.get("id"), "operation": record.get("op"),
                    "removed_fields": removed,
                })
    audit.append({"event": "checker_projection_actor_stamp", "stamped_records": stamped,
                  "actors": sorted(set(_SCENARIO_ACTORS.values()))})
    Path(output_history).write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records), encoding="utf-8",
    )
    with _history_lock, Path(audit_path).open("a", encoding="utf-8") as handle:
        for item in audit:
            handle.write(json.dumps(item, sort_keys=True) + "\n")
    return {"records": len(records), "stamped": stamped, "normalized": normalized}


def finalize_checked_scenario(
    scenario: str,
    history_path: str | Path,
    *,
    edit_operations: set[str],
    read_operations: set[str],
    unavailability_seconds: float | None = None,
    extra_evidence: Iterable[str] = (),
    checker_history_path: str | Path | None = None,
) -> None:
    """Persist checker, cost, and manifest records under ``run/*.jsonl``.

    ``checker_history_path`` lets a scenario validate a transparent projection
    while latency metrics continue to use the raw client history.
    """

    history = Path(history_path)
    checker_history = Path(checker_history_path) if checker_history_path is not None else history
    prefix = ROOT / "run" / scenario
    actor_history = prefix.with_name(prefix.name + "_actor_checker_history.jsonl")
    actor_audit = prefix.with_name(prefix.name + "_actor_projection_audit.jsonl")
    project_scenario_actors(checker_history, actor_history, actor_audit)
    checker_history = actor_history
    checker_path = prefix.with_name(prefix.name + "_checker.jsonl")
    metrics_path = prefix.with_name(prefix.name + "_metrics.jsonl")
    manifest_path = prefix.with_name(prefix.name + "_manifest.jsonl")
    write_checker_evidence(checker_history, checker_path, run_dir=ROOT / "run")
    write_latency_summary(
        history,
        metrics_path,
        edit_operations=edit_operations,
        read_operations=read_operations,
        unavailability_seconds=unavailability_seconds,
    )
    manifest_evidence: dict[str, Any] = {"actor_projection_audit": actor_audit.name}
    if checker_history != history:
        manifest_evidence["checker_history"] = checker_history.name
    write_manifest(
        manifest_path,
        scenario,
        history=history.name,
        checker=checker_path.name,
        metrics=metrics_path.name,
        extra_evidence=list(extra_evidence),
        **manifest_evidence,
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
    """Wait for equal applied indexes and identical durable decks, then read once.

    Stage F refuses follower-local deck reads, so per-replica convergence is
    each replica's applied durable deck record; the returned slide comes from
    one authenticated leader read that ``check.py`` linearizes.
    """

    from cluster_support import raft_state

    config = cluster.load()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = cluster.status(config)
        applied = [status.get("last_applied") for status in statuses.values() if status]
        if len(applied) == len(config["names"]) and len(set(applied)) == 1:
            decks = {
                json.dumps((raft_state(ROOT / "run", slot)["fsm"].get("decks") or {}).get(deck_id),
                           sort_keys=True)
                for slot in config["names"]
            }
            if len(decks) != 1:
                raise RuntimeError(f"durable presentation replicas did not converge for {deck_id}")
            return require_status(client.call("get_slide", deck_id=deck_id, slide_id=slide_id), "OK")
        time.sleep(0.2)
    raise RuntimeError("presentation replicas did not catch up before the scenario deadline")


def stable_uuid(name: str) -> str:
    """Return a repeatable UUID valid for clean-cluster scenario fixtures."""

    return str(uuid5(NAMESPACE_URL, f"distributed-slides-stage-b1:{name}"))


def published_deck(deck_id: str, slide_ids: Iterable[str], *, name: str) -> dict[str, Any]:
    """Build a canonical empty presentation snapshot for a live scenario."""

    return {
        "presentation": {
            "schema_version": 2,
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
                        "background": "#112233FF",
                    },
                    "elements": [],
                }
                for slide_id in slide_ids
            ],
        }
    }


def owned_deck(deck_id: str, slide_ids: Iterable[str], *, name: str,
               owner_user_id: str = SCENARIO_OWNER) -> dict[str, Any]:
    """Build a v3 publication whose immutable owner is the publishing account."""

    publication = published_deck(deck_id, slide_ids, name=name)
    publication["presentation"]["schema_version"] = 3
    publication["presentation"]["metadata"]["owner_user_id"] = owner_user_id
    return publication


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


def login_scenario_user(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    *,
    email: str = SCENARIO_EMAIL,
    password: str = SCENARIO_PASSWORD,
    node: str | None = None,
    timeout: float = 15.0,
) -> dict[str, str]:
    """Issue a password login and return an _auth dict {"access_token": ...}.

    The credential binds to ``client_name``, which must be the canonical UUID
    the scenario also sends on every guarded request. Uses recorded_request so
    the password and token are stripped from the JSONL history (ADR-010 §10).
    """
    if str(UUID(client_name)) != client_name:
        raise ValueError(f"scenario client identity must be a canonical UUID: {client_name!r}")
    resp = recorded_request(
        client_name,
        history_path,
        request_id,
        "login",
        {"email": email, "password": password, "client_id": client_name},
        node=node,
        timeout=timeout,
    )
    result = resp.get("result") or {}
    if result.get("status") != "AUTHENTICATED":
        raise RuntimeError(f"scenario login failed for {email!r}: {resp}")
    bearer = {"access_token": result["access_token"]}
    _SCENARIO_ACTORS[client_name] = result["user_id"]
    _SCENARIO_BEARERS[client_name] = bearer
    return bearer


class ScenarioClient(Client):
    """The starter ``Client`` with the scenario account's bearer on every call.

    The client identity must be a canonical UUID because a Stage E login binds
    it. ``call`` keeps the starter retry, redirect and history behaviour, but
    injects ``_auth`` only into the wire payload. The bearer comes from the
    latest ``login_scenario_user`` for this identity; with ``login=True`` the
    client signs in on construction.
    """

    def __init__(self, name: str, history: str | Path, *, login: bool = False, **kwargs: Any) -> None:
        if str(UUID(name)) != name:
            raise ValueError(f"scenario client identity must be a canonical UUID: {name!r}")
        super().__init__(name, history, **kwargs)
        if login:
            login_scenario_user(name, history, f"{name}:login")

    def call(self, op, node=None, **args):  # type: ignore[override]
        bearer = _SCENARIO_BEARERS.get(self.name)
        request_id = f"{self.name}-{uuid4().hex[:8]}"
        wire_args = {**args, "_auth": bearer} if bearer is not None else args
        start, deadline = time.time(), time.time() + self.timeout
        target = node or random.choice(list(self.nodes))
        response: dict[str, Any] = {"ok": False, "error": "sin respuesta"}
        served = None
        while time.time() < deadline:
            try:
                response = _post(self.nodes[target],
                                 {"id": request_id, "op": op, "args": wire_args, "client": self.name},
                                 max(0.5, min(6.0, deadline - time.time())))
            except Exception as error:
                response = {"ok": False, "error": type(error).__name__}
            served = target
            if response.get("redirect") in self.nodes:
                target = response["redirect"]
                continue
            if response.get("ok"):
                break
            if node is None:
                target = random.choice([candidate for candidate in self.nodes if candidate != target] or [target])
            time.sleep(0.2)
        self._record({
            "id": request_id, "client": self.name, "op": op,
            "args": {k: v for k, v in args.items() if not is_auth_secret_key(k)},
            "invoke": start, "complete": time.time() if response.get("ok") else None,
            "ok": bool(response.get("ok")), "result": redact_auth_data(response.get("result")),
            "error": response.get("error"), "node": served,
        })
        return response
