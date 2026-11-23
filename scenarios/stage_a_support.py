"""Shared checks for executable Stage A.1 fault-evidence scenarios.

The helpers deliberately use the starter's cluster status and ``/client``
interfaces.  They do not inspect a replica's in-memory Raft state.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cluster


def wait_for_leader(config: dict[str, Any], timeout: float = 15.0, *, exclude: set[str] | None = None) -> str:
    """Return a live leader observed through the starter ``/status`` route."""
    blocked = exclude or set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = [
            slot
            for slot, status in cluster.status(config).items()
            if slot not in blocked and status and status.get("role") == "leader"
        ]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.2)
    raise RuntimeError("no single leader became available before the scenario deadline")


def direct_request(url: str, *, request_id: str, client: str, op: str, args: dict[str, Any]) -> dict[str, Any]:
    """Send one request without following an application redirect or retrying."""
    payload = json.dumps({"id": request_id, "client": client, "op": op, "args": args}).encode()
    request = urllib.request.Request(url + "/client", payload, {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=6) as response:
            return json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, ValueError) as error:
        return {"ok": False, "error": type(error).__name__}


def record_trace(path: str | Path, event: str, **fields: Any) -> None:
    """Append a machine-readable observation archived by ``cluster.py down``."""
    record = {"timestamp": time.time(), "event": event, **fields}
    with Path(path).open("a") as trace:
        trace.write(json.dumps(record, sort_keys=True) + "\n")


def require_no_acknowledged_write(response: dict[str, Any]) -> None:
    """Fail if an isolated/minority one-hop request was confirmed as committed."""
    if response.get("ok") is True:
        raise RuntimeError(f"minority acknowledged a write: {response}")


def require_converged(finals: dict[str, dict[str, Any]]) -> Any:
    """Validate successful equal final local reads from every configured replica."""
    if not finals or not all(response.get("ok") is True for response in finals.values()):
        raise RuntimeError(f"final local reads were unavailable: {finals}")
    values = {json.dumps(response.get("result"), sort_keys=True) for response in finals.values()}
    if len(values) != 1:
        raise RuntimeError(f"replicas did not converge: {finals}")
    return next(iter(finals.values())).get("result")


def wait_for_catch_up(client: Any, key: str, timeout: float = 15.0) -> Any:
    """Wait for matching applied indexes, then record exactly one final-read set."""
    config = cluster.load()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = cluster.status(config)
        applied = [status.get("last_applied") for status in statuses.values() if status]
        if len(applied) == len(config["names"]) and len(set(applied)) == 1:
            finals = client.final("get", key=key)
            return require_converged(finals)
        time.sleep(0.2)
    raise RuntimeError("replicas did not catch up before the scenario deadline")


def new_request_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"
