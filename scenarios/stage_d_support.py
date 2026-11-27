"""Shared helpers for Stage D.1 distributed scenarios.

Extends stage_c_support with Stage D helpers for settings revisions,
deck/slide settings operations, and convergence checks.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Re-export everything from Stage C support (which re-exports Stage B)
from stage_c_support import (  # noqa: F401, E402
    INACTIVITY_TIMEOUT,
    RENEWAL_INTERVAL,
    SCENARIO_EMAIL,
    SCENARIO_PASSWORD,
    STAGE_C1_ENV,
    _post,
    append_evidence,
    assert_all_replicas_lock_state,
    build_expiration_checker_records,
    extract_internal_expiration_events,
    finalize_checked_scenario,
    last_history_entry,
    login_scenario_user,
    published_deck,
    recorded_request,
    renew_lock_request,
    require_status,
    stable_uuid,
    text_element,
    wait_for_event,
    wait_for_lease_epoch,
    wait_for_lock_expiration,
    wait_for_presentation_catch_up,
    write_checker_evidence,
    write_checker_projection,
    write_latency_summary,
    write_manifest,
)

import cluster


def update_deck_settings_request(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    deck_id: str,
    expected_deck_settings_version: int,
    patch: dict[str, Any],
    *,
    _auth: dict[str, Any] | None = None,
    node: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Record an update_deck_settings request with an exact ID."""
    return recorded_request(
        client_name,
        history_path,
        request_id,
        "update_deck_settings",
        {
            "deck_id": deck_id,
            "expected_deck_settings_version": expected_deck_settings_version,
            "patch": patch,
        },
        _auth=_auth,
        node=node,
        timeout=timeout,
    )


def update_slide_settings_request(
    client_name: str,
    history_path: str | Path,
    request_id: str,
    deck_id: str,
    slide_id: str,
    expected_slide_settings_version: int,
    patch: dict[str, Any],
    *,
    _auth: dict[str, Any] | None = None,
    node: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Record an update_slide_settings request with an exact ID."""
    return recorded_request(
        client_name,
        history_path,
        request_id,
        "update_slide_settings",
        {
            "deck_id": deck_id,
            "slide_id": slide_id,
            "expected_slide_settings_version": expected_slide_settings_version,
            "patch": patch,
        },
        _auth=_auth,
        node=node,
        timeout=timeout,
    )


def get_deck_preview(
    node_url: str,
    deck_id: str,
    *,
    timeout: float = 5.0,
    request_id: str | None = None,
    client_name: str = "_observe",
) -> dict[str, Any]:
    """Read the deck preview (settings + deck_version + slide_ids) from one node."""
    import time as _t
    rid = request_id or f"get-deck-preview-{int(_t.time() * 1000)}"
    return _post(
        node_url,
        {"id": rid, "op": "get_deck_preview", "args": {"deck_id": deck_id}, "client": client_name},
        timeout,
    )


def get_slide_obs(
    node_url: str,
    deck_id: str,
    slide_id: str,
    *,
    timeout: float = 5.0,
    request_id: str | None = None,
    client_name: str = "_observe",
) -> dict[str, Any]:
    """Read one slide (settings + ordinal) from one node."""
    import time as _t
    rid = request_id or f"get-slide-obs-{int(_t.time() * 1000)}"
    return _post(
        node_url,
        {
            "id": rid,
            "op": "get_slide",
            "args": {"deck_id": deck_id, "slide_id": slide_id},
            "client": client_name,
        },
        timeout,
    )


def wait_for_deck_settings_version(
    deck_id: str,
    min_version: int,
    *,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Poll any node until deck_settings_version >= min_version."""
    nodes = cluster.load()["host_nodes"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node_url in nodes.values():
            try:
                resp = _post(
                    node_url,
                    {
                        "id": f"wait-dsv-{deck_id[:8]}-{int(time.time() * 1000)}",
                        "op": "get_deck_preview",
                        "args": {"deck_id": deck_id},
                        "client": "_wait_helper",
                    },
                    2.0,
                )
                if resp.get("ok"):
                    result = resp.get("result") or {}
                    if result.get("deck_settings_version", -1) >= min_version:
                        return result
            except Exception:
                pass
        time.sleep(0.1)
    raise RuntimeError(
        f"deck_settings_version for {deck_id} did not reach {min_version} within {timeout}s"
    )


def wait_for_slide_settings_version(
    deck_id: str,
    slide_id: str,
    min_version: int,
    *,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Poll any node until slide_settings_version >= min_version."""
    nodes = cluster.load()["host_nodes"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for node_url in nodes.values():
            try:
                resp = _post(
                    node_url,
                    {
                        "id": f"wait-ssv-{slide_id[:8]}-{int(time.time() * 1000)}",
                        "op": "get_slide",
                        "args": {"deck_id": deck_id, "slide_id": slide_id},
                        "client": "_wait_helper",
                    },
                    2.0,
                )
                if resp.get("ok"):
                    result = resp.get("result") or {}
                    slide = result.get("slide") or {}
                    meta = slide.get("metadata") or {}
                    if meta.get("slide_settings_version", -1) >= min_version:
                        return result
            except Exception:
                pass
        time.sleep(0.1)
    raise RuntimeError(
        f"slide_settings_version for {slide_id} did not reach {min_version} within {timeout}s"
    )


def durable_deck(slot: str, deck_id: str, *, run_dir: Path | None = None) -> dict[str, Any]:
    """Return one replica's applied deck record from its durable Raft snapshot."""
    disk = json.loads(((run_dir or ROOT / "run") / slot / "disk.json").read_text())
    deck = (disk["raft_state"]["fsm"].get("decks") or {}).get(deck_id)
    if deck is None:
        raise KeyError(f"deck {deck_id} absent from {slot} snapshot")
    return deck


def assert_all_replicas_deck_settings(
    nodes_map: dict[str, str],
    deck_id: str,
    *,
    expected_deck_settings_version: int,
    expected_name: str | None = None,
    expected_width_px: int | None = None,
    expected_height_px: int | None = None,
    expected_deck_version: int | None = None,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    """Assert deck settings convergence on every replica's durable snapshot.

    Stage F (ADR-017) serves deck content only through the leader's bearer
    checked ReadIndex, so a per-replica check reads each replica's applied
    ``disk.json`` instead of a follower-local ``get_deck_preview``.
    Returns the per-node observations dict.
    """
    observations: dict[str, Any] = {}
    for slot in nodes_map:
        try:
            deck = durable_deck(slot, deck_id, run_dir=run_dir)
            observations[slot] = {
                "deck_id": deck_id, "deck_version": deck["deck_version"],
                "deck_settings_version": deck.get("deck_settings_version", 0),
                "metadata": dict(deck["metadata"]),
            }
        except Exception as exc:
            observations[slot] = {"error": str(exc)}

    for slot, obs in observations.items():
        if "error" in obs:
            raise AssertionError(f"replica {slot} unreachable: {obs['error']}\nall: {observations}")
        actual_dsv = obs.get("deck_settings_version")
        if actual_dsv != expected_deck_settings_version:
            raise AssertionError(
                f"replica {slot}: deck_settings_version={actual_dsv} "
                f"!= expected {expected_deck_settings_version}\nall: {observations}"
            )
        meta = obs.get("metadata") or {}
        if expected_name is not None and meta.get("name") != expected_name:
            raise AssertionError(
                f"replica {slot}: name={meta.get('name')!r} != {expected_name!r}\nall: {observations}"
            )
        if expected_width_px is not None and meta.get("width_px") != expected_width_px:
            raise AssertionError(
                f"replica {slot}: width_px={meta.get('width_px')} != {expected_width_px}\nall: {observations}"
            )
        if expected_height_px is not None and meta.get("height_px") != expected_height_px:
            raise AssertionError(
                f"replica {slot}: height_px={meta.get('height_px')} != {expected_height_px}\nall: {observations}"
            )
        if expected_deck_version is not None and obs.get("deck_version") != expected_deck_version:
            raise AssertionError(
                f"replica {slot}: deck_version={obs.get('deck_version')} "
                f"!= {expected_deck_version}\nall: {observations}"
            )
    return observations


def assert_all_replicas_slide_settings(
    nodes_map: dict[str, str],
    deck_id: str,
    slide_id: str,
    *,
    expected_slide_settings_version: int,
    expected_background: str | None = None,
    expected_ordinal: int | None = None,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    """Assert slide settings convergence on every replica's durable snapshot.

    Returns the per-node observations dict (``get_slide``-shaped).
    """
    observations: dict[str, Any] = {}
    for slot in nodes_map:
        try:
            deck = durable_deck(slot, deck_id, run_dir=run_dir)
            ordinal, slide = next(
                (index, candidate) for index, candidate in enumerate(deck["slides"])
                if candidate["metadata"]["slide_id"] == slide_id
            )
            metadata = {**slide["metadata"], "ordinal": ordinal}
            observations[slot] = {"deck_id": deck_id, "slide": {**slide, "metadata": metadata}}
        except Exception as exc:
            observations[slot] = {"error": str(exc)}

    for slot, obs in observations.items():
        if "error" in obs:
            raise AssertionError(f"replica {slot} unreachable: {obs['error']}\nall: {observations}")
        slide = obs.get("slide") or {}
        meta = slide.get("metadata") or {}
        actual_ssv = meta.get("slide_settings_version", 0)
        if actual_ssv != expected_slide_settings_version:
            raise AssertionError(
                f"replica {slot}: slide_settings_version={actual_ssv} "
                f"!= expected {expected_slide_settings_version}\nall: {observations}"
            )
        if expected_background is not None and meta.get("background") != expected_background:
            raise AssertionError(
                f"replica {slot}: background={meta.get('background')!r} != {expected_background!r}\nall: {observations}"
            )
        if expected_ordinal is not None and meta.get("ordinal") != expected_ordinal:
            raise AssertionError(
                f"replica {slot}: ordinal={meta.get('ordinal')} != {expected_ordinal}\nall: {observations}"
            )
    return observations


def wait_for_all_replicas_applied_equal(
    *,
    timeout: float = 20.0,
) -> dict[str, int]:
    """Wait until all replicas report the same last_applied index."""
    config = cluster.load()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = cluster.status(config)
        applied = {
            slot: status.get("last_applied")
            for slot, status in statuses.items()
            if status and status.get("last_applied") is not None
        }
        if len(applied) == len(config["names"]) and len(set(applied.values())) == 1:
            return applied
        time.sleep(0.2)
    raise RuntimeError(
        f"replicas did not reach same applied index within {timeout}s — last seen: {applied}"
    )
