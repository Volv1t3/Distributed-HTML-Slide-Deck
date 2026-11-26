"""Durable, client-local synchronization for Stage B shared intentions.

This module deliberately has no Raft or replica state.  It journals client
intentions before using the existing request path; Raft remains the only
authority that can turn an intention into a shared result.
"""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import time
from typing import Any, Callable
from uuid import uuid4

from auth_dto import is_auth_secret_key

# Confidence values that block protected mutations (Scoped Authority Gate).
_BLOCKED_CONFIDENCE_VALUES = frozenset({"UNVERIFIED", "LOST"})


def _contains_auth_fields(value: Any) -> bool:
    if isinstance(value, dict):
        return any(is_auth_secret_key(key) or _contains_auth_fields(item)
                   for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_auth_fields(item) for item in value)
    return False


class Lifecycle(str, Enum):
    """The durable lifecycle of one client intention."""

    PENDING_SYNC = "PENDING_SYNC"
    IN_FLIGHT = "IN_FLIGHT"
    COMMITTED = "COMMITTED"
    REJECTED_FINAL = "REJECTED_FINAL"
    SUPERSEDED = "SUPERSEDED"
    NEEDS_MANUAL_RECONCILIATION = "NEEDS_MANUAL_RECONCILIATION"


TERMINAL_LIFECYCLES = {
    Lifecycle.COMMITTED,
    Lifecycle.REJECTED_FINAL,
    Lifecycle.SUPERSEDED,
    Lifecycle.NEEDS_MANUAL_RECONCILIATION,
}


@dataclass(frozen=True)
class ResourceScope:
    """Hierarchical resource identity used by scheduling and validation."""

    deck_id: str
    slide_id: str | None = None
    element_id: str | None = None


@dataclass(frozen=True)
class OperationPolicy:
    """Stable metadata for one shared operation.

    Keeping scope, success, and index effects together prevents new operations
    from requiring edits to several unrelated switch statements.
    """

    deck_source: str = "argument"
    slide_source: str = "none"
    element_source: str = "none"
    success_statuses: frozenset[str] = frozenset()
    index_effect: str = "none"
    needs_creation_result: bool = False


OPERATION_POLICIES = {
    "publish_deck": OperationPolicy(
        deck_source="presentation", success_statuses=frozenset({"PUBLISHED"}), index_effect="publish",
    ),
    "create_slide": OperationPolicy(
        slide_source="slide_definition", success_statuses=frozenset({"CREATED"}), index_effect="create_slide",
    ),
    "delete_slide": OperationPolicy(
        slide_source="argument", success_statuses=frozenset({"DELETED"}), index_effect="delete_slide",
    ),
    "reorder_slides": OperationPolicy(success_statuses=frozenset({"REORDERED"})),
    "create_element": OperationPolicy(
        slide_source="argument", element_source="element_definition",
        success_statuses=frozenset({"CREATED_AND_LOCKED"}), index_effect="create_element",
    ),
    "acquire_lock": OperationPolicy(
        element_source="argument", success_statuses=frozenset({"LOCK_ACQUIRED", "ALREADY_HELD"}),
    ),
    "release_lock": OperationPolicy(
        element_source="argument", success_statuses=frozenset({"RELEASED"}), needs_creation_result=True,
    ),
    "update_element": OperationPolicy(
        element_source="argument", success_statuses=frozenset({"SAVED"}), needs_creation_result=True,
    ),
    "delete_element": OperationPolicy(
        element_source="argument", success_statuses=frozenset({"DELETED"}), needs_creation_result=True,
    ),
    "renew_lock": OperationPolicy(
        element_source="argument", success_statuses=frozenset({"RENEWED"}),
    ),
    # Stage D settings operations — no element authority required.
    "update_deck_settings": OperationPolicy(
        success_statuses=frozenset({"DECK_SETTINGS_UPDATED"}),
    ),
    "update_slide_settings": OperationPolicy(
        slide_source="argument",
        success_statuses=frozenset({"SLIDE_SETTINGS_UPDATED"}),
    ),
}

# Operations that require active Lock Authority to be transmitted.
_AUTHORITY_GATED_OPERATIONS = frozenset({"update_element", "delete_element"})

ELEMENT_OPERATIONS = frozenset(
    operation for operation, policy in OPERATION_POLICIES.items() if policy.element_source != "none"
)
EDIT_OPERATIONS = frozenset(OPERATION_POLICIES) - {"publish_deck"}


class LocalIntentionRejected(ValueError):
    """The projected local resource hierarchy cannot accept an intention."""


@dataclass
class Intention:
    """A replayable shared command owned by one logical client."""

    request_id: str
    client_id: str
    operation: str
    arguments: dict[str, Any]
    lifecycle: Lifecycle = Lifecycle.PENDING_SYNC
    dependencies: tuple[str, ...] = ()
    result: dict[str, Any] | None = None
    reason: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "client_id": self.client_id,
            "operation": self.operation,
            "arguments": self.arguments,
            "lifecycle": self.lifecycle.value,
            "dependencies": list(self.dependencies),
            "result": self.result,
            "reason": self.reason,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "Intention":
        return cls(
            request_id=value["request_id"],
            client_id=value["client_id"],
            operation=value["operation"],
            arguments=value["arguments"],
            lifecycle=Lifecycle(value.get("lifecycle", Lifecycle.PENDING_SYNC.value)),
            dependencies=tuple(value.get("dependencies", ())),
            result=value.get("result"),
            reason=value.get("reason"),
        )


class ClientIntentionOutbox:
    """Append-only Client Write-Ahead Journal plus dependency-aware scheduler.

    ``sender`` callbacks intentionally remain small adapters around the normal
    HTTP request path.  They never receive or expose Raft internals.
    """

    def __init__(self, logical_client_id: str, journal_path: str | Path, session_id: str,
                 *, user_id: str | None = None, deck_id: str | None = None) -> None:
        if not logical_client_id:
            raise ValueError("logical_client_id must not be empty")
        self.logical_client_id = logical_client_id
        self.session_id = session_id
        self.journal_path = Path(journal_path)
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        if (user_id is None) != (deck_id is None):
            raise ValueError("account and deck binding must be provided together")
        self.user_id = user_id
        self.deck_id = deck_id
        self._gate_suspended = user_id is not None
        self._requires_explicit_reconciliation = False
        self._last_observed_access = "UNKNOWN"
        self._access_version: int | None = None
        self._applied_permission_index: int | None = None
        self._observation_epoch = 0
        self._accepted_observation_epoch = -1
        self._approved_after_loss: set[str] = set()
        self._intentions: OrderedDict[str, Intention] = OrderedDict()
        self._reconciliation: dict[str, dict[str, Any]] = {}
        self._base_projection: dict[str, Any] = {}
        self._highest_applied_index = -1
        # Scoped Authority Gate: element_id → confidence string ("UNVERIFIED", "LOST", …)
        self._lock_confidence: dict[str, str] = {}
        self._replay()
        if self.user_id is not None and not self.journal_path.exists():
            self._append({"kind": "scope", "user_id": self.user_id, "deck_id": self.deck_id})
        self._recover_uncertain_in_flight()
        self._rebuild_resource_index()

    @property
    def highest_applied_index(self) -> int:
        return self._highest_applied_index

    @property
    def base_projection(self) -> dict[str, Any]:
        return deepcopy(self._base_projection)

    @property
    def speculative_projection(self) -> dict[str, Any]:
        """Local-only pending state; it is never presented as committed state."""
        pending_elements: list[dict[str, Any]] = []
        pending_deletions: list[str] = []
        for intention in self._intentions.values():
            if intention.lifecycle not in {Lifecycle.PENDING_SYNC, Lifecycle.IN_FLIGHT}:
                continue
            if intention.operation == "create_element":
                element = intention.arguments.get("element", {})
                pending_elements.append({
                    "deck_id": intention.arguments.get("deck_id"),
                    "slide_id": intention.arguments.get("slide_id"),
                    "element_id": element.get("element_id"),
                    "state": "PENDING_ELEMENT_CREATION",
                })
            if intention.operation == "delete_slide":
                pending_deletions.append(str(intention.arguments.get("slide_id")))
        return {
            "pending_element_creations": pending_elements,
            "pending_slide_deletions": pending_deletions,
        }

    def get(self, request_id: str) -> Intention:
        return deepcopy(self._intentions[request_id])

    @property
    def gate(self) -> dict[str, Any]:
        return {"user_id": self.user_id, "deck_id": self.deck_id,
                "suspended": self._gate_suspended,
                "requires_explicit_reconciliation": self._requires_explicit_reconciliation,
                "last_observed_access": self._last_observed_access}

    def begin_permission_observation(self) -> int:
        self._observation_epoch += 1
        return self._observation_epoch

    def observe_permission(self, result: dict[str, Any], *, epoch: int) -> bool:
        """Apply a fresh self-role result, fencing delayed responses locally."""
        if self.user_id is None or result.get("status") != "OK":
            return False
        role = result.get("effective_access")
        if role not in {"OWNER", "EDIT", "VIEW", "NONE"} or epoch < self._accepted_observation_epoch:
            return False
        version = result.get("access_version")
        index = result.get("applied_index")
        if role != "NONE" and (type(version) is not int or type(index) is not int):
            return False
        if role != "NONE" and self._access_version is not None and version < self._access_version:
            return False
        previous = self._last_observed_access
        self._accepted_observation_epoch = epoch
        self._last_observed_access = role
        if role != "NONE":
            self._access_version, self._applied_permission_index = version, index
        if role in {"VIEW", "NONE"}:
            self._approved_after_loss.clear()
            if any(i.lifecycle in {Lifecycle.PENDING_SYNC, Lifecycle.IN_FLIGHT}
                   and i.operation in EDIT_OPERATIONS for i in self._intentions.values()):
                self._requires_explicit_reconciliation = True
            self._gate_suspended = True
        else:
            self._gate_suspended = self._requires_explicit_reconciliation
        self._persist_gate()
        return True

    def suspend_permission(self, *, access_lost: bool = False) -> None:
        if self.user_id is None:
            return
        self._gate_suspended = True
        self._observation_epoch += 1
        self._accepted_observation_epoch = self._observation_epoch
        self._last_observed_access = "UNKNOWN"
        if access_lost:
            self._requires_explicit_reconciliation = True
            self._approved_after_loss.clear()
        self._persist_gate()

    def _persist_gate(self) -> None:
        self._append({"kind": "gate", "suspended": self._gate_suspended,
                      "requires_explicit_reconciliation": self._requires_explicit_reconciliation,
                      "last_observed_access": self._last_observed_access,
                      "approved_request_ids": sorted(self._approved_after_loss)})

    def denied_recovery_view(self) -> list[Intention]:
        """Read-only access to retained terminal denials, including original patches."""
        return [deepcopy(i) for i in self._intentions.values()
                if i.lifecycle is Lifecycle.REJECTED_FINAL
                and (i.result or {}).get("status") == "ACCESS_DENIED"]

    def apply_receipt(self, request_id: str, response: dict[str, Any] | None) -> Intention:
        """Record minimal committed evidence without inventing result values."""
        intention = self._intentions[request_id]
        if intention.lifecycle in TERMINAL_LIFECYCLES:
            return self.get(request_id)
        result = self._successful_result(response)
        status = result.get("status") if result is not None else None
        if status == "COMMITTED_SUCCESS":
            self._transition(intention, Lifecycle.COMMITTED, result={"status": status})
        elif status == "COMMITTED_REJECTED":
            self._transition(intention, Lifecycle.REJECTED_FINAL, result={"status": status})
        self._rebuild_resource_index()
        return self.get(request_id)

    def resume_after_reconciliation(self) -> None:
        """Explicitly release a reviewed gate once old unsent edits are resolved."""
        if self._last_observed_access not in {"OWNER", "EDIT"}:
            raise LocalIntentionRejected("fresh EDIT permission is required")
        if any(i.lifecycle is Lifecycle.PENDING_SYNC and i.operation in EDIT_OPERATIONS
               and i.request_id not in self._approved_after_loss
               for i in self._intentions.values()):
            raise LocalIntentionRejected("pending edits require explicit review")
        self._requires_explicit_reconciliation = False
        self._gate_suspended = False
        self._persist_gate()

    def approve_reconciled_intention(self, original_request_id: str, operation: str,
                                     arguments: dict[str, Any], *, request_id: str | None = None) -> Intention:
        """Record a reviewed replacement with a new ID and current preconditions."""
        original = self._intentions[original_request_id]
        if original.operation != operation or original.lifecycle not in {
            Lifecycle.PENDING_SYNC, Lifecycle.REJECTED_FINAL, Lifecycle.NEEDS_MANUAL_RECONCILIATION,
        }:
            raise LocalIntentionRejected("original is not available for manual correction")
        if self._last_observed_access not in {"OWNER", "EDIT"} or self._highest_applied_index < 0:
            raise LocalIntentionRejected("fresh permission and deck refresh are required")
        if request_id == original_request_id:
            raise LocalIntentionRejected("corrected operation requires a new request id")
        replacement = self.enqueue(operation, arguments, request_id=request_id)
        if original.lifecycle is Lifecycle.PENDING_SYNC:
            self._transition(original, Lifecycle.SUPERSEDED, reason=f"replaced_by:{replacement.request_id}")
        self._approved_after_loss.add(replacement.request_id)
        self._persist_gate()
        self._rebuild_resource_index()
        return replacement

    def set_lock_confidence(self, element_id: str, confidence: Any) -> None:
        """Record the client's current MaintainedLockConfidence for one element.

        Accepts a MaintainedLockConfidence enum value or its string equivalent.
        The gate uses only the value string, so no import of lock_maintenance
        is required here.
        """
        self._lock_confidence[element_id] = str(confidence.value if hasattr(confidence, "value") else confidence)

    def get_lock_confidence(self, element_id: str) -> str:
        """Return the last recorded confidence string, or "VERIFIED" if unknown."""
        return self._lock_confidence.get(element_id, "VERIFIED")

    def enqueue(self, operation: str, arguments: dict[str, Any], *, request_id: str | None = None) -> Intention:
        """Persist a validated intention before any caller may send it."""
        if not isinstance(arguments, dict):
            raise LocalIntentionRejected("intention arguments must be an object")
        if _contains_auth_fields(arguments):
            raise LocalIntentionRejected("authentication fields cannot be journaled")
        request_id = request_id or uuid4().hex
        if request_id in self._intentions:
            raise LocalIntentionRejected(f"request id {request_id!r} already exists in this journal")
        copied = deepcopy(arguments)
        if operation in ELEMENT_OPERATIONS:
            copied.setdefault("session_id", self.session_id)
        scope = self._scope(operation, copied)
        self._check_scope(scope.deck_id)
        self._validate_local_scope(operation, scope)
        if operation == "delete_slide":
            self._supersede_unsent_descendants(scope, request_id)
        dependencies = self._dependencies_with_settings_fifo(operation, scope)
        intention = Intention(request_id, self.logical_client_id, operation, copied, dependencies=dependencies)
        self._intentions[request_id] = intention
        self._append({"kind": "intent", "intention": intention.to_json()})
        self._rebuild_resource_index()
        return self.get(request_id)

    def synchronize(self, sender: Callable[[Intention], dict[str, Any] | None]) -> list[Intention]:
        """Attempt each currently eligible intention without imposing global FIFO."""
        sent: list[Intention] = []
        # Iterate over a stable snapshot: sender callbacks can enqueue another intent.
        for request_id in list(self._intentions):
            intention = self._intentions[request_id]
            if not self._eligible(intention):
                continue
            if not self._materialize_creation_preconditions(intention):
                continue
            self._materialize_settings_preconditions(intention)
            self._transition(intention, Lifecycle.IN_FLIGHT)
            try:
                response = sender(self.get(request_id))
            except Exception as error:  # transport failures are deliberately uncertain
                self._transition(intention, Lifecycle.PENDING_SYNC, reason=type(error).__name__)
                continue
            sent.append(self.get(request_id))
            self._record_response(intention, response)
        return sent

    def poll(
        self,
        deck_id: str,
        sender: Callable[[str, dict[str, Any], str], dict[str, Any] | None],
        *,
        watched_element_ids: tuple[str, ...] = (),
    ) -> bool:
        """Poll protected leader reads without installing an incomplete projection."""
        self._check_scope(deck_id)
        epoch = self.begin_permission_observation()
        response = sender("get_deck_preview", {"deck_id": deck_id}, self._read_request_id())
        preview = self._successful_result(response)
        if preview is not None and preview.get("status") == "ACCESS_DENIED":
            self.suspend_permission(access_lost=True)
            return False
        if preview is None or not isinstance(preview.get("applied_index"), int):
            self.suspend_permission()
            return False
        if self.user_id is not None and not self.observe_permission(preview, epoch=epoch):
            return False
        if preview["applied_index"] < self._highest_applied_index:
            return False

        previous_slides = set(self._base_projection.get("slide_ids", ()))
        new_slides = set(preview.get("slide_ids", ()))
        projection = {
            "deck_id": deck_id,
            "applied_index": preview["applied_index"],
            "deck_version": preview.get("deck_version"),
            "deck_settings_version": preview.get("deck_settings_version"),
            "slide_ids": list(preview.get("slide_ids", ())),
            "metadata": deepcopy(preview.get("metadata")),
            "slides": deepcopy(self._base_projection.get("slides", {})),
            "elements": deepcopy(self._base_projection.get("elements", {})),
            "locks": deepcopy(self._base_projection.get("locks", {})),
        }
        # Any newly applied command may be an element edit that deliberately
        # leaves deck_version unchanged.  Refresh the deck's slides whenever
        # the replica's applied index advances; a lagging observation is
        # rejected above and an equal observation needs no subordinate reads.
        if preview["applied_index"] > self._highest_applied_index:
            changed_slides = new_slides
        else:
            changed_slides = new_slides - previous_slides
        for slide_id in changed_slides:
            slide_response = sender("get_slide", {"deck_id": deck_id, "slide_id": slide_id}, self._read_request_id())
            slide = self._successful_result(slide_response)
            if slide is None or slide.get("status") == "ACCESS_DENIED":
                self.suspend_permission(access_lost=slide is not None)
                return False
            if slide is not None and slide.get("status") == "OK":
                projection["slides"][slide_id] = deepcopy(slide["slide"])
        for slide_id in previous_slides - new_slides:
            projection["slides"].pop(slide_id, None)
        observed_element_ids = set(watched_element_ids)
        for slide in projection["slides"].values():
            for element in slide.get("elements", ()):
                element_id = element.get("element_id")
                if isinstance(element_id, str):
                    observed_element_ids.add(element_id)
        for element_id in sorted(observed_element_ids):
            element_response = sender("get_element", {"deck_id": deck_id, "element_id": element_id}, self._read_request_id())
            element = self._successful_result(element_response)
            if element is None or element.get("status") == "ACCESS_DENIED":
                self.suspend_permission(access_lost=element is not None)
                return False
            if element is not None and element.get("status") == "OK":
                projection["elements"][element_id] = deepcopy(element)
            lock_response = sender(
                "read_lock_status", {"deck_id": deck_id, "element_id": element_id}, self._read_request_id(),
            )
            lock = self._successful_result(lock_response)
            if lock is None or lock.get("status") == "ACCESS_DENIED":
                self.suspend_permission(access_lost=lock is not None)
                return False
            if lock is not None:
                projection["locks"][element_id] = deepcopy(lock)
        self.record_base_projection(projection)
        return True

    def discard(self, request_id: str) -> Intention:
        """Persist the user's decision to abandon a pending or conflicted draft."""
        intention = self._intentions[request_id]
        if intention.lifecycle not in {
            Lifecycle.PENDING_SYNC,
            Lifecycle.NEEDS_MANUAL_RECONCILIATION,
        }:
            raise LocalIntentionRejected("only a pending or conflicted intention can be discarded")
        self._transition(intention, Lifecycle.SUPERSEDED, reason="user_discarded", allow_terminal=True)
        self._rebuild_resource_index()
        return self.get(request_id)

    def record_base_projection(self, projection: dict[str, Any]) -> None:
        """Install a non-authoritative observation only if it is monotonic."""
        applied_index = projection.get("applied_index")
        self._check_scope(projection.get("deck_id"))
        if not isinstance(applied_index, int):
            raise ValueError("base projection requires an integer applied_index")
        if applied_index < self._highest_applied_index:
            return
        self._base_projection = deepcopy(projection)
        self._highest_applied_index = applied_index
        self._rebuild_resource_index()

    def prepare_reconciliation(
        self,
        original_request_id: str,
        sender: Callable[[str, dict[str, Any], str], dict[str, Any] | None],
    ) -> dict[str, Any]:
        """Perform the user-selected fresh-lock and authoritative-refresh sequence.

        This intentionally stops before a corrected save.  A user must provide
        the replacement patch through :meth:`submit_reconciled_update`.
        """
        original = self._intentions[original_request_id]
        if original.lifecycle is not Lifecycle.NEEDS_MANUAL_RECONCILIATION:
            raise LocalIntentionRejected("only a conflicted intention can be reconciled")
        if original.operation != "update_element":
            raise LocalIntentionRejected("manual reconciliation currently applies to element updates")
        conflict = original.result or {}
        conflict_value = conflict.get("value", {})
        min_commit_index = conflict_value.get("min_commit_index")
        if not isinstance(min_commit_index, int):
            raise LocalIntentionRejected("conflict result did not provide min_commit_index")
        lock = self.enqueue(
            "acquire_lock",
            {"deck_id": original.arguments["deck_id"], "element_id": original.arguments["element_id"]},
        )
        lock_response = self._send_one(lock.request_id, sender)
        lock_result = self._intentions[lock.request_id]
        if lock_result.lifecycle is not Lifecycle.COMMITTED:
            raise LocalIntentionRejected("fresh lock acquisition was not committed")
        lock_value = (lock_result.result or {}).get("value", {})
        fencing_token = lock_value.get("fencing_token")
        if not isinstance(fencing_token, int):
            raise LocalIntentionRejected("fresh lock result did not provide a fencing token")
        lock_commit_index = lock_response.get("committed_index") if isinstance(lock_response, dict) else None
        if not isinstance(lock_commit_index, int):
            raise LocalIntentionRejected("fresh lock result did not provide its commit index")
        refresh_floor = max(min_commit_index, lock_commit_index)
        refresh = sender(
            "authoritative_refresh_element",
            {
                "deck_id": original.arguments["deck_id"],
                "element_id": original.arguments["element_id"],
                "min_commit_index": refresh_floor,
            },
            self._read_request_id(),
        )
        authoritative = self._successful_result(refresh)
        if authoritative is None or authoritative.get("status") != "OK":
            raise LocalIntentionRejected("authoritative refresh was unavailable")
        version = authoritative.get("element_version")
        if not isinstance(version, int):
            raise LocalIntentionRejected("authoritative refresh did not provide an element version")
        context = {
            "fencing_token": fencing_token,
            "expected_version": version,
            "min_commit_index": refresh_floor,
            "authoritative": deepcopy(authoritative),
        }
        self._reconciliation[original_request_id] = context
        self._append({"kind": "reconciliation_ready", "request_id": original_request_id, "context": context})
        return deepcopy(context)

    def submit_reconciled_update(
        self, original_request_id: str, corrected_patch: dict[str, Any], *, request_id: str | None = None,
    ) -> Intention:
        """Journal, but do not automatically send, a user-corrected replacement."""
        original = self._intentions[original_request_id]
        context = self._reconciliation.get(original_request_id)
        if original.lifecycle is not Lifecycle.NEEDS_MANUAL_RECONCILIATION or context is None:
            raise LocalIntentionRejected("reconciliation has not been prepared")
        if not isinstance(corrected_patch, dict) or not corrected_patch:
            raise LocalIntentionRejected("a non-empty user correction is required")
        if request_id == original_request_id:
            raise LocalIntentionRejected("corrected update requires a new request id")
        arguments = deepcopy(original.arguments)
        arguments["fencing_token"] = context["fencing_token"]
        arguments["expected_version"] = context["expected_version"]
        arguments["patch"] = deepcopy(corrected_patch)
        return self.enqueue("update_element", arguments, request_id=request_id)

    def submit_reconciled_settings(
        self,
        original_request_id: str,
        corrected_patch: dict[str, Any],
        *,
        request_id: str | None = None,
    ) -> "Intention":
        """Journal a user-corrected settings patch using the conflict's returned revision.

        The original conflicted intention remains as terminal audit history in
        NEEDS_MANUAL_RECONCILIATION.  The corrected intention is a fresh PENDING_SYNC
        entry with the server's returned settings revision as its expected version.
        """
        original = self._intentions[original_request_id]
        if original.lifecycle is not Lifecycle.NEEDS_MANUAL_RECONCILIATION:
            raise LocalIntentionRejected("only a conflicted intention can be reconciled")
        if original.operation not in {"update_deck_settings", "update_slide_settings"}:
            raise LocalIntentionRejected(
                "submit_reconciled_settings applies only to settings operations; "
                "use submit_reconciled_update for element conflicts"
            )
        if request_id == original_request_id:
            raise LocalIntentionRejected("corrected settings requires a new request id")
        if not isinstance(corrected_patch, dict) or not corrected_patch:
            raise LocalIntentionRejected("a non-empty corrected patch is required")
        conflict_value = (original.result or {}).get("value", {})
        arguments = deepcopy(original.arguments)
        arguments["patch"] = deepcopy(corrected_patch)
        if original.operation == "update_deck_settings":
            confirmed_version = conflict_value.get("deck_settings_version")
            if not isinstance(confirmed_version, int):
                raise LocalIntentionRejected(
                    "conflict result did not provide deck_settings_version"
                )
            arguments["expected_deck_settings_version"] = confirmed_version
        else:
            confirmed_version = conflict_value.get("slide_settings_version")
            if not isinstance(confirmed_version, int):
                raise LocalIntentionRejected(
                    "conflict result did not provide slide_settings_version"
                )
            arguments["expected_slide_settings_version"] = confirmed_version
        return self.enqueue(original.operation, arguments, request_id=request_id)

    def _send_one(
        self,
        request_id: str,
        sender: Callable[[str, dict[str, Any], str], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Send exactly one eligible intention for an explicit workflow step."""
        intention = self._intentions[request_id]
        if not self._eligible(intention) or not self._materialize_creation_preconditions(intention):
            return None
        self._materialize_settings_preconditions(intention)
        self._transition(intention, Lifecycle.IN_FLIGHT)
        try:
            response = sender(intention.operation, deepcopy(intention.arguments), intention.request_id)
        except Exception as error:
            self._transition(intention, Lifecycle.PENDING_SYNC, reason=type(error).__name__)
            return None
        self._record_response(intention, response)
        return response

    def _scope(self, operation: str, arguments: dict[str, Any]) -> ResourceScope:
        policy = OPERATION_POLICIES.get(operation)
        if policy is None:
            raise LocalIntentionRejected(f"unsupported shared operation {operation!r}")
        if policy.deck_source == "presentation":
            try:
                deck_id = str(arguments["presentation"]["metadata"]["deck_id"])
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("publication requires presentation metadata.deck_id") from error
        else:
            deck_id = arguments.get("deck_id")
            if not isinstance(deck_id, str) or not deck_id:
                raise LocalIntentionRejected(f"{operation} requires deck_id")

        slide_id = None
        if policy.slide_source == "slide_definition":
            try:
                slide_id = str(arguments["slide"]["metadata"]["slide_id"])
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("slide creation requires slide metadata.slide_id") from error
        elif policy.slide_source == "argument":
            slide_id = self._required_string(arguments, "slide_id")

        element_id = None
        if policy.element_source == "element_definition":
            try:
                element_id = str(arguments["element"]["element_id"])
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("element creation requires element.element_id") from error
        elif policy.element_source == "argument":
            element_id = self._required_string(arguments, "element_id")
        return ResourceScope(deck_id, slide_id, element_id)

    @staticmethod
    def _required_string(arguments: dict[str, Any], key: str) -> str:
        value = arguments.get(key)
        if not isinstance(value, str) or not value:
            raise LocalIntentionRejected(f"requires non-empty {key}")
        return value

    def _validate_local_scope(self, operation: str, scope: ResourceScope) -> None:
        if operation != "publish_deck" and scope.deck_id not in self._decks:
            raise LocalIntentionRejected("target deck is missing from the projected hierarchy")
        if scope.deck_id in self._pending_deleted_decks:
            raise LocalIntentionRejected("target deck is pending deletion")
        if scope.slide_id is not None:
            if scope.slide_id in self._pending_deleted_slides:
                raise LocalIntentionRejected("target slide is pending deletion")
            if operation != "create_slide" and (scope.deck_id, scope.slide_id) not in self._slides:
                raise LocalIntentionRejected("target slide is missing from the projected hierarchy")
        if scope.element_id is not None and operation != "create_element":
            resource = self._elements.get((scope.deck_id, scope.element_id))
            if resource is None:
                raise LocalIntentionRejected("target element is missing from the projected hierarchy")
            if resource[1] in self._pending_deleted_slides:
                raise LocalIntentionRejected("target element belongs to a pending-deletion slide")

    # Lifecycle states that are considered "terminal" when computing FIFO
    # dependency chains for new settings intentions.  NEEDS_MANUAL_RECONCILIATION
    # is included so a corrected resubmission does not depend on the original
    # conflicted intention — the conflict result is already the reconciliation base.
    _SETTINGS_FIFO_TERMINAL = frozenset({
        Lifecycle.COMMITTED,
        Lifecycle.REJECTED_FINAL,
        Lifecycle.SUPERSEDED,
        Lifecycle.NEEDS_MANUAL_RECONCILIATION,
    })

    def _dependencies(self, scope: ResourceScope) -> tuple[str, ...]:
        dependencies: list[str] = []
        for intention in self._intentions.values():
            if intention.lifecycle in {Lifecycle.COMMITTED, Lifecycle.REJECTED_FINAL, Lifecycle.SUPERSEDED}:
                continue
            other = self._scope(intention.operation, intention.arguments)
            if other.deck_id != scope.deck_id:
                continue
            if intention.operation == "publish_deck":
                dependencies.append(intention.request_id)
            if scope.slide_id is not None and intention.operation == "create_slide" and other.slide_id == scope.slide_id:
                dependencies.append(intention.request_id)
            if scope.element_id is not None and intention.operation == "create_element" and other.element_id == scope.element_id:
                dependencies.append(intention.request_id)
        return tuple(dict.fromkeys(dependencies))

    def _dependencies_with_settings_fifo(
        self, operation: str, scope: ResourceScope
    ) -> tuple[str, ...]:
        """Full dependency chain including settings FIFO ordering."""
        structural = list(self._dependencies(scope))
        if operation not in {"update_deck_settings", "update_slide_settings"}:
            return tuple(dict.fromkeys(structural))
        fifo: list[str] = []
        for intention in self._intentions.values():
            if intention.lifecycle in self._SETTINGS_FIFO_TERMINAL:
                continue
            if intention.operation != operation:
                continue
            other = self._scope(intention.operation, intention.arguments)
            if other.deck_id != scope.deck_id:
                continue
            if operation == "update_deck_settings":
                # Deck settings FIFO: same deck_id, no slide scoping
                fifo.append(intention.request_id)
            elif operation == "update_slide_settings":
                # Slide settings FIFO: same (deck_id, slide_id)
                if other.slide_id == scope.slide_id:
                    fifo.append(intention.request_id)
        return tuple(dict.fromkeys(structural + fifo))

    def _supersede_unsent_descendants(self, scope: ResourceScope, delete_request_id: str) -> None:
        for intention in list(self._intentions.values()):
            if intention.request_id == delete_request_id or intention.lifecycle is not Lifecycle.PENDING_SYNC:
                continue
            other = self._scope(intention.operation, intention.arguments)
            if other.deck_id != scope.deck_id:
                continue
            if other.slide_id == scope.slide_id or (
                other.element_id is not None
                and self._elements.get((scope.deck_id, other.element_id), (None, None))[1] == scope.slide_id
            ):
                self._transition(intention, Lifecycle.SUPERSEDED, reason=f"superseded_by:{delete_request_id}")

    def _eligible(self, intention: Intention) -> bool:
        if (intention.operation in EDIT_OPERATIONS and self._gate_suspended
                and not (self._last_observed_access in {"OWNER", "EDIT"}
                         and intention.request_id in self._approved_after_loss)):
            return False
        if intention.lifecycle is not Lifecycle.PENDING_SYNC:
            return False
        if not all(
            self._intentions[dependency].lifecycle in {
                Lifecycle.COMMITTED,
                Lifecycle.REJECTED_FINAL,
                Lifecycle.SUPERSEDED,
                Lifecycle.NEEDS_MANUAL_RECONCILIATION,
            }
            for dependency in intention.dependencies
        ):
            return False
        # Scoped Authority Gate: protected mutations are withheld when the
        # client's lock confidence is uncertain or lost.
        if intention.operation in _AUTHORITY_GATED_OPERATIONS:
            element_id = intention.arguments.get("element_id")
            if element_id and self._lock_confidence.get(element_id, "VERIFIED") in _BLOCKED_CONFIDENCE_VALUES:
                return False
        return True

    def _materialize_creation_preconditions(self, intention: Intention) -> bool:
        """Fill an offline child's lock/version only from a committed creator.

        A pending element can have later local draft edits, but it has no
        client-invented fencing token.  The materialized values are journaled
        before the dependent command's first send so a restart retries the
        same authoritative preconditions.
        """
        if not OPERATION_POLICIES[intention.operation].needs_creation_result:
            return True
        element_id = intention.arguments.get("element_id")
        changed = False
        for dependency in intention.dependencies:
            creator = self._intentions[dependency]
            if creator.operation != "create_element":
                continue
            created = creator.arguments.get("element", {}).get("element_id")
            if created != element_id:
                continue
            value = (creator.result or {}).get("value", {})
            fencing_token = value.get("fencing_token")
            element_version = value.get("element_version")
            if not isinstance(fencing_token, int):
                return False
            if intention.arguments.get("fencing_token") != fencing_token:
                intention.arguments["fencing_token"] = fencing_token
                changed = True
            if intention.operation in {"update_element", "delete_element"} and isinstance(element_version, int):
                if intention.arguments.get("expected_version") != element_version:
                    intention.arguments["expected_version"] = element_version
                    changed = True
        if changed:
            self._append({
                "kind": "arguments_materialized", "request_id": intention.request_id,
                "arguments": intention.arguments,
            })
        return True

    def _materialize_settings_preconditions(self, intention: Intention) -> None:
        """Fill the expected settings revision from the predecessor's committed result.

        A later settings intention in the same FIFO lane inherits the predecessor's
        confirmed settings revision before its first network send.  This is journaled
        so a restart retries the same authoritative version expectation.
        """
        if intention.operation not in {"update_deck_settings", "update_slide_settings"}:
            return
        scope = self._scope(intention.operation, intention.arguments)
        changed = False
        # A denied predecessor is terminal but has no revision. Search the
        # established local history for the last successful result in this lane.
        for dep in reversed(list(self._intentions.values())):
            if dep.request_id == intention.request_id:
                continue
            if dep.operation != intention.operation:
                continue
            if dep.lifecycle is not Lifecycle.COMMITTED:
                continue
            dep_scope = self._scope(dep.operation, dep.arguments)
            # Check same settings resource
            if dep_scope.deck_id != scope.deck_id:
                continue
            if intention.operation == "update_slide_settings" and dep_scope.slide_id != scope.slide_id:
                continue
            value = (dep.result or {}).get("value", {})
            if intention.operation == "update_deck_settings":
                confirmed = value.get("deck_settings_version")
                if isinstance(confirmed, int) and intention.arguments.get("expected_deck_settings_version") != confirmed:
                    intention.arguments["expected_deck_settings_version"] = confirmed
                    changed = True
                if isinstance(confirmed, int):
                    break
            else:
                confirmed = value.get("slide_settings_version")
                if isinstance(confirmed, int) and intention.arguments.get("expected_slide_settings_version") != confirmed:
                    intention.arguments["expected_slide_settings_version"] = confirmed
                    changed = True
                if isinstance(confirmed, int):
                    break
        if changed:
            self._append({
                "kind": "arguments_materialized",
                "request_id": intention.request_id,
                "arguments": intention.arguments,
            })

    def _record_response(self, intention: Intention, response: dict[str, Any] | None) -> None:
        result = self._successful_result(response)
        if _contains_auth_fields(result):
            self._transition(intention, Lifecycle.PENDING_SYNC, reason="invalid_secret_bearing_response")
            return
        if result is None or result.get("status") == "QUORUM_UNAVAILABLE":
            self._transition(intention, Lifecycle.PENDING_SYNC, reason="uncertain_transport_or_quorum")
            return
        status = result.get("status")
        if status == "ACCESS_DENIED":
            self.suspend_permission(access_lost=True)
        _CONFLICT_STATUSES = {
            "VERSION_CONFLICT",
            "DECK_SETTINGS_VERSION_CONFLICT",
            "SLIDE_SETTINGS_VERSION_CONFLICT",
        }
        if status in _CONFLICT_STATUSES:
            self._transition(intention, Lifecycle.NEEDS_MANUAL_RECONCILIATION, result=result)
        elif status in OPERATION_POLICIES[intention.operation].success_statuses:
            self._transition(intention, Lifecycle.COMMITTED, result=result)
        else:
            self._transition(intention, Lifecycle.REJECTED_FINAL, result=result)
        self._rebuild_resource_index()

    @staticmethod
    def _successful_result(response: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(response, dict) or not response.get("ok"):
            return None
        result = response.get("result")
        return result if isinstance(result, dict) else None

    def _transition(
        self, intention: Intention, lifecycle: Lifecycle, *, result: dict[str, Any] | None = None,
        reason: str | None = None, allow_terminal: bool = False,
    ) -> None:
        if intention.lifecycle in TERMINAL_LIFECYCLES and not allow_terminal:
            return
        intention.lifecycle = lifecycle
        if result is not None:
            intention.result = deepcopy(result)
        if reason is not None:
            intention.reason = reason
        self._append({
            "kind": "transition", "request_id": intention.request_id, "lifecycle": lifecycle.value,
            "result": intention.result, "reason": intention.reason,
        })

    def _replay(self) -> None:
        if not self.journal_path.exists():
            return
        saw_scope = False
        for line_number, line in enumerate(self.journal_path.read_text(encoding="utf-8").splitlines(), start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid journal JSON at line {line_number}") from error
            kind = event.get("kind")
            if kind == "intent":
                intention = Intention.from_json(event["intention"])
                if intention.client_id != self.logical_client_id:
                    raise ValueError("journal belongs to a different logical client")
                if self.deck_id is not None and self._scope(intention.operation, intention.arguments).deck_id != self.deck_id:
                    raise ValueError("legacy journal contains another deck; explicit migration required")
                self._intentions[intention.request_id] = intention
            elif kind == "scope":
                if (self.user_id is None or event.get("user_id") != self.user_id
                        or event.get("deck_id") != self.deck_id or saw_scope):
                    raise ValueError("journal account/deck binding mismatch")
                saw_scope = True
            elif kind == "gate":
                self._gate_suspended = bool(event["suspended"])
                self._requires_explicit_reconciliation = bool(event["requires_explicit_reconciliation"])
                self._last_observed_access = event["last_observed_access"]
                self._approved_after_loss = set(event.get("approved_request_ids", ()))
            elif kind == "transition":
                intention = self._intentions[event["request_id"]]
                intention.lifecycle = Lifecycle(event["lifecycle"])
                intention.result = event.get("result")
                intention.reason = event.get("reason")
            elif kind == "reconciliation_ready":
                self._reconciliation[event["request_id"]] = event["context"]
            elif kind == "arguments_materialized":
                self._intentions[event["request_id"]].arguments = event["arguments"]
            else:
                raise ValueError(f"unknown journal event at line {line_number}")
        if self.user_id is not None and not saw_scope:
            # An unbound historical journal is retained intact until a caller
            # performs a proven, lossless migration; never guess ownership.
            raise ValueError("legacy journal lacks account/deck binding; explicit migration required")
        if self.user_id is not None:
            # Permission observations are volatile even though gate closures persist.
            self._last_observed_access = "UNKNOWN"
            self._gate_suspended = True

    def _check_scope(self, deck_id: object) -> None:
        if self.deck_id is not None and deck_id != self.deck_id:
            raise LocalIntentionRejected("outbox belongs to another deck")

    def _recover_uncertain_in_flight(self) -> None:
        for intention in self._intentions.values():
            if intention.lifecycle is Lifecycle.IN_FLIGHT:
                self._transition(intention, Lifecycle.PENDING_SYNC, reason="replayed_after_uncertain_response")

    def _rebuild_resource_index(self) -> None:
        self._decks: set[str] = set()
        self._slides: set[tuple[str, str]] = set()
        self._elements: dict[tuple[str, str], tuple[str, str]] = {}
        self._pending_deleted_decks: set[str] = set()
        self._pending_deleted_slides: set[str] = set()
        deck_id = self._base_projection.get("deck_id")
        if isinstance(deck_id, str):
            self._decks.add(deck_id)
            for slide_id, slide in self._base_projection.get("slides", {}).items():
                self._slides.add((deck_id, slide_id))
                for element in slide.get("elements", ()):
                    element_id = element.get("element_id")
                    if isinstance(element_id, str):
                        self._elements[(deck_id, element_id)] = (deck_id, slide_id)
            for element_id, element in self._base_projection.get("elements", {}).items():
                if isinstance(element_id, str) and isinstance(element, dict) and isinstance(element.get("slide_id"), str):
                    self._elements[(deck_id, element_id)] = (deck_id, element["slide_id"])
        for intention in self._intentions.values():
            if intention.lifecycle in {Lifecycle.REJECTED_FINAL, Lifecycle.SUPERSEDED}:
                continue
            operation = intention.operation
            policy = OPERATION_POLICIES[operation]
            scope = self._scope(operation, intention.arguments)
            if policy.index_effect == "publish":
                self._decks.add(scope.deck_id)
                presentation = intention.arguments["presentation"]
                for slide in presentation.get("slides", ()):
                    published_slide_id = str(slide["metadata"]["slide_id"])
                    self._slides.add((scope.deck_id, published_slide_id))
                    for element in slide.get("elements", ()):
                        published_element_id = element.get("element_id")
                        if isinstance(published_element_id, str):
                            self._elements[(scope.deck_id, published_element_id)] = (
                                scope.deck_id, published_slide_id,
                            )
            elif policy.index_effect == "create_slide" and scope.slide_id is not None:
                self._slides.add((scope.deck_id, scope.slide_id))
            elif policy.index_effect == "create_element" and scope.slide_id is not None and scope.element_id is not None:
                self._elements[(scope.deck_id, scope.element_id)] = (scope.deck_id, scope.slide_id)
            elif policy.index_effect == "delete_slide" and scope.slide_id is not None:
                self._pending_deleted_slides.add(scope.slide_id)

    def _append(self, event: dict[str, Any]) -> None:
        if _contains_auth_fields(event):
            raise LocalIntentionRejected("authentication fields cannot be journaled")
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
        with open(self.journal_path, "a", encoding="utf-8") as handle:
            handle.write(encoded + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _read_request_id() -> str:
        return f"local-read-{uuid4().hex}"


RequestSender = Callable[[str, dict[str, Any], str, str], dict[str, Any] | None]


class JournaledClient:
    """Routes durable intentions and observations through configured replicas.

    The caller supplies the HTTP transport boundary.  That keeps the
    instructor-owned ``client.py`` unchanged while still requiring the exact
    durable request ID to reach the normal ``POST /client`` request path.
    """

    def __init__(
        self,
        logical_client_id: str,
        journal_path: str | Path,
        session_id: str,
        *,
        request_sender: RequestSender,
        local_replica: str,
        leader_replica: str,
        user_id: str | None = None,
        deck_id: str | None = None,
        poll_interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not local_replica or not leader_replica:
            raise ValueError("local_replica and leader_replica must be configured")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        sender_client = getattr(getattr(request_sender, "__self__", None), "client_id", None)
        if sender_client is not None and sender_client != logical_client_id:
            raise ValueError("request sender belongs to a different logical client")
        self.outbox = ClientIntentionOutbox(logical_client_id, journal_path, session_id,
                                           user_id=user_id, deck_id=deck_id)
        self._request_sender = request_sender
        self._account_source = getattr(request_sender, "__self__", None)
        if hasattr(self._account_source, "current_user_id") and (user_id is None or deck_id is None):
            raise ValueError("authenticated journal requires an account/deck binding")
        if user_id is not None and hasattr(self._account_source, "register_deck_gate"):
            self._account_source.register_deck_gate(user_id, deck_id, self.outbox)
        self._local_replica = local_replica
        self._leader_replica = leader_replica
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock
        self._next_poll_at = float("-inf")

    def enqueue(
        self, operation: str, arguments: dict[str, Any], *, request_id: str | None = None,
    ) -> Intention:
        self._check_account()
        return self.outbox.enqueue(operation, arguments, request_id=request_id)

    def synchronize(self) -> list[Intention]:
        self._check_account()
        sent = self.outbox.synchronize(
            lambda intention: self._send(
                intention.operation, intention.arguments, intention.request_id, self._leader_replica,
            )
        )
        if any((self.outbox.get(i.request_id).result or {}).get("status") == "ACCESS_DENIED" for i in sent):
            self.refresh_permission()
        return sent

    def refresh_permission(self) -> bool:
        """Query the account's current role without using content as an auth token."""
        self._check_account()
        if self.outbox.deck_id is None:
            raise LocalIntentionRejected("permission discovery requires a bound outbox")
        epoch = self.outbox.begin_permission_observation()
        response = self._send("query_grants", {"deck_id": self.outbox.deck_id},
                              self.outbox._read_request_id(), self._leader_replica)
        result = self.outbox._successful_result(response)
        if result is None or not self.outbox.observe_permission(result, epoch=epoch):
            self.outbox.suspend_permission()
            return False
        return True

    def check_receipt(self, request_id: str) -> Intention:
        """Look up an uncertain original ID without retrying its mutation."""
        self._check_account()
        self.outbox.get(request_id)
        response = self._send("query_operation_receipt", {"original_request_id": request_id},
                              self.outbox._read_request_id(), self._leader_replica)
        return self.outbox.apply_receipt(request_id, response)

    def denied_recovery_view(self) -> list[Intention]:
        self._check_account()
        return self.outbox.denied_recovery_view()

    def resume_after_reconciliation(self) -> None:
        self._check_account()
        self.outbox.resume_after_reconciliation()

    def approve_reconciled_intention(self, original_request_id: str, operation: str,
                                     arguments: dict[str, Any], *, request_id: str | None = None) -> Intention:
        self._check_account()
        return self.outbox.approve_reconciled_intention(
            original_request_id, operation, arguments, request_id=request_id,
        )

    def _check_account(self) -> None:
        if self.outbox.user_id is None:
            return
        current = getattr(self._account_source, "current_user_id", None)
        if current is not None and current != self.outbox.user_id:
            raise LocalIntentionRejected("outbox belongs to another signed-in account")
        if current is None and self._account_source is not None and hasattr(self._account_source, "current_user_id"):
            raise LocalIntentionRejected("signed-in account is required")

    def poll_if_due(self, deck_id: str, *, watched_element_ids: tuple[str, ...] = ()) -> bool:
        """Run at most one leader-bound observation cycle per configured interval."""
        self._check_account()
        self.outbox._check_scope(deck_id)
        now = self._clock()
        if now < self._next_poll_at:
            return False
        self._next_poll_at = now + self._poll_interval_seconds
        if self.outbox.gate["last_observed_access"] == "NONE":
            self.refresh_permission()
            return False
        refreshed = self.outbox.poll(
            deck_id,
            lambda operation, arguments, request_id: self._send(
                operation, arguments, request_id, self._leader_replica,
            ),
            watched_element_ids=watched_element_ids,
        )
        if not refreshed and self.outbox.gate["last_observed_access"] == "UNKNOWN":
            self.refresh_permission()
        return refreshed

    def prepare_reconciliation(self, request_id: str) -> dict[str, Any]:
        self._check_account()
        return self.outbox.prepare_reconciliation(
            request_id,
            lambda operation, arguments, fresh_request_id: self._send(
                operation, arguments, fresh_request_id, self._leader_replica,
            ),
        )

    def submit_reconciled_update(
        self,
        request_id: str,
        corrected_patch: dict[str, Any],
        *,
        replacement_request_id: str | None = None,
    ) -> Intention:
        self._check_account()
        return self.outbox.submit_reconciled_update(
            request_id, corrected_patch, request_id=replacement_request_id,
        )

    def submit_reconciled_settings(
        self,
        request_id: str,
        corrected_patch: dict[str, Any],
        *,
        replacement_request_id: str | None = None,
    ) -> Intention:
        """Submit a user-corrected settings patch using the conflict's returned revision."""
        self._check_account()
        return self.outbox.submit_reconciled_settings(
            request_id, corrected_patch, request_id=replacement_request_id,
        )

    def discard(self, request_id: str) -> Intention:
        self._check_account()
        return self.outbox.discard(request_id)

    def _send(
        self,
        operation: str,
        arguments: dict[str, Any],
        request_id: str,
        node: str,
    ) -> dict[str, Any] | None:
        return self._request_sender(operation, deepcopy(arguments), request_id, node)
