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

# Confidence values that block protected mutations (Scoped Authority Gate).
_BLOCKED_CONFIDENCE_VALUES = frozenset({"UNVERIFIED", "LOST"})


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

    def __init__(self, logical_client_id: str, journal_path: str | Path, session_id: str) -> None:
        if not logical_client_id:
            raise ValueError("logical_client_id must not be empty")
        self.logical_client_id = logical_client_id
        self.session_id = session_id
        self.journal_path = Path(journal_path)
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        self._intentions: OrderedDict[str, Intention] = OrderedDict()
        self._reconciliation: dict[str, dict[str, Any]] = {}
        self._base_projection: dict[str, Any] = {}
        self._highest_applied_index = -1
        # Scoped Authority Gate: element_id → confidence string ("UNVERIFIED", "LOST", …)
        self._lock_confidence: dict[str, str] = {}
        self._replay()
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
        request_id = request_id or uuid4().hex
        if request_id in self._intentions:
            raise LocalIntentionRejected(f"request id {request_id!r} already exists in this journal")
        copied = deepcopy(arguments)
        if operation in ELEMENT_OPERATIONS:
            copied.setdefault("session_id", self.session_id)
        scope = self._scope(operation, copied)
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
        """Poll local eventual reads without regressing the authoritative base."""
        response = sender("get_deck_preview", {"deck_id": deck_id}, self._read_request_id())
        preview = self._successful_result(response)
        if preview is None or not isinstance(preview.get("applied_index"), int):
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
            if element is not None and element.get("status") == "OK":
                projection["elements"][element_id] = deepcopy(element)
            lock_response = sender(
                "read_lock_status", {"deck_id": deck_id, "element_id": element_id}, self._read_request_id(),
            )
            lock = self._successful_result(lock_response)
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
        for dep_id in intention.dependencies:
            dep = self._intentions.get(dep_id)
            if dep is None or dep.operation != intention.operation:
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
            else:
                confirmed = value.get("slide_settings_version")
                if isinstance(confirmed, int) and intention.arguments.get("expected_slide_settings_version") != confirmed:
                    intention.arguments["expected_slide_settings_version"] = confirmed
                    changed = True
        if changed:
            self._append({
                "kind": "arguments_materialized",
                "request_id": intention.request_id,
                "arguments": intention.arguments,
            })

    def _record_response(self, intention: Intention, response: dict[str, Any] | None) -> None:
        result = self._successful_result(response)
        if result is None or result.get("status") == "QUORUM_UNAVAILABLE":
            self._transition(intention, Lifecycle.PENDING_SYNC, reason="uncertain_transport_or_quorum")
            return
        status = result.get("status")
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
                self._intentions[intention.request_id] = intention
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
        poll_interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not local_replica or not leader_replica:
            raise ValueError("local_replica and leader_replica must be configured")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        self.outbox = ClientIntentionOutbox(logical_client_id, journal_path, session_id)
        self._request_sender = request_sender
        self._local_replica = local_replica
        self._leader_replica = leader_replica
        self._poll_interval_seconds = poll_interval_seconds
        self._clock = clock
        self._next_poll_at = float("-inf")

    def enqueue(
        self, operation: str, arguments: dict[str, Any], *, request_id: str | None = None,
    ) -> Intention:
        return self.outbox.enqueue(operation, arguments, request_id=request_id)

    def synchronize(self) -> list[Intention]:
        return self.outbox.synchronize(
            lambda intention: self._send(
                intention.operation, intention.arguments, intention.request_id, self._leader_replica,
            )
        )

    def poll_if_due(self, deck_id: str, *, watched_element_ids: tuple[str, ...] = ()) -> bool:
        """Run at most one local observation cycle per configured interval."""
        now = self._clock()
        if now < self._next_poll_at:
            return False
        self._next_poll_at = now + self._poll_interval_seconds
        return self.outbox.poll(
            deck_id,
            lambda operation, arguments, request_id: self._send(
                operation, arguments, request_id, self._local_replica,
            ),
            watched_element_ids=watched_element_ids,
        )

    def prepare_reconciliation(self, request_id: str) -> dict[str, Any]:
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
        return self.outbox.submit_reconciled_settings(
            request_id, corrected_patch, request_id=replacement_request_id,
        )

    def discard(self, request_id: str) -> Intention:
        return self.outbox.discard(request_id)

    def _send(
        self,
        operation: str,
        arguments: dict[str, Any],
        request_id: str,
        node: str,
    ) -> dict[str, Any] | None:
        return self._request_sender(operation, deepcopy(arguments), request_id, node)
