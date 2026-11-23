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
from typing import Any, Callable
from uuid import uuid4


class Lifecycle(str, Enum):
    """The durable lifecycle of one client intention."""

    PENDING_SYNC = "PENDING_SYNC"
    IN_FLIGHT = "IN_FLIGHT"
    COMMITTED = "COMMITTED"
    REJECTED_FINAL = "REJECTED_FINAL"
    SUPERSEDED = "SUPERSEDED"
    NEEDS_MANUAL_RECONCILIATION = "NEEDS_MANUAL_RECONCILIATION"


TERMINAL = {
    Lifecycle.COMMITTED,
    Lifecycle.REJECTED_FINAL,
    Lifecycle.SUPERSEDED,
    Lifecycle.NEEDS_MANUAL_RECONCILIATION,
}

SUCCESS_STATUSES = {
    "PUBLISHED", "CREATED", "CREATED_AND_LOCKED", "REORDERED", "LOCK_ACQUIRED",
    "ALREADY_HELD", "SAVED", "RELEASED", "DELETED",
}

ELEMENT_OPERATIONS = {"create_element", "acquire_lock", "release_lock", "update_element", "delete_element"}


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
        deck_id, slide_id, element_id = self._scope(operation, copied)
        self._validate_local_scope(operation, deck_id, slide_id, element_id)
        dependencies = self._dependencies(operation, deck_id, slide_id, element_id)
        intention = Intention(request_id, self.logical_client_id, operation, copied, dependencies=dependencies)
        self._intentions[request_id] = intention
        self._append({"kind": "intent", "intention": intention.to_json()})
        if operation == "delete_slide":
            self._supersede_unsent_descendants(deck_id, slide_id, request_id)
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
            "slide_ids": list(preview.get("slide_ids", ())),
            "metadata": deepcopy(preview.get("metadata")),
            "slides": deepcopy(self._base_projection.get("slides", {})),
            "elements": deepcopy(self._base_projection.get("elements", {})),
        }
        # A structural version change is the protocol's signal to refresh slides;
        # otherwise only newly discovered slides need fetching.
        if preview.get("deck_version") != self._base_projection.get("deck_version"):
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
        for element_id in watched_element_ids:
            element_response = sender("get_element", {"deck_id": deck_id, "element_id": element_id}, self._read_request_id())
            element = self._successful_result(element_response)
            if element is not None:
                projection["elements"][element_id] = deepcopy(element)
        self.record_base_projection(projection)
        return True

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
        self.synchronize(lambda intention: sender(intention.operation, intention.arguments, intention.request_id))
        lock_result = self._intentions[lock.request_id]
        if lock_result.lifecycle is not Lifecycle.COMMITTED:
            raise LocalIntentionRejected("fresh lock acquisition was not committed")
        lock_value = (lock_result.result or {}).get("value", {})
        fencing_token = lock_value.get("fencing_token")
        if not isinstance(fencing_token, int):
            raise LocalIntentionRejected("fresh lock result did not provide a fencing token")
        refresh = sender(
            "authoritative_refresh_element",
            {
                "deck_id": original.arguments["deck_id"],
                "element_id": original.arguments["element_id"],
                "min_commit_index": min_commit_index,
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

    def _scope(self, operation: str, arguments: dict[str, Any]) -> tuple[str, str | None, str | None]:
        if operation == "publish_deck":
            try:
                return str(arguments["presentation"]["metadata"]["deck_id"]), None, None
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("publication requires presentation metadata.deck_id") from error
        deck_id = arguments.get("deck_id")
        if not isinstance(deck_id, str) or not deck_id:
            raise LocalIntentionRejected(f"{operation} requires deck_id")
        if operation == "create_slide":
            try:
                return deck_id, str(arguments["slide"]["metadata"]["slide_id"]), None
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("slide creation requires slide metadata.slide_id") from error
        if operation == "delete_slide":
            return deck_id, self._required_string(arguments, "slide_id"), None
        if operation == "create_element":
            try:
                return deck_id, self._required_string(arguments, "slide_id"), str(arguments["element"]["element_id"])
            except (KeyError, TypeError) as error:
                raise LocalIntentionRejected("element creation requires slide_id and element.element_id") from error
        if operation in ELEMENT_OPERATIONS:
            return deck_id, None, self._required_string(arguments, "element_id")
        if operation == "reorder_slides":
            return deck_id, None, None
        raise LocalIntentionRejected(f"unsupported shared operation {operation!r}")

    @staticmethod
    def _required_string(arguments: dict[str, Any], key: str) -> str:
        value = arguments.get(key)
        if not isinstance(value, str) or not value:
            raise LocalIntentionRejected(f"requires non-empty {key}")
        return value

    def _validate_local_scope(self, operation: str, deck_id: str, slide_id: str | None, element_id: str | None) -> None:
        if operation != "publish_deck" and deck_id not in self._decks:
            raise LocalIntentionRejected("target deck is missing from the projected hierarchy")
        if deck_id in self._pending_deleted_decks:
            raise LocalIntentionRejected("target deck is pending deletion")
        if slide_id is not None:
            if slide_id in self._pending_deleted_slides:
                raise LocalIntentionRejected("target slide is pending deletion")
            if operation != "create_slide" and (deck_id, slide_id) not in self._slides:
                raise LocalIntentionRejected("target slide is missing from the projected hierarchy")
        if element_id is not None and operation != "create_element":
            resource = self._elements.get((deck_id, element_id))
            if resource is None:
                raise LocalIntentionRejected("target element is missing from the projected hierarchy")
            if resource[1] in self._pending_deleted_slides:
                raise LocalIntentionRejected("target element belongs to a pending-deletion slide")

    def _dependencies(self, operation: str, deck_id: str, slide_id: str | None, element_id: str | None) -> tuple[str, ...]:
        dependencies: list[str] = []
        for intention in self._intentions.values():
            other_deck, other_slide, other_element = self._scope(intention.operation, intention.arguments)
            if other_deck != deck_id:
                continue
            if intention.operation == "publish_deck" and intention.lifecycle is not Lifecycle.COMMITTED:
                dependencies.append(intention.request_id)
            if slide_id is not None and intention.operation == "create_slide" and other_slide == slide_id and intention.lifecycle is not Lifecycle.COMMITTED:
                dependencies.append(intention.request_id)
            if element_id is not None and intention.operation == "create_element" and other_element == element_id and intention.lifecycle is not Lifecycle.COMMITTED:
                dependencies.append(intention.request_id)
        return tuple(dict.fromkeys(dependencies))

    def _supersede_unsent_descendants(self, deck_id: str, slide_id: str | None, delete_request_id: str) -> None:
        for intention in list(self._intentions.values()):
            if intention.request_id == delete_request_id or intention.lifecycle is not Lifecycle.PENDING_SYNC:
                continue
            other_deck, other_slide, other_element = self._scope(intention.operation, intention.arguments)
            if other_deck != deck_id:
                continue
            if other_slide == slide_id or (other_element is not None and self._elements.get((deck_id, other_element), (None, None))[1] == slide_id):
                self._transition(intention, Lifecycle.SUPERSEDED, reason=f"superseded_by:{delete_request_id}")

    def _eligible(self, intention: Intention) -> bool:
        return (
            intention.lifecycle is Lifecycle.PENDING_SYNC
            and all(self._intentions[dependency].lifecycle is Lifecycle.COMMITTED for dependency in intention.dependencies)
        )

    def _materialize_creation_preconditions(self, intention: Intention) -> bool:
        """Fill an offline child's lock/version only from a committed creator.

        A pending element can have later local draft edits, but it has no
        client-invented fencing token.  The materialized values are journaled
        before the dependent command's first send so a restart retries the
        same authoritative preconditions.
        """
        if intention.operation not in {"update_element", "delete_element", "release_lock"}:
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

    def _record_response(self, intention: Intention, response: dict[str, Any] | None) -> None:
        result = self._successful_result(response)
        if result is None or result.get("status") == "QUORUM_UNAVAILABLE":
            self._transition(intention, Lifecycle.PENDING_SYNC, reason="uncertain_transport_or_quorum")
            return
        status = result.get("status")
        if status == "VERSION_CONFLICT":
            self._transition(intention, Lifecycle.NEEDS_MANUAL_RECONCILIATION, result=result)
        elif status in SUCCESS_STATUSES:
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
        reason: str | None = None,
    ) -> None:
        if intention.lifecycle in TERMINAL:
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
            current_deck, slide_id, element_id = self._scope(operation, intention.arguments)
            if operation == "publish_deck":
                self._decks.add(current_deck)
                presentation = intention.arguments["presentation"]
                for slide in presentation.get("slides", ()):
                    published_slide_id = str(slide["metadata"]["slide_id"])
                    self._slides.add((current_deck, published_slide_id))
                    for element in slide.get("elements", ()):
                        published_element_id = element.get("element_id")
                        if isinstance(published_element_id, str):
                            self._elements[(current_deck, published_element_id)] = (current_deck, published_slide_id)
            elif operation == "create_slide" and slide_id is not None:
                self._slides.add((current_deck, slide_id))
            elif operation == "create_element" and slide_id is not None and element_id is not None:
                self._elements[(current_deck, element_id)] = (current_deck, slide_id)
            elif operation == "delete_slide" and slide_id is not None:
                self._pending_deleted_slides.add(slide_id)

    def _append(self, event: dict[str, Any]) -> None:
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"))
        with open(self.journal_path, "a", encoding="utf-8") as handle:
            handle.write(encoded + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _read_request_id() -> str:
        return f"local-read-{uuid4().hex}"
