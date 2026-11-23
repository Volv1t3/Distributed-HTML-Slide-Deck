"""Sequential reference model for the client operations checked by ``check.py``."""

from copy import deepcopy
import json

# Structural commands are committed through Raft and therefore linearizable.
# ``get`` remains the Stage A local eventual read until Stage B deck reads land.
GUARANTEE = {
    "put": "linearizable",
    "get": "eventual",
    "publish_deck": "linearizable",
    "create_slide": "linearizable",
    "delete_slide": "linearizable",
    "reorder_slides": "linearizable",
    "create_element": "linearizable",
    "acquire_lock": "linearizable",
    "release_lock": "linearizable",
    "update_element": "linearizable",
    "delete_element": "linearizable",
    "renew_lock": "linearizable",
    "get_deck_preview": "eventual",
    "get_slide": "eventual",
    "get_element": "eventual",
    "read_lock_status": "eventual",
    "authoritative_refresh_element": "linearizable",
}

READS = {"get", "get_deck_preview", "get_slide", "get_element", "read_lock_status", "authoritative_refresh_element"}
PRESENTATION_MUTATIONS = {
    "publish_deck", "create_slide", "delete_slide", "reorder_slides", "create_element",
    "acquire_lock", "release_lock", "update_element", "delete_element",
    "renew_lock",
}
PRESENTATION_READS = READS - {"get"}
PRESENTATION_OPERATIONS = PRESENTATION_MUTATIONS | PRESENTATION_READS
INITIAL = None


def _result(status, value=None, version=None):
    """Match the public ``ClientResultDTO`` response shape."""

    return {"status": status, "value": value, "version": version, "details": {}}


def _deck_id(op, args):
    if op == "publish_deck":
        return args["presentation"]["metadata"]["deck_id"]
    return args.get("deck_id")


def key(op, args):
    """Group every structural operation by its shared deck."""

    if op in PRESENTATION_OPERATIONS:
        return _deck_id(op, args)
    return args.get("key")


def _normalized_deck(state):
    """Upgrade compact pre-Stage-B model states without changing their meaning."""

    if state is None:
        return None
    deck = deepcopy(state)
    deck.setdefault("deck_id", None)
    deck.setdefault("metadata", {})
    deck.setdefault("slide_data", {
        slide_id: {"metadata": {"slide_id": slide_id}, "elements": []}
        for slide_id in deck.get("slides", [])
    })
    deck.setdefault("deleted", [])
    deck.setdefault("element_tombstones", [])
    deck.setdefault("next_creation_sequence", len(deck.get("elements", {})) + 1)
    for element_id, element in deck.get("elements", {}).items():
        element.setdefault("element_id", element_id)
        element.setdefault("kind", "text_box")
        element.setdefault("configuration", {})
        element.setdefault("creation_sequence", 0)
        slide = deck["slide_data"].setdefault(
            element["slide_id"], {"metadata": {"slide_id": element["slide_id"]}, "elements": []},
        )
        if element_id not in slide["elements"]:
            slide["elements"].append(element_id)
    return deck


def _public_element(element):
    return {
        "element_id": element["element_id"],
        "kind": element["kind"],
        "version": element["version"],
        "configuration": deepcopy(element["configuration"]),
        "creation_sequence": element["creation_sequence"],
    }


def _lock(element):
    if element["lock_owner"] is None:
        return None
    return {
        "session_id": element["lock_owner"],
        "fencing_token": element["fencing_token"],
        "lease_epoch": element.get("lease_epoch", 0),
    }


def _read_presentation(state, op, args):
    deck_id = args["deck_id"]
    if state is None:
        return {"status": "NOT_FOUND"}
    deck = _normalized_deck(state)
    if op == "get_deck_preview":
        return {
            "status": "OK",
            "deck_id": deck_id,
            "deck_version": deck["deck_version"],
            "metadata": deepcopy(deck["metadata"]),
            "slide_ids": list(deck["slides"]),
        }
    if op == "get_slide":
        slide_id = args["slide_id"]
        if slide_id not in deck["slides"]:
            return {
                "status": "SLIDE_DELETED" if slide_id in deck["deleted"] else "NOT_FOUND",
                "deck_id": deck_id,
                "deck_version": deck["deck_version"],
            }
        slide = deepcopy(deck["slide_data"][slide_id])
        slide["elements"] = sorted(
            (_public_element(deck["elements"][element_id]) for element_id in slide["elements"]),
            key=lambda element: (
                element["configuration"].get("z_index", 0),
                element["creation_sequence"],
            ),
        )
        return {
            "status": "OK",
            "deck_id": deck_id,
            "deck_version": deck["deck_version"],
            "slide": slide,
        }
    element_id = args["element_id"]
    element = deck["elements"].get(element_id)
    if element is None:
        observation = {
            "status": "NOT_FOUND",
            "deck_id": deck_id,
            "element_tombstoned": element_id in deck["element_tombstones"],
        }
    else:
        observation = {
            "status": "OK",
            "deck_id": deck_id,
            "slide_id": element["slide_id"],
            "element_version": element["version"],
            "element": _public_element(element),
            "lock": _lock(element),
        }
    if op == "read_lock_status":
        return {
            "status": observation["status"],
            "deck_id": deck_id,
            "element_id": element_id,
            "element_version": observation.get("element_version"),
            "lock": observation.get("lock"),
        }
    return observation


def _apply_operation(state, op, args):
    """Apply one public operation to one sequential object state."""

    if op == "put":
        return args["value"], "ok"
    if op == "get":
        return state, state

    if op in PRESENTATION_READS:
        read_operation = "get_element" if op == "authoritative_refresh_element" else op
        return state, _read_presentation(state, read_operation, args)

    if op == "publish_deck":
        deck_id = _deck_id(op, args)
        if state is not None:
            return state, _result(
                "DECK_ALREADY_PUBLISHED",
                {"deck_id": deck_id, "deck_version": state["deck_version"]},
                state["deck_version"],
            )
        presentation = args["presentation"]
        slide_ids = [slide["metadata"]["slide_id"] for slide in presentation["slides"]]
        elements = {}
        slide_data = {}
        creation_sequence = 1
        for slide in presentation["slides"]:
            slide_id = slide["metadata"]["slide_id"]
            element_ids = []
            for source in slide["elements"]:
                element_id = source["element_id"]
                element_ids.append(element_id)
                elements[element_id] = {
                    "element_id": element_id,
                    "slide_id": slide_id,
                    "kind": source["kind"],
                    "configuration": deepcopy(source["configuration"]),
                    "version": 1,
                    "creation_sequence": creation_sequence,
                    "fencing_token": 0,
                    "lock_owner": None,
                }
                creation_sequence += 1
            slide_data[slide_id] = {
                "metadata": deepcopy(slide["metadata"]),
                "elements": element_ids,
            }
        metadata = deepcopy(presentation["metadata"])
        metadata.pop("owner_session_id", None)
        return {
            "deck_id": deck_id,
            "deck_version": 0,
            "metadata": metadata,
            "slides": slide_ids,
            "slide_data": slide_data,
            "deleted": [],
            "elements": elements,
            "element_tombstones": [],
            "next_creation_sequence": creation_sequence,
        }, _result(
            "PUBLISHED", {"deck_id": deck_id, "deck_version": 0}, 0
        )

    if state is None:
        return state, _result("NOT_FOUND")

    state = _normalized_deck(state)
    deck_id = _deck_id(op, args)
    if op == "create_element":
        slide_id = args["slide_id"]
        element_id = args["element"]["element_id"]
        if (
            slide_id not in state["slides"]
            or element_id in state["elements"]
            or element_id in state["element_tombstones"]
        ):
            return state, _result("NOT_FOUND" if slide_id not in state["slides"] else "INVALID_ARGUMENT")
        elements = dict(state["elements"])
        source = args["element"]
        elements[element_id] = {
            "element_id": element_id,
            "slide_id": slide_id,
            "kind": source.get("kind", "text_box"),
            "configuration": deepcopy(source.get("configuration", {})),
            "version": 1,
            "creation_sequence": state["next_creation_sequence"],
            "fencing_token": 1,
            "lock_owner": args["session_id"],
            "lease_epoch": 0,
        }
        slide_data = deepcopy(state["slide_data"])
        slide_data[slide_id]["elements"].append(element_id)
        new = {
            **state,
            "elements": elements,
            "slide_data": slide_data,
            "next_creation_sequence": state["next_creation_sequence"] + 1,
        }
        return new, _result("CREATED_AND_LOCKED", {
            "deck_id": deck_id, "slide_id": slide_id, "element_id": element_id,
            "element_version": 1, "fencing_token": 1, "lease_epoch": 0,
        }, 1)

    if op in {"acquire_lock", "release_lock", "update_element", "delete_element", "renew_lock"}:
        element_id = args["element_id"]
        element = state["elements"].get(element_id)
        if element is None:
            return state, _result("NOT_FOUND")
        if op == "acquire_lock":
            if element["lock_owner"] is not None:
                if element["lock_owner"] == args["session_id"]:
                    return state, _result("ALREADY_HELD", {
                        "deck_id": deck_id, "element_id": element_id,
                        "fencing_token": element["fencing_token"],
                        "lease_epoch": element.get("lease_epoch", 0),
                    }, element["version"])
                return state, _result("LOCKED", {
                    "owner_session": element["lock_owner"],
                    "fencing_token": element["fencing_token"],
                }, element["version"])
            replacement = {**element, "fencing_token": element["fencing_token"] + 1,
                           "lock_owner": args["session_id"], "lease_epoch": 0}
            elements = {**state["elements"], element_id: replacement}
            return {**state, "elements": elements}, _result("LOCK_ACQUIRED", {
                "deck_id": deck_id, "element_id": element_id,
                "fencing_token": replacement["fencing_token"],
                "element_version": replacement["version"],
                "lease_epoch": 0,
            }, replacement["version"])
        if op == "renew_lock":
            if element["lock_owner"] is None or element["fencing_token"] != args["fencing_token"]:
                return state, _result("STALE_TOKEN")
            if element["lock_owner"] != args["session_id"]:
                return state, _result("NOT_OWNER")
            new_epoch = element.get("lease_epoch", 0) + 1
            replacement = {**element, "lease_epoch": new_epoch}
            return {**state, "elements": {**state["elements"], element_id: replacement}}, _result(
                "RENEWED", {
                    "deck_id": deck_id, "element_id": element_id,
                    "fencing_token": args["fencing_token"],
                    "lease_epoch": new_epoch,
                }, element["version"]
            )
        if element["fencing_token"] != args["fencing_token"]:
            return state, _result("STALE_TOKEN")
        if element["lock_owner"] != args["session_id"]:
            return state, _result("NOT_OWNER")
        if op == "release_lock":
            elements = {**state["elements"], element_id: {**element, "lock_owner": None}}
            return {**state, "elements": elements}, _result("RELEASED", {
                "deck_id": deck_id, "element_id": element_id,
                "fencing_token": args["fencing_token"],
            }, element["version"])
        if element["version"] != args["expected_version"]:
            return state, _result("VERSION_CONFLICT", {"current_version": element["version"]}, element["version"])
        if op == "update_element":
            updated = {
                **element,
                "configuration": {**element["configuration"], **deepcopy(args["patch"])},
                "version": element["version"] + 1,
            }
            return {**state, "elements": {**state["elements"], element_id: updated}}, _result(
                "SAVED",
                {"deck_id": deck_id, "element_id": element_id, "element_version": updated["version"]},
                updated["version"],
            )
        elements = dict(state["elements"])
        del elements[element_id]
        slide_data = deepcopy(state["slide_data"])
        slide_data[element["slide_id"]]["elements"].remove(element_id)
        deleted_version = element["version"] + 1
        return {
            **state,
            "elements": elements,
            "slide_data": slide_data,
            "element_tombstones": state["element_tombstones"] + [element_id],
        }, _result("DELETED", {
            "deck_id": deck_id, "element_id": element_id, "element_version": deleted_version,
        }, deleted_version)

    if args["expected_deck_version"] != state["deck_version"]:
        return state, _result(
            "STRUCTURE_VERSION_CONFLICT",
            {"deck_version": state["deck_version"]},
            state["deck_version"],
        )

    if op == "create_slide":
        slide_id = args["slide"]["metadata"]["slide_id"]
        if slide_id in state["slides"]:
            return state, _result("INVALID_ARGUMENT")
        if slide_id in state["deleted"]:
            return state, _result("SLIDE_DELETED")
        predecessor = args.get("insert_after_slide_id")
        if predecessor is None:
            insert_at = len(state["slides"])
        elif predecessor in state["deleted"]:
            return state, _result("SLIDE_DELETED")
        elif predecessor not in state["slides"]:
            return state, _result("NOT_FOUND")
        else:
            insert_at = state["slides"].index(predecessor) + 1
        slides = state["slides"][:]
        slides.insert(insert_at, slide_id)
        slide_data = {
            **state["slide_data"],
            slide_id: {
                "metadata": deepcopy(args["slide"]["metadata"]),
                "elements": [],
            },
        }
        version = state["deck_version"] + 1
        new = {**state, "deck_version": version, "slides": slides, "slide_data": slide_data}
        return new, _result(
            "CREATED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version}, version
        )

    if op == "reorder_slides":
        requested = args["slide_ids"]
        if len(requested) != len(set(requested)) or set(requested) != set(state["slides"]):
            return state, _result("INVALID_ARGUMENT")
        version = state["deck_version"] + 1
        new = {**state, "deck_version": version, "slides": requested}
        return new, _result(
            "REORDERED", {"deck_id": deck_id, "deck_version": version, "slide_ids": requested}, version
        )

    if op == "delete_slide":
        slide_id = args["slide_id"]
        if slide_id not in state["slides"]:
            return state, _result("SLIDE_DELETED" if slide_id in state["deleted"] else "NOT_FOUND")
        version = state["deck_version"] + 1
        descendant_ids = list(state["slide_data"][slide_id]["elements"])
        elements = {key: value for key, value in state["elements"].items() if key not in descendant_ids}
        slide_data = dict(state["slide_data"])
        del slide_data[slide_id]
        new = {
            **state,
            "deck_version": version,
            "slides": [active for active in state["slides"] if active != slide_id],
            "deleted": state["deleted"] + [slide_id],
            "elements": elements,
            "slide_data": slide_data,
            "element_tombstones": state["element_tombstones"] + descendant_ids,
        }
        return new, _result(
            "DELETED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version}, version
        )

    raise ValueError(f"operacion desconocida: {op}")


def apply(state, op, args):
    """Apply one public history operation to the independent model."""

    return _apply_operation(state, op, args)


def converged_ok(applied, observed):
    """Validate final presentation convergence while ignoring only progress metadata."""

    if not isinstance(observed, dict):
        return False
    applied_index = observed.get("applied_index")
    if type(applied_index) is not int or applied_index < 0:
        return False
    deck_id = observed.get("deck_id")
    if not isinstance(deck_id, str):
        return False
    if isinstance(observed.get("slide"), dict):
        slide_id = observed["slide"].get("metadata", {}).get("slide_id")
        read_operation = "get_slide"
        read_arguments = {"deck_id": deck_id, "slide_id": slide_id}
    elif "element" in observed or "element_tombstoned" in observed:
        element = observed.get("element") or {}
        element_id = element.get("element_id")
        read_operation = "get_element"
        read_arguments = {"deck_id": deck_id, "element_id": element_id}
    elif "slide_ids" in observed:
        read_operation = "get_deck_preview"
        read_arguments = {"deck_id": deck_id}
    else:
        element_id = observed.get("element_id")
        read_operation = "read_lock_status"
        read_arguments = {"deck_id": deck_id, "element_id": element_id}
    normalized = deepcopy(observed)
    normalized.pop("applied_index", None)

    operations = list(applied)
    ends = [
        operation.get("complete") if operation.get("ok") else float("inf")
        for operation in operations
    ]
    seen = set()

    def search(done, state):
        if len(done) == len(operations):
            _, expected = apply(state, read_operation, read_arguments)
            return normalized == expected
        state_key = json.dumps(state, sort_keys=True, separators=(",", ":"))
        key_value = (done, state_key)
        if key_value in seen:
            return False
        seen.add(key_value)
        horizon = min(ends[index] for index in range(len(operations)) if index not in done)
        for index, operation in enumerate(operations):
            if index in done or operation.get("invoke", 0) > horizon:
                continue
            new_state, result = apply(state, operation["op"], operation["args"])
            if operation.get("ok") and result != operation.get("result"):
                continue
            if search(done | {index}, new_state):
                return True
        return False

    return search(frozenset(), INITIAL)
