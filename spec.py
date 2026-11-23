"""Sequential reference model for the client operations checked by ``check.py``."""

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
}

READS = {"get"}
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

    if op in {"publish_deck", "create_slide", "delete_slide", "reorder_slides", "create_element"}:
        return _deck_id(op, args)
    return args.get("key")


def apply(state, op, args):
    """Apply one public operation to one sequential object state."""

    if op == "put":
        return args["value"], "ok"
    if op == "get":
        return state, state

    if op == "publish_deck":
        deck_id = _deck_id(op, args)
        if state is not None:
            return state, _result(
                "DECK_ALREADY_PUBLISHED",
                {"deck_id": deck_id, "deck_version": state["deck_version"]},
                state["deck_version"],
            )
        slide_ids = [slide["metadata"]["slide_id"] for slide in args["presentation"]["slides"]]
        return {"deck_version": 0, "slides": slide_ids, "deleted": [], "elements": {}}, _result(
            "PUBLISHED", {"deck_id": deck_id, "deck_version": 0}, 0
        )

    if state is None:
        return state, _result("NOT_FOUND")

    deck_id = _deck_id(op, args)
    if op == "create_element":
        slide_id = args["slide_id"]
        element_id = args["element"]["element_id"]
        if slide_id not in state["slides"] or element_id in state["elements"]:
            return state, _result("NOT_FOUND" if slide_id not in state["slides"] else "INVALID_ARGUMENT")
        elements = dict(state["elements"])
        elements[element_id] = {"slide_id": slide_id, "version": 1, "fencing_token": 1}
        new = {**state, "elements": elements}
        return new, _result("CREATED_AND_LOCKED", {
            "deck_id": deck_id, "slide_id": slide_id, "element_id": element_id,
            "element_version": 1, "fencing_token": 1,
        }, 1)

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
        version = state["deck_version"] + 1
        new = {"deck_version": version, "slides": slides, "deleted": state["deleted"], "elements": state["elements"]}
        return new, _result(
            "CREATED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version}, version
        )

    if op == "reorder_slides":
        requested = args["slide_ids"]
        if len(requested) != len(set(requested)) or set(requested) != set(state["slides"]):
            return state, _result("INVALID_ARGUMENT")
        version = state["deck_version"] + 1
        new = {"deck_version": version, "slides": requested, "deleted": state["deleted"], "elements": state["elements"]}
        return new, _result(
            "REORDERED", {"deck_id": deck_id, "deck_version": version, "slide_ids": requested}, version
        )

    if op == "delete_slide":
        slide_id = args["slide_id"]
        if slide_id not in state["slides"]:
            return state, _result("SLIDE_DELETED" if slide_id in state["deleted"] else "NOT_FOUND")
        version = state["deck_version"] + 1
        new = {
            "deck_version": version,
            "slides": [active for active in state["slides"] if active != slide_id],
            "deleted": state["deleted"] + [slide_id],
            "elements": state["elements"],
        }
        return new, _result(
            "DELETED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version}, version
        )

    raise ValueError(f"operacion desconocida: {op}")
