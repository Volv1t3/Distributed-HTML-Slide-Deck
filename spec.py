"""Sequential reference model for the client operations checked by ``check.py``."""

from copy import deepcopy
import json

from auth_catalog import load_account_catalog
from auth_dto import issue_access_token_command_fingerprint
from command_dto import (
    AccessChangeStatus, DeckRole, EDITING_ROLES, ELEMENT_OBSERVATION_READ_OPERATIONS,
)
from dto_base import sha256_json

# Structural commands are committed through Raft and therefore linearizable.
# ``get`` remains the Stage A local eventual read. Stage F (ADR-015/017) serves
# every shared-deck read, plus self-permission discovery, only from the current
# leader after a fresh bearer-checked ReadIndex, so those reads are linearizable.
GUARANTEE = {
    "put": "linearizable",
    "issue_access_token": "linearizable",
    "revoke_access_token": "linearizable",
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
    "expire_lock": "linearizable",
    "update_deck_settings": "linearizable",
    "update_slide_settings": "linearizable",
    "grant_access": "linearizable",
    "revoke_access": "linearizable",
    "get_deck_preview": "linearizable",
    "get_slide": "linearizable",
    "get_element": "linearizable",
    "read_lock_status": "linearizable",
    "authoritative_refresh_element": "linearizable",
    "refresh_element": "linearizable",
    "query_grants": "linearizable",
    "query_operation_receipt": "linearizable",
    "query_access_state": "linearizable",
    "resolve_registered_user": "linearizable",
    "migrate_legacy_deck": "linearizable",
}

READS = {
    "get", "get_deck_preview", "get_slide", "get_element", "read_lock_status",
    "authoritative_refresh_element", "refresh_element", "query_grants", "query_operation_receipt",
    "query_access_state", "resolve_registered_user",
}
PRESENTATION_MUTATIONS = {
    "publish_deck", "create_slide", "delete_slide", "reorder_slides", "create_element",
    "acquire_lock", "release_lock", "update_element", "delete_element",
    "renew_lock", "expire_lock", "update_deck_settings", "update_slide_settings",
    "grant_access", "revoke_access", "migrate_legacy_deck",
}
# History entries for Stage F protected operations carry the bearer-derived
# account as ``actor_user_id``; the server never trusts it from the wire.
ACTOR_FIELD = "actor_user_id"
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


AUTH_CREDENTIALS_KEY = "_auth_credentials"


def key(op, args):
    """Group operations by the sequential object whose state they mutate."""

    if op in {"issue_access_token", "revoke_access_token"}:
        return AUTH_CREDENTIALS_KEY

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


def _role_of(owner_user_id, grants, actor_user_id):
    """Resolve one account's role on an owned deck from its owner and grant map."""

    if owner_user_id == actor_user_id:
        return DeckRole.OWNER
    return DeckRole(grants.get(actor_user_id, DeckRole.NONE))


def _effective_access(state, actor_user_id):
    """OWNER/EDIT/VIEW/NONE; unknown and ownerless legacy decks are both NONE."""

    if state is None or "access" not in state:
        return DeckRole.NONE
    return _role_of(state["metadata"].get("owner_user_id"), state["access"]["grants"], actor_user_id)


def _access_change(*, owner_user_id, access_version, grants, operation, payload, registered):
    """Validate one owner-authorized grant change in ADR-013 precedence order.

    Returns ``(status, new_grants)``; ``new_grants`` is None when the change is
    rejected or has no effect, so the access revision must not advance.
    """

    target = str(payload["target_user_id"])
    if payload["expected_access_version"] != access_version:
        return AccessChangeStatus.ACCESS_VERSION_CONFLICT, None
    if target == owner_user_id:
        return AccessChangeStatus.INVALID_ACCESS_TARGET, None
    if target not in registered:
        return AccessChangeStatus.INVALID_TARGET_USER, None
    changed = dict(grants)
    if operation == "revoke_access":
        if target not in changed:
            return AccessChangeStatus.ACCESS_NOT_FOUND, None
        del changed[target]
        return AccessChangeStatus.ACCESS_REVOKED, changed
    previous = changed.get(target)
    if previous == payload["access_level"]:
        return AccessChangeStatus.ACCESS_UNCHANGED, None
    changed[target] = payload["access_level"]
    if previous is None:
        return AccessChangeStatus.ACCESS_GRANTED, changed
    return AccessChangeStatus.ACCESS_UPDATED, changed


def _registered_user_ids():
    return {str(user.profile.user_id) for user in load_account_catalog().users}


def _query_grants(state, args):
    role = _effective_access(state, args.get(ACTOR_FIELD))
    if role is DeckRole.NONE:
        return {"status": "OK", "effective_access": DeckRole.NONE}
    return {
        "status": "OK", "effective_access": role, "deck_id": args["deck_id"],
        "access_version": state["access"]["access_version"],
    }


def _query_access_state(state, args):
    """OWNER-only roster; non-owners and unknown decks share one denial."""

    if _effective_access(state, args.get(ACTOR_FIELD)) is not DeckRole.OWNER:
        return {"status": "ACCESS_DENIED"}
    grants = state["access"]["grants"]
    return {
        "status": "OK", "deck_id": args["deck_id"],
        "access_version": state["access"]["access_version"],
        "grants": [{"target_user_id": user_id, "access_level": grants[user_id]}
                   for user_id in sorted(grants)],
    }


def _resolve_registered_user(state, args):
    """OWNER-only exact normalized-email lookup over the frozen catalog."""

    if _effective_access(state, args.get(ACTOR_FIELD)) is not DeckRole.OWNER:
        return {"status": "ACCESS_DENIED"}
    email = args["email"].strip().lower()
    for user in load_account_catalog().users:
        if user.profile.email == email:
            return {"status": "OK", "user": {
                "user_id": str(user.profile.user_id), "first_name": user.profile.first_name,
                "last_name": user.profile.last_name, "email": user.profile.email,
            }}
    return {"status": "USER_NOT_FOUND"}


def _read_presentation(state, op, args):
    deck_id = args["deck_id"]
    role = None
    if ACTOR_FIELD in args or (state is not None and "access" in state):
        # Stage F: authorize before projecting; NONE and unknown are identical.
        role = _effective_access(state, args.get(ACTOR_FIELD))
        if role is DeckRole.NONE:
            return {"status": "ACCESS_DENIED"}
    if state is None:
        return {"status": "NOT_FOUND"}
    deck = _normalized_deck(state)
    if op == "get_deck_preview":
        preview = {
            "status": "OK",
            "deck_id": deck_id,
            "deck_version": deck["deck_version"],
            "deck_settings_version": deck.get("deck_settings_version", 0),
            "metadata": deepcopy(deck["metadata"]),
            "slide_ids": list(deck["slides"]),
        }
        if role is not None:
            preview["effective_access"] = role
            preview["access_version"] = deck["access"]["access_version"]
        return preview
    if op == "get_slide":
        slide_id = args["slide_id"]
        if slide_id not in deck["slides"]:
            return {
                "status": "SLIDE_DELETED" if slide_id in deck["deleted"] else "NOT_FOUND",
                "deck_id": deck_id,
                "deck_version": deck["deck_version"],
            }
        ordinal = deck["slides"].index(slide_id)
        slide = deepcopy(deck["slide_data"][slide_id])
        slide["metadata"]["ordinal"] = ordinal
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


def _apply_auth_issuance(state, operation_args):
    """Model the trusted internal issue_access_token Raft transition.

    credential_id and token_digest here are server-generated command material
    produced after password admission; they are not raw login fields supplied
    by the external client. request_id is reference-model harness metadata used
    to model the existing Raft deduplication namespace.
    """
    if state is None:
        state = {
            "users": sorted(str(u.profile.user_id) for u in load_account_catalog().users),
            "client_bindings": {},
            "active_credentials": {},
            "retired_credentials": {},
            "legacy_quarantined_credentials": {},
            "schema_version": 2,
            "request_results": {},
        }
    current = deepcopy(state)
    command_payload = {
        field: operation_args[field]
        for field in ("user_id", "client_id", "credential_id", "token_digest")
    }
    user_id = str(command_payload["user_id"])
    client_id = str(command_payload["client_id"])
    credential_id = str(command_payload["credential_id"])
    digest = command_payload["token_digest"]
    request_id = operation_args.get("request_id")
    request_key = sha256_json([client_id, request_id]) if request_id is not None else None
    fingerprint = issue_access_token_command_fingerprint(
        client_id=client_id,
        payload=command_payload,
    )
    if request_key is not None and request_key in current["request_results"]:
        old = current["request_results"][request_key]
        if old["fingerprint"] == fingerprint:
            return current, deepcopy(old["result"])
        return current, _result("REQUEST_ID_REUSE")

    if user_id not in current["users"]:
        result = _result("USER_NOT_FOUND")
    elif current["client_bindings"].get(client_id) not in (None, user_id):
        result = _result("CLIENT_ALREADY_BOUND")
    else:
        known = list(current["active_credentials"].values())
        if current.get("schema_version", 1) == 2:
            known.extend(current["retired_credentials"].values())
            known.extend(current.get("legacy_quarantined_credentials", {}).values())
        if any(item["credential_id"] == credential_id for item in known):
            result = _result("CREDENTIAL_ID_REUSE")
        elif any(item["token_digest"] == digest for item in known):
            result = _result("TOKEN_DIGEST_REUSE")
        else:
            previous = current["active_credentials"].get(client_id)
            quarantined = current.get("legacy_quarantined_credentials", {}).get(client_id)
            predecessor = previous if previous is not None else quarantined
            generation = predecessor["credential_generation"] + 1 if predecessor else 1
            if current.get("schema_version", 1) == 2:
                current.setdefault("legacy_quarantined_credentials", {})
            if previous is not None and current.get("schema_version", 1) == 2:
                current["retired_credentials"][previous["credential_id"]] = {
                    "user_id": previous["user_id"],
                    "client_id": previous["client_id"],
                    "credential_id": previous["credential_id"],
                    "token_digest": previous["token_digest"],
                    "credential_generation": previous["credential_generation"],
                    "retirement_reason": "REVOKED" if previous["revoked"] else "ROTATED",
                }
            current["client_bindings"][client_id] = user_id
            current["active_credentials"][client_id] = {
                "user_id": user_id, "client_id": client_id,
                "credential_id": credential_id, "token_digest": digest,
                "credential_generation": generation, "revoked": False,
            }
            result = _result("TOKEN_ISSUED", {
                "user_id": user_id, "client_id": client_id,
                "credential_id": credential_id, "credential_generation": generation,
            })
    if request_key is not None:
        current["request_results"][request_key] = {"fingerprint": fingerprint, "result": deepcopy(result)}
    return current, result


def _apply_operation(state, op, args):
    """Apply one public operation to one sequential object state."""

    if op == "issue_access_token":
        return _apply_auth_issuance(state, args)
    if op == "revoke_access_token":
        current = deepcopy(state)
        client_id = str(args["client_id"])
        request_id = args.get("request_id")
        request_key = sha256_json([client_id, request_id]) if request_id is not None else None
        # Match command_fingerprint: the stamped credential reference and its
        # generation are transient proof material, excluded so an exact revoke
        # retry after reauthentication replays TOKEN_REVOKED instead of
        # REQUEST_ID_REUSE. The expected fence stays part of the identity.
        fingerprint = sha256_json({
            k: v for k, v in args.items()
            if k not in {"request_id", "credential_id", "credential_generation"}
        })
        if request_key is not None and request_key in current["request_results"]:
            previous = current["request_results"][request_key]
            return current, deepcopy(previous["result"]) if previous["fingerprint"] == fingerprint else _result("REQUEST_ID_REUSE")
        record = current["active_credentials"].get(client_id)
        if (record is None or record["revoked"]
                or record["user_id"] != str(args["authenticated_actor_user_id"])
                or record["credential_id"] != str(args["expected_credential_id"])
                or record["credential_generation"] != args["expected_generation"]
                or record["credential_id"] != str(args["credential_id"])
                or record["credential_generation"] != args["credential_generation"]):
            result = _result("STALE_CREDENTIAL")
        else:
            record["revoked"] = True
            record["credential_generation"] += 1
            result = _result("TOKEN_REVOKED", {
                "client_id": client_id, "credential_id": record["credential_id"],
                "credential_generation": record["credential_generation"],
            })
        if request_key is not None:
            current["request_results"][request_key] = {"fingerprint": fingerprint, "result": deepcopy(result)}
        return current, result
    if op == "put":
        return args["value"], "ok"
    if op == "get":
        return state, state

    if op == "query_grants":
        return state, _query_grants(state, args)
    if op == "query_access_state":
        return state, _query_access_state(state, args)
    if op == "resolve_registered_user":
        return state, _resolve_registered_user(state, args)
    if op == "query_operation_receipt":
        # The per-deck reference state does not contain Raft's retained log or
        # dedup table. Without that proof, the only safe modeled result is unknown.
        return state, {"status": "OUTCOME_UNKNOWN"}
    if op in PRESENTATION_READS:
        read_operation = (
            "get_element" if op in ELEMENT_OBSERVATION_READ_OPERATIONS else op
        )
        return state, _read_presentation(state, read_operation, args)
    if op in {"grant_access", "revoke_access"}:
        return _apply_access_operation(state, op, args)
    if op == "migrate_legacy_deck":
        return _apply_legacy_migration(state, args)

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
            slide_meta = deepcopy(slide["metadata"])
            slide_meta.setdefault("slide_settings_version", 0)
            slide_data[slide_id] = {
                "metadata": slide_meta,
                "elements": element_ids,
            }
        metadata = deepcopy(presentation["metadata"])
        metadata.pop("owner_session_id", None)
        access = (
            {"access": {"access_version": 0, "grants": {}}}
            if presentation.get("schema_version") == 3 else {}
        )
        return {
            **access,
            "deck_id": deck_id,
            "deck_version": 0,
            "deck_settings_version": 0,
            "metadata": metadata,
            "slides": slide_ids,
            "slide_data": slide_data,
            "deleted": [],
            "elements": elements,
            "element_tombstones": [],
            "next_creation_sequence": creation_sequence,
        }, _result(
            "PUBLISHED", {"deck_id": deck_id, "deck_version": 0, "slide_ids": slide_ids}, 0
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

    if op in {"acquire_lock", "release_lock", "update_element", "delete_element", "renew_lock", "expire_lock"}:
        element_id = args["element_id"]
        element = state["elements"].get(element_id)
        if element is None:
            if op == "expire_lock":
                return state, _result("NO_OP_STALE_EXPIRY")
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
        if op == "expire_lock":
            expected_token = args.get("expected_token")
            expected_epoch = args.get("expected_lease_epoch")
            lock_matches = (
                element["lock_owner"] is not None
                and element["fencing_token"] == expected_token
                and element.get("lease_epoch", 0) == expected_epoch
            )
            if not lock_matches:
                return state, _result("NO_OP_STALE_EXPIRY")
            replacement = {**element, "lock_owner": None, "lease_epoch": 0}
            return {**state, "elements": {**state["elements"], element_id: replacement}}, _result(
                "EXPIRED", {
                    "deck_id": deck_id, "element_id": element_id,
                    "fencing_token": expected_token,
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

    if op == "update_deck_settings":
        return _apply_settings_operation(state, op, args)

    if op == "update_slide_settings":
        slide_id = args["slide_id"]
        if slide_id not in state["slides"]:
            return state, _result("SLIDE_DELETED" if slide_id in state["deleted"] else "NOT_FOUND")
        ordinal = state["slides"].index(slide_id)
        slide_meta = state["slide_data"][slide_id]["metadata"]
        current_version = slide_meta.get("slide_settings_version", 0)
        expected = args.get("expected_slide_settings_version", 0)
        if expected != current_version:
            return state, _result(
                "SLIDE_SETTINGS_VERSION_CONFLICT",
                {
                    "slide_settings_version": current_version,
                    "background": slide_meta["background"],
                    "deck_version": state["deck_version"],
                    "slide_id": slide_id,
                    "ordinal": ordinal,
                },
                current_version,
            )
        patch = args.get("patch", {})
        new_slide_meta = {**slide_meta, **patch, "slide_settings_version": current_version + 1}
        new_slide_data = {
            **state["slide_data"],
            slide_id: {**state["slide_data"][slide_id], "metadata": new_slide_meta},
        }
        new_version = current_version + 1
        new_state = {**state, "slide_data": new_slide_data}
        return new_state, _result(
            "SLIDE_SETTINGS_UPDATED",
            {
                "deck_id": deck_id,
                "slide_id": slide_id,
                "slide_settings_version": new_version,
                "background": new_slide_meta["background"],
                "deck_version": state["deck_version"],
                "ordinal": ordinal,
            },
            new_version,
        )

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
        new_slide_meta = deepcopy(args["slide"]["metadata"])
        new_slide_meta.setdefault("slide_settings_version", 0)
        slide_data = {
            **state["slide_data"],
            slide_id: {
                "metadata": new_slide_meta,
                "elements": [],
            },
        }
        version = state["deck_version"] + 1
        new = {**state, "deck_version": version, "slides": slides, "slide_data": slide_data}
        return new, _result(
            "CREATED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version, "slide_ids": new["slides"]}, version
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
            "DELETED", {"deck_id": deck_id, "slide_id": slide_id, "deck_version": version, "slide_ids": new["slides"]}, version
        )

    raise ValueError(f"operacion desconocida: {op}")


def _apply_legacy_migration(state, args):
    """Model an operator-requested migration whose provenance the replicas verified.

    Provenance lives in the retained Raft log, which this per-deck model lacks;
    a history records ``migrate_legacy_deck`` only for a committed operator
    request, so the model checks the state transition, not the proof.
    """

    deck_id = args["deck_id"]
    owner = str(args["owner_user_id"])
    if state is None:
        return state, _result("LEGACY_DECK_NOT_FOUND")
    if "access" in state:
        if state["metadata"].get("owner_user_id") == owner:
            return state, _result("LEGACY_DECK_ALREADY_MIGRATED")
        return state, _result("LEGACY_MIGRATION_CONFLICT")
    migrated = {
        **deepcopy(state),
        "metadata": {**state["metadata"], "owner_user_id": owner},
        "access": {"access_version": 0, "grants": {}},
    }
    return migrated, _result("LEGACY_DECK_MIGRATED", {"deck_id": deck_id})


def _apply_access_operation(state, op, args):
    """Model owner-only, independently versioned grant changes (ADR-013/014)."""

    if state is None or "access" not in state:
        return state, _result("ACCESS_DENIED")
    if _effective_access(state, args.get(ACTOR_FIELD)) is not DeckRole.OWNER:
        return state, _result("ACCESS_DENIED")
    current = state["access"]["access_version"]
    status, grants = _access_change(
        owner_user_id=state["metadata"].get("owner_user_id"), access_version=current,
        grants=state["access"]["grants"], operation=op, payload=args, registered=_registered_user_ids(),
    )
    if grants is None:
        unchanged = status in {AccessChangeStatus.ACCESS_NOT_FOUND, AccessChangeStatus.ACCESS_UNCHANGED}
        return state, _result(status, {"access_version": current} if unchanged else None)
    new_state = {**state, "access": {"access_version": current + 1, "grants": grants}}
    return new_state, _result(status, {"access_version": current + 1})


def _apply_settings_operation(state, op, args):
    """Apply settings commands that use their own independent version domains."""

    if op == "update_deck_settings":
        if state is None:
            return state, _result("NOT_FOUND")
        state = _normalized_deck(state)
        expected = args["expected_deck_settings_version"]
        current = state.get("deck_settings_version", 0)
        if expected != current:
            conflict_value = {
                "deck_settings_version": current,
                "name": state["metadata"].get("name"),
                "width_px": state["metadata"].get("width_px"),
                "height_px": state["metadata"].get("height_px"),
                "deck_version": state["deck_version"],
            }
            return state, _result("DECK_SETTINGS_VERSION_CONFLICT", conflict_value, current)
        patch = args.get("patch", {})
        new_name = patch.get("name", state["metadata"].get("name"))
        new_width = patch.get("width_px", state["metadata"].get("width_px"))
        new_height = patch.get("height_px", state["metadata"].get("height_px"))
        # Validate prospective complete object
        if not new_name or not isinstance(new_width, int) or not isinstance(new_height, int):
            return state, _result("INVALID_ARGUMENT")
        if new_width < 320 or new_width > 3840 or new_height < 320 or new_height > 3840:
            return state, _result("INVALID_ARGUMENT")
        new_meta = {**state["metadata"], "name": new_name, "width_px": new_width, "height_px": new_height}
        new_settings_version = current + 1
        new_state = {
            **state,
            "deck_settings_version": new_settings_version,
            "metadata": new_meta,
        }
        result_value = {
            "deck_id": state["metadata"]["deck_id"],
            "deck_settings_version": new_settings_version,
            "name": new_name,
            "width_px": new_width,
            "height_px": new_height,
            "deck_version": state["deck_version"],
        }
        return new_state, _result("DECK_SETTINGS_UPDATED", result_value, new_settings_version)

    raise ValueError(f"operacion de configuracion desconocida: {op}")


def apply(state, op, args):
    """Apply one public history operation to the independent model."""

    return _apply_operation(state, op, args)


def check_committed_auth_order(entries, request_results):
    """Replay non-secret auth and deck-settings transitions in committed Raft order.

    This is an independent reference for the Stage E.A partition gate. It
    checks durable outcomes, including a committed UNAUTHENTICATED write that
    the presentation-only history projection cannot feed to check.py.
    """
    auth = None
    decks = {}
    evidence = []
    supported = {"issue_access_token", "revoke_access_token", "publish_deck", "update_deck_settings"}
    observed = [(entry["index"], entry["command"]["operation"],
                 entry["command"].get("request_id")) for entry in entries]
    for entry in sorted(entries, key=lambda item: item["index"]):
        command = entry["command"]
        operation = command["operation"]
        if operation not in supported:
            continue
        client_id = command["client_id"]
        request_id = command["request_id"]
        args = deepcopy(command["payload"])
        args["request_id"] = request_id
        if command.get("deck_id") is not None:
            args["deck_id"] = command["deck_id"]
        if operation in {"issue_access_token", "revoke_access_token"}:
            auth, predicted = apply(auth, operation, args)
        else:
            deck_id = _deck_id(operation, args)
            credential = (auth or {}).get("active_credentials", {}).get(client_id)
            admitted = (
                credential is not None
                and (auth or {}).get("client_bindings", {}).get(client_id) == args.get("authenticated_actor_user_id")
                and credential["user_id"] == args.get("authenticated_actor_user_id")
                and credential["credential_id"] == args.get("credential_id")
                and credential["credential_generation"] == args.get("credential_generation")
                and not credential["revoked"]
            )
            if admitted:
                decks[deck_id], predicted = apply(decks.get(deck_id), operation, args)
            else:
                predicted = _result("UNAUTHENTICATED")
        stored = request_results.get(sha256_json([client_id, request_id]))
        if stored is None or stored["committed_index"] != entry["index"]:
            raise AssertionError(f"missing committed outcome: {request_id}")
        if stored["result"]["status"] != predicted["status"]:
            raise AssertionError(
                f"{request_id} at index {entry['index']}: expected {predicted['status']}, "
                f"stored {stored['result']['status']}; entries={observed}"
            )
        evidence.append({
            "index": entry["index"], "request_id": request_id,
            "operation": operation, "predicted_status": predicted["status"],
            "stored_status": stored["result"]["status"],
            "deck_settings_version": decks.get(_deck_id(operation, args), {}).get("deck_settings_version")
            if operation in {"publish_deck", "update_deck_settings"} else None,
        })
    return evidence


ROLE_GUARDED_DECK_OPERATIONS = {
    "create_slide", "delete_slide", "reorder_slides", "create_element", "acquire_lock",
    "release_lock", "update_element", "delete_element", "renew_lock",
    "update_deck_settings", "update_slide_settings",
}
OWNER_ONLY_DECK_OPERATIONS = {"grant_access", "revoke_access"}


def check_committed_access_order(entries, request_results):
    """Replay committed ownership, grants and deck-role decisions in Raft order.

    This is the Stage F counterpart of ``check_committed_auth_order``: an
    independent sequential reference for the role matrix (ADR-014). It models
    only the v3 activation fence, publication ownership, verified legacy
    migration and owner-only grant changes, then predicts for every external
    deck mutation whether apply must return ``ACCESS_DENIED``. Credential
    failures (``UNAUTHENTICATED``) precede the role check and are recorded but
    not role-predicted. A durable outcome that disagrees raises AssertionError.
    """
    fence = None
    decks = {}
    evidence = []
    registered = _registered_user_ids()
    for entry in sorted(entries, key=lambda item: item["index"]):
        command = entry["command"]
        operation = command["operation"]
        index = entry["index"]
        payload = command.get("payload") or {}
        stored = request_results.get(sha256_json([command["client_id"], command["request_id"]]))
        if stored is None or stored["committed_index"] != index:
            # A later duplicate entry is answered from the first committed result.
            continue
        status = stored["result"]["status"]
        if operation == "activate_presentation_schema_v3":
            if status == "PRESENTATION_SCHEMA_ACTIVATED":
                fence = index + 1
            continue
        if operation == "publish_deck":
            if status == "PUBLISHED":
                presentation = payload["presentation"]
                metadata = presentation["metadata"]
                decks[str(metadata["deck_id"])] = {
                    "schema_version": presentation["schema_version"],
                    "owner": metadata.get("owner_user_id"),
                    "grants": {}, "access_version": 0,
                }
            continue
        if operation == "migrate_legacy_deck":
            if status == "LEGACY_DECK_MIGRATED":
                deck = decks.setdefault(str(command["deck_id"]), {"grants": {}, "access_version": 0})
                deck.update(schema_version=3, owner=str(payload["owner_user_id"]))
            continue
        if operation not in ROLE_GUARDED_DECK_OPERATIONS | OWNER_ONLY_DECK_OPERATIONS:
            continue
        deck_id = str(command.get("deck_id"))
        actor = payload.get("authenticated_actor_user_id")
        deck = decks.get(deck_id)
        record = {"index": index, "request_id": command["request_id"], "operation": operation,
                  "deck_id": deck_id, "actor_user_id": actor, "stored_status": status}
        if status == "UNAUTHENTICATED":
            evidence.append({**record, "role": None, "predicted_status": "UNAUTHENTICATED_PRECEDES_ROLE"})
            continue
        if fence is None or index < fence:
            evidence.append({**record, "role": None, "predicted_status": "BEFORE_V3_FENCE"})
            continue
        if deck is None or deck.get("schema_version") != 3:
            role = DeckRole.NONE
        else:
            role = _role_of(deck["owner"], deck["grants"], actor)
        allowed = (role is DeckRole.OWNER if operation in OWNER_ONLY_DECK_OPERATIONS
                   else role in EDITING_ROLES)
        predicted = "AUTHORIZED" if allowed else "ACCESS_DENIED"
        if allowed and operation in OWNER_ONLY_DECK_OPERATIONS:
            predicted = _predict_access_change(deck, operation, payload, registered)
        if predicted == "AUTHORIZED":
            if status == "ACCESS_DENIED":
                raise AssertionError(f"{command['request_id']} at index {index}: role {role} "
                                     f"was authorized, stored ACCESS_DENIED")
        elif status != predicted:
            raise AssertionError(f"{command['request_id']} at index {index}: expected {predicted}, "
                                 f"stored {status}")
        evidence.append({**record, "role": role, "predicted_status": predicted,
                         "access_version": deck["access_version"] if deck else None})
    return evidence


def _predict_access_change(deck, operation, payload, registered):
    """Apply one owner-authorized grant change to the reference deck in place."""
    status, grants = _access_change(
        owner_user_id=deck["owner"], access_version=deck["access_version"], grants=deck["grants"],
        operation=operation, payload=payload, registered=registered,
    )
    if grants is not None:
        deck["grants"] = grants
        deck["access_version"] += 1
    return status


def _is_presentation_observation(observed):
    return isinstance(observed, dict) and "applied_index" in observed


def converged_ok(applied, observed):
    """Validate final convergence while ignoring only progress metadata.

    ``check.py`` routes every eventual final read here. A Stage A generic KV
    ``get`` observes the bare stored value; presentation reads carry their
    Raft-assigned ``applied_index``.
    """

    if not _is_presentation_observation(observed):
        if isinstance(observed, dict):
            return False
        return _final_state_reachable(applied, "get", {}, observed)
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
    return _final_state_reachable(applied, read_operation, read_arguments, normalized)


def _final_state_reachable(applied, read_operation, read_arguments, expected_result):
    """Whether some real-time-respecting order of ``applied`` yields the final read."""

    operations = list(applied)
    ends = [
        operation.get("complete") if operation.get("ok") else float("inf")
        for operation in operations
    ]
    seen = set()

    def search(done, state):
        if len(done) == len(operations):
            _, expected = apply(state, read_operation, read_arguments)
            return expected_result == expected
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
