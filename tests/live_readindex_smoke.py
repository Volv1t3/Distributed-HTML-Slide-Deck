"""Manual live three-replica regression; independent of the client-journal checker."""
import json
import sys
import time
import urllib.request
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cluster
import chaos


def request(url, op, args, client="readindex-smoke"):
    payload = json.dumps({"id": str(uuid4()), "client": client, "op": op, "args": args}).encode()
    req = urllib.request.Request(url + "/client", data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as response:
        return json.load(response)


def leader_from(c, names=None):
    names = names or c["names"]
    states = cluster.status(c)
    leaders = [(name, states[name]["term"]) for name in names if states[name] and states[name]["role"] == "leader"]
    return max(leaders, key=lambda x: x[1])[0] if leaders else None


def wait_for_leader(c, names, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        leader = leader_from(c, names)
        if leader:
            return leader
        time.sleep(0.15)
    raise RuntimeError("leader election failed")


def must_status(result, status):
    assert result.get("ok") and result.get("result", {}).get("status") == status, result


def run():
    evidence = {"scenario": "readindex-stale-former-leader"}
    cluster.up(3, True)
    try:
        c = cluster.load()
        initial_leader = wait_for_leader(c, c["names"])
        initial_url = c["host_nodes"][initial_leader]
        deck_id, slide_id, element_id, session_id = map(str, (uuid4() for _ in range(4)))
        evidence.update(initial_leader=initial_leader, deck_id=deck_id, element_id=element_id)
        publish = request(initial_url, "publish_deck", {
            "presentation": {"schema_version": 1,
                "metadata": {"deck_id": deck_id, "name": "ReadIndex regression",
                             "owner_session_id": session_id, "width_px": 1280, "height_px": 720},
                "slides": [{"metadata": {"slide_id": slide_id, "background": "#112233", "ordinal": 0}, "elements": []}]}})
        must_status(publish, "PUBLISHED")
        created = request(initial_url, "create_element", {
            "deck_id": deck_id, "session_id": session_id, "slide_id": slide_id,
            "element": {"element_id": element_id, "kind": "text_box", "version": 1,
                "configuration": {"content": "OLD", "generic_fallback": "sans-serif",
                                  "font_size": 2400, "color": "#000000FF", "x": 0, "y": 0}}})
        must_status(created, "CREATED_AND_LOCKED")
        # Current-term log entry has committed; read must complete via quorum without adding a new entry.
        before_read_state = cluster.status(c)[initial_leader]
        prior = request(initial_url, "authoritative_refresh_element", {
            "deck_id": deck_id, "element_id": element_id,
            "min_commit_index": before_read_state["last_applied"]})
        assert prior["ok"] and prior["result"]["element_version"] == 1, prior
        after_read_state = cluster.status(c)[initial_leader]
        assert after_read_state["commit_index"] == before_read_state["commit_index"], (
            "ReadIndex must not append and commit a new Raft log entry")
        evidence["no_read_log_append"] = True
        evidence["initial_read"] = prior
        evidence["original_applied_index"] = prior["result"]["applied_index"]
        remaining = [n for n in c["names"] if n != initial_leader]
        chaos.set_faults({"partition": [[initial_leader], remaining]})
        new_leader = wait_for_leader(c, remaining)
        evidence["new_leader"] = new_leader
        majority_url = c["host_nodes"][new_leader]
        saved = request(majority_url, "update_element", {
            "deck_id": deck_id, "element_id": element_id, "session_id": session_id,
            "fencing_token": created["result"]["value"]["fencing_token"],
            "expected_version": 1, "patch": {"content": "NEW"}})
        must_status(saved, "SAVED")
        evidence["majority_edit"] = saved
        fresh = request(majority_url, "authoritative_refresh_element", {
            "deck_id": deck_id, "element_id": element_id,
            "min_commit_index": evidence["original_applied_index"]})
        assert fresh["ok"] and fresh["result"]["element_version"] == 2, fresh
        evidence["majority_read"] = fresh
        stale = request(initial_url, "authoritative_refresh_element", {
            "deck_id": deck_id, "element_id": element_id,
            "min_commit_index": evidence["original_applied_index"]})
        evidence["former_leader_read"] = stale
        assert not stale.get("ok"), "a partitioned leader served a linearizable read: " + str(stale)
        assert stale.get("error") in {"QUORUM_UNAVAILABLE", "NO_LEADER", "timeout"}, stale
        chaos.set_faults({})
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            observations = [request(c["host_nodes"][n], "get_element", {"deck_id": deck_id, "element_id": element_id})
                            for n in c["names"]]
            if all(o.get("ok") and o["result"].get("element_version") == 2 for o in observations):
                evidence["healed_versions"] = [o["result"]["element_version"] for o in observations]
                break
            time.sleep(0.25)
        else:
            raise AssertionError("replicas did not converge after healing")
        evidence["passed"] = True
    finally:
        chaos.set_faults({})
        cluster.down(quiet=True)
        Path(__file__).resolve().parents[1].joinpath("readindex-live-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
