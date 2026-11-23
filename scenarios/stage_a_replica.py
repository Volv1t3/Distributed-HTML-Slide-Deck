"""Follower crash/restart with intact disk and subsequent catch-up evidence."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import chaos
import cluster
from client import Client
from stage_a_support import record_trace, wait_for_catch_up, wait_for_leader


def main() -> None:
    client = Client("stage-a-replica", "run/stage_a_replica.jsonl")
    config = cluster.load()
    leader = wait_for_leader(config)
    follower = next(slot for slot in config["names"] if slot != leader)
    if client.call("put", key="stage-a-replica", value="before-restart") != {"ok": True, "result": "ok"}:
        raise RuntimeError("baseline write was not committed")
    chaos.main(["kill", follower])
    record_trace("run/stage_a_replica_probes.jsonl", "follower_killed", leader=leader, follower=follower)
    if client.call("put", key="stage-a-replica", value="while-follower-down") != {"ok": True, "result": "ok"}:
        raise RuntimeError("majority did not commit with one follower down")
    chaos.main(["start", follower])
    record_trace("run/stage_a_replica_probes.jsonl", "follower_restarted", follower=follower)
    print("final", wait_for_catch_up(client, "stage-a-replica"))


if __name__ == "__main__":
    main()
