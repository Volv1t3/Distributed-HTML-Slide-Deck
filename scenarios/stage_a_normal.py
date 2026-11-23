"""Healthy three-replica Stage A.1 KV replication and convergence evidence."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import Client
from stage_a_support import wait_for_catch_up, wait_for_leader


def main() -> None:
    history = "run/stage_a_normal.jsonl"
    client = Client("stage-a-normal", history)
    leader = wait_for_leader(__import__("cluster").load())
    print("leader", leader)
    for value in ("one", "two", "three"):
        response = client.call("put", key="stage-a-normal", value=value)
        if response != {"ok": True, "result": "ok"}:
            raise RuntimeError(f"healthy write was not committed: {response}")
    print("final", wait_for_catch_up(client, "stage-a-normal"))


if __name__ == "__main__":
    main()
