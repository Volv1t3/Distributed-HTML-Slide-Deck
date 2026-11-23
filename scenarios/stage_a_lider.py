"""Leader failure, durable baseline, re-election, restart, and convergence."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import chaos
import cluster
from client import Client
from stage_a_support import record_trace, wait_for_catch_up, wait_for_leader


def main() -> None:
    client = Client("stage-a-leader", "run/stage_a_lider.jsonl")
    config = cluster.load()
    old_leader = wait_for_leader(config)
    if client.call("put", key="stage-a-durable", value="acknowledged-before-failover") != {"ok": True, "result": "ok"}:
        raise RuntimeError("durability baseline was not acknowledged")
    chaos.main(["kill", old_leader])
    record_trace("run/stage_a_lider_probes.jsonl", "leader_killed", leader=old_leader)
    new_leader = wait_for_leader(config, exclude={old_leader})
    record_trace("run/stage_a_lider_probes.jsonl", "replacement_elected", leader=new_leader)
    if client.call("put", key="stage-a-after-failover", value="committed-by-replacement") != {"ok": True, "result": "ok"}:
        raise RuntimeError("replacement leader did not commit a write")
    chaos.main(["start", old_leader])
    record_trace("run/stage_a_lider_probes.jsonl", "former_leader_restarted", leader=old_leader)
    if wait_for_catch_up(client, "stage-a-durable") != "acknowledged-before-failover":
        raise RuntimeError("an acknowledged write was lost across failover")
    print("final", wait_for_catch_up(client, "stage-a-after-failover"))


if __name__ == "__main__":
    main()
