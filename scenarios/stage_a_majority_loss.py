"""Deliberate two-of-three failure: no quorum acknowledgement, then recovery."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import chaos
import cluster
from client import Client
from stage_a_support import direct_request, new_request_id, record_trace, require_no_acknowledged_write, wait_for_catch_up, wait_for_leader


def main() -> None:
    client = Client("stage-a-majority-loss", "run/stage_a_majority_loss.jsonl")
    config = cluster.load()
    survivor = wait_for_leader(config)
    failed = [slot for slot in config["names"] if slot != survivor]
    if client.call("put", key="stage-a-majority-loss", value="acknowledged-before-majority-loss") != {"ok": True, "result": "ok"}:
        raise RuntimeError("pre-failure write was not committed")
    for slot in failed:
        chaos.main(["kill", slot])
    probe = direct_request(
        config["host_nodes"][survivor], request_id=new_request_id("no-quorum"), client="no-quorum-probe",
        op="put", args={"key": "stage-a-no-quorum", "value": "must-not-commit"},
    )
    record_trace("run/stage_a_majority_loss_probes.jsonl", "majority_loss_write_probe", survivor=survivor, failed=failed, response=probe)
    require_no_acknowledged_write(probe)
    chaos.main(["start", failed[0]])
    wait_for_leader(config)
    if client.call("put", key="stage-a-majority-loss-recovered", value="committed-after-quorum-return") != {"ok": True, "result": "ok"}:
        raise RuntimeError("cluster did not resume after quorum returned")
    chaos.main(["start", failed[1]])
    if wait_for_catch_up(client, "stage-a-majority-loss") != "acknowledged-before-majority-loss":
        raise RuntimeError("acknowledged pre-failure write was lost")
    print("final", wait_for_catch_up(client, "stage-a-majority-loss-recovered"))


if __name__ == "__main__":
    main()
