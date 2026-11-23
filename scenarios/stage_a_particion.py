"""One-versus-two partition: minority refusal, majority progress, and healing."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import chaos
import cluster
from client import Client
from stage_a_support import direct_request, new_request_id, record_trace, require_no_acknowledged_write, wait_for_catch_up, wait_for_leader


def main() -> None:
    client = Client("stage-a-partition", "run/stage_a_particion.jsonl")
    config = cluster.load()
    isolated = wait_for_leader(config)
    majority = [slot for slot in config["names"] if slot != isolated]
    if client.call("put", key="stage-a-partition", value="before-partition") != {"ok": True, "result": "ok"}:
        raise RuntimeError("partition baseline was not committed")
    chaos.main(["partition", isolated, "/", *majority])
    probe = direct_request(
        config["host_nodes"][isolated], request_id=new_request_id("isolated"), client="minority-probe",
        op="put", args={"key": "stage-a-minority", "value": "must-not-commit"},
    )
    record_trace("run/stage_a_particion_probes.jsonl", "minority_write_probe", target=isolated, response=probe)
    require_no_acknowledged_write(probe)
    replacement = wait_for_leader(config, exclude={isolated})
    record_trace("run/stage_a_particion_probes.jsonl", "majority_leader", leader=replacement)
    if client.call("put", node=replacement, key="stage-a-partition", value="majority-committed") != {"ok": True, "result": "ok"}:
        raise RuntimeError("majority partition did not make progress")
    chaos.main(["heal"])
    print("final", wait_for_catch_up(client, "stage-a-partition"))


if __name__ == "__main__":
    main()
