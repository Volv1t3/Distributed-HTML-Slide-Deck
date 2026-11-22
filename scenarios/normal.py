"""Escenario de ejemplo: operaciones sin fallas.

    python3 cluster.py up
    python3 scenarios/normal.py
    python3 check.py run/normal.jsonl

Copia este archivo para tus escenarios y usa chaos.main([...]) para las
fallas, por ejemplo chaos.main(["kill", "leader"]).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import Client  # noqa: E402

c = Client("c1", "run/normal.jsonl")
for i in range(5):
    c.call("put", key="x", value=i)
    print("get x ->", c.call("get", key="x").get("result"))
