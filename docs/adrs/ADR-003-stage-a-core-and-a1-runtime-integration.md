# ADR-003 — Stage A Core and Stage A.1 Runtime Integration Boundary

- **Status:** Accepted
- **Date:** 2026-11-22
- **Scope:** Delivery boundaries for the Raft protocol core and its instructor-runtime integration
- **Related:** ADR-002, ADR-007; Stage A Raft Implementation Specification

## Context

The supplied Lab 5 code validates important Raft decision rules, while the project starter supplies the actual multi-process HTTP runtime, fault injection, persistence API, client histories, and checker. Treating every concern as one implementation stage hides whether a failure belongs to the protocol core or the adapter that runs it in the instructor environment.

## Decision

Stage A and Stage A.1 are separate delivery gates.

**Stage A — Core** implements a pure production Raft engine derived from the Lab 5 rules, strict and versioned DTO validation, injected transport and persistence interfaces, election, AppendEntries replication, current-term commitment, ordered deterministic application, request deduplication, and the Raft-neutral key-value proof state machine. It is validated with deterministic unit and component tests.

**Stage A.1 — Runtime integration** connects the core to the unchanged starter callbacks and HTTP transport through the Raft adapter. It persists through `self.disk`/`self.save()`, performs slot/UUID registration and RPC correlation, supplies structured operational logs, uses the project-built Pydantic Docker image, and proves the system through the starter's local and container cluster, fault tools, histories, and checker.

Stage B adds presentation operations, locks, conflict recovery, and client synchronization primarily as state-machine and client concerns. It must not introduce a second consensus mechanism. ADR-007 records the narrow Raft ReadIndex safety exception required for an authoritative conflict-recovery read.

## Consequences

- The Stage A core has a narrow, deterministic completion gate.
- Stage A.1 is the only stage that may claim behavior in the instructor's real runtime.
- Existing `node.py`, `client.py`, `chaos.py`, and `check.py` interfaces stay intact.
- Stage A.1 must pass the integrated failure evidence before Stage B relies on the core.
- Later stages may amend this boundary only through an explicit ADR that preserves the established consensus and runtime invariants.

## Alternatives rejected

- One undifferentiated Stage A: failures are harder to localize and live-runtime claims can mask an incomplete core.
- Replacing the starter transport: bypasses the supplied fault and grading semantics.
