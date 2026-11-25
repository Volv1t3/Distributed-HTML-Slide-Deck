# Stage E.A validation

Run the normal scenario from the repository root:

```sh
python3 scenarios/run_stage_ea.py --scenario stage_e_normal
```

The runner refuses to start when `run/` already exists. It starts three local replicas, runs the scenario, and shuts down the cluster even if an assertion fails. `cluster.py down` archives the JSONL evidence and each replica's `roles.log` and `events.log` under a unique directory in `historiales/`.

The normal scenario logs in with the pre-provisioned Sofia fixture, then publishes a deck and exercises authenticated element and settings mutations. It checks a wrong editor session, stale element version, stale Deck Settings Version, and stale Slide Settings Version. Final reads and durable auth summaries must agree across all replicas.

The archive contains the redacted client history, checker projection and audit, checker result, manifest with a clean run ID, fault trace, credential-generation trace without bearer material, and separate latency buckets for login, token issuance, and authenticated edits. Token issuance latency measures the leader's interval from `login_issuance_proposed` to `login_completed`; login latency measures the complete client call. A single scenario gives one login and one issuance sample, so its p50 and p95 are identical. The scenario scans the disposable `run/` tree for its literal test password and issued bearer before shutdown.
