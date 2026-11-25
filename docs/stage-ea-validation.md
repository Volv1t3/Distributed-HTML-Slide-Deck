# Stage E.A validation

Run the normal scenario from the repository root:

```sh
python3 scenarios/run_stage_ea.py --scenario stage_e_normal
python3 scenarios/run_stage_ea.py --scenario stage_e_rotation
```

The runner refuses to start when `run/` already exists. It starts three local replicas, runs the scenario, and shuts down the cluster even if an assertion fails. `cluster.py down` archives the JSONL evidence and each replica's `roles.log` and `events.log` under a unique directory in `historiales/`.

The normal scenario logs in with the pre-provisioned Sofia fixture, then publishes a deck and exercises authenticated element and settings mutations. It checks a wrong editor session, stale element version, stale Deck Settings Version, and stale Slide Settings Version. Final reads and durable auth summaries must agree across all replicas.

The rotation scenario logs in twice with one stable client UUID. It checks that a repeated first login ID does not issue another credential, the older bearer is rejected after rotation, the newer bearer can edit, a stale revoke cannot invalidate it, and the current credential revokes once. An exact revoke retry and a fresh edit with the revoked bearer both fail admission. Phase-by-phase auth summaries record the credential generation and revocation state on all three replicas.

The archive contains the redacted client history, checker projection and audit, checker result, manifest with a clean run ID, fault trace, credential-generation trace without bearer material, and separate latency buckets for login, token issuance, token revocation, and authenticated edits. Token issuance latency measures the leader's interval from `login_issuance_proposed` to `login_completed`; login and revoke latency measure complete client calls. The checker projection excludes login, revocation, and explicit authentication admission denials, which the deterministic presentation model does not represent; the audit names each exclusion. The scenarios scan the disposable `run/` tree for their literal test password and issued bearers before shutdown.
