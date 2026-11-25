# Stage E.A validation

Run the normal scenario from the repository root:

```sh
python3 scenarios/run_stage_ea.py --scenario stage_e_normal
python3 scenarios/run_stage_ea.py --scenario stage_e_rotation
python3 scenarios/run_stage_ea.py --scenario stage_e_failover
python3 scenarios/run_stage_ea.py --scenario stage_e_lost_login
```

The runner refuses to start when `run/` already exists. It starts three local replicas, runs the scenario, and shuts down the cluster even if an assertion fails. `cluster.py down` archives the JSONL evidence and each replica's `roles.log` and `events.log` under a unique directory in `historiales/`.

The normal scenario logs in with the pre-provisioned Sofia fixture, then publishes a deck and exercises authenticated element and settings mutations. It checks a wrong editor session, stale element version, stale Deck Settings Version, and stale Slide Settings Version. Final reads and durable auth summaries must agree across all replicas.

The rotation scenario logs in twice with one stable client UUID. It checks that a repeated first login ID does not issue another credential, the older bearer is rejected after rotation, the newer bearer can edit, a stale revoke cannot invalidate it, and the current credential revokes once. An exact revoke retry and a fresh edit with the revoked bearer both fail admission. Phase-by-phase auth summaries record the credential generation and revocation state on all three replicas.

The failover scenario stops one follower before login, verifies the issuance entry is durable, committed, and applied on the issuing leader and the other voter, then stops that leader. It restarts the lagging follower with its disk intact so the up-to-date voter can win election, uses the issued bearer for protected writes on that replacement leader, and restarts the old leader. Final auth summaries compare the client binding, credential identity, generation, revocation state, and digest across all three replicas. Presentation settings and applied indexes must converge. The fault trace and election unavailability measurement remain in the same archive.

The lost-login scenario intercepts the committed HTTP response at the authenticated client's transport boundary. After the issuing leader and another voter durably apply the issuance, the scenario stops that leader and discards the response before `AuthenticatedClient.login()` can install the bearer. The client history records a failed delivery, and no credential file is created. After election, the same installation sends the old login ID to the replacement leader and receives `LOGIN_SECRET_UNAVAILABLE`; a new login ID reaches generation 2. A durable publication intention queued before login keeps its original request ID through reauthentication and commits with the new bearer. The scenario verifies the old digest is retired, all replicas converge, and the literal lost and replacement bearers are absent from the disposable run tree, including Raft disk snapshots and logs.

The archive contains the redacted client history, checker projection and audit, checker result, manifest with a clean run ID, fault trace, credential-generation trace without bearer material, and separate latency buckets for login, token issuance, token revocation, and authenticated edits. Token issuance latency measures the leader's interval from `login_issuance_proposed` to `login_completed`; login and revoke latency measure complete client calls. The checker projection excludes login, revocation, and explicit authentication admission denials, which the deterministic presentation model does not represent; the audit names each exclusion. The scenarios scan the disposable `run/` tree for their literal test password and issued bearers before shutdown.
