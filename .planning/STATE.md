---
gsd_state_version: 1.0
milestone: v1.1
milestone_name: Productionize & Merge
status: 08-03 shipped -- host-side client leg gets full rucio install + reachable bootstrapped server (ports override + run_tests.sh -i) + rucio_client.cfg/cert wiring, with legacy-faithful in-container fallback on unreachable-server/import failure
stopped_at: Completed 08-03-PLAN.md
last_updated: "2026-06-30T11:11:06.638Z"
last_activity: 2026-06-30 -- 08-03 executed
progress:
  total_phases: 3
  completed_phases: 1
  total_plans: 7
  completed_plans: 6
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-20)

**Core value:** Running any test suite should be a single `pytest` command with arguments -- no shell scripts, no matrix parsers, no manual container management.
**Current focus:** v1.1 Productionize & Merge -- Phase 7 (suite-filtering parity) next.

## Current Position

Milestone: v1.1 Productionize & Merge (in progress)
Phase: 8 of 9 (CI for real) -- in progress
Plan: 08-03 complete (host-side client provisioning + in-container fallback); 08-02/08-01 prior
Status: 08-03 shipped -- host-side client leg gets full rucio install + reachable bootstrapped server (ports override + run_tests.sh -i) + rucio_client.cfg/cert wiring, with legacy-faithful in-container fallback on unreachable-server/import failure
Last activity: 2026-06-30 -- 08-03 executed

Progress: v1.0 [██████████] 100% | v1.1 [██░░░░░░░░] 17%

## Performance Metrics

**Velocity:**
- Total plans completed: 7
- Average duration: 1.9min
- Total execution time: 0.20 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| Phase 01 P01 | 2min | 2 tasks | 3 files |
| Phase 01 P02 | 3min | 2 tasks | 3 files |
| Phase 02 P01 | 2min | 1 task | 1 file |
| Phase 02 P02 | 2min | 2 tasks | 2 files |
| Phase 03 P01 | 2min | 2 tasks | 2 files |

**Recent Trend:**
- Last 5 plans: 2min, 2min, 2min, 1min, 1min
- Trend: stable/improving

| Phase 03 P02 | 1min | 2 tasks | 2 files |
| Phase 03 P03 | 1min | 2 tasks | 2 files |

*Updated after each plan completion*
| Phase 04 P01 | 4min | 2 tasks | 4 files |
| Phase 04 P02 | 2min | 2 tasks | 3 files |
| Phase 05 P02 | 1min | 2 tasks | 1 files |
| Phase 05 P01 | 2min | 2 tasks | 2 files |
| Phase 6 P1 | 4min | 2 tasks | 2 files |
| Phase 6 P2 | 7min | 2 tasks | 2 files |
| Phase 06 P03 | 3min | 3 tasks | 3 files |
| Phase 06 P04 | 2min | 2 tasks | 2 files |
| Phase 07 P01 | 8min | 4 tasks | 6 files |
| Phase 07 P03 | 3min | 2 tasks | 2 files |
| Phase 07 P02 | 9min | 3 tasks | 3 files |
| Phase 08 P02 | 2min | 2 tasks | 1 files |
| Phase 08 P01 | 6min | 2 tasks | 7 files |
| Phase 08 P03 | 6 | 2 tasks | 1 files |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Roadmap: 5 phases derived from requirement dependencies; plugin skeleton first, CI migration last
- Roadmap: Phase 4 (collection/parallelism) depends only on Phase 1, not Phase 3; could run earlier if needed
- 01-01: Only postgres14 is xdist-compatible; sqlite, oracle, mysql8 auto-disable xdist
- 01-01: Plugin dormant when --suite not provided (no default suite)
- 01-01: Workers resolve profile independently but skip xdist config and summary printing
- 01-01: SUITE env var set for backward compatibility with existing test code
- 01-02: Plugin registered via pytest_plugins tuple, not setuptools entry point
- 01-02: Suite resolved from plugin stash key with getoption fallback
- 01-02: All getoption calls updated to use default=None for dormant mode
- 01-02: Terminal reporter fallback to print() when not available during early pytest_configure
- 02-01: Separated _build_database and _create_base_vo_and_root_account into distinct methods for testability
- 02-01: Cached _is_sqlite flag during _purge_database to avoid redundant engine detection
- 02-01: Extracted _delete_sqlite_file and _purge_remote_db as private helpers for DRY purge logic
- 02-02: Lazy import of InfraManager inside profile.name != client guard to avoid import-time side effects
- 02-02: Registered --keep-db in plugin.py rucio option group rather than conftest.py to centralize CLI options
- 03-01: ContainerManager detects in-container execution via /.dockerenv or RUCIO_SOURCE_DIR and skips compose lifecycle
- 03-01: start_new_session=True on compose down subprocess to prevent SIGINT propagation to cleanup process
- 03-01: _capture_logs is a stub pass for Plan 02 to implement
- 03-02: Log capture is best-effort: all errors caught as warnings, never raises
- 03-02: Combined log uses project_name as filename, per-service logs use service name
- 03-02: JUnit XML integration uses add_global_property with container_log: prefix
- 03-03: Signal handlers registered before any blocking Docker operations (compose up, readiness check)
- 03-03: Removed same-name exclusion from orphan filter; safe because cleanup runs before startup
- 03-03: Client suite compose_profiles set to empty tuple to skip container lifecycle
- [Phase 04]: Moved stash keys to __init__.py to avoid circular imports between plugin.py and collection.py
- [Phase 04]: --infra without --suite creates synthetic merged profile; --dry-run skips container lifecycle entirely
- [Phase 04]: Positional args in noparallel marker take precedence over EXCLUSIVE default
- [Phase 04]: Suite-aware grouping activates only for synthetic merged profiles ('+' in name); single-suite unchanged
- [Phase 04]: Conflict report passed through config.stash[noparallel_report_key] for scheduler-to-terminal decoupling
- [Phase 05]: Copied python_annotations from autotest.yml (ruff) not code-quality.yml (flake8) for lint.yml
- [Phase 05]: Reused pinned action hashes from runtime_images.yml for CI workflow consistency
- [Phase 05]: No backward-compatibility shims for removed lifecycle fixtures; InfraManager handles all lifecycle
- [Phase 06]: 06-03: register_container_stream attaches unconditionally so xdist controller emits every worker report once
- [Phase 06]: 06-03: missing bind mount is a hard UsageError (no copy fallback); image staleness only warns
- [Phase 06]: 06-03: host reads results only from mounted JSONL (inherited pipe) to avoid double-draining
- [Phase 06]: 06-04: forwarding_applies computed once (single _should_forward_to_container call) reused by dry-run guard and delegation; --dry-run/--co forward into the container for container suites, host suites keep the fast early-exit
- [Phase 06 gap, live-found]: Forwarded host exit code MUST be applied via pytest.exit(returncode=...), not session.exitstatus — pytest _main() overwrites exitstatus from testsfailed/testscollected, and host collection is suppressed (config.args=[]) so testscollected is always 0; all-pass/--co runs wrongly exited 5. finalize_host_exit() fixes FWD-05. Live-verified: --co EXIT=0 (was 5), 29/29 unit tests. Commit 23319a643
- [Phase 07]: 07-01: votest selection reimplemented (absorbed) in votest_support.py; repo-relative paths, is_file drop -> atlas=36/belleii=52
- [Phase 07]: 07-01: --policy flag wins over POLICY env; missing/unknown policy raises UsageError (data-driven from YAML keys)
- [Phase 07]: 07-01: cfg [policy] rewrite only, no policy-package pip install (CI reality per RESEARCH Pitfall 3)
- [Phase 07]: 07-03: parity baseline is checked-in JSON of sorted file paths; comparison is sorted SETS (votest order non-deterministic)
- [Phase 07]: 07-03: drift guard is import-free (no rucio import / server / container); test_drift_detected proves it fails on inventory or YAML allow/deny changes
- [Phase 07]: 07-02: multi_vo per-VO run-twice realized as child python -m pytest subprocesses from InfraManager.run_multi_vo() (no plugin.py change, no in-process runtestloop)
- [Phase 07]: 07-02: per-VO rucio.cfg destination = /opt/rucio/etc/multi_vo/{tst,ts2}/etc/rucio.cfg (verified via tst cfg [alembic] line); merge_configs copied verbatim, not imported
- [Phase 07]: 07-02: run_multi_vo() triggered as final step of setup() for multi_vo suite; bootstrap_vo re-points RUCIO_HOME with no DB reset; stop-on-tst-failure preserved
- [Phase 08]: 08-02: host driver deps installed via pip -c constraints against requirements.dev.txt (not -r) so legs get pinned pytest/xdist/pyyaml without full server env
- [Phase 08]: 08-02: votest leg gets policy=atlas via matrix.policy + POLICY env export; sqlite leg dropped -> 5-leg matrix
- [Phase 08]: 08-02: pytest output tee'd to <leg>.pytest.log with set -o pipefail; on-failure host-logs artifact = .test-forward + pytest.log (alongside container .test-logs)
- [Phase 08]: 08-01: sqlite removed from plugin SUITE_PROFILES/--suite choices and parity baseline+guard; legacy autotest CI keeps sqlite (untouched); generic rdbms_override != sqlite plumbing retained
- [Phase 08]: 08-03: host-side client leg uses checked-in etc/certs/* host certs (sed-rewrite cfg) instead of copying certs out of the container
- [Phase 08]: 08-03: single 'reachable' gate (httpd ping + host pytest --co) switches client between host-side run and legacy in-container fallback; both emit junit to test-results/
- [Phase 08]: 08-03: bin/ prepended to PATH on host (no console_scripts in pyproject) so test_bin_rucio finds the rucio CLI

### Pending Todos

None yet.

### Roadmap Evolution

- Phase 6 added: Host pytest with optional --run-in-container forwarding to container pytest

### Blockers/Concerns

- RESOLVED: All Docker operations must remain host-side (rucio container has no Docker socket access, verified in 03-RESEARCH.md)
- Research flag: Validate `config.option.numprocesses = 0` as correct xdist disable mechanism in pytest-xdist 3.5.0 during Phase 1
- Research flag: Map conftest.py fixture dependency graph before Phase 5 refactoring

## Session Continuity

Last session: 2026-06-30T11:10:14.519Z
Stopped at: Completed 08-03-PLAN.md
Resume file: None
