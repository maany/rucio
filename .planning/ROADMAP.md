# Roadmap: Rucio Test Framework Overhaul

## Overview

This roadmap transforms Rucio's 6-script CI test chain into a single `pytest --suite=<name>` command. The journey starts with the plugin skeleton and suite profile data structures that everything else reads from, progresses through database lifecycle extraction and container orchestration (the two heaviest subsystems), adds test collection filtering, and concludes with CI workflow integration and conftest.py migration. Each phase delivers a verifiable capability that builds on the previous one.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Plugin Skeleton and Suite Profiles** - In-repo plugin with CLI options, suite profile registry, and xdist configuration (completed 2026-02-20)
- [x] **Phase 2: Database Lifecycle and Bootstrap** - DB purge/build/seed, httpd restart, memcache flush, and bootstrap data extracted into InfraManager (completed 2026-03-05)
- [ ] **Phase 3: Container Lifecycle and Cleanup** - Docker compose orchestration with readiness checks, signal handlers, orphan cleanup, and log capture
- [ ] **Phase 4: Test Collection and Parallelism Integration** - Suite-based test filtering, independent infra override, and NoParallelScheduler wiring
- [ ] **Phase 5: CI Workflow and Migration** - simple-autotest.yml using pytest directly, conftest.py refactored to fixtures only, lint workflow separated

## Phase Details

### Phase 1: Plugin Skeleton and Suite Profiles
**Goal**: A working pytest plugin that parses --suite, resolves suite profiles, configures xdist, and stores state in config.stash with correct xdist controller/worker guards
**Depends on**: Nothing (first phase)
**Requirements**: PLUG-01, PLUG-02, SUIT-01, SUIT-02, PARA-02, PARA-03, PARA-04, PARA-05
**Success Criteria** (what must be TRUE):
  1. Running `pytest --suite=remote_dbs --co` loads the plugin and prints the resolved suite profile without errors
  2. Running `pytest --suite=sqlite --co` shows xdist is auto-disabled (0 workers) for incompatible RDBMS
  3. Running `pytest --xdist-workers=4 --suite=remote_dbs --co` shows 4 workers configured
  4. Plugin is registered via `tests/ruciopytest/` package with no new pip dependencies
  5. When xdist is active, pytest_configure setup logic runs only on the controller process, not on workers
**Plans**: 2 plans

Plans:
- [x] 01-01-PLAN.md — Create plugin modules (profiles.py, xdist_config.py, plugin.py) with suite profile dataclass, xdist auto-config, and plugin hooks
- [x] 01-02-PLAN.md — Wire plugin into conftest.py, migrate --suite option, validate end-to-end activation

### Phase 2: Database Lifecycle and Bootstrap
**Goal**: InfraManager handles complete database lifecycle (purge, schema build, seed) and bootstrap data creation, extracted from the existing conftest.py into a testable module
**Depends on**: Phase 1
**Requirements**: DBBS-01, DBBS-02, DBBS-03, DBBS-04, DBBS-05, SUIT-06, CONT-08
**Success Criteria** (what must be TRUE):
  1. Running `pytest --suite=remote_dbs` against an already-running container stack purges the database, rebuilds schema, creates root account and base VO, and seeds test data (accounts, scopes, RSEs, metadata keys)
  2. Running `pytest --suite=remote_dbs --keep-db` skips database purge/rebuild and proceeds directly to tests
  3. Running `pytest --suite=sqlite` deletes the SQLite file and rebuilds from scratch (SQLite-specific lifecycle)
  4. Memcache is flushed before test session starts when the suite profile includes memcache
  5. httpd is restarted gracefully after database rebuild completes
**Plans**: 2 plans

Plans:
- [x] 02-01-PLAN.md — Create InfraManager class with full DB lifecycle methods (purge, build, seed, memcache flush, httpd restart, bootstrap data, RSE sync, metadata sync)
- [x] 02-02-PLAN.md — Wire InfraManager into plugin.py, register --keep-db option, remove DB lifecycle code from conftest.py

### Phase 3: Container Lifecycle and Cleanup
**Goal**: Plugin manages full Docker container lifecycle (up, readiness, down) with belt-and-suspenders cleanup ensuring no orphaned containers survive failed runs
**Depends on**: Phase 2
**Requirements**: PLUG-03, CONT-01, CONT-02, CONT-03, CONT-04, CONT-05, CONT-06, CONT-07, SUIT-05
**Success Criteria** (what must be TRUE):
  1. Running `pytest --suite=remote_dbs` starts required containers via docker compose, waits for readiness (DB accepts connections, httpd responds), then runs tests
  2. After tests complete (pass or fail), containers are stopped and removed via docker compose down
  3. If pytest is interrupted with SIGTERM or SIGINT, containers are still cleaned up
  4. If a previous run left orphaned containers (matching the compose project name pattern), they are detected and removed at startup before new containers start
  5. When tests fail, container logs (httpd, DB) are captured and available in the test report
**Plans**: 3 plans

Plans:
- [x] 03-01-PLAN.md — Create ContainerManager class with compose lifecycle (up/down/readiness), orphan cleanup, and belt-and-suspenders cleanup handlers; wire into plugin.py
- [x] 03-02-PLAN.md — Implement log capture in ContainerManager and add pytest_terminal_summary hook for log visibility and JUnit XML integration
- [ ] 03-03-PLAN.md — Fix signal handler timing, same-name orphan cleanup, and client suite container skip (UAT gap closure)

### Phase 4: Test Collection and Parallelism Integration
**Goal**: Plugin filters test collection based on suite profile and integrates with existing NoParallelScheduler for conflict-free parallel execution
**Depends on**: Phase 1
**Requirements**: SUIT-03, SUIT-04, PARA-01
**Success Criteria** (what must be TRUE):
  1. Running `pytest --suite=remote_dbs --co` collects only tests matching the suite's configured test paths and markers (not all tests in the repo)
  2. Running `pytest --infra=postgres14,httpd` without --suite selects infrastructure independently and collects all tests (power-user override mode)
  3. Tests marked with `noparallel` are scheduled by the existing NoParallelScheduler with disjoint-set conflict resolution when xdist is active
**Plans**: 2 plans

Plans:
- [ ] 04-01-PLAN.md — Suite-aware test collection filtering, --infra override, --dry-run support
- [ ] 04-02-PLAN.md — Enhance NoParallelScheduler with suite-aware grouping, configurable conflict sets, and conflict reporting

### Phase 5: CI Workflow and Migration
**Goal**: simple-autotest.yml replaces the matrix/script orchestration chain, running all suites via pytest commands identical to local execution, with lint/type checks in a separate workflow
**Depends on**: Phase 3, Phase 4
**Requirements**: CICD-01, CICD-02, CICD-03, CICD-04
**Success Criteria** (what must be TRUE):
  1. simple-autotest.yml runs all 5 suites (remote_dbs, sqlite, multi_vo, client, votest) using only `pytest --suite=<name>` commands with no intermediate shell scripts
  2. A developer running `pytest --suite=remote_dbs` locally gets identical behavior to the CI run (same container setup, same test selection, same output)
  3. CI produces JUnit XML output that GitHub Actions displays as test results with pass/fail annotations
  4. Type checking and syntax checking run in a separate lint/format workflow, not in simple-autotest.yml
**Plans**: TBD

Plans:
- [ ] 05-01: TBD
- [ ] 05-02: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5
(Note: Phase 4 depends on Phase 1, not Phase 3, so it could theoretically run in parallel with Phases 2-3. However, sequential execution is simpler.)

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Plugin Skeleton and Suite Profiles | 2/2 | Complete    | 2026-02-20 |
| 2. Database Lifecycle and Bootstrap | 2/2 | Complete    | 2026-03-05 |
| 3. Container Lifecycle and Cleanup | 2/3 | In progress | - |
| 4. Test Collection and Parallelism Integration | 0/2 | Not started | - |
| 5. CI Workflow and Migration | 0/2 | Not started | - |

### Phase 6: Host pytest with optional --run-in-container forwarding to container pytest

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 5
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd:plan-phase 6 to break down)
