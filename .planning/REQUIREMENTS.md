# Requirements: Rucio Test Framework Overhaul

**Defined:** 2026-02-20
**Core Value:** Running any test suite should be a single `pytest` command with arguments — no shell scripts, no matrix parsers, no manual container management.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Container Management

- [x] **CONT-01**: Plugin starts required Docker containers via `docker compose` when pytest session begins
- [x] **CONT-02**: Plugin stops and removes containers via `docker compose down` when pytest session ends
- [x] **CONT-03**: Plugin performs readiness checks with retries and configurable timeout before proceeding to tests
- [x] **CONT-04**: Plugin registers atexit handlers to clean up containers on normal Python exit
- [x] **CONT-05**: Plugin registers signal handlers (SIGTERM, SIGINT) to clean up containers on interruption
- [x] **CONT-06**: Plugin detects and removes orphaned containers from previous failed runs at startup
- [x] **CONT-07**: Plugin captures container logs (httpd, DB) and attaches them to test report on failure
- [x] **CONT-08**: Plugin flushes memcache before test session starts

### Suite & Test Selection

- [x] **SUIT-01**: User can select a test suite via `--suite=<name>` (remote_dbs, sqlite, multi_vo, client, votest)
- [x] **SUIT-02**: Each suite maps to a declarative profile specifying compose profiles, services, xdist settings, and test paths/markers
- [x] **SUIT-03**: User can override infrastructure independently via `--infra=postgres14,httpd` without passing `--suite`
- [x] **SUIT-04**: Plugin filters test collection based on suite profile (include/exclude by path and marker)
- [x] **SUIT-05**: Each test run uses a unique compose project name (`rucio-test-{suite}-{rdbms}`) to avoid collisions
- [x] **SUIT-06**: User can pass `--keep-db` to skip database rebuild on subsequent runs

### Database & Bootstrap

- [x] **DBBS-01**: Plugin purges and rebuilds database schema when starting a test session (unless `--keep-db`)
- [x] **DBBS-02**: Plugin creates root account and base VO after schema build
- [x] **DBBS-03**: Plugin restarts httpd gracefully after database rebuild
- [x] **DBBS-04**: Plugin bootstraps test data (accounts, scopes, RSEs, metadata keys)
- [x] **DBBS-05**: Plugin handles SQLite (file delete), PostgreSQL, MySQL, and Oracle database lifecycle

### Parallelism

- [x] **PARA-01**: Plugin preserves and integrates existing NoParallelScheduler with disjoint-set conflict resolution
- [x] **PARA-02**: Plugin auto-disables xdist for incompatible RDBMS (sqlite, mysql, oracle)
- [x] **PARA-03**: User can configure xdist worker count via `--xdist-workers=N` CLI argument
- [x] **PARA-04**: Plugin auto-detects CI environment and sets sensible xdist defaults (3 workers on GHA, auto locally)
- [x] **PARA-05**: Plugin guards `pytest_configure` to run setup only on xdist controller, not workers

### CI Integration

- [ ] **CICD-01**: `simple-autotest.yml` workflow runs all suites using only `pytest` commands
- [ ] **CICD-02**: Local and CI test execution are identical (same pytest command, same container behavior)
- [ ] **CICD-03**: Plugin produces JUnit XML output compatible with GitHub Actions test reporting
- [ ] **CICD-04**: Type checking and syntax checking moved to a separate lint/format workflow

### Plugin Architecture

- [x] **PLUG-01**: Plugin lives in-repo at `tests/ruciopytest/` as a package
- [x] **PLUG-02**: Plugin uses zero new pip dependencies (subprocess, stdlib, existing pytest hooks only)
- [x] **PLUG-03**: Plugin reuses existing docker-compose files in `etc/docker/dev/`

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Enhanced UX

- **UX-01**: Rich progress reporting during infrastructure setup (spinner, status messages)
- **UX-02**: `--dry-run` mode showing what containers/tests would be used without executing

### Advanced Suites

- **ADVS-01**: Integration test suite support (real file transfers)
- **ADVS-02**: Votest suite auto-configuration from `matrix_policy_package_tests.yml`

## Out of Scope

| Feature | Reason |
|---------|--------|
| Per-worker container isolation | Too expensive (N stacks); NoParallelScheduler handles conflicts |
| testcontainers-style programmatic containers | Reuse existing compose files per project constraint |
| New docker-compose files | Existing files are battle-tested; avoid maintenance drift |
| Standalone installable pytest-rucio package | Tightly coupled to Rucio internals; no external users |
| Container image building from plugin | CI workflow concern, not pytest concern |
| Kubernetes/remote Docker support | Nobody needs this; target local Docker daemon only |
| Test result database/dashboard | Use JUnit XML + CI tools for visualization |
| Linting/type-checking in test plugin | Separate workflow per project decision |
| Hot-reloading containers during test runs | IDE territory, not test infrastructure |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| CONT-01 | Phase 3 | Complete |
| CONT-02 | Phase 3 | Complete |
| CONT-03 | Phase 3 | Complete |
| CONT-04 | Phase 3 | Complete |
| CONT-05 | Phase 3 | Complete |
| CONT-06 | Phase 3 | Complete |
| CONT-07 | Phase 3 | Complete |
| CONT-08 | Phase 2 | Complete |
| SUIT-01 | Phase 1 | Complete |
| SUIT-02 | Phase 1 | Complete |
| SUIT-03 | Phase 4 | Complete |
| SUIT-04 | Phase 4 | Complete |
| SUIT-05 | Phase 3 | Complete |
| SUIT-06 | Phase 2 | Complete |
| DBBS-01 | Phase 2 | Complete |
| DBBS-02 | Phase 2 | Complete |
| DBBS-03 | Phase 2 | Complete |
| DBBS-04 | Phase 2 | Complete |
| DBBS-05 | Phase 2 | Complete |
| PARA-01 | Phase 4 | Complete |
| PARA-02 | Phase 1 | Complete |
| PARA-03 | Phase 1 | Complete |
| PARA-04 | Phase 1 | Complete |
| PARA-05 | Phase 1 | Complete |
| CICD-01 | Phase 5 | Pending |
| CICD-02 | Phase 5 | Pending |
| CICD-03 | Phase 5 | Pending |
| CICD-04 | Phase 5 | Pending |
| PLUG-01 | Phase 1 | Complete |
| PLUG-02 | Phase 1 | Complete |
| PLUG-03 | Phase 1 | Complete |

**Coverage:**
- v1 requirements: 31 total
- Mapped to phases: 31
- Unmapped: 0

---
*Requirements defined: 2026-02-20*
*Last updated: 2026-02-20 after roadmap creation*
