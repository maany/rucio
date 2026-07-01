# Requirements: Rucio Test Framework Overhaul — v1.1 Productionize & Merge

**Defined:** 2026-06-15
**Core Value:** Running any test suite should be a single `pytest` command with arguments — no shell scripts, no matrix parsers, no manual container management.

## v1.1 Requirements

Requirements for the productionize-and-merge milestone. Delivery (history reconstruction
+ stacked PRs) is a final manual step, not a code requirement — see `.planning/v1.1-DESIGN.md`.

### Suite Parity

- [x] **SUIT-07**: `pytest --suite=votest` selects the POLICY-specific test set (the legacy `votest_helper.py` + `matrix_policy_package_tests.yml` selection), not all of `tests/`
- [x] **SUIT-08**: `pytest --suite=multi_vo` runs the multi-VO test selection with the 2-VO configuration, matching legacy `test.sh`/`run_multi_vo_tests_docker.sh`
- [x] **SUIT-09**: `client`, `remote_dbs`, and `sqlite` selections match legacy `test.sh`, with an automated parity guard against silent drift

### CI Execution

- [x] **CICD-05**: `simple-autotest.yml` installs host Python and plugin dependencies (setup-python + pytest/pytest-xdist) so every matrix leg starts (closes carried CICD-01)
- [x] **CICD-06**: the host-side `client` suite gets a full rucio install on the runner so it executes (closes carried CICD-02)
- [x] **CICD-07**: CI uploads junit XML artifacts and renders a per-leg test report for every matrix entry
- [x] **CICD-08**: all 5 `simple-autotest.yml` matrix legs pass green on a PR

### Documentation

- [x] **DOC-01**: `tests/ruciopytest/README.md` documents the suite table and all CLI options (`--suite`, `--run-in-container`/`--no-run-in-container`, `--container-env`, `--keep-db`, `--infra`, `--dry-run`/`--co`) with usage examples

## Out of Scope

| Feature | Reason |
|---------|--------|
| Upstream rucio/rucio PRs | Fork-first; PRs go to maany/rucio in v1.1 |
| New plugin capabilities beyond legacy parity | v1.1 is productionization, not new features |
| Changing test logic / test cases | Only the infrastructure around them |
| Rewriting docker-compose files | Reuse existing |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| SUIT-07 | Phase 7 | Complete |
| SUIT-08 | Phase 7 | Complete |
| SUIT-09 | Phase 7 | Complete |
| CICD-05 | Phase 8 | Complete |
| CICD-06 | Phase 8 | Complete |
| CICD-07 | Phase 8 | Complete |
| CICD-08 | Phase 8 | Complete |
| DOC-01 | Phase 9 | Complete |

**Coverage:**
- v1.1 requirements: 8 total
- Mapped to phases: 8 (provisional — roadmapper confirms)
- Unmapped: 0

---
*Requirements defined: 2026-06-15*
*Last updated: 2026-06-15 after v1.1 milestone start*
