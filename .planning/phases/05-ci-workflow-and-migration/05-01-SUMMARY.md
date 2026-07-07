---
phase: 05-ci-workflow-and-migration
plan: 01
subsystem: infra
tags: [github-actions, ci, pytest, junit-xml, docker]

requires:
  - phase: 01-plugin-skeleton
    provides: "pytest --suite plugin that delegates orchestration"
  - phase: 02-infra-manager
    provides: "InfraManager handles DB lifecycle and bootstrap in pytest_configure"
  - phase: 03-container-lifecycle
    provides: "ContainerManager handles compose up/down/readiness/log capture"
provides:
  - "Clean CI workflow (simple-autotest.yml) running all 6 suite variants via pytest --suite"
  - "Cleaned conftest.py with fixtures only, no lifecycle code"
affects: [05-02-PLAN]

tech-stack:
  added: [mikepenz/action-junit-report@v6, runtime_images.yml]
  patterns: [plugin-delegated-ci, matrix-based-suite-execution]

key-files:
  created: []
  modified:
    - ".github/workflows/simple-autotest.yml"
    - "tests/conftest.py"

key-decisions:
  - "Reused pinned action hashes from runtime_images.yml for consistency (checkout, login, buildx)"
  - "No backward-compatibility shims for removed lifecycle fixtures"

patterns-established:
  - "CI workflow delegates all test orchestration to pytest plugin via --suite flag"
  - "No docker compose, shell scripts, or manual container lifecycle in CI workflow"

requirements-completed: [CICD-01, CICD-02, CICD-03]

duration: 2min
completed: 2026-03-13
---

# Phase 05 Plan 01: CI Workflow and conftest Cleanup Summary

**Clean CI workflow with 6 matrix suites via pytest --suite, conftest.py stripped of all legacy lifecycle fixtures**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-13T12:01:29Z
- **Completed:** 2026-03-13T12:03:46Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Removed 3 legacy lifecycle fixtures (test_environment_setup, database_setup, rucio_bootstrap) from conftest.py
- Removed rucio_bootstrap ordering dependency from 8 client fixture signatures
- Created clean simple-autotest.yml with 6 matrix entries delegating all orchestration to the pytest plugin

## Task Commits

Each task was committed atomically:

1. **Task 1: Clean conftest.py -- remove legacy lifecycle fixtures** - `2d7164757` (refactor)
2. **Task 2: Create simple-autotest.yml workflow** - `d09ae0120` (feat)

## Files Created/Modified
- `tests/conftest.py` - Removed 3 lifecycle fixtures and rucio_bootstrap parameter from 8 client fixtures
- `.github/workflows/simple-autotest.yml` - Clean CI workflow with runtime_images.yml, pytest --suite, JUnit XML reporting

## Decisions Made
- Reused pinned action commit hashes from runtime_images.yml (checkout, login, buildx) for consistency
- No backward-compatibility shims for removed lifecycle fixtures (InfraManager handles everything)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- CI workflow ready for testing via PR push
- Plan 05-02 can proceed with migration/integration testing

## Self-Check: PASSED

All files exist, all commits verified.

---
*Phase: 05-ci-workflow-and-migration*
*Completed: 2026-03-13*
