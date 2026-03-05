---
phase: 02-database-lifecycle-and-bootstrap
plan: 01
subsystem: testing
tags: [pytest, database, lifecycle, bootstrap, sqlite, postgres, rse, metadata]

# Dependency graph
requires:
  - phase: 01-plugin-skeleton-and-suite-profiles
    provides: SuiteProfile dataclass and profile registry
provides:
  - InfraManager class with full DB lifecycle and bootstrap orchestration
  - Discrete methods for each lifecycle step (purge, build, bootstrap, sync)
affects: [02-02, 03-container-orchestration, 05-conftest-refactoring]

# Tech tracking
tech-stack:
  added: []
  patterns: [lazy-imports, best-effort-vs-critical-steps, infra-manager-orchestration]

key-files:
  created: [tests/ruciopytest/infra_manager.py]
  modified: []

key-decisions:
  - "Separated _build_database and _create_base_vo_and_root_account into distinct methods for testability"
  - "Used _is_sqlite cached flag to avoid redundant engine detection across methods"
  - "Extracted _delete_sqlite_file and _purge_remote_db as private helpers for DRY purge logic"

patterns-established:
  - "Lazy imports: All rucio.* imports inside method bodies, never at module level"
  - "Best-effort vs critical: memcache flush and temp cleanup swallow exceptions; DB and bootstrap raise RuntimeError"
  - "TYPE_CHECKING guard: SuiteProfile import only under TYPE_CHECKING to avoid circular/heavy imports"

requirements-completed: [DBBS-01, DBBS-02, DBBS-03, DBBS-04, DBBS-05, CONT-08]

# Metrics
duration: 2min
completed: 2026-03-05
---

# Phase 02 Plan 01: InfraManager with DB Lifecycle and Bootstrap Summary

**InfraManager class extracting full DB lifecycle (purge, build, bootstrap, RSE/metadata sync) from conftest.py into discrete testable methods with lazy imports**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-05T15:12:18Z
- **Completed:** 2026-03-05T15:14:09Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- Extracted ~200 lines of monolithic procedural DB lifecycle code from conftest.py into a structured InfraManager class with 11 discrete methods
- All rucio imports are lazy (17 imports inside method bodies, zero at module level) preventing premature config loading
- Fixed sys import bug from conftest.py line 1227 where sys.stderr was used without importing sys
- keep_db=True provides clean short-circuit for the --keep-db CLI flag

## Task Commits

Each task was committed atomically:

1. **Task 1: Create InfraManager class with DB lifecycle methods** - `ddb508e8d` (feat)

## Files Created/Modified
- `tests/ruciopytest/infra_manager.py` - InfraManager class with full DB lifecycle orchestration (468 lines)

## Decisions Made
- Separated `_build_database` and `_create_base_vo_and_root_account` into distinct methods rather than combining them as in conftest.py, for better testability and error isolation
- Cached `_is_sqlite` flag during `_purge_database` to avoid redundant engine URL inspection in `_fix_sqlite_permissions`
- Extracted `_delete_sqlite_file` and `_purge_remote_db` as private helpers to keep `_purge_database` DRY across the three code paths (sqlite suite, remote_dbs suite, auto-detect)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- InfraManager is ready to be wired into the pytest plugin in Plan 02
- The class has a clean `setup()` entry point that Plan 02 will call from `pytest_configure`
- All methods are independently callable for future testing

---
*Phase: 02-database-lifecycle-and-bootstrap*
*Completed: 2026-03-05*
