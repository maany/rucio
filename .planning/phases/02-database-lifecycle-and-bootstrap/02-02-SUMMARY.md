---
phase: 02-database-lifecycle-and-bootstrap
plan: 02
subsystem: testing
tags: [pytest, database, lifecycle, plugin, conftest, cleanup]

# Dependency graph
requires:
  - phase: 02-database-lifecycle-and-bootstrap
    provides: InfraManager class with full DB lifecycle and bootstrap orchestration
provides:
  - Plugin wired to InfraManager for DB lifecycle in pytest_configure
  - Cleaned conftest.py without DB lifecycle or bootstrap helpers
affects: [03-container-orchestration, 05-conftest-refactoring]

# Tech tracking
tech-stack:
  added: []
  patterns: [lazy-import-in-conditional, guard-by-profile-name]

key-files:
  created: []
  modified: [tests/ruciopytest/plugin.py, tests/conftest.py]

key-decisions:
  - "Lazy import of InfraManager inside profile.name != client guard to avoid import-time side effects"
  - "Registered --keep-db in plugin.py rucio option group rather than conftest.py to centralize CLI options"

patterns-established:
  - "Profile-based guard: profile.name != 'client' determines whether DB lifecycle runs"
  - "Controller-only execution: InfraManager only runs in the if not is_worker block"

requirements-completed: [SUIT-06, DBBS-01, DBBS-05]

# Metrics
duration: 2min
completed: 2026-03-05
---

# Phase 02 Plan 02: Plugin-InfraManager Wiring and conftest.py Cleanup Summary

**Wired InfraManager into plugin pytest_configure with --keep-db support and removed ~370 lines of DB lifecycle code from conftest.py**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-05T15:20:23Z
- **Completed:** 2026-03-05T15:22:46Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Wired InfraManager.setup() into plugin.py pytest_configure for all non-client suites on the controller process
- Registered --keep-db CLI option in plugin.py rucio option group (removed from conftest.py)
- Removed ~370 lines of DB lifecycle code from conftest.py: pytest_configure DB block, _run_bootstrap_tests, _run_sync_rses, _run_sync_meta
- Preserved all marker registrations, xdist scheduler wiring, and existing fixtures in conftest.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Register --keep-db in plugin and wire InfraManager into pytest_configure** - `c9b7224fc` (feat)
2. **Task 2: Remove DB lifecycle code from conftest.py** - `cda040d78` (refactor)

## Files Created/Modified
- `tests/ruciopytest/plugin.py` - Added --keep-db option and InfraManager integration in pytest_configure
- `tests/conftest.py` - Removed DB lifecycle code, bootstrap helpers, and --keep-db registration

## Decisions Made
- Lazy import of InfraManager inside the `profile.name != "client"` conditional to avoid import-time side effects from rucio modules
- Registered --keep-db in the plugin's rucio option group rather than conftest.py to centralize all suite-related CLI options in the plugin

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 2 complete: InfraManager is fully wired into the pytest plugin lifecycle
- conftest.py is cleaned of all DB lifecycle code, ready for Phase 5 fixture refactoring
- Phase 3 (container orchestration) can proceed with container management integration

---
*Phase: 02-database-lifecycle-and-bootstrap*
*Completed: 2026-03-05*
