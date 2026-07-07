---
phase: 03-container-lifecycle-and-cleanup
plan: 03
subsystem: testing
tags: [docker-compose, signal-handling, container-lifecycle, cleanup]

# Dependency graph
requires:
  - phase: 03-container-lifecycle-and-cleanup (plans 01-02)
    provides: ContainerManager class, plugin wiring, log capture
provides:
  - Signal handlers registered before blocking Docker operations
  - Same-name orphan detection and removal with correct compose files
  - Client suite skips container lifecycle entirely
affects: [04-collection-and-parallelism, 05-ci-migration]

# Tech tracking
tech-stack:
  added: []
  patterns: [signal-before-blocking, inclusive-orphan-cleanup]

key-files:
  created: []
  modified:
    - tests/ruciopytest/container_manager.py
    - tests/ruciopytest/profiles.py

key-decisions:
  - "Removed same-name exclusion from orphan filter since cleanup runs before compose up"
  - "Signal handlers registered immediately after env setup, before any Docker operations"
  - "Client suite uses empty compose_profiles tuple to skip container lifecycle"

patterns-established:
  - "Signal handlers must be registered before any blocking subprocess calls"
  - "Orphan cleanup includes same-name projects (safe because it runs pre-startup)"

requirements-completed: [CONT-03, CONT-04, SUIT-05]

# Metrics
duration: 1min
completed: 2026-03-09
---

# Phase 03 Plan 03: UAT Gap Closure Summary

**Signal handler early registration, inclusive orphan cleanup with ConfigFiles, and client suite container bypass**

## Performance

- **Duration:** 1 min
- **Started:** 2026-03-09T20:07:17Z
- **Completed:** 2026-03-09T20:08:26Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Signal handlers now active before any blocking Docker operations (compose up, readiness check)
- Same-name orphan projects are cleaned up on re-runs, with correct `-f` compose file flags
- Client suite profile has empty `compose_profiles`, correctly skipping container lifecycle

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix signal handler registration and orphan cleanup** - `bae9df226` (fix)
2. **Task 2: Fix client suite profile to skip container lifecycle** - `2cef57053` (fix)

## Files Created/Modified
- `tests/ruciopytest/container_manager.py` - Reordered start() method; fixed orphan filter and down command
- `tests/ruciopytest/profiles.py` - Changed client suite compose_profiles from ("client",) to ()

## Decisions Made
- Removed same-name exclusion from orphan filter -- safe because _cleanup_orphans() runs before _compose_up()
- Added ConfigFiles parsing from `docker compose ls --format json` output to pass `-f` flags during orphan removal
- Set `self._started = True` between compose_up and wait_for_readiness so interrupted readiness checks still trigger cleanup

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All three UAT gaps (signal cleanup, orphan detection, client suite) are closed
- Phase 03 container lifecycle is now complete
- Ready for Phase 04 (collection and parallelism)

---
*Phase: 03-container-lifecycle-and-cleanup*
*Completed: 2026-03-09*
