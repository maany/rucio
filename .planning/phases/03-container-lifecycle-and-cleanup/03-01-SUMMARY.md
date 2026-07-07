---
phase: 03-container-lifecycle-and-cleanup
plan: 01
subsystem: testing
tags: [docker-compose, subprocess, signal-handling, atexit, container-lifecycle]

# Dependency graph
requires:
  - phase: 01-plugin-skeleton-and-profiles
    provides: SuiteProfile with compose_profiles field, plugin.py pytest_configure hook
  - phase: 02-database-lifecycle-and-bootstrap
    provides: InfraManager wired into plugin.py (runs after container start)
provides:
  - ContainerManager class with full Docker Compose lifecycle (up/down/readiness/orphan-cleanup)
  - Belt-and-suspenders cleanup via atexit and signal handlers
  - Container lifecycle wired into plugin.py before InfraManager
  - pytest_unconfigure hook for orderly container shutdown
affects: [03-02-log-capture, 05-ci-workflow]

# Tech tracking
tech-stack:
  added: []
  patterns: [host-side compose orchestration via subprocess, idempotent cleanup with boolean flag, signal handler save/restore pattern]

key-files:
  created: [tests/ruciopytest/container_manager.py]
  modified: [tests/ruciopytest/plugin.py]

key-decisions:
  - "ContainerManager detects in-container execution via /.dockerenv or RUCIO_SOURCE_DIR and skips compose lifecycle"
  - "start_new_session=True on compose down subprocess to prevent SIGINT propagation to cleanup process"
  - "_capture_logs is a stub pass for Plan 02 to implement"

patterns-established:
  - "Host-side Docker Compose orchestration: all compose commands run from host pytest process via subprocess"
  - "Idempotent cleanup: _cleaned_up boolean flag prevents double-cleanup between atexit, signal, and pytest_unconfigure"

requirements-completed: [PLUG-03, CONT-01, CONT-02, CONT-03, CONT-04, CONT-05, CONT-06, SUIT-05]

# Metrics
duration: 2min
completed: 2026-03-06
---

# Phase 3 Plan 1: Container Lifecycle Summary

**ContainerManager with Docker Compose up/down, orphan cleanup, httpd readiness checks, and signal/atexit cleanup handlers**

## Performance

- **Duration:** 2 min
- **Started:** 2026-03-06T11:37:04Z
- **Completed:** 2026-03-06T11:38:49Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- ContainerManager class encapsulating full Docker Compose lifecycle: orphan cleanup, compose up with --wait, httpd readiness, compose down
- Belt-and-suspenders cleanup via atexit handler, SIGTERM/SIGINT signal handlers, and pytest_unconfigure hook
- Plugin integration: containers start before InfraManager, skip when inside container or when compose_profiles is empty

## Task Commits

Each task was committed atomically:

1. **Task 1: Create ContainerManager class** - `6d01036c2` (feat)
2. **Task 2: Wire ContainerManager into plugin.py** - `27566a22b` (feat)

## Files Created/Modified
- `tests/ruciopytest/container_manager.py` - ContainerManager class with compose lifecycle, readiness, orphan cleanup, signal handlers
- `tests/ruciopytest/plugin.py` - container_manager_key stash key, ContainerManager creation in pytest_configure, pytest_unconfigure hook

## Decisions Made
- In-container detection uses `/.dockerenv` file or `RUCIO_SOURCE_DIR` env var to skip compose lifecycle when pytest runs inside a container
- `start_new_session=True` on compose down subprocess prevents SIGINT from propagating to cleanup child process (Pitfall 4)
- `_capture_logs()` left as stub (pass) for Plan 02 to implement full log capture

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- ContainerManager ready for Plan 02 to implement `_capture_logs()` with full log capture and JUnit XML integration
- STATE.md blocker resolved: all Docker operations confirmed host-side (rucio container has no Docker socket)

---
*Phase: 03-container-lifecycle-and-cleanup*
*Completed: 2026-03-06*
