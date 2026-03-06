---
phase: 03-container-lifecycle-and-cleanup
plan: 02
subsystem: testing
tags: [docker-compose, log-capture, pytest-terminal-summary, junitxml]

# Dependency graph
requires:
  - phase: 03-container-lifecycle-and-cleanup
    provides: ContainerManager with _capture_logs stub and LOG_DIR constant
provides:
  - Full _capture_logs implementation saving per-service and combined logs to .test-logs/
  - pytest_terminal_summary hook printing log file paths and adding JUnit XML properties
  - log_dir property on ContainerManager for external access
affects: [05-ci-workflow]

# Tech tracking
tech-stack:
  added: []
  patterns: [best-effort log capture with graceful error handling, pytest terminal summary for CI visibility]

key-files:
  created: []
  modified: [tests/ruciopytest/container_manager.py, tests/ruciopytest/plugin.py]

key-decisions:
  - "Log capture is best-effort: all errors caught and printed as warnings, never raises"
  - "Combined log uses project_name as filename, per-service logs use service name"
  - "JUnit XML integration uses add_global_property with container_log: prefix"

patterns-established:
  - "Best-effort capture pattern: wrap each subprocess call individually so one failure does not block others"
  - "pytest_terminal_summary for post-test visibility: log paths printed in yellow for developer attention"

requirements-completed: [CONT-07]

# Metrics
duration: 1min
completed: 2026-03-06
---

# Phase 3 Plan 2: Log Capture and Reporting Summary

**Container log capture to .test-logs/ with per-service files and pytest terminal summary for debugging visibility**

## Performance

- **Duration:** 1 min
- **Started:** 2026-03-06T11:41:08Z
- **Completed:** 2026-03-06T11:42:18Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Replaced _capture_logs stub with full implementation: combined log (all services) and per-service individual log files saved to .test-logs/
- Added log_dir property to ContainerManager for external access without exposing internal constant
- Added pytest_terminal_summary hook that prints log file paths in yellow and attaches to JUnit XML as global properties

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement _capture_logs in ContainerManager** - `11a1c3c67` (feat)
2. **Task 2: Add pytest_terminal_summary hook** - `b41a9d09f` (feat)

## Files Created/Modified
- `tests/ruciopytest/container_manager.py` - Full _capture_logs implementation with combined and per-service logs, log_dir property
- `tests/ruciopytest/plugin.py` - pytest_terminal_summary hook for log visibility and JUnit XML integration

## Decisions Made
- Log capture is best-effort: each subprocess call wrapped individually so one service failure does not block other service log captures
- Combined log named after project_name (e.g. rucio-test-remote_dbs-postgres14.log), per-service logs named after service (e.g. rucio.log, postgres14.log)
- JUnit XML properties use `container_log:{filename}` key prefix for structured access

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 3 (Container Lifecycle and Cleanup) fully complete
- Container logs will be available in CI via JUnit XML properties and terminal output
- Ready for Phase 4 (Collection and Parallelism) or Phase 5 (CI Workflow)

---
*Phase: 03-container-lifecycle-and-cleanup*
*Completed: 2026-03-06*
