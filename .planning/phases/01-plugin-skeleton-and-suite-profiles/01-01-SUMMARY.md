---
phase: 01-plugin-skeleton-and-suite-profiles
plan: 01
subsystem: testing
tags: [pytest, dataclass, xdist, plugin, suite-profiles]

# Dependency graph
requires: []
provides:
  - SuiteProfile frozen dataclass with RDBMS, compose, xdist, test path fields
  - SUITE_PROFILES registry with all 5 suites (remote_dbs, sqlite, multi_vo, client, votest)
  - resolve_profile function with RDBMS override and xdist recalculation
  - configure_xdist function with CI detection and RDBMS compatibility
  - pytest_addoption hook with --suite and --xdist-workers options
  - pytest_configure hook with worker guard, stash storage, profile summary
affects: [01-02, 02-infrastructure-lifecycle, 03-docker-compose-automation]

# Tech tracking
tech-stack:
  added: []
  patterns: [frozen-dataclass-profiles, stashkey-state-storage, xdist-worker-guard, terminal-writer-output]

key-files:
  created:
    - tests/ruciopytest/profiles.py
    - tests/ruciopytest/xdist_config.py
    - tests/ruciopytest/plugin.py
  modified: []

key-decisions:
  - "Only postgres14 is xdist-compatible; sqlite, oracle, mysql8 auto-disable xdist"
  - "Plugin dormant when --suite not provided (no default suite)"
  - "Workers resolve profile independently but skip xdist config and summary printing"
  - "SUITE env var set for backward compatibility with existing test code"

patterns-established:
  - "Frozen dataclass for immutable suite configuration"
  - "pytest.StashKey[SuiteProfile] for type-safe state sharing"
  - "hasattr(config, 'workerinput') guard in pytest_configure"
  - "config.get_terminal_writer() for plugin output"

requirements-completed: [PLUG-01, PLUG-02, SUIT-01, SUIT-02, PARA-02, PARA-03, PARA-04, PARA-05]

# Metrics
duration: 2min
completed: 2026-02-20
---

# Phase 1 Plan 01: Plugin Skeleton and Suite Profiles Summary

**Frozen SuiteProfile dataclass with 5-suite registry, xdist auto-configuration with RDBMS compatibility, and pytest plugin hooks with --suite/--xdist-workers CLI options**

## Performance

- **Duration:** 2 min
- **Started:** 2026-02-20T13:04:39Z
- **Completed:** 2026-02-20T13:06:28Z
- **Tasks:** 2
- **Files created:** 3

## Accomplishments
- SuiteProfile frozen dataclass with all required fields (name, rdbms, compose_profiles, xdist_enabled, default_workers_ci, default_workers_local, test_paths, markers, env_vars)
- All 5 suite profiles defined with correct RDBMS and xdist settings in SUITE_PROFILES registry
- resolve_profile function handles RDBMS override with automatic xdist recalculation
- xdist auto-configuration with CI detection (GITHUB_ACTIONS, CI env vars), RDBMS compatibility check, and explicit worker override
- Plugin hooks: pytest_addoption (--suite, --xdist-workers) and pytest_configure with xdist worker guard

## Task Commits

Each task was committed atomically:

1. **Task 1: Create SuiteProfile dataclass and profile registry** - `3a8506a9c` (feat)
2. **Task 2: Create xdist configuration module and main plugin module** - `a2b628fd3` (feat)

## Files Created/Modified
- `tests/ruciopytest/profiles.py` - SuiteProfile frozen dataclass, SUITE_PROFILES registry (5 suites), resolve_profile with RDBMS override
- `tests/ruciopytest/xdist_config.py` - configure_xdist: auto-disable for incompatible RDBMS, CI detection, explicit worker override
- `tests/ruciopytest/plugin.py` - pytest_addoption (--suite, --xdist-workers), pytest_configure with worker guard, stash storage, profile summary

## Decisions Made
- Only postgres14 is xdist-compatible; sqlite, oracle, mysql8 auto-disable xdist (based on CI matrix analysis)
- Plugin is dormant when --suite is not provided (no default suite name)
- xdist workers resolve the profile independently and store it in stash, but skip xdist configuration and summary printing
- SUITE env var set in os.environ for backward compatibility with existing test code that checks it
- Used config.get_terminal_writer() with sep() for profile summary (cleaner than manual box drawing)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- pytest was not installed in the local Python environment, requiring pip install for verification. This is not a project issue (tests run inside Docker containers where pytest is installed).

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All three modules (profiles.py, xdist_config.py, plugin.py) are importable and tested
- Ready for Plan 02 which will wire the plugin into conftest.py via pytest_plugins and migrate --suite option
- configure_xdist is a standalone function ready to be called from pytest_configure

## Self-Check: PASSED

All 3 created files verified on disk. Both task commits (3a8506a9c, a2b628fd3) verified in git log.

---
*Phase: 01-plugin-skeleton-and-suite-profiles*
*Completed: 2026-02-20*
