---
phase: 01-plugin-skeleton-and-suite-profiles
plan: 02
subsystem: testing
tags: [pytest, conftest, plugin-registration, suite-profiles, xdist]

# Dependency graph
requires:
  - phase: 01-01
    provides: plugin.py with pytest_addoption/pytest_configure hooks, profiles.py, xdist_config.py
provides:
  - Plugin registered in conftest.py via pytest_plugins tuple
  - --suite option migrated from conftest.py to plugin (no collision)
  - conftest.py reads suite via plugin stash key with getoption fallback
  - SuiteProfile and SUITE_PROFILES exported from tests.ruciopytest package
affects: [02-infrastructure-lifecycle, 03-docker-compose-automation, 05-conftest-refactor]

# Tech tracking
tech-stack:
  added: []
  patterns: [pytest-plugins-registration, stash-key-suite-resolution, terminal-reporter-fallback]

key-files:
  created: []
  modified:
    - tests/conftest.py
    - tests/ruciopytest/__init__.py
    - tests/ruciopytest/plugin.py

key-decisions:
  - "Plugin registered via pytest_plugins tuple, not setuptools entry point"
  - "Suite resolved from plugin stash key with getoption fallback for backward compatibility"
  - "All getoption calls updated to use default=None for dormant mode compatibility"
  - "Terminal reporter fallback to print() when not yet available during early pytest_configure"

patterns-established:
  - "pytest_plugins tuple in conftest.py for plugin registration"
  - "config.stash[suite_profile_key] as primary suite resolution path"
  - "getoption('suite', default=None) pattern for dormant-mode safety"

requirements-completed: [PLUG-01, SUIT-01, PARA-05]

# Metrics
duration: 3min
completed: 2026-02-20
---

# Phase 1 Plan 02: Plugin Wiring and Suite Migration Summary

**Plugin registered in conftest.py via pytest_plugins, --suite migrated to plugin with stash-key resolution, and SuiteProfile/SUITE_PROFILES exported for downstream use**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-20T13:08:47Z
- **Completed:** 2026-02-20T13:11:46Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Plugin registered in conftest.py's pytest_plugins tuple alongside existing artifacts_plugin
- --suite option removed from conftest.py (now defined only in plugin), eliminating collision risk
- conftest.py reads suite via plugin's stash key with getoption fallback for backward compatibility
- SuiteProfile and SUITE_PROFILES exported from tests.ruciopytest.__init__.py for convenience

## Task Commits

Each task was committed atomically:

1. **Task 1: Register plugin and migrate --suite from conftest.py** - `e0d194eea` (feat)
2. **Task 2: Validate end-to-end plugin activation** - `969d9869b` (feat)

## Files Created/Modified
- `tests/conftest.py` - Registered plugin, removed --suite from addoption, updated suite resolution to use stash key, fixed all getoption calls for dormant mode
- `tests/ruciopytest/__init__.py` - Added SuiteProfile and SUITE_PROFILES convenience exports
- `tests/ruciopytest/plugin.py` - Fixed _print_profile_summary to handle missing terminal reporter during early pytest_configure

## Decisions Made
- Plugin registered via pytest_plugins tuple (not entry point) -- simplest approach, co-located with conftest.py
- Suite resolved from plugin stash key first, with getoption fallback for backward compatibility when plugin hasn't resolved a profile
- All fixture getoption("--suite") calls updated to use default=None to handle dormant mode (no --suite provided)
- Terminal reporter fallback: when get_terminal_writer() isn't available during early pytest_configure, fall back to plain print()

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed terminal reporter assertion error in _print_profile_summary**
- **Found during:** Task 1 (Plugin registration verification)
- **Issue:** config.get_terminal_writer() asserts terminal reporter is not None, but during early pytest_configure (called via call_historic), the terminal reporter plugin hasn't been registered yet
- **Fix:** Check pluginmanager.get_plugin("terminalreporter") first; if None, fall back to plain print() output
- **Files modified:** tests/ruciopytest/plugin.py
- **Verification:** pytest --suite=remote_dbs --co now prints profile summary without assertion error
- **Committed in:** e0d194eea (Task 1 commit)

**2. [Rule 1 - Bug] Fixed getoption calls in fixtures for dormant mode compatibility**
- **Found during:** Task 1 (reviewing conftest.py for --suite references)
- **Issue:** test_environment_setup, database_setup, and rucio_bootstrap fixtures used getoption("--suite") which would return None when plugin is dormant (old default was "remote_dbs"), causing potential TypeError on os.environ['SUITE'] = None
- **Fix:** Updated all three fixtures to use getoption("suite", default=None) with None guard
- **Files modified:** tests/conftest.py
- **Verification:** pytest --co -q (without --suite) collects tests normally without errors
- **Committed in:** e0d194eea (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for correct plugin activation. No scope creep.

## Issues Encountered
- alembic module not installed locally (only in Docker containers) causes conftest.py's database setup to fail on `from alembic import command`. This is expected and pre-existing -- tests run inside Docker where alembic is installed. Does not affect plugin activation verification.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 1 complete: plugin skeleton with suite profiles is fully wired into conftest.py
- Ready for Phase 2 (infrastructure lifecycle) which will migrate database setup logic from conftest.py to the plugin
- All 5 suite profiles accessible via --suite flag
- xdist auto-configuration active for compatible RDBMS types

## Self-Check: PASSED

All 3 modified files verified on disk. Both task commits (e0d194eea, 969d9869b) verified in git log.

---
*Phase: 01-plugin-skeleton-and-suite-profiles*
*Completed: 2026-02-20*
