---
phase: 04-test-collection-and-parallelism-integration
plan: 01
subsystem: testing
tags: [pytest, collection-hooks, fnmatch, cli-options, dry-run, infrastructure]

requires:
  - phase: 01-plugin-skeleton-and-suite-resolution
    provides: SuiteProfile dataclass, SUITE_PROFILES registry, plugin.py hooks, stash keys
provides:
  - Suite-aware test collection filtering via pytest_collection_modifyitems
  - --infra CLI option with service-to-profile resolution
  - --dry-run and --dry-run-json for plan inspection
  - Overlap detection warnings for multi-suite test matches
  - _resolve_infra and _infer_suites_from_infra helpers
affects: [04-02, 05-ci-migration]

tech-stack:
  added: [fnmatch, json]
  patterns: [collection-hook-filtering, service-to-profile-map, stash-keys-in-init]

key-files:
  created:
    - tests/ruciopytest/collection.py
  modified:
    - tests/ruciopytest/profiles.py
    - tests/ruciopytest/plugin.py
    - tests/ruciopytest/__init__.py

key-decisions:
  - "Moved stash keys (suite_profile_key, container_manager_key, delegate_to_container_key) from plugin.py to __init__.py to avoid circular imports between plugin.py and collection.py"
  - "--dry-run returns early in pytest_configure after storing profile and configuring xdist, skipping entire container lifecycle"
  - "--infra without --suite creates synthetic merged profile from union of all matching suite test_paths/markers/exclude_paths"
  - "xdist disabled note always printed when xdist_enabled is False, not just when markers present"

patterns-established:
  - "Stash keys in __init__.py: shared stash keys live in the package init to prevent circular imports between plugin modules"
  - "Collection hook registration via pluginmanager.register: collection.py registered as 'rucio_collection' plugin in pytest_configure"
  - "Service-to-profile map: _SERVICE_TO_PROFILE dict maps docker-compose service names to their compose profile names"

requirements-completed: [SUIT-04, SUIT-03]

duration: 4min
completed: 2026-03-11
---

# Phase 4 Plan 01: Test Collection and Parallelism Integration Summary

**Suite-aware test collection filtering with path/marker matching, --infra service resolution, --dry-run plan inspection, and overlap detection warnings**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-11T14:38:52Z
- **Completed:** 2026-03-11T14:42:49Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Created collection.py with full suite-aware test filtering via pytest_collection_modifyitems
- Implemented --infra CLI option that resolves service names to compose profiles and infers matching suites
- Added --dry-run and --dry-run-json for infrastructure plan and collection inspection without starting containers
- Extended SuiteProfile with exclude_paths field for path exclusion patterns
- Overlap warnings detect and report tests matching multiple suite profiles

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend SuiteProfile and create collection.py** - `da9026d81` (feat)
2. **Task 2: Register --infra and --dry-run options in plugin.py** - `be61bbdb7` (feat)

## Files Created/Modified
- `tests/ruciopytest/collection.py` - Suite-aware test collection filtering, infra resolution, dry-run reports, overlap detection
- `tests/ruciopytest/profiles.py` - Added exclude_paths field to SuiteProfile, updated resolve_profile
- `tests/ruciopytest/plugin.py` - Added --infra, --dry-run, --dry-run-json CLI options; --infra suite inference; dry-run lifecycle skip; collection hook registration
- `tests/ruciopytest/__init__.py` - Moved stash keys here from plugin.py for shared access

## Decisions Made
- Moved stash keys to __init__.py to avoid circular imports (collection.py needs suite_profile_key, plugin.py defines it -- resolution: shared location)
- --dry-run exits early in pytest_configure after profile resolution and xdist config, before any container operations
- --infra without --suite creates a synthetic merged profile combining all matching suites' test_paths/markers/exclude_paths
- xdist disabled note printed unconditionally when xdist_enabled is False (applies to sqlite, oracle, mysql8 suites)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Collection filtering ready for Phase 4 Plan 02 (scheduler enhancement)
- All CLI options registered and wired
- collection.py hooks properly registered via pluginmanager

---
*Phase: 04-test-collection-and-parallelism-integration*
*Completed: 2026-03-11*
