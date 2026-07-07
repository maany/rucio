---
phase: 07-suite-filtering-parity
plan: 01
subsystem: testing
tags: [pytest, votest, policy, configparser, pyyaml, matrix_policy_package_tests]

# Dependency graph
requires:
  - phase: 01-foundation
    provides: SuiteProfile dataclass, resolve_profile, pytest plugin skeleton
  - phase: 02-infra-manager
    provides: InfraManager.setup lifecycle (httpd restart hook point)
  - phase: 04-collection
    provides: collection path-filter that deselects non-matching test_paths
provides:
  - votest_support.py (load_matrix, collect_votest_paths, rewrite_policy_section, resolve_policy)
  - SuiteProfile.policy optional field carried through all reconstructions
  - plugin wiring of --policy/POLICY into votest test_paths at configure time
  - InfraManager._apply_votest_policy in-container [policy] rewrite before httpd restart
affects: [08-ci-for-real, multi_vo-parity, parity-guard]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "votest split: selection computed at pytest_configure (host/container), cfg rewrite at InfraManager.setup (in-container)"
    - "policy field threaded through every SuiteProfile reconstruction so it is never dropped"
    - "data-driven policy validation from YAML keys (no hardcoded policy list)"

key-files:
  created:
    - tests/ruciopytest/votest_support.py
    - tests/ruciopytest/test_votest_support.py
    - tests/ruciopytest/test_plugin_votest.py
  modified:
    - tests/ruciopytest/profiles.py
    - tests/ruciopytest/plugin.py
    - tests/ruciopytest/infra_manager.py

key-decisions:
  - "Reimplemented collect_tests/persist_config_overrides in votest_support.py (absorb, not import legacy votest_helper)"
  - "Emit repo-relative tests/test_X.py paths (not /opt/rucio/...) so the collection matcher matches"
  - "No policy-package pip install (live CI installs none; cfg rewrite only) — reconciled CONTEXT wording to CI reality"
  - "votest selection computed once after profile finalization, before stash set, regardless of branch"

patterns-established:
  - "Pattern: frozen SuiteProfile mutated via dataclasses.replace at configure time"
  - "Pattern: in-container-only infra side effects gated by profile.name + profile.policy"

requirements-completed: [SUIT-07]

# Metrics
duration: 8min
completed: 2026-06-23
---

# Phase 7 Plan 01: votest filtering parity Summary

**`pytest --suite=votest --policy=atlas|belleii` now selects exactly the legacy YAML-derived test set (atlas=36, belleii=52 repo-relative files) and rewrites the in-container rucio.cfg `[policy]` section before httpd restart.**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-23T10:52:45Z
- **Completed:** 2026-06-23T11:00:xx Z
- **Tasks:** 4
- **Files modified:** 6 (3 created, 3 modified)

## Accomplishments
- New `votest_support.py` absorbing `collect_tests` + `persist_config_overrides`: `collect_votest_paths` reproduces CI ground-truth (atlas=36, belleii=52) with the parity-critical `is_file()` drop, emitting repo-relative paths.
- `SuiteProfile.policy` optional field threaded through all three SuiteProfile reconstructions (resolve_profile rdbms-override, plugin --suite+--infra, plugin --infra-only).
- `--policy` flag (wins over `POLICY` env) wired into `pytest_configure`: votest builds explicit `test_paths` from the matrix YAML; missing/unknown policy raises a clear `UsageError` listing available policies.
- `InfraManager._apply_votest_policy` rewrites the live `[policy]` section before httpd restart, for votest only, with no policy-package pip install.

## Task Commits

1. **Task 1: votest_support.py + unit tests** - `94eead375` (feat)
2. **Task 2: SuiteProfile.policy field + passthrough** - `b30b8a5d6` (feat)
3. **Task 3: --policy/POLICY plugin wiring + InfraManager rewrite** - `0574e9ec9` (feat)
4. **Task 4: end-to-end configure->stash wiring guard** - `b4c8ed883` (test)

## Files Created/Modified
- `tests/ruciopytest/votest_support.py` - load_matrix, collect_votest_paths, rewrite_policy_section, resolve_policy
- `tests/ruciopytest/test_votest_support.py` - counts (36/52), drop, rewrite, policy resolution unit tests
- `tests/ruciopytest/test_plugin_votest.py` - drives pytest_configure votest path with a fake Config; asserts 36 stashed atlas paths + missing-policy UsageError
- `tests/ruciopytest/profiles.py` - added `policy` field + resolve_profile passthrough
- `tests/ruciopytest/plugin.py` - `--policy` option, policy carried through reconstructions, votest test_paths injection
- `tests/ruciopytest/infra_manager.py` - `_apply_votest_policy` hook before `_restart_httpd`

## Decisions Made
- Absorbed (reimplemented) the legacy logic rather than importing `votest_helper.py`, per CONTEXT lock.
- Computed votest selection once after profile finalization to be branch-agnostic.
- Implemented cfg-rewrite only (no policy-package install), reconciling CONTEXT's "package install" wording against verified CI reality (RESEARCH Pitfall 3).

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None. The `Optional` type used by the new `policy` field is referenced as a string annotation, so the existing `TYPE_CHECKING`-only import is sufficient at runtime (verified).

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- votest selection + policy setup are parity-faithful and unit-guarded (47->49 plugin tests green).
- Ready for: multi_vo parity (SUIT-08) and the checked-in parity baseline guard (SUIT-09), which can reuse `collect_votest_paths` directly.

## Self-Check: PASSED

All created files present; all 4 task commits found.

---
*Phase: 07-suite-filtering-parity*
*Completed: 2026-06-23*
