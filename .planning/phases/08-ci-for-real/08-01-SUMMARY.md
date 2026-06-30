---
phase: 08-ci-for-real
plan: 01
subsystem: testing
tags: [pytest, suite-profiles, parity-guard, ci, sqlite-descope]

# Dependency graph
requires:
  - phase: 07-suite-filtering-parity
    provides: parity_baselines.json + import-free drift guard (test_parity.py), SUITE_PROFILES registry
provides:
  - sqlite-free plugin suite registry (no "sqlite" in SUITE_PROFILES or --suite choices)
  - sqlite-free parity baseline + guard (loops over remote_dbs/multi_vo only)
  - /test-results/ git-ignored as legitimate CI junit output
  - ROADMAP/REQUIREMENTS aligned to a 5-leg green bar (CICD-06 host-side client only, CICD-08 5 legs)
affects: [08-02, 08-03, 08-04]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Generic RDBMS plumbing (rdbms_override != 'sqlite') stays decoupled from the named suite registry"

key-files:
  created:
    - .planning/phases/08-ci-for-real/08-01-SUMMARY.md
  modified:
    - tests/ruciopytest/profiles.py
    - tests/ruciopytest/plugin.py
    - tests/ruciopytest/parity_baselines.json
    - tests/ruciopytest/test_parity.py
    - .gitignore
    - .planning/ROADMAP.md
    - .planning/REQUIREMENTS.md

key-decisions:
  - "Removed sqlite only from the plugin/parity ecosystem; legacy autotest CI keeps running sqlite (untouched)"
  - "Kept generic rdbms_override != 'sqlite' backend branch in resolve_profile (generic plumbing, not the suite)"
  - "Left SUIT-09 requirement text naming sqlite untouched (completed Phase-7 historical statement)"

patterns-established:
  - "Suite descope = registry + --suite choices + parity baseline + guard loop, in one atomic change"

requirements-completed: [CICD-06, CICD-08]

# Metrics
duration: 6min
completed: 2026-06-30
---

# Phase 8 Plan 01: sqlite Descope + 5-Leg Docs Summary

**Removed sqlite as a selectable plugin suite (registry, --suite choices, parity baseline + guard) and aligned ROADMAP/REQUIREMENTS to a 5-leg green bar, with /test-results/ now git-ignored.**

## Performance

- **Duration:** ~6 min
- **Started:** 2026-06-30T12:54Z
- **Completed:** 2026-06-30T13:01Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Deleted the `sqlite` SuiteProfile from `SUITE_PROFILES` and dropped `sqlite` from the `--suite` choices in plugin.py
- Removed the top-level `sqlite` key from `parity_baselines.json` (still valid JSON) and narrowed the `test_baseline_parity` loop to `(remote_dbs, multi_vo)`
- Parity guard remains green (4 passed) and still bites on drift (`test_drift_detected`)
- Added `/test-results/` to `.gitignore`; aligned ROADMAP Phase 8 goal and REQUIREMENTS CICD-06/CICD-08 to 5 legs (no sqlite)
- Confirmed legacy `.github/workflows/autotest.yml` and `tools/test/test.sh` are untouched (sqlite still runs there)

## Task Commits

1. **Task 1: Remove sqlite suite from plugin registry, choices, and parity guard** - `4c5fc7547` (feat)
2. **Task 2: Ignore test-results/ and 5-leg docs (ROADMAP/REQUIREMENTS)** - `e3975bf71` (docs)
3. **Task 2 follow-up: commit dropped .gitignore rule** - `bca2a2a76` (chore)

_Note: a concurrent 08-02 executor interleaved commits (`effb8554a`, `b433b9202`) and a stat-cache race silently dropped `.gitignore` from the e3975bf71 commit; it was re-committed atomically as bca2a2a76._

## Files Created/Modified
- `tests/ruciopytest/profiles.py` - Removed `sqlite` SuiteProfile; dropped sqlite from resolve_profile docstring (kept generic `rdbms_override != "sqlite"` branch)
- `tests/ruciopytest/plugin.py` - Removed `"sqlite"` from `--suite` choices
- `tests/ruciopytest/parity_baselines.json` - Deleted top-level `sqlite` key
- `tests/ruciopytest/test_parity.py` - `test_baseline_parity` loop now `(remote_dbs, multi_vo)`
- `.gitignore` - Added `/test-results/`
- `.planning/ROADMAP.md` - Phase 8 goal: all 6 -> all 5 matrix legs
- `.planning/REQUIREMENTS.md` - CICD-06 host-side `client` only (no sqlite); CICD-08 all 5 legs

## Decisions Made
- Kept the generic `rdbms_override != "sqlite"` plumbing and the sqlite comment in profiles.py: that is backend handling, not the named suite (per plan scope).
- Left SUIT-09's requirement text (which legitimately names sqlite as a completed Phase-7 historical statement) untouched.

## Deviations from Plan

None - plan executed exactly as written. (The `.gitignore` re-commit was an environment/concurrency artifact, not a plan deviation.)

## Issues Encountered
- A concurrent 08-02 executor was committing in parallel; a git index stat-cache race dropped the `.gitignore` change from the Task 2 commit. Detected via post-commit verification (`git show HEAD:.gitignore`), re-staged with `git add`, and committed atomically as `bca2a2a76`. Verified present in HEAD.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Plugin and parity guard now agree there is no sqlite suite: 08-02 (5-leg matrix workflow) and 08-04 (live green verification) can build on a faithful 5-leg matrix.
- No blockers.

---
*Phase: 08-ci-for-real*
*Completed: 2026-06-30*

## Self-Check: PASSED

All modified files present; all task commits (4c5fc7547, e3975bf71, bca2a2a76) exist in history.
