---
phase: 12-multi-vo-legacy-parity
plan: 01
subsystem: testing
tags: [ci, github-actions, multi_vo, pytest, rucio]

# Dependency graph
requires:
  - phase: 08.1-multi-vo-parallel-legs
    provides: "The split multi_vo-tst/multi_vo-ts2 parallel matrix legs being reverted here"
  - phase: 07-suite-parity
    provides: "run_multi_vo() sequential shared-DB branch (infra_manager.py:402-428) reused unchanged"
provides:
  - "Single multi_vo matrix leg (no vo:) in simple-autotest.yml -> RUCIO_MULTI_VO_LEG unset -> sequential shared-DB path"
  - "README doc sync: single-leg sequential model, CI-mapping table + parity notes"
affects: [12-02-live-verification]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "CI multi_vo runs as one leg with RUCIO_MULTI_VO_LEG unset for legacy shared-DB parity; tst/ts2 remain a local/dev single-VO override"

key-files:
  created: []
  modified:
    - .github/workflows/simple-autotest.yml
    - tests/ruciopytest/README.md

key-decisions:
  - "Reverted 8.1 parallel split by removing vo: field entirely rather than hardcoding a leg selector, so run_multi_vo() falls through to the existing sequential branch with zero infra_manager.py changes"
  - "Kept the RUCIO_MULTI_VO_LEG: ${{ matrix.vo }} env line (resolves empty) as the single source of truth matching the infra_manager contract; only its comment changed"

patterns-established:
  - "junit/report/artifact names key off matrix.leg, so leg: multi_vo auto-reverts naming to non-split multi_vo-py3.9.* with no hardcoding"

requirements-completed: [SUIT-08]

# Metrics
duration: 4min
completed: 2026-07-06
---

# Phase 12 Plan 01: Revert multi_vo Matrix Split Summary

**Collapsed 8.1's parallel multi_vo-tst/multi_vo-ts2 matrix legs into one `multi_vo` leg (no `vo:` field) so `RUCIO_MULTI_VO_LEG` resolves empty and `run_multi_vo()` takes the legacy sequential shared-DB path, plus README doc sync.**

## Performance

- **Duration:** 4 min
- **Started:** 2026-07-06
- **Completed:** 2026-07-06
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Removed the two parallel per-VO legs (`multi_vo-tst` / `multi_vo-ts2`) from `simple-autotest.yml`, leaving one `multi_vo` leg with no `vo:` field (suite=multi_vo, py3.9, postgres14)
- The single leg leaves `RUCIO_MULTI_VO_LEG` unset -> `run_multi_vo()` sequential shared-DB branch (tst first, ts2 only on tst success, no DB reset)
- junit/report/artifact names revert to non-split `multi_vo-py3.9.*` automatically via `matrix.leg` (no hardcoding)
- Synced `tests/ruciopytest/README.md`: reframed `RUCIO_MULTI_VO_LEG` as a local/dev override, collapsed the CI-mapping table to a single `multi_vo` row, rewrote the parity note to describe the sequential shared-DB single leg with the accepted ~35 min long-pole
- `infra_manager.py` and the single-leg selector unit tests untouched (still supported for local/dev)

## Task Commits

Each task was committed atomically:

1. **Task 1: Collapse the multi_vo matrix split into a single sequential leg** - `d8dbd6104` (refactor)
2. **Task 2: Sync README to the reverted single-leg sequential model** - `fb7500776` (docs)

## Files Created/Modified
- `.github/workflows/simple-autotest.yml` - Replaced two parallel multi_vo legs with one `leg: multi_vo` (no `vo:`); updated the `RUCIO_MULTI_VO_LEG` env comment
- `tests/ruciopytest/README.md` - Reframed multi_vo doc para, CI-mapping table row, and parity note to the single sequential shared-DB model

## Decisions Made
- Reverted the split by dropping the `vo:` field entirely (rather than adding an explicit selector), so the empty `RUCIO_MULTI_VO_LEG` falls through to the pre-existing sequential branch — no `infra_manager.py` change needed
- Retained the `RUCIO_MULTI_VO_LEG: ${{ matrix.vo }}` env line as single source of truth (harmless empty resolution); only the comment changed

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- The active shell python was a foreign project venv without PyYAML/pip; used `/usr/bin/python3` (which has PyYAML) for the YAML-parse verification. No impact on the change itself.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Structural revert complete; sets up Plan 12-02 to live-CI verify the single multi_vo leg green and prove the sequential branch via logs ("Running tests for VO tst" -> "ts2", no reset)

## Self-Check: PASSED

---
*Phase: 12-multi-vo-legacy-parity*
*Completed: 2026-07-06*
