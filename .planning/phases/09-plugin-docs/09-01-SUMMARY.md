---
phase: 09-plugin-docs
plan: 01
subsystem: testing
tags: [documentation, pytest, ruciopytest, ci, docker, multi-vo]

# Dependency graph
requires:
  - phase: 08.1-ci-multi-vo
    provides: post-8.1 CI matrix (multi_vo split tst/ts2, forwarded xdist) that the README documents
provides:
  - tests/ruciopytest/README.md — onboarding docs for the ruciopytest plugin (quickstart, how-it-works, suite table, CLI reference, per-flag examples, CI mapping, troubleshooting)
affects: [contributor onboarding, future plugin CLI/suite changes must keep README in sync]

# Tech tracking
tech-stack:
  added: []
  patterns: [grounded-docs: every documented flag/suite/leg verified against plugin.py/profiles.py/forwarding.py/simple-autotest.yml]

key-files:
  created: [tests/ruciopytest/README.md]
  modified: []

key-decisions:
  - "Quickstart uses the client suite (host-side, no container wait) as the fastest first run"
  - "Output shown only for --co and --dry-run, marked illustrative, to avoid staleness"
  - "Dev-environment setup linked out (relative link to etc/docker/dev + CONTRIBUTING) rather than re-explained"

patterns-established:
  - "Grounded docs: README flags/suites/CI legs are traceable to source files, not memory"

requirements-completed: [DOC-01]

# Metrics
duration: 2min
completed: 2026-07-01
---

# Phase 9 Plan 01: ruciopytest Plugin Documentation Summary

**Single `tests/ruciopytest/README.md` (363 lines) teaching contributors to run every suite via `pytest --suite=...` — quickstart, host-vs-forwarded mental model, 4-row suite table, 10-flag CLI reference, per-flag examples, post-8.1 CI leg→command mapping, and troubleshooting.**

## Performance

- **Duration:** 2 min
- **Started:** 2026-07-01T11:15:35Z
- **Completed:** 2026-07-01T11:17:35Z
- **Tasks:** 2
- **Files modified:** 1 (created)

## Accomplishments
- Quickstart-first README that gets a new contributor running the `client` suite in one command, with the dormant-plugin caveat.
- "How it works" section covering the four load-bearing concepts before the reference: host vs container forwarding, DB purge/rebuild/seed lifecycle, multi-VO (RUCIO_MULTI_VO_LEG), and forwarded xdist.
- Complete CLI reference table for all 10 `pytest_addoption` flags plus per-flag Examples section (host-side, forwarded, `--keep-db`, `--infra`, `--container-env`, `--policy`, `--co`, `--dry-run`).
- CI mapping table for all 6 post-Phase-8.1 matrix legs (multi_vo split tst/ts2) with the env each sets and the equivalent local command, plus a `test.sh` migration note.

## Task Commits

Each task was committed atomically:

1. **Task 1: Quickstart, How it works, Suite table, CLI reference** - `a3f5266bc` (docs)
2. **Task 2: Examples, CI mapping, test.sh migration, Troubleshooting** - `1f277edd1` (docs)

**Plan metadata:** committed separately (docs: complete plan)

## Files Created/Modified
- `tests/ruciopytest/README.md` - Complete ruciopytest plugin usage documentation (363 lines)

## Decisions Made
- Quickstart uses `client` (host-side, no container startup) as the fastest first run.
- Representative output shown only for `--co` and `--dry-run`, explicitly marked illustrative, to avoid snapshot staleness.
- Dev-environment / Docker setup linked out (relative `../../etc/docker/dev` + CONTRIBUTING) rather than re-explained, per audience assumption.

## Deviations from Plan

None - plan executed exactly as written. Both task verification blocks passed on first run; all flags, suites, and CI legs reconciled with the source files (plugin.py, profiles.py, forwarding.py, multi_vo_support.py, infra_manager.py, simple-autotest.yml, test.sh).

## Issues Encountered
None.

## User Setup Required
None - documentation-only change, no external service configuration required.

## Next Phase Readiness
- DOC-01 closed; the plugin now has an onboarding surface.
- Maintenance note: future changes to plugin CLI options, `SUITE_PROFILES`, or the CI matrix should be mirrored in `tests/ruciopytest/README.md` (a deferred idea proposes an automated staleness check).

---
*Phase: 09-plugin-docs*
*Completed: 2026-07-01*

## Self-Check: PASSED

- FOUND: tests/ruciopytest/README.md
- FOUND: .planning/phases/09-plugin-docs/09-01-SUMMARY.md
- FOUND commit: a3f5266bc (Task 1)
- FOUND commit: 1f277edd1 (Task 2)
