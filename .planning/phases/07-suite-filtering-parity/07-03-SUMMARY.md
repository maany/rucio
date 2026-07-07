---
phase: 07-suite-filtering-parity
plan: 03
subsystem: testing
tags: [pytest, parity, baseline, drift-guard, votest, json]

# Dependency graph
requires:
  - phase: 07-suite-filtering-parity
    plan: 01
    provides: votest_support.collect_votest_paths / load_matrix / resolve_policy
  - phase: 01-foundation
    provides: SUITE_PROFILES (profiles.py) with client test_paths
provides:
  - tests/ruciopytest/parity_baselines.json (ground-truthed sorted file sets per suite/policy)
  - tests/ruciopytest/test_parity.py (import-free drift guard, runs in plain CI)
  - drift protection on tests/ inventory + policy YAML allow/deny lists
affects: [08-ci-for-real, parity-guard]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "parity baseline as checked-in JSON of sorted repo-relative paths; comparison is sorted SETS (votest order is set-derived / non-deterministic)"
    - "import-free guard: pure path math (glob + collect_votest_paths), no rucio import, no live server, no container"
    - "readable added/removed diff on drift forces an intentional, reviewable baseline update"
    - "drift self-test (test_drift_detected) proves the guard bites instead of silently tolerating change"

key-files:
  created:
    - tests/ruciopytest/parity_baselines.json
    - tests/ruciopytest/test_parity.py
  modified: []

key-decisions:
  - "Baseline stored as file-level sorted paths keyed by suite, with votest keyed per-(suite,policy): votest:atlas / votest:belleii"
  - "Baseline generated deterministically from profiles + matrix YAML (not hand-typed) so it is reproducible"
  - "client compared from SUITE_PROFILES['client'].test_paths; remote_dbs/sqlite/multi_vo compared against full glob(tests/test_*.py)"
  - "test_drift_detected catches pytest.fail.Exception explicitly (Failed is a BaseException, not Exception)"

requirements-completed: [SUIT-09]

# Metrics
metrics:
  duration: 3min
  tasks: 2
  files: 2
  completed: "2026-06-23"
---

# Phase 7 Plan 3: Suite-selection Parity Guard Summary

Checked-in JSON baseline of each suite's computed file-selection plus an import-free pytest guard that fails the build on any silent drift in `tests/` inventory or the policy YAML allow/deny lists (atlas=36, belleii=52, client=3, remote_dbs/sqlite/multi_vo=full `tests/`=93).

## What Was Built

**Task 1 — `parity_baselines.json` (commit `0aeda7341`):** A ground-truthed baseline generated deterministically from `profiles.SUITE_PROFILES["client"].test_paths`, the full `tests/test_*.py` glob, and `votest_support.collect_votest_paths` for each policy in the matrix YAML. Verified counts: votest:atlas=36, votest:belleii=52, client=3, and remote_dbs==sqlite==multi_vo (identical full-tests sets of 93 files). All entries are sorted repo-relative `.py` paths.

**Task 2 — `test_parity.py` (commit `d938b779d`):** An import-free drift guard with four tests:
- `test_votest_selection` (SUIT-07/09): votest computed set matches baseline for every policy (data-driven over YAML keys), plus literal count locks atlas==36 / belleii==52.
- `test_policy_resolution` (SUIT-07): `--policy` flag wins over `POLICY` env; both-missing → `None`; unknown policy detectable against matrix keys. Uses a tiny fake config object.
- `test_baseline_parity` (SUIT-09): client + remote_dbs/sqlite/multi_vo match baseline.
- `test_drift_detected` (SUIT-09): a tampered baseline (fake path) raises a readable added/removed diff naming the offending path, proving the guard bites.

The guard runs with plain `pytest` — no rucio import, no server, no container.

## Verification

- `python -m pytest tests/ruciopytest/test_parity.py -x -q` → 4 passed.
- `python -m pytest tests/ruciopytest/ -q` → 53 passed (whole plugin suite stays green).
- Sanity drift check: `git mv tests/test_clients.py ...` made `test_votest_selection` + `test_baseline_parity` FAIL; revert restored 4 passed. Drift protection confirmed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] test_drift_detected caught the wrong exception type**
- **Found during:** Task 2
- **Issue:** `pytest.raises(Exception)` did not catch `pytest.fail(...)` — the `Failed` outcome inherits from `BaseException`, not `Exception`, so the drift self-test failed.
- **Fix:** Changed to `pytest.raises(pytest.fail.Exception)`.
- **Files modified:** tests/ruciopytest/test_parity.py
- **Commit:** d938b779d (caught before commit during the verify step)

## Self-Check: PASSED
- FOUND: tests/ruciopytest/parity_baselines.json
- FOUND: tests/ruciopytest/test_parity.py
- FOUND commit: 0aeda7341
- FOUND commit: d938b779d
