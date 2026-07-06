---
phase: 11-simplify-votests-workflow
plan: 01
subsystem: ci
tags: [ci, github-actions, votest, ruciopytest, policy]
requires:
  - ruciopytest plugin --suite=votest --policy=<X> (Phase 07-01)
  - .github/workflows/runtime_images.yml
provides:
  - Dedicated two-policy (atlas + belleii) votest CI workflow via the plugin
affects:
  - .github/workflows/simple-autotest.yml (votest leg removed)
tech-stack:
  added: []
  patterns:
    - Static two-policy matrix keyed off matrix.leg for per-policy junit/artifact/report isolation
    - Plugin-driven votest (python -m pytest --suite=votest --policy=<X>) instead of legacy votest_helper.py dynamic matrix
key-files:
  created:
    - .github/workflows/simplify_votests.yml
  modified:
    - .github/workflows/simple-autotest.yml
decisions:
  - Per-PR cadence (pull_request + push + workflow_dispatch + nightly) intentionally supersedes ROADMAP criterion #1 "not per-PR", per locked 11-CONTEXT decision to match legacy vo_tests.yml exactly
  - No belleii-specific policy-package install; rely on plugin in-container [policy] cfg rewrite (Phase 07-01), identical for both policies. belleii provisioning gaps deferred to Plan 11-02 live verification
requirements-completed: [SUIT-07]
metrics:
  duration: 3min
  completed: 2026-07-06
---

# Phase 11 Plan 01: Simplify VO-tests Workflow Summary

Restored belleii votest CI coverage by adding a dedicated `simplify_votests.yml` that runs
both `atlas` and `belleii` via the ruciopytest plugin, and removed the atlas-only votest leg
from `simple-autotest.yml` so votest lives in exactly one workflow.

## What Was Built

**Task 1 — `simplify_votests.yml` (new):** A near-copy of `simple-autotest.yml`'s votest leg
plus the shared runtime-images/report scaffolding, specialized to a static two-policy matrix:

- `name: VO-specific tests (ruciopytest)`
- Triggers: `pull_request`, `push`, `workflow_dispatch`, `schedule` (cron `0 3 * * *`) — matches
  legacy `vo_tests.yml` cadence with pull_request/push active.
- `concurrency` group `${{ github.workflow }}-${{ github.ref }}`, cancel-in-progress.
- `jobs.runtime_images` reuses `./.github/workflows/runtime_images.yml` verbatim.
- `jobs.test` static matrix: `{policy: atlas, leg: votest-atlas}` and `{policy: belleii, leg: votest-belleii}`,
  both py3.9 + postgres14.
- Shared steps copied in order (Checkout → Set up Python → Install host plugin deps → GHCR login →
  Buildx → Compute image metadata → Build runtime image locally → Run tests → Upload test results →
  Upload container logs → Upload host logs → Publish test report), with pinned action SHAs preserved.
- Python fixed at 3.9, so `py39_image` is used directly for `RUCIO_TEST_IMAGE` and build `tags`
  (the 3.10 ternary was dropped).
- Run step: `python -m pytest --suite=votest --policy=${{ matrix.policy }}` with `POLICY` env and
  junit/log paths keyed off `${{ matrix.leg }}` so atlas/belleii never collide.
- Client-provisioning steps and `RUCIO_MULTI_VO_LEG` env were intentionally NOT copied.
- Report `check_name: Tests - ${{ matrix.leg }} (py${{ matrix.python }})`, `fail_on_failure: true`.

**Task 2 — `simple-autotest.yml` (edited):** Deleted the votest matrix `include` entry
(suite=votest / leg=votest / policy=atlas). Matrix now has exactly 5 legs: `remote_dbs` (py3.9),
`remote_dbs` (py3.10), `multi_vo-tst`, `multi_vo-ts2`, `client`. Shared steps untouched; the votest
report no longer renders since no votest leg exists in the matrix. `POLICY: ${{ matrix.policy }}`
left in the shared env (empty for remaining legs, harmless).

## Deviations from Plan

None - plan executed exactly as written.

Note on verification tooling: PyYAML was not available in the default `python` (a foreign project
venv is on PATH); the plan's `python -c "import yaml..."` verify commands were run with
`/usr/bin/python3`, which has PyYAML. Both verifications passed. This is an environment note, not a
plan deviation.

## Cadence decision (supersedes ROADMAP criterion #1)

ROADMAP success criterion #1 described votest as "not per-PR". The locked 11-CONTEXT decision is to
match legacy `vo_tests.yml` exactly — i.e. `pull_request` + `push` + `workflow_dispatch` + nightly
schedule. Per that decision, `simplify_votests.yml` runs per-PR, and criterion #1's "not per-PR"
wording is intentionally superseded.

## Verification

- Both workflow files parse as valid YAML.
- `simplify_votests.yml`: four triggers, static atlas+belleii matrix, `--suite=votest --policy=<X>`
  plugin invocation, `runtime_images.yml` call, per-policy naming, `fail-fast: false` +
  `fail_on_failure: true`. (verify: OK)
- `simple-autotest.yml`: 5 legs, no votest. (verify: `OK legs: ['remote_dbs', 'remote_dbs',
  'multi_vo-tst', 'multi_vo-ts2', 'client']`)
- `git diff --stat` shows exactly the two intended files changed.

## Follow-ups

- Plan 11-02: live `workflow_dispatch` verification of both policy legs; discover/fix any
  belleii-specific provisioning gaps there.

## Self-Check: PASSED

- FOUND: .github/workflows/simplify_votests.yml
- FOUND: .github/workflows/simple-autotest.yml
- FOUND: .planning/phases/11-simplify-votests-workflow/11-01-SUMMARY.md
- FOUND commit: 1045c2156 (Task 1)
- FOUND commit: bcd71115b (Task 2)
