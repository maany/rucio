---
phase: 11-simplify-votests-workflow
plan: 02
subsystem: ci
tags: [ci, github-actions, votest, ruciopytest, policy, belleii, dirac, bootstrap]
requires:
  - phase: 11-01
    provides: dedicated simplify_votests.yml two-policy votest workflow (plugin path)
  - phase: 07-01
    provides: ruciopytest --suite=votest --policy=<X> selection + in-container [policy] rewrite
provides:
  - A green two-policy (atlas + belleii) votest run proven live on maany/rucio
  - belleii plugin-path provisioning (belleii scopes + /belle CONTAINER DID hierarchy) in InfraManager
affects:
  - future votest / policy work relying on the plugin path for belleii
tech-stack:
  added: []
  patterns:
    - POLICY-gated in-container test-data provisioning inside InfraManager._bootstrap_test_data (belleii scope + /belle DID hierarchy)
key-files:
  created:
    - .planning/phases/11-simplify-votests-workflow/11-02-SUMMARY.md
  modified:
    - tests/ruciopytest/infra_manager.py
key-decisions:
  - "belleii DIRAC provisioning belongs in the plugin (InfraManager._bootstrap_test_data), not the workflow YAML, because the plugin owns container setup + test run inside one pytest process — a workflow step cannot run 'after setup, before tests'"
  - "Ported belleii_bootstrap verbatim from tools/bootstrap_tests.py (upstream parity) rather than inventing a new scope set"
patterns-established:
  - "Policy-specific bootstrap gated on self._profile.policy, idempotent (Duplicate/DuplicateContent tolerant)"
requirements-completed: [SUIT-07]
duration: 30min
completed: 2026-07-06
---

# Phase 11 Plan 02: Live two-policy votest verification Summary

**Proved `simplify_votests.yml` goes green for BOTH atlas and belleii on live maany/rucio runs, driving belleii through the plugin path for the first time, and fixed the belleii DIRAC ScopeNotFound gap by porting `belleii_bootstrap` into the plugin's InfraManager.**

## Performance

- **Duration:** ~30 min (multiple live CI iterations)
- **Started:** 2026-07-06T09:08:00Z
- **Completed:** 2026-07-06T~09:55:00Z
- **Tasks:** 2 (Task 1 autonomous drive-to-green; Task 2 human-verify checkpoint APPROVED)
- **Files modified:** 1 code file (+ this summary / state docs)

## Accomplishments

- Pushed `simplify_votests.yml` (from 11-01) to `clean-autotests`; drove live runs via `gh --repo maany/rucio`.
- First live belleii plugin-path run: **votest-atlas green, votest-belleii FAILURE** — 585 passed, 299 skipped, exactly 3 failures, all in `tests/test_belleii.py` (`test_dirac_addfile[86400.0]`, `test_dirac_addfile[None]`, `test_dirac_addfile_with_parents_meta`), all `ScopeNotFound`.
- Diagnosed root cause: the belleii DIRAC tests build LFNs under `/belle` and resolve the scope against existing scopes (lib/rucio/core/dirac.py), requiring the belleii scope set + `/belle*` CONTAINER DID hierarchy that upstream provisions via `tools/bootstrap_tests.py::belleii_bootstrap`. The plugin's `InfraManager._bootstrap_test_data()` only created generic accounts + `mock`/`archive` scopes, so belleii was missing this provisioning (atlas doesn't need it → atlas green).
- Ported `belleii_bootstrap` into `_bootstrap_test_data()`, POLICY-gated on `self._profile.policy == 'belleii'` (commit `79eeb27b8`).
- Re-ran live: **both votest-atlas and votest-belleii green** on the fix sha `79eeb27b8`, on both the workflow_dispatch and pull_request runs.

## Green run URLs (fix sha `79eeb27b8`)

- Dispatch: https://github.com/maany/rucio/actions/runs/28782189273 — runtime_images + votest-atlas + votest-belleii all `success`
- Pull request: https://github.com/maany/rucio/actions/runs/28782165867 — runtime_images + votest-atlas + votest-belleii all `success`

belleii ran through the plugin path (`python -m pytest --suite=votest --policy=belleii ...`), not the legacy `votest_helper.py` / `run_tests.py` driver.

## Task Commits

1. **Task 1: Push + drive live two-policy run; fix belleii until green** — `79eeb27b8` (fix)
   - (The `simplify_votests.yml` workflow itself was already committed in 11-01 as `1045c2156`; no further workflow change was needed — the belleii fix lives in the plugin.)

**Plan metadata:** see finalization docs commit below.

## Files Created/Modified

- `tests/ruciopytest/infra_manager.py` — added POLICY-gated belleii bootstrap (belleii scope set + `/belle`, `/belle/mc`, `/belle/Data`, `/belle/user`, `/belle/raw`, `/belle/mock` CONTAINER DIDs, children attached under scope=`other` name=`/belle`); imports `DuplicateContent` and `extract_scope`. Idempotent.

## Decisions Made

- Fixed in the plugin, not the workflow: the plugin owns container setup + test run in one pytest process, so belleii provisioning must happen inside `_bootstrap_test_data()` (after generic scopes, before tests), which a workflow step cannot cleanly do.
- Mirrored upstream `belleii_bootstrap` exactly (same scope list, same `/belle*` DIDs) rather than inventing a new set — the 3 failing DIRAC tests needed precisely that hierarchy.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] belleii DIRAC provisioning ported into the plugin**
- **Found during:** Task 1 (first live belleii plugin-path run)
- **Issue:** `tests/test_belleii.py::test_dirac_addfile*` (3 tests) failed with `rucio.common.exception.ScopeNotFound`. The belleii scope set and `/belle` CONTAINER DID hierarchy that DIRAC scope-resolution depends on were never created on the plugin path — the plugin's `_bootstrap_test_data()` omitted `belleii_bootstrap`.
- **Fix:** Ported `belleii_bootstrap` (from `tools/bootstrap_tests.py`) into `InfraManager._bootstrap_test_data()`, gated on `self._profile.policy == 'belleii'`; added `DuplicateContent` + `extract_scope` imports. Idempotent (Duplicate/DuplicateContent tolerant).
- **Files modified:** `tests/ruciopytest/infra_manager.py`
- **Verification:** Live re-run on sha `79eeb27b8` — votest-belleii = `success` (was FAILURE), votest-atlas still `success`, on both dispatch (28782189273) and PR (28782165867) runs.
- **Committed in:** `79eeb27b8`

**Scope note:** This expands `files_modified` beyond the plan's declared `.github/workflows/simplify_votests.yml` to include `tests/ruciopytest/infra_manager.py`. This is a **plan-anticipated deviation** — the plan's Task 1 action explicitly instructed: "if the plugin does not install/config belleii, add the minimal provisioning". The workflow YAML needed no change; the correct, minimal provisioning was in the plugin.

---

**Total deviations:** 1 auto-fixed (1 missing-critical, plan-anticipated).
**Impact on plan:** Necessary for belleii correctness on the plugin path. No scope creep — provisioning mirrors upstream exactly.

## Issues Encountered

- **Concurrency cancellations:** push + workflow_dispatch share the branch-ref concurrency group (`cancel-in-progress`), so triggering a dispatch cancels the in-flight push run. Resolved by tracking the surviving branch-ref run (dispatch) plus the independent pull_request run (different concurrency group, `refs/pull/N/merge`).
- **`gh` default repo:** this clone has `upstream = rucio/rucio`, so `gh` defaulted to upstream and 404'd. Resolved by passing `--repo maany/rucio` on every `gh` call.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- ROADMAP success criterion #3 (both policy legs green on a live run of the new workflow) is met.
- belleii's plugin path is proven live for the first time; the DIRAC provisioning gap is closed.
- Ready for phase verification / phase-complete (handled by the orchestrator).

## Self-Check: PASSED

- FOUND: tests/ruciopytest/infra_manager.py (belleii bootstrap block present)
- FOUND commit: 79eeb27b8 (Task 1 fix)
- VERIFIED green: run 28782189273 (dispatch) + 28782165867 (PR), all jobs success

---
*Phase: 11-simplify-votests-workflow*
*Completed: 2026-07-06*
