---
phase: 11-simplify-votests-workflow
verified: 2026-07-06T00:00:00Z
status: passed
score: 5/5 must-haves verified
---

# Phase 11: simplify_votests workflow Verification Report

**Phase Goal:** The policy (votest) tests run in CI for BOTH policies (atlas AND belleii),
mimicking legacy `vo_tests.yml` behaviour but driven by the ruciopytest plugin — restoring the
belleii coverage that PR CI currently omits.
**Verified:** 2026-07-06
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `simplify_votests.yml` exists, valid YAML, four triggers (pull_request/push/workflow_dispatch/schedule cron `0 3 * * *`) | ✓ VERIFIED | File present; `on:` block has all four triggers, cron `'0 3 * * *'` at line 8 |
| 2 | Static atlas+belleii matrix, calls `runtime_images.yml`, plugin invocation `python -m pytest --suite=votest --policy=<X>` with per-policy junit/artifact/report naming | ✓ VERIFIED | Matrix has exactly `policy: atlas/leg: votest-atlas` and `policy: belleii/leg: votest-belleii`; `uses: ./.github/workflows/runtime_images.yml` at line 16; Run tests step at lines 89-105 uses `--suite=votest --policy=${{ matrix.policy }}`, junit/log/artifact names keyed off `${{ matrix.leg }}`; `fail-fast: false` (line 28) and `fail_on_failure: true` (line 145) both present |
| 3 | `simple-autotest.yml` no longer has a votest leg (5 legs remain: remote_dbs x2, multi_vo-tst, multi_vo-ts2, client) | ✓ VERIFIED | Matrix `include` in `simple-autotest.yml` has exactly 5 entries, no `votest` leg |
| 4 | belleii plugin-path fix present: `_bootstrap_test_data()` runs a belleii bootstrap (belleii scope set + `/belle` CONTAINER DID hierarchy) gated on `policy == 'belleii'` | ✓ VERIFIED | `tests/ruciopytest/infra_manager.py` lines 850-879: `if self._profile.policy == 'belleii':` block creates 11 belleii scopes + 6 `/belle*` CONTAINER DIDs attached under `scope='other' name='/belle'`, idempotent (Duplicate/DuplicateContent tolerant) |
| 5 | Live-run evidence: both votest-atlas and votest-belleii green on a live maany/rucio run via the plugin path | ✓ VERIFIED | Confirmed live via `gh run view 28782189273 --repo maany/rucio` (workflow_dispatch) and `gh run view 28782165867` (pull_request): both show `test (belleii, votest-belleii, 3.9, postgres14) = success` and `test (atlas, votest-atlas, 3.9, postgres14) = success`, plus `runtime_images = success` |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `.github/workflows/simplify_votests.yml` | Dedicated two-policy votest workflow driven by the ruciopytest plugin | ✓ VERIFIED | Exists, valid YAML, matches all Plan 11-01 must-haves |
| `.github/workflows/simple-autotest.yml` | PR autotest workflow with votest leg removed | ✓ VERIFIED | 5 legs, no votest |
| `tests/ruciopytest/infra_manager.py` | belleii bootstrap provisioning (plan-anticipated deviation, correctly scoped) | ✓ VERIFIED | POLICY-gated bootstrap present and wired into `_bootstrap_test_data()`, called from setup paths (lines 117, 249) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `simplify_votests.yml` | `runtime_images.yml` | `uses: ./.github/workflows/runtime_images.yml` | WIRED | Line 16, job `runtime_images` |
| `simplify_votests.yml` | ruciopytest plugin | `python -m pytest --suite=votest --policy=${{ matrix.policy }}` | WIRED | Line 98-100, Run tests step |
| `simplify_votests.yml` | GitHub Actions live run (maany/rucio) | `gh run view` | WIRED | Both dispatch (28782189273) and pull_request (28782165867) runs show votest-atlas + votest-belleii = success, on fix sha `79eeb27b8` |
| `infra_manager.py::_bootstrap_test_data` | belleii DIRAC tests (`tests/test_belleii.py`) | POLICY-gated scope/DID bootstrap | WIRED | Fixed the previously-failing `ScopeNotFound` errors in `test_dirac_addfile*`; confirmed green post-fix |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| SUIT-07 | 11-01, 11-02 | votest coverage extended to belleii (already satisfied for atlas) | ✓ SATISFIED | `simplify_votests.yml` runs both policies via plugin path; both green on live run; REQUIREMENTS.md traceability row: "Phase 11 \| SUIT-07 (votest coverage) \| belleii policy never run in CI — new simplify_votests.yml..." — closed |

No orphaned requirements: both plans declare `requirements: [SUIT-07]` in frontmatter, matching the single ID mapped to Phase 11 in REQUIREMENTS.md's gap-closure table.

### Anti-Patterns Found

None. No TODO/FIXME/placeholder markers, no empty handlers, no static returns in
`simplify_votests.yml`, the `simple-autotest.yml` diff, or the `infra_manager.py` belleii
bootstrap block. The bootstrap gracefully tolerates `Duplicate`/`DuplicateContent` (idempotency,
not a stub) and prints diagnostic errors rather than swallowing failures silently for unexpected
exceptions.

### Human Verification Required

None required — Plan 11-02's human-verify checkpoint (Task 2) was already completed and approved
during phase execution (per 11-02-SUMMARY: "Task 2 human-verify checkpoint APPROVED"), and live
run evidence independently confirms the green state via `gh run view`.

### Gaps Summary

No gaps found. Both plans' must-haves are fully satisfied:
- `simplify_votests.yml` matches the required shape exactly (triggers, matrix, plugin invocation,
  naming, fail-fast/fail_on_failure).
- `simple-autotest.yml`'s votest leg was cleanly removed, leaving 5 legs.
- The belleii plugin-path gap (missing scope/DID provisioning causing `ScopeNotFound` in DIRAC
  tests) was diagnosed and fixed in `infra_manager.py`, and both policy legs are confirmed green
  on two independent live runs (workflow_dispatch and pull_request) on sha `79eeb27b8`.
- ROADMAP.md Phase 11 entry is marked complete (`- [x] Phase 11: simplify_votests workflow ...
  (completed 2026-07-06)`), consistent with this verification.

Note: Phase 11's ROADMAP success criterion #1 wording ("not per-PR") is intentionally superseded
by the 11-CONTEXT locked decision to match legacy `vo_tests.yml` cadence exactly (pull_request +
push + workflow_dispatch + schedule). This supersession is documented in both the ROADMAP.md
Phase 11 note and 11-01-SUMMARY.md, and does not constitute a gap.

---

*Verified: 2026-07-06*
*Verifier: Claude (gsd-verifier)*
