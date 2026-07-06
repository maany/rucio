---
phase: 12-multi-vo-legacy-parity
verified: 2026-07-06T00:00:00Z
status: passed
score: 8/8 must-haves verified
---

# Phase 12: multi_vo Legacy Parity Verification Report

**Phase Goal:** `multi_vo` CI runs both VOs (tst→ts2) sequentially against one shared instance/DB, matching legacy `run_multi_vo_tests_docker.sh`, restoring the shared-DB multi-tenancy coverage and stop-on-failure gate that the Phase 8.1 parallel split removed.
**Verified:** 2026-07-06
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
| --- | --- | --- | --- |
| 1 | `simple-autotest.yml` runs multi_vo as a single matrix leg with no `vo:` field, so `RUCIO_MULTI_VO_LEG` resolves empty and `run_multi_vo()` takes the sequential shared-DB path | ✓ VERIFIED | `.github/workflows/simple-autotest.yml` lines 39-42: single `{suite: multi_vo, leg: multi_vo, python: "3.9", rdbms: postgres14}` entry, no `vo:` key. YAML parse confirms exactly one multi_vo entry with no `vo` field (`{'suite': 'multi_vo', 'leg': 'multi_vo', 'python': '3.9', 'rdbms': 'postgres14'}`). |
| 2 | The two parallel per-VO legs (`multi_vo-tst`/`multi_vo-ts2`) from 8.1 are gone | ✓ VERIFIED | `grep -n "multi_vo-tst\|multi_vo-ts2" .github/workflows/simple-autotest.yml` → no matches. Full matrix leg list = `['remote_dbs', 'remote_dbs', 'multi_vo', 'client']`. |
| 3 | junit/report/artifact naming for the single leg reverts to non-split `multi_vo-py3.9.*` | ✓ VERIFIED | Naming keys off `matrix.leg` (unchanged code, `${{ matrix.leg }}-py${{ matrix.python }}.xml` etc.); live run artifact confirmed via `gh api .../artifacts` = `test-results-multi_vo-py3.9` (no `-tst`/`-ts2` suffix). |
| 4 | README CI-mapping table and 8.1 parity notes describe the reverted single-leg sequential model | ✓ VERIFIED | `tests/ruciopytest/README.md` line 291: single `multi_vo` row noting "runs tst then ts2 sequentially, shared DB"; line 297: "`multi_vo` is a single sequential leg (legacy parity)"; no `multi_vo-tst`/`multi_vo-ts2`/"split into two parallel" strings remain (grep empty). |
| 5 | `infra_manager.py`'s sequential branch (unmodified) is what actually executes: tst first, ts2 only on tst success, no DB reset between VOs | ✓ VERIFIED | `tests/ruciopytest/infra_manager.py` lines 388-428: `leg = os.environ.get("RUCIO_MULTI_VO_LEG", "").strip()`; when not in `{"tst","ts2"}` falls to lines 411-428 sequential path — `bootstrap_vo(TST_HOME)` → tst run → gate on `tst.returncode != 0` (return early, skip ts2) → `bootstrap_vo(TS2_HOME)` (re-points RUCIO_HOME only, no `_purge_database`/`_build_database` call) → ts2 run → return ts2 code. |
| 6 | The single multi_vo leg is green on a live PR run of `simple-autotest.yml` on maany/rucio | ✓ VERIFIED | `gh run view 28787193259 --repo maany/rucio`: job `test (multi_vo, multi_vo, 3.9, postgres14)` (databaseId `85356695724`) conclusion = `success`; sha `f2643a959d48065cb9f5dccd2b2d0e4683088042` matches claim; host log: `1240 passed, 365 skipped, 7 xfailed ... 0 failed` in 47:02. Only ONE multi_vo job present in the run's job list. |
| 7 | The run log/env proves the SEQUENTIAL branch executed, not the single-leg selector | ✓ VERIFIED | Host log: `RUCIO_MULTI_VO_LEG:` (empty value) at the "Run tests" step — this is the branch determinant (`leg = ""` → falls out of the `if leg in HOMES:` single-leg-selector guard → sequential path). `grep -c "single-leg selector"` in the log = 0. tst run streamed and passed (`RUCIO_HOME: /opt/rucio/etc/multi_vo/tst`, `test_multi_vo.py` cases PASSED). The literal print lines "Running tests for VO tst"/"ts2" are not in the retained host log because ts2 runs with `forward_stream=False` by design (container combined log is an on-failure-only artifact, not retained for this green run) — this is a documented, understood gap in direct log-string evidence, not a functional gap; the branch selection itself (the empty env var) is directly observed. |
| 8 | tst and ts2 ran against the SAME shared DB with no inter-VO reset | ✓ VERIFIED | Code: sequential branch calls `bootstrap_vo()` between VOs (re-points `RUCIO_HOME`, no purge/rebuild) — confirmed by reading lines 210-214 and 422-424; `_purge_database`/`_build_database` are separate methods (lines 533, 591) called elsewhere (setup), not inside `run_multi_vo()`'s sequential path. Live log grep for purge/rebuild strings between the VO runs found no infra_manager DB-reset log lines (only unrelated test names containing "purge"). |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| --- | --- | --- | --- |
| `.github/workflows/simple-autotest.yml` | Single multi_vo matrix leg (suite=multi_vo, leg=multi_vo, no vo:), sequential shared-DB path | ✓ VERIFIED | Contains `leg: multi_vo` (line 40); no `vo:` field on that entry; `RUCIO_MULTI_VO_LEG: ${{ matrix.vo }}` env line retained (line 178) with updated comment (lines 173-177) explaining it resolves empty for this leg. |
| `tests/ruciopytest/README.md` | Doc sync: single multi_vo CI leg, sequential tst->ts2 shared-DB, accepted long-pole | ✓ VERIFIED | Contains "multi_vo" single-row CI-mapping (line 291), sequential-single-leg parity note (line 297), long-pole trade-off documented; no stale split references. |
| `tests/ruciopytest/infra_manager.py` | Sequential branch (lines 402-428), unmodified | ✓ VERIFIED (untouched by design) | Sequential branch present and unmodified per plan intent (plan explicitly directs "Do NOT touch infra_manager.py"); confirmed present at lines 402-428. |

### Key Link Verification

| From | To | Via | Status | Details |
| --- | --- | --- | --- | --- |
| `simple-autotest.yml` matrix (`leg: multi_vo`, no `vo:`) | `infra_manager.py run_multi_vo()` sequential branch (lines 402-428) | `RUCIO_MULTI_VO_LEG` env resolves empty (`matrix.vo` unset) | ✓ WIRED | Workflow YAML confirmed to omit `vo:` on the multi_vo entry; `RUCIO_MULTI_VO_LEG: ${{ matrix.vo }}` env line present; live run host log shows `RUCIO_MULTI_VO_LEG:` empty at runtime, and code at line 388-389 (`leg = ...strip(); if leg in HOMES:`) confirms an empty string does not match `{"tst","ts2"}`, falling through to the sequential path (lines 402-428). Live run job green (0 failed), consistent with the sequential path having executed successfully end-to-end. |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| --- | --- | --- | --- | --- |
| SUIT-08 | 12-01-PLAN.md, 12-02-PLAN.md | `pytest --suite=multi_vo` runs the multi-VO test selection with the 2-VO configuration, matching legacy `test.sh`/`run_multi_vo_tests_docker.sh` | ✓ SATISFIED | Already marked Complete under Phase 7 (`REQUIREMENTS.md` line 44); Phase 12 closes the residual live-CI gap (the CI wiring around SUIT-08, not the requirement's core test-selection logic) — confirmed via single-leg matrix + live green run with sequential-branch env proof. REQUIREMENTS.md line 67 explicitly maps Phase 12 to this closure. |

No orphaned requirements found: SUIT-08 is the only ID declared in either plan's frontmatter, and REQUIREMENTS.md's Phase-12 mapping row references only SUIT-08.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| --- | --- | --- | --- | --- |
| — | — | None found (no TODO/FIXME/placeholder/stub patterns in the modified workflow or README) | — | — |

### Human Verification Required

None outstanding — Plan 12-02's Task 2 human-verify checkpoint was already completed as part of phase execution (per SUMMARY, coordinator confirmed with independent evidence), and this verification independently re-confirmed the live run's job status, sha, log content, and artifact naming directly via `gh`.

One residual note (not a gap, documented both in 12-02-SUMMARY.md and reconfirmed here): the literal print lines `"Running tests for VO tst"` / `"Running tests for VO ts2"` are not present in the retained GitHub Actions host log for this green run, because `run_multi_vo()`'s ts2 subprocess runs with `forward_stream=False` (its container-combined log is only uploaded by CI on failure). The sequential-branch determination is instead proven directly from the empty `RUCIO_MULTI_VO_LEG` env value (the actual branch-selection condition in code) plus the streamed tst pass and overall leg success — this is stronger/equivalent evidence to the log-string search and was independently reproduced during this verification via `gh run view --job 85356695724 --log`.

### Gaps Summary

No gaps found. All 8 derived observable truths verified against the live codebase and a live, independently-re-queried GitHub Actions run (28787193259, job 85356695724, sha f2643a959d48065cb9f5dccd2b2d0e4683088042). The single multi_vo leg is present, unsplit, correctly wired to the sequential shared-DB branch in `infra_manager.py` (untouched, as intended), documented in the README, non-split artifact-named (`test-results-multi_vo-py3.9`), and green with 0 failed tests. The unrelated `remote_dbs` py3.10 leg failure noted in 12-02-SUMMARY.md is correctly out of scope for this phase (multi_vo-only revert) and does not affect Phase 12's goal achievement.

---

_Verified: 2026-07-06_
_Verifier: Claude (gsd-verifier)_
