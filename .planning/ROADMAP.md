# Roadmap: Rucio Test Framework Overhaul

## Milestones

- ✅ **v1.0 Pytest Test-Runner** — Phases 1-6 (shipped 2026-06-15) — full detail in `milestones/v1.0-ROADMAP.md`
- 🚧 **v1.1 Productionize & Merge** — parity + CI-for-real + docs + delivery (planning) — see `v1.1-DESIGN.md`

## Phases

<details>
<summary>✅ v1.0 Pytest Test-Runner (Phases 1-6) — SHIPPED 2026-06-15</summary>

- [x] Phase 1: Plugin Skeleton and Suite Profiles (2/2 plans)
- [x] Phase 2: Database Lifecycle and Bootstrap (2/2 plans)
- [x] Phase 3: Container Lifecycle and Cleanup (3/3 plans)
- [x] Phase 4: Test Collection and Parallelism Integration (2/2 plans)
- [x] Phase 5: CI Workflow and Migration (2/2 plans)
- [x] Phase 6: Host pytest with optional --run-in-container forwarding (4/4 plans)

Full detail: `milestones/v1.0-ROADMAP.md`. Known gaps carried to v1.1: `MILESTONES.md`.

</details>

### 🚧 v1.1 Productionize & Merge (in progress)

Scope (see `v1.1-DESIGN.md`): suite-filtering parity (votest/multi_vo), CI that actually
runs green with artifacts, plugin README, then clean rucio-convention stacked-PR delivery
to maany/rucio (delivery is a manual final step, not a phase).

- [x] Phase 7: Suite-filtering parity — votest POLICY selection, multi_vo 2-VO config, parity guard (completed 2026-06-23)
- [x] Phase 8: CI for real — host deps install, full rucio for host suites, artifacts/junit, all 5 legs green (completed 2026-07-01)
- [x] Phase 8.1: multi_vo parallel VO legs — split tst/ts2 into parallel matrix legs (INSERTED)
- [ ] Phase 9: Plugin docs — tests/ruciopytest/README.md
- [x] Phase 10: Docs & traceability cleanup — README env-var fix + SUIT-09 frontmatter backfill + STATE version reconcile (gap closure, audit v1.1) (completed 2026-07-02)
- [x] Phase 11: simplify_votests workflow — new nightly/dispatch votest workflow (atlas + belleii), plugin-driven (gap closure, audit v1.1) (completed 2026-07-06)
- [x] Phase 12: multi_vo legacy parity — revert 8.1 split, single shared-DB sequential leg (tst→ts2, gate) in simple-autotest.yml (gap closure, audit v1.1) (completed 2026-07-06)

## Phase Details

### Phase 7: Suite-filtering parity
**Goal**: Each suite profile selects the same tests the legacy `tools/test/test.sh` ran, so the plugin is a faithful drop-in.
**Depends on**: v1.0 (Phases 1-6)
**Requirements**: SUIT-07, SUIT-08, SUIT-09
**Success Criteria**:
  1. `pytest --suite=votest --co` (with a POLICY) lists exactly the policy-package test set from `matrix_policy_package_tests.yml`, not all of `tests/`
  2. `pytest --suite=multi_vo --co` lists the multi-VO test selection and the run uses the 2-VO config
  3. `client`/`remote_dbs`/`sqlite` selections equal the legacy `test.sh` selections
  4. An automated test fails if any suite's selection drifts from the legacy baseline

**Plans:** 3/3 plans complete
- [ ] 07-01-PLAN.md — votest POLICY selection (absorbed collect_tests → atlas=36/belleii=52) + --policy/POLICY wiring + in-container [policy] rewrite (SUIT-07)
- [ ] 07-02-PLAN.md — multi_vo 2-VO config generation (absorbed merge_configs) + per-VO run (tst then ts2-on-success) in InfraManager (SUIT-08)
- [ ] 07-03-PLAN.md — import-free parity baseline + drift guard test (SUIT-09)

### Phase 8: CI for real
**Goal**: `simple-autotest.yml` actually runs and goes green for all 5 matrix legs, with artifacts.
**Depends on**: Phase 7
**Requirements**: CICD-05, CICD-06, CICD-07, CICD-08
**Success Criteria**:
  1. Each matrix leg installs host Python + plugin deps and reaches the pytest invocation
  2. `client` leg has a full rucio install and executes host-side
  3. Every leg uploads junit XML and renders a test report
  4. All 5 legs pass green on a PR to maany/rucio

**Plans:** 4 plans
- [ ] 08-01-PLAN.md — sqlite descope (plugin profile + choices + parity baseline/guard) + docs + .gitignore (CICD-06 doc, CICD-08)
- [ ] 08-02-PLAN.md — workflow: 5-leg matrix + setup-python/host plugin deps + votest POLICY + junit/artifacts/host-log capture (CICD-05, CICD-07)
- [ ] 08-03-PLAN.md — host-side client provisioning: full rucio install + reachable bootstrapped server + cfg/certs + in-container fallback (CICD-06)
- [ ] 08-04-PLAN.md — live CI green verification: push, gh-watch the 5-leg run, human-verify all green (CICD-08)

### Phase 08.1: multi_vo parallel VO legs + xdist for forwarded suites (INSERTED)

**Goal:** Bring `simple-autotest.yml` wall time to at/below legacy autotest (~36 min) via two
parallelism fixes: (A) inject xdist into the FORWARDED container runs so remote_dbs/votest stop
running serially (~41→~15, ~21→~8 min), and (B) split multi_vo's tst/ts2 into two parallel matrix
legs (~51→~25 min). Net target ~20-25 min. Every leg stays green with its own report.
**Requirements**: CICD-08 (perf optimization of the CI legs; no new requirement IDs)
**Depends on:** Phase 8
**Plans:** 4/4 plans complete
- [x] 08.1-01-PLAN.md — multi_vo single-VO selector (RUCIO_MULTI_VO_LEG) + per-VO compose project name (wave 1)
- [x] 08.1-01b-PLAN.md — inject xdist into forwarded container runs (remote_dbs/votest) via forwarding.py + tests (wave 1)
- [x] 08.1-02-PLAN.md — workflow multi_vo matrix split (tst/ts2) + per-leg unique naming (wave 2)
- [x] 08.1-03-PLAN.md — live-CI verify BOTH levers: GREEN run 28503879697 (sha 70cdaafa8), 7/7 jobs, multi_vo tst/ts2 parallel + forwarded xdist material (remote_dbs 41->28m, votest 21->15m), total wall 51.5m->30.8m (below legacy ~36m) (wave 3)

### Phase 9: Plugin docs
**Goal**: A developer can learn to use the plugin from a README without reading the code.
**Depends on**: Phase 8
**Requirements**: DOC-01
**Success Criteria**:
  1. `tests/ruciopytest/README.md` documents the suite table and every CLI option with runnable examples
  2. Examples cover host-side, forwarded, `--keep-db`, `--infra`, and `--dry-run`/`--co` usage

**Plans:** 1 plan
- [x] 09-01-PLAN.md — write tests/ruciopytest/README.md (Quickstart → How it works → Suite table → CLI reference → Examples → CI mapping → Troubleshooting), grounded in plugin.py/profiles.py/forwarding.py/multi_vo_support.py + post-8.1 simple-autotest.yml (DOC-01)

### Phase 10: Docs & traceability cleanup (gap closure — audit v1.1)
**Goal**: The shipped docs and planning metadata are accurate — no reader is misled and traceability is internally consistent.
**Depends on**: Phase 9
**Requirements**: DOC-01 (accuracy hardening; already satisfied), plus planning-metadata fixes (no new REQ IDs)
**Gap Closure**: Closes tech-debt items from `.planning/v1.1-MILESTONE-AUDIT.md`
**Success Criteria**:
  1. `tests/ruciopytest/README.md` no longer claims `RUCIO_MULTI_VO_LEG` "defaults to `tst`" (lines ~105 and ~348-349); it states that unset/unrecognized runs both VOs sequentially, matching `infra_manager.py:388-400` and the README's own "How it works" section
  2. `07-03-SUMMARY.md` frontmatter lists `requirements-completed: [SUIT-09]`
  3. `.planning/STATE.md` frontmatter milestone reconciled to `v1.1` (matching ROADMAP/REQUIREMENTS; `v1.0` = shipped Pytest Test-Runner)

**Plans:** 1/1 plans complete
- [ ] 10-01-PLAN.md — README RUCIO_MULTI_VO_LEG "defaults to tst" fix (2 spots) + 07-03-SUMMARY requirements-completed:[SUIT-09] backfill + STATE.md milestone v1.0→v1.1 reconcile (DOC-01)

### Phase 11: simplify_votests workflow (gap closure — audit v1.1)
**Goal**: The policy (votest) tests run in CI for BOTH policies, mimicking legacy `vo_tests.yml` behaviour but driven by the ruciopytest plugin — restoring the belleii coverage that PR CI currently omits.
**Depends on**: Phase 8.1 (post-split votest leg shape), Phase 7 (votest POLICY selection)
**Requirements**: SUIT-07 (votest coverage extended to belleii; already satisfied for atlas)
**Gap Closure**: Closes the votest-belleii CI-coverage gap from `.planning/v1.1-MILESTONE-AUDIT.md`
**Success Criteria**:
  1. New `.github/workflows/simplify_votests.yml` exists, triggered on `schedule` (nightly) + `workflow_dispatch` (mirroring legacy `vo_tests.yml` cadence, not per-PR)
  2. It runs a policy matrix of both `atlas` and `belleii` via the plugin (`python -m pytest --suite=votest --policy=<X>`), with runtime-images, host deps, junit artifacts, and a per-policy report
  3. Both policy legs pass green on a manual `workflow_dispatch` run
  4. Decision recorded (in the plan) on whether the fast atlas smoke stays in `simple-autotest.yml` per-PR or votest moves entirely into the new workflow (pure legacy match)

> **Note:** Success criterion #1's "not per-PR" wording is SUPERSEDED by the 11-CONTEXT locked
> decision — upstream legacy `vo_tests.yml` runs votest on `pull_request` + `push` +
> `workflow_dispatch` + `schedule`; the new workflow matches that (per-PR + push + nightly).
> Per 11-CONTEXT, votest is REMOVED from `simple-autotest.yml` and lives only in the new workflow.

**Plans:** 2/2 plans complete
- [ ] 11-01-PLAN.md — create `simplify_votests.yml` (static atlas+belleii matrix, plugin-driven, legacy cadence) + remove votest leg from `simple-autotest.yml` (SUIT-07)
- [ ] 11-02-PLAN.md — live-CI verify: push/dispatch, drive both legs, fix belleii plugin-path failures until green, human-verify (SUIT-07)

### Phase 12: multi_vo legacy parity (gap closure — audit v1.1)
**Goal**: `multi_vo` CI runs both VOs (tst→ts2) **sequentially against one shared instance/DB**, matching legacy `run_multi_vo_tests_docker.sh`, restoring the shared-DB multi-tenancy coverage and stop-on-failure gate that the Phase 8.1 parallel split removed. Deliberately trades the 8.1 wall-time win for legacy-faithful correctness.
**Depends on**: Phase 8.1 (undoes its multi_vo matrix split), Phase 7 (multi_vo 2-VO setup)
**Requirements**: SUIT-08 (multi_vo faithfulness; already satisfied), closes the multi_vo integration gap from the audit
**Gap Closure**: Closes the "multi_vo tst→ts2 shared-DB sequential gate has no live-CI coverage" gap from `.planning/v1.1-MILESTONE-AUDIT.md` (Item 4)
**Rationale (grounded in upstream data)**: rucio/rucio autotest runs multi_vo as one leg per Python version at ~33-37 min (both VOs sequential, shared DB, xdist inside), parallelized only across Python versions — not across VOs. This is the legacy behaviour to match.
**Success Criteria**:
  1. `simple-autotest.yml` runs `multi_vo` as a **single** matrix leg with `RUCIO_MULTI_VO_LEG` **unset**, so `InfraManager.run_multi_vo()` takes the sequential shared-DB path (`infra_manager.py:402-428`): tst runs first, ts2 runs only on tst success, no DB reset between VOs, both against the same instance
  2. The two parallel per-VO legs (`multi_vo-tst` / `multi_vo-ts2` on separate compose stacks) introduced in 8.1 are removed from the matrix
  3. The multi_vo leg is green on a PR run, with tst→ts2 both executed on one shared DB (verify via logs: "Running tests for VO tst" then "Running tests for VO ts2", no inter-VO reset)
  4. junit/report/naming for the single multi_vo leg restored to non-split form; README CI-mapping + any 8.1 parity notes updated to reflect the reverted single-leg model and the accepted ~35 min long-pole

**Plans:** 2/2 plans complete
- [x] 12-01-PLAN.md — revert 8.1 matrix split: single multi_vo leg (no `vo:`, RUCIO_MULTI_VO_LEG unset → sequential shared-DB path) in simple-autotest.yml + README CI-mapping/parity doc sync (SUIT-08)
- [x] 12-02-PLAN.md — live-CI verify: multi_vo leg GREEN on PR run 28787193259 (sha f2643a959, 1240 passed/0 failed); sequential shared-DB branch proven via RUCIO_MULTI_VO_LEG-empty discriminator + tst-streamed pass, non-split naming (test-results-multi_vo-py3.9) — closes audit Item-4 (SUIT-08) (completed 2026-07-06)

## Progress

| Phase | Milestone | Plans | Status | Completed |
| ----- | --------- | ----- | ------ | --------- |
| 1. Plugin Skeleton | v1.0 | 2/2 | Complete | 2026-02-20 |
| 2. Database Lifecycle | v1.0 | 2/2 | Complete | 2026-03-05 |
| 3. Container Lifecycle | v1.0 | 3/3 | Complete | 2026-03-09 |
| 4. Test Collection | v1.0 | 2/2 | Complete | 2026-03-11 |
| 5. CI Workflow | v1.0 | 2/2 | Complete | 2026-03-13 |
| 6. Host Forwarding | v1.0 | 4/4 | Complete | 2026-06-15 |
| 7. Suite Parity | v1.1 | Complete    | 2026-06-23 | - |
| 8. CI for Real | v1.1 | 4/4 | Complete | 2026-07-01 |
| 8.1 Multi-VO Parallel Legs | v1.1 | 4/4 | Complete | 2026-07-01 |
| 9. Plugin Docs | v1.1 | 0/1 | Planned | - |
| 10. Docs & Traceability Cleanup | 1/1 | Complete    | 2026-07-02 | - |
| 11. simplify_votests Workflow | 2/2 | Complete    | 2026-07-06 | - |
| 12. multi_vo Legacy Parity | 2/2 | Complete   | 2026-07-06 | - |
