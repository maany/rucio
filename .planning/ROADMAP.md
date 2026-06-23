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
- [ ] Phase 8: CI for real — host deps install, full rucio for host suites, artifacts/junit, all 6 legs green
- [ ] Phase 9: Plugin docs — tests/ruciopytest/README.md

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
**Goal**: `simple-autotest.yml` actually runs and goes green for all 6 matrix legs, with artifacts.
**Depends on**: Phase 7
**Requirements**: CICD-05, CICD-06, CICD-07, CICD-08
**Success Criteria**:
  1. Each matrix leg installs host Python + plugin deps and reaches the pytest invocation
  2. `client` and `sqlite` legs have a full rucio install and execute host-side
  3. Every leg uploads junit XML and renders a test report
  4. All 6 legs pass green on a PR to maany/rucio

### Phase 9: Plugin docs
**Goal**: A developer can learn to use the plugin from a README without reading the code.
**Depends on**: Phase 8
**Requirements**: DOC-01
**Success Criteria**:
  1. `tests/ruciopytest/README.md` documents the suite table and every CLI option with runnable examples
  2. Examples cover host-side, forwarded, `--keep-db`, `--infra`, and `--dry-run`/`--co` usage

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
| 8. CI for Real | v1.1 | 0/? | Not started | - |
| 9. Plugin Docs | v1.1 | 0/? | Not started | - |
