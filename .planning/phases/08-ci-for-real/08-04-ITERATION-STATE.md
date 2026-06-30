# 08-04 Iteration State (CI-faithfulness grind)

Working state for the live CI green effort. This is scratch coordination, not the SUMMARY.

## Target
- Fork PR: maany/rucio PR #2 — https://github.com/maany/rucio/pull/2
- Branch: clean-autotests. gh defaults to upstream; ALWAYS use `--repo maany/rucio`.
- Workflow: "Simple Autotests" (.github/workflows/simple-autotest.yml), 5 legs:
  remote_dbs(3.9), remote_dbs(3.10), multi_vo(3.9), client(3.9), votest(3.9, atlas).
- Exit bar (CICD-08): all 5 legs green in one PR run, artifacts/reports rendered.
- SCOPE RULE: fix infra/provisioning/bootstrap-parity gaps. Do NOT patch product tests.
  A test that fails the SAME way it would under legacy autotest = OUT OF SCOPE (inherit).

## Commits so far (on clean-autotests)
- 64385b0b9  fix: reusable-workflow caller needs `contents: read` (startup_failure)
- 31624460b  fix: forwarding INTERNALERROR — skip-report longrepr re-tupled in replay_report_line + regression test
- 39aff0ba5  fix: client host pip urllib3 conflict (install server+dev env, continue-on-error fallback)
- e3d6fbb24  feat: ContainerManager.start() bridges mounted source into RUCIO_HOME (symlink bin/* onto PATH, fill missing etc/ fixtures: mail_templates/, google-cloud-storage-test.json, rse_repository.json without clobbering entrypoint cfg); client in-container fallback pip install -e /rucio_source before run_tests.sh -i; workflow uploads .test-forward/ (include-hidden-files: true)

## Latest run = 28444418286 (sha e3d6fbb24) — conclusion: failure
runtime_images: success. Per-leg diagnosis (from artifact + log triage):

### remote_dbs 3.9 / 3.10 — bootstrap SOLVED (108 → 9 failures)
- Counts: 1673 run, 1244 passed, 420 skipped, 9 failed (both legs identical).
- The ~99 `rucio: command not found` / exit-127 / missing-etc-fixture failures are GONE.
- Residual 9 (genuine-looking, need legacy-parity check — several smell like missing daemon/service, i.e. possibly still infra):
  - tests/test_cli_client_structure.py — InvalidRSEExpression
  - TestJudgeRepairer — ConnectionRefusedError [Errno 111]
  - tests/test_oidc.py — AssertionError assert None == 'eyJhbG...'
  - tests/test_reaper.py:65 ×5 — rucio.common.exception.DatabaseException
  - tests/ruciopytest/test_plugin_votest.py:88 — RuntimeError: Failed to purge database
- INFRA NIT (forces container exit 1 regardless of the 9):
  PermissionError [Errno 13] writing junit /test-results/remote_dbs-py3.9.xml to host-mounted dir (container UID vs host-mounted dir perms). MUST fix — leg can't go green while pytest exits 1 on XML write.

### client 3.9 — bootstrap now RUNS; config-parity gap
- 79 collected: 28 passed, 1 skipped, 50 failed (40 failed + 10 errors).
- EVERY failure in tests/test_bin_rucio.py = configparser.NoSectionError: No section: 'database'.
- Cause: client container rucio.cfg has no [database] section that test_bin_rucio fixtures need.
- IN SCOPE (config generation parity for client leg).

### votest 3.9 (atlas) — exit 3 / collected 0 — INFRA path bug
- Container traceback: infra_manager.py:363 _apply_votest_policy →
  RuntimeError: votest: live rucio.cfg not found: /home/runner/work/rucio/rucio/rucio.cfg
- Crash in pytest_configure (plugin.py:295 → infra_manager.setup:85 → _apply_votest_policy:363).
- Cause: _apply_votest_policy looks for live cfg at HOST repo path instead of container RUCIO_HOME (/opt/rucio/etc/rucio.cfg). DB setup itself succeeded ("Base VO and root account created").
- IN SCOPE. Clear fix: resolve cfg path from RUCIO_HOME inside the container.

### multi_vo 3.9 — exit 3 / collected 0 — INFRA non-idempotent bootstrap
- Container traceback: psycopg UniqueViolation "VOS_PK" Key (vo)=(def) already exists →
  infra_manager.py:169 run_multi_vo → :147 bootstrap_vo → :326 _create_base_vo_and_root_account → RuntimeError("Failed to build database"), from pytest_configure → collected 0 → exit 3.
- Cause: main setup already created base VO `def`; run_multi_vo()→bootstrap_vo(TST_HOME)→_create_base_vo_and_root_account() re-inserts `def`. Not idempotent (no already-exists guard).
- Everything before that succeeded (DB build, tst/ts2 configs, httpd restart, data/RSE/metadata sync).
- IN SCOPE. Fix: make base-VO/root creation idempotent OR don't recreate def in per-VO bootstrap.

## Batch applied on top of e3d6fbb24 (items 1-4) — pushed, awaiting run
- 013f5e2dc  fix(08-04): votest cfg path + idempotent base-VO/root in infra_manager
    - Item 1 (votest): `_apply_votest_policy` now resolves the live cfg via new
      `_resolve_live_rucio_cfg()` ($RUCIO_HOME/etc/rucio.cfg → $RUCIO_HOME/rucio.cfg
      → /opt/rucio/etc/rucio.cfg), not the inherited host path. Fixes votest exit 3.
    - Item 2 (multi_vo): `_create_base_vo_and_root_account` is now idempotent via
      `_is_already_exists_error()` (swallows Duplicate / UniqueViolation VOS_PK /
      IntegrityError, then session.remove()). Per-VO bootstrap over shared schema
      no longer crashes collection (exit 3).
- b0b680435  fix(08-04): strip --junitxml from forwarded container pytest args
    - Item 4 (remote_dbs junit PermissionError): `build_inner_pytest_args` now drops
      `--junitxml=PATH` and `--junitxml PATH`. Host junitxml plugin already produces
      the report from replayed reports (FWD-06) and owns the mounted path; the
      container's redundant write was the Errno 13 that forced container exit 1.
      Updated test_forwarding.py (passes-through test split; added strip test).
- 41378b84d  ci(08-04): give client leg rucio.cfg a [database] section
    - Item 3 (client NoSectionError): in-container client fallback appends
      etc/docker/test/extra/rucio_${RDBMS}.cfg ([database]-only) after copying
      rucio_client.cfg, so test_bin_rucio's server-side fixtures get a DB section.

Local sanity before push: YAML parse OK; infra_manager/forwarding import OK;
test_forwarding.py 41 passed/1 skipped; test_multi_vo_support.py 17 passed.

NOTE for next run triage:
- Host client provisioning copies rucio_client.cfg to CLIENT_HOME but the
  `postgres14` service is NOT published in docker-compose.ports.yml (only
  `ruciodb`), so the host path cannot reach the DB for server-side fixtures.
  The in-container fallback is the intended green path for client; if a future
  run shows reachable=true (host path) failing on DB, publish/point the DB or
  add [database] to CLIENT_HOME cfg pointing at the published port.

## Remaining fix list (batch into few pushes)
1. votest: _apply_votest_policy resolves live rucio.cfg from container RUCIO_HOME, not host repo path.
2. multi_vo: idempotent base-VO/root creation (guard VOS_PK already-exists) so per-VO bootstrap doesn't recrash.
3. client: ensure client leg rucio.cfg includes a [database] section (config-generation parity vs legacy client cfg).
4. remote_dbs junit-XML PermissionError: make container pytest able to write test-results/ junit (fix host-mounted dir perms / UID, or write junit to a container path then copy). Leg can't go green until container pytest exits 0.
5. remote_dbs residual 9: AFTER 1-4, re-run and re-examine. For each, determine if it fails the SAME way under LEGACY autotest (check tools/test/test.sh + legacy autotest.yml selection / known xfail-skip). If legacy is green on it → it's an infra delta (missing daemon/service in our bring-up) → IN SCOPE fix. If legacy also fails/xfails it identically → OUT OF SCOPE, inherit. Do NOT patch the product tests themselves.

## Run 28452774335 (sha da28886d7, items 1-4) — conclusion: failure BUT all 4 fixes worked
client = SUCCESS (green). All other legs ADVANCED past their prior crashes.

| Leg | Now |
|-----|-----|
| client 3.9 | GREEN ✓ (the rucio_${RDBMS}.cfg [database] append fixed it) |
| votest 3.9 | collects+runs: 328 passed, 6 failed, 65 skipped, 1 xfail |
| multi_vo 3.9 | collects+runs: 1157 passed, 101 failed, 416 skipped |
| remote_dbs 3.9/3.10 | PermissionError GONE; 1244 passed, 9 failed, 414 skipped (identical) |

### Shared root causes of remaining failures
A. `data13_hip` scope FK — reaper ×5 (remote_dbs AND votest). test_reaper.py hardcodes scope `data13_hip`
   (lines 78,116,386,446,472), never creates it (no scope_factory). bootstrap_tests.py adds an explicit
   scope list ONLY for belleii policy; atlas/tst gets only jdoe/mock + root/archive. => provision data13_hip
   (or mirror however legacy makes it exist). VERIFY legacy creates it before assuming in-scope.
B. mock RSEs not provisioned — test_cli_client_structure::test_rse InvalidRSEExpression "empty set"
   (remote_dbs AND votest). Legacy runs tools/docker_activate_rses.sh; simple pipeline doesn't. => run RSE
   activation as part of bring-up (mirror legacy).
C. multi_vo CannotAuthenticate to account root (~90 of multi_vo's 101) across test_bin_rucio,
   test_cli_client_structure, TestOpenDataCLI, TestRucioServer, test_upload. Per-VO root creds / client cfg
   don't match the multi_vo server. => multi_vo client-auth wiring delta. Biggest multi_vo lever.
   (multi_vo also has a few test_bb8 ScopeNotFound + the shared 5× reaper FK.)

### Needs legacy-parity DECISION (match legacy, do NOT stand up new services)
D. test_oidc (assert None == token) — needs OIDC IdP/IAM. Legacy gates OIDC behind needs_iam marker /
   separate integration workflow. If legacy does NOT run oidc in the standard suite, INHERIT the skip
   (match legacy selection), don't stand up IAM. CHECK legacy selection/markers.
E. TestJudgeRepairer ConnectionRefused [Errno 111] — needs a listening service legacy provides. CHECK
   whether legacy runs/skips it in this suite; match.
F. test_plugin_votest::test_votest_configure_stashes_atlas_test_paths "Failed to purge database" — our OWN
   plugin meta-test that purges the live DB mid-suite. Self-inflicted: this meta-test should not run inside
   the live product suite (it's a unit test of the plugin). Exclude it from the suite selection / mark it so
   it doesn't run against the live test DB.

### Next batch plan (items 6-9)
6. Provision `data13_hip` scope in bring-up (clears reaper ×5 on remote_dbs + votest). Mirror legacy.
7. Run mock-RSE activation (tools/docker_activate_rses.sh) in bring-up (clears test_rse on remote_dbs + votest).
   -> 6+7 should make VOTEST GREEN and drop remote_dbs to ~3 (judge, oidc, plugin_votest meta-test).
8. multi_vo client-auth: make per-VO client cfg/root creds authenticate to the multi_vo server (clears ~90).
9. Legacy-parity for oidc/judge/plugin-votest-meta: inspect legacy selection (tools/test/test.sh,
   autotest.yml, pytest markers needs_iam/skip/xfail) and MATCH it (inherit skips / exclude meta-test),
   rather than building new services. Document each decision.

## Batch applied on top of da28886d7 (items 6/7/9) — pushed, awaiting run
Grounded on the artifact for run 28452774335 (host-logs-remote_dbs-py3.9):
exact tracebacks read for every residual. KEY DISCOVERY: missing **memcached**
is the single root cause behind THREE of the "decision" residuals, and the
mock-RSE-activation hypothesis (old item 7) was WRONG.

- 36d0346cb  fix(08-04): memcached + data13_hip scope in container bring-up
  - **memcached (legacy parity, IN SCOPE).** infra_manager.setup() now starts
    `memcached -u root -d` (new `_start_memcache`, idempotent) mirroring
    run_tests.sh:17. Live cfg keeps `[cache] url = 127.0.0.1:11211`
    (rucio.common.cache.CACHE_URL default), so the dogpile MemcacheRegion is
    REQUIRED in-container. Our plugin path replaced run_tests.sh and never
    started it. Evidence from the artifact:
      * `test_oidc::test_token_cache` -> `assert None == 'eyJ...'`: the OIDC token
        cache IS memcache-backed; set/get to a dead socket -> get None. NOT an
        IAM/OIDC dependency (hypothesis D was wrong; test_oidc has no needs_iam
        marker and legacy runs it in the standard suite — it passes there only
        because legacy starts memcached).
      * `TestJudgeRepairer` -> `ConnectionRefused [Errno 111]` straight out of
        `region.delete` -> pymemcache `_connect` to 127.0.0.1:11211 (hypothesis
        E: the "listening service legacy provides" = memcached).
      * `test_cli_client_structure::test_rse` -> `InvalidRSEExpression: empty
        set` at line 694 `list_rses(rse_name)` AFTER `rse remove`. Mechanism:
        parse_expression() caches results in MemcacheRegion (expiry 600s). With
        memcache UP, line 669's lookup caches {rse}; after removal line 694 is a
        cache HIT -> returns the cached list, no re-eval, assert passes (== legacy).
        With memcache DOWN every call is a MISS, so line 694 re-evaluates the
        now-removed RSE -> empty set -> raises. So test_rse is ALSO a memcache
        delta, NOT a mock-RSE-provisioning gap. The test creates+removes its OWN
        RSE, so docker_activate_rses.sh would not affect it.
  - **data13_hip scope (legacy parity, IN SCOPE).** Verified legacy creates it:
    `tools/sync_meta.py:61-65` does `c.add_scope('root', value)` for every
    `project` metadata value. infra_manager._sync_metadata() omitted that.
    Added it (Duplicate-tolerant). Artifact proof: reaper x5 fail with
    `ForeignKeyViolation DIDS_SCOPE_FK: Key (scope)=(data13_hip) not present in
    table "scopes"`.
- 3d5e61aed  fix(08-04): exclude_paths=('tests/ruciopytest/*',) on
  remote_dbs/multi_vo/votest. test_plugin_votest meta-test re-enters
  plugin.pytest_configure (in-container -> manager.setup() -> _purge_database)
  and purges the live DB mid-suite ("Failed to purge database"). These plugin
  meta-tests are new Phase-8 additions legacy never collected; standalone runs
  unaffected (plugin dormant without --suite). votest already excluded them via
  explicit matrix test_paths; this is belt-and-suspenders + the real fix for the
  tests/-glob suites.

### Legacy-parity DECISIONS this batch (each from artifact + legacy evidence)
- **item 7 / mock-RSE activation: DO NOT DO IT.** Legacy `tools/test/test.sh`
  runs `run_tests.sh` WITHOUT `-r` for remote_dbs/votest/multi_vo, so legacy
  NEVER runs docker_activate_rses.sh in these suites (it needs XRD/FTS storage
  containers absent here). The test_rse failure it was meant to fix is actually
  the memcache delta above. Standing up RSE activation would be NON-faithful.
- **test_oidc::test_token_cache: FIX (memcache), not inherit-skip.** It is not an
  IAM test; it round-trips the memcache token cache. Fixed by starting memcached.
- **TestJudgeRepairer: FIX (memcache).** Legacy runs it and passes because
  run_tests.sh starts memcached; our delta was the missing daemon.
- **test_plugin_votest meta-test: EXCLUDE from live suite.** Self-inflicted
  plugin unit test; not a legacy product test.

Expected after this run: data13_hip clears reaper x5 (remote_dbs + votest);
memcached clears test_rse + test_oidc + judge; exclusion clears the plugin
meta-test. remote_dbs 9 residuals should -> 0 (GREEN). votest 6 -> likely 0
(GREEN). multi_vo still has the ~90 CannotAuthenticate (item 8, deferred) plus
the now-fixed reaper x5 / cache tests.

### Item 8 (multi_vo CannotAuthenticate) — root-cause note for next batch (DEFERRED)
Skimmed run_multi_vo / per-VO bootstrap + legacy run_multi_vo_tests_docker.sh:
- Legacy (run_multi_vo_tests_docker.sh) for the tst VO does a FULL
  `tools/reset_database.py` (line 67) WITH `RUCIO_HOME=/opt/rucio/etc/multi_vo/tst`
  ACTIVE (cfg has multi_vo=True, vo=testvo1). So create_base_vo/create_root_account
  AND bootstrap_tests.py's `add_vo(issuer='super_root', vo=DEFAULT_VO)` all run
  under the multi_vo config, provisioning super_root + the per-VO root identity.
  ts2 then re-bootstraps with NO second DB reset.
- OUR infra_manager builds the DB once in the MAIN setup() (under the default
  /opt/rucio cfg, multi_vo possibly false), creates base VO `def` + root there,
  THEN run_multi_vo() re-bootstraps tst/ts2 over that schema. Our item-2
  idempotency guard makes `_create_base_vo_and_root_account` SWALLOW already-exists
  on the per-VO pass — which likely MASKS provisioning of each VO's root userpass
  identity. The client cfg authenticates as username=ddmlab/password=secret →
  account=root (rucio_autotests_common.cfg); `create_root_account()` (lib/rucio/
  db/sqla/util.py:168-187) wires that ddmlab userpass identity. If testvo1/testvo2
  root never gets the ddmlab identity (because add_vo ran under the wrong
  cfg/issuer context, or the guard skipped it), the client gets CannotAuthenticate.
- Next batch: make the DB build/bootstrap happen with RUCIO_HOME pointed at the
  tst (multi_vo) cfg BEFORE create_base_vo/root (mirror reset under tst), and
  verify add_vo provisions each VO root's ddmlab userpass identity (or add it
  explicitly per VO in bootstrap_vo). Confirm a `super_root` account+identity
  exists in `def` so add_vo(issuer='super_root') succeeds.

### Item 8 FIX applied (run 28456808306 had multi_vo as the lone red; 4/5 green)
Verified the exact legacy chain in source before coding:
- `lib/rucio/db/sqla/util.py::create_root_account()` branches on
  `common.multi_vo`: when True it creates the **`super_root`** account (in
  DEFAULT_VO `def`) with the `ddmlab`/`secret` USERPASS identity; when False it
  creates **`root`**.
- `lib/rucio/core/vo.py::add_vo()` creates each per-VO `root` and then
  `for ident in list_identities(InternalAccount('super_root', vo=def)): add_account_identity(... root@<vo> ...)`
  — i.e. per-VO root inherits ddmlab ONLY IF super_root exists with it.
- ROOT CAUSE: our base `setup()` ran under the DEFAULT cfg (multi_vo=False), so
  create_root_account created `root`, never `super_root`. add_vo's gateway
  permission passes on the *string* `super_root` even when the account row is
  absent, so `add_vo` "succeeded" but `list_identities(super_root)` was empty →
  root@testvo1/testvo2 got only `root@<vo>/password`, never ddmlab/secret →
  client (ddmlab/secret) `CannotAuthenticate`. SECONDARY bug: the parent-process
  per-VO bootstrap never reloaded the config singleton after re-pointing
  RUCIO_HOME, so `_bootstrap_test_data` kept stale `vo`/`multi_vo` and the second
  VO's `add_vo` effectively never ran for ts2.

FIX (infra-only, `tests/ruciopytest/infra_manager.py`):
1. `setup()` multi_vo branch now generates the per-VO cfgs and calls new
   `_activate_multi_vo_base_config()` (sets `RUCIO_HOME=/opt/rucio/etc/multi_vo/tst`
   + `clean_cached_config()`) BEFORE purge/build/create — so the whole base
   bring-up runs under multi_vo=True and `create_root_account()` provisions
   `super_root` + ddmlab. Mirrors run_multi_vo_tests_docker.sh exporting
   RUCIO_HOME=tst before reset_database.py. The later `_setup_multi_vo()` call is
   now guarded to non-multi_vo (already generated above).
2. `bootstrap_vo()` now `clean_cached_config()` after re-pointing RUCIO_HOME, so
   `_bootstrap_test_data` reads THIS VO's `[client] vo` (testvo1 vs testvo2) and
   `multi_vo=True`, ensuring `add_vo(testvo2)` actually runs for ts2 and copies
   ddmlab onto root@ts2.
All per-VO cfgs share the same `[database]` (postgres14/rucio, schema=dev) so the
cached DB session/engine is unaffected — only the config singleton is dropped.

Why this fixes the ~90 CannotAuthenticate: super_root now exists in `def` with
ddmlab; add_vo copies ddmlab onto each per-VO root; client authenticates
ddmlab/secret → root@<vo>. Scope: provisioning/bootstrap-parity only; no product
test touched.

Residual multi_vo failures expected to remain (NOT auth, OUT OF SCOPE / inherit):
- a few `test_bb8` ScopeNotFound and the shared reaper FK class if any survive —
  these fail the SAME way under legacy bring-up (no extra scope provisioning in
  legacy multi_vo path) → inherit, do NOT patch product tests. Re-assess against
  the live artifact if multi_vo is still red.

Local sanity: infra_manager ast-parse + import OK; clean_cached_config import OK;
test_multi_vo_support.py 6 passed (incl. setup-order guard).

### Item 8 FOLLOW-UP (run 28460113280, sha 9662bdeb2): auth FIXED, new build regression
Coordinator artifact (multi_vo job 84345461928): ZERO CannotAuthenticate (the
super_root/ddmlab fix worked). But the base bring-up now runs under the tst cfg,
and `_build_database()` did:
  `alembic_cfg = Config(config_get('alembic','cfg')); command.stamp(alembic_cfg,'head')`
-> `CommandError: No 'script_location' key found in configuration` -> "Failed to
build database" -> no tests ran, container pytest exit 3.

ROOT CAUSE: the per-VO source cfgs (etc/docker/test/extra/rucio_multi_vo_*_
postgres14.cfg) override `[alembic] cfg = /opt/rucio/etc/multi_vo/<vo>/etc/
alembic.ini`. Legacy's multi_vo docker image creates those per-VO alembic.ini
files; the simple-autotest runtime image does NOT. So config_get('alembic','cfg')
returned a path to a non-existent ini -> alembic Config with no script_location.
The base cfg (rucio_autotests_common.cfg) points [alembic] cfg at
/opt/rucio/etc/alembic.ini, which DOES exist (remote_dbs builds fine via it).

FIX (infra/bootstrap-parity, `tests/ruciopytest/multi_vo_support.py`):
generate_multi_vo_configs() now post-processes each generated cfg via new
`_carry_over_section(base, dest, "alembic")`, forcing the base cfg's [alembic]
section (cfg=/opt/rucio/etc/alembic.ini) back over the per-VO override while the
last-source-wins per-VO DB/VO settings (vo=testvo1/2, multi_vo=True) are kept.
Verified locally: generated tst/ts2 cfgs now have
`[alembic] cfg=/opt/rucio/etc/alembic.ini`, vo=testvo1/testvo2, multi_vo=True.
Did NOT edit the source extra/ cfgs (shared with legacy, which needs the per-VO
path). test_multi_vo_support.py 6 passed.

Expected next: _build_database() succeeds under tst cfg -> per-VO bootstrap runs
(validates super_root/ddmlab) -> multi_vo tests execute. Residual if any: a few
test_bb8 ScopeNotFound = OUT OF SCOPE / inherit legacy; do NOT patch product tests.

### Batch applied on top of dd159a417 — the last 3 multi_vo failures
Context at start: 4/5 legs GREEN; multi_vo fully bootstraps + runs (tst sub-run
1232 passed / 378 skipped / 2 failed, +1 in the plugin pass). 3 child-run
failures to close:
  (a) tests/ruciopytest/test_plugin_votest.py::test_votest_configure_stashes_atlas_test_paths
      -> "Failed to purge database" (DROP TYPE ... DependentObjectsStillExist)
  (b) tests/test_bad_replica.py::test_rest_bad_replica_methods_for_ui  (assert 369==364, off-by-5)
  (c) tests/test_did.py::TestDIDClients::test_list_recursive (cross-scope attach -> masked DatabaseException)

ROOT-CAUSE (single shared cause for all 3 = OUR harness execution-model delta):
`run_multi_vo()` spawned the per-VO child as a BARE **serial** `pytest tests/ -v
--tb=short` with NO `--suite`. Two consequences, both pure infra/harness-parity
gaps vs legacy `tools/run_multi_vo_tests_docker.sh` -> `tools/pytest.sh -v --tb=short`:
  1. No `--suite` => rucio plugin dormant in the child => collection.py's
     exclude_paths filter never applied => the Phase-8 plugin meta-test
     `test_plugin_votest` was COLLECTED and run. Inside the container its
     `_in_container` branch (`/.dockerenv` exists) re-enters
     `plugin.pytest_configure -> InfraManager.setup -> _purge_database`, purging
     the LIVE DB mid-suite => failure (a).
  2. Bare `pytest` ran SERIALLY with NO xdist. Legacy `tools/pytest.sh` runs the
     suite under pytest-xdist (`--numprocesses=3` on GitHub Actions, `auto`
     locally); with xdist present `tests/conftest.py:76-79` registers the rucio
     **noparallel scheduler** so `@pytest.mark.noparallel` tests are isolated/
     ordered exactly as under legacy. Both (b) and (c) ARE noparallel tests
     (`test_bad_replica` noparallel 'runs minos, acts on all bad pfns';
     `test_list_recursive` noparallel 'uses pre-defined scope names'); their
     shared-DB off-by-5 leak / cross-scope masked DatabaseException are
     execution-model/ordering symptoms of running serially without the
     scheduler that legacy uses. => IN-SCOPE harness parity (the scope rule
     explicitly lists "execution-model/xdist isolation differing from legacy" as
     in-scope to fix).

FIX (infra-only, `tests/ruciopytest/infra_manager.py`): new
`_multi_vo_pytest_cmd()` builds the per-VO child argv to mirror legacy:
  - xdist parity: `-p xdist --numprocesses=3` (GITHUB_ACTIONS) / `=auto` locally,
    gated on `profile.xdist_enabled`. This re-engages the noparallel scheduler.
  - exclusion parity: translate `profile.exclude_paths` (`tests/ruciopytest/*`)
    into `--ignore-glob=tests/ruciopytest/*` + `--ignore=tests/ruciopytest`, so
    the child skips the plugin meta-tests (legacy never carried these files, so
    this preserves legacy product-test selection exactly).
`run_multi_vo()` now calls the helper. Added unit test
`test_multi_vo_pytest_cmd_excludes_plugin_metatests_and_uses_xdist`.

PARITY VERDICTS:
- (a) IN-SCOPE, FIXED. Definitive: our exclusion didn't reach the child run.
  Preferred per item-9 decision (exclude the meta-test from the live suite, NOT
  make mid-suite purge work). Verified locally: child argv now carries the
  ignores; test_plugin_votest + test_multi_vo_support + forwarding stay green.
- (b)/(c) IN-SCOPE harness delta (serial-no-xdist vs legacy xdist+noparallel
  scheduler), addressed by the xdist parity fix. NOTE: cannot prove locally
  (needs the live DB/container) that xdist fully clears them — the live CI run
  is the verification. Justification stands independently as faithfulness
  (legacy multi_vo demonstrably runs per-VO with xdist; we didn't). IF either
  survives under faithful xdist execution, that's the signal it's an inherited
  product failure legacy also exhibits -> bring the inherit decision to the user
  (do NOT patch test_bad_replica / test_did).

Local sanity: infra_manager AST+import OK; test_multi_vo_support.py +
test_plugin_votest.py 9 passed; test_forwarding.py 41 passed/1 skipped.
Pushed NORMAL (no force). Awaiting orchestrator watch.

## Watch protocol
- After pushing, hand orchestrator the new sha + run id. Orchestrator watches and returns per-leg results + artifacts.
- Useful: gh run view <id> --repo maany/rucio --json jobs --jq '.jobs[]|"\(.databaseId) \(.name) \(.conclusion)"'
  gh run view --repo maany/rucio --job <id> --log-failed | tail -150
  gh run download <id> --repo maany/rucio -n host-logs-<leg>-py3.9 -D <dir>  (then read .test-forward/*.container-stdout.log)
- No force-push (histories aligned). No fabricated green.

## LOCAL REPRO + VERIFICATION (sha 282ce5a43, no push) — multi_vo double-exec + test_did

Goal: reproduce the multi_vo leg LOCALLY, root-cause the `test_did` unknown from
the SERVER log, develop+verify the wiring fix, report BEFORE pushing. CI is slow
(~30 min); local repro lets us read the apache/wsgi error behind the masked
DatabaseException and iterate fast.

### Local repro setup (WORKED)
- Host driver venv: system `python3.12` venv (no 3.9 on host; pyenv only had
  3.13) with `pytest==7.4.3 pytest-xdist==3.5.0 pyyaml` — pytest pinned to EXACTLY
  the container's (7.4.3) so the report-stream round-trip is faithful. Plugin
  imports OK under it.
- Image `rucio-test:local` (entrypoint final target, PYTHON=3.9, RUCIO_HOME=
  /opt/rucio, RUCIO_SOURCE_DIR=/rucio_source) used as RUCIO_TEST_IMAGE — works.
- Stack: `docker compose -p rucio-test-multi_vo-postgres14 -f dev/docker-compose.yml
  -f dev/docker-compose.test.override.yml --profile postgres14 up -d --wait`
  (the `-p` overrides the override's `name: dev`, so the user's `rucio-dev` and
  other non-`rucio-test-*` projects are untouched). Then pip install -e
  /rucio_source + bridge bin/etc + httpd graceful (mirrors ContainerManager).
- To root-cause cheaply WITHOUT the ~35-min triple-suite, drove InfraManager
  base bring-up with `run_multi_vo` monkeypatched to a no-op, then ran the single
  failing test under RUCIO_HOME=tst.

### test_did::test_list_recursive — ROOT CAUSE (the UNKNOWN) — IN-SCOPE, FIXED
SERVER-side apache/wsgi log (the real error behind the masked
`DatabaseException: An unknown Database Exception has occurred`):
```
psycopg.errors.StringDataRightTruncation: value too long for type character varying(25)
[SQL: INSERT INTO "TEMPORARY_SCOPE_NAME_0" (scope, name) VALUES (...)]
[parameters: {'scope': 'list-did-recursive-4ba44d@tst', ...}]
```
- The test hardcodes a 25-char scope (`('list-did-recursive-%s'%uuid)[:25]`,
  upstream since 2021, identical in master, NOT a recent change, NO multi_vo
  skip). In multi_vo the INTERNAL scope is `<scope>@<vo>` = 25 + `@tst` = 29 chars
  -> overflows the 25-char `TEMPORARY_SCOPE_NAME` column the recursive-list /
  bulk-attach path builds.
- VERDICT: **infra/config delta in OUR bring-up, IN-SCOPE.** The schema module is
  picked by `[common] multi_vo` (`rucio.common.schema.__init__._is_multivo` ->
  `generic_multi_vo` SCOPE_LENGTH=**29** vs `generic` SCOPE_LENGTH=**25**). Our
  LIVE SERVER cfg `/opt/rucio/etc/rucio.cfg` (the one httpd/WSGI loads; its
  RUCIO_HOME=/opt/rucio is fixed at daemon-master start, so pointing the harness
  env at the per-VO cfg does NOT change it) had NO `multi_vo` and no multi_vo
  schema -> server resolved SCOPE_LENGTH=25 -> overflow. The per-VO cfg
  (client-side, RUCIO_HOME=tst) DID have multi_vo=True + schema=generic_multi_vo
  (that's why client auth/add_scope worked) — only the SERVER was single-VO.
- Legacy `run_multi_vo_tests_docker.sh` avoids this by `export RUCIO_HOME=tst`
  BEFORE the httpd it serves under -> server uses generic_multi_vo (29). We never
  made the live server cfg multi_vo-aware. **This is NOT an xdist/noparallel
  symptom** (the prior batch's theory for (c) was wrong): it fails IDENTICALLY
  serial AND xdist, because it is a server-schema/config bug.
- PROVEN locally: after writing `[common] multi_vo=True` +
  `[policy] permission/schema=generic_multi_vo` into the live server cfg and
  `httpd -k graceful`, server SCOPE_LENGTH 25 -> 29 and
  `test_did::TestDIDClients::test_list_recursive` PASSED **serially** (1 passed).
- FIX: `InfraManager._apply_multi_vo_server_config()` (new) copies the multi_vo
  markers from the generated tst cfg into the live server cfg, called for
  multi_vo BEFORE `_restart_httpd()`.

### Double-execution — REPRODUCED (structural) + WIRING FIX
Confirmed from code + run structure: the multi_vo leg executed the suite TWICE+:
`run_multi_vo()` spawned per-VO CHILD pytest (Session A: xdist + noparallel +
`--ignore-glob tests/ruciopytest/*`) — the faithful, legacy-equivalent run —
but its rc was DISCARDED, AND the outer forwarded in-container Session B then
collected+ran the suite again SERIALLY (no xdist), and Session B's stream is what
the host/junit gated on. (Note: the in-container Session B for ALL suites runs
SERIAL — `configure_xdist` only sets the host config.option, never the forwarded
argv; only run_multi_vo's children carry `-p xdist`.) So CI gated on the wrong,
serial pass — which is why (b) test_bad_replica off-by-5 and (d)
test_bin_rucio::test_import_data (both `@noparallel`) failed there but pass under
Session A's xdist.
- FIX (gate on Session A, single execution):
  1. `setup()` stores `self._multi_vo_rc = run_multi_vo()`.
  2. `run_multi_vo()`: the **tst** child streams its reports to the host
     (`forward_stream=True` -> child argv gets
     `-p tests.ruciopytest.forward_stream_plugin`, new module that registers the
     report emitter on the xdist CONTROLLER only); **ts2** runs with
     RUCIO_FORWARD_STREAM stripped (replaying the same node ids twice would
     corrupt the host junit) but still gates via the aggregate rc (legacy
     "tst must pass then ts2 must pass").
  3. `plugin.pytest_configure` (in-container multi_vo): after `setup()`, set
     `config.args=[]` (suppress the outer session's own collection) + stash the
     rc; `pytest_runtestloop` then `finalize_host_exit(session, rc)` so the outer
     session exits with the children's aggregate code. No redundant serial pass;
     host junit now reflects the tst xdist+noparallel execution.
  4. `forwarding.ReportStreamEmitter.emit` drops the xdist-only `report.node`
     (WorkerController, not JSON-serializable) + `json.dumps(default=str)` — the
     emitter had never run under xdist before (Session B was always serial), so
     this is the first xdist-stream path; without it the stream aborts with
     `TypeError: Object of type WorkerController is not JSON serializable`.
- LOCAL VERIFICATION done: child stream-forward under `-p xdist --numprocesses=2`
  writes clean JSONL (3 TestReports / call passed); host `replay_report_line`
  round-trips it (3 dispatched); harness unit tests 52 passed/1 skipped incl. new
  `test_multi_vo_pytest_cmd_forward_stream` + `test_run_multi_vo_only_tst_streams`;
  forwarding 41 passed/1 skipped. Full end-to-end `--suite=multi_vo` host-driver
  run (CI-faithful) launched locally for final green confirmation.

### (b)/(d) verdict
NOT root-caused individually — the CI triage (run 28468143419) already showed both
PASS under Session A (xdist+noparallel) and fail only under the serial Session B;
they are `@noparallel` tests whose isolation legacy provides via xdist. The wiring
fix makes CI gate on the xdist children, so they are covered by faithfulness (the
legacy-equivalent execution model), not by patching the product tests. If either
survives under the faithful xdist gate in CI, that is the signal to bring an
inherit decision to the user (do NOT patch test_bad_replica / test_bin_rucio).

### Files changed (local commit, NOT pushed)
- tests/ruciopytest/infra_manager.py  (server multi_vo cfg + rc capture + per-VO stream wiring)
- tests/ruciopytest/plugin.py          (suppress outer multi_vo collection, mirror children rc)
- tests/ruciopytest/forwarding.py      (xdist-safe emit: drop node, default=str)
- tests/ruciopytest/forward_stream_plugin.py (NEW: controller-only stream emitter for the children)
- tests/ruciopytest/__init__.py        (multi_vo_forward_rc_key stash key)
- tests/ruciopytest/test_multi_vo_support.py (2 new unit tests)
