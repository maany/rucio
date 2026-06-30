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

## Watch protocol
- After pushing, hand orchestrator the new sha + run id. Orchestrator watches and returns per-leg results + artifacts.
- Useful: gh run view <id> --repo maany/rucio --json jobs --jq '.jobs[]|"\(.databaseId) \(.name) \(.conclusion)"'
  gh run view --repo maany/rucio --job <id> --log-failed | tail -150
  gh run download <id> --repo maany/rucio -n host-logs-<leg>-py3.9 -D <dir>  (then read .test-forward/*.container-stdout.log)
- No force-push (histories aligned). No fabricated green.
