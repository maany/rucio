# ruciopytest — the Rucio test-suite pytest plugin

`ruciopytest` is a pytest plugin that runs Rucio's test suites with a single
`pytest` command. It replaces the legacy `tools/test/test.sh` driver: no shell
scripts, no CI matrix parsing, and no manual `docker compose` container
management. You pick a suite with `--suite=...`, and the plugin resolves the
right database backend, brings up (or reuses) the containers it needs, forwards
execution into the Rucio container when required, and mirrors the results back
to your terminal.

**Assumptions.** This guide assumes you already have Docker (with the Compose
plugin) installed and a working Rucio development checkout. If you don't, set up
the [dockerized dev environment](../../etc/docker/dev) first and read the
project [CONTRIBUTING](https://rucio.cern.ch/documentation/contributing)
guidelines. This README does not re-explain installing Docker or bootstrapping
the repo — it explains the plugin.

Everything below is grounded in the actual plugin source:
[`plugin.py`](plugin.py) (CLI options), [`profiles.py`](profiles.py) (suite
table), [`forwarding.py`](forwarding.py) (host-vs-container),
[`multi_vo_support.py`](multi_vo_support.py) (multi-VO), and
[`.github/workflows/simple-autotest.yml`](../../.github/workflows/simple-autotest.yml)
(the CI matrix).

## Contents

- [Quickstart](#quickstart)
- [How it works](#how-it-works)
- [Suite table](#suite-table)
- [CLI reference](#cli-reference)
- [Examples](#examples)
- [CI mapping](#ci-mapping)
- [Migrating from test.sh](#migrating-from-testsh)
- [Troubleshooting](#troubleshooting)

## Quickstart

Run your first suite in ~30 seconds. The `client` suite runs entirely on the
host (no container to wait for), so it's the fastest way to see the plugin work
end to end:

```bash
python -m pytest --suite=client tests/
```

That command runs the client-library tests (`tests/test_clients.py`,
`tests/test_bin_rucio.py`, `tests/test_module_import.py`) against a running
Rucio server. When you're ready for a full server-side run against PostgreSQL,
switch to a forwarded suite — the plugin will start the container for you:

```bash
python -m pytest --suite=remote_dbs tests/
```

> The plugin is **dormant** until you pass `--suite` (or `--infra`). A plain
> `python -m pytest tests/` with neither flag behaves like stock pytest and does
> not touch containers or the database.

## How it works

Read this before the reference — the flags make a lot more sense once you have
the mental model.

### Host execution vs container forwarding

Every suite has a **default execution mode** driven by its `run_in_container`
profile flag ([`profiles.py`](profiles.py)):

- **Host suites** (`client`) run pytest directly on your machine, against an
  externally managed Rucio server. No containers are started.
- **Forwarded suites** (`remote_dbs`, `multi_vo`, `votest`) re-run pytest
  *inside* the Rucio container. On the host, the plugin starts the container
  stack, suppresses host-side collection, and delegates the whole run into the
  container via `docker compose exec`. Each in-container test report is streamed
  back and replayed natively, so N container tests surface as N results in your
  terminal and the container's exit code becomes your exit code
  ([`forwarding.py`](forwarding.py)).

You can override the default with `--run-in-container` (force forwarding) or
`--no-run-in-container` (force host execution). Forcing a normally-forwarded
suite onto the host prints a loud warning — results may be unreliable because
the host environment lacks the container's services.

**Env crossing the boundary.** Only environment variables prefixed with
`RUCIO_` (plus `SUITE`, `POLICY`, `RDBMS`, `GITHUB_ACTIONS`, and anything you
add with `--container-env KEY=VALUE`) are forwarded into the container
(`_ENV_ALLOWLIST_PREFIXES = ("RUCIO_",)` in [`forwarding.py`](forwarding.py)).
Arbitrary host env does **not** leak in.

### Database lifecycle

Each run purges the database, rebuilds the schema, and re-seeds the base VO /
root account before tests execute (`InfraManager.setup()` in
[`infra_manager.py`](infra_manager.py)). Pass `--keep-db` to skip that entire
purge/rebuild/seed cycle and reuse the database from the previous run — much
faster for iterating, at the cost of potentially stale state.

### Multi-VO

The `multi_vo` suite exercises two virtual organisations: `tst` (`testvo1`) and
`ts2` (`testvo2`). It generates a per-VO `rucio.cfg` under
`/opt/rucio/etc/multi_vo/{tst,ts2}/etc` ([`multi_vo_support.py`](multi_vo_support.py))
and, by default, runs both VOs. The `RUCIO_MULTI_VO_LEG` environment variable
selects a single VO leg (`tst` or `ts2`); this is how CI parallelizes multi_vo
into two independent jobs. Unset or unrecognized values default to `tst`.

### Forwarded xdist (parallel workers)

Forwarded suites that declare `xdist_enabled` on a parallel-capable backend get
pytest-xdist workers injected automatically inside the container
(`build_forward_xdist_args` in [`forwarding.py`](forwarding.py)). Only
`postgres14` is xdist-compatible — SQLite is single-writer, and Oracle/MySQL hit
connection limits under load. The worker count defaults to 3 on CI
(`GITHUB_ACTIONS=true`) and `auto` locally; `--xdist-workers=N` overrides it.
(`multi_vo` is the exception: its per-VO child processes own their own xdist, so
the outer forwarded run is not parallelized.)

## Suite table

Four suites are registered in the plugin (`SUITE_PROFILES` in
[`profiles.py`](profiles.py)):

| Suite        | RDBMS backend | Compose/container profiles | Default execution mode | What it covers |
| ------------ | ------------- | -------------------------- | ---------------------- | -------------- |
| `client`     | postgres14    | none (`()`)                | **host-side**          | Client-library tests only: `tests/test_clients.py`, `tests/test_bin_rucio.py`, `tests/test_module_import.py`. Runs on the host against a running server. |
| `remote_dbs` | postgres14    | `postgres14`               | **forwarded**          | Full server-side test suite (`tests/`, excluding `tests/ruciopytest/*`) against the RDBMS. The main correctness suite. |
| `multi_vo`   | postgres14    | `postgres14`               | **forwarded**          | Full suite run under multi-VO configuration for two VOs (`tst`=testvo1, `ts2`=testvo2). Verifies VO isolation. |
| `votest`     | postgres14    | `postgres14`               | **forwarded**          | Policy-package tests selected from `matrix_policy_package_tests.yml`; requires a policy (`--policy=atlas` / `belleii`, or the `POLICY` env). |

> **Note:** SQLite is **not** a plugin suite — it was descoped in Phase 8. Only
> `postgres14` remains, and it is the only xdist-compatible backend. SQLite
> still exists in the legacy `tools/test/test.sh` driver, but not here.

## CLI reference

All options below live in the `rucio` option group registered by
`pytest_addoption` in [`plugin.py`](plugin.py). They combine with any standard
pytest flag — notably `--co` (collect-only), `-k`, `-m`, `-x`, `-v`.

| Flag | Argument | Default | Effect |
| ---- | -------- | ------- | ------ |
| `--suite` | `{client,remote_dbs,multi_vo,votest}` | `None` | Test suite to run. The plugin is **dormant** if neither `--suite` nor `--infra` is given. |
| `--keep-db` | *(flag)* | `False` | Keep the database from the previous run (skip purge/rebuild/seed). |
| `--policy` | `PKG` | `None` | votest policy package (e.g. `atlas`, `belleii`); falls back to the `POLICY` env var. Required for `--suite=votest`. |
| `--xdist-workers` | `N` (int) | `None` | Number of xdist workers, overriding auto-detection (3 on CI, `auto` locally). |
| `--infra` | `STR` | `None` | Override infrastructure: comma-separated compose profiles or service names. Can run without `--suite` (suites are inferred) or with it (suite's tests, overridden infra). |
| `--dry-run` | *(flag)* | `False` | Show the infrastructure plan and test collection **without executing** tests. |
| `--dry-run-json` | *(flag)* | `False` | Emit the dry-run report as JSON (implies `--dry-run`). |
| `--run-in-container` | *(flag)* | `None` | Force forwarding execution **into** the Rucio container, even for a host suite. |
| `--no-run-in-container` | *(flag)* | `None` | Force running on the **host** even for a container suite (prints a warning; results may be unreliable). |
| `--container-env` | `KEY=VALUE` | `[]` | Set an arbitrary env var inside the container for the forwarded run. Repeatable. |

`--run-in-container` / `--no-run-in-container` share one destination — the last
one wins, and neither is forwarded into the container (they only decide *where*
the run happens).

A few non-obvious flags, at a glance (full worked examples are in
[Examples](#examples)):

```bash
# --infra: run the standard suite but point it at an explicit compose profile.
python -m pytest --infra=postgres14 tests/

# --container-env: inject an env var into the forwarded in-container run.
python -m pytest --suite=remote_dbs --container-env=RUCIO_LOG_LEVEL=DEBUG tests/

# --policy: required to select the votest policy package.
python -m pytest --suite=votest --policy=atlas tests/
```
