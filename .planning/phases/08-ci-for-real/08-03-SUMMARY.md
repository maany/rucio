---
phase: 08-ci-for-real
plan: 03
subsystem: ci
tags: [ci, github-actions, client-suite, docker-compose, legacy-parity]
requires:
  - "08-02: 5-leg matrix + setup-python + host plugin deps + per-leg junit/report"
provides:
  - "host-side client leg: full rucio install + reachable bootstrapped server + cfg/cert wiring"
  - "legacy-faithful in-container client fallback on unreachable-server/import failure"
affects:
  - ".github/workflows/simple-autotest.yml"
tech-stack:
  added: []
  patterns:
    - "client-gated steps (if: matrix.suite == 'client') layered onto shared 5-leg matrix"
    - "reachability gate (httpd ping + host --co) drives host-vs-container execution"
    - "junit written to /rucio_source bind mount lands in host test-results/"
key-files:
  created: []
  modified:
    - ".github/workflows/simple-autotest.yml"
decisions:
  - "Reuse checked-in etc/certs/*.pem on the host instead of copying certs out of the container"
  - "Single reachable gate folds httpd-ping AND host collection-import into one host-vs-fallback switch"
  - "bin/ added to PATH (no console_scripts in pyproject) so test_bin_rucio finds the rucio CLI"
metrics:
  duration: 6min
  completed: 2026-06-30
---

# Phase 8 Plan 03: Host-side client leg provisioning + in-container fallback Summary

Gave the host-side `client` leg a full rucio install plus a reachable, bootstrapped rucio
server mirroring the legacy autotest path (`tools/test/test.sh` SUITE=client), with a
legacy-faithful in-container fallback when host bring-up is unreachable. Closes CICD-06.

## What Was Built

**Task 1 — Host-side client server provisioning** (commit `5a1ebbedf`)
Three client-gated steps appended before the shared "Run tests" step:
- `Install full rucio on host (client)`: editable `rucio` + `requirements.server.txt`
  + `requirements.client.txt` + `requirements.dev.txt`; prepends `$GITHUB_WORKSPACE/bin`
  to PATH (pyproject declares no console_scripts, so `test_bin_rucio` needs bin/ on PATH).
- `Provision rucio server for client (host)` (id `client_provision`, `continue-on-error`):
  brings up dev compose (`docker-compose.yml` + `docker-compose.test.override.yml` +
  `docker-compose.ports.yml`, `--profile postgres14`) so httpd is on `127.0.0.1:8443`,
  runs the legacy `tools/run_tests.sh -i` bootstrap inside the rucio container, then wires
  `rucio_client.cfg` into a host `RUCIO_HOME` — rewriting `:443 -> :8443` and the
  `/opt/rucio/etc/certs/*` paths to the checked-in `etc/certs/*` host certs. Exports
  `RUCIO_HOME` via `$GITHUB_ENV` and emits a `reachable` output.

**Task 2 — Client run + in-container fallback** (commit `97ef93255`)
- Shared "Run tests" step now gated `matrix.suite != 'client' || reachable == 'true'`:
  non-client legs are unchanged; the client leg runs host-side only when the server is
  reachable and host collection imports cleanly.
- `Run client tests in-container (fallback)` (gated `matrix.suite == 'client' && reachable
  != 'true'`): reproduces the legacy in-container path exactly — `cp rucio_client.cfg
  /opt/rucio/etc/rucio.cfg` then `tools/pytest.sh` over the 3 client files inside the rucio
  container, with junit written to `/rucio_source/test-results/client-py<py>.xml` (lands on
  the host via the repo-root bind mount) so the report step still renders.

## Key Implementation Details

- **Reachability gate** = httpd ping (`curl -k https://localhost:8443/ping`) AND host
  `pytest --suite=client --co` both succeed. Either failure flips the leg to the
  in-container fallback. With GitHub's default `set -eo pipefail`, an aborted bring-up
  leaves `reachable` unset (`!= 'true'`), which also routes to the fallback.
- **Certs**: `etc/certs/*.pem` are checked into the repo and baked into the runtime image;
  the host client cfg points directly at those host paths — no container cert copy needed.
- **Compose project** `rucio-client-ci` is shared between the provisioning and fallback
  steps so the fallback execs into the same already-running rucio container.
- **5-leg matrix untouched**: remote_dbs×2, multi_vo, client, votest all preserved; the
  three new steps are strictly client-gated.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] rucio CLI not on host PATH after editable install**
- **Found during:** Task 1
- **Issue:** `pyproject.toml` declares no `console_scripts`/`scripts`, so `pip install -e .`
  does not place the `rucio` executable on PATH; `tests/test_bin_rucio.py` shells out to
  `rucio` and would fail host-side.
- **Fix:** Append `$GITHUB_WORKSPACE/bin` to `$GITHUB_PATH` in the host-install step.
- **Files modified:** `.github/workflows/simple-autotest.yml`
- **Commit:** `5a1ebbedf`

**2. [Rule 1 - Bug] Cert paths pointed inside the container, not the host**
- **Found during:** Task 1
- **Issue:** `rucio_client.cfg` references `/opt/rucio/etc/certs/*`, which do not exist on
  the bare runner; host pytest TLS would fail.
- **Fix:** `sed`-rewrite the cfg's `ca_cert`/`client_cert`/`client_key`/`[test]` paths to the
  checked-in `etc/certs/*` host certs (simpler and more faithful than copying out of the
  container, which the plan allowed as "e.g.").
- **Files modified:** `.github/workflows/simple-autotest.yml`
- **Commit:** `5a1ebbedf`

## Verification

- `python -c "import yaml; yaml.safe_load(open('.github/workflows/simple-autotest.yml'))"` — valid.
- Task 1 automated check: 2 client-gated provisioning steps present; blob contains
  `rucio_client.cfg` and `8443`.
- Task 2 automated check: `test_clients.py` and `pytest.sh` present; YAML valid.
- Matrix legs unchanged: `[remote_dbs, remote_dbs, multi_vo, client, votest]`.
- Live green confirmed in 08-04 (this plan authors the YAML; it does not run CI).

## Self-Check: PASSED
