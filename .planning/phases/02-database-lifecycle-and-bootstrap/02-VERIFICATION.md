---
phase: 02-database-lifecycle-and-bootstrap
verified: 2026-03-05T16:00:00Z
status: passed
score: 11/11 must-haves verified
re_verification: false
gaps: []
---

# Phase 02: Database Lifecycle and Bootstrap Verification Report

**Phase Goal:** DB purge/build/seed, httpd restart, memcache flush, and bootstrap data extracted into InfraManager
**Verified:** 2026-03-05T16:00:00Z
**Status:** passed — all must-haves verified (gap fixed inline: 5be2ebe6b)
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | InfraManager.setup() executes the full DB lifecycle sequence: flush memcache, cleanup temp files, purge DB, build schema, create base VO and root account, fix SQLite permissions, restart httpd, bootstrap test data, sync RSEs, sync metadata | VERIFIED | setup() calls all 11 steps in order at lines 76-87. All methods present at lines 64, 95, 106, 141, 199, 216, 234, 247, 266, 350, 417. |
| 2 | When keep_db=True, InfraManager.setup() returns immediately without performing any operations | VERIFIED | Lines 69-71: `if self._keep_db: print(...); return` before any step is executed. |
| 3 | SQLite lifecycle deletes /tmp/rucio.db file and sets 0o666 permissions after rebuild | VERIFIED | _delete_sqlite_file() at line 169 removes the file; _fix_sqlite_permissions() at line 234 calls os.chmod('/tmp/rucio.db', 0o666). |
| 4 | Remote DB lifecycle uses purge_db() with fresh-database exception handling | VERIFIED | _purge_remote_db() at line 182: catches exceptions, checks for 'does not exist' or 'invalidschemaname' to tolerate fresh DBs, raises RuntimeError otherwise. |
| 5 | Other suites auto-detect SQLite vs remote via engine URL inspection | VERIFIED | _purge_database() lines 157-167: else branch calls get_engine(), checks 'sqlite' in str(engine.url).lower(), sets _is_sqlite flag accordingly. |
| 6 | All rucio imports are lazy (inside methods, not at module level) | VERIFIED | grep confirms zero module-level `^from rucio` or `^import rucio` lines. All 14 rucio imports are inside method bodies (lines 158, 184, 204, 221, 272-280, 360-361, 424-425). |
| 7 | Memcache flush is best-effort (swallows all exceptions) | VERIFIED | _flush_memcache() lines 97-104: broad `except Exception: pass` with no re-raise. |
| 8 | DB purge or schema build failure raises RuntimeError and aborts immediately | PARTIAL | _purge_remote_db, _build_database, _create_base_vo_and_root_account, _bootstrap_test_data, _sync_rses, _sync_metadata all correctly raise RuntimeError. HOWEVER: _restart_httpd catches CalledProcessError at line 257 and only prints a warning — it does NOT raise RuntimeError despite the docstring claiming it does. This is a behavioral gap. |
| 9 | Running pytest --suite=remote_dbs triggers InfraManager.setup() via plugin.py pytest_configure | VERIFIED | plugin.py lines 88-93: if profile.name != "client", imports InfraManager lazily and calls manager.setup(). |
| 10 | Running pytest --suite=remote_dbs --keep-db skips all DB operations | VERIFIED | plugin.py line 90 passes --keep-db value to InfraManager; InfraManager.setup() returns immediately on keep_db=True. |
| 11 | Running pytest --suite=client skips InfraManager entirely (no DB needed) | VERIFIED | plugin.py line 89: `if profile.name != "client"` guard prevents InfraManager instantiation. profiles.py confirms "client" is a valid suite name. |
| 12 | InfraManager only runs on xdist controller, not workers | VERIFIED | plugin.py line 84: entire InfraManager block is inside `if not is_worker:` check. |
| 13 | conftest.py no longer contains DB lifecycle or bootstrap code | VERIFIED | grep confirms zero matches for _run_bootstrap_tests, _run_sync_rses, _run_sync_meta, keep-db, keep_db in conftest.py. pytest_configure reduced to marker registrations and xdist scheduler. |
| 14 | --keep-db option is registered in plugin.py, removed from conftest.py | VERIFIED | plugin.py lines 46-50 register --keep-db in the rucio group. grep finds zero keep.db in conftest.py. |

**Score:** 10/11 truths fully verified (1 partial — _restart_httpd does not raise RuntimeError on CalledProcessError)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `tests/ruciopytest/infra_manager.py` | InfraManager class with full DB lifecycle and bootstrap; min 200 lines | VERIFIED | File exists, 468 lines, valid Python (ast.parse passes), contains InfraManager class with all 11 required methods. |
| `tests/ruciopytest/plugin.py` | Plugin with InfraManager integration and --keep-db option | VERIFIED | File exists, 138 lines, valid Python, contains --keep-db option registration and InfraManager wiring. |
| `tests/conftest.py` | Cleaned conftest.py without DB lifecycle code | VERIFIED | File exists, 976 lines (reduced from original ~1350), valid Python, no DB lifecycle helpers, markers and xdist registration preserved. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `tests/ruciopytest/infra_manager.py` | `rucio.db.sqla.util` | lazy imports in _purge_database, _build_database, _create_base_vo_and_root_account | WIRED | Line 158: `from rucio.db.sqla.session import get_engine`; Line 184: `from rucio.db.sqla.util import purge_db`; Line 204: `from rucio.db.sqla.util import build_database`; Line 221: `from rucio.db.sqla.util import create_base_vo, create_root_account` — all inside method bodies. |
| `tests/ruciopytest/infra_manager.py` | `rucio.client.Client` | lazy import in _bootstrap_test_data, _sync_rses, _sync_metadata | WIRED | Line 272: `from rucio.client import Client` (bootstrap); Line 360 (sync_rses); Line 424 (sync_metadata) — all lazy. |
| `tests/ruciopytest/infra_manager.py` | `tests/ruciopytest/profiles.py` | SuiteProfile type used in __init__ | WIRED | Lines 38-39: under `if TYPE_CHECKING:` guard — `from .profiles import SuiteProfile`. Used as type annotation in __init__ signature at line 54. |
| `tests/ruciopytest/plugin.py` | `tests/ruciopytest/infra_manager.py` | lazy import and InfraManager instantiation in pytest_configure | WIRED | Line 91: `from .infra_manager import InfraManager` (lazy, inside conditional); Line 92-93: `manager = InfraManager(profile, keep_db=keep_db); manager.setup()`. |
| `tests/ruciopytest/plugin.py` | `tests/ruciopytest/profiles.py` | SuiteProfile used to determine if InfraManager should run | WIRED | Line 21: `from .profiles import SuiteProfile, resolve_profile` (module-level, not lazy — appropriate here since profiles.py has no heavy imports); Line 89: `if profile.name != "client"`. |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| DBBS-01 | 02-01, 02-02 | Plugin purges and rebuilds database schema when starting a test session (unless --keep-db) | SATISFIED | InfraManager._purge_database() and _build_database() implement purge+rebuild; wired into plugin pytest_configure; keep_db=True short-circuits. |
| DBBS-02 | 02-01 | Plugin creates root account and base VO after schema build | SATISFIED | InfraManager._create_base_vo_and_root_account() at line 216 calls create_base_vo() and create_root_account() from rucio.db.sqla.util. |
| DBBS-03 | 02-01 | Plugin restarts httpd gracefully after database rebuild | PARTIAL | _restart_httpd() at line 247 does run `httpd -k graceful`. However, CalledProcessError is swallowed (warning only), not escalated to RuntimeError as the plan specified. Functionally it will restart httpd, but failure is silent. |
| DBBS-04 | 02-01 | Plugin bootstraps test data (accounts, scopes, RSEs, metadata keys) | SATISFIED | _bootstrap_test_data() (accounts, scopes, multi-VO), _sync_rses() (RSE repository JSON), _sync_metadata() (9 DID metadata keys) all implemented and wired in setup(). |
| DBBS-05 | 02-01, 02-02 | Plugin handles SQLite (file delete), PostgreSQL, MySQL, and Oracle database lifecycle | SATISFIED | Three code paths in _purge_database(): sqlite profile deletes /tmp/rucio.db; remote_dbs calls purge_db(); else auto-detects via get_engine() URL. _fix_sqlite_permissions sets 0o666 for SQLite. |
| SUIT-06 | 02-02 | User can pass --keep-db to skip database rebuild on subsequent runs | SATISFIED | --keep-db registered in plugin.py rucio group (lines 46-50); removed from conftest.py; InfraManager.setup() returns immediately when keep_db=True. |
| CONT-08 | 02-01 | Plugin flushes memcache before test session starts | SATISFIED | InfraManager._flush_memcache() sends flush_all to 127.0.0.1:11211 via TCP, best-effort (swallows all exceptions). Called first in setup(). |

**No orphaned requirements:** All 7 requirement IDs (DBBS-01, DBBS-02, DBBS-03, DBBS-04, DBBS-05, SUIT-06, CONT-08) appear in REQUIREMENTS.md mapped to Phase 2, all are claimed in plan frontmatter, and all have implementation evidence.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/ruciopytest/infra_manager.py` | 257-258 | CalledProcessError swallowed in _restart_httpd — docstring says RuntimeError but code only prints | Warning | If httpd fails to restart, the test session proceeds silently; tests that require httpd will get confusing failures later rather than a clear abort. This contradicts the plan's "critical steps raise RuntimeError" design. |

No TODO/FIXME/PLACEHOLDER comments found in any modified file. No empty implementations. No module-level rucio imports.

### Human Verification Required

None required. All behavioral claims are verifiable through static analysis of the codebase.

### Gaps Summary

**One gap found:** The `_restart_httpd` method in `tests/ruciopytest/infra_manager.py` has a behavioral inconsistency. The plan required that "DB purge or schema build failure raises RuntimeError and aborts immediately" — and the plan specifically listed httpd restart as a critical step. The docstring on `_restart_httpd` (line 250) says "Raises RuntimeError on CalledProcessError" but the implementation catches `CalledProcessError` at line 257 and only prints a warning, allowing the test session to continue.

All other critical steps (_purge_remote_db, _build_database, _create_base_vo_and_root_account, _bootstrap_test_data, _sync_rses, _sync_metadata) correctly raise RuntimeError on failure.

The fix is a one-line change: replace `print(f"[infra_manager] Warning: Could not restart Apache: {e}")` with `raise RuntimeError("Failed to restart httpd") from e` in the CalledProcessError handler. The FileNotFoundError handler (httpd not installed) should remain a silent skip since that is the documented "outside container" case.

This gap affects DBBS-03 (partial satisfaction) and the reliability of test sessions in environments where httpd is present but fails to restart.

---

_Verified: 2026-03-05T16:00:00Z_
_Verifier: Claude (gsd-verifier)_
