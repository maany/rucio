# Phase 2: Database Lifecycle and Bootstrap - Research

**Researched:** 2026-03-05
**Domain:** Database lifecycle management, test data seeding, pytest session hooks
**Confidence:** HIGH

## Summary

Phase 2 extracts the database lifecycle and bootstrap logic currently embedded in `tests/conftest.py` into a clean, testable module within the `tests/ruciopytest/` package. The existing code in `conftest.py` performs a well-defined sequence: (1) cleanup/memcache flush, (2) database purge or SQLite file deletion, (3) schema rebuild via `build_database()`, (4) base VO and root account creation, (5) httpd graceful restart, (6) bootstrap test data (accounts, scopes), (7) RSE sync from JSON, and (8) metadata key sync. All of this runs inside `pytest_configure` -- before test collection.

The existing implementation is functional but monolithic: approximately 200 lines of procedural code in `pytest_configure` plus three helper functions (`_run_bootstrap_tests`, `_run_sync_rses`, `_run_sync_meta`). The goal is to extract this into an `InfraManager` class (or module) that can be unit-tested independently and integrated cleanly with the Phase 1 plugin infrastructure.

**Primary recommendation:** Create a single `tests/ruciopytest/infra_manager.py` module containing an `InfraManager` class with methods for each lifecycle step, called from `pytest_configure` in the plugin. Use the existing `SuiteProfile` from Phase 1 to determine which operations to perform.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- **--keep-db semantics**: --keep-db skips ALL DB-related operations: no purge, no rebuild, no re-seeding. Also skips memcache flush and httpd restart -- full skip, trust previous state. Works with SQLite too -- reuses the existing .db file instead of deleting and rebuilding. Tests run against whatever state the DB is in from the last run.
- **Bootstrap data scope**: Extract bootstrap data creation as-is from conftest.py -- mirror exact current behavior. Same accounts, scopes, RSEs, metadata keys that conftest.py currently creates. No configurability needed -- proven seed data that works with existing tests.
- **Multi-RDBMS handling**: PostgreSQL and SQLite are the primary paths -- implement and test these thoroughly. MySQL and Oracle get basic support but less attention. These two correspond to the main CI suites (remote_dbs + sqlite).
- **Failure behavior**: DB purge or schema rebuild failure: abort session immediately. Fail fast, no retries. httpd restart failure: abort session immediately. No partial setup. No partial state -- if any infrastructure step fails, the entire session stops.

### Claude's Discretion
- DB health check with --keep-db (whether to validate DB is reachable before proceeding)
- Multi-VO bootstrap approach (replicate whatever conftest.py currently does)
- InfraManager module structure (single file vs split by concern)
- Bootstrap logging verbosity (match existing conftest.py behavior)
- DB purge strategy (drop/recreate vs truncate -- replicate existing behavior)
- DB connection resolution pattern (match how conftest.py currently gets connection info)
- Oracle lifecycle scope (full implementation vs stub -- match current test infrastructure)
- Error reporting style (pytest-native vs custom -- pick best integration approach)
- Timeout values for DB operations (determine if needed based on existing behavior)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| DBBS-01 | Plugin purges and rebuilds database schema when starting a test session (unless --keep-db) | Existing `purge_db()` + `build_database()` functions in `rucio.db.sqla.util`; conftest.py lines 139-233 show exact flow |
| DBBS-02 | Plugin creates root account and base VO after schema build | `create_base_vo()` + `create_root_account()` in `rucio.db.sqla.util`; called in conftest.py lines 209-219 |
| DBBS-03 | Plugin restarts httpd gracefully after database rebuild | conftest.py lines 236-247: `subprocess.run(['httpd', '-k', 'graceful'])` with 2s sleep |
| DBBS-04 | Plugin bootstraps test data (accounts, scopes, RSEs, metadata keys) | `_run_bootstrap_tests()` (lines 1187-1255), `_run_sync_rses()` (lines 1257-1312), `_run_sync_meta()` (lines 1314-1349) |
| DBBS-05 | Plugin handles SQLite (file delete), PostgreSQL, MySQL, and Oracle database lifecycle | conftest.py lines 150-205: SQLite deletes `/tmp/rucio.db`; remote_dbs uses `purge_db()`; other suites auto-detect via engine URL |
| SUIT-06 | User can pass --keep-db to skip database rebuild on subsequent runs | Already registered in conftest.py line 62-63; needs integration with InfraManager |
| CONT-08 | Plugin flushes memcache before test session starts | conftest.py lines 98-107: raw socket to 127.0.0.1:11211, send `flush_all\r\n` |

</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| SQLAlchemy | (existing in project) | ORM / DB engine / schema operations | Already used throughout rucio; `get_engine()`, `get_session()` from `rucio.db.sqla.session` |
| Alembic | (existing in project) | Schema versioning stamp after build | `command.stamp(alembic_cfg, "head")` in `build_database()` |
| pytest | >=7.0 | Session hooks, stash, config | Phase 1 already uses `pytest.StashKey`, `pytest_configure` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| subprocess (stdlib) | N/A | httpd restart, optional cleanup | `httpd -k graceful` command |
| socket (stdlib) | N/A | Memcache flush | Raw TCP to 127.0.0.1:11211 |
| rucio.db.sqla.util | N/A | `purge_db()`, `build_database()`, `create_base_vo()`, `create_root_account()` | Core DB lifecycle operations |
| rucio.client.Client | N/A | Bootstrap data creation (accounts, scopes, RSEs, metadata) | Uses HTTP API to create test data post-httpd restart |
| rucio.tests.common_server | N/A | `reset_config_table()` for VO mappings | Called during bootstrap before client operations |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Single InfraManager class | Separate modules per concern (db_lifecycle.py, bootstrap.py, memcache.py) | Single file is simpler for ~300 lines; split only if it grows beyond 500 lines |
| Raw socket for memcache | pymemcache library | Raw socket avoids new dependency (PLUG-02: zero new pip dependencies) |

## Architecture Patterns

### Recommended Project Structure
```
tests/ruciopytest/
    __init__.py              # existing
    plugin.py                # existing - add InfraManager integration here
    profiles.py              # existing
    infra_manager.py         # NEW - InfraManager class
    xdist_config.py          # existing
    xdist_noparallel_*.py    # existing
    artifacts_plugin.py      # existing
```

### Pattern 1: InfraManager Class with Step Methods
**What:** A class that encapsulates the entire database lifecycle as discrete, testable methods.
**When to use:** Always -- this is the core extraction pattern.
**Example:**
```python
# tests/ruciopytest/infra_manager.py

from __future__ import annotations

import os
import socket
import subprocess
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .profiles import SuiteProfile


class InfraManager:
    """Manages database lifecycle and test infrastructure setup.

    Extracted from conftest.py pytest_configure logic. Each step is
    a separate method for testability and clarity.
    """

    def __init__(self, profile: SuiteProfile, keep_db: bool = False) -> None:
        self.profile = profile
        self.keep_db = keep_db

    def setup(self) -> None:
        """Run the full infrastructure setup sequence.

        Order matters: purge -> build -> base_vo -> root_account ->
        httpd_restart -> bootstrap_data -> sync_rses -> sync_meta
        """
        if self.keep_db:
            return

        self._flush_memcache()
        self._cleanup_temp_files()
        self._purge_database()
        self._build_database()
        self._create_base_vo_and_root_account()
        self._fix_sqlite_permissions()
        self._restart_httpd()
        self._bootstrap_test_data()
        self._sync_rses()
        self._sync_metadata()

    def _flush_memcache(self) -> None:
        """Flush memcache via raw TCP socket. Best-effort."""
        ...

    def _purge_database(self) -> None:
        """Purge DB (remote) or delete SQLite file."""
        ...

    def _build_database(self) -> None:
        """Build schema via build_database() + alembic stamp."""
        ...

    # ... etc
```

### Pattern 2: Integration with pytest_configure via Plugin
**What:** The plugin's `pytest_configure` hook instantiates InfraManager and calls `setup()`.
**When to use:** In `tests/ruciopytest/plugin.py` to wire InfraManager into pytest lifecycle.
**Example:**
```python
# In plugin.py pytest_configure, after profile resolution:

def pytest_configure(config: pytest.Config) -> None:
    # ... existing profile resolution ...

    if not is_worker:
        configure_xdist(config, profile)
        _print_profile_summary(config, profile)

        # Database lifecycle (Phase 2)
        if profile.name != "client":
            keep_db = config.getoption("--keep-db", default=False)
            from .infra_manager import InfraManager
            manager = InfraManager(profile, keep_db=keep_db)
            manager.setup()
```

### Pattern 3: --keep-db Registration
**What:** Register `--keep-db` option in the ruciopytest plugin's `pytest_addoption`, removing it from conftest.py.
**When to use:** Move the option registration from conftest.py to the plugin.
**Example:**
```python
# In plugin.py pytest_addoption:
group.addoption(
    "--keep-db",
    action="store_true",
    default=False,
    help="Keep database from previous run (skip purge/rebuild/seed)",
)
```

### Anti-Patterns to Avoid
- **Importing rucio modules at module level in infra_manager.py:** Rucio modules trigger config loading and DB connections on import. Use lazy imports inside methods, matching the existing conftest.py pattern.
- **Making bootstrap data configurable:** The decision is locked -- mirror conftest.py exactly. No configuration knobs.
- **Retrying failed DB operations:** The decision is locked -- fail fast, abort session immediately.
- **Running InfraManager on xdist workers:** DB setup must only run on the controller. Workers connect to an already-set-up database.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Database purge | Custom DROP TABLE loops | `rucio.db.sqla.util.purge_db()` | Handles cyclical FK constraints, schema-aware, PostgreSQL enum cleanup |
| Schema creation | Raw CREATE TABLE statements | `rucio.db.sqla.util.build_database()` | Uses SQLAlchemy models + Alembic stamp |
| Root account creation | Manual INSERT into accounts table | `rucio.db.sqla.util.create_root_account()` | Handles multi-VO, identity types, password hashing |
| Base VO creation | Manual INSERT into VO table | `rucio.db.sqla.util.create_base_vo()` | Uses correct DEFAULT_VO constant |
| RSE sync | Custom RSE creation logic | Existing `_run_sync_rses()` pattern using `rucio.client.Client` | Handles protocols, attributes, Duplicate exceptions |
| Metadata sync | Custom metadata key insertion | Existing `_run_sync_meta()` pattern using `rucio.client.Client` | Handles key types, regexps, allowed values |

**Key insight:** The rucio project has well-tested utility functions for all DB operations. The InfraManager's job is orchestration and error handling, not reimplementing DB primitives.

## Common Pitfalls

### Pitfall 1: Import-Time Database Connections
**What goes wrong:** Importing `rucio.db.sqla.session` or `rucio.client.Client` at module level triggers config loading and can attempt DB connections before the DB is ready.
**Why it happens:** Rucio's module initialization is eager -- config reads happen on import.
**How to avoid:** Use lazy imports inside InfraManager methods (matching conftest.py's existing pattern). All rucio imports inside function bodies, not at module top.
**Warning signs:** `ImportError` or `ConnectionRefused` during test collection.

### Pitfall 2: pytest_configure Ordering
**What goes wrong:** The ruciopytest plugin's `pytest_configure` must run BEFORE conftest.py's `pytest_configure` so that the suite profile is available in stash.
**Why it happens:** `pytest_plugins` registration in conftest.py controls hook ordering. Plugins registered via `pytest_plugins` get their hooks called before the conftest's own hooks.
**How to avoid:** The plugin is already registered via `pytest_plugins` in conftest.py (line 53-56), ensuring correct ordering. InfraManager runs in the plugin's `pytest_configure`, not conftest's.
**Warning signs:** `suite_profile_key` not found in stash when conftest tries to read it.

### Pitfall 3: SQLite File Permissions
**What goes wrong:** After building the SQLite database at `/tmp/rucio.db`, the file may have restrictive permissions preventing access by httpd or other processes.
**Why it happens:** Python's default file creation umask.
**How to avoid:** Call `os.chmod(db_path, 0o666)` after SQLite database creation (existing conftest.py pattern, line 227).
**Warning signs:** Permission denied errors when httpd tries to read the SQLite DB.

### Pitfall 4: httpd Restart Timing
**What goes wrong:** Tests start before httpd has fully restarted, leading to connection refused or 500 errors when bootstrap creates test data via the Client API.
**Why it happens:** `httpd -k graceful` returns immediately; actual restart takes time.
**How to avoid:** Add a brief sleep (2 seconds, matching existing conftest.py) after httpd restart. The `_run_bootstrap_tests` function creates a `Client()` which will fail if httpd isn't ready.
**Warning signs:** `RucioException: Internal Server Error` when creating the Client in bootstrap.

### Pitfall 5: Fresh Database vs Existing Database on purge_db()
**What goes wrong:** `purge_db()` raises an exception on a fresh PostgreSQL database where the schema doesn't exist yet.
**Why it happens:** `inspector.get_sorted_table_and_fkc_names()` fails when the schema is missing.
**How to avoid:** Catch exceptions where the error message contains `'does not exist'` or `'invalidschemaname'` and treat as fresh database (existing conftest.py pattern, lines 172-176).
**Warning signs:** `purge_db()` raises an error on first-ever test run.

### Pitfall 6: sys Module Missing in _run_bootstrap_tests
**What goes wrong:** The existing `_run_bootstrap_tests()` references `sys.stderr` (line 1227) but `sys` is not imported within that function.
**Why it happens:** Bug in the existing conftest.py -- `sys` is imported inside `rucio_bootstrap` fixture but not in the standalone function.
**How to avoid:** Add `import sys` to the bootstrap function when extracting it.
**Warning signs:** `NameError: name 'sys' is not defined` if the httpd error log reading path is triggered.

### Pitfall 7: Client Suite Should Skip All DB Operations
**What goes wrong:** Running InfraManager for the `client` suite attempts DB operations that are not needed and will fail (no DB container).
**Why it happens:** Client tests only need a running rucio server, not direct DB access.
**How to avoid:** Check `profile.name != "client"` before running any InfraManager operations. The existing conftest.py already does this check (line 87, 139).
**Warning signs:** Connection refused to database when running client suite.

## Code Examples

Verified patterns extracted from the existing `tests/conftest.py`:

### Database Purge Flow (Remote DBs)
```python
# Source: tests/conftest.py lines 165-181
from rucio.db.sqla.util import purge_db

try:
    purge_db()
except Exception as e:
    error_str = str(e).lower()
    if 'does not exist' in error_str or 'invalidschemaname' in error_str:
        # Fresh database, schema doesn't exist yet -- safe to continue
        pass
    else:
        raise RuntimeError("Failed to purge database") from e
```

### SQLite Lifecycle
```python
# Source: tests/conftest.py lines 150-163
import os

sqlite_path = '/tmp/rucio.db'
if os.path.exists(sqlite_path):
    os.remove(sqlite_path)

# After build_database():
if os.path.exists(sqlite_path):
    os.chmod(sqlite_path, 0o666)
```

### Schema Build + Base VO + Root Account
```python
# Source: tests/conftest.py lines 208-219
from rucio.db.sqla.util import build_database, create_base_vo, create_root_account

build_database()
create_base_vo()
create_root_account()
```

### Memcache Flush
```python
# Source: tests/conftest.py lines 98-107
import socket

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        sock.connect(('127.0.0.1', 11211))
        sock.sendall(b'flush_all\r\n')
except Exception:
    pass  # Not critical, memcache might not be running
```

### httpd Restart
```python
# Source: tests/conftest.py lines 236-247
import subprocess
import time

try:
    subprocess.run(['httpd', '-k', 'graceful'], check=True, capture_output=True)
    time.sleep(2)
except subprocess.CalledProcessError as e:
    raise RuntimeError(f"Failed to restart httpd: {e}") from e
except FileNotFoundError:
    pass  # httpd not found -- may be running outside container
```

### Bootstrap Test Data (accounts + scopes)
```python
# Source: tests/conftest.py lines 1187-1255
import sys
import time
from rucio.client import Client
from rucio.common.config import config_get, config_get_bool
from rucio.common.constants import DEFAULT_VO
from rucio.common.exception import Duplicate, RucioException
from rucio.common.types import InternalAccount
from rucio.core.account import add_account_attribute
from rucio.core.vo import map_vo
from rucio.gateway.vo import add_vo
from rucio.tests.common_server import reset_config_table

# Step 1: Reset config table (VO mappings)
reset_config_table()

# Step 2: Multi-VO setup if enabled
if config_get_bool('common', 'multi_vo', raise_exception=False, default=False):
    vo = {'vo': map_vo(config_get('client', 'vo', raise_exception=False, default='tst'))}
    try:
        add_vo(new_vo=vo['vo'], issuer='super_root', description='A VO to test multi-vo features',
               email='N/A', vo=DEFAULT_VO)
    except Duplicate:
        pass
else:
    vo = {}

# Step 3: Create client and test accounts
client = Client()

try:
    client.add_account('jdoe', 'SERVICE', 'jdoe@email.com')
except Duplicate:
    pass

try:
    add_account_attribute(account=InternalAccount('root', **vo), key='admin', value=True)
except Exception:
    pass

try:
    client.add_account('panda', 'SERVICE', 'panda@email.com')
    add_account_attribute(account=InternalAccount('panda', **vo), key='admin', value=True)
except Duplicate:
    pass

# Step 4: Create scopes
try:
    client.add_scope('jdoe', 'mock')
except Duplicate:
    pass

try:
    client.add_scope('root', 'archive')
except Duplicate:
    pass
```

### RSE Sync
```python
# Source: tests/conftest.py lines 1257-1312
import json
from rucio.client import Client
from rucio.common.exception import Duplicate, InvalidObject

rse_repo = 'etc/rse_repository.json'
with open(rse_repo) as f:
    rses_list = json.load(f)

c = Client()
for rse in rses_list:
    try:
        c.add_rse(rse)
    except Duplicate:
        pass

    # Add protocols
    for p_id in rses_list[rse].get('protocols', []):
        try:
            c.add_protocol(rse, p_id)
        except Duplicate:
            pass

    # Add attributes
    for attr in rses_list[rse].get('attributes', {}):
        try:
            c.add_rse_attribute(rse, attr, rses_list[rse]['attributes'][attr])
        except Duplicate:
            pass
```

### Metadata Key Sync
```python
# Source: tests/conftest.py lines 1314-1349
from rucio.client import Client
from rucio.common.exception import Duplicate

meta_keys = [
    ('project', 'ALL', None, ['data13_hip', 'NoProjectDefined']),
    ('run_number', 'ALL', None, ['NoRunNumberDefined']),
    ('stream_name', 'ALL', None, ['NoStreamNameDefined']),
    ('prod_step', 'ALL', None, ['merge', 'recon', 'simul', 'evgen', 'NoProdstepDefined', 'user']),
    ('datatype', 'ALL', None, ['HITS', 'AOD', 'EVNT', 'NTUP_TRIG', 'NTUP_SMWZ', 'NoDatatypeDefined', 'DPD']),
    ('version', 'ALL', None, []),
    ('campaign', 'ALL', None, []),
    ('guid', 'FILE', r'^(\{){0,1}[0-9a-fA-F]{8}-?...{12}(\}){0,1}$', []),
    ('events', 'DERIVED', r'^\d+$', []),
]

c = Client()
for key, key_type, value_regexp, values in meta_keys:
    try:
        c.add_did_meta(key, key_type, value_regexp)
    except Duplicate:
        pass
    for value in values:
        try:
            c.add_did_meta(key, key_type, value_regexp, value)
        except Duplicate:
            pass
```

## Architecture Recommendations (Claude's Discretion Areas)

### DB Health Check with --keep-db
**Recommendation: YES, add a lightweight health check.** When `--keep-db` is set, perform a simple `SELECT 1` query (or check SQLite file existence) to verify the database is reachable. This prevents confusing failures later during test collection. Cost: one SQL query. Benefit: clear error message if DB is unreachable.

### Multi-VO Bootstrap
**Recommendation: Replicate exactly.** The existing `_run_bootstrap_tests()` already handles multi-VO: it checks `config_get_bool('common', 'multi_vo')` and conditionally calls `add_vo()`. Extract this logic as-is.

### InfraManager Module Structure
**Recommendation: Single file `infra_manager.py`.** The total extracted code is approximately 250-300 lines. A single file with the `InfraManager` class keeps it simple and grep-friendly. If it grows beyond 500 lines in future phases, split then.

### Bootstrap Logging Verbosity
**Recommendation: Match existing `print()` pattern with `[infra_manager]` prefix.** The existing conftest.py uses `print("[pytest_configure] ...")` throughout. Replace the prefix with `[infra_manager]` for clarity about where the output comes from.

### DB Purge Strategy
**Recommendation: Use existing `purge_db()` for remote DBs, file deletion for SQLite.** This is exactly what conftest.py does. `purge_db()` drops all tables/constraints/enums -- it's a thorough purge, not a truncate.

### DB Connection Resolution
**Recommendation: Use existing `get_engine()` from `rucio.db.sqla.session`.** This reads `[database] default` from rucio config. For SQLite detection, check `'sqlite' in str(engine.url).lower()`. This matches conftest.py's approach.

### Oracle Lifecycle Scope
**Recommendation: Full implementation matching conftest.py.** Oracle uses the same `purge_db()` path as PostgreSQL. No special Oracle-specific logic is needed beyond what `purge_db()` already handles (it skips `DROP CONSTRAINT` for dialects that don't support ALTER).

### Error Reporting Style
**Recommendation: Raise `RuntimeError` with descriptive messages, matching existing conftest.py pattern.** Use `raise RuntimeError("Failed to purge database") from e`. Pytest will catch this and abort the session with a clear traceback.

### Timeout Values
**Recommendation: Only the 2-second sleep after httpd restart and 1-second socket timeout for memcache.** These match existing behavior. No additional timeouts are needed -- `purge_db()` and `build_database()` are synchronous SQLAlchemy operations.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Monolithic conftest.py `pytest_configure` | Extracted InfraManager class | Phase 2 (this phase) | Testable, maintainable lifecycle management |
| `database_setup` + `test_environment_setup` + `rucio_bootstrap` fixture chain | Single InfraManager.setup() call in plugin | Phase 2 (this phase) | Removes redundant fixture chain; all setup in pytest_configure |
| Separate bootstrap_tests.py script | Inline `_run_bootstrap_tests()` function | Already done in conftest.py | No subprocess overhead; direct Python calls |

## Open Questions

1. **conftest.py cleanup scope**
   - What we know: Phase 2 extracts DB lifecycle into InfraManager. The `--keep-db` option registration, `database_setup`, `test_environment_setup`, and `rucio_bootstrap` fixtures become partially or fully redundant.
   - What's unclear: How much of conftest.py should be cleaned up in Phase 2 vs deferred to later phases?
   - Recommendation: Move `--keep-db` registration to the plugin. Remove `database_setup` and `test_environment_setup` fixtures. Keep `rucio_bootstrap` as a thin wrapper that delegates to InfraManager (for backward compat with fixtures that depend on it). Full conftest.py refactoring deferred to Phase 5.

2. **xdist controller-only execution**
   - What we know: Phase 1 already guards with `is_worker = hasattr(config, "workerinput")`. InfraManager must only run on controller.
   - What's unclear: Nothing -- this is straightforward.
   - Recommendation: InfraManager.setup() is called inside the existing `if not is_worker:` block in plugin.py.

3. **Cleanup operations scope**
   - What we know: conftest.py cleans `/tmp/.rucio_*`, `/tmp/rucio_rse`, and `.pyc` files. These are in `test_environment_setup` fixture.
   - What's unclear: Should these cleanups be part of InfraManager or remain separate?
   - Recommendation: Include them in InfraManager since they're part of the "prepare environment" step and should also be skipped with `--keep-db`.

## Sources

### Primary (HIGH confidence)
- `tests/conftest.py` - Complete existing implementation of DB lifecycle (lines 59-285) and bootstrap functions (lines 1052-1349)
- `lib/rucio/db/sqla/util.py` - `purge_db()`, `build_database()`, `create_base_vo()`, `create_root_account()` implementations
- `lib/rucio/db/sqla/session.py` - `get_engine()`, `get_session()` for DB connection resolution
- `tests/ruciopytest/plugin.py` - Phase 1 plugin structure, `pytest_configure` hook, `suite_profile_key`
- `tests/ruciopytest/profiles.py` - `SuiteProfile` dataclass, `resolve_profile()`, `SUITE_PROFILES`
- `etc/rse_repository.json` - RSE definitions used by `_run_sync_rses()`
- `lib/rucio/tests/common_server.py` - `reset_config_table()` for VO map seeding

### Secondary (MEDIUM confidence)
- N/A -- all findings are from direct codebase inspection

### Tertiary (LOW confidence)
- N/A

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries already in use in the project; no new dependencies
- Architecture: HIGH - direct extraction of existing working code with clear structure
- Pitfalls: HIGH - identified from actual code inspection and existing bug patterns
- Bootstrap data: HIGH - exact data (accounts, scopes, RSEs, metadata) documented from source

**Research date:** 2026-03-05
**Valid until:** 2026-04-05 (stable -- rucio DB utilities rarely change)
