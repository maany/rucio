# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Infrastructure manager for Rucio test database lifecycle and bootstrap.

Encapsulates the full DB lifecycle sequence previously embedded in
conftest.py's ``pytest_configure``: flush memcache, cleanup temp files,
purge DB, build schema, create base VO and root account, fix SQLite
permissions, restart httpd, bootstrap test data, sync RSEs, and sync
metadata.

All ``rucio.*`` imports are **lazy** (inside method bodies) because Rucio
modules trigger config loading and DB connections on import.
"""

from __future__ import annotations

import glob
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .profiles import SuiteProfile


class InfraManager:
    """Orchestrates DB lifecycle and test-data bootstrap for a test suite.

    Parameters
    ----------
    profile:
        The resolved :class:`SuiteProfile` for the current test run.
    keep_db:
        When ``True``, :meth:`setup` returns immediately without
        performing any operations.  Used by ``--keep-db`` CLI flag.
    """

    def __init__(self, profile: SuiteProfile, keep_db: bool = False) -> None:
        self._profile = profile
        self._keep_db = keep_db
        # Cached flag: whether the current DB is SQLite (set during purge)
        self._is_sqlite: bool | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Execute the full DB lifecycle sequence.

        Returns immediately when *keep_db* is ``True``.
        """
        if self._keep_db:
            print("[infra_manager] keep_db=True, skipping DB lifecycle")
            return

        print(f"\n[infra_manager] Setting up database for suite: {self._profile.name}")

        # Best-effort steps (swallow all exceptions)
        self._flush_memcache()
        self._cleanup_temp_files()

        # Critical steps (raise RuntimeError on failure)
        self._purge_database()
        self._build_database()
        self._create_base_vo_and_root_account()
        self._fix_sqlite_permissions()
        self._apply_votest_policy()
        # Generate both VO configs BEFORE httpd restart so the restart picks
        # them up. No-op for non-multi_vo suites.
        self._setup_multi_vo()
        self._restart_httpd()
        self._bootstrap_test_data()
        self._sync_rses()
        self._sync_metadata()

        # For the multi_vo suite, drive the legacy per-VO execution (tst, then
        # ts2 on tst success) as the FINAL step. This is the ONLY trigger for
        # run_multi_vo(); no plugin.py change is required (plugin already calls
        # manager.setup() in-container). No-op for other suites.
        if self._profile.name == "multi_vo":
            self.run_multi_vo()

        print("[infra_manager] Database lifecycle complete\n")

    # ------------------------------------------------------------------
    # Multi-VO setup + per-VO execution
    # ------------------------------------------------------------------

    def _setup_multi_vo(self) -> None:
        """Generate both VO ``rucio.cfg`` files for the multi_vo suite.

        Reproduces the legacy ``test.sh`` two-merge step: merges
        ``rucio_autotests_common.cfg`` with each of the per-VO source cfgs
        into the live per-VO etc dirs
        (``/opt/rucio/etc/multi_vo/{tst,ts2}/etc/rucio.cfg``). Runs strictly
        BEFORE :meth:`_restart_httpd` so the restart picks up the new configs.

        No-op for non-multi_vo suites.

        Raises:
            RuntimeError: when a source cfg is missing or a write fails.
        """
        if self._profile.name != "multi_vo":
            return

        from . import multi_vo_support

        # repo_root = in-container source dir (matches the convention used by
        # _apply_votest_policy / _sync_rses, which read RUCIO_SOURCE_DIR).
        repo_root = Path(os.environ.get("RUCIO_SOURCE_DIR", "/opt/rucio"))
        try:
            multi_vo_support.generate_multi_vo_configs(repo_root)
        except Exception as e:
            raise RuntimeError(
                f"[infra_manager] multi_vo config generation failed: {e}"
            ) from e
        print("[infra_manager] Generated multi_vo configs (tst, ts2)")

    def bootstrap_vo(self, vo_home: str) -> None:
        """Re-point ``RUCIO_HOME`` and bootstrap a single VO (no DB reset).

        Runs ONLY the bootstrap/sync steps -- deliberately NOT
        ``_purge_database``/``_build_database`` -- so the second VO (ts2)
        reuses the schema created for tst, mirroring
        ``run_multi_vo_tests_docker.sh`` (no 2nd DB reset).
        """
        os.environ["RUCIO_HOME"] = vo_home
        print(f"[infra_manager] Bootstrapping VO at RUCIO_HOME={vo_home}")
        self._create_base_vo_and_root_account()
        self._bootstrap_test_data()
        self._sync_rses()
        self._sync_metadata()

    def run_multi_vo(self) -> int:
        """Run the full ``tests/`` suite once per VO (tst, then ts2 on success).

        COMMITTED design: each VO leg is a CHILD ``python -m pytest`` process
        (mirrors ``run_multi_vo_tests_docker.sh``'s ``pytest tests/ -v
        --tb=short``). This keeps all ownership inside InfraManager with no
        plugin.py change. Legacy "stop if tst fails" semantics are preserved:
        ts2 only runs when tst passes, and there is NO 2nd DB reset.

        Returns:
            The exit code of the tst run if it failed, otherwise the ts2 code.
        """
        TST_HOME = "/opt/rucio/etc/multi_vo/tst"
        TS2_HOME = "/opt/rucio/etc/multi_vo/ts2"
        pytest_cmd = [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"]

        # --- VO tst ---
        self.bootstrap_vo(TST_HOME)
        print("[infra_manager] Running tests for VO tst")
        tst = subprocess.run(pytest_cmd, env={**os.environ, "RUCIO_HOME": TST_HOME})
        if tst.returncode != 0:
            print(
                f"[infra_manager] tst VO failed (rc={tst.returncode}); "
                "not attempting ts2"
            )
            return tst.returncode

        # --- VO ts2 (only on tst success; no DB reset) ---
        self.bootstrap_vo(TS2_HOME)
        print("[infra_manager] Running tests for VO ts2")
        ts2 = subprocess.run(pytest_cmd, env={**os.environ, "RUCIO_HOME": TS2_HOME})
        return ts2.returncode

    # ------------------------------------------------------------------
    # Best-effort steps
    # ------------------------------------------------------------------

    def _flush_memcache(self) -> None:
        """Send ``flush_all`` to a local memcache instance (best-effort)."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(('127.0.0.1', 11211))
                sock.sendall(b'flush_all\r\n')
            print("[infra_manager] Memcache cleared")
        except Exception:
            pass  # Not critical, memcache might not be running

    def _cleanup_temp_files(self) -> None:
        """Remove authentication tokens, RSE dirs, and .pyc files."""
        # Clean authentication tokens
        for pattern in ['/tmp/.rucio_*']:
            try:
                for path in glob.glob(pattern):
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
            except Exception:
                pass  # Not critical

        # Clean RSE directories
        try:
            rse_dir = '/tmp/rucio_rse'
            if os.path.exists(rse_dir):
                shutil.rmtree(rse_dir, ignore_errors=True)
                os.makedirs(rse_dir, exist_ok=True)
        except Exception:
            pass  # Not critical

        # Clean .pyc files
        try:
            subprocess.run(
                ['find', 'lib', '-iname', '*.pyc', '-delete'],
                check=False, capture_output=True, timeout=5,
            )
        except Exception:
            pass  # Not critical

        print("[infra_manager] Temp file cleanup completed")

    # ------------------------------------------------------------------
    # Critical DB steps
    # ------------------------------------------------------------------

    def _purge_database(self) -> None:
        """Purge the database: delete SQLite file or call ``purge_db()``.

        Raises :class:`RuntimeError` on unrecoverable failure.
        """
        print("[infra_manager] Resetting database tables")

        if self._profile.rdbms == "sqlite":
            self._is_sqlite = True
            self._delete_sqlite_file()

        elif self._profile.name == "remote_dbs":
            self._is_sqlite = False
            self._purge_remote_db()

        else:
            # Auto-detect from engine URL
            from rucio.db.sqla.session import get_engine
            engine = get_engine()
            self._is_sqlite = 'sqlite' in str(engine.url).lower()

            if self._is_sqlite:
                print("[infra_manager] Detected SQLite, deleting database file")
                self._delete_sqlite_file()
            else:
                print("[infra_manager] Detected remote database, purging")
                self._purge_remote_db()

    def _delete_sqlite_file(self) -> None:
        """Delete the SQLite database file at ``/tmp/rucio.db``."""
        db_path = '/tmp/rucio.db'
        if os.path.exists(db_path):
            print(f"[infra_manager] Removing old SQLite database: {db_path}")
            try:
                os.remove(db_path)
                print(f"[infra_manager] SQLite database {db_path} deleted successfully")
            except Exception as e:
                print(f"[infra_manager] Warning: Could not remove {db_path}: {e}")
        else:
            print(f"[infra_manager] SQLite database {db_path} does not exist (will be created fresh)")

    def _purge_remote_db(self) -> None:
        """Purge a remote database, tolerating fresh-database errors."""
        from rucio.db.sqla.util import purge_db

        try:
            purge_db()
            print("[infra_manager] Database purge completed")
        except Exception as e:
            error_str = str(e).lower()
            if 'does not exist' in error_str or 'invalidschemaname' in error_str:
                print("[infra_manager] Schema doesn't exist (fresh database), skipping purge")
            else:
                print(f"[infra_manager] Database purge failed: {e}")
                import traceback
                traceback.print_exc()
                raise RuntimeError("Failed to purge database") from e

    def _build_database(self) -> None:
        """Build database schema and tables via ``build_database()``.

        Raises :class:`RuntimeError` on failure.
        """
        from rucio.db.sqla.util import build_database

        try:
            print("[infra_manager] Building database schema and tables")
            build_database()
            print("[infra_manager] Database build completed")
        except Exception as e:
            print(f"[infra_manager] Database build failed: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError("Failed to build database") from e

    def _create_base_vo_and_root_account(self) -> None:
        """Create the base VO and root account.

        Raises :class:`RuntimeError` on failure.
        """
        from rucio.db.sqla.util import create_base_vo, create_root_account

        try:
            print("[infra_manager] Creating base VO and root account")
            create_base_vo()
            create_root_account()
            print("[infra_manager] Base VO and root account created")
        except Exception as e:
            print(f"[infra_manager] Failed to create base VO / root account: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError("Failed to build database") from e

    def _fix_sqlite_permissions(self) -> None:
        """Set ``/tmp/rucio.db`` to world-readable/writable (0o666).

        Only runs when the database is SQLite.
        """
        if not self._is_sqlite:
            return

        db_path = '/tmp/rucio.db'
        if os.path.exists(db_path):
            print(f"[infra_manager] Setting SQLite database permissions: {db_path}")
            os.chmod(db_path, 0o666)

    def _apply_votest_policy(self) -> None:
        """Rewrite the live rucio.cfg ``[policy]`` section for votest.

        Runs only for the votest suite when a policy is set. The rewrite must
        happen before the httpd restart so the server picks up the new config.
        No policy-package pip install is performed (live CI installs none; the
        rewrite alone is sufficient).

        Raises:
            RuntimeError: if the live ``rucio.cfg`` or the matrix YAML is missing.
        """
        if not self._profile.policy or self._profile.name != "votest":
            return

        # RUCIO_HOME already IS the live etc dir, so rucio.cfg sits directly in it.
        rucio_cfg = os.path.join(os.environ["RUCIO_HOME"], "rucio.cfg")
        matrix_path = (
            Path(os.environ["RUCIO_SOURCE_DIR"])
            / "etc/docker/test/matrix_policy_package_tests.yml"
        )

        if not os.path.exists(rucio_cfg):
            raise RuntimeError(
                f"[infra_manager] votest: live rucio.cfg not found: {rucio_cfg}"
            )
        if not matrix_path.exists():
            raise RuntimeError(
                f"[infra_manager] votest: matrix YAML not found: {matrix_path}"
            )

        from . import votest_support

        matrix = votest_support.load_matrix(matrix_path)
        votest_support.rewrite_policy_section(
            rucio_cfg, matrix[self._profile.policy]["config_overrides"]
        )
        print(f"[infra_manager] Rewrote [policy] for votest policy={self._profile.policy}")

    def _restart_httpd(self) -> None:
        """Gracefully restart Apache httpd and wait for readiness.

        Raises :class:`RuntimeError` on ``CalledProcessError``.
        Silently skips when httpd is not installed (``FileNotFoundError``).
        """
        try:
            subprocess.run(['httpd', '-k', 'graceful'], check=True, capture_output=True)
            print("[infra_manager] Apache httpd restarted")
            time.sleep(2)
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Failed to restart httpd") from e
        except FileNotFoundError:
            print("[infra_manager] Warning: httpd not found, skipping Apache restart")

    # ------------------------------------------------------------------
    # Bootstrap steps
    # ------------------------------------------------------------------

    def _bootstrap_test_data(self) -> None:
        """Create accounts, scopes, and multi-VO setup.

        Mirrors the ``_run_bootstrap_tests()`` helper in conftest.py.
        Raises :class:`RuntimeError` on failure.
        """
        from rucio.client import Client
        from rucio.common.config import config_get, config_get_bool
        from rucio.common.constants import DEFAULT_VO
        from rucio.common.exception import Duplicate, RucioException
        from rucio.common.types import InternalAccount
        from rucio.core.account import add_account_attribute
        from rucio.core.vo import map_vo
        from rucio.gateway.vo import add_vo
        from rucio.tests.common_server import reset_config_table

        try:
            print("[infra_manager] Bootstrapping test data")

            # Create config table including the long VO mappings
            reset_config_table()

            if config_get_bool('common', 'multi_vo', raise_exception=False, default=False):
                vo = {'vo': map_vo(config_get('client', 'vo', raise_exception=False, default='tst'))}
                try:
                    add_vo(
                        new_vo=vo['vo'],
                        issuer='super_root',
                        description='A VO to test multi-vo features',
                        email='N/A',
                        vo=DEFAULT_VO,
                    )
                except Duplicate:
                    print(f'[infra_manager] VO {vo["vo"]} already added')
            else:
                vo = {}

            try:
                client = Client()
            except RucioException as e:
                error_msg = str(e)
                print(f'[infra_manager] Creating client failed: {error_msg}')
                if 'Internal Server Error' in error_msg:
                    server_log = '/var/log/rucio/httpd_error_log'
                    if os.path.exists(server_log):
                        time.sleep(5)
                        with open(server_log, 'r') as fhandle:
                            print(fhandle.readlines()[-200:], file=sys.stderr)
                raise

            try:
                client.add_account('jdoe', 'SERVICE', 'jdoe@email.com')
            except Duplicate:
                print('[infra_manager] Account jdoe already added')

            try:
                add_account_attribute(account=InternalAccount('root', **vo), key='admin', value=True)
            except Exception as error:
                print(f'[infra_manager] {error}')

            try:
                client.add_account('panda', 'SERVICE', 'panda@email.com')
                add_account_attribute(account=InternalAccount('panda', **vo), key='admin', value=True)
            except Duplicate:
                print('[infra_manager] Account panda already added')

            try:
                client.add_scope('jdoe', 'mock')
            except Duplicate:
                print('[infra_manager] Scope mock already added')

            try:
                client.add_scope('root', 'archive')
            except Duplicate:
                print('[infra_manager] Scope archive already added')

            print("[infra_manager] Test data bootstrap completed")

        except Exception as e:
            print(f"[infra_manager] Bootstrap failed: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError("Failed to bootstrap test data") from e

    def _sync_rses(self) -> None:
        """Load RSE repository JSON and create RSEs via Client.

        Uses ``etc/rse_repository.json`` by default; falls back to
        ``etc/rse_repository.json.special`` for the *special* suite.
        Raises :class:`RuntimeError` on failure.
        """
        import json
        import traceback as tb

        from rucio.client import Client
        from rucio.common.exception import Duplicate

        try:
            print("[infra_manager] Syncing RSE repository")

            # Resolve paths relative to source dir when running inside
            # a container (CWD=/opt/rucio, source at /rucio_source).
            source_dir = os.environ.get('RUCIO_SOURCE_DIR', '')
            if self._profile.name == "special":
                special = os.path.join(source_dir, 'etc/rse_repository.json.special')
                if os.path.exists(special):
                    rse_repo = special
                else:
                    rse_repo = os.path.join(source_dir, 'etc/rse_repository.json')
            else:
                rse_repo = os.path.join(source_dir, 'etc/rse_repository.json')

            with open(rse_repo) as f:
                rses_list = json.load(f)

            c = Client()

            for rse in rses_list:
                try:
                    c.add_rse(rse)
                except Duplicate:
                    pass
                except Exception:
                    print("[infra_manager] Failed to add RSE " + rse)
                    tb.print_exc()

                try:
                    supported = rses_list[rse]['protocols'].get('supported', {})
                    for scheme, proto in supported.items():
                        try:
                            c.add_protocol(rse, {**proto, 'scheme': scheme})
                        except Duplicate:
                            pass
                        except Exception:
                            print("[infra_manager] Failed to add protocol to RSE " + rse + ": " + scheme)
                            tb.print_exc()
                except KeyError:
                    pass

                try:
                    for attr in rses_list[rse]['attributes']:
                        try:
                            c.add_rse_attribute(rse, attr, rses_list[rse]['attributes'][attr])
                        except Duplicate:
                            pass
                        except Exception:
                            print("[infra_manager] Failed to add attribute " + attr + " to RSE " + rse)
                            tb.print_exc()
                except KeyError:
                    pass

            print("[infra_manager] RSE repository sync completed")

        except Exception as e:
            print(f"[infra_manager] RSE sync failed: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError("Failed to sync RSE repository") from e

    def _sync_metadata(self) -> None:
        """Create DID metadata keys via Client.

        Raises :class:`RuntimeError` on failure.
        """
        import traceback as tb

        from rucio.client import Client
        from rucio.common.exception import Duplicate

        try:
            print("[infra_manager] Syncing metadata keys")

            meta_keys = [
                ('project', 'ALL', None, ['data13_hip', 'NoProjectDefined']),
                ('run_number', 'ALL', None, ['NoRunNumberDefined']),
                ('stream_name', 'ALL', None, ['NoStreamNameDefined']),
                ('prod_step', 'ALL', None, ['merge', 'recon', 'simul', 'evgen', 'NoProdstepDefined', 'user']),
                ('datatype', 'ALL', None, ['HITS', 'AOD', 'EVNT', 'NTUP_TRIG', 'NTUP_SMWZ', 'NoDatatypeDefined', 'DPD']),
                ('version', 'ALL', None, []),
                ('campaign', 'ALL', None, []),
                ('guid', 'FILE', r'^(\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\}){0,1}$', []),
                ('events', 'DERIVED', r'^\d+$', []),
            ]

            c = Client()

            for key, key_type, value_regexp, values in meta_keys:
                try:
                    c.add_key(key, key_type, value_regexp=value_regexp)
                except Duplicate:
                    pass
                except Exception:
                    print("[infra_manager] Failed to add key " + key)
                    tb.print_exc()

                for value in values:
                    try:
                        c.add_value(key, value)
                    except Duplicate:
                        pass
                    except Exception:
                        print("[infra_manager] Failed to add value " + value + " to key " + key)
                        tb.print_exc()

            print("[infra_manager] Metadata sync completed\n")

        except Exception as e:
            print(f"[infra_manager] Metadata sync failed: {e}")
            import traceback
            traceback.print_exc()
            raise RuntimeError("Failed to sync metadata") from e
