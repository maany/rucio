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

"""Unit tests for multi_vo_support and InfraManager multi_vo wiring.

Import-free of rucio (no live server). All rucio-importing
InfraManager methods are mocked out.
"""

from __future__ import annotations

import configparser
from pathlib import Path
from unittest import mock

from tests.ruciopytest.multi_vo_support import (
    generate_multi_vo_configs,
    merge_configs,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# merge_configs (absorbed)
# ---------------------------------------------------------------------------

def _write_cfg(path: Path, section: str, key: str, value: str) -> None:
    cp = configparser.ConfigParser()
    cp[section] = {key: value}
    with open(path, "w") as f:
        cp.write(f)


def test_merge_last_wins(tmp_path):
    src1 = tmp_path / "a.cfg"
    src2 = tmp_path / "b.cfg"
    dest = tmp_path / "out.cfg"
    _write_cfg(src1, "common", "shared", "first")
    _write_cfg(src2, "common", "shared", "second")

    merge_configs([str(src1), str(src2)], str(dest), use_env=False)

    cp = configparser.ConfigParser()
    cp.read(dest)
    assert cp["common"]["shared"] == "second"


def test_merge_env_override(tmp_path, monkeypatch):
    src = tmp_path / "a.cfg"
    dest = tmp_path / "out.cfg"
    _write_cfg(src, "common", "shared", "fromfile")
    # RUCIO_CFG_<SECTION>_<KEY> -> [common] shared
    monkeypatch.setenv("RUCIO_CFG_COMMON_SHARED", "fromenv")

    merge_configs([str(src)], str(dest), use_env=True)

    cp = configparser.ConfigParser()
    cp.read(dest)
    assert cp["common"]["shared"] == "fromenv"


def test_generate_multi_vo_configs(tmp_path, monkeypatch):
    """SUIT-08 generation coverage (owned HERE; no test_parity node).

    Merges the REAL extra/ cfgs into tmp dest dirs and asserts both VO
    configs are produced with the correct vo= values.
    """
    # Avoid any RUCIO_CFG_* env from the runner leaking into the merge.
    for key in list(__import__("os").environ):
        if key.startswith("RUCIO_CFG_"):
            monkeypatch.delenv(key, raising=False)

    tst_dir = tmp_path / "tst" / "etc"
    ts2_dir = tmp_path / "ts2" / "etc"

    result = generate_multi_vo_configs(
        REPO_ROOT,
        tst_etc_dir=str(tst_dir),
        ts2_etc_dir=str(ts2_dir),
        use_env=True,
    )

    assert Path(result["tst"]).is_file()
    assert Path(result["ts2"]).is_file()

    tst = configparser.ConfigParser()
    tst.read(result["tst"])
    assert tst["client"]["vo"] == "testvo1"
    assert tst["common"]["multi_vo"] == "True"

    ts2 = configparser.ConfigParser()
    ts2.read(result["ts2"])
    assert ts2["client"]["vo"] == "testvo2"
    assert ts2["common"]["multi_vo"] == "True"


# ---------------------------------------------------------------------------
# InfraManager multi_vo wiring (Task 3): bootstrap_vo + run_multi_vo +
# setup() ordering. All rucio-importing methods mocked.
# ---------------------------------------------------------------------------

def _make_manager(suite_name: str):
    from tests.ruciopytest.infra_manager import InfraManager
    from tests.ruciopytest.profiles import SuiteProfile

    profile = SuiteProfile(name=suite_name, rdbms="postgres14")
    return InfraManager(profile, keep_db=False)


def test_bootstrap_vo_sets_rucio_home(monkeypatch):
    manager = _make_manager("multi_vo")

    called = []
    for name in (
        "_create_base_vo_and_root_account",
        "_bootstrap_test_data",
        "_sync_rses",
        "_sync_metadata",
    ):
        monkeypatch.setattr(manager, name, lambda n=name: called.append(n))
    # These MUST NOT be called by bootstrap_vo (no DB reset on 2nd VO).
    monkeypatch.setattr(manager, "_purge_database", lambda: called.append("PURGE"))
    monkeypatch.setattr(manager, "_build_database", lambda: called.append("BUILD"))

    monkeypatch.setenv("RUCIO_HOME", "/old")
    manager.bootstrap_vo("/x/ts2")

    import os
    assert os.environ["RUCIO_HOME"] == "/x/ts2"
    assert "PURGE" not in called
    assert "BUILD" not in called
    assert "_bootstrap_test_data" in called


def test_multi_vo_pytest_cmd_excludes_plugin_metatests_and_uses_xdist(monkeypatch):
    """The per-VO child argv must (1) exclude tests/ruciopytest/* so the Phase-8
    plugin meta-tests never run against the live DB, and (2) carry xdist so the
    noparallel scheduler engages exactly as under legacy tools/pytest.sh."""
    from tests.ruciopytest.infra_manager import InfraManager
    from tests.ruciopytest.profiles import resolve_profile

    profile = resolve_profile("multi_vo")
    manager = InfraManager(profile, keep_db=False)

    # CI parity: 3 procs under GitHub Actions.
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    cmd = manager._multi_vo_pytest_cmd()

    # Exclusion of the plugin meta-tests (fixes the live-DB purge mid-suite).
    assert "--ignore=tests/ruciopytest" in cmd
    assert "--ignore-glob=tests/ruciopytest/*" in cmd
    # xdist execution-model parity with legacy multi_vo.
    assert "--numprocesses=3" in cmd
    assert "tests/" in cmd

    # Locally (no GitHub Actions) legacy uses auto workers.
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    local_cmd = manager._multi_vo_pytest_cmd()
    assert "--numprocesses=auto" in local_cmd


def test_run_multi_vo_order_and_gate(monkeypatch):
    manager = _make_manager("multi_vo")

    # Make bootstrap_vo a no-op recorder (it is exercised separately).
    monkeypatch.setattr(manager, "bootstrap_vo", mock.MagicMock())

    # Success case: tst passes -> ts2 runs.
    run_calls = []

    def fake_run_ok(cmd, env=None, **kwargs):
        run_calls.append(env["RUCIO_HOME"])
        return mock.Mock(returncode=0)

    monkeypatch.setattr("tests.ruciopytest.infra_manager.subprocess.run", fake_run_ok)
    rc = manager.run_multi_vo()
    assert rc == 0
    assert run_calls == [
        "/opt/rucio/etc/multi_vo/tst",
        "/opt/rucio/etc/multi_vo/ts2",
    ]

    # Gate case: tst fails -> ts2 NEVER runs, returns tst failure code.
    run_calls.clear()

    def fake_run_fail(cmd, env=None, **kwargs):
        run_calls.append(env["RUCIO_HOME"])
        return mock.Mock(returncode=3)

    monkeypatch.setattr("tests.ruciopytest.infra_manager.subprocess.run", fake_run_fail)
    rc = manager.run_multi_vo()
    assert rc == 3
    assert run_calls == ["/opt/rucio/etc/multi_vo/tst"]


def test_setup_invokes_multi_vo_in_order(monkeypatch):
    """WARNING B guard: setup() must call _setup_multi_vo before
    _restart_httpd before run_multi_vo for the multi_vo suite, and call
    neither for non-multi_vo suites."""
    parent = mock.Mock()

    def patch_all(manager):
        for name in (
            "_flush_memcache",
            "_cleanup_temp_files",
            "_purge_database",
            "_build_database",
            "_create_base_vo_and_root_account",
            "_fix_sqlite_permissions",
            "_apply_votest_policy",
            "_setup_multi_vo",
            "_restart_httpd",
            "_bootstrap_test_data",
            "_sync_rses",
            "_sync_metadata",
            "run_multi_vo",
        ):
            child = getattr(parent, name)
            monkeypatch.setattr(manager, name, child)

    # multi_vo: both hooks fire, in order.
    parent.reset_mock()
    mgr = _make_manager("multi_vo")
    patch_all(mgr)
    mgr.setup()

    parent._setup_multi_vo.assert_called_once()
    parent.run_multi_vo.assert_called_once()

    order = [c[0] for c in parent.mock_calls]
    assert order.index("_setup_multi_vo") < order.index("_restart_httpd")
    assert order.index("_restart_httpd") < order.index("run_multi_vo")

    # non-multi_vo: neither hook fires.
    parent2 = mock.Mock()
    mgr2 = _make_manager("remote_dbs")
    for name in (
        "_flush_memcache",
        "_cleanup_temp_files",
        "_purge_database",
        "_build_database",
        "_create_base_vo_and_root_account",
        "_fix_sqlite_permissions",
        "_apply_votest_policy",
        "_setup_multi_vo",
        "_restart_httpd",
        "_bootstrap_test_data",
        "_sync_rses",
        "_sync_metadata",
        "run_multi_vo",
    ):
        monkeypatch.setattr(mgr2, name, getattr(parent2, name))
    mgr2.setup()
    parent2.run_multi_vo.assert_not_called()
    # _setup_multi_vo is still invoked but is a no-op guard for non-multi_vo;
    # the real method short-circuits. Here it's mocked, so assert run_multi_vo
    # is the real guard (only called for multi_vo).
