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

from __future__ import annotations

import os

import pytest

from .profiles import SuiteProfile, resolve_profile
from .xdist_config import configure_xdist

# ---------------------------------------------------------------------------
# Type-safe stash keys
# ---------------------------------------------------------------------------

suite_profile_key = pytest.StashKey[SuiteProfile]()
container_manager_key = pytest.StashKey["ContainerManager"]()


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register rucio-specific CLI options."""
    group = parser.getgroup("rucio", "Rucio test framework")
    group.addoption(
        "--suite",
        choices=["client", "remote_dbs", "sqlite", "multi_vo", "votest"],
        default=None,
        help="Test suite to run",
    )
    group.addoption(
        "--keep-db",
        action="store_true",
        default=False,
        help="Keep database from previous run (skip purge/rebuild/seed)",
    )
    group.addoption(
        "--xdist-workers",
        type=int,
        default=None,
        dest="xdist_workers",
        help="Number of xdist workers (overrides auto-detection)",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Resolve suite profile and configure xdist.

    On xdist workers the profile is still resolved (it is cheap) and
    stored in stash so that fixtures can access it, but xdist
    configuration and the summary banner are skipped -- those only
    need to run on the controller.
    """
    is_worker = hasattr(config, "workerinput")

    suite_name = config.getoption("suite", default=None)
    if suite_name is None:
        return  # Plugin dormant when --suite not provided

    # Resolve profile (with optional RDBMS override from CI matrix)
    rdbms_override = os.environ.get("RDBMS")
    profile = resolve_profile(suite_name, rdbms_override)

    # Store in stash (available on both controller and workers)
    config.stash[suite_profile_key] = profile

    # Backward compatibility: many existing tests check os.environ["SUITE"]
    os.environ["SUITE"] = suite_name

    if not is_worker:
        configure_xdist(config, profile)
        _print_profile_summary(config, profile)

        # Container lifecycle (Phase 3) -- host-side only
        _in_container = os.path.exists("/.dockerenv") or os.environ.get("RUCIO_SOURCE_DIR")
        if _in_container:
            print("[plugin] Running inside container, skipping Docker Compose lifecycle")
        elif profile.compose_profiles:
            from .container_manager import ContainerManager

            project_name = ContainerManager.make_project_name(profile.name, profile.rdbms)
            cm = ContainerManager(project_name, profile.compose_profiles, str(config.rootdir))
            cm.start()
            config.stash[container_manager_key] = cm

        # Database lifecycle (Phase 2)
        if profile.name != "client":
            keep_db = config.getoption("--keep-db", default=False)
            from .infra_manager import InfraManager
            manager = InfraManager(profile, keep_db=keep_db)
            manager.setup()


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print container log file locations and add to JUnit XML."""
    cm = config.stash.get(container_manager_key, None)
    if cm is None:
        return

    log_dir = cm.log_dir
    if not log_dir.exists():
        return

    log_files = sorted(log_dir.glob("*.log"))
    if not log_files:
        return

    # Print log file locations in terminal
    terminalreporter.write_sep("=", "Container Logs")
    for log_file in log_files:
        terminalreporter.write_line(f"  {log_file}", yellow=True)
    terminalreporter.write_sep("=")

    # Add to JUnit XML if --junitxml was specified
    xml_plugin = config.pluginmanager.get_plugin("junitxml")
    if xml_plugin is not None:
        try:
            for log_file in log_files:
                xml_plugin.add_global_property(
                    f"container_log:{log_file.name}",
                    str(log_file)
                )
        except (AttributeError, TypeError):
            # Fallback: junitxml API may vary across pytest versions
            pass


def pytest_unconfigure(config: pytest.Config) -> None:
    """Stop containers on session end."""
    cm = config.stash.get(container_manager_key, None)
    if cm is not None:
        cm.stop(capture_logs=True)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _print_profile_summary(config: pytest.Config, profile: SuiteProfile) -> None:
    """Print a summary box of the resolved suite profile."""
    if config.pluginmanager.hasplugin("xdist"):
        workers = getattr(config.option, "numprocesses", 0)
    else:
        workers = 0

    # Terminal reporter may not be registered yet during early pytest_configure.
    # Use pluginmanager to check; fall back to plain print if unavailable.
    terminalreporter = config.pluginmanager.get_plugin("terminalreporter")
    if terminalreporter is not None:
        tw = terminalreporter._tw
        tw.line()
        tw.sep("=", "Rucio Test Suite Configuration")
        tw.line(f"  Suite:          {profile.name}")
        tw.line(f"  RDBMS:          {profile.rdbms}")
        tw.line(f"  xdist enabled:  {profile.xdist_enabled}")
        tw.line(f"  Workers:        {workers}")
        tw.line(f"  Test paths:     {', '.join(profile.test_paths)}")
        if profile.env_vars:
            tw.line(f"  Env vars:       {profile.env_vars}")
        tw.sep("=")
        tw.line()
    else:
        # Fallback: plain print when terminal writer is not yet available
        print()
        print("=" * 60)
        print("  Rucio Test Suite Configuration")
        print("=" * 60)
        print(f"  Suite:          {profile.name}")
        print(f"  RDBMS:          {profile.rdbms}")
        print(f"  xdist enabled:  {profile.xdist_enabled}")
        print(f"  Workers:        {workers}")
        print(f"  Test paths:     {', '.join(profile.test_paths)}")
        if profile.env_vars:
            print(f"  Env vars:       {profile.env_vars}")
        print("=" * 60)
        print()
