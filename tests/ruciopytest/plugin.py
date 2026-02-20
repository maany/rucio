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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _print_profile_summary(config: pytest.Config, profile: SuiteProfile) -> None:
    """Print a summary box of the resolved suite profile."""
    if config.pluginmanager.hasplugin("xdist"):
        workers = getattr(config.option, "numprocesses", 0)
    else:
        workers = 0

    tw = config.get_terminal_writer()
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
