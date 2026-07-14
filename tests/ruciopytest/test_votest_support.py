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

"""Unit tests for votest_support (import-free of rucio, no live server)."""

import configparser
from pathlib import Path

from tests.ruciopytest.votest_support import (
    collect_votest_paths,
    load_matrix,
    resolve_policy,
    rewrite_policy_section,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = REPO_ROOT / "etc/docker/test/matrix_policy_package_tests.yml"


def test_collect_votest_paths_atlas_count():
    matrix = load_matrix(MATRIX_PATH)
    paths = collect_votest_paths(matrix, "atlas", REPO_ROOT)
    assert len(paths) == 36, f"expected 36 atlas files, got {len(paths)}"
    assert all(p.startswith("tests/") for p in paths)
    assert all(p.endswith(".py") for p in paths)


def test_collect_votest_paths_belleii_count():
    matrix = load_matrix(MATRIX_PATH)
    paths = collect_votest_paths(matrix, "belleii", REPO_ROOT)
    assert len(paths) == 52, f"expected 52 belleii files, got {len(paths)}"
    assert all(p.startswith("tests/") for p in paths)
    assert all(p.endswith(".py") for p in paths)


def test_collect_drops_nonexistent(tmp_path):
    # Build a tiny fake repo: tests/ with exactly one real test file.
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_real.py").write_text("# real\n")

    matrix = {
        "fake": {
            "tests": {
                "allow": [
                    "rucio_tests/test_real.py",
                    "rucio_tests/test_missing.py",  # does not exist -> dropped
                ],
                "deny": [],
            }
        }
    }
    paths = collect_votest_paths(matrix, "fake", tmp_path)
    assert paths == ["tests/test_real.py"]


def test_rewrite_policy_section(tmp_path):
    cfg_path = tmp_path / "rucio.cfg"
    cp = configparser.ConfigParser()
    cp["policy"] = {"stale_key": "old_value"}
    cp["client"] = {"vo": "tst"}
    with open(cfg_path, "w") as f:
        cp.write(f)

    rewrite_policy_section(str(cfg_path), {"permission": "atlas", "schema": "atlas"})

    out = configparser.ConfigParser()
    out.read(cfg_path)
    assert dict(out["policy"]) == {"permission": "atlas", "schema": "atlas"}
    assert "stale_key" not in out["policy"]
    # Unrelated section untouched.
    assert dict(out["client"]) == {"vo": "tst"}


def test_rewrite_policy_section_creates_missing(tmp_path):
    cfg_path = tmp_path / "rucio.cfg"
    cp = configparser.ConfigParser()
    cp["client"] = {"vo": "tst"}
    with open(cfg_path, "w") as f:
        cp.write(f)

    rewrite_policy_section(str(cfg_path), {"permission": "belleii"})

    out = configparser.ConfigParser()
    out.read(cfg_path)
    assert dict(out["policy"]) == {"permission": "belleii"}


class _FakeConfig:
    def __init__(self, policy):
        self._policy = policy

    def getoption(self, name, default=None):
        if name == "policy":
            return self._policy
        return default


def test_resolve_policy_flag_wins():
    cfg = _FakeConfig("atlas")
    assert resolve_policy(cfg, {"POLICY": "belleii"}) == "atlas"


def test_resolve_policy_env_fallback():
    cfg = _FakeConfig(None)
    assert resolve_policy(cfg, {"POLICY": "belleii"}) == "belleii"


def test_resolve_policy_none():
    cfg = _FakeConfig(None)
    assert resolve_policy(cfg, {}) is None
