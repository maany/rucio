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

"""Suite-selection parity guard (SUIT-07 / SUIT-09).

A checked-in baseline (``parity_baselines.json``) records the sorted file-path
set each suite selects. This test fails on any silent drift -- a change to the
``tests/`` inventory or to the policy YAML allow/deny lists makes the build fail
instead of silently altering what each suite runs.

The guard is **import-free of rucio** and runs in plain CI: no live server, no
container, no rucio import. votest selection is pure path math; the whole-``tests/``
suites are a deterministic glob; ``client`` is the 3 declared profile paths.

Comparisons are SORTED SETS -- votest order is set-derived / non-deterministic.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.ruciopytest import profiles, votest_support

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_PATH = REPO_ROOT / "tests/ruciopytest/parity_baselines.json"
MATRIX_PATH = REPO_ROOT / "etc/docker/test/matrix_policy_package_tests.yml"


def load_baseline() -> dict:
    """Load the checked-in parity baseline JSON."""
    with open(BASELINE_PATH) as f:
        return json.load(f)


def full_tests() -> list[str]:
    """Repo-relative sorted set of all ``tests/test_*.py`` files (the full suite)."""
    return sorted(str(p.relative_to(REPO_ROOT)) for p in (REPO_ROOT / "tests").glob("test_*.py"))


def assert_set_equals(actual, expected, label: str) -> None:
    """Assert two collections are equal as sorted sets; on drift, fail readably.

    Builds an added/removed diff so the baseline update is intentional and
    reviewable. ``added`` = in actual but not expected; ``removed`` = in expected
    but not actual.
    """
    a, e = set(actual), set(expected)
    if a == e:
        return
    added = sorted(a - e)
    removed = sorted(e - a)
    msg = [f"parity drift for {label!r}:"]
    if added:
        msg.append(f"  added (now selected, not in baseline): {added}")
    if removed:
        msg.append(f"  removed (in baseline, no longer selected): {removed}")
    msg.append("  -> if intentional, update tests/ruciopytest/parity_baselines.json")
    pytest.fail("\n".join(msg))


def _matrix() -> dict:
    return votest_support.load_matrix(MATRIX_PATH)


def test_votest_selection() -> None:
    """SUIT-07/SUIT-09: votest computed set matches baseline for every policy.

    Data-driven over the YAML policy keys (no hardcoded list). Also locks the
    literal CI ground-truth counts atlas==36, belleii==52.
    """
    baseline = load_baseline()
    matrix = _matrix()
    for policy in matrix:
        computed = votest_support.collect_votest_paths(matrix, policy, REPO_ROOT)
        assert_set_equals(computed, baseline[f"votest:{policy}"], f"votest:{policy}")

    atlas = votest_support.collect_votest_paths(matrix, "atlas", REPO_ROOT)
    belleii = votest_support.collect_votest_paths(matrix, "belleii", REPO_ROOT)
    assert len(atlas) == 36, f"atlas drift: {len(atlas)} != 36"
    assert len(belleii) == 52, f"belleii drift: {len(belleii)} != 52"


def test_policy_resolution() -> None:
    """SUIT-07: --policy flag wins over POLICY env; both-missing -> None.

    Uses a tiny fake config object exposing ``getoption("policy")`` and a dict
    env. Documents the plugin's UsageError contract: votest with a None policy is
    rejected; an unknown policy (not a matrix key) is detectable.
    """

    class FakeConfig:
        def __init__(self, policy):
            self._policy = policy

        def getoption(self, name, default=None):
            assert name == "policy"
            return self._policy if self._policy is not None else default

    # flag wins over env
    assert votest_support.resolve_policy(FakeConfig("atlas"), {"POLICY": "belleii"}) == "atlas"
    # env fallback when no flag
    assert votest_support.resolve_policy(FakeConfig(None), {"POLICY": "belleii"}) == "belleii"
    # both missing -> None (plugin raises UsageError on None for votest)
    assert votest_support.resolve_policy(FakeConfig(None), {}) is None

    # unknown policy is detectable against the data-driven matrix keys
    matrix = _matrix()
    resolved = votest_support.resolve_policy(FakeConfig("nope"), {})
    assert resolved not in matrix


def test_baseline_parity() -> None:
    """SUIT-09: client / remote_dbs / sqlite / multi_vo match the baseline."""
    baseline = load_baseline()
    client_paths = profiles.SUITE_PROFILES["client"].test_paths
    assert_set_equals(client_paths, baseline["client"], "client")

    for name in ("remote_dbs", "sqlite", "multi_vo"):
        assert_set_equals(full_tests(), baseline[name], name)


def test_drift_detected() -> None:
    """SUIT-09: the guard bites -- a tampered baseline triggers a readable diff.

    Take the real full-tests set, add a fake non-existent path, and confirm
    assert_set_equals raises (pytest.fail) with the offending path named. This
    proves drift is never silently tolerated.
    """
    fake = "tests/test_DOES_NOT_EXIST.py"
    tampered = full_tests() + [fake]
    # pytest.fail raises the Failed outcome (a BaseException), so catch it explicitly.
    with pytest.raises(pytest.fail.Exception) as excinfo:
        assert_set_equals(full_tests(), tampered, "drift-probe")
    assert fake in str(excinfo.value)
