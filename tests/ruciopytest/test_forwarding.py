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

"""Unit tests for the Docker-free forwarding transport core.

These tests cover the pure, unit-testable pieces of the container forwarder:
serialize -> JSON-lines -> replay round-trips, host-only argv filtering, exit-code
mirroring, and curated env-flag construction. No live Docker daemon is required.

Hook signatures (verified against the repo-pinned pytest 7.4.x, _pytest/hookspec.py):
    pytest_report_to_serializable(config, report) -> dict
    pytest_report_from_serializable(config, data) -> report | None
Both accept ``config=`` as a keyword, so forwarding.py calls them with config=...
"""

from __future__ import annotations

import json

import pytest

from tests.ruciopytest.forwarding import (
    REPORT_STREAM_ENV,
    ReportStreamEmitter,
    build_env_flags,
    build_inner_pytest_args,
    make_emitter_from_env,
    mirror_exit_code,
    replay_report_line,
)

pytest_plugins = ["pytester"]


# ---------------------------------------------------------------------------
# Helpers: obtain REAL TestReport objects via an in-process pytest run.
# ---------------------------------------------------------------------------

class _ReportRecorder:
    """A plugin that records every TestReport / CollectReport it receives."""

    def __init__(self):
        self.test_reports = []
        self.collect_reports = []

    def pytest_runtest_logreport(self, report):
        self.test_reports.append(report)

    def pytest_collectreport(self, report):
        self.collect_reports.append(report)


def _run_inline_and_collect(pytester, source):
    """Run an inline test module in a nested in-process pytest, return its recorder.

    The recorder holds the real TestReport / CollectReport objects the nested run
    produced. Serialization and replay in the tests below use the *outer*, still-live
    pytester config (``pytester._request.config``); pytest's report hooks are
    config-agnostic for these report types, so any live config round-trips them.
    """
    pytester.makepyfile(source)
    recorder = _ReportRecorder()
    pytester.runpytest_inprocess("-p", "no:cacheprovider", plugins=[recorder])
    return recorder


@pytest.fixture()
def call_reports(pytester):
    """Return (config, calls): real "call"-phase TestReports (one pass, one fail).

    ``config`` is the live outer pytester config used for serialize/deserialize.
    """
    recorder = _run_inline_and_collect(
        pytester,
        """
        def test_alpha_pass():
            assert True

        def test_beta_fail():
            assert False
        """,
    )
    config = pytester._request.config
    calls = [r for r in recorder.test_reports if r.when == "call"]
    return config, calls


# ---------------------------------------------------------------------------
# Behavior 1: round-trip serialize -> json -> deserialize (1:1 fidelity).
# ---------------------------------------------------------------------------

def test_roundtrip_preserves_nodeid_outcome_when(call_reports):
    config, calls = call_reports
    assert calls, "expected at least one call-phase report"

    for report in calls:
        data = config.hook.pytest_report_to_serializable(config=config, report=report)
        # Survive a true JSON encode/decode (the transport is JSON lines).
        data = json.loads(json.dumps(data))
        restored = config.hook.pytest_report_from_serializable(config=config, data=data)

        assert restored is not None
        assert restored.nodeid == report.nodeid
        assert restored.when == report.when
        assert restored.outcome == report.outcome


def test_roundtrip_failure_preserves_longrepr(call_reports):
    config, calls = call_reports
    failing = [r for r in calls if r.outcome == "failed"]
    assert failing, "expected a failing call report"
    report = failing[0]

    data = config.hook.pytest_report_to_serializable(config=config, report=report)
    data = json.loads(json.dumps(data))
    restored = config.hook.pytest_report_from_serializable(config=config, data=data)

    assert restored.longrepr is not None
    # Renders without raising (terminal/junit consume it on the host side).
    assert str(restored.longrepr)


# ---------------------------------------------------------------------------
# Behavior 2: replay_report_line re-dispatches via the right hook.
# ---------------------------------------------------------------------------

def _serialize_line(config, report):
    data = config.hook.pytest_report_to_serializable(config=config, report=report)
    return json.dumps(data)


def test_replay_dispatches_testreport_once(call_reports, monkeypatch):
    config, calls = call_reports
    report = calls[0]
    line = _serialize_line(config, report)

    recorder = _ReportRecorder()
    # Build a tiny session-like object exposing config with our recorder registered.
    config.pluginmanager.register(recorder, name="replay-recorder-test")
    try:
        session = type("S", (), {"config": config})()
        dispatched = replay_report_line(session, line)
    finally:
        config.pluginmanager.unregister(name="replay-recorder-test")

    assert dispatched is True
    assert len(recorder.test_reports) == 1
    assert recorder.test_reports[0].nodeid == report.nodeid
    assert recorder.collect_reports == []


def test_replay_dispatches_collectreport(pytester):
    # Produce a real CollectReport by running a tiny module and capturing collect.
    recorder = _run_inline_and_collect(
        pytester,
        """
        def test_gamma():
            assert True
        """,
    )
    config = pytester._request.config
    assert recorder.collect_reports, "expected a collect report"
    creport = recorder.collect_reports[0]
    line = _serialize_line(config, creport)

    sink = _ReportRecorder()
    config.pluginmanager.register(sink, name="collect-sink-test")
    try:
        session = type("S", (), {"config": config})()
        dispatched = replay_report_line(session, line)
    finally:
        config.pluginmanager.unregister(name="collect-sink-test")

    assert dispatched is True
    assert len(sink.collect_reports) == 1


def test_replay_blank_line_is_noop(call_reports):
    config, _calls = call_reports
    sink = _ReportRecorder()
    config.pluginmanager.register(sink, name="blank-sink-test")
    try:
        session = type("S", (), {"config": config})()
        assert replay_report_line(session, "") is False
        assert replay_report_line(session, "   \n") is False
    finally:
        config.pluginmanager.unregister(name="blank-sink-test")
    assert sink.test_reports == []
    assert sink.collect_reports == []


# ---------------------------------------------------------------------------
# Behavior 3: 1:1 mapping, never collapse N reports into one wrapper.
# ---------------------------------------------------------------------------

def test_replay_three_reports_yields_three_distinct_dispatches(pytester):
    recorder = _run_inline_and_collect(
        pytester,
        """
        def test_one():
            assert True

        def test_two():
            assert True

        def test_three():
            assert True
        """,
    )
    config = pytester._request.config
    calls = [r for r in recorder.test_reports if r.when == "call"]
    assert len(calls) == 3

    lines = [_serialize_line(config, r) for r in calls]

    sink = _ReportRecorder()
    config.pluginmanager.register(sink, name="three-sink-test")
    try:
        session = type("S", (), {"config": config})()
        for line in lines:
            assert replay_report_line(session, line) is True
    finally:
        config.pluginmanager.unregister(name="three-sink-test")

    assert len(sink.test_reports) == 3
    nodeids = {r.nodeid for r in sink.test_reports}
    assert len(nodeids) == 3  # never collapsed into a single wrapper


# ---------------------------------------------------------------------------
# Behavior 4: build_inner_pytest_args strips only host-only flags.
# ---------------------------------------------------------------------------

def test_build_inner_args_strips_host_only_split_form():
    argv = [
        "--suite=remote_dbs",
        "--no-run-in-container",
        "--container-env",
        "A=1",
        "-k",
        "foo",
        "-v",
        "tests/test_x.py",
    ]
    assert build_inner_pytest_args(argv) == [
        "--suite=remote_dbs",
        "-k",
        "foo",
        "-v",
        "tests/test_x.py",
    ]


def test_build_inner_args_strips_attached_and_run_in_container():
    argv = [
        "--run-in-container",
        "--container-env=A=1",
        "--suite=client",
        "-m",
        "noparallel",
        "tests/test_y.py",
    ]
    assert build_inner_pytest_args(argv) == [
        "--suite=client",
        "-m",
        "noparallel",
        "tests/test_y.py",
    ]


def test_build_inner_args_passes_everything_else_through():
    argv = ["--co", "--dry-run", "--junitxml=out.xml", "-x", "-vv", "tests/"]
    assert build_inner_pytest_args(argv) == argv


# ---------------------------------------------------------------------------
# Behavior 5: mirror_exit_code surfaces the exact container returncode.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("code", [0, 1, 2, 3, 4, 5])
def test_mirror_exit_code_is_identity(code):
    assert mirror_exit_code(code) == code


# ---------------------------------------------------------------------------
# Behavior 6: build_env_flags applies the curated allowlist + extras.
# ---------------------------------------------------------------------------

def test_build_env_flags_allowlist_and_extras():
    environ = {
        "RUCIO_HOME": "/x",
        "SUITE": "remote_dbs",
        "RDBMS": "postgres14",
        "PATH": "/bin",
        "POLICY": "atlas",
    }
    flags = build_env_flags(environ, ["FOO=bar"])

    # Represent the flag list as (-e, "K=V") pairs for set membership assertions.
    pairs = set(zip(flags[::2], flags[1::2]))
    assert all(flag == "-e" for flag in flags[::2])

    assert ("-e", "RUCIO_HOME=/x") in pairs
    assert ("-e", "SUITE=remote_dbs") in pairs
    assert ("-e", "RDBMS=postgres14") in pairs
    assert ("-e", "POLICY=atlas") in pairs
    assert ("-e", "FOO=bar") in pairs
    assert ("-e", "PATH=/bin") not in pairs


def test_build_env_flags_empty_extras():
    flags = build_env_flags({"RUCIO_CFG": "/etc"}, [])
    assert flags == ["-e", "RUCIO_CFG=/etc"]


# ---------------------------------------------------------------------------
# Emitter: env-driven construction + JSON-lines emission.
# ---------------------------------------------------------------------------

def test_make_emitter_from_env_none_when_unset(call_reports, monkeypatch):
    config, _calls = call_reports
    monkeypatch.delenv(REPORT_STREAM_ENV, raising=False)
    assert make_emitter_from_env(config) is None


def test_emitter_writes_one_json_object_per_report(call_reports, tmp_path):
    config, calls = call_reports
    assert calls
    stream = tmp_path / "reports.jsonl"

    emitter = ReportStreamEmitter(config, str(stream))
    try:
        for report in calls:
            emitter.emit(report)
    finally:
        emitter.close()

    lines = [ln for ln in stream.read_text().splitlines() if ln.strip()]
    assert len(lines) == len(calls)
    for ln in lines:
        obj = json.loads(ln)  # each line is a standalone JSON object
        assert "$report_type" in obj
