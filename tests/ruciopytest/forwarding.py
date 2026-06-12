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

"""Docker-free transport core for the container pytest forwarder.

This module holds the pure, unit-testable glue that lets a host-side pytest run
mirror, 1:1, the reports produced by a pytest run *inside* a container -- without
introducing any new pip dependency. It uses ONLY the Python standard library plus
pytest's own core report-serialization hooks.

Pieces:

* ``build_inner_pytest_args`` -- strip host-only forwarding-control flags from the
  argv before it is handed to the inner (container) pytest; everything else passes
  through untouched.
* ``ReportStreamEmitter`` / ``make_emitter_from_env`` -- container side. Serialize
  each report through the core ``pytest_report_to_serializable`` hook and write one
  JSON object per line to a stream file.
* ``replay_report_line`` -- host side. Reconstruct a report through the core
  ``pytest_report_from_serializable`` hook and re-dispatch it through
  ``pytest_runtest_logreport`` / ``pytest_collectreport`` so the host's terminal,
  junitxml and ``session.testsfailed`` all behave as if the test ran locally.
* ``mirror_exit_code`` -- map the container pytest returncode onto the host outcome.
* ``build_env_flags`` -- curated env allowlist + explicit overrides -> ``-e K=V``.

Hook signatures (pytest 7.4.x, _pytest/hookspec.py): both
``pytest_report_to_serializable(config, report)`` and
``pytest_report_from_serializable(config, data)`` accept ``config=`` as a keyword,
so the calls below pass ``config=...`` explicitly.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Iterable, List, Mapping, Optional

if TYPE_CHECKING:  # pragma: no cover - typing only
    from _pytest.config import Config
    from _pytest.main import Session

__all__ = [
    "REPORT_STREAM_ENV",
    "ReportStreamEmitter",
    "build_env_flags",
    "build_inner_pytest_args",
    "make_emitter_from_env",
    "mirror_exit_code",
    "register_container_stream",
    "replay_report_line",
    "run_forwarded_session",
]

# ---------------------------------------------------------------------------
# argv filtering
# ---------------------------------------------------------------------------

# Host-only store flags: meaningful on the host, never forwarded into the
# container's pytest invocation.
_HOST_ONLY_FLAGS = frozenset({"--run-in-container", "--no-run-in-container"})

# --container-env consumes a following value in split form ("--container-env A=1")
# or carries it attached ("--container-env=A=1"). Both forms are host-only.
_CONTAINER_ENV_FLAG = "--container-env"
_CONTAINER_ENV_ATTACHED_PREFIX = _CONTAINER_ENV_FLAG + "="


def build_inner_pytest_args(argv: List[str]) -> List[str]:
    """Return ``argv`` with host-only forwarding-control flags removed.

    Stripped:
      * ``--run-in-container`` / ``--no-run-in-container`` (store flags)
      * ``--container-env VALUE`` (split form: flag and its value)
      * ``--container-env=VALUE`` (attached form)

    Everything else -- ``--suite``, ``--keep-db``, ``--xdist-workers``, ``-k``,
    ``-m``, ``-x``, ``-v``, ``--junitxml``, ``--co``, ``--dry-run`` and test paths --
    passes through in its original order.
    """
    result: List[str] = []
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if token in _HOST_ONLY_FLAGS:
            i += 1
            continue
        if token == _CONTAINER_ENV_FLAG:
            # Skip the flag and its value (if a value follows).
            i += 2
            continue
        if token.startswith(_CONTAINER_ENV_ATTACHED_PREFIX):
            i += 1
            continue
        result.append(token)
        i += 1
    return result


# ---------------------------------------------------------------------------
# env flags
# ---------------------------------------------------------------------------

# Curated allowlist of environment variables forwarded into the container.
_ENV_ALLOWLIST_EXACT = frozenset({"SUITE", "POLICY", "RDBMS"})
_ENV_ALLOWLIST_PREFIXES = ("RUCIO_",)


def _is_allowlisted(key: str) -> bool:
    return key in _ENV_ALLOWLIST_EXACT or key.startswith(_ENV_ALLOWLIST_PREFIXES)


def build_env_flags(
    environ: Mapping[str, str],
    extra_container_env: Iterable[str],
) -> List[str]:
    """Build repeatable ``-e KEY=VALUE`` flags for ``docker exec``/``run``.

    Includes every entry of ``environ`` whose key is in the curated allowlist
    (exact match in :data:`_ENV_ALLOWLIST_EXACT` or carrying an allowlisted
    prefix), followed by each explicit ``KEY=VALUE`` string in
    ``extra_container_env``.
    """
    flags: List[str] = []
    for key, value in environ.items():
        if _is_allowlisted(key):
            flags += ["-e", f"{key}={value}"]
    for kv in extra_container_env:
        flags += ["-e", kv]
    return flags


# ---------------------------------------------------------------------------
# report stream emitter (container side)
# ---------------------------------------------------------------------------

# Path the host sets when stream mode is active; the container-side hooks emit
# serialized reports here, one JSON object per line.
REPORT_STREAM_ENV = "RUCIO_FORWARD_STREAM"


class ReportStreamEmitter:
    """Serialize reports to a JSON-lines stream using pytest's core hook.

    Each :meth:`emit` call serializes one report through
    ``config.hook.pytest_report_to_serializable`` and writes it as a single JSON
    line, flushing immediately so the host can consume the stream incrementally.
    """

    def __init__(self, config: "Config", path: str) -> None:
        self._config = config
        self._path = path
        # Line-buffered append so concurrent host tailing sees whole lines.
        self._fh = open(path, "a", buffering=1, encoding="utf-8")

    def emit(self, report) -> None:
        data = self._config.hook.pytest_report_to_serializable(
            config=self._config, report=report
        )
        self._fh.write(json.dumps(data) + "\n")
        self._fh.flush()

    def close(self) -> None:
        if self._fh is not None and not self._fh.closed:
            self._fh.flush()
            self._fh.close()


def make_emitter_from_env(config: "Config") -> Optional[ReportStreamEmitter]:
    """Return a :class:`ReportStreamEmitter` if stream mode is active, else ``None``.

    Stream mode is active when :data:`REPORT_STREAM_ENV` is set to a non-empty
    path in the environment (the host sets this before launching the container's
    pytest). When unset/empty, returns ``None`` so the container-side hooks no-op.
    """
    path = os.environ.get(REPORT_STREAM_ENV)
    if not path:
        return None
    return ReportStreamEmitter(config, path)


class _StreamReportPlugin:
    """Container-side pytest plugin that emits every report through an emitter.

    Registered (only when stream mode is active) on the in-container pytest's
    plugin manager so that each ``TestReport`` / ``CollectReport`` is serialized
    to the JSON-lines stream the host tails. Under xdist this lives on the
    controller, which receives the worker reports too -- so every test surfaces
    exactly once on the host (Pitfall 5).
    """

    def __init__(self, emitter: "ReportStreamEmitter") -> None:
        self._emitter = emitter

    def pytest_runtest_logreport(self, report) -> None:
        self._emitter.emit(report)

    def pytest_collectreport(self, report) -> None:
        self._emitter.emit(report)


def register_container_stream(config: "Config") -> bool:
    """Attach the stream-emitter plugin if :data:`REPORT_STREAM_ENV` is set.

    Called from the container-side ``pytest_configure``. Registers
    unconditionally (no ``is_worker`` gate) so the xdist controller -- which
    fires ``pytest_runtest_logreport`` for worker reports -- emits every test's
    reports. Stores the emitter on ``config._rucio_forward_emitter`` so
    ``pytest_unconfigure`` can close it. Returns ``True`` when registered.
    """
    emitter = make_emitter_from_env(config)
    if emitter is None:
        return False
    config.pluginmanager.register(_StreamReportPlugin(emitter), "rucio_forward_stream")
    config._rucio_forward_emitter = emitter  # closed at unconfigure
    return True


# ---------------------------------------------------------------------------
# replay (host side)
# ---------------------------------------------------------------------------

def replay_report_line(session: "Session", line: str) -> bool:
    """Reconstruct one serialized report from ``line`` and re-dispatch it.

    Returns ``True`` if a report was dispatched, ``False`` for blank/whitespace
    lines or data the core could not deserialize. ``CollectReport`` payloads are
    routed through ``pytest_collectreport``; everything else (``TestReport``)
    through ``pytest_runtest_logreport`` -- preserving the host's per-test
    accounting so N container tests surface as N host reports (never collapsed
    into a single wrapper).
    """
    line = line.strip()
    if not line:
        return False

    data = json.loads(line)
    config = session.config
    report = config.hook.pytest_report_from_serializable(config=config, data=data)
    if report is None:
        return False

    if data.get("$report_type") == "CollectReport":
        config.hook.pytest_collectreport(report=report)
    else:
        config.hook.pytest_runtest_logreport(report=report)
    return True


# ---------------------------------------------------------------------------
# exit code mirror
# ---------------------------------------------------------------------------

def mirror_exit_code(returncode: int) -> int:
    """Mirror the container pytest returncode onto the host outcome.

    A deliberately trivial identity seam (0 OK, 1 failed, 2 interrupted,
    3 internal error, 4 usage error, 5 no tests collected) so Plan 03 has a
    single tested home for the exit-mirror requirement (FWD-05): it can assign
    the result to ``session.exitstatus`` and/or pass it to ``pytest.exit``.
    """
    return int(returncode)
