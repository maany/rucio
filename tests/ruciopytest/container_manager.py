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

"""Docker Compose container lifecycle manager for Rucio test suites.

This module runs on the **host side** -- it manages Docker containers from
the pytest process running on the host (or CI runner).  It does NOT run
inside a container; the rucio container has no Docker socket access.

Lifecycle:
    1. Detect and remove orphaned ``rucio-test-*`` compose projects
    2. ``docker compose up -d --wait`` with the correct project name,
       compose files, and profiles
    3. Wait for httpd readiness inside the rucio container
    4. Run tests (handled by pytest, not this module)
    5. ``docker compose down -v`` on session end, signal, or atexit
"""

from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Optional


class ContainerManager:
    """Manages Docker Compose container lifecycle for a test suite.

    Parameters
    ----------
    project_name:
        Compose project name, e.g. ``rucio-test-remote_dbs-postgres14``.
    profiles:
        Tuple of compose profile names to activate (from
        ``SuiteProfile.compose_profiles``).
    root_dir:
        Repository root directory (``config.rootdir``).  Compose file
        paths are resolved relative to this directory to avoid
        CWD-relative path issues (Pitfall 5).
    """

    COMPOSE_DIR = "etc/docker/dev"
    COMPOSE_FILES = (
        "docker-compose.yml",
        "docker-compose.test.override.yml",
    )
    PROJECT_PREFIX = "rucio-test-"
    LOG_DIR = ".test-logs"

    def __init__(
        self,
        project_name: str,
        profiles: tuple[str, ...],
        root_dir: str,
    ) -> None:
        self._project_name = project_name
        self._profiles = profiles
        self._root_dir = root_dir
        self._started = False
        self._cleaned_up = False
        self._original_sigterm: Any = None
        self._original_sigint: Any = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def project_name(self) -> str:
        """The compose project name."""
        return self._project_name

    @property
    def log_dir(self) -> Path:
        """Path to the container log output directory."""
        return Path(self._root_dir) / self.LOG_DIR

    def start(self) -> None:
        """Start containers: clean orphans, compose up, readiness check."""
        self._cleanup_orphans()
        self._compose_up()
        self._wait_for_readiness()
        self._started = True
        self._register_cleanup_handlers()

    def stop(self, capture_logs: bool = True) -> None:
        """Stop and remove containers.

        Idempotent -- subsequent calls are no-ops (Pitfall 3).
        """
        if self._cleaned_up:
            return
        self._cleaned_up = True

        if capture_logs and self._started:
            self._capture_logs()

        self._compose_down()
        self._restore_signal_handlers()

    @staticmethod
    def make_project_name(suite_name: str, rdbms: str) -> str:
        """Build a compose project name from suite and RDBMS.

        Returns a string like ``rucio-test-remote_dbs-postgres14``
        (SUIT-05).
        """
        return f"rucio-test-{suite_name}-{rdbms}"

    # ------------------------------------------------------------------
    # Compose command builder
    # ------------------------------------------------------------------

    def _compose_cmd(self, *args: str) -> list[str]:
        """Build a full ``docker compose`` command with project/file/profile flags."""
        cmd = ["docker", "compose", "-p", self._project_name]
        for fname in self.COMPOSE_FILES:
            fpath = os.path.join(self._root_dir, self.COMPOSE_DIR, fname)
            cmd.extend(["-f", fpath])
        for profile in self._profiles:
            cmd.extend(["--profile", profile])
        cmd.extend(args)
        return cmd

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    def _cleanup_orphans(self) -> None:
        """Detect and remove orphaned ``rucio-test-*`` compose projects."""
        try:
            result = subprocess.run(
                ["docker", "compose", "ls", "--format", "json", "-a"],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            print(f"[container_manager] Warning: could not list compose projects: {exc}")
            return

        if result.returncode != 0:
            print("[container_manager] Warning: could not list compose projects")
            return

        try:
            projects = json.loads(result.stdout)
        except json.JSONDecodeError:
            print("[container_manager] Warning: could not parse compose project list")
            return

        for project in projects:
            name = project.get("Name", "")
            if name.startswith(self.PROJECT_PREFIX) and name != self._project_name:
                print(f"[container_manager] Removing orphaned project: {name}")
                try:
                    subprocess.run(
                        ["docker", "compose", "-p", name, "down", "-v", "-t", "10"],
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    print(f"[container_manager] Warning: failed to remove orphan {name}")

    def _compose_up(self) -> None:
        """Run ``docker compose up -d --wait`` and raise on failure."""
        cmd = self._compose_cmd("up", "-d", "--wait", "--wait-timeout", "120")
        print(f"[container_manager] Starting containers: {self._project_name}")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Timed out waiting for containers to start: {self._project_name}"
            )

        if result.returncode != 0:
            print(f"[container_manager] compose up failed:\n{result.stderr}")
            raise RuntimeError(f"Failed to start containers: {result.stderr}")

        print("[container_manager] Containers started and healthy")

    def _wait_for_readiness(self) -> None:
        """Wait for httpd readiness inside the rucio container.

        Database readiness is handled by ``docker compose up --wait``
        which respects the healthcheck blocks in docker-compose.yml.
        This method adds an extra check for httpd (Apache) inside the
        rucio service container.
        """
        cmd = self._compose_cmd(
            "exec", "-T", "rucio",
            "curl",
            "--retry", "15",
            "--retry-all-errors",
            "--retry-delay", "2",
            "-k", "-s", "-o", "/dev/null",
            "-w", "%{http_code}",
            "https://localhost/ping",
        )
        print("[container_manager] Waiting for httpd readiness...")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("httpd readiness check timed out")

        if result.returncode != 0:
            print(f"[container_manager] httpd readiness check failed:\n{result.stderr}")
            raise RuntimeError(f"httpd readiness check failed: {result.stderr}")

        print("[container_manager] httpd is ready")

    def _compose_down(self) -> None:
        """Run ``docker compose down -v`` (best-effort, does not raise)."""
        cmd = self._compose_cmd("down", "-v", "-t", "30")
        print(f"[container_manager] Stopping containers: {self._project_name}")
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
                start_new_session=True,
            )
            if result.returncode != 0:
                print(
                    f"[container_manager] Warning: compose down returned "
                    f"exit code {result.returncode}: {result.stderr}"
                )
            else:
                print("[container_manager] Containers stopped and removed")
        except subprocess.TimeoutExpired:
            print("[container_manager] Warning: compose down timed out")
        except FileNotFoundError:
            print("[container_manager] Warning: docker not found during cleanup")

    def _capture_logs(self) -> None:
        """Capture container logs to .test-logs/ directory.

        Saves a combined log file (all services) and individual per-service
        log files.  All errors are handled gracefully -- log capture must
        never prevent cleanup.
        """
        log_dir = self.log_dir
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            print(f"[container_manager] Warning: could not create log directory: {exc}")
            return

        # Combined log from all services
        try:
            result = subprocess.run(
                self._compose_cmd("logs", "--no-color", "--timestamps"),
                capture_output=True,
                text=True,
                timeout=60,
            )
            combined_log = log_dir / f"{self._project_name}.log"
            combined_log.write_text(result.stdout)
            print(f"[container_manager] Combined logs saved to {combined_log}")
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
            print(f"[container_manager] Warning: failed to capture combined logs: {exc}")

        # Per-service logs
        try:
            svc_result = subprocess.run(
                self._compose_cmd("config", "--services"),
                capture_output=True,
                text=True,
                timeout=10,
            )
            services = [s.strip() for s in svc_result.stdout.splitlines() if s.strip()]
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
            print(f"[container_manager] Warning: could not list services: {exc}")
            return

        for service in services:
            try:
                result = subprocess.run(
                    self._compose_cmd("logs", "--no-color", "--timestamps", service),
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.stdout:
                    service_log = log_dir / f"{service}.log"
                    service_log.write_text(result.stdout)
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
                print(f"[container_manager] Warning: failed to capture logs for {service}: {exc}")

        print(f"[container_manager] Per-service logs saved to {log_dir}")

    # ------------------------------------------------------------------
    # Cleanup handlers
    # ------------------------------------------------------------------

    def _register_cleanup_handlers(self) -> None:
        """Register atexit and signal handlers for belt-and-suspenders cleanup."""
        atexit.register(self.stop, capture_logs=False)

        self._original_sigterm = signal.getsignal(signal.SIGTERM)
        self._original_sigint = signal.getsignal(signal.SIGINT)

        def _signal_handler(signum: int, frame: Any) -> None:
            self.stop(capture_logs=True)

            # Restore original handler and re-raise so the process exits
            # with the correct signal status.
            original = (
                self._original_sigterm
                if signum == signal.SIGTERM
                else self._original_sigint
            )
            if callable(original):
                original(signum, frame)
            elif original == signal.SIG_DFL:
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers saved during registration."""
        if self._original_sigterm is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm)
            self._original_sigterm = None
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)
            self._original_sigint = None
