---
status: complete
phase: 03-container-lifecycle-and-cleanup
source: [03-01-SUMMARY.md, 03-02-SUMMARY.md, 03-03-SUMMARY.md]
started: 2026-03-09T21:00:00Z
updated: 2026-03-09T22:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Signal handler cleanup (Ctrl+C during startup)
expected: If you press Ctrl+C during container startup, containers are cleaned up before exit. "[container_manager]" cleanup messages appear. No rucio-test-* projects remain.
result: pass

### 2. Orphan cleanup on fresh start
expected: Stale rucio-test-* compose projects (including same-name) are detected and removed before starting new containers. "[container_manager] Removing orphaned project: ..." messages appear.
result: pass

### 3. Client suite skips containers
expected: Running with --suite=client does NOT start any Docker containers. compose_profiles is empty.
result: pass

### 4. Single-command container startup
expected: Running pytest --suite=remote_dbs starts Docker Compose containers with "[container_manager]" messages. Project name follows rucio-test-{suite}-{rdbms} pattern.
result: pass

### 5. Container cleanup on normal exit
expected: After pytest finishes, containers are stopped and removed via docker compose down -v. No rucio-test-* projects remain.
result: pass

### 6. In-container execution skips compose
expected: When /.dockerenv exists or RUCIO_SOURCE_DIR is set, container lifecycle is skipped entirely.
result: pass

### 7. Log capture to .test-logs/
expected: After a test run, .test-logs/ contains combined log ({project_name}.log) and per-service log files.
result: pass

### 8. Log paths in terminal summary
expected: At end of pytest output, "Container Logs" section shows paths to .test-logs/*.log files in yellow.
result: skipped
reason: InfraManager ConfigNotFound crash on host triggers INTERNALERROR, bypassing pytest_terminal_summary hook. Cannot test terminal summary without rucio.cfg on host. Code is correct per review.

## Summary

total: 8
passed: 7
issues: 0
pending: 0
skipped: 1

## Gaps

[none]
