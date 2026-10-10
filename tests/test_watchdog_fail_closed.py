"""Regression tests for watchdog compatibility and result-boundary enforcement."""
import ast
import logging
import os
import pathlib
import time
import uuid
import sys
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from source import brd_processor as bp
import job_manager as jm


class MockProc:
    def __init__(self, pid=412):
        self.pid = pid
        self.alive = True
        self.terminated = False
        self.killed = False

    def is_alive(self):
        return self.alive

    def terminate(self):
        self.terminated = True
        self.alive = False

    def kill(self):
        self.killed = True
        self.alive = False

    def join(self, timeout=None):
        pass


def mock_manager(shared, proc, brd):
    manager = jm.JobManager.__new__(jm.JobManager)
    manager.shared_data = {"shared_dict": shared}
    manager.active_processes = [proc]
    manager._pid_to_brd = {proc.pid: brd}
    manager._timed_out_pids = {}
    manager.logger = logging.getLogger("watchdog-regression")
    return manager


def timeout_state(proc, with_id=True):
    now = time.monotonic()
    state = {
        "pid": proc.pid, "paused": False, "gpt_slot_held": False,
        "phase": "live_job",
        "wall_started_monotonic": now - 20,
        "started_monotonic": now - 20,
    }
    if with_id:
        state["invocation_id"] = "old-uuid"
    return state


def test_missing_invocation_id_fails_closed():
    brd = pathlib.Path("BRD_old_protocol.txt")
    proc = MockProc()
    key = f"function_call::{brd.name}"
    shared = {key: timeout_state(proc, with_id=False)}
    manager = mock_manager(shared, proc, brd)
    with patch.object(jm, "FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", 1.0):
        manager._check_function_timeouts()
        recorded = manager._timed_out_pids[proc.pid]
        assert recorded["legacy_protocol"]
        assert "missing invocation ID" in recorded["reason"]
        assert f"cancel::{brd.name}" not in shared
        assert not proc.terminated
        # Even without reliable semaphore status, the fallback is bounded.
        recorded["requested_at"] = time.monotonic() - 1000
        manager._check_function_timeouts()
    assert proc.terminated, "Legacy worker must not run indefinitely"
    assert proc.pid in manager._timed_out_pids, "Never erase a confirmed violation"


def test_advanced_invocation_preserves_confirmed_timeout():
    brd = pathlib.Path("BRD_advance.txt")
    proc = MockProc()
    key = f"function_call::{brd.name}"
    cancel = f"cancel::{brd.name}"
    shared = {key: timeout_state(proc)}
    manager = mock_manager(shared, proc, brd)
    with patch.object(jm, "FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", 1.0):
        manager._check_function_timeouts()
        assert shared[cancel]["invocation_id"] == "old-uuid"
        # The worker completed the violating invocation, then began another.
        shared[key] = {**timeout_state(proc), "invocation_id": "new-uuid",
                       "wall_started_monotonic": time.monotonic()}
        manager._check_function_timeouts()
        assert proc.pid in manager._timed_out_pids
        assert cancel not in shared, "Do not cancel the successor invocation"
        manager._timed_out_pids[proc.pid]["requested_at"] = time.monotonic() - 1000
        manager._check_function_timeouts()
    assert proc.terminated
    assert proc.pid in manager._timed_out_pids


def test_finished_marker_does_not_erase_violation():
    brd = pathlib.Path("BRD_finished.txt")
    proc = MockProc()
    key = f"function_call::{brd.name}"
    shared = {key: timeout_state(proc)}
    manager = mock_manager(shared, proc, brd)
    with patch.object(jm, "FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", 1.0):
        manager._check_function_timeouts()
        shared.pop(key)
        manager._check_function_timeouts()
        assert proc.pid in manager._timed_out_pids
        manager._timed_out_pids[proc.pid]["requested_at"] = time.monotonic() - 1000
        manager._check_function_timeouts()
    assert proc.terminated


def test_final_result_budget_without_shared_marker():
    # Even if a legacy worker never publishes a marker, a returned result that
    # exceeds its total wall budget must not be accepted as successful.
    with patch.object(bp, "_active_function_watchdog", None), \
         patch.object(bp, "FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", 0.01):
        def slow(_):
            time.sleep(0.025)
            return 123

        try:
            bp._invoke_monitored(slow, "x", phase="live_job")
        except bp.FunctionExecutionLimitExceeded:
            pass
        else:
            raise AssertionError("Late result accepted with missing marker")


def worker_marker(shared, brd):
    """Use the actual nested worker marker, not a rewritten imitation."""
    source = pathlib.Path(bp.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    worker = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "process_single_brd_standalone")
    fn = next(n for n in ast.walk(worker) if isinstance(n, ast.FunctionDef)
              and n.name == "_mark_function_call")
    fragment = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(fragment)
    namespace = dict(shared_data={"shared_dict": shared}, brd_path=brd,
                     time=time, os=os, uuid=uuid,
                     FUNCTION_CALL_TIMEOUT_SECONDS=bp.FUNCTION_CALL_TIMEOUT_SECONDS,
                     FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS=bp.FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS)
    exec(compile(fragment, "watchdog-marker", "exec"), namespace)
    return namespace["_mark_function_call"]


def test_worker_checks_compute_budget_at_return():
    brd = pathlib.Path("BRD_worker_budget.txt")
    shared = {}
    marker = worker_marker(shared, brd)
    with patch.object(bp, "_active_function_watchdog", marker), \
         patch.object(bp, "FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", 0):
        # The marker uses the same budget constant from source.
        def near_limit(_):
            # Mutating only timing fields simulates an invocation that is
            # already above the computation limit before it returns.
            state = shared[f"function_call::{brd.name}"]
            state["started_monotonic"] = time.monotonic() - bp.FUNCTION_CALL_TIMEOUT_SECONDS - 5
            shared[f"function_call::{brd.name}"] = state
            return 3

        try:
            bp._invoke_monitored(near_limit, "x", phase="live_job")
        except bp.FunctionExecutionLimitExceeded:
            pass
        else:
            raise AssertionError("Over-compute-budget result accepted")
    assert not shared.get(f"function_call::{brd.name}")


if __name__ == "__main__":
    test_missing_invocation_id_fails_closed()
    test_advanced_invocation_preserves_confirmed_timeout()
    test_finished_marker_does_not_erase_violation()
    test_final_result_budget_without_shared_marker()
    test_worker_checks_compute_budget_at_return()
    print("PASS: missing IDs, late invocation, vanished marker, and final result limits")
