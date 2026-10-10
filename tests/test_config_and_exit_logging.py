"""Keep watchdog settings shared and distinguish expected watchdog exits from crashes."""
import logging
import pathlib
import sys
import tempfile
import types
from unittest.mock import Mock, patch

try:
    from openai import APIConnectionError
except ImportError:
    sdk = types.ModuleType('openai')
    for name in ('APIConnectionError', 'APIStatusError', 'APITimeoutError', 'RateLimitError'):
        setattr(sdk, name, type(name, (Exception,), {}))
    sdk.AzureOpenAI = type('AzureOpenAI', (), {})
    sys.modules['openai'] = sdk

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from source import brd_processor as bp
import job_manager as jm


def test_shared_defaults_and_generation_budget():
    assert jm.FUNCTION_CALL_TIMEOUT_SECONDS == bp.FUNCTION_CALL_TIMEOUT_SECONDS
    assert jm.FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS == bp.FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS
    assert bp.GPT_GENERATION_REQUEST_TIMEOUT_SECONDS == 300
    assert bp.GPT_GENERATION_TOOL_TIMEOUT_SECONDS == 600
    with patch.object(bp, 'gpt_call_with_retry', return_value=types.SimpleNamespace(
        choices=[types.SimpleNamespace(message=types.SimpleNamespace(content='def demo(x): return x'))]
    )) as gpt:
        bp.ask_gpt_with_naming_convention_to_make_func(None, 'demo', 'demo', ['x'])
        assert gpt.call_args.kwargs['request_timeout_seconds'] == 300
        assert gpt.call_args.kwargs['tool_timeout_seconds'] == 600


class DummyProc:
    def __init__(self, exitcode):
        self.pid = 1304
        self.exitcode = exitcode

    def is_alive(self):
        return False

    def join(self, timeout=None):
        pass

    def close(self):
        pass


def exercise_cleanup(exitcode, watchdog_failure=None):
    with tempfile.TemporaryDirectory() as td:
        brd = pathlib.Path(td) / 'BRD_dummy.txt'
        brd.write_text('BRD for exit logging', encoding='utf8')
        brd_hash = bp.brd_hash_signature(brd.read_text())
        proc = DummyProc(exitcode)
        manager = jm.JobManager.__new__(jm.JobManager)
        manager.active_processes = [proc]
        manager._pid_to_brd = {proc.pid: brd}
        manager._pid_to_brd_hash = {proc.pid: brd_hash}
        manager._pid_to_log = {proc.pid: None}
        manager._inflight_brds = {brd}
        manager._timed_out_pids = {proc.pid: watchdog_failure} if watchdog_failure else {}
        manager.shared_data = {'shared_dict': {}, 'lock': None}
        manager._consider_worker_yield = Mock()
        manager._stop_worker_log = Mock()
        manager.logger = Mock()
        with patch.object(bp, 'get_approved_handcrafted_recovery', return_value=None), \
             patch.object(bp, 'block_brd_until_changed') as block:
            manager._cleanup_finished()
            assert block.call_count == 1
            logged = [c.args[0] % c.args[1:] for c in manager.logger.error.call_args_list]
            assert len(logged) == 1, logged
            assert 'BRD hash' in logged[0] and 'blocked' in logged[0]
            return logged[0], manager, proc, block.call_args.kwargs


def test_watchdog_normal_exit_log():
    msg, manager, proc, kw = exercise_cleanup(0, {'reason': 'total_wall budget exceeded'})
    assert 'watchdog budget violation' in msg
    assert 'exited unexpectedly' not in msg
    assert '(exit code=0)' in msg
    assert manager._consider_worker_yield.call_count == 0, 'Timed-out worker must not yield as healthy'
    assert kw['origin'] == 'unexpected_worker_exit'  # Existing block policy unchanged.


def test_abnormal_exit_log():
    msg, manager, proc, kw = exercise_cleanup(2)
    assert 'exited unexpectedly' in msg
    assert '(exit code=2)' in msg
    assert 'watchdog budget violation' not in msg
    assert kw['origin'] == 'unexpected_worker_exit'


if __name__ == '__main__':
    test_shared_defaults_and_generation_budget()
    test_watchdog_normal_exit_log()
    test_abnormal_exit_log()
    print('PASS: unified watchdog defaults, 600s generation budget, watchdog-vs-crash logs')
