import importlib.util
import logging
import pathlib
import tempfile
from collections import Counter
from types import SimpleNamespace
from unittest.mock import patch
import sys, types
# The sandbox has no OpenAI SDK; scheduler tests never call the API.
try:
    import openai
    from openai import APIConnectionError
except ImportError:
    sdk = types.ModuleType('openai')
    for name in ('APIConnectionError','APIStatusError','APITimeoutError','RateLimitError'):
        setattr(sdk, name, type(name,(Exception,),{}))
    sdk.AzureOpenAI = type('AzureOpenAI', (), {})
    sys.modules['openai'] = sdk

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
SPEC = importlib.util.spec_from_file_location('job_manager_fair_test', pathlib.Path(__file__).resolve().parents[1] / 'job_manager.py')
jm = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(jm)


def run_scheduler_simulation(n_brds=20, slots=2, max_dispatches=45):
    """Run the actual manager's scan loop with fake short, always-busy processes."""
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        docs = root / 'documents'; docs.mkdir()
        saved = root / 'saved_functions'; saved.mkdir()
        registry = saved / 'registry.json'
        for i in range(n_brds):
            name = chr(ord('A') + i)
            (docs / f'BRD_{name}.txt').write_text(f'{name} BRD', encoding='utf-8')

        clock = SimpleNamespace(now=2000.0, loops=0, issued=[])
        manager = jm.JobManager.__new__(jm.JobManager)
        manager.logger = logging.getLogger('scheduling')
        manager.max_concurrent_BRD_agents = slots
        manager.active_processes = []
        manager._inflight_brds = set()
        manager._pid_to_brd = {}
        manager._pid_to_brd_hash = {}
        manager._pid_to_log = {}
        manager._timed_out_pids = {}
        manager._blocked_log_ts = {}
        manager._snoozed_fingerprints = {}
        manager._retry_after = {}
        manager._session_blocks = set()
        manager._dispatch_sequence = 0
        manager._last_dispatched_sequence = {}
        manager.shared_data = {'shared_dict': {}, 'lock': None}
        manager.gpt_semaphore = object()
        manager.log_q = object()
        manager._shutdown_requested = False
        manager._start_worker_log = lambda: (None, None)
        manager._stop_worker_log = lambda log: None
        manager._check_function_timeouts = lambda: None
        manager._deep_cleanup = lambda: None
        manager.clean_saved_functions_and_registry = lambda: None
        manager.shutdown = lambda: None

        class FakeProcess:
            counter = 91000
            def __init__(self, *, target, args, name):
                self.args=args; self.name=name
                FakeProcess.counter += 1
                self.pid=FakeProcess.counter
                self.started_at=None
                self.exitcode=None
                self.ended=False
            def start(self):
                self.started_at=clock.now
                clock.issued.append(self.name.replace('BRD-BRD_', ''))
            def is_alive(self):
                if self.started_at is None:
                    return False
                if clock.now >= self.started_at + 2.0:
                    self.exitcode=0
                    if not self.ended:
                        self.ended=True
                        brd=self.args[0]
                        manager.shared_data['shared_dict'][f'worker_yield::{brd.name}']={
                            'pid': self.pid, 'reason': 'quantum'}
                    return False
                return True
            def join(self, timeout=None):
                self.is_alive()
            def close(self): pass
            def terminate(self): self.exitcode=-15
            def kill(self): self.exitcode=-9

        def fake_sleep(duration):
            clock.now += max(0.05, duration)
            clock.loops += 1
            if len(clock.issued) >= max_dispatches or clock.loops > 250:
                manager._shutdown_requested = True

        with (patch.object(jm,'PROJECT_ROOT',root),
              patch.object(jm,'SAVED_FUNC_DIR',saved),
              patch.object(jm,'REGISTRY',registry),
              patch.object(jm.brd_processor,'get_brd_runtime_block',return_value=None),
              patch.object(jm.brd_processor,'get_approved_handcrafted_recovery',return_value=None),
              patch.object(jm.multiprocessing,'Process',FakeProcess),
              patch.object(jm.time,'monotonic',side_effect=lambda: clock.now),
              patch.object(jm.time,'time',side_effect=lambda: clock.now),
              patch.object(jm.time,'sleep',side_effect=fake_sleep)):
            manager.run_continuous(check_interval=1)

        # The manager should dispatch every never-served BRD before returning to A.
        issued=clock.issued
        assert len(issued)>=n_brds, issued
        initial=issued[:n_brds]
        expected=[chr(ord('A')+i) for i in range(n_brds)]
        assert sorted(initial)==expected, f'Initial {n_brds} dispatches were {initial!r}'
        assert len(set(initial))==n_brds, f'BRD repeated before all had run: {initial}'
        counts=Counter(issued)
        assert max(counts.values())-min(counts.values())<=1, counts
        assert len(issued)>=max_dispatches, (issued,clock.loops)
        return issued,manager


def test_ordering():
    manager=jm.JobManager.__new__(jm.JobManager)
    manager._last_dispatched_sequence={'BRD_A.txt':7,'BRD_B.txt':3,'BRD_C.txt':4}
    files=[pathlib.Path('BRD_'+x+'.txt') for x in 'ABCDE']
    order=[x.name for x in manager._ordered_brds_for_dispatch(files)]
    assert order==['BRD_D.txt','BRD_E.txt','BRD_B.txt','BRD_C.txt','BRD_A.txt'],order


def test_retry_preserved_with_no_free_capacity():
    with tempfile.TemporaryDirectory() as td:
        root=pathlib.Path(td); docs=root/'documents';docs.mkdir();saved=root/'saved_functions';saved.mkdir()
        a=docs/'BRD_A.txt';a.write_text('test',encoding='utf-8')
        clock=SimpleNamespace(now=1000.0, iterations=0)
        manager=jm.JobManager.__new__(jm.JobManager)
        manager.logger=logging.getLogger('retry-preservation')
        manager.max_concurrent_BRD_agents=1
        class BusyProcess:
            pid = 99999
            def is_alive(self): return True
        manager.active_processes=[BusyProcess()] # permanently occupied
        manager._inflight_brds=set()
        manager._pid_to_brd={}
        manager._pid_to_brd_hash={}
        manager._pid_to_log={}
        manager._timed_out_pids={}
        manager._blocked_log_ts={}
        manager._session_blocks=set()
        manager._snoozed_fingerprints={a.name:jm.JobManager._work_fingerprint(a)}
        manager._retry_after={a.name:clock.now-10} # expired, but work still pending
        manager._last_dispatched_sequence={a.name:1}
        manager._dispatch_sequence=1
        manager._shutdown_requested=False
        manager.shared_data={'shared_dict':{},'lock':None}
        manager.clean_saved_functions_and_registry=lambda: None
        manager._cleanup_finished=lambda: None
        manager._check_function_timeouts=lambda: None
        manager.shutdown=lambda: None
        manager._deep_cleanup=lambda: None
        def sleep(_): manager._shutdown_requested=True
        with (patch.object(jm,'PROJECT_ROOT',root),patch.object(jm,'SAVED_FUNC_DIR',saved),
              patch.object(jm,'REGISTRY',saved/'registry.json'),
              patch.object(jm.brd_processor,'get_brd_runtime_block',return_value=None),
              patch.object(jm.time,'time',return_value=clock.now),
              patch.object(jm.time,'monotonic',return_value=clock.now),
              patch.object(jm.time,'sleep',side_effect=sleep)):
            manager.run_continuous(check_interval=1)
        assert a.name in manager._retry_after, 'Pending retry was discarded without getting a worker slot'


if __name__=='__main__':
    test_ordering()
    test_retry_preserved_with_no_free_capacity()
    issued,mgr=run_scheduler_simulation()
    print('PASS: 20 busy BRDs share 2 slots fairly; first 20 unique:',issued[:20])
    print('PASS: re-dispatched counts:',dict(Counter(issued)))
    print('PASS: pending expired retry preserved while slots are occupied')