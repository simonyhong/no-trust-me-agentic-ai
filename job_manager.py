import json
import logging
import logging.handlers
import multiprocessing
import sys
import threading
import signal
import time, os
from typing import Dict
import queue
import pathlib
import shutil
import uuid
import gc, psutil
from datetime import datetime, timezone, timedelta  # NEW
from source import brd_processor

QUARANTINE_TTL_DAYS = 30  # set to 0/None to disable age-based pruning
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
SAVED_FUNC_DIR = PROJECT_ROOT / "saved_functions"
REGISTRY = SAVED_FUNC_DIR / "registry.json"
MAX_RECORD_BYTES = 64_000     # 64 KB cap per log record
RESET_DONE_STATE = os.getenv("RESET_DONE_STATE", "0").strip().lower() in {"1", "true", "yes", "on"}
# Cumulative non-GPT computation limit per function call. Set to 0 to disable.
# This watchdog limits accidental hangs; it is not a security sandbox.
FUNCTION_CALL_TIMEOUT_SECONDS = float(os.getenv("FUNCTION_CALL_TIMEOUT_SECONDS", "120"))
# Total time per function invocation, INCLUDING GPT waits. Set to 0 to disable.
FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS = float(os.getenv("FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", "300"))
# Give a cancelled invocation an opportunity to finish without leaking a GPT slot.
FUNCTION_CANCELLATION_GRACE_SECONDS = max(0.0, float(os.getenv("FUNCTION_CANCELLATION_GRACE_SECONDS", "3")))

##################################### Job Manager ###################################################
SEMAPHORE_LIMIT = 5 # Only 5 concurrent GPT calls allowed 
show_time=True
class JobManager:
    def __init__(self, max_concurrent_BRD_agents, to_activate_console):
        # ── core state ──────────────────────────────────────────────────────────────
        self.max_concurrent_BRD_agents = max_concurrent_BRD_agents
        self._inflight_brds: set[pathlib.Path] = set()
        self._pid_to_brd: dict[int, pathlib.Path] = {}
        self._pid_to_brd_hash: dict[int, str] = {}
        self._pid_to_log: dict[int, tuple] = {}
        self._timed_out_pids: dict[int, dict] = {}
        self._blocked_log_ts: dict[str, float] = {}
        # Idle/waiting workers release their process slots. Re-launch only if
        # relevant input files change, or after a bounded service-retry delay.
        self._snoozed_fingerprints: dict[str, tuple] = {}
        self._retry_after: dict[str, float] = {}
        self._session_blocks: set[str] = set()  # GPT configuration issues until manager restart
        # Oldest-served-first dispatch. Newly discovered BRDs have no sequence
        # and are considered before BRDs that have already occupied a slot.
        self._dispatch_sequence = 0
        self._last_dispatched_sequence: dict[str, int] = {}
        self.active_processes = []
        self.gpt_semaphore = multiprocessing.BoundedSemaphore(SEMAPHORE_LIMIT)

        # Shared manager objects
        self._manager = multiprocessing.Manager()
        self.shared_data = {
            "shared_dict": self._manager.dict(),
            "lock": self._manager.Lock(),
        }

        # Heartbeat
        self._hb_interval = 3
        self._hb_stop = threading.Event()
        self._hb_id = str(uuid.uuid4())
        try:
            shared = self.shared_data["shared_dict"]
            shared["hb"] = {"id": self._hb_id, "pid": os.getpid(), "ts": time.time()}
        except Exception:
            pass
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, name="manager-hb", daemon=True)
        self._hb_thread.start()

        # ── logging (manager is the ONLY formatter) ────────────────────────────────
        self._console_enabled = bool(to_activate_console)
        self._show_time = bool(show_time)

        # Each worker has its own log queue. Manager log records bypass worker
        # queues so a crashed worker cannot silence ESCALATE messages.

        # Fresh file each run
        log_path = str(PROJECT_ROOT / "all_process.log")
        try:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            with open(log_path, "w", encoding="utf-8"):
                pass  # truncate
        except Exception:
            pass

        # Build one formatter (toggle timestamp via show_time)
        base_fmt = "%(levelname)s %(name)s %(message)s"
        # base_fmt = "%(levelname)s %(name)s [pid=%(process)d] %(message)s"
        fmt = f"%(asctime)s {base_fmt}" if self._show_time else base_fmt
        datefmt = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt=datefmt)

        # Sinks for QueueListener
        file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        sinks = [file_handler]

        if self._console_enabled:
            console = logging.StreamHandler(sys.stdout)
            console.setLevel(logging.INFO)
            console.setFormatter(formatter)
            sinks.append(console)

        # Worker listeners share synchronized logging handlers, but each gets
        # a distinct queue. The manager writes directly to those handlers.
        self._log_sinks = sinks
        self.logger = logging.getLogger("JobManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
        for handler in sinks:
            self.logger.addHandler(handler)
        # ───────────────────────────────────────────────────────────────

        # bookkeeping
        self._shutdown_requested = False


    def _prune_quarantine_entries(self, meta: dict, sig: str) -> tuple[int, int]:
        """
        Prune quarantine entries for a single registry meta:
          - drop entries whose script file no longer exists on disk
          - drop entries older than QUARANTINE_TTL_DAYS (if enabled)
        Returns (pruned_count, kept_count).
        """
        q = meta.get("quarantine")
        if not isinstance(q, list) or not q:
            return (0, 0)

        folder = meta.get("folder_name") or sig
        ttl = QUARANTINE_TTL_DAYS if QUARANTINE_TTL_DAYS else 0
        cutoff = None
        if ttl and ttl > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(ttl))

        def _exists_for(item: dict) -> bool:
            script = item.get("script")
            if not script or not isinstance(script, str):
                return False
            origin = (item.get("origin") or "").lower()
            if origin == "handcrafted":
                base = SAVED_FUNC_DIR / f"{folder}_handcrafted"
            else:
                base = SAVED_FUNC_DIR / folder
            return (base / script).exists()

        def _is_old(item: dict) -> bool:
            if not cutoff:
                return False
            ts = item.get("ts")
            if not isinstance(ts, str):
                return False
            # registry stores ISO8601; handle trailing "Z"
            try:
                iso = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                return dt < cutoff
            except Exception:
                # if timestamp is malformed, be conservative and keep it
                return False

        kept = []
        for item in q:
            if not isinstance(item, dict):
                continue
            if not _exists_for(item):
                continue
            if _is_old(item):
                continue
            kept.append(item)

        pruned = len(q) - len(kept)
        if pruned:
            meta["quarantine"] = kept
        return (pruned, len(kept))
    
    def clean_saved_functions_and_registry(self):
        registry_lock = self.shared_data.get("lock")
        lock_acquired = False
        try:
            if registry_lock is not None:
                registry_lock.acquire()
                lock_acquired = True
            if not REGISTRY.exists():
                return

            registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
            docs_dir = PROJECT_ROOT / "documents"
            stale_sigs = []
            updated = False

            # Current BRD content hash -> BRD filename
            current_sigs: dict[str, str] = {}
            for brd_path in docs_dir.glob("BRD_*.txt"):
                try:
                    sig = brd_processor.brd_hash_signature(brd_path.read_text(encoding="utf-8"))
                    current_sigs[sig] = brd_path.name
                except FileNotFoundError:
                    continue

            def _folder_for(meta: dict) -> str | None:
                folder = meta.get("folder_name")
                if folder:
                    return folder
                title = meta.get("title")
                return pathlib.Path(title).stem if isinstance(title, str) and title else None

            # Prune obsolete registry entries without deleting a folder still used by a newer hash.
            for sig, meta in list(registry.items()):
                if not isinstance(meta, dict):
                    registry.pop(sig, None)
                    stale_sigs.append(sig)
                    updated = True
                    continue

                brd_name = meta.get("title")

                # add_quarantine() can create an entry before any generated implementation is saved.
                # If this hash is still the live BRD, backfill enough metadata for housekeeping.
                if not brd_name and sig in current_sigs:
                    brd_name = current_sigs[sig]
                    meta["title"] = brd_name
                    meta["folder_name"] = pathlib.Path(brd_name).stem
                    updated = True

                missing = (not brd_name) or not (docs_dir / brd_name).exists()
                sig_mis = current_sigs.get(sig) != brd_name

                if missing or sig_mis:
                    reason = f"missing {brd_name} file" if missing else "signature mismatch"
                    self.logger.info("Deleting obsolete registry entry for %s (%s)", brd_name, reason)
                    folder = _folder_for(meta)
                    registry.pop(sig, None)

                    try:
                        if folder:
                            shared = any(_folder_for(other) == folder for other in registry.values() if isinstance(other, dict))
                            if shared:
                                # Every hash of one BRD shares the same generated folder. Delete only
                                # files belonging to the stale hash; never touch handcrafted code here.
                                stale_scripts = {meta.get("latest_script")}
                                stale_scripts.update(
                                    item.get("script")
                                    for item in meta.get("quarantine", [])
                                    if isinstance(item, dict) and (item.get("origin") or "").lower() != "handcrafted"
                                )
                                for script in filter(None, stale_scripts):
                                    (SAVED_FUNC_DIR / folder / script).unlink(missing_ok=True)
                            else:
                                shutil.rmtree(SAVED_FUNC_DIR / folder, ignore_errors=True)
                    except Exception as exc:
                        self.logger.warning("Could not prune generated files for %s: %s", brd_name, exc)

                    stale_sigs.append(sig)
                    updated = True
                else:
                    pruned, kept = self._prune_quarantine_entries(meta, sig)
                    if pruned:
                        self.logger.info(
                            "Pruned %d quarantine entr%s for %s (hash=%s); %d remain",
                            pruned, "y" if pruned == 1 else "ies", brd_name, sig, kept
                        )
                        updated = True

            if updated:
                brd_processor.save_registry(REGISTRY, registry)
                self.logger.info(
                    "Registry cleanup complete — removed %d obsolete entries; quarantine lists updated.",
                    len(stale_sigs),
                )

        except Exception as exc:
            self.logger.warning("Registry cleanup error: %s", exc)
        finally:
            if lock_acquired:
                try:
                    registry_lock.release()
                except Exception:
                    pass

        # Handcrafted folders contain user-written source. Never delete them automatically.

    ########################   RUN LOOP   ########################################
    @staticmethod
    def _work_fingerprint(brd: pathlib.Path) -> tuple:
        """Lightweight change detector; no expensive whole-jobs JSON scan per poll."""
        jobs = brd.with_name(f"{brd.stem}_jobs.json")
        done = brd.with_name(f"done_{jobs.stem}.json")
        def stamp(path):
            try:
                stat = path.stat()
                return (stat.st_mtime_ns, stat.st_size)
            except OSError:
                return None
        return (stamp(brd), stamp(jobs), stamp(done))

    def _ordered_brds_for_dispatch(self, brds: list[pathlib.Path]) -> list[pathlib.Path]:
        """Choose longest-waiting BRDs first, not alphabetical BRDs first.

        A successful process start records a monotonically increasing sequence.
        BRDs never dispatched get priority (-1), then older dispatches. Name
        breaks ties deterministically, but cannot repeatedly outrank a BRD
        that has waited longer since its last dispatch.
        """
        served = self._last_dispatched_sequence
        return sorted(brds, key=lambda brd: (served.get(brd.name, -1), brd.name))

    def _consider_worker_yield(self, brd: pathlib.Path, proc_pid: int) -> None:
        """Capture clean worker yield before freeing its concurrency slot."""
        reason = None
        try:
            shared = self.shared_data.get("shared_dict")
            value = shared.get(f"worker_yield::{brd.name}") if shared is not None else None
            if isinstance(value, dict) and value.get("pid") == proc_pid:
                reason = value.get("reason")
                shared.pop(f"worker_yield::{brd.name}", None)
        except (OSError, EOFError, BrokenPipeError):
            pass
        self._snoozed_fingerprints[brd.name] = self._work_fingerprint(brd)
        if reason == "blocked_configuration":
            self._session_blocks.add(brd.name)
        elif reason == "service_retry":
            self._retry_after[brd.name] = time.monotonic() + 45.0
        elif reason == "quantum":
            # Yield behind queued BRDs rather than immediately monopolizing a slot.
            self._retry_after[brd.name] = time.monotonic() + 5.0
        elif reason is None:
            # Unexpected clean exit without a yield marker: prevent rapid respawn.
            self._retry_after[brd.name] = time.monotonic() + 15.0
        else:
            self._retry_after.pop(brd.name, None)
        if reason:
            self.logger.info("BRD %s released worker slot (%s)", brd.name, reason)

    def run_continuous(self, check_interval: int = 1, only_list=None, exclude_list=None):
        self.logger.info("Starting with max %d concurrent jobs. Checking every %ds", self.max_concurrent_BRD_agents, check_interval)

        last_log_time = 0
        last_function_clean_time = 0
        iteration = 0
        cleaned = False
        try:
            while not self._shutdown_requested:
                try:
                    iteration += 1
                    current_time = time.time()

                    # housekeeping
                    self._check_function_timeouts()
                    self._cleanup_finished()

                    if current_time - last_function_clean_time >= 60:
                        last_function_clean_time = current_time
                        self.clean_saved_functions_and_registry()

                    # scan BRDs
                    docs_dir = PROJECT_ROOT / "documents"
                    if not docs_dir.exists():
                        self.logger.warning("Documents directory not found: %s", docs_dir)
                        docs_dir.mkdir(parents=True, exist_ok=True)

                    brd_files = list(docs_dir.glob("BRD_*.txt"))
                    if exclude_list:
                        brd_files = [b for b in brd_files if b.name not in exclude_list]
                    if only_list:
                        brd_files = [b for b in brd_files if b.name in only_list]

                    # Reorder every scan based on actual successful dispatches.
                    # The oldest-served eligible BRD always gets first choice.
                    for brd in self._ordered_brds_for_dispatch(brd_files):
                        if brd in self._inflight_brds:
                            continue
                        if brd.name in self._session_blocks:
                            continue  # operator fixes service config and restarts manager

                        # A block is tied to the exact BRD content hash, and persists
                        # across JobManager restarts. A human BRD edit yields a new hash.
                        try:
                            launch_hash = brd_processor.brd_hash_signature(
                                brd.read_text(encoding="utf-8")
                            )
                            block = brd_processor.get_brd_runtime_block(REGISTRY, launch_hash)
                        except (OSError, UnicodeError) as exc:
                            self.logger.warning("Cannot inspect BRD %s: %s", brd.name, exc)
                            continue
                        approval = brd_processor.get_approved_handcrafted_recovery(
                            REGISTRY, launch_hash, SAVED_FUNC_DIR, brd
                        ) if block else None
                        if block and not approval:
                            key = f"{brd.name}:{launch_hash}"
                            if current_time - self._blocked_log_ts.get(key, 0) >= 30:
                                self.logger.error(
                                    "ESCALATE TO HUMAN: not launching blocked %s (BRD hash %s): %s",
                                    brd.name, launch_hash, block.get("reason", "human review required"),
                                )
                                self._blocked_log_ts[key] = current_time
                            continue
                        if not approval:
                            snap = self._work_fingerprint(brd)
                            unchanged = self._snoozed_fingerprints.get(brd.name) == snap
                            retry_time = self._retry_after.get(brd.name)
                            if retry_time is not None and time.monotonic() < retry_time:
                                continue
                            if unchanged and retry_time is None:
                                continue
                            # Crucial: do NOT consume an expired retry marker until
                            # this BRD actually starts a worker. If all slots are
                            # occupied, dropping it makes unchanged pending work
                            # look idle forever (including unfinished batches).
                        if not self._can_start_new_job():
                            break

                        abs_brd = brd.resolve()
                        self.logger.info("Starting new process for %s", abs_brd.name)

                        # Clear only stale stop/yield flags for the new worker.
                        try:
                            shared = self.shared_data.get("shared_dict")
                            if shared is not None:
                                shared[f"stop::{abs_brd.name}"] = False
                                shared.pop(f"worker_yield::{abs_brd.name}", None)
                                # Clear a cancellation left by a previous worker.
                                shared.pop(f"cancel::{abs_brd.name}", None)
                        except Exception:
                            pass

                        # preflight readability
                        try:
                            _ = abs_brd.read_bytes()
                        except FileNotFoundError:
                            continue
                        except Exception as exc:
                            self.logger.warning("Skipping %s — cannot read (%s).", abs_brd.name, exc)
                            continue

                        worker_log = self._start_worker_log()
                        proc = multiprocessing.Process(
                            target=brd_processor.process_single_brd_standalone,
                            args=(abs_brd, SAVED_FUNC_DIR, REGISTRY, self.gpt_semaphore,
                                worker_log[0], self.shared_data, True if only_list else False,
                                max(1, int(check_interval))),
                            name=f"BRD-{abs_brd.stem}"
                        )

                        try:
                            proc.start()
                            self.active_processes.append(proc)
                            self._inflight_brds.add(brd)
                            self._pid_to_brd[proc.pid] = brd
                            self._pid_to_brd_hash[proc.pid] = launch_hash
                            self._pid_to_log[proc.pid] = worker_log
                            # Mark service only after process startup succeeds.
                            self._dispatch_sequence += 1
                            self._last_dispatched_sequence[brd.name] = self._dispatch_sequence
                            self._retry_after.pop(brd.name, None)
                            self.logger.info("Started process %d for %s", proc.pid, brd.name)
                        except Exception as exc:
                            self.logger.error("Failed to start process for %s: %s", brd.name, exc)
                            self._inflight_brds.discard(brd)
                            self._stop_worker_log(worker_log)

                    # periodic status
                    if (current_time - last_log_time >= 10):
                        stats = self.get_stats()
                        active_brds = [self._pid_to_brd.get(p.pid, "unknown")
                                    for p in self.active_processes if p.is_alive()]
                        self.logger.info("Cycle %d — Active: %s processing: %s",
                                        iteration, stats["active_jobs"],
                                        [getattr(b, 'name', str(b)) for b in active_brds])
                        last_log_time = current_time

                    # deleted BRD → request stop
                    for p in self.active_processes[:]:
                        brd = self._pid_to_brd.get(p.pid)
                        if brd and not brd.exists():
                            self.logger.info("Detected deleted BRD %s — requesting worker stop.", brd.name)
                            self._signal_worker_stop(brd)

                    # idle cleanup
                    is_system_idle = (len(self.active_processes) == 0)
                    if is_system_idle:
                        if not cleaned:
                            self.logger.info("System idle (no active jobs, no pending incidents)")
                            self._deep_cleanup()
                            cleaned = True
                    else:
                        cleaned = False

                    # adaptive sleep
                    sleep_time = min(check_interval * 2, 5) if len(self.active_processes) >= self.max_concurrent_BRD_agents else check_interval
                    time.sleep(sleep_time)

                except Exception as exc:
                    # ← swallow per-iteration errors so the manager keeps running
                    self.logger.exception("Run-loop iteration error: %s", exc)
                    time.sleep(0.5)  # small backoff so we don't spin

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt → shutting down")
        finally:
            # Once shutdown starts, ignore additional Ctrl+C so cleanup can finish cleanly.
            previous_sigint = None
            try:
                previous_sigint = signal.getsignal(signal.SIGINT)
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            except (AttributeError, ValueError):
                pass

            try:
                self.shutdown()
            finally:
                if previous_sigint is not None:
                    try:
                        signal.signal(signal.SIGINT, previous_sigint)
                    except (AttributeError, ValueError):
                        pass

    ################################################################################

    # ---------------------------------------------------------------------
    #  Process lifecycle
    # ---------------------------------------------------------------------
    def _can_start_new_job(self) -> bool:
        return len(self.active_processes) < self.max_concurrent_BRD_agents

    def _start_worker_log(self) -> tuple:
        """Create a private log queue and daemon listener for one worker."""
        q = multiprocessing.Queue(maxsize=10_000)
        listener = logging.handlers.QueueListener(
            q, *self._log_sinks, respect_handler_level=True
        )
        listener.start()
        return q, listener

    def _stop_worker_log(self, worker_log, timeout: float = 3.0) -> None:
        """Bound the wait if a dying worker left a damaged queue record."""
        if not worker_log:
            return
        q, listener = worker_log
        try:
            listener.enqueue_sentinel()
        except Exception:
            pass
        thread = getattr(listener, "_thread", None)
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                self.logger.warning(
                    "Worker log stream did not drain after %.1fs; "
                    "some final worker messages may be lost.", timeout,
                )
            else:
                listener._thread = None
        try:
            q.cancel_join_thread()
            q.close()
        except (OSError, ValueError):
            pass

    def _check_function_timeouts(self) -> None:
        """Enforce cumulative computation AND total invocation wall-time limits.

        Request cancellation first. A generated GPT wrapper observes the request
        before subsequent calls and after a current API response. Kill a stuck
        worker after a bounded grace period, waiting longer if it currently holds
        a GPT slot to reduce the chance of stranding a shared semaphore permit.
        """
        compute_limit = FUNCTION_CALL_TIMEOUT_SECONDS
        wall_limit = FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS
        if compute_limit <= 0 and wall_limit <= 0:
            return
        try:
            shared = self.shared_data["shared_dict"]
        except (KeyError, OSError):
            return
        now = time.monotonic()
        for proc in list(self.active_processes):
            if not proc.is_alive():
                continue
            brd = self._pid_to_brd.get(proc.pid)
            if brd is None:
                continue
            try:
                key = f"function_call::{brd.name}"
                state = shared.get(key)
                if not isinstance(state, dict) or state.get("pid") != proc.pid:
                    continue
                paused = bool(state.get("paused", False))
                compute_elapsed = now - float(state.get("started_monotonic", now))
                wall_elapsed = now - float(
                    state.get("wall_started_monotonic", state.get("started_monotonic", now))
                )
                exceeded = (
                    (compute_limit > 0 and not paused and compute_elapsed > compute_limit)
                    or (wall_limit > 0 and wall_elapsed > wall_limit)
                )
                failure = self._timed_out_pids.get(proc.pid)
                if failure is None:
                    if not exceeded:
                        continue
                    kind = "total_wall" if wall_limit > 0 and wall_elapsed > wall_limit else "computation"
                    elapsed = wall_elapsed if kind == "total_wall" else compute_elapsed
                    limit = wall_limit if kind == "total_wall" else compute_limit
                    reason = (
                        f"Function exceeded {kind} budget: {elapsed:.1f}s elapsed "
                        f"(limit {limit:.1f}s, phase={state.get('phase', 'function_call')})"
                    )
                    # The worker alone writes function_call::<BRD>. The manager
                    # writes cancel::<BRD> with a per-invocation token instead;
                    # no read-modify-write race can reset gpt_slot_held.
                    invocation_id = state.get("invocation_id")
                    if not invocation_id:
                        self.logger.warning("Missing invocation ID for BRD %s; cannot cancel safely", brd.name)
                        continue
                    # A finished or replaced invocation should not be cancelled.
                    latest = shared.get(key)
                    if not isinstance(latest, dict) or latest.get("pid") != proc.pid or latest.get("invocation_id") != invocation_id:
                        continue
                    failure = {
                        "phase": str(state.get("phase", "function_call")),
                        "elapsed": elapsed, "limit": limit,
                        "kind": kind, "reason": reason, "requested_at": now,
                        "invocation_id": invocation_id,
                    }
                    self._timed_out_pids[proc.pid] = failure
                    shared[f"cancel::{brd.name}"] = {
                        "pid": proc.pid, "invocation_id": invocation_id, "reason": reason,
                    }
                    self.logger.error(
                        "ESCALATE TO HUMAN: BRD %s worker %s %s. "
                        "Requesting cooperative cancellation.", brd.name, proc.pid, reason,
                    )
                    continue

                # Both keys have a single owner. Never write back the worker's
                # timing/slot marker from the manager's stale read.
                if failure.get("invocation_id") != state.get("invocation_id"):
                    self.logger.warning(
                        "BRD %s advanced invocation after cancellation request; "
                        "discarding stale cancellation for %s", brd.name, failure.get("invocation_id"),
                    )
                    cancel_key = f"cancel::{brd.name}"
                    current_cancel = shared.get(cancel_key)
                    if isinstance(current_cancel, dict) and current_cancel.get("pid") == proc.pid and current_cancel.get("invocation_id") == failure.get("invocation_id"):
                        shared.pop(cancel_key, None)
                    self._timed_out_pids.pop(proc.pid, None)
                    continue

                # The worker has been told to stop. Once it returns from a GPT
                # call it can release the semaphore before honoring cancellation.
                elapsed_since_cancel = now - float(failure.get("requested_at", now))
                wait_grace = FUNCTION_CANCELLATION_GRACE_SECONDS
                if state.get("gpt_slot_held", False):
                    # An HTTP request can take up to the configured per-request
                    # deadline. Force kill only after a bounded opportunity to
                    # release the GPT semaphore. This is best effort, not a broker.
                    wait_grace = max(
                        wait_grace,
                        brd_processor.GPT_REQUEST_TIMEOUT_SECONDS + 5.0,
                    )
                if elapsed_since_cancel < wait_grace:
                    continue
                self.logger.error(
                    "ESCALATE TO HUMAN: BRD %s did not honor cancellation after %.1fs; "
                    "force-terminating PID %s (GPT slot held=%s).",
                    brd.name, elapsed_since_cancel, proc.pid,
                    bool(state.get("gpt_slot_held", False)),
                )
                proc.terminate()
                proc.join(timeout=2)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=2)
            except Exception as exc:
                self.logger.error("Function watchdog error for %s: %s", brd.name, exc)

    def _cleanup_finished(self):
        """
        Remove completed worker processes from `self.active_processes`
        and log their final status.
        """
        finished_count = 0

        # Iterate over a snapshot so we can mutate self.active_processes
        for proc in self.active_processes[:]:
            if proc.is_alive():
                continue                 # still running

            finished_count += 1
            exit_code = "unknown"

            try:
                # First, join quickly to harvest the real exit code
                proc.join(timeout=1)
                exit_code = proc.exitcode if proc.exitcode is not None else "unknown"

                # Handle a hung process that never reported an exit code
                if proc.exitcode is None:
                    self.logger.warning(
                        "Force-terminating unresponsive PID %s", proc.pid
                    )
                    proc.terminate()
                    proc.join(timeout=5)

                    if proc.is_alive():
                        # Last-ditch kill (Py ≥ 3.9 or POSIX kill)
                        try:
                            proc.kill()
                            proc.join(timeout=2)
                            exit_code = "killed"
                        except AttributeError:
                            exit_code = "force-terminated"
                    else:
                        exit_code = "terminated"

                # Non-zero but non-None means the process exited with an error
                elif proc.exitcode != 0:
                    exit_code = proc.exitcode

            except Exception as exc:
                self.logger.error(
                    "Error while cleaning PID %s: %s", proc.pid, exc
                )
                exit_code = "cleanup-error"

            # Remove from active list
            try:
                self.active_processes.remove(proc)
            except ValueError:
                self.logger.warning(
                    "PID %s already removed from active list", proc.pid
                )

            # Mark BRD available; clean exits may intentionally yield an idle slot.
            brd_done = self._pid_to_brd.pop(proc.pid, None)
            launched_hash = self._pid_to_brd_hash.pop(proc.pid, None)
            if brd_done is not None and exit_code == 0:
                self._consider_worker_yield(brd_done, proc.pid)
            watchdog_failure = self._timed_out_pids.pop(proc.pid, None)
            worker_log = self._pid_to_log.pop(proc.pid, None)
            if brd_done is not None:
                self._inflight_brds.discard(brd_done)
                shared = self.shared_data.get("shared_dict")
                stop_requested = False
                active_impl = None
                try:
                    if shared is not None:
                        stop_requested = bool(shared.get(f"stop::{brd_done.name}", False))
                        active_impl = shared.pop(f"active_impl::{brd_done.name}", None)
                except Exception:
                    pass

                unexpected_exit = (
                    (exit_code != 0 or watchdog_failure is not None)
                    and not stop_requested and brd_done.is_file()
                )
                if unexpected_exit and launched_hash:
                    try:
                        current_hash = brd_processor.brd_hash_signature(
                            brd_done.read_text(encoding="utf-8")
                        )
                        if current_hash == launched_hash:
                            pending_recovery = brd_processor.get_approved_handcrafted_recovery(
                                REGISTRY, launched_hash, SAVED_FUNC_DIR, brd_done
                            )
                            if pending_recovery:
                                # Failed/hung approved candidates do not get unlimited
                                # retries after a hard process crash or timeout.
                                brd_processor.finish_handcrafted_recovery(
                                    REGISTRY, launched_hash, SAVED_FUNC_DIR, brd_done,
                                    successful=False, registry_lock=self.shared_data.get("lock"),
                                )
                            reason = (
                                watchdog_failure.get("reason", "Function execution limit exceeded")
                                if watchdog_failure is not None
                                else f"Unexpected worker process exit (code={exit_code})"
                            )
                            # A hard worker exit does not prove the generated function
                            # was responsible; conservatively stop this BRD for review.
                            if isinstance(active_impl, dict) and active_impl.get("hash") == launched_hash:
                                try:
                                    brd_processor.add_quarantine(
                                        reg_path=REGISTRY,
                                        hash_signature=launched_hash,
                                        script_name=active_impl.get("script_name", "<unknown>"),
                                        origin=active_impl.get("origin", "unknown"),
                                        reason=reason,
                                        registry_lock=self.shared_data.get("lock"),
                                        script_path=active_impl.get("script_path"),
                                    )
                                except Exception as exc:
                                    self.logger.error(
                                        "Could not quarantine implementation after worker exit: %s", exc
                                    )
                            brd_processor.block_brd_until_changed(
                                REGISTRY, launched_hash, brd_done.name,
                                reason + "; human review required",
                                registry_lock=self.shared_data.get("lock"),
                                origin="unexpected_worker_exit",
                            )
                            self.logger.error(
                                "ESCALATE TO HUMAN: worker for %s exited unexpectedly (%s). "
                                "BRD hash %s blocked; no automatic restart.",
                                brd_done.name, exit_code, launched_hash,
                            )
                        else:
                            self.logger.warning(
                                "Worker %s exited after its BRD changed (%s -> %s); "
                                "new BRD content is not blocked.",
                                proc.pid, launched_hash, current_hash,
                            )
                    except Exception as exc:
                        self.logger.error(
                            "Could not persist worker-exit escalation for %s: %s",
                            brd_done.name, exc,
                        )
                # Clear stop flag for this BRD to avoid leaking True.
                try:
                    if shared is not None:
                        shared[f"stop::{brd_done.name}"] = False
                except Exception:
                    pass

            # Clear this exited worker's cancellation without affecting a new PID.
            if brd_done is not None:
                try:
                    shared = self.shared_data.get("shared_dict")
                    if shared is not None:
                        key = f"cancel::{brd_done.name}"
                        cancel = shared.get(key)
                        if isinstance(cancel, dict) and cancel.get("pid") == proc.pid:
                            shared.pop(key, None)
                except (OSError, EOFError, BrokenPipeError):
                    pass

            # Abandon only this worker's potentially corrupted logging pipe.
            # The manager's own logs go directly to the file handler.
            self._stop_worker_log(worker_log)
            # A worker killed during a call may leave a stale marker.
            if brd_done is not None:
                try:
                    shared = self.shared_data.get("shared_dict")
                    if shared is not None:
                        marker = shared.get(f"function_call::{brd_done.name}")
                        if isinstance(marker, dict) and marker.get("pid") == proc.pid:
                            shared.pop(f"function_call::{brd_done.name}", None)
                except Exception:
                    pass

            # Log outcome
            log_level = logging.INFO if exit_code == 0 else logging.WARNING
            self.logger.log(
                log_level,
                "Process %s finished (exit: %s)",
                proc.pid,
                exit_code,
            )

            # Free OS resources (3.11+)
            try:
                proc.close()
            except AttributeError:
                pass

        if finished_count:
            self.logger.debug(
                "Cleaned up %d finished processes", finished_count
            )


    def get_stats(self) -> Dict:
        """Get current system statistics."""
        return {
            "active_jobs": len(self.active_processes),
            "max_concurrent": self.max_concurrent_BRD_agents,
            "capacity_used": f"{len(self.active_processes)}/{self.max_concurrent_BRD_agents}",
        }

    def _signal_worker_stop(self, brd_path: pathlib.Path) -> None:
        """Request a graceful stop for the worker handling this BRD."""
        try:
            shared = self.shared_data.get("shared_dict")
            if shared is None:
                return
            key = f"stop::{brd_path.name}"
            shared[key] = True
            self.logger.info("Stop requested for %s (key=%s)", brd_path.name, key)
        except Exception as exc:
            self.logger.warning("Could not set stop flag for %s: %s", brd_path.name, exc)

    def stop_worker_for_brd(self, brd_filename: str, timeout: float = 30.0) -> bool:
        """
        Politely stop a single worker by BRD file name.
        Returns True if the worker exited within timeout, else False.
        """
        target_proc = None
        target_brd = None
        for p in self.active_processes:
            brd = self._pid_to_brd.get(p.pid)
            if brd is not None and brd.name == brd_filename:
                target_proc = p
                target_brd = brd
                break

        if target_proc is None or target_brd is None:
            self.logger.info("No active worker found for %s", brd_filename)
            return True  # already stopped

        # Ask nicely
        self._signal_worker_stop(target_brd)

        # Wait for graceful exit
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not target_proc.is_alive():
                try:
                    target_proc.join(timeout=1)
                except Exception:
                    pass
                self.logger.info("Worker for %s exited gracefully", brd_filename)
                try:
                    self.active_processes.remove(target_proc)
                except ValueError:
                    pass
                self._inflight_brds.discard(target_brd)
                self._pid_to_brd.pop(target_proc.pid, None)
                self._pid_to_brd_hash.pop(target_proc.pid, None)
                self._stop_worker_log(self._pid_to_log.pop(target_proc.pid, None))
                self._timed_out_pids.pop(target_proc.pid, None)
                try:
                    shared = self.shared_data.get("shared_dict")
                    if shared is not None:
                        shared[f"stop::{target_brd.name}"] = False
                except Exception:
                    pass
                return True
            time.sleep(0.25)

        # Escalate
        self.logger.warning("Worker for %s did not exit in %.1fs; terminating.", brd_filename, timeout)
        try:
            target_proc.terminate()
            target_proc.join(timeout=5)
            if target_proc.is_alive():
                try:
                    target_proc.kill()
                    target_proc.join(timeout=2)
                except AttributeError:
                    pass
        except Exception as exc:
            self.logger.warning("Error force-stopping %s: %s", brd_filename, exc)

        try:
            self.active_processes.remove(target_proc)
        except ValueError:
            pass
        self._inflight_brds.discard(target_brd)
        self._pid_to_brd.pop(target_proc.pid, None)
        self._pid_to_brd_hash.pop(target_proc.pid, None)
        self._stop_worker_log(self._pid_to_log.pop(target_proc.pid, None))
        self._timed_out_pids.pop(target_proc.pid, None)
        return False

    def _deep_cleanup(self):
        """Run lightweight memory cleanup while the manager is idle."""
        self.logger.info("Idle → deep cleanup")
        before = psutil.Process().memory_info().rss / 2**20
        gc.collect()
        after = psutil.Process().memory_info().rss / 2**20
        self.logger.info("Cleanup freed %.1f MB (RSS now %.1f MB)", before - after, after)

    def _heartbeat_loop(self):
        shared = self.shared_data.get("shared_dict")
        while not self._hb_stop.is_set():
            try:
                if shared is not None:
                    hb = shared.get("hb", {})
                    hb["id"] = getattr(self, "_hb_id", "unknown")
                    hb["pid"] = os.getpid()
                    hb["ts"] = time.time()
                    shared["hb"] = hb
            except Exception:
                pass
            if self._hb_stop.wait(self._hb_interval):
                break

    # ------------------------------------------------------------------
    def shutdown(self):
        """Gracefully stop workers and tear down logging without enqueue-after-close errors."""
        # prevent new work
        self._shutdown_requested = True

        # 1) announce (ok to log now; queue/listener still active)
        try:
            self.logger.info("Shutting down %d active processes...", len(self.active_processes))
        except Exception:
            pass

        # 2) politely ask all workers to stop
        try:
            for p in list(self.active_processes):
                brd = self._pid_to_brd.get(p.pid)
                if brd is not None:
                    self._signal_worker_stop(brd)
        except Exception:
            pass

        # 3) wait up to N seconds for graceful exit
        polite_timeout = 30.0
        deadline = time.time() + polite_timeout
        for p in list(self.active_processes):
            remaining = max(0.0, deadline - time.time())
            if remaining <= 0:
                break
            try:
                p.join(timeout=remaining)
            except Exception:
                pass

        # 4) escalate on stragglers (terminate/kill), then clean bookkeeping
        for p in list(self.active_processes):
            if p.is_alive():
                try:
                    self.logger.warning("Force-terminating stubborn PID %s", p.pid)
                except Exception:
                    pass
                try:
                    p.terminate()
                    p.join(timeout=5)
                    if p.is_alive():
                        try:
                            p.kill()  # may not exist on very old Pythons/Windows
                            p.join(timeout=2)
                        except AttributeError:
                            pass
                except Exception:
                    pass

            # remove from active list and mappings
            try:
                self.active_processes.remove(p)
            except ValueError:
                pass
            brd_done = self._pid_to_brd.pop(p.pid, None)
            self._pid_to_brd_hash.pop(p.pid, None)
            self._timed_out_pids.pop(p.pid, None)
            self._stop_worker_log(self._pid_to_log.pop(p.pid, None))
            if brd_done is not None:
                self._inflight_brds.discard(brd_done)
                try:
                    sd = self.shared_data.get("shared_dict")
                    if sd is not None:
                        sd[f"stop::{brd_done.name}"] = False
                except Exception:
                    pass

            # free proc resources (3.11+)
            try:
                p.close()
            except Exception:
                pass

        # 5) stop heartbeat thread BEFORE touching logging objects
        try:
            self._hb_stop.set()
            self._hb_thread.join(timeout=2)
        except Exception:
            pass

        # broadcast global shutdown flag for anyone watching shared state
        try:
            sd = self.shared_data.get("shared_dict")
            if sd is not None:
                sd["shutdown_requested"] = True
        except Exception:
            pass

        # 6) Each worker stream has a bounded drain. Never wait forever on a
        # queue record corrupted by abrupt worker termination.
        for pid in list(self._pid_to_log):
            self._stop_worker_log(self._pid_to_log.pop(pid, None))

        # 8) shut down the multiprocessing.Manager (after workers are gone)
        try:
            self._manager.shutdown()
        except Exception:
            pass

        # 9) final logging shutdown (no more handlers should reference the queue)
        try:
            for handler in list(self.logger.handlers):
                self.logger.removeHandler(handler)
            logging.shutdown()
        except Exception:
            pass

        # (optional) final, non-logging notification to stderr (avoids logging after detach)
        try:
            sys.stderr.write("Shutdown completed.\n")
        except Exception:
            pass
###################################################


def main():
    # Explicit human approval is the ONLY route to recover a blocked BRD
    # without changing its BRD text. Stop JobManager before invoking this command.
    if len(sys.argv) >= 2 and sys.argv[1] == "--approve-handcrafted":
        if len(sys.argv) != 3 or pathlib.Path(sys.argv[2]).name != sys.argv[2]:
            raise SystemExit("Usage: python job_manager.py --approve-handcrafted BRD_example.txt")
        brd = PROJECT_ROOT / "documents" / sys.argv[2]
        approval = brd_processor.approve_handcrafted_recovery(brd, SAVED_FUNC_DIR, REGISTRY)
        print(f"Approved handcrafted retry: {brd.name} -> {approval['script']}")
        print("Restart JobManager. The block clears ONLY if the handcrafted code passes all BRD and N-job tests.")
        return
    if len(sys.argv) > 1:
        raise SystemExit("Usage: python job_manager.py [--approve-handcrafted BRD_example.txt]")

    # ── [1] safe start-method (Windows needs this BEFORE anything else) ──
    if multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("spawn", force=True)
    multiprocessing.freeze_support()  # harmless elsewhere, required for frozen exe on Win

    # ── [2] resolve project root ────────────────────────────────────────
    root = PROJECT_ROOT

    # ── [3] optional DEBUG helper: reset done-state to re-run jobs ─────
    docs_dir = root / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    if RESET_DONE_STATE:
        deleted_state_files = 0
        for jobs_json in docs_dir.glob("BRD_*_jobs.json"):
            done_file = jobs_json.parent / f"done_{jobs_json.stem}.json"
            if done_file.exists():
                done_file.unlink()
                deleted_state_files += 1
                logging.info("Deleted debug state file %s", done_file)
        logging.info("Debug reset complete - removed %d runtime state files.", deleted_state_files)

    # ── [4] launch the JobManager ───────────────────────────────────────
    # NOTE: set to_activate_console=False if you don’t want console logs
    JobManager(max_concurrent_BRD_agents=10, to_activate_console=False).run_continuous(
        check_interval=1,
        # only_list=["BRD_email_router.txt"],
        # only_list=["BRD_tolerate_OCR_mistakes.txt"],
        # exclude_list=[],
    )

if __name__ == "__main__":
    main()