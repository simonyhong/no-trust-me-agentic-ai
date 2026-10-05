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

class SafeQueueHandler(logging.handlers.QueueHandler):
    def __init__(self, q, fallback_path: str | None = None, formatter: logging.Formatter | None = None):
        super().__init__(q)
        self._dead = False
        self._fallback_path = fallback_path
        self._fallback = None
        self._fallback_formatter = formatter

    def _get_fallback(self):
        if self._fallback is None and self._fallback_path:
            self._fallback = logging.FileHandler(self._fallback_path, mode="a", encoding="utf-8")
            if self._fallback_formatter:
                self._fallback.setFormatter(self._fallback_formatter)
        return self._fallback

    def emit(self, record):
        if self._dead:
            fallback = self._get_fallback()
            if fallback:
                try:
                    fallback.emit(record)
                except Exception:
                    pass
            return

        try:
            rec = self.prepare(record)
            if isinstance(rec.msg, str) and len(rec.msg) > MAX_RECORD_BYTES:
                rec.msg = rec.msg[:MAX_RECORD_BYTES] + " …<truncated>"
            self.enqueue(rec)
        except queue.Full:
            # Transient back-pressure: divert only this record and keep the queue handler alive.
            fallback = self._get_fallback()
            if fallback:
                try:
                    fallback.emit(record)
                except Exception:
                    pass
        except Exception:
            self._dead = True
            fallback = self._get_fallback()
            if fallback:
                try:
                    fallback.emit(record)
                except Exception:
                    pass

    def close(self):
        self._dead = True
        if self._fallback is not None:
            try:
                self._fallback.close()
            except Exception:
                pass
        try:
            super().close()
        except Exception:
            pass

##################################### Job Manager ###################################################
SEMAPHORE_LIMIT = 5 # Only 5 concurrent GPT calls allowed 
show_time=True
class JobManager:
    def __init__(self, max_concurrent_BRD_agents, to_activate_console):
        # ── core state ──────────────────────────────────────────────────────────────
        self.max_concurrent_BRD_agents = max_concurrent_BRD_agents
        self._inflight_brds: set[pathlib.Path] = set()
        self._pid_to_brd: dict[int, pathlib.Path] = {}
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

        # Queue that workers write into
        self.log_q = multiprocessing.Queue(maxsize=10_000)

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

        # Start the listener that formats everything (workers send raw records)
        self._log_listener = logging.handlers.QueueListener(
            self.log_q, *sinks, respect_handler_level=True
        )
        self._log_listener.start()

        # App logger (do NOT touch root)
        self.logger = logging.getLogger("JobManager")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        # Queue handler; fallback file is created lazily only if the queue fails
        self._qh = SafeQueueHandler(
            self.log_q,
            fallback_path=str(PROJECT_ROOT / "all_process_fallback.log"),
            formatter=formatter,
        )
        self.logger.addHandler(self._qh)
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
                    self._cleanup_finished()

                    if current_time - last_function_clean_time >= 60:
                        last_function_clean_time = current_time
                        self.clean_saved_functions_and_registry()

                    # scan BRDs
                    docs_dir = PROJECT_ROOT / "documents"
                    if not docs_dir.exists():
                        self.logger.warning("Documents directory not found: %s", docs_dir)
                        docs_dir.mkdir(parents=True, exist_ok=True)

                    brd_files = sorted(docs_dir.glob("BRD_*.txt"))
                    if exclude_list:
                        brd_files = [b for b in brd_files if b.name not in exclude_list]
                    if only_list:
                        brd_files = [b for b in brd_files if b.name in only_list]

                    for brd in brd_files:
                        if brd in self._inflight_brds:
                            continue
                        if not self._can_start_new_job():
                            break

                        abs_brd = brd.resolve()
                        self.logger.info("Starting new process for %s", abs_brd.name)

                        # clear stale stop flag
                        try:
                            shared = self.shared_data.get("shared_dict")
                            if shared is not None:
                                shared[f"stop::{abs_brd.name}"] = False
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

                        proc = multiprocessing.Process(
                            target=brd_processor.process_single_brd_standalone,
                            args=(abs_brd, SAVED_FUNC_DIR, REGISTRY, self.gpt_semaphore,
                                self.log_q, self.shared_data, True if only_list else False),
                            name=f"BRD-{abs_brd.stem}"
                        )

                        try:
                            proc.start()
                            self.active_processes.append(proc)
                            self._inflight_brds.add(brd)
                            self._pid_to_brd[proc.pid] = brd
                            self.logger.info("Started process %d for %s", proc.pid, brd.name)
                        except Exception as exc:
                            self.logger.error("Failed to start process for %s: %s", brd.name, exc)
                            self._inflight_brds.discard(brd)

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

            # mark the BRD as available again    
            brd_done = self._pid_to_brd.pop(proc.pid, None)
            if brd_done is not None:
                self._inflight_brds.discard(brd_done)
                # Clear stop flag for this BRD to avoid leaking True
                try:
                    shared = self.shared_data.get("shared_dict")
                    if shared is not None:
                        shared[f"stop::{brd_done.name}"] = False
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

        # 6) DETACH queue handler first so nothing else enqueues
        try:
            if hasattr(self, "_qh") and self._qh:
                try:
                    self.logger.removeHandler(self._qh)
                except Exception:
                    pass
                try:
                    self._qh.close()
                except Exception:
                    pass
        except Exception:
            pass

        # 7) stop listener (drains queue into handlers), then close the queue
        try:
            if hasattr(self, "_log_listener") and self._log_listener:
                self._log_listener.stop()
        except Exception:
            pass
        try:
            if hasattr(self, "log_q") and self.log_q:
                self.log_q.close()
                self.log_q.join_thread()
        except Exception:
            pass

        # 8) shut down the multiprocessing.Manager (after workers are gone)
        try:
            self._manager.shutdown()
        except Exception:
            pass

        # 9) final logging shutdown (no more handlers should reference the queue)
        try:
            self.logger.handlers[:] = []
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
    # ── [1] safe start-method (Windows needs this BEFORE anything else) ──
    if multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("spawn", force=True)
    multiprocessing.freeze_support()  # harmless elsewhere, required for frozen exe on Win

    # ── [2] resolve project root ────────────────────────────────────────
    root = PROJECT_ROOT

    # ── [3] optional DEBUG helper: purge done markers so all jobs rerun ─
    docs_dir = root / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    if RESET_DONE_STATE:
        deleted_done_files = 0
        for jobs_json in docs_dir.glob("BRD_*_jobs.json"):
            done_file = jobs_json.parent / f"done_{jobs_json.stem}.json"
            if done_file.exists():
                done_file.unlink()
                deleted_done_files += 1
                logging.info("Deleted debug done-file %s", done_file)
        logging.info("Debug reset complete - removed %d done-state files.", deleted_done_files)

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