"""Independent worker with bounded child execution, heartbeat, fencing and SIGTERM drain."""
from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import os
import signal
import socket
import threading
import time
import uuid

from asyncforge.core import LEASE_SECONDS, claim, complete, heartbeat, timeout_reap
from asyncforge.handlers import PermanentTaskError, RetryableTaskError, execute

logger = logging.getLogger("asyncforge.worker")


def child_run(task_type, payload, attempt, pipe):
    try:
        value = execute(task_type, payload, attempt)
        pipe.send(("success", value))
    except RetryableTaskError:
        pipe.send(("retryable", "RETRYABLE_TASK_ERROR"))
    except PermanentTaskError:
        pipe.send(("permanent", "PERMANENT_TASK_ERROR"))
    except Exception:
        pipe.send(("permanent", "HANDLER_FAILURE"))
    finally:
        pipe.close()


class Worker:
    def __init__(self, worker_id=None, queue="default", poll=0.10, lease=LEASE_SECONDS, grace=10):
        self.worker_id = worker_id or (socket.gethostname() + "-" + str(os.getpid()) + "-" + uuid.uuid4().hex[:8])
        self.queue, self.poll, self.lease, self.grace = queue, poll, lease, grace
        self.stop = threading.Event()
        self.stopping_since = None

    def ask_stop(self, *_):
        if self.stopping_since is None:
            self.stopping_since = time.monotonic()
        self.stop.set()

    def _run_one(self, j):
        ctx = mp.get_context("spawn")
        recv, send = ctx.Pipe(duplex=False)
        child = ctx.Process(target=child_run, args=(j["task_type"], j["payload"], j["attempt"], send))
        child.start()
        send.close()
        began = time.monotonic()
        last_heartbeat = began
        outcome = None
        try:
            while True:
                if recv.poll(0.05):
                    try:
                        outcome = recv.recv()
                    except EOFError:
                        outcome = ("retryable", "CHILD_PROCESS_LOST")
                    break
                if not child.is_alive():
                    outcome = ("retryable", "CHILD_PROCESS_LOST")
                    break
                elapsed = time.monotonic() - began
                if elapsed >= j["timeout"]:
                    outcome = ("retryable", "TASK_TIMEOUT")
                    break
                if self.stop.is_set() and self.stopping_since is not None and time.monotonic() - self.stopping_since >= self.grace:
                    outcome = ("retryable", "SHUTDOWN_INTERRUPTED")
                    break
                if time.monotonic() - last_heartbeat >= max(0.2, self.lease / 3):
                    try:
                        if not heartbeat(j["id"], self.worker_id, j["generation"], self.lease):
                            outcome = ("lost", "LEASE_LOST")
                            break
                    except Exception:
                        logger.warning("heartbeat DB unavailable; stopping task; never report success")
                        outcome = ("lost", "DATABASE_UNAVAILABLE")
                        break
                    last_heartbeat = time.monotonic()
        finally:
            if child.is_alive():
                child.terminate()
            child.join(timeout=2)
            if child.is_alive():
                child.kill()
                child.join(timeout=2)
            recv.close()
        if outcome[0] == "lost":
            logger.warning("lease lost job_id=%s", j["id"])
            return
        try:
            ok = complete(j["id"], self.worker_id, j["generation"],
                          result=outcome[1] if outcome[0] == "success" else None,
                          error_code=None if outcome[0] == "success" else outcome[1],
                          retryable=outcome[0] == "retryable")
            if not ok:
                logger.warning("stale completion fenced job_id=%s", j["id"])
        except Exception:
            logger.warning("completion database failure; lease will recover")

    def run(self, max_idle=None):
        signal.signal(signal.SIGTERM, self.ask_stop)
        signal.signal(signal.SIGINT, self.ask_stop)
        idle = 0
        while not self.stop.is_set():
            try:
                timeout_reap()
                j = claim(self.worker_id, self.queue, self.lease)
                if j:
                    idle = 0
                    self._run_one(j)
                else:
                    idle += self.poll
                    if max_idle is not None and idle >= max_idle:
                        break
                    self.stop.wait(self.poll)
            except Exception:
                logger.warning("database failure; retry after wait; no in-memory fallback")
                self.stop.wait(max(0.5, self.poll))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="default")
    parser.add_argument("--worker-id")
    parser.add_argument("--max-idle", type=float)
    parser.add_argument("--grace", type=float, default=10)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    Worker(worker_id=args.worker_id, queue=args.queue, max_idle=None, grace=args.grace).run(max_idle=args.max_idle)


if __name__ == "__main__":
    main()
