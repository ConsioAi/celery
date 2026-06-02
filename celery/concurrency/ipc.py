"""Exec-based IPC execution pool.

This pool starts a fresh Python interpreter for each task using fork+execve and
communicates with it over an anonymous UNIX socketpair.
"""
from __future__ import annotations

import os
import pickle
import signal as _signal
import socket
import struct
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from itertools import count
from types import FrameType
from typing import Any, Callable

from billiard.einfo import ExceptionInfo
from billiard.exceptions import SoftTimeLimitExceeded, WorkerLostError

from celery.concurrency.base import BasePool
from celery.concurrency.prefork import process_destructor, process_initializer
from celery.exceptions import WorkerShutdown, WorkerTerminate
from celery.utils.log import get_logger

__all__ = ('TaskPool', 'child_main')

logger = get_logger(__name__)

_HEADER = struct.Struct('!I')
_EXEC_ENV = 'FORKED_BY_MULTIPROCESSING'

MSG_RUN = 'RUN'
MSG_ACCEPTED = 'ACCEPTED'
MSG_RESULT = 'RESULT'
MSG_EXCEPTION = 'EXCEPTION'


def _send_message(sock: socket.socket, message: Any) -> None:
    payload = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(_HEADER.pack(len(payload)))
    sock.sendall(payload)


def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def _recv_message(sock: socket.socket) -> Any:
    header = _recv_exact(sock, _HEADER.size)
    if header is None:
        raise EOFError()
    size, = _HEADER.unpack(header)
    payload = _recv_exact(sock, size)
    if payload is None:
        raise EOFError()
    return pickle.loads(payload)


def _exception_info(exc_info: tuple | None = None) -> ExceptionInfo:
    return ExceptionInfo(exc_info=exc_info) if exc_info else ExceptionInfo()


def _worker_lost(message: str) -> ExceptionInfo:
    try:
        raise WorkerLostError(message)
    except WorkerLostError:
        return ExceptionInfo()


def _install_soft_timeout_handler() -> None:
    if not hasattr(_signal, 'SIGUSR1'):
        return

    def on_soft_timeout(signum: int, frame: FrameType | None) -> None:
        raise SoftTimeLimitExceeded()

    _signal.signal(_signal.SIGUSR1, on_soft_timeout)


def _run_child_job(sock: socket.socket, message: dict[str, Any]) -> None:
    app = message.get('app')
    hostname = message.get('hostname')
    if app is not None:
        process_initializer(app, hostname)

    if message.get('soft_timeout'):
        _install_soft_timeout_handler()

    _send_message(sock, (MSG_ACCEPTED, os.getpid(), time.monotonic()))

    target = message['target']
    args = message.get('args') or ()
    kwargs = message.get('kwargs') or {}
    try:
        result = target(*args, **kwargs)
    except (WorkerShutdown, WorkerTerminate):
        raise
    except BaseException:
        _send_message(sock, (MSG_EXCEPTION, _exception_info(sys.exc_info())))
    else:
        _send_message(sock, (MSG_RESULT, result))


def child_main(fd: int) -> int:
    """Entry point used after execve by :class:`TaskPool` children."""
    sock = socket.socket(fileno=fd)
    try:
        message_type, message = _recv_message(sock)
        if message_type != MSG_RUN:
            raise RuntimeError(f'Unknown IPC pool message: {message_type!r}')
        _run_child_job(sock, message)
        return 0
    except (WorkerShutdown, WorkerTerminate):
        raise
    except BaseException:
        try:
            _send_message(sock, (MSG_EXCEPTION, _exception_info(sys.exc_info())))
        except Exception:
            logger.critical('IPC pool child could not report failure', exc_info=True)
        return 1
    finally:
        sock.close()


class ApplyResult:
    """Small result object compatible with Celery request termination hooks."""

    def __init__(self, job: '_Job') -> None:
        self._job = job
        self._ready = threading.Event()
        self._value = None

    def _set(self, value: Any) -> None:
        self._value = value
        self._ready.set()

    def get(self, timeout: float | None = None) -> Any:
        if not self._ready.wait(timeout):
            raise TimeoutError()
        return self._value

    def wait(self, timeout: float | None = None) -> bool:
        return self._ready.wait(timeout)

    def ready(self) -> bool:
        return self._ready.is_set()

    def terminate(self, signal: int | None = None) -> None:
        self._job.terminate(signal)


@dataclass(eq=False)
class _Job:
    pool: 'TaskPool'
    target: Callable
    args: tuple
    kwargs: dict
    callback: Callable | None
    error_callback: Callable | None
    accept_callback: Callable | None
    timeout_callback: Callable | None
    soft_timeout: float | None
    timeout: float | None
    correlation_id: str | None
    result: ApplyResult | None = None
    pid: int | None = None
    sock: socket.socket | None = None
    soft_timer: threading.Timer | None = None
    hard_timer: threading.Timer | None = None
    watcher: threading.Thread | None = None
    timed_out: bool = False
    terminated: bool = False

    def start(self) -> None:
        parent_sock, child_sock = socket.socketpair()
        child_sock.set_inheritable(True)
        pid = os.fork()
        if pid == 0:  # pragma: no cover - exercised through parent tests.
            try:
                parent_sock.close()
                os.environ[_EXEC_ENV] = '1'
                os.execv(sys.executable, [
                    sys.executable,
                    '-m',
                    'celery.concurrency.ipc',
                    str(child_sock.fileno()),
                ])
            finally:
                os._exit(1)

        child_sock.close()
        self.pid = pid
        self.sock = parent_sock
        self.pool._register_pid(pid, self)
        self._send_run_message()
        self.watcher = threading.Thread(
            target=self._watch,
            name=f'ipc-pool-result-{pid}',
            daemon=True,
        )
        self.watcher.start()

    def _send_run_message(self) -> None:
        assert self.sock is not None
        _send_message(self.sock, (MSG_RUN, {
            'app': self.pool.app,
            'hostname': self.pool.hostname,
            'target': self.target,
            'args': self.args,
            'kwargs': self.kwargs,
            'soft_timeout': self.soft_timeout,
            'timeout': self.timeout,
            'correlation_id': self.correlation_id,
        }))

    def _watch(self) -> None:
        exitcode = None
        try:
            while True:
                msg = _recv_message(self.sock)
                self._handle_message(msg)
                if msg[0] in {MSG_RESULT, MSG_EXCEPTION}:
                    break
        except EOFError:
            if not self.timed_out and not self.terminated:
                self._handle_lost('IPC pool child exited before reporting a result')
            elif not self.result.ready():
                self.result._set(None)
        except BaseException as exc:  # pragma: no cover - defensive.
            logger.error('IPC pool result handler failed: %r', exc, exc_info=True)
            self._handle_lost(f'IPC pool result handler failed: {exc!r}')
        finally:
            self._cancel_timers()
            if self.sock is not None:
                self.sock.close()
            if self.pid is not None:
                try:
                    _, status = os.waitpid(self.pid, 0)
                    exitcode = os.waitstatus_to_exitcode(status)
                except ChildProcessError:
                    exitcode = None
                self.pool._unregister_pid(self.pid)
                process_destructor(self.pid, exitcode)
            self.pool._job_done(self)

    def _handle_message(self, msg: tuple) -> None:
        kind = msg[0]
        if kind == MSG_ACCEPTED:
            _, pid, accepted_at = msg
            self.pid = pid
            if self.accept_callback:
                self.accept_callback(pid, accepted_at)
            self._arm_timers()
        elif kind == MSG_RESULT:
            _, value = msg
            self.result._set(value)
            if self.callback:
                self.callback(value)
        elif kind == MSG_EXCEPTION:
            _, einfo = msg
            self.result._set(einfo)
            if self.error_callback:
                self.error_callback(einfo)
            elif self.callback:
                self.callback(einfo)

    def _handle_lost(self, message: str) -> None:
        einfo = _worker_lost(message)
        self.result._set(einfo)
        if self.error_callback:
            self.error_callback(einfo)

    def _arm_timers(self) -> None:
        if self.soft_timeout:
            self.soft_timer = threading.Timer(self.soft_timeout, self._on_soft_timeout)
            self.soft_timer.daemon = True
            self.soft_timer.start()
        if self.timeout:
            self.hard_timer = threading.Timer(self.timeout, self._on_hard_timeout)
            self.hard_timer.daemon = True
            self.hard_timer.start()

    def _cancel_timers(self) -> None:
        for timer in (self.soft_timer, self.hard_timer):
            if timer is not None:
                timer.cancel()

    def _on_soft_timeout(self) -> None:
        if self.result.ready() or self.pid is None:
            return
        if self.timeout_callback:
            self.timeout_callback(True, self.soft_timeout)
        if hasattr(_signal, 'SIGUSR1'):
            self._kill(_signal.SIGUSR1)

    def _on_hard_timeout(self) -> None:
        if self.result.ready():
            return
        self.timed_out = True
        if self.timeout_callback:
            self.timeout_callback(False, self.timeout)
        self.terminate(_signal.SIGKILL)

    def _kill(self, sig: int) -> None:
        if self.pid is None:
            return
        try:
            os.kill(self.pid, sig)
        except ProcessLookupError:
            pass

    def terminate(self, sig: int | None = None) -> None:
        self.terminated = True
        self._kill(sig or _signal.SIGTERM)


class TaskPool(BasePool):
    """Process pool that executes every task in a fresh execve child."""

    signal_safe = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        initargs = self.options.get('initargs') or ()
        self.hostname = initargs[1] if len(initargs) > 1 else socket.gethostname()
        self._pending: deque[_Job] = deque()
        self._active: set[_Job] = set()
        self._jobs_by_pid: dict[int, _Job] = {}
        self._lock = threading.RLock()
        self._closing = False
        self._job_counter = count()

    def on_start(self) -> None:
        self._closing = False

    def on_apply(self, target, args=None, kwargs=None, callback=None,
                 accept_callback=None, timeout_callback=None,
                 error_callback=None, soft_timeout=None, timeout=None,
                 correlation_id=None, **_):
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        job = _Job(
            pool=self,
            target=target,
            args=args,
            kwargs=kwargs,
            callback=callback,
            error_callback=error_callback,
            accept_callback=accept_callback,
            timeout_callback=timeout_callback,
            soft_timeout=soft_timeout,
            timeout=timeout,
            correlation_id=correlation_id or str(next(self._job_counter)),
        )
        result = ApplyResult(job)
        job.result = result
        with self._lock:
            if self._closing:
                raise RuntimeError('IPC pool is closing')
            self._pending.append(job)
            self._maybe_start_jobs()
        return result

    def _maybe_start_jobs(self) -> None:
        while self._pending and len(self._active) < self.limit:
            job = self._pending.popleft()
            self._active.add(job)
            try:
                job.start()
            except BaseException:
                self._active.discard(job)
                raise

    def _job_done(self, job: _Job) -> None:
        with self._lock:
            self._active.discard(job)
            if not self._closing:
                self._maybe_start_jobs()

    def _register_pid(self, pid: int, job: _Job) -> None:
        with self._lock:
            self._jobs_by_pid[pid] = job

    def _unregister_pid(self, pid: int) -> None:
        with self._lock:
            self._jobs_by_pid.pop(pid, None)

    def terminate_job(self, pid, signal=None):
        with self._lock:
            job = self._jobs_by_pid.get(pid)
        if job is not None:
            job.terminate(signal)

    def grow(self, n=1):
        with self._lock:
            self.limit += n
            self._maybe_start_jobs()

    def shrink(self, n=1):
        with self._lock:
            self.limit = max(0, self.limit - n)

    def restart(self):
        self.terminate()
        self.start()

    def on_close(self):
        self._closing = True

    def on_stop(self):
        self._closing = True
        self._join_active()

    def on_terminate(self):
        self._closing = True
        with self._lock:
            jobs = list(self._active)
            self._pending.clear()
        for job in jobs:
            job.terminate()
        self._join_active()

    def _join_active(self) -> None:
        with self._lock:
            watchers = [job.watcher for job in self._active if job.watcher is not None]
        for watcher in watchers:
            watcher.join()

    def _get_info(self):
        info = super()._get_info()
        with self._lock:
            processes = sorted(self._jobs_by_pid)
            pending = len(self._pending)
            active = len(self._active)
        info.update({
            'max-concurrency': self.limit,
            'processes': processes,
            'pending': pending,
            'active': active,
            'timeouts': (
                self.options.get('soft_timeout') or 0,
                self.options.get('timeout') or 0,
            ),
            'put-guarded-by-semaphore': self.putlocks,
        })
        return info

    @property
    def num_processes(self):
        return self.limit


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(child_main(int(sys.argv[1])))
