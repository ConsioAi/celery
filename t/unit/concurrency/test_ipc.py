import os
import signal
import sys
import time

import pytest
from billiard.einfo import ExceptionInfo

from celery.concurrency.ipc import TaskPool
from celery.exceptions import SoftTimeLimitExceeded

pytestmark = pytest.mark.skipif(
    sys.platform == 'win32' or not hasattr(os, 'fork'),
    reason='ipc pool requires fork+execve and UNIX socketpairs',
)


def add(x, y):
    return x + y


def fail():
    raise KeyError('boom')


def sleep_for(seconds):
    time.sleep(seconds)
    return 'slept'


def soft_limited():
    try:
        time.sleep(5)
    except SoftTimeLimitExceeded:
        return 'soft-timeout'
    return 'missed-soft-timeout'


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class test_TaskPool:

    def test_apply_async_success(self):
        pool = TaskPool(1)
        pool.start()
        callbacks = []
        accepted = []
        try:
            result = pool.apply_async(
                add,
                args=(2, 3),
                callback=callbacks.append,
                accept_callback=lambda pid, t: accepted.append((pid, t)),
            )

            assert result.get(timeout=10) == 5
            assert wait_for(lambda: callbacks == [5])
            assert accepted
            assert accepted[0][0] > 0
            assert accepted[0][1] > 0
        finally:
            pool.stop()

    def test_apply_async_failure_calls_error_callback(self):
        pool = TaskPool(1)
        pool.start()
        errors = []
        try:
            result = pool.apply_async(fail, error_callback=errors.append)
            einfo = result.get(timeout=10)

            assert isinstance(einfo, ExceptionInfo)
            assert isinstance(einfo.exception, KeyError)
            assert wait_for(lambda: len(errors) == 1)
            assert isinstance(errors[0].exception, KeyError)
        finally:
            pool.stop()

    def test_soft_timeout_signals_child(self):
        pool = TaskPool(1)
        pool.start()
        timeouts = []
        try:
            result = pool.apply_async(
                soft_limited,
                soft_timeout=0.2,
                timeout=2,
                timeout_callback=lambda soft, timeout: timeouts.append((soft, timeout)),
            )

            assert result.get(timeout=10) == 'soft-timeout'
            assert wait_for(lambda: timeouts == [(True, 0.2)])
        finally:
            pool.stop()

    def test_hard_timeout_kills_child(self):
        pool = TaskPool(1)
        pool.start()
        timeouts = []
        try:
            result = pool.apply_async(
                sleep_for,
                args=(5,),
                timeout=0.2,
                timeout_callback=lambda soft, timeout: timeouts.append((soft, timeout)),
            )

            assert result.get(timeout=10) is None
            assert wait_for(lambda: timeouts == [(False, 0.2)])
        finally:
            pool.terminate()

    def test_terminate_job(self):
        pool = TaskPool(1)
        pool.start()
        accepted = []
        try:
            result = pool.apply_async(
                sleep_for,
                args=(5,),
                accept_callback=lambda pid, t: accepted.append(pid),
            )
            assert wait_for(lambda: bool(accepted))

            pool.terminate_job(accepted[0], signal.SIGTERM)

            assert result.get(timeout=10) is None
            assert wait_for(lambda: accepted[0] not in pool._jobs_by_pid)
        finally:
            pool.terminate()

    def test_grow_shrink(self):
        pool = TaskPool(1)
        pool.start()
        try:
            pool.grow(2)
            assert pool.num_processes == 3
            pool.shrink(1)
            assert pool.num_processes == 2
        finally:
            pool.stop()

    def test_info(self):
        pool = TaskPool(2, timeout=10, soft_timeout=5)
        pool.start()
        try:
            info = pool.info
            assert info['implementation'] == 'celery.concurrency.ipc:TaskPool'
            assert info['max-concurrency'] == 2
            assert info['processes'] == []
            assert info['pending'] == 0
            assert info['active'] == 0
            assert info['timeouts'] == (5, 10)
        finally:
            pool.stop()
