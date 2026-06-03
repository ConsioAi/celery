import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from celery.exceptions import TimeLimitExceeded
from t.integration.tasks import ExpectedException, add, fail
from t.smoke.tasks import long_running_task, non_eta_task_with_retries, self_termination_delay_timeout


class test_ipc_pool:

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_pool = 'ipc'
        app.conf.worker_concurrency = 1
        app.conf.worker_prefetch_multiplier = 1
        return app

    def test_success(self, celery_setup: CeleryTestSetup):
        sig = add.s(2, 2).set(queue=celery_setup.worker.worker_queue)
        assert sig.delay().get(timeout=RESULT_TIMEOUT) == 4

    def test_failure(self, celery_setup: CeleryTestSetup):
        sig = fail.s().set(queue=celery_setup.worker.worker_queue)
        with pytest.raises(ExpectedException):
            sig.delay().get(timeout=RESULT_TIMEOUT)

    def test_retry(self, celery_setup: CeleryTestSetup):
        sig = non_eta_task_with_retries.s().set(queue=celery_setup.worker.worker_queue)
        assert sig.delay().get(timeout=RESULT_TIMEOUT) == 2

    def test_hard_timeout(self, celery_setup: CeleryTestSetup):
        sig = self_termination_delay_timeout.s().set(queue=celery_setup.worker.worker_queue)
        with pytest.raises(TimeLimitExceeded):
            sig.delay().get(timeout=RESULT_TIMEOUT)

    def test_revoke_terminate(self, celery_setup: CeleryTestSetup):
        sig = long_running_task.s(30).set(queue=celery_setup.worker.worker_queue)
        result = sig.delay()

        celery_setup.worker.assert_log_exists('Starting long running task')
        result.revoke(terminate=True)

        celery_setup.worker.assert_log_exists(f'Terminating {result.id}')
