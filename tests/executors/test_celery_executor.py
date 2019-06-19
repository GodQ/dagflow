# __author__ = 'godq'
import unittest
import time

import dagflow.executors.celery_executor as celery_executor
from celery import states as celery_states


class CeleryExecutorTest(unittest.TestCase):
    '''
    first run "celery worker -A dagflow.executors.celery_executor" to start celery worker
    '''

    def test_common_celery_task(self):
        task = celery_executor.common_celery_task.s("hello_plugin2", {"name": "hello"})
        async_result = task.delay()
        print(async_result)
        while async_result.state not in celery_states.READY_STATES:
            time.sleep(1)
        print(async_result.state)

    def test_celery_executor(self):
        kwargs = {
            "task_func": "hello_plugin2",
            "args": {"name": "aaaaa"},
            "async_flag": False
        }
        executor = celery_executor.CeleryExecutor(**kwargs)
        executor.start()
        executor.join()
        print(executor.result())
