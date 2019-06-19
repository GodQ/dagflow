# __author__ = 'godq'
import unittest
import time

import dagflow.executors.sequential_executor as sequential_executor


class SequentialExecutorTest(unittest.TestCase):
    '''
    first run "celery worker -A dagflow.executors.celery_executor" to start celery worker
    '''

    def test_common_celery_task(self):
        result = sequential_executor.common_task("hello_plugin2", {"name": "hello"})
        print(result)

    def test_celery_executor(self):
        kwargs = {
            "task_func": "hello_plugin2",
            "args": {"name": "aaaaa"},
            "async_flag": False
        }
        executor = sequential_executor.SequentialExecutor(**kwargs)
        executor.start()
        executor.join()
        print(executor.result())
