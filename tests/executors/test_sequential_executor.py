# __author__ = 'godq'
import unittest
import time

import dagflow.executors.sequential_executor as sequential_executor


class SequentialExecutorTest(unittest.TestCase):

    def test_celery_executor(self):
        dag_run_id = "1561109110.3026867"
        kwargs = {
            "dag_name": "dag_def_test",
            "dag_run_id": dag_run_id,
            "step_name": "step1",
            "task_func": "hello_plugin1",
            "args": {"name": "aaaaa"},
        }
        executor = sequential_executor.SequentialExecutor(kwargs)
        executor.start()
        executor.join()
        print(executor.result())
