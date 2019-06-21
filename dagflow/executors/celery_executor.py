__author__ = 'godq'
from celery import Celery
from celery import states as celery_states
import logging
import random
import os
import subprocess
import time

from dagflow.executors.base_executor import BaseExecutor, task_func_run
from dagflow.config import Config

logger = logging.getLogger('celery-executor')

'''
To start the celery worker, run the command:
dagflow worker
'''


class CeleryConfigClass:
    def __init__(self, dict_config):
        assert isinstance(dict_config, dict)
        for k, v in dict_config.items():
            setattr(self, k, v)


app = Celery(Config.celery_app_name)
app.config_from_object(CeleryConfigClass(Config.celery_configs))


class PythonFuncTask(app.Task):
    def __init__(self, *args, **kwargs):
        super(PythonFuncTask, self).__init__(*args, **kwargs)

    def after_return(self, status, retval, *args, **kwargs):
        pass


@app.task(name='workflow.common_workflow', bind=True,
          base=PythonFuncTask)
def common_celery_task(self, dag_name, dag_run_id, step_name, func_name, args):
    # func_name = BaseExecutor.get_step_func(func_name)
    ret = task_func_run(dag_name, dag_run_id, step_name, func_name, args)
    # ret = func(args)
    return ret


class CeleryExecutor(BaseExecutor):
    def __init__(self, kwargs):
        super(CeleryExecutor, self).__init__(kwargs)
        self.celery_async_result = None

    def __start__(self):
        if self.command:
            pass
        elif self.task_func_name:
            task_obj = common_celery_task.s(self.dag_name, self.dag_run_id, self.step_name, self.task_func_name, self.args)
            countdown = random.uniform(0.1, 2.0)
            self.celery_async_result = task_obj.apply_async(countdown=countdown)
            logger.info("New Celery Task Created for func {}, id: {}".
                        format(self.task_func_name, self.celery_async_result.id))

    def __join__(self):
        # if not async, should wait for task finish
        assert self.celery_async_result
        task = self.celery_async_result

        while task.state not in celery_states.READY_STATES:
            time.sleep(1)
        logger.info("Celery Task finished, id:{}, status:{}".format(task.id, task.state))
        print(task.ready())
        return task.id

    def __result__(self):
        assert self.celery_async_result
        return self.celery_async_result.get()

