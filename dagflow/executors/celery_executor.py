# __author__ = 'godq'
from celery import Celery
from celery import states as celery_states
import logging
import random
import os
import subprocess
import time

from dagflow.executors.base_executor import BaseExecutor
from dagflow.config import Config as configuration

logger = logging.getLogger('celery-executor')

'''
To start the celery worker, run the command:
dagflow worker
'''

celery_configuration = configuration.CELERY_CONFIGS


class CeleryConfigClass:
    def __int__(self, dict_config):
        assert isinstance(dict_config, dict)
        for k, v in dict_config.items():
            setattr(self, k, v)


app = Celery(
    configuration.CELERY_APP_NAME,
    config_source=CeleryConfigClass(celery_configuration))


class PythonFuncTask(app.Task):
    def __init__(self, *args, **kwargs):
        super(PythonFuncTask, self).__init__(*args, **kwargs)

    def after_return(self, status, retval, *args, **kwargs):
        pass


def execute_command(command_to_exec):
    logger.info("Executing command in Celery: %s", command_to_exec)
    env = os.environ.copy()
    try:
        subprocess.check_call(command_to_exec, stderr=subprocess.STDOUT,
                              close_fds=True, env=env)
    except subprocess.CalledProcessError as e:
        logger.exception('execute_command encountered a CalledProcessError')
        logger.error(e.output)

        raise Exception('Celery command failed')


@app.task(name='workflow.common_workflow', bind=True,
          base=PythonFuncTask)
def common_celery_task(self, func_name, args):
    func = None
    ret = func(args)
    return ret


class CeleryExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super(CeleryExecutor).__init__(**kwargs)
        self.celery_async_result = None

    def start(self):
        if self.command:
            pass
        elif self.task_func:
            task_obj = common_celery_task.s(self.task_func, self.args)
            countdown = random.uniform(0.1, 2.0)
            self.celery_async_result = task_obj.apply_async(countdown=countdown)
            logger.info("New Celery Task Created for func {}, id: {}".
                        format(self.task_func, self.celery_async_result.id))

    def join(self):
        # if not async, should wait for task finish
        assert self.celery_async_result
        task = self.celery_async_result

        if self.async_flag is True:
            logger.info("Celery Task skip join, id:{}, status:{}".format(task.id, task.state))
            return

        while task.state not in celery_states.READY_STATES:
            time.sleep(5)
        logger.info("Celery Task finished, id:{}, status:{}".format(task.id, task.state))

    def result(self):
        assert self.celery_async_result
        return self.celery_async_result

