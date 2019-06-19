__author__ = 'godq'

import logging
import random
import os
import subprocess
import time

from dagflow.executors.base_executor import BaseExecutor
from dagflow.config import Config

logger = logging.getLogger('sequential-executor')


class CeleryConfigClass:
    def __init__(self, dict_config):
        assert isinstance(dict_config, dict)
        for k, v in dict_config.items():
            setattr(self, k, v)


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


def common_task(func_name, args):
    func = BaseExecutor.get_step_func(func_name)
    ret = func(args)
    return ret


class SequentialExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super(SequentialExecutor, self).__init__(**kwargs)
        self.func_result = None

    def start(self):
        if self.command:
            pass
        elif self.task_func:
            ret = common_task(self.task_func, self.args)
            self.func_result = ret

    def join(self):
        logger.info("New Task Finished for func {}".format(self.task_func))

    def result(self):
        assert self.func_result
        return self.func_result

