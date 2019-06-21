__author__ = 'godq'

import logging
import random
import os
import subprocess
import time

from dagflow.executors.base_executor import BaseExecutor, task_func_run
from dagflow.config import Config

logger = logging.getLogger('sequential-executor')


class CeleryConfigClass:
    def __init__(self, dict_config):
        assert isinstance(dict_config, dict)
        for k, v in dict_config.items():
            setattr(self, k, v)


class SequentialExecutor(BaseExecutor):
    def __init__(self, kwargs):
        super(SequentialExecutor, self).__init__(kwargs)
        self.func_result = None

    def __start__(self):
        if self.command:
            pass
        elif self.task_func_name:
            print("run {} by SequentialExecutor".format(self.step_name))
            ret = self.task_func_run()
            self.func_result = ret if ret else "Finished"

    def __join__(self):
        logger.info("New Task Finished for func {}".format(self.task_func_name))

    def __result__(self):
        assert self.func_result
        return self.func_result

