__author__ = 'godq'
from dagflow.plugin_registry import get_plugin


class BaseExecutor:
    def __init__(self, **kwargs):
        self.step_desc_dict = kwargs.get("step_desc_dict")
        if self.step_desc_dict:
            self.async_flag = self.step_desc_dict.get("async_flag", False)
            self.command = self.step_desc_dict.get("command", None)
            self.task_func = self.step_desc_dict.get("task_func", None)
            self.args = self.step_desc_dict.get("args", None)
        else:
            self.async_flag = kwargs.get("async_flag", False)
            self.command = kwargs.get("command", None)
            self.task_func = kwargs.get("task_func", None)
            self.args = kwargs.get("args", None)

    def start(self):
        raise NotImplemented()

    def join(self):
        '''
        if not async, should wait for task finish
        '''
        raise NotImplemented()

    def result(self):
        raise NotImplemented()

    @classmethod
    def get_step_func(cls, name):
        return get_plugin(name)