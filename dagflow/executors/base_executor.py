__author__ = 'godq'

import time
import traceback

from dagflow.plugin_registry import get_plugin
from dagflow.event import send_event_message
from dagflow.step import StepStatus
from dagflow.event import EventOperation


def task_func_run(dag_name, dag_run_id, step_name, func, args, async_flag=False):
    if isinstance(func, str):
        func = BaseExecutor.get_step_func(func)

    event = dict()
    try:
        result = func(args)
        if async_flag is True:
            event["status"] = StepStatus.WaitingEvent
        else:
            event["status"] = StepStatus.Succeeded
        event['message'] = "Succeeded"
        event['result'] = result
    except Exception as e:
        event["status"] = StepStatus.Failed
        event['message'] = "{}: {} \n {}".format(type(e), str(e), traceback.format_exc())
        event['result'] = None
    assert isinstance(event, dict)
    event['dag_name'] = dag_name
    event['dag_run_id'] = dag_run_id
    event['step_name'] = step_name
    event['time'] = time.time()
    if async_flag is True:
        event['operation'] = EventOperation.Waiting_Event
    else:
        event['operation'] = EventOperation.Finish_Step
    print(event)
    send_event_message(event)


class BaseExecutor:
    def __init__(self, kwargs):
        assert isinstance(kwargs, dict)
        self.dag_name = kwargs["dag_name"]
        self.dag_run_id = kwargs["dag_run_id"]
        self.step_name = kwargs["step_name"]
        self.command = kwargs.get("command", None)
        self.task_func_name = kwargs.get("task_func", None)
        self.args = kwargs.get("args", None)
        self.async_flag = kwargs.get("async_flag", False)

    def task_func_run(self):
        """
        this method call the common task_func_run
        """
        print("async_flag: {}".format(self.async_flag))
        return task_func_run(self.dag_name, self.dag_run_id, self.step_name,
                             func=self.task_func_name, args=self.args, async_flag=self.async_flag)

    def __start__(self):
        """
        call common_task in __start__, if use celery, common_task must be in the celery task
        :return:
        """
        raise NotImplemented()

    def __join__(self):
        raise NotImplemented()

    def __result__(self):
        raise NotImplemented()

    def start(self):
        return self.__start__()

    def join(self):
        return self.__join__()

    def result(self):
        return self.__result__()

    @classmethod
    def send_event_message(cls, event):
        return send_event_message(event)

    @classmethod
    def get_step_func(cls, name):
        return get_plugin(name)
