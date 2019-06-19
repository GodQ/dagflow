__author__ = 'godq'


class StepStatus:
    Created = "Created"
    Succeeded = "Succeeded"
    Failed = "Failed"

    finished_status = [Failed, Succeeded]

    @classmethod
    def is_finished_status(cls, status):
        if status in cls.finished_status:
            return True
        else:
            return False


class BaseStep:
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

    def run(self):
        pass


