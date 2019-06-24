__author__ = 'godq'


class StepStatus:
    Created = "Created"
    Succeeded = "Succeeded"
    Failed = "Failed"
    WaitingEvent = "WaitingEvent"

    finished_status = [Failed, Succeeded]

    @classmethod
    def is_finished_status(cls, status):
        if status in cls.finished_status:
            return True
        else:
            return False



