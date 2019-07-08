__author__ = 'godq'


class StepStatus:
    Created = "Created"
    Succeeded = "Succeeded"
    Failed = "Failed"   # fail
    Stopped = "Stopped"  # user abort
    Running = "Running"
    WaitingEvent = "WaitingEvent"
    ReScheduled = "ReScheduled"  # reschedule by filter
    Forbidden = "Forbidden"   # forbid by filter

    finished_status = [Failed, Succeeded, Stopped, Forbidden]
    stopped_status = [Failed, Stopped, Forbidden]

    @classmethod
    def is_finished_status(cls, status):
        if status in cls.finished_status:
            return True
        else:
            return False

    @classmethod
    def is_stopped_status(cls, status):
        if status in cls.stopped_status:
            return True
        else:
            return False



