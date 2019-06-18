# __author__ = 'godq'


class BaseDagRepo:

    @staticmethod
    def find_dag(dag_name):
        raise NotImplemented

    @staticmethod
    def find_workstep(self, dag_name, step_name):
        raise NotImplemented

    @staticmethod
    def find_dag_runs(self, dag_name):
        raise NotImplemented