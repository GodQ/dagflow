# __author__ = 'godq'
from dagflow.dag_repos.base_dag_repo import BaseDagRepo


class RedisDagRepo(BaseDagRepo):
    @staticmethod
    def find_dag(dag_name):
        raise NotImplemented

    @staticmethod
    def find_workstep(self, dag_name, step_name):
        raise NotImplemented

    @staticmethod
    def find_dag_runs(self, dag_name):
        raise NotImplemented