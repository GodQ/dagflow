__author__ = 'godq'


class BaseDagRepo:
    def add_dag(self, dag_name, content):
        raise NotImplemented

    def update_dag(self, dag_name, content):
        raise NotImplemented

    def delete_dag(self, dag_name):
        raise NotImplemented

    def find_dag(self, dag_name):
        raise NotImplemented

    def find_step_def(self, dag_name, step_name):
        raise NotImplemented

    def add_dag_run(self, dag_name, start_time):
        raise NotImplemented

    def find_dag_runs(self, dag_name):
        raise NotImplemented

    def add_dag_run_event(self, dag_name, run_id, status):
        raise NotImplemented

    def find_dag_run_events(self, dag_name, run_id):
        raise NotImplemented