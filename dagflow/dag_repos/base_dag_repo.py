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

    def list_dags(self, detail=False):
        raise NotImplemented

    def find_step_def(self, dag_name, step_name):
        raise NotImplemented

    def add_dag_run(self, dag_name, dag_run_id=None):
        raise NotImplemented

    def list_dag_runs(self, dag_name):
        raise NotImplemented

    def find_dag_run(self, dag_name, dag_run_id):
        raise NotImplemented

    def mark_dag_run_status(self, dag_name, dag_run_id, status):
        raise NotImplemented

    def add_dag_run_event(self, dag_name, dag_run_id, event):
        raise NotImplemented

    def find_dag_run_events(self, dag_name, run_id):
        raise NotImplemented