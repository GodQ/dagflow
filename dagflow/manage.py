__author__ = 'godq'

import subprocess
import fire
import os
import sys
import json
from pprint import pprint

from dagflow.loader import get_DagRepo_Object
dag_repo = get_DagRepo_Object()


class Manage:
    def __init__(self):
        pass

    @staticmethod
    def __load_dag_def_file(dag_def_path):
        if not os.path.isfile(dag_def_path):
            raise Exception("Dag def file {} not found!".format(dag_def_path))
        with open(dag_def_path, "r") as fd:
            data = fd.read()
        if isinstance(data, bytes):
            data = data.decode()
        dag = json.loads(data)
        return dag

    def register_dag(self, dag_def_path):
        dag = self.__load_dag_def_file(dag_def_path)
        dag_name = dag.get("name")
        dag_repo.add_dag(dag_name=dag_name, content=dag)
        print("Dag {} created successfully by path {}".format(dag_name, dag_def_path))

    def update_dag(self, dag_def_path):
        dag = self.__load_dag_def_file(dag_def_path)
        dag_name = dag.get("name")
        dag_repo.update_dag(dag_name=dag_name, content=dag)
        print("Dag {} updated successfully by path {}".format(dag_name, dag_def_path))

    def init_project(self, dst_dir="dagflow_project"):
        from dagflow.tools.init_project import create_project
        dst = create_project(dst_dir)
        dst = os.path.abspath(dst)
        print("New dagflow project has created in path {}".format(dst))

    def start_event_center(self):
        # if in user folder, load user's plugins
        cwd = os.path.abspath(os.getcwd())
        if os.path.isdir("plugins"):
            os.environ["USER_PLUGINS_PATH"] = os.path.join(cwd, "plugins")

        sys.path.append(cwd)

        from dagflow.event_center.event_center import start_event_center

        start_event_center()

    def start_worker(self, worker_count=None):
        from dagflow.utils.command import run_cmd
        from dagflow.config import Config
        # if in user folder, load user's plugins
        cwd = os.path.abspath(os.getcwd())
        if os.path.isdir("plugins"):
            os.environ["USER_PLUGINS_PATH"] = os.path.join(cwd, "plugins")
        if not worker_count:
            worker_count = Config.celery_configs.get("worker_count", 1)

        cmd = "celery worker -A dagflow.executors.celery_executor -c {}".format(worker_count)
        print(cmd)
        run_cmd(cmd, daemon=True)
        print("Dagflow worker has started successfully")

    def run_dag(self, dag_name):
        import time
        from dagflow.flow_operation import send_start_flow_msg
        dag_run_id = str(time.time())
        send_start_flow_msg(dag_name, dag_run_id)
        print("Dag {} started successfully with dag_run_id {}".format(dag_name, dag_run_id))

    def list_dags(self, detail=False):
        from dagflow.loader import get_DagRepo_Object
        repo = get_DagRepo_Object()
        detail = str(detail).strip().lower()
        detail = True if detail == "true" else False
        dag_list = repo.list_dags(detail=detail)
        for dag in dag_list:
            pprint(dag)

    def list_dag_runs(self, dag_name):
        from dagflow.loader import get_DagRepo_Object
        repo = get_DagRepo_Object()
        dag_run_list = repo.list_dag_runs(dag_name=dag_name)
        for dag in dag_run_list:
            pprint(dag)

    def list_dag_run_events(self, dag_name, dag_run_id):
        from dagflow.loader import get_DagRepo_Object
        repo = get_DagRepo_Object()
        dag_run_events_list = repo.find_dag_run_events(dag_name=dag_name, dag_run_id=dag_run_id)
        for dag in dag_run_events_list:
            pprint(dag)


def main():
    manage = Manage()
    fire.Fire(manage)


if __name__ == "__main__":
    main()
