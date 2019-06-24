__author__ = 'godq'

import subprocess
import fire
import os
import json

from dagflow.loader import get_DagRepo_Object
from dagflow.event_center.event_center import start_event_center

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

    def update_dag(self, dag_def_path):
        dag = self.__load_dag_def_file(dag_def_path)
        dag_name = dag.get("name")
        dag_repo.update_dag(dag_name=dag_name, content=dag)

    def start_event_center(self):
        start_event_center()


if __name__ == "__main__":
    manage = Manage()
    fire.Fire(manage)
