__author__ = 'godq'
import os
import sys

from dagflow.flow_operation import send_start_flow_msg as sdk_send_start_flow_msg, \
    send_finish_step_msg as sdk_send_finish_step_msg
from dagflow.loader import get_DagRepo_Object
dag_repo = get_DagRepo_Object()


def send_finish_step_msg(dag_name, dag_run_id, step_name, status=None, message=None, result=None):
    return sdk_send_finish_step_msg(dag_name, dag_run_id, step_name, status, message, result)


def send_start_flow_msg(dag_name, dag_run_id):
    return sdk_send_start_flow_msg(dag_name, dag_run_id)


def register_dag(dag_def):
    assert isinstance(dag_def, dict)
    dag_name = dag_def.get("name")
    dag_repo.add_dag(dag_name=dag_name, content=dag_def)
    # print("Dag {} created successfully".format(dag_name))


def update_dag(dag_def):
    assert isinstance(dag_def, dict)
    dag_name = dag_def.get("name")
    dag_repo.update_dag(dag_name=dag_name, content=dag_def)
    # print("Dag {} updated successfully".format(dag_name))


def start_event_center():
    # if in user folder, load user's plugins
    cwd = os.path.abspath(os.getcwd())
    if os.path.isdir("plugins"):
        os.environ["USER_PLUGINS_PATH"] = os.path.join(cwd, "plugins")

    sys.path.append(cwd)

    from dagflow.event_center.event_center import start_event_center

    start_event_center()


def start_worker(worker_count=None):
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
    # print("Dagflow worker has started successfully")


def get_dag(dag_name):
    assert dag_name
    return dag_repo.find_dag(dag_name)


def run_dag(dag_name, dag_run_id=None):
    import time
    from dagflow.flow_operation import send_start_flow_msg
    if not dag_run_id:
        dag_run_id = str(time.time())
    send_start_flow_msg(dag_name, dag_run_id)
    # print("Dag {} started successfully with dag_run_id {}".format(dag_name, dag_run_id))
    return dag_run_id


def stop_dag_run(dag_name, dag_run_id):
    assert dag_name and dag_run_id
    dag_repo.stop_dag_run(dag_name, dag_run_id)
    # print("Dag {} stopped successfully with dag_run_id {}".format(dag_name, dag_run_id))


def get_dag_run(dag_name, dag_run_id):
    assert dag_name and dag_run_id
    return dag_repo.find_dag_run(dag_name, dag_run_id)


def list_dags(detail=False):
    from dagflow.loader import get_DagRepo_Object
    repo = get_DagRepo_Object()
    detail = str(detail).strip().lower()
    detail = True if detail == "true" else False
    dag_list = repo.list_dags(detail=detail)
    return dag_list


def list_dag_runs(dag_name):
    from dagflow.loader import get_DagRepo_Object
    repo = get_DagRepo_Object()
    dag_run_list = repo.list_dag_runs(dag_name=dag_name)
    return dag_run_list


def list_dag_run_events(dag_name, dag_run_id):
    from dagflow.loader import get_DagRepo_Object
    repo = get_DagRepo_Object()
    dag_run_events_list = repo.find_dag_run_events(dag_name=dag_name, dag_run_id=dag_run_id)
    return dag_run_events_list