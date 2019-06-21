__author__ = 'godq'

from dagflow.loader import get_DagRepo_Object
from dagflow.loader import get_StepExecutor_Class
from dagflow.dag import Dag
from dagflow.utils.session_manager import SessionManager


dag_repo = get_DagRepo_Object()
StepExecutor = get_StepExecutor_Class()


def start_flow(dag_name, dag_run_id=None):
    dag_run_id = dag_repo.add_dag_run(dag_name, dag_run_id)
    session = {
        "dag_name": dag_name,
        "dag_run_id": dag_run_id
    }
    session_manager = SessionManager(dag_name, dag_run_id)
    session_manager.set_session(data=session)
    steps_to_do = continue_flow(dag_name, dag_run_id)
    return dag_run_id, steps_to_do


def continue_flow(dag_name, dag_run_id, current_event=None):
    dag = Dag(dag_name, dag_run_id, event=current_event)
    steps_to_do = dag.fetch_steps_to_run(max_count=2)
    for step_name in steps_to_do:
        step = dag.fetch_step_info(step_name)
        task_func = step["task_func"]
        args = step["args"]
        kwargs = {
            "dag_name": dag_name,
            "dag_run_id": dag_run_id,
            "step_name": step_name,
            "task_func": task_func,
            "args": args,
        }
        executor = StepExecutor(kwargs)
        executor.start()
    return steps_to_do
