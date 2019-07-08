__author__ = 'godq'
import time

from dagflow.loader import get_DagRepo_Object
from dagflow.loader import get_StepExecutor_Class
from dagflow.loader import get_MQ_Broker_Object
from dagflow.event import EventOperation
from dagflow.dag import Dag
# from dagflow.utils.session_manager import SessionManager
from dagflow.step import StepStatus


dag_repo = get_DagRepo_Object()
StepExecutor = get_StepExecutor_Class()


def start_flow(dag_name, dag_run_id):
    if dag_repo.find_dag_run(dag_name, dag_run_id) is None:
        dag_run_id = dag_repo.add_dag_run(dag_name, dag_run_id)
    # session = {
    #     "dag_name": dag_name,
    #     "dag_run_id": dag_run_id
    # }
    # session_manager = SessionManager(dag_name, dag_run_id)
    # session_manager.set_session(data=session)
    steps_to_do = continue_flow(dag_name, dag_run_id)
    return dag_run_id, steps_to_do


def continue_flow(dag_name, dag_run_id, current_event=None):
    dag = Dag(dag_name, dag_run_id, event=current_event)
    steps_to_do = dag.fetch_steps_to_run(max_count=2)
    for step_name in steps_to_do:
        step = dag.fetch_step_info(step_name)
        task_func = step["task_func"]
        args = step.get("args", None)
        async_flag = step.get("async_flag", False)
        if not args:
            args = dict()
        args.update({
            "dag_name": dag_name,
            "dag_run_id": dag_run_id,
            "step_name": step_name,
            "task_func": task_func,
            "async_flag": async_flag,
        })
        kwargs = {
            "dag_name": dag_name,
            "dag_run_id": dag_run_id,
            "step_name": step_name,
            "task_func": task_func,
            "args": args,
            "async_flag": async_flag,
        }
        executor = StepExecutor(kwargs)
        executor.start()
    return steps_to_do


def stop_flow(dag_name, dag_run_id):
    dag_repo.stop_dag_run(dag_name, dag_run_id)


def specify_step_to_run(dag_name, dag_run_id, step_name):
    dag = Dag(dag_name, dag_run_id)
    steps_to_do = dag.specify_step_to_run(step_name)
    for step_name in steps_to_do:
        step = dag.fetch_step_info(step_name)
        task_func = step["task_func"]
        args = step.get("args", None)
        async_flag = step.get("async_flag", False)
        if not args:
            args = dict()
        args.update({
            "dag_name": dag_name,
            "dag_run_id": dag_run_id,
            "step_name": step_name,
            "task_func": task_func,
            "async_flag": async_flag,
        })
        kwargs = {
            "dag_name": dag_name,
            "dag_run_id": dag_run_id,
            "step_name": step_name,
            "task_func": task_func,
            "args": args,
            "async_flag": async_flag,
        }
        executor = StepExecutor(kwargs)
        executor.start()
    return steps_to_do


def send_start_flow_msg(dag_name, dag_run_id):
    step_finish_event = {
        "dag_name": dag_name,
        "dag_run_id": dag_run_id,
        "operation": EventOperation.Start_Flow,
        "time": time.time(),
    }
    print(step_finish_event)
    mq_broker = get_MQ_Broker_Object()
    mq_broker.send_msg(step_finish_event)


def send_finish_step_msg(dag_name, dag_run_id, step_name, status=None, message=None, result=None):
    if not status:
        status = StepStatus.Succeeded
    step_finish_event = {
        "dag_name": dag_name,
        "dag_run_id": dag_run_id,
        "operation": EventOperation.Finish_Step,
        "time": time.time(),
        "step_name": step_name,
        "status": status,
        "message": message,
        "result": result,
    }
    print(step_finish_event)
    mq_broker = get_MQ_Broker_Object()
    mq_broker.send_msg(step_finish_event)
