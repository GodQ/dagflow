import time
import json
import logging
import logging.config
import sys
import os
sys.path.append(os.path.dirname(__file__))

from dagflow.event_center.base_request_filter import RequestFilter, RequestFilterStatus, auto_load_filter
from dagflow.flow_operation import start_flow, continue_flow, specify_step_to_run
from dagflow.dag import Dag
from dagflow.loader import get_DagRepo_Object, get_MQ_Broker_Object
from dagflow.step import StepStatus
from dagflow.event import EventOperation


logger = logging.getLogger('dagflow')
# auto_load_filter()
dag_repo = get_DagRepo_Object()
mq_broker = get_MQ_Broker_Object()

"""
event fields:
             dag_name: ...
             run_id: 111
             step_name: ...
             operation: start_flow or finish_step or continue_flow
             time: current_time
             status: step.StepStatus
             message: ...
             result: return of this step
"""


def on_message(channel, method_frame, header_frame, event_body):
    try:
        if isinstance(event_body, bytes):
            event_body = event_body.decode()
        if isinstance(event_body, str):
            event_body = json.loads(event_body)

        # logger.info("now: {}".format(time.time()))
        # logger.info('method_frame.delivery_tag: {}'.format(method_frame.delivery_tag))
        logger.info('body: {}'.format(event_body))

        operation = event_body.get("operation", None)
        dag_name = event_body.get("dag_name", None)
        dag_run_id = event_body.get("dag_run_id", None)
        step_name = event_body.get("step_name", None)
        step_status = event_body.get("status", None)
        error_handle_step = None
        error_handle_flag = False
        dag_run_info = dag_repo.find_dag_run(dag_name, dag_run_id)

        logger.info("Step {} of dag run <{}>:<{}> {}".format(
            step_name, dag_name, dag_run_id, step_status
        ))

        # record this event to repo
        dag_repo.add_dag_run_event(dag_name, dag_run_id, event_body)

        if operation == EventOperation.Finish_Step:
            step = Dag(dag_name).fetch_step_info(step_name)
            error_handle_step = step.get("error_handle_step", None)
            last_step_flag = step.get("last_step_flag", False)
            dag_run_status = dag_run_info.get("status", None)

            if step_status == StepStatus.Failed:
                logger.error("Step {} of dag run <{}>:<{}> failed, downstream steps will not be triggered".format(
                    step_name, dag_name, dag_run_id
                ))
                dag_repo.mark_dag_run_status(dag_name, dag_run_id, status=StepStatus.Failed)
                if not error_handle_step:
                    return
                else:
                    error_handle_flag = True
            elif dag_run_status == StepStatus.Failed:
                logger.error("dag run <{}>:<{}> has failed, step {} {}, downstream steps will not be triggered".format(
                    dag_name, dag_run_id, step_name, step_status
                ))
                return
            elif dag_run_status == StepStatus.Stopped:
                logger.error("dag run <{}>:<{}> has been stopped, step {} {}, downstream steps will not be triggered".format(
                    dag_name, dag_run_id, step_name, step_status
                ))
                return
            elif dag_run_status == StepStatus.Forbidden:
                logger.error("dag run <{}>:<{}> has been forbidden, step {} {}, downstream steps will not be triggered".format(
                    dag_name, dag_run_id, step_name, step_status
                ))
                return
            elif dag_run_status == StepStatus.Succeeded:
                logger.info("dag run <{}>:<{}> has been successful, step {} {}, downstream steps will not be triggered".format(
                    dag_name, dag_run_id, step_name, step_status
                ))
                return

            if step_status == StepStatus.Succeeded and last_step_flag is True:
                logger.info("Last Step {} of dag run <{}>:<{}> succeeded, dag run succeeds!".format(
                    step_name, dag_name, dag_run_id
                ))
                dag_repo.mark_dag_run_status(dag_name, dag_run_id, status=StepStatus.Succeeded)
                return
        elif operation == EventOperation.Waiting_Event:
            logger.info("Step {} of dag run <{}>:<{}> finished, but is async, "
                        "will do nothing and wait for following event".format(
                step_name, dag_name, dag_run_id
            ))
            dag = Dag(dag_name, dag_run_id, event_body)
            dag.update_step_status()
            return

        status, msg = RequestFilter.run_filter(event_body)
        logger.warning("Event Request Filter Result: {}".format(RequestFilterStatus.to_str(status)))

        if status == RequestFilterStatus.PASS:
            if operation == EventOperation.Start_Flow:
                run_id, steps_to_do = start_flow(dag_name=dag_name, dag_run_id=dag_run_id)
                logger.info("New dag run id <{}> created for dag <{}>, steps_to_run: {}".format(
                    dag_run_id, dag_name, steps_to_do))

            elif operation in [EventOperation.Finish_Step]:
                if error_handle_flag is False:
                    steps_to_do = continue_flow(dag_name=dag_name, dag_run_id=dag_run_id, current_event=event_body)
                    logger.info("Existent dag run <{}> of dag <{}>, steps_to_run: {}".format(
                        dag_run_id, dag_name, steps_to_do))
                else:
                    specify_step_to_run(dag_name=dag_name, dag_run_id=dag_run_id,
                                        step_name=error_handle_step)
                    logger.info("Existent dag run <{}> of dag <{}>, run error handle step: {}".format(
                        dag_run_id, dag_name, step_name))
            dag_repo.mark_dag_run_status(dag_name, dag_run_id, status=StepStatus.Running)

        elif status == RequestFilterStatus.DELAY:
            mq_broker.send_delayed_msg(event_body, delay_seconds=30)
            logger.warning("Request will be re-checked in 30s, {}".format(event_body))
            dag_repo.mark_dag_run_status(dag_name, dag_run_id, status=StepStatus.ReScheduled)
        elif status == RequestFilterStatus.FORBID:
            logger.error("Request is rejected, \n"
                           "Request: {} \n"
                           "Error Info: {}".format(event_body, msg))
            dag_repo.mark_dag_run_status(dag_name, dag_run_id, status=StepStatus.Forbidden)
    finally:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def start_event_center():
    mq_broker.run_listener(on_message=on_message)


if __name__ == "__main__":
    start_event_center()