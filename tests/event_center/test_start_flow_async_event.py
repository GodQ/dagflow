__author__ = 'godq'

import unittest
import time

from dagflow.loader import get_MQ_Broker_Object, get_DagRepo_Object
from dagflow.event import EventOperation
from dagflow.flow_operation import send_finish_step_msg, send_start_flow_msg

mq_broker = get_MQ_Broker_Object()
dag_repo = get_DagRepo_Object()


class StartFlowAsyncEventTest(unittest.TestCase):

    def test_start_async_flow(self):
        dag_name = "dag_def_test_{}".format(time.time())
        dag = {
            "name": dag_name,
            "steps": [
                {
                    "name": "step1",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world1"
                    },
                    "upstreams": [],
                    "downstreams": ["step2"]
                },
                {
                    "name": "step2",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world2"
                    },
                    "async_flag": True,
                    "upstreams": [],
                    "downstreams": ["step3"]
                },
                {
                    "name": "step3",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world3"
                    },
                    "async_flag": True,
                    "upstreams": [],
                    "downstreams": ["step4"]
                },
                {
                    "name": "step4",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world4"
                    },
                    "upstreams": [],
                    "downstreams": []
                },
                {
                    "name": "step5",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world5"
                    },
                    "upstreams": [],
                    "downstreams": []
                },
            ]
        }

        dag_repo.delete_dag(dag_name)
        dag_repo.add_dag(dag_name, dag)
        run_id = dag_repo.add_dag_run(dag_name, time.time())
        print(run_id)

        # start_flow_event = {
        #     "dag_name": dag_name,
        #     "dag_run_id": run_id,
        #     "operation": EventOperation.Start_Flow
        # }
        # print(start_flow_event)
        # mq_broker.send_msg(start_flow_event)
        send_start_flow_msg(dag_name, run_id)

        print("wait 20s")
        time.sleep(20)
        print("send step2 finish msg")

        # step2_finish_event = {
        #     "dag_name": dag_name,
        #     "dag_run_id": run_id,
        #     "operation": EventOperation.Finish_Step,
        #     "step_name": "step2"
        # }
        # print(step2_finish_event)
        # mq_broker.send_msg(step2_finish_event)
        send_finish_step_msg(dag_name, run_id, step_name="step2")

        print("wait 20s")
        time.sleep(20)
        print("send step3 finish msg")

        # step3_finish_event = {
        #     "dag_name": dag_name,
        #     "dag_run_id": run_id,
        #     "operation": EventOperation.Finish_Step,
        #     "step_name": "step3"
        # }
        # print(step3_finish_event)
        # mq_broker.send_msg(step3_finish_event)
        send_finish_step_msg(dag_name, run_id, step_name="step3")
