__author__ = 'godq'

import unittest
import time

from dagflow.loader import get_MQ_Broker_Object, get_DagRepo_Object
from dagflow.event import EventOperation

mq_broker = get_MQ_Broker_Object()
dag_repo = get_DagRepo_Object()


class StartFlowEventTest(unittest.TestCase):

    def test_start_flow(self):
        dag_name = "dag_def_test_{}".format(time.time())
        dag = {
            "name": dag_name,
            "steps": [
                {
                    "name": "step1",
                    "task_func": "hello_plugin",
                    "args": {
                        "name": "hello world1"
                    },
                    "upstreams": [],
                    "downstreams": ["step2", "step3"]
                },
                {
                    "name": "step2",
                    "task_func": "hello_plugin",
                    "args": {
                        "name": "hello world2"
                    },
                    "upstreams": [],
                    "downstreams": ["step4"]
                },
                {
                    "name": "step3",
                    "task_func": "hello_plugin",
                    "args": {
                        "name": "hello world3"
                    },
                    "upstreams": [],
                    "downstreams": ["step4"]
                },
                {
                    "name": "step4",
                    "task_func": "hello_plugin",
                    "args": {
                        "name": "hello world4"
                    },
                    "upstreams": [],
                    "downstreams": [],
                    "last_step_flag": True
                },
                {
                    "name": "step5",
                    "task_func": "hello_plugin",
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

        start_flow_event = {
            "dag_name": dag_name,
            "dag_run_id": run_id,
            "operation": EventOperation.Start_Flow
        }
        print(start_flow_event)
        mq_broker.send_msg(start_flow_event)
